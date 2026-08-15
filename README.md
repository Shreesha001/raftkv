# raftkv

**A distributed key-value store in Go, with the Raft consensus algorithm implemented from scratch.**

No `hashicorp/raft`. No `etcd/raft`. No consensus library of any kind — leader election, log replication, and the safety rules are written here, from the [paper](https://raft.github.io/raft.pdf).

Three nodes replicate a log of commands. The cluster survives losing one node without losing a single acknowledged write, and without ever electing two leaders.

```
go test ./... -race     →  105 tests, deterministic, no sleeps
```

---

## Kill the leader

The whole point of the system, in one terminal session. This is real output.

```console
$ curl -L -X PUT localhost:8082/kv/before -d 'kept'      # write via a follower
write: 200

$ curl -s localhost:8081/status
{"commit":2,"id":1,"leader":1,"state":"Leader","term":1}

$ kill -9 <leader pid>                                    # no warning, no cleanup
```

Four seconds later:

```console
$ curl -s localhost:8082/status
{"commit":3,"id":2,"leader":2,"state":"Leader","term":2}   ← node 2 took over

$ curl -L localhost:8082/kv/before
kept [200]                                                 ← the write survived

$ curl -L -X PUT localhost:8082/kv/after -d 'still works'
write after failover: 200                                  ← still serving
```

Bring the dead node back and it rejoins, discovers it has been superseded, and catches up on everything it missed — with no operator involvement:

```console
$ ./raftkvd -id 1 ...                                      # restart it

$ curl -s localhost:8081/status
{"commit":3,"id":1,"leader":2,"state":"Follower","term":2}

$ curl -L localhost:8081/kv/after
still works                                                ← caught up
```

Nobody told node 1 that node 2 had died. Nobody told node 1 it had been replaced. Both facts were inferred from silence and from a number that went up.

---

## The problem this solves

One server holding your data is a single point of failure. Three servers are worse, unless you are very careful.

Two clients write at the same time:

```
Client A → Server 1:  SET x=5
Client B → Server 2:  SET x=9
```

Messages cross the network and arrive in different orders at different servers:

```
Server 1 hears:  x=5, x=9   →  x is 9
Server 2 hears:  x=9, x=5   →  x is 5
Server 3 hears:  x=5, x=9   →  x is 9
```

Three servers, three answers, no way to say which is right. **The hard problem in distributed systems is not storage — it is agreement.**

### How Raft solves it

**1 — Elect one leader.** All writes go through a single node, so one brain decides the order. Not for speed; for the existence of an order at all.

**2 — Replicate the log, not the data.** Nodes exchange numbered instructions, never values:

```
index:    1          2          3          4
term:     1          1          3          3
        SET x=5    SET y=2    DEL x     SET x=9
```

Same instructions, same order, same starting point → same final state. This is *state machine replication*, and it works because **"do you have entry 7?" is a question with a crisp answer that survives crashes, retries, and duplicate packets.** "Is your data correct?" is not.

**3 — Require a majority.** A leader may only act while it can reach more than half the cluster. Two groups each larger than half must share at least one member, and that member will not back a second leader. So split-brain is impossible **by arithmetic, not by luck** — which is why clusters are 3, 5, or 7 nodes, tolerating 1, 2, or 3 failures.

### How failure is detected

A dead machine cannot announce that it died. It just goes quiet.

So the leader sends an empty **heartbeat** every few tens of milliseconds meaning *"still here."* A follower that hears nothing for a full election timeout concludes the leader is gone and stands for election. **Silence is the signal.** Everything else — the failover above — follows from that one idea.

---

## Watch an election happen

Run at `-v=2` and the protocol narrates itself:

```
"node started"           node=1  peers=2
"node started"           node=2  peers=2
"node started"           node=3  peers=2

"election timeout elapsed"  node=2  term=0  state="Follower"   ← node 2's timer fires first
"standing for election"     node=2  term=1
"stepping down"             node=1  oldTerm=0 newTerm=1        ← 1 and 3 see the higher term
"vote requested"            node=1  candidate=2  granted=true
"stepping down"             node=3  oldTerm=0 newTerm=1
"vote requested"            node=3  candidate=2  granted=true
"vote received"             node=2  from=3  granted=true  needed=2
"became leader"             node=2  term=1                     ← majority reached
```

Kill node 2, and node 1 works it out for itself:

```
"election timeout elapsed"  node=1  term=1  state="Follower"   ← heartbeats stopped
"standing for election"     node=1  term=2                     ← new term
"vote requested"            node=3  candidate=1  granted=true
"became leader"             node=1  term=2
```

Verbosity is klog-style, so this detail is off by default and one flag away when you need it:

| `-v` | Shows |
|:----:|-------|
| `0` | errors only |
| `1` | elections, leader changes *(default)* |
| `2` | replication, commits |
| `3` | every heartbeat and RPC |

---

## Design

```
cmd/raftkvd/          flags, wiring, graceful shutdown
internal/
  raft/               consensus core — pure logic, zero I/O
  storage/            durable term, vote, and log
  transport/          Raft messages as JSON over HTTP
  kv/                 the state machine: commands → map
  server/             the single goroutine that owns a node
  api/                client HTTP, with leader redirect
  logging/            klog verbosity levels
```

### The core knows nothing

`internal/raft` has no idea what a network, a disk, a clock, or a key-value store is. They arrive as interfaces, and commands are opaque `[]byte` it moves without ever interpreting.

This is not a stylistic preference — it is verified:

```console
$ go list -deps ./internal/raft | grep raftkv
github.com/Shreesha001/raftkv/internal/raft      ← imports nothing else in the project
```

### The node is a pure state machine

`raft.Node` owns no goroutine, no timer, and no connection. Time and messages are *pushed in*; outbound messages are *pulled out*:

```go
n.Tick()                  // one unit of time passed
n.Step(msg)               // a message arrived
msgs := n.Messages()      // what do you want sent?
entries := n.CommittedEntries()   // what is safe to apply?
```

The goroutine, the real clock, and the network live in `internal/server`, outside the protocol. Which leads to the part of this project I would actually defend in an interview:

### Deterministic tests for a distributed system

Because the node has no clock and no network, a test can run an entire cluster in **one goroutine**, decide exactly which messages arrive and which are dropped, and advance one node's clock while another's stands still.

```go
c := newCluster(t, 1, 2, 3)
leader := c.awaitLeader(100)

c.isolated[leader.ID()] = true          // partition the leader away
replacement := c.awaitLeader(100, leader.ID())

c.isolated[leader.ID()] = false         // heal the partition
c.run(300)
// assert: exactly one leader, all terms agree, no committed entry lost
```

**No `time.Sleep` appears anywhere in the test suite.** Split votes, network partitions, divergent logs, and crashed leaders are reproduced in microseconds and give the identical result on every run. This is the difference between tests that find consensus bugs and tests that flake in CI and get retried until green.

Real-process integration tests over loopback HTTP cover the parts determinism cannot reach.

### Other decisions worth naming

- **`Term` and `Index` are distinct types**, not bare `uint64`. Raft code handles `prevLogIndex`, `prevLogTerm`, `lastLogIndex`, and `lastLogTerm` side by side; transposing two is the classic bug and produces behaviour that looks almost right. Now it does not compile.
- **A sentinel entry at log index 0.** The consistency check constantly asks for the term *before* a position, which for the first entry is index 0. One fake entry removes a special case from a dozen call sites.
- **Durable before acknowledged.** A vote is written to disk before it is promised, and entries before they are acknowledged — because a node that forgets its vote after a crash can vote twice in one term, which elects two leaders, which loses data. Storage failure panics rather than continues: crashing is recoverable, silent divergence is not.
- **Atomic persistence.** Write to a temp file → `fsync` → `rename` → `fsync` the directory. A crash at any point leaves either the complete old state or the complete new one.

---

## Two bugs worth reading about

Both were invisible until the system was actually run, and both are the interesting kind.

### Stale reads after failover

A newly elected leader had a committed entry sitting in its log — and answered `404` for it.

Not a coding mistake. **Raft §5.4.2 forbids a leader from committing entries inherited from earlier terms**, because such an entry can still be overwritten by a future leader. With no new writes arriving, that entry stayed uncommitted forever, so the state machine never applied it, so a value already acknowledged to a client read back as missing.

The fix is §8 of the paper: **a new leader appends a no-op entry the instant it takes office.** That entry belongs to the current term, so it can commit — and by the log matching property it drags every inherited entry into committed state with it. Plus a rule that a leader refuses reads until that no-op commits, closing the stale-read window entirely.

### Two sentinel errors

`raft.ErrNotLeader` and `server.ErrNotLeader` were different values. `errors.Is` said no, so writes to a follower returned `503` instead of redirecting to the leader. Fixed by translating at the package boundary — callers should match on one package's sentinel and never need to know which layer refused.

---

## Try it

```bash
go build -o raftkvd ./cmd/raftkvd

PEERS=1=localhost:8081,2=localhost:8082,3=localhost:8083
./raftkvd -id 1 -peers $PEERS -data-dir ./data &
./raftkvd -id 2 -peers $PEERS -data-dir ./data &
./raftkvd -id 3 -peers $PEERS -data-dir ./data &
```

Write to any node — followers redirect, so `-L` is all you need:

```bash
curl -L -X PUT localhost:8082/kv/greeting -d 'hello world'
curl -L localhost:8081/kv/greeting          # hello world
curl -L -X DELETE localhost:8083/kv/greeting
curl -s localhost:8081/status
```

| Endpoint | Behaviour |
|---|---|
| `GET /kv/{key}` | `200` value, `404` unknown, `307` to leader, `503` no leader |
| `PUT /kv/{key}` | Returns `200` only once a majority holds the write durably |
| `DELETE /kv/{key}` | Same guarantee |
| `GET /status` | Role, term, leader, commit index |

Then kill the leader and watch the top of this README happen.

---

## What is not here

Stated plainly, because a system's limits matter as much as its features:

- **No snapshotting.** The log grows without bound, and every write rewrites the whole state file. Correct, and obviously wrong above a certain scale.
- **Reads are not fully linearizable.** A leader deposed by a partition can serve stale data until it notices. Closing that requires confirming leadership with a heartbeat round before each read.
- **Fixed membership.** Nodes cannot be added or removed while running.
- **JSON over HTTP, not gRPC.** One struct, low volume, no protobuf toolchain. Any type satisfying the `Transport` interface can replace it.

Each is documented in the code at the place it matters.

---

## Reference

[*In Search of an Understandable Consensus Algorithm*](https://raft.github.io/raft.pdf) — Diego Ongaro and John Ousterhout. Section 5.4.1 is the election restriction, 5.4.2 the commitment rule, and section 8 the no-op that fixed the bug above.
