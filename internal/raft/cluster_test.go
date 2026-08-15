package raft

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/go-logr/logr"
)

// cluster runs several nodes in one goroutine with an in-process network.
//
// Nothing here is concurrent and no real time passes: the test decides when
// clocks advance and which messages arrive. That is what makes scenarios like
// partitions and leader crashes reproducible rather than timing-dependent, and
// it is why the Node is a pure state machine with no goroutine of its own.
type cluster struct {
	t     *testing.T
	nodes map[NodeID]*Node
	// isolated nodes exchange messages only with each other, modelling a
	// network partition. Delivery is silently dropped across the divide, which
	// is exactly what a real partition looks like to Raft.
	isolated map[NodeID]bool
}

func newCluster(t *testing.T, ids ...NodeID) *cluster {
	t.Helper()

	c := &cluster{
		t:        t,
		nodes:    make(map[NodeID]*Node, len(ids)),
		isolated: make(map[NodeID]bool),
	}
	for _, id := range ids {
		peers := make([]NodeID, 0, len(ids)-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		c.nodes[id] = NewNode(Config{
			ID:            id,
			Peers:         peers,
			ElectionTick:  10,
			HeartbeatTick: 2,
			Logger:        logr.Discard(),
			// Distinct seeds so nodes do not all time out on the same tick.
			Rand: rand.New(rand.NewSource(int64(id) * 7919)),
		})
	}
	return c
}

// reachable reports whether a message between two nodes crosses a partition.
func (c *cluster) reachable(from, to NodeID) bool {
	return c.isolated[from] == c.isolated[to]
}

// deliver moves every pending message to its recipient, repeating until the
// cluster falls silent so a single call settles all cascading responses.
func (c *cluster) deliver() {
	for range 100 {
		var pending []Message
		for _, n := range c.nodes {
			pending = append(pending, n.Messages()...)
		}
		if len(pending) == 0 {
			return
		}
		for _, m := range pending {
			if c.reachable(m.From, m.To) {
				c.nodes[m.To].Step(m)
			}
		}
	}
	c.t.Fatal("cluster never fell silent: messages are looping")
}

// tick advances every node's clock by one and delivers what results.
func (c *cluster) tick() {
	for _, n := range c.nodes {
		n.Tick()
	}
	c.deliver()
}

// run advances the cluster by n ticks.
func (c *cluster) run(ticks int) {
	for range ticks {
		c.tick()
	}
}

// leaders returns every node currently believing itself leader, ignoring any
// listed as excluded.
func (c *cluster) leaders(exclude ...NodeID) []*Node {
	skip := make(map[NodeID]bool, len(exclude))
	for _, id := range exclude {
		skip[id] = true
	}

	var found []*Node
	for id, n := range c.nodes {
		if !skip[id] && n.State() == Leader {
			found = append(found, n)
		}
	}
	return found
}

// awaitLeader runs the cluster until exactly one node leads, and returns it.
func (c *cluster) awaitLeader(maxTicks int, exclude ...NodeID) *Node {
	c.t.Helper()

	for range maxTicks {
		c.tick()
		if found := c.leaders(exclude...); len(found) == 1 {
			return found[0]
		}
	}
	c.t.Fatalf("no single leader after %d ticks: %s", maxTicks, c.describe())
	return nil
}

// describe summarises the cluster for test failure messages.
func (c *cluster) describe() string {
	var out strings.Builder
	for id := 1; id <= len(c.nodes); id++ {
		n, ok := c.nodes[NodeID(id)]
		if !ok {
			continue
		}
		fmt.Fprintf(&out, "%v=%v term=%d", n.ID(), n.State(), n.Term())
		if c.isolated[n.ID()] {
			out.WriteString("(isolated)")
		}
		out.WriteString(" ")
	}
	return out.String()
}

// propose submits a command to the current leader and settles the cluster.
func (c *cluster) propose(leader *Node, command string) Index {
	c.t.Helper()

	index, err := leader.Propose([]byte(command))
	if err != nil {
		c.t.Fatalf("Propose(%q): %v", command, err)
	}
	c.deliver()
	// Followers learn the new commit index from the leader's next message, so
	// run far enough for a heartbeat to carry it.
	c.run(c.nodes[leader.ID()].cfg.HeartbeatTick + 1)
	return index
}

// commands returns a node's log contents as strings, for comparison.
func (c *cluster) commands(id NodeID) []string {
	n := c.nodes[id]

	var out []string
	for i := Index(1); i <= n.log.LastIndex(); i++ {
		entry, ok := n.log.EntryAt(i)
		if !ok {
			break
		}
		// Skip the empty entry a leader commits on taking office: it is
		// internal bookkeeping, not a client command.
		if len(entry.Command) == 0 {
			continue
		}
		out = append(out, string(entry.Command))
	}
	return out
}

func TestClusterElectsExactlyOneLeader(t *testing.T) {
	c := newCluster(t, 1, 2, 3)

	leader := c.awaitLeader(100)

	// Everyone else must be a follower on the same term, and must agree who
	// leads: two nodes believing themselves leader is the failure this whole
	// algorithm exists to prevent.
	for id, n := range c.nodes {
		if id == leader.ID() {
			continue
		}
		if got := n.State(); got != Follower {
			t.Errorf("%v: State() = %v, want Follower", id, got)
		}
		if got := n.Term(); got != leader.Term() {
			t.Errorf("%v: Term() = %d, want %d", id, got, leader.Term())
		}
	}
}

// A healthy leader keeps its followers quiet indefinitely. If heartbeats fail
// to suppress elections, the term climbs and this fails.
func TestStableClusterHoldsOneLeaderWithoutReelecting(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	leader := c.awaitLeader(100)
	term := leader.Term()

	c.run(500)

	if got := len(c.leaders()); got != 1 {
		t.Fatalf("leaders = %d after 500 quiet ticks, want 1: %s", got, c.describe())
	}
	if got := leader.Term(); got != term {
		t.Errorf("term drifted from %d to %d with a healthy leader; "+
			"heartbeats are not suppressing elections", term, got)
	}
	if got := leader.State(); got != Leader {
		t.Errorf("original leader is now %v, want Leader", got)
	}
}

// The headline scenario: lose the leader, keep the cluster.
func TestClusterElectsNewLeaderAfterLeaderFails(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	original := c.awaitLeader(100)

	c.isolated[original.ID()] = true

	// The two remaining nodes are a majority of three, so they can elect.
	replacement := c.awaitLeader(100, original.ID())

	if replacement.ID() == original.ID() {
		t.Fatal("replacement is the failed leader")
	}
	if replacement.Term() <= original.Term() {
		t.Errorf("new leader term %d, want greater than the old term %d",
			replacement.Term(), original.Term())
	}
}

// A node cut off from the cluster cannot elect itself, however long it waits:
// one vote out of three is not a majority. This is what prevents split-brain.
func TestIsolatedNodeCannotBecomeLeader(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	leader := c.awaitLeader(100)

	// Isolate a follower.
	var victim NodeID
	for id := range c.nodes {
		if id != leader.ID() {
			victim = id
			break
		}
	}
	c.isolated[victim] = true

	c.run(300)

	if got := c.nodes[victim].State(); got == Leader {
		t.Errorf("%v became leader while isolated from the majority", victim)
	}
	// Meanwhile the majority side carries on undisturbed.
	if got := len(c.leaders(victim)); got != 1 {
		t.Errorf("majority side has %d leaders, want 1: %s", got, c.describe())
	}
}

// The full partition-and-heal cycle. The isolated old leader has been raising
// its term the whole time it was away, so on rejoining it disrupts the cluster
// — but it cannot win, and the cluster must settle on exactly one leader.
func TestClusterRecoversAfterPartitionHeals(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	original := c.awaitLeader(100)

	c.isolated[original.ID()] = true
	c.awaitLeader(100, original.ID())

	c.isolated[original.ID()] = false
	c.run(300)

	found := c.leaders()
	if len(found) != 1 {
		t.Fatalf("leaders = %d after healing, want 1: %s", len(found), c.describe())
	}

	// And the whole cluster agrees on the term.
	leader := found[0]
	for id, n := range c.nodes {
		if n.Term() != leader.Term() {
			t.Errorf("%v: Term() = %d, want %d", id, n.Term(), leader.Term())
		}
	}
}

// Five nodes tolerate two failures; a third leaves the survivors short of a
// majority and the cluster correctly stops electing anyone.
func TestFiveNodeClusterLosesQuorum(t *testing.T) {
	c := newCluster(t, 1, 2, 3, 4, 5)
	c.awaitLeader(100)

	// Isolate three of five: neither side holds a majority.
	c.isolated[1], c.isolated[2], c.isolated[3] = true, true, true

	c.run(300)

	// The two-node side cannot reach three votes.
	for _, id := range []NodeID{4, 5} {
		if got := c.nodes[id].State(); got == Leader {
			t.Errorf("%v leads with only 2 of 5 nodes reachable", id)
		}
	}
	// The three-node side is itself a majority, so it may legitimately elect.
	if got := len(c.leaders(4, 5)); got > 1 {
		t.Errorf("majority side has %d leaders, want at most 1", got)
	}
}

// End to end: a write reaches every node, in order, and is committed.
func TestClusterReplicatesProposalsToEveryNode(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	leader := c.awaitLeader(100)

	for _, cmd := range []string{"set x 1", "set y 2", "del x"} {
		c.propose(leader, cmd)
	}

	want := []string{"set x 1", "set y 2", "del x"}
	for id := range c.nodes {
		got := c.commands(id)
		if len(got) != len(want) {
			t.Fatalf("%v: log has %d entries %q, want %d %q", id, len(got), got, len(want), want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%v: entry %d = %q, want %q", id, i+1, got[i], want[i])
			}
		}
	}

	// And every node agrees on how far it is safe to apply. The commit index
	// counts the leader's no-op as well as the three commands.
	wantCommit := Index(len(want) + 1)
	for id, n := range c.nodes {
		if got := n.CommitIndex(); got != wantCommit {
			t.Errorf("%v: CommitIndex() = %d, want %d", id, got, wantCommit)
		}
	}
}

// Committed entries reach the application exactly once, in log order, and
// identically on every node — the property the whole system exists to provide.
func TestClusterDeliversIdenticalCommittedEntries(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	leader := c.awaitLeader(100)

	for _, cmd := range []string{"a", "b", "c", "d"} {
		c.propose(leader, cmd)
	}

	applied := map[NodeID][]string{}
	for id, n := range c.nodes {
		for _, entry := range n.CommittedEntries() {
			if len(entry.Command) == 0 {
				continue // leader's no-op
			}
			applied[id] = append(applied[id], string(entry.Command))
		}
	}

	want := []string{"a", "b", "c", "d"}
	for id, got := range applied {
		if len(got) != len(want) {
			t.Fatalf("%v: applied %q, want %q", id, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%v: applied[%d] = %q, want %q", id, i, got[i], want[i])
			}
		}
	}
}

// A follower that misses writes while partitioned is brought back into line by
// the leader once it returns. No operator action, no special recovery path.
func TestPartitionedFollowerCatchesUp(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	leader := c.awaitLeader(100)

	var victim NodeID
	for id := range c.nodes {
		if id != leader.ID() {
			victim = id
			break
		}
	}
	c.isolated[victim] = true

	// The remaining two are still a majority, so writes continue committing.
	for _, cmd := range []string{"a", "b", "c"} {
		c.propose(leader, cmd)
	}
	if got := len(c.commands(victim)); got != 0 {
		t.Fatalf("isolated node has %d entries, want 0", got)
	}

	c.isolated[victim] = false
	c.run(50)

	got := c.commands(victim)
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("after healing, log = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("entry %d = %q, want %q", i+1, got[i], want[i])
		}
	}
}

// Acknowledged writes survive losing the leader: the election restriction
// guarantees the replacement already holds every committed entry.
func TestCommittedEntriesSurviveLeaderFailure(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	original := c.awaitLeader(100)

	for _, cmd := range []string{"a", "b"} {
		c.propose(original, cmd)
	}

	c.isolated[original.ID()] = true
	replacement := c.awaitLeader(100, original.ID())

	got := c.commands(replacement.ID())
	if len(got) < 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("new leader's log = %q, want it to retain committed entries a, b", got)
	}

	// And the cluster keeps accepting writes.
	c.propose(replacement, "c")
	if got := c.commands(replacement.ID()); got[len(got)-1] != "c" {
		t.Errorf("log = %q, want it to end with the new write", got)
	}
}

// A leader whose log diverged while partitioned has its extra entries
// discarded on rejoining. Those entries were never committed, so nothing that
// was ever acknowledged is lost.
func TestDivergentEntriesAreOverwrittenAfterPartition(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	original := c.awaitLeader(100)
	c.propose(original, "shared")

	// Isolate the leader and let it accept writes nobody else sees. Alone, it
	// cannot reach a majority, so these can never commit.
	c.isolated[original.ID()] = true
	for _, cmd := range []string{"orphan1", "orphan2"} {
		if _, err := original.Propose([]byte(cmd)); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	c.deliver()

	// The majority elects a new leader and makes progress without it.
	replacement := c.awaitLeader(100, original.ID())
	c.propose(replacement, "real")

	c.isolated[original.ID()] = false
	c.run(100)

	got := c.commands(original.ID())
	for _, cmd := range got {
		if cmd == "orphan1" || cmd == "orphan2" {
			t.Fatalf("uncommitted entry %q survived; log = %q", cmd, got)
		}
	}
	if len(got) == 0 || got[len(got)-1] != "real" {
		t.Errorf("log = %q, want it to end with the committed write %q", got, "real")
	}
}
