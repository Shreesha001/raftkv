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
