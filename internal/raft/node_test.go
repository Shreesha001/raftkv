package raft

import (
	"math/rand"
	"testing"

	"github.com/go-logr/logr"
)

// newTestNode builds a node with a fixed random source so elections are
// reproducible run to run.
func newTestNode(id NodeID, peers ...NodeID) *Node {
	return NewNode(Config{
		ID:            id,
		Peers:         peers,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Logger:        logr.Discard(),
		Rand:          rand.New(rand.NewSource(1)),
	})
}

// tickUntilCandidate advances the clock until the node stands for election, or
// fails after a bound generous enough to cover the randomised timeout.
func tickUntilCandidate(t *testing.T, n *Node) int {
	t.Helper()
	for i := 1; i <= 100; i++ {
		n.Tick()
		if n.State() == Candidate {
			return i
		}
	}
	t.Fatalf("node did not become candidate within 100 ticks")
	return 0
}

func TestNodeStartsAsFollower(t *testing.T) {
	n := newTestNode(1, 2, 3)

	if got := n.State(); got != Follower {
		t.Errorf("State() = %v, want Follower", got)
	}
	if got := n.Term(); got != 0 {
		t.Errorf("Term() = %d, want 0", got)
	}
	if got := n.ID(); got != 1 {
		t.Errorf("ID() = %v, want node-1", got)
	}
}

func TestFollowerStaysFollowerBeforeTimeout(t *testing.T) {
	n := newTestNode(1, 2, 3)

	// ElectionTick is 10 and the randomised timeout is at least that, so a
	// node cannot legitimately stand for election before then.
	for range 9 {
		n.Tick()
	}

	if got := n.State(); got != Follower {
		t.Errorf("State() = %v after 9 ticks, want Follower", got)
	}
	if got := n.Term(); got != 0 {
		t.Errorf("Term() = %d, want 0", got)
	}
}

func TestFollowerBecomesCandidateAfterElectionTimeout(t *testing.T) {
	n := newTestNode(1, 2, 3)

	ticks := tickUntilCandidate(t, n)

	if ticks < 10 {
		t.Errorf("became candidate after %d ticks, want at least 10", ticks)
	}
	if got := n.State(); got != Candidate {
		t.Errorf("State() = %v, want Candidate", got)
	}
}

// Standing for election starts a new term, and a candidate always votes for
// itself: that vote is one of the majority it needs.
func TestCandidateIncrementsTermAndVotesForSelf(t *testing.T) {
	n := newTestNode(1, 2, 3)

	tickUntilCandidate(t, n)

	if got := n.Term(); got != 1 {
		t.Errorf("Term() = %d, want 1", got)
	}
	if got := n.VotedFor(); got != 1 {
		t.Errorf("VotedFor() = %v, want node-1", got)
	}
}

func TestCandidateRequestsVotesFromEveryPeer(t *testing.T) {
	n := newTestNode(1, 2, 3)

	tickUntilCandidate(t, n)
	msgs := n.Messages()

	if len(msgs) != 2 {
		t.Fatalf("sent %d messages, want 2 (one per peer)", len(msgs))
	}

	got := map[NodeID]Message{}
	for _, m := range msgs {
		got[m.To] = m
	}
	for _, peer := range []NodeID{2, 3} {
		m, ok := got[peer]
		if !ok {
			t.Fatalf("no RequestVote sent to %v", peer)
		}
		if m.Type != MsgRequestVote {
			t.Errorf("to %v: Type = %v, want MsgRequestVote", peer, m.Type)
		}
		if m.From != 1 {
			t.Errorf("to %v: From = %v, want node-1", peer, m.From)
		}
		if m.Term != 1 {
			t.Errorf("to %v: Term = %d, want 1", peer, m.Term)
		}
		if m.LastLogIndex != 0 || m.LastLogTerm != 0 {
			t.Errorf("to %v: last log = (%d, %d), want (0, 0) for an empty log",
				peer, m.LastLogIndex, m.LastLogTerm)
		}
	}
}

// Messages drains: the caller has taken them, so the node must not send them
// again on the next call.
func TestMessagesDrains(t *testing.T) {
	n := newTestNode(1, 2, 3)
	tickUntilCandidate(t, n)

	if got := len(n.Messages()); got != 2 {
		t.Fatalf("first Messages() returned %d, want 2", got)
	}
	if got := len(n.Messages()); got != 0 {
		t.Errorf("second Messages() returned %d, want 0", got)
	}
}

// A single-node cluster has no one to ask, and one vote is already a majority
// of one, so it becomes leader the moment it stands.
func TestSingleNodeClusterElectsItself(t *testing.T) {
	n := newTestNode(1)

	for range 100 {
		n.Tick()
		if n.State() == Leader {
			return
		}
	}
	t.Fatalf("single node never became leader, state = %v", n.State())
}

// Randomised timeouts are what stop every node standing for election at the
// same moment and splitting the vote forever.
func TestElectionTimeoutsAreRandomised(t *testing.T) {
	seen := map[int]bool{}
	for seed := range 20 {
		n := NewNode(Config{
			ID:            1,
			Peers:         []NodeID{2, 3},
			ElectionTick:  10,
			HeartbeatTick: 1,
			Logger:        logr.Discard(),
			Rand:          rand.New(rand.NewSource(int64(seed))),
		})
		seen[tickUntilCandidate(t, n)] = true
	}

	if len(seen) < 2 {
		t.Errorf("election timeout was identical across 20 seeds (%v); "+
			"without randomisation nodes split the vote repeatedly", seen)
	}
}

func TestElectionTimeoutStaysWithinRange(t *testing.T) {
	for seed := range 50 {
		n := NewNode(Config{
			ID:            1,
			Peers:         []NodeID{2, 3},
			ElectionTick:  10,
			HeartbeatTick: 1,
			Logger:        logr.Discard(),
			Rand:          rand.New(rand.NewSource(int64(seed))),
		})
		ticks := tickUntilCandidate(t, n)
		if ticks < 10 || ticks >= 20 {
			t.Errorf("seed %d: timeout %d ticks, want within [10, 20)", seed, ticks)
		}
	}
}
