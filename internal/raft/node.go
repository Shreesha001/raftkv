package raft

import (
	"fmt"
	"math/rand"

	"github.com/go-logr/logr"
)

// None is the NodeID meaning "nobody": no vote cast, no leader known.
const None NodeID = -1

// Config describes one node and its cluster.
type Config struct {
	// ID identifies this node. Must be unique in the cluster and non-negative.
	ID NodeID
	// Peers are the other members, excluding this node. A cluster of one is
	// legal and useful for testing.
	Peers []NodeID

	// ElectionTick is how many ticks a follower waits without hearing from a
	// leader before standing for election. The actual timeout is randomised in
	// [ElectionTick, 2*ElectionTick) so nodes do not all stand at once.
	ElectionTick int
	// HeartbeatTick is how many ticks a leader waits between heartbeats. It
	// must be well below ElectionTick, or followers time out on a healthy
	// leader and the cluster churns through pointless elections.
	HeartbeatTick int

	// Logger receives protocol events. Use logr.Discard() to silence a node.
	Logger logr.Logger
	// Rand supplies election timeout jitter. Nil means a source seeded from
	// the node ID, which keeps behaviour reproducible.
	Rand *rand.Rand
}

func (c Config) validate() error {
	if c.ID < 0 {
		return fmt.Errorf("raft: node ID must be non-negative, got %d", c.ID)
	}
	if c.ElectionTick <= 0 {
		return fmt.Errorf("raft: ElectionTick must be positive, got %d", c.ElectionTick)
	}
	if c.HeartbeatTick <= 0 {
		return fmt.Errorf("raft: HeartbeatTick must be positive, got %d", c.HeartbeatTick)
	}
	if c.HeartbeatTick >= c.ElectionTick {
		return fmt.Errorf("raft: HeartbeatTick (%d) must be below ElectionTick (%d), "+
			"or followers time out on a healthy leader",
			c.HeartbeatTick, c.ElectionTick)
	}
	for _, p := range c.Peers {
		if p == c.ID {
			return fmt.Errorf("raft: node %v listed among its own peers", p)
		}
	}
	return nil
}

// Node is one member of a Raft cluster.
//
// It is a pure state machine: it owns no goroutine, no timer, and no network
// connection. Time arrives through Tick, messages arrive through Step, and
// messages it wants sent are collected by Messages. A caller supplies the
// goroutine, the clock, and the transport.
//
// That design is what makes the protocol testable. A test can run a whole
// cluster in one goroutine, deliver messages in any order it likes, drop them,
// or advance one node's clock while another's stands still — and get the same
// result on every run. No sleeps, no flakes.
//
// Node is not safe for concurrent use; a single caller must own it.
type Node struct {
	id    NodeID
	peers []NodeID

	// Persistent state. Once storage exists these must survive a restart:
	// forgetting a vote allows a node to vote twice in one term, which allows
	// two leaders.
	currentTerm Term
	votedFor    NodeID
	log         *Log

	// Volatile state, rebuilt after a restart.
	state State
	// votes records who has answered this node's candidacy, and how.
	votes map[NodeID]bool

	// commitIndex is the highest entry known to be on a majority, and so safe
	// to apply. lastApplied is how far the application has actually consumed.
	commitIndex Index
	lastApplied Index

	// Leader-only bookkeeping, rebuilt at every election.
	//
	// nextIndex is the leader's guess at what to send each follower next. It
	// starts optimistic and is walked back when a follower rejects, which is
	// how a leader discovers where a divergent log first differs.
	//
	// matchIndex is what the leader knows a follower actually holds. Only
	// acknowledgements raise it, and it is what commitment is counted from.
	nextIndex  map[NodeID]Index
	matchIndex map[NodeID]Index

	electionElapsed  int
	heartbeatElapsed int
	// electionTimeout is re-randomised for every election.
	electionTimeout int

	cfg    Config
	rand   *rand.Rand
	outbox []Message
	logger logr.Logger
}

// NewNode returns a node in the Follower state. It panics on an invalid
// configuration: a misconfigured cluster cannot be made safe at runtime, and
// failing at construction is better than electing two leaders later.
func NewNode(cfg Config) *Node {
	if err := cfg.validate(); err != nil {
		panic(err)
	}

	rng := cfg.Rand
	if rng == nil {
		rng = rand.New(rand.NewSource(int64(cfg.ID)))
	}

	n := &Node{
		id:       cfg.ID,
		peers:    append([]NodeID(nil), cfg.Peers...),
		votedFor: None,
		log:      NewLog(),
		state:    Follower,
		votes:    make(map[NodeID]bool),
		cfg:      cfg,
		rand:     rng,
		logger:   cfg.Logger.WithValues("node", int(cfg.ID)),
	}
	n.resetElectionTimeout()

	n.logger.V(1).Info("node started", "peers", len(n.peers))
	return n
}

// ID returns this node's identifier.
func (n *Node) ID() NodeID { return n.id }

// State returns the node's current role.
func (n *Node) State() State { return n.state }

// Term returns the current term.
func (n *Node) Term() Term { return n.currentTerm }

// VotedFor returns who this node voted for in the current term, or None.
func (n *Node) VotedFor() NodeID { return n.votedFor }

// CommitIndex returns the highest log index known to be committed.
func (n *Node) CommitIndex() Index { return n.commitIndex }

// CommittedEntries returns entries that have become safe to apply since the
// last call, in log order, and marks them consumed.
//
// This is how committed commands reach the application. Raft hands over
// opaque bytes and never learns what they meant.
func (n *Node) CommittedEntries() []LogEntry {
	if n.lastApplied >= n.commitIndex {
		return nil
	}

	out := make([]LogEntry, 0, n.commitIndex-n.lastApplied)
	for i := n.lastApplied + 1; i <= n.commitIndex; i++ {
		entry, ok := n.log.EntryAt(i)
		if !ok {
			break
		}
		out = append(out, entry)
	}
	n.lastApplied = n.commitIndex

	return out
}

// Messages returns everything the node wants sent and clears its outbox. The
// caller is responsible for delivery, and Raft assumes nothing about whether
// it succeeds — dropped messages are recovered by retry and timeout.
func (n *Node) Messages() []Message {
	if len(n.outbox) == 0 {
		return nil
	}
	out := n.outbox
	n.outbox = nil
	return out
}

// Tick advances the node's logical clock by one unit.
//
// A follower or candidate that goes a full election timeout without hearing
// from a leader stands for election. This is the entire failure detector: a
// dead leader cannot announce its death, so silence is the signal.
func (n *Node) Tick() {
	switch n.state {
	case Follower, Candidate:
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			n.logger.V(2).Info("election timeout elapsed",
				"term", n.currentTerm, "state", n.state)
			n.startElection()
		}
	case Leader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTick {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
	}
}

// quorum is the number of votes a decision needs: a strict majority of the
// cluster, this node included.
//
// Majorities are what make split-brain impossible. Any two majorities of the
// same cluster share at least one member, and that member will not support two
// leaders in one term.
func (n *Node) quorum() int {
	return (len(n.peers)+1)/2 + 1
}

// resetElectionTimeout picks a fresh randomised timeout and restarts the clock.
//
// The randomisation is essential rather than cosmetic. With a fixed timeout,
// nodes that start together time out together, split the vote so nobody reaches
// a majority, then time out together again — potentially forever. Jitter breaks
// the symmetry so one node reliably gets there first.
func (n *Node) resetElectionTimeout() {
	n.electionElapsed = 0
	n.electionTimeout = n.cfg.ElectionTick + n.rand.Intn(n.cfg.ElectionTick)
}

// becomeFollower steps down to the given term.
//
// Every path that observes a term higher than our own ends here. It is the
// rule that retires stale leaders: a partitioned leader that rejoins sees a
// newer term and stands down rather than competing.
func (n *Node) becomeFollower(term Term, votedFor NodeID) {
	if term != n.currentTerm {
		n.logger.V(1).Info("stepping down", "from", n.state,
			"oldTerm", n.currentTerm, "newTerm", term)
	}
	n.state = Follower
	n.currentTerm = term
	n.votedFor = votedFor
	n.votes = make(map[NodeID]bool)
	n.resetElectionTimeout()
}

// startElection begins a new term with this node as a candidate.
func (n *Node) startElection() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.votes = map[NodeID]bool{n.id: true} // a candidate always votes for itself
	n.resetElectionTimeout()

	n.logger.V(1).Info("standing for election", "term", n.currentTerm)

	// A single-node cluster is its own majority, so there is nobody to ask.
	if n.tallyVotes() {
		return
	}

	for _, peer := range n.peers {
		n.send(Message{
			Type:         MsgRequestVote,
			To:           peer,
			Term:         n.currentTerm,
			LastLogIndex: n.log.LastIndex(),
			LastLogTerm:  n.log.LastTerm(),
		})
	}
}

// tallyVotes promotes the node to Leader if it has a majority, and reports
// whether it did.
func (n *Node) tallyVotes() bool {
	granted := 0
	for _, ok := range n.votes {
		if ok {
			granted++
		}
	}
	if granted < n.quorum() {
		return false
	}

	n.becomeLeader()
	return true
}

// becomeLeader takes office and immediately announces it.
func (n *Node) becomeLeader() {
	n.state = Leader
	n.log.SetTerm(n.currentTerm)
	n.heartbeatElapsed = 0

	// A new leader knows nothing about its followers' logs, so it assumes they
	// match its own and corrects downwards as rejections arrive. Guessing high
	// costs a few extra round trips; guessing low would risk overwriting
	// entries a follower legitimately holds.
	n.nextIndex = make(map[NodeID]Index, len(n.peers))
	n.matchIndex = make(map[NodeID]Index, len(n.peers))
	for _, peer := range n.peers {
		n.nextIndex[peer] = n.log.LastIndex() + 1
		n.matchIndex[peer] = 0
	}

	n.logger.V(1).Info("became leader", "term", n.currentTerm,
		"lastIndex", n.log.LastIndex())

	// Announce immediately rather than waiting a heartbeat interval: until
	// followers hear from the new leader they are still counting down to their
	// own elections.
	n.broadcastAppend()
}

// send queues a message for the caller to deliver.
func (n *Node) send(m Message) {
	m.From = n.id
	n.outbox = append(n.outbox, m)
	n.logger.V(3).Info("send", "type", m.Type, "to", int(m.To), "term", m.Term)
}
