// Package raft implements the Raft consensus algorithm.
//
// The package deliberately knows nothing about networks, disks, clocks, or the
// application whose state it replicates. Those arrive as interfaces, which is
// what allows cluster scenarios — partitions, split votes, crashed leaders —
// to be tested deterministically and without real time passing.
package raft

import "fmt"

// NodeID identifies one member of the cluster. IDs are fixed at startup.
type NodeID int

// String renders the ID for log output.
func (n NodeID) String() string {
	return fmt.Sprintf("node-%d", int(n))
}

// Term is Raft's logical clock. It increases monotonically, and every election
// begins a new term. A node seeing a term higher than its own always steps down
// to Follower — that single rule is what keeps stale leaders harmless.
type Term uint64

// Index is a position in the replicated log. The log is 1-indexed; index 0 is a
// sentinel meaning "before the beginning".
type Index uint64

// LogEntry is one instruction in the replicated log.
//
// Command is opaque: Raft copies it between nodes and hands it to the
// application on commit, but never interprets it.
type LogEntry struct {
	Term    Term
	Index   Index
	Command []byte
}

// State is a node's role. Every node is exactly one of these at any moment.
type State int

const (
	// Follower is passive: it responds to leaders and candidates. This is the
	// zero value, so a freshly constructed node starts as a follower.
	Follower State = iota
	// Candidate is standing for election, having timed out waiting for a leader.
	Candidate
	// Leader accepts client requests and replicates them to followers.
	Leader
)

// String renders the state for log output.
func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("Unknown(%d)", int(s))
	}
}
