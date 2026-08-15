package raft

import "fmt"

// MessageType identifies which RPC a Message carries.
type MessageType int

const (
	// MsgRequestVote is a candidate asking a peer for its vote.
	MsgRequestVote MessageType = iota
	// MsgRequestVoteResponse answers a vote request.
	MsgRequestVoteResponse
	// MsgAppendEntries is a leader replicating entries. An empty Entries slice
	// is a heartbeat: it carries no data and exists only to announce that the
	// leader is alive, which is what stops followers standing for election.
	MsgAppendEntries
	// MsgAppendEntriesResponse answers a replication attempt.
	MsgAppendEntriesResponse
)

// String renders the type for log output.
func (t MessageType) String() string {
	switch t {
	case MsgRequestVote:
		return "RequestVote"
	case MsgRequestVoteResponse:
		return "RequestVoteResponse"
	case MsgAppendEntries:
		return "AppendEntries"
	case MsgAppendEntriesResponse:
		return "AppendEntriesResponse"
	default:
		return fmt.Sprintf("Unknown(%d)", int(t))
	}
}

// Message is one RPC between nodes.
//
// Every RPC is carried by this single struct rather than a type per message.
// Which fields are meaningful depends on Type, which is less tidy than
// separate types but keeps the transport layer trivial: it serialises one
// struct and never needs to know what any of it means.
//
// Term appears on every message and is the most important field on all of
// them. A node receiving a higher term than its own immediately steps down to
// Follower, whatever it was doing — the rule that makes stale leaders harmless.
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	Term Term

	// LastLogIndex and LastLogTerm describe the end of a candidate's log, and
	// let a voter refuse a candidate less current than itself.
	// (MsgRequestVote)
	LastLogIndex Index
	LastLogTerm  Term

	// Granted reports whether a vote was given. (MsgRequestVoteResponse)
	Granted bool

	// PrevLogIndex and PrevLogTerm identify the entry immediately before
	// Entries. The follower accepts the entries only if its own log matches
	// there, which is what keeps logs from silently diverging.
	// (MsgAppendEntries)
	PrevLogIndex Index
	PrevLogTerm  Term

	// Entries are the entries to append; empty means heartbeat.
	// (MsgAppendEntries)
	Entries []LogEntry

	// LeaderCommit is the leader's commit index, telling followers how far it
	// is safe to apply. (MsgAppendEntries)
	LeaderCommit Index

	// Success reports whether the consistency check passed.
	// (MsgAppendEntriesResponse)
	Success bool

	// MatchIndex is the highest index the follower now holds, reported back so
	// the leader learns how far replication reached without having to infer it
	// from what it happened to send. (MsgAppendEntriesResponse)
	MatchIndex Index
}
