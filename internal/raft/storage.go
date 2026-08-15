package raft

// PersistentState is everything that must survive a crash.
//
// The list is short but not negotiable. Losing the term or the vote lets a
// node vote twice in one term, which lets two leaders exist, which loses data.
// Losing entries breaks the promise made to any client whose write was
// acknowledged. Raft's safety argument assumes these three survive; a node
// that cannot guarantee it must not participate.
type PersistentState struct {
	Term     Term       `json:"term"`
	VotedFor NodeID     `json:"votedFor"`
	Entries  []LogEntry `json:"entries"`
}

// Storage persists a node's state across restarts.
//
// The interface is declared here, where it is consumed, rather than alongside
// its implementations. That keeps this package free of dependencies on any
// particular storage mechanism — an in-memory implementation for tests and a
// file-backed one for production satisfy the same three methods.
type Storage interface {
	// Save records the state durably. It must not return before the data
	// would survive a power loss, because Raft's guarantees are written in
	// terms of what a restarted node remembers.
	Save(PersistentState) error

	// Load returns the stored state. The boolean reports whether any state
	// existed; a fresh node finds none, which is not an error.
	Load() (PersistentState, bool, error)
}
