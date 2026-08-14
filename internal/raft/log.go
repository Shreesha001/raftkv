package raft

// Log is the replicated log: an append-only, 1-indexed sequence of entries.
//
// Raft replicates this log rather than the application's data. Every node that
// holds the same entries in the same order, and applies them in order, reaches
// the same state — so agreeing on the log is the same thing as agreeing on the
// data, and "do you have entry 7?" is a far easier question to answer reliably
// across an unreliable network than "is your data correct?".
//
// Entry 0 is a permanent sentinel with term 0. Raft's consistency check
// repeatedly asks for the term at the index *before* some position, and on an
// empty log that position is 0. The sentinel makes that answer well defined
// instead of a special case at every call site.
//
// Log is not safe for concurrent use. It is owned by a single goroutine — the
// one that owns all Raft state.
type Log struct {
	entries []LogEntry
	term    Term // term stamped on entries added by Append
}

// NewLog returns an empty log containing only the sentinel entry.
func NewLog() *Log {
	return &Log{
		entries: []LogEntry{{Term: 0, Index: 0}},
	}
}

// SetTerm sets the term stamped onto subsequently appended entries. A leader
// calls this when it takes office.
func (l *Log) SetTerm(term Term) {
	l.term = term
}

// LastIndex returns the index of the final entry, or 0 if the log is empty.
func (l *Log) LastIndex() Index {
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the term of the final entry, or 0 if the log is empty.
func (l *Log) LastTerm() Term {
	return l.entries[len(l.entries)-1].Term
}

// Append adds commands to the end of the log, stamped with the current term,
// and returns the index of the last one added.
//
// This is the leader's path: only a leader creates entries. Followers receive
// entries with indexes and terms already assigned.
func (l *Log) Append(commands ...[]byte) Index {
	for _, cmd := range commands {
		l.entries = append(l.entries, LogEntry{
			Term:    l.term,
			Index:   l.LastIndex() + 1,
			Command: cmd,
		})
	}
	return l.LastIndex()
}

// TermAt returns the term of the entry at index, and whether that index exists.
func (l *Log) TermAt(index Index) (Term, bool) {
	entry, ok := l.EntryAt(index)
	if !ok {
		return 0, false
	}
	return entry.Term, true
}

// EntryAt returns the entry at index, and whether it exists.
func (l *Log) EntryAt(index Index) (LogEntry, bool) {
	if index > l.LastIndex() {
		return LogEntry{}, false
	}
	// Entries are contiguous from 0, so the index is also the slice position.
	return l.entries[index], true
}

// Matches reports whether this log holds an entry at index with the given term.
//
// This is Raft's log matching property. If two logs contain an entry with the
// same index and term, then every preceding entry in both logs is identical.
// That guarantee is what lets a leader repair a divergent follower by stepping
// backwards one index at a time until Matches finally returns true: a single
// agreed point implies agreement on everything before it.
func (l *Log) Matches(index Index, term Term) bool {
	entryTerm, ok := l.TermAt(index)
	return ok && entryTerm == term
}

// TruncateFrom discards the entry at index and everything after it.
//
// A follower calls this on discovering that its log conflicts with the
// leader's. Conflicting entries were never committed — a committed entry is by
// definition present on a majority, and the election restriction guarantees the
// leader holds every such entry — so discarding them loses nothing.
func (l *Log) TruncateFrom(index Index) {
	if index == 0 {
		// Never remove the sentinel.
		index = 1
	}
	if index > l.LastIndex() {
		return
	}
	l.entries = l.entries[:index]
}

// EntriesFrom returns a copy of all entries from index onward, or nil if index
// is past the end.
//
// The result is a fresh slice: the caller typically hands it to the transport,
// and a later Append must not mutate data already in flight.
func (l *Log) EntriesFrom(index Index) []LogEntry {
	if index == 0 {
		index = 1
	}
	if index > l.LastIndex() {
		return nil
	}
	out := make([]LogEntry, l.LastIndex()-index+1)
	copy(out, l.entries[index:])
	return out
}

// IsUpToDate reports whether a candidate whose last entry is (lastIndex,
// lastTerm) has a log at least as current as this one.
//
// This is the election restriction from section 5.4.1 of the Raft paper. A
// voter refuses any candidate that is behind it, which guarantees every new
// leader already holds every committed entry — so committed data can never be
// lost to an election.
//
// "At least as current" compares term first, then length: a higher last term
// wins outright, and only when the terms are equal does the longer log win. A
// long log full of uncommitted entries from an old term must not beat a short
// log that has seen a more recent leader.
func (l *Log) IsUpToDate(lastIndex Index, lastTerm Term) bool {
	ourTerm := l.LastTerm()
	if lastTerm != ourTerm {
		return lastTerm > ourTerm
	}
	return lastIndex >= l.LastIndex()
}
