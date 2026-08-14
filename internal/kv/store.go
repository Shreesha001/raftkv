package kv

import "sync"

// Store is the state machine Raft replicates: a key-value map built by
// applying committed log entries in index order.
//
// Two stores that apply the same commands in the same order, starting empty,
// always hold identical data. That property is the whole point of replicating
// a log rather than shipping values around: agreeing on an ordered list of
// instructions is a question with a checkable answer, where "is your data
// correct?" is not.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

// Apply executes one committed log entry against the store.
//
// It takes raw bytes rather than a Command because that is what the Raft layer
// hands over; Raft has no knowledge of what a command means.
//
// An error means the entry could not be decoded, which indicates log
// corruption or a version mismatch — never a normal condition.
func (s *Store) Apply(data []byte) error {
	cmd, err := DecodeCommand(data)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Op {
	case OpSet:
		s.data[cmd.Key] = cmd.Value
	case OpDelete:
		// Deleting an absent key is intentionally not an error: a node
		// replaying its log must never fail on account of ordering it does not
		// control.
		delete(s.data, cmd.Key)
	}
	return nil
}

// Get returns the value stored at key, and whether it was present.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	return value, ok
}

// Len returns the number of keys currently stored.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.data)
}
