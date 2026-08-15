// Package storage provides durable and in-memory implementations of
// raft.Storage.
package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/Shreesha001/raftkv/internal/raft"
)

// Memory keeps state in memory only. Use it in tests; a node using it forgets
// everything on restart, which is unsafe in production.
type Memory struct {
	state raft.PersistentState
	found bool
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory { return &Memory{} }

// Save records the state.
func (m *Memory) Save(state raft.PersistentState) error {
	// Copy the entries so a later mutation by the caller cannot alter what is
	// notionally already written.
	stored := state
	stored.Entries = append([]raft.LogEntry(nil), state.Entries...)

	m.state = stored
	m.found = true
	return nil
}

// Load returns the stored state.
func (m *Memory) Load() (raft.PersistentState, bool, error) {
	return m.state, m.found, nil
}

// File stores state as JSON in a single file.
//
// Every Save rewrites the whole file rather than appending, which is more work
// than a real implementation would do — production systems append to a segment
// log and checkpoint periodically. It is chosen here because correctness is
// obvious by inspection: there is exactly one file, and it is either the old
// state or the new one, never a mixture.
type File struct {
	path string
}

// NewFile returns a store backed by the given path. The parent directory is
// created if missing.
func NewFile(path string) (*File, error) {
	if path == "" {
		return nil, errors.New("storage: path must not be empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("storage: create directory: %w", err)
	}
	return &File{path: path}, nil
}

// Save writes the state durably.
//
// The write goes to a temporary file that is flushed to disk and then renamed
// over the target. Rename is atomic within a directory, so a crash at any
// point leaves either the complete previous state or the complete new one —
// never a half-written file. Skipping the fsync would leave the data in the
// operating system's cache, where a power loss destroys it while the rename
// appears to have succeeded.
func (f *File) Save(state raft.PersistentState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("storage: encode state: %w", err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(f.path), filepath.Base(f.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("storage: create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op once the rename has succeeded

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("storage: write state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("storage: sync state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("storage: close state: %w", err)
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		return fmt.Errorf("storage: replace state file: %w", err)
	}

	// The rename itself is only durable once the directory entry is flushed.
	dir, err := os.Open(filepath.Dir(f.path))
	if err != nil {
		return fmt.Errorf("storage: open directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("storage: sync directory: %w", err)
	}

	return nil
}

// Load reads the stored state. A missing file means a fresh node, which is not
// an error.
func (f *File) Load() (raft.PersistentState, bool, error) {
	data, err := os.ReadFile(f.path)
	if errors.Is(err, fs.ErrNotExist) {
		return raft.PersistentState{}, false, nil
	}
	if err != nil {
		return raft.PersistentState{}, false, fmt.Errorf("storage: read state: %w", err)
	}

	var state raft.PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return raft.PersistentState{}, false, fmt.Errorf("storage: decode state: %w", err)
	}
	return state, true, nil
}
