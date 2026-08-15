package storage

import (
	"path/filepath"
	"testing"

	"github.com/Shreesha001/raftkv/internal/raft"
)

// stores returns each implementation under test, so both are held to the same
// contract.
func stores(t *testing.T) map[string]raft.Storage {
	t.Helper()

	file, err := NewFile(filepath.Join(t.TempDir(), "raft", "state.json"))
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	return map[string]raft.Storage{
		"memory": NewMemory(),
		"file":   file,
	}
}

func TestLoadOnEmptyStoreReportsNotFound(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			_, found, err := s.Load()
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if found {
				t.Error("found = true on an empty store, want false")
			}
		})
	}
}

func TestSaveThenLoadRoundTrips(t *testing.T) {
	want := raft.PersistentState{
		Term:     7,
		VotedFor: 2,
		Entries: []raft.LogEntry{
			{Term: 0, Index: 0},
			{Term: 1, Index: 1, Command: []byte("a")},
			{Term: 4, Index: 2, Command: []byte("b")},
		},
	}

	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			if err := s.Save(want); err != nil {
				t.Fatalf("Save: %v", err)
			}

			got, found, err := s.Load()
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if !found {
				t.Fatal("found = false after Save, want true")
			}
			if got.Term != want.Term {
				t.Errorf("Term = %d, want %d", got.Term, want.Term)
			}
			if got.VotedFor != want.VotedFor {
				t.Errorf("VotedFor = %v, want %v", got.VotedFor, want.VotedFor)
			}
			if len(got.Entries) != len(want.Entries) {
				t.Fatalf("got %d entries, want %d", len(got.Entries), len(want.Entries))
			}
			for i := range want.Entries {
				if got.Entries[i].Term != want.Entries[i].Term ||
					got.Entries[i].Index != want.Entries[i].Index ||
					string(got.Entries[i].Command) != string(want.Entries[i].Command) {
					t.Errorf("entry %d = %+v, want %+v", i, got.Entries[i], want.Entries[i])
				}
			}
		})
	}
}

func TestSaveOverwritesPreviousState(t *testing.T) {
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			if err := s.Save(raft.PersistentState{Term: 1, VotedFor: 1}); err != nil {
				t.Fatalf("Save: %v", err)
			}
			if err := s.Save(raft.PersistentState{Term: 2, VotedFor: raft.None}); err != nil {
				t.Fatalf("Save: %v", err)
			}

			got, _, err := s.Load()
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if got.Term != 2 {
				t.Errorf("Term = %d, want 2", got.Term)
			}
			if got.VotedFor != raft.None {
				t.Errorf("VotedFor = %v, want None", got.VotedFor)
			}
		})
	}
}

// State written by one process must be readable by the next: this is the whole
// point of persisting it.
func TestFileStateSurvivesReopening(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")

	first, err := NewFile(path)
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	err = first.Save(raft.PersistentState{
		Term:     3,
		VotedFor: 2,
		Entries:  []raft.LogEntry{{Term: 0, Index: 0}, {Term: 3, Index: 1, Command: []byte("x")}},
	})
	if err != nil {
		t.Fatalf("Save: %v", err)
	}

	second, err := NewFile(path)
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	got, found, err := second.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !found {
		t.Fatal("found = false in a reopened store, want true")
	}
	if got.Term != 3 || got.VotedFor != 2 || len(got.Entries) != 2 {
		t.Errorf("state = %+v, want term 3, vote node-2, 2 entries", got)
	}
}

// A Save must leave no partial files behind, whatever happens mid-write.
func TestFileSaveLeavesNoTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFile(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}

	for range 5 {
		if err := s.Save(raft.PersistentState{Term: 1}); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.tmp-*"))
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("temporary files left behind: %v", matches)
	}
}

func TestNewFileRejectsEmptyPath(t *testing.T) {
	if _, err := NewFile(""); err == nil {
		t.Error("NewFile(\"\"): got nil error, want error")
	}
}
