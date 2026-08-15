package storage_test

import (
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"

	"github.com/Shreesha001/raftkv/internal/raft"
	"github.com/Shreesha001/raftkv/internal/storage"
)

// restart simulates a crash and reboot: the node object is discarded and a new
// one built over the same storage, exactly as a restarted process would.
func restart(t *testing.T, store raft.Storage) *raft.Node {
	t.Helper()
	return raft.NewNode(raft.Config{
		ID: 1, Peers: []raft.NodeID{2, 3},
		ElectionTick: 10, HeartbeatTick: 1,
		Storage: store,
		Logger:  logr.Discard(),
	})
}

// A node must not forget who it voted for. Forgetting allows a second vote in
// the same term, which allows two leaders.
func TestVoteSurvivesRestart(t *testing.T) {
	store, err := storage.NewFile(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}

	n := restart(t, store)
	n.Step(raft.Message{
		Type: raft.MsgRequestVote, From: 2, To: 1, Term: 4,
		LastLogIndex: 0, LastLogTerm: 0,
	})
	if n.VotedFor() != 2 {
		t.Fatalf("setup: VotedFor() = %v, want node-2", n.VotedFor())
	}

	revived := restart(t, store)

	if got := revived.Term(); got != 4 {
		t.Errorf("Term() = %d after restart, want 4", got)
	}
	if got := revived.VotedFor(); got != 2 {
		t.Errorf("VotedFor() = %v after restart, want node-2", got)
	}
}

// A restarted node must refuse to vote again in a term it already voted in.
func TestRestartedNodeWillNotVoteTwiceInOneTerm(t *testing.T) {
	store, err := storage.NewFile(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}

	n := restart(t, store)
	n.Step(raft.Message{Type: raft.MsgRequestVote, From: 2, To: 1, Term: 4})
	n.Messages()

	revived := restart(t, store)
	revived.Step(raft.Message{Type: raft.MsgRequestVote, From: 3, To: 1, Term: 4})

	msgs := revived.Messages()
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	if msgs[0].Granted {
		t.Error("Granted = true for a second candidate in a term already voted in, want false")
	}
}

// Log entries must survive too: a follower that acknowledged an entry may have
// been the copy that made it committed.
func TestLogEntriesSurviveRestart(t *testing.T) {
	store, err := storage.NewFile(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}

	n := restart(t, store)
	n.Step(raft.Message{
		Type: raft.MsgAppendEntries, From: 2, To: 1, Term: 2,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []raft.LogEntry{
			{Term: 2, Index: 1, Command: []byte("set x 1")},
			{Term: 2, Index: 2, Command: []byte("set y 2")},
		},
		LeaderCommit: 2,
	})
	n.Messages()

	revived := restart(t, store)

	// Replaying the same entries must be accepted as duplicates, proving they
	// were still there.
	revived.Step(raft.Message{
		Type: raft.MsgAppendEntries, From: 2, To: 1, Term: 2,
		PrevLogIndex: 2, PrevLogTerm: 2,
		LeaderCommit: 2,
	})
	msgs := revived.Messages()
	if len(msgs) != 1 || !msgs[0].Success {
		t.Fatalf("restarted node no longer holds entries up to index 2: %+v", msgs)
	}
	if got := revived.CommitIndex(); got != 2 {
		t.Errorf("CommitIndex() = %d, want 2", got)
	}

	entries := revived.CommittedEntries()
	if len(entries) != 2 {
		t.Fatalf("CommittedEntries() returned %d, want 2", len(entries))
	}
	if string(entries[0].Command) != "set x 1" || string(entries[1].Command) != "set y 2" {
		t.Errorf("recovered commands = %q, %q, want %q, %q",
			entries[0].Command, entries[1].Command, "set x 1", "set y 2")
	}
}

// A node with no stored state starts clean rather than failing.
func TestFreshNodeStartsEmpty(t *testing.T) {
	store, err := storage.NewFile(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}

	n := restart(t, store)

	if got := n.Term(); got != 0 {
		t.Errorf("Term() = %d, want 0", got)
	}
	if got := n.VotedFor(); got != raft.None {
		t.Errorf("VotedFor() = %v, want None", got)
	}
}
