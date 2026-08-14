package raft

import "testing"

// appendAt stamps entries with the given term, mimicking a leader in that term.
func appendAt(l *Log, term Term, commands ...string) {
	l.SetTerm(term)
	for _, c := range commands {
		l.Append([]byte(c))
	}
}

func TestEmptyLog(t *testing.T) {
	l := NewLog()

	if got := l.LastIndex(); got != 0 {
		t.Errorf("LastIndex() = %d, want 0", got)
	}
	if got := l.LastTerm(); got != 0 {
		t.Errorf("LastTerm() = %d, want 0", got)
	}
}

func TestAppendAssignsSequentialIndexes(t *testing.T) {
	l := NewLog()
	l.SetTerm(1)

	for want := Index(1); want <= 3; want++ {
		if got := l.Append([]byte("cmd")); got != want {
			t.Errorf("Append returned index %d, want %d", got, want)
		}
	}
	if got := l.LastIndex(); got != 3 {
		t.Errorf("LastIndex() = %d, want 3", got)
	}
	if got := l.LastTerm(); got != 1 {
		t.Errorf("LastTerm() = %d, want 1", got)
	}
}

func TestAppendStampsCurrentTerm(t *testing.T) {
	l := NewLog()
	appendAt(l, 1, "a", "b")
	appendAt(l, 5, "c")

	want := []Term{0, 1, 1, 5} // index 0 is the sentinel
	for i, wantTerm := range want {
		got, ok := l.TermAt(Index(i))
		if !ok {
			t.Fatalf("TermAt(%d): not found", i)
		}
		if got != wantTerm {
			t.Errorf("TermAt(%d) = %d, want %d", i, got, wantTerm)
		}
	}
}

func TestTermAtSentinelAndOutOfRange(t *testing.T) {
	l := NewLog()

	// The sentinel at index 0 always exists: it is what makes the consistency
	// check work on an empty log without a special case.
	if got, ok := l.TermAt(0); !ok || got != 0 {
		t.Errorf("TermAt(0) = (%d, %v), want (0, true)", got, ok)
	}
	if _, ok := l.TermAt(1); ok {
		t.Error("TermAt(1) on empty log: ok = true, want false")
	}
	if _, ok := l.TermAt(99); ok {
		t.Error("TermAt(99): ok = true, want false")
	}
}

func TestEntryAt(t *testing.T) {
	l := NewLog()
	appendAt(l, 2, "hello")

	entry, ok := l.EntryAt(1)
	if !ok {
		t.Fatal("EntryAt(1): not found")
	}
	if entry.Index != 1 || entry.Term != 2 || string(entry.Command) != "hello" {
		t.Errorf("EntryAt(1) = %+v, want {Term:2 Index:1 Command:hello}", entry)
	}

	if _, ok := l.EntryAt(2); ok {
		t.Error("EntryAt(2): ok = true, want false")
	}
}

func TestMatches(t *testing.T) {
	l := NewLog()
	appendAt(l, 1, "a", "b")
	appendAt(l, 3, "c")

	tests := []struct {
		name  string
		index Index
		term  Term
		want  bool
	}{
		{"empty log position always matches", 0, 0, true},
		{"correct index and term", 1, 1, true},
		{"correct index, wrong term", 1, 2, false},
		{"later entry, correct term", 3, 3, true},
		{"index beyond log", 4, 3, false},
		{"index far beyond log", 99, 1, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := l.Matches(tt.index, tt.term); got != tt.want {
				t.Errorf("Matches(%d, %d) = %v, want %v", tt.index, tt.term, got, tt.want)
			}
		})
	}
}

func TestTruncateFrom(t *testing.T) {
	l := NewLog()
	appendAt(l, 1, "a", "b", "c", "d")

	l.TruncateFrom(3) // discard indexes 3 and 4

	if got := l.LastIndex(); got != 2 {
		t.Errorf("LastIndex() = %d, want 2", got)
	}
	if _, ok := l.EntryAt(3); ok {
		t.Error("EntryAt(3) after truncate: ok = true, want false")
	}
	if entry, ok := l.EntryAt(2); !ok || string(entry.Command) != "b" {
		t.Errorf("EntryAt(2) = (%+v, %v), want command %q", entry, ok, "b")
	}
}

func TestTruncateFromBeyondEndIsNoOp(t *testing.T) {
	l := NewLog()
	appendAt(l, 1, "a", "b")

	l.TruncateFrom(99)

	if got := l.LastIndex(); got != 2 {
		t.Errorf("LastIndex() = %d, want 2", got)
	}
}

// Truncating from index 1 empties the log but must leave the sentinel intact.
func TestTruncateFromOneEmptiesLogButKeepsSentinel(t *testing.T) {
	l := NewLog()
	appendAt(l, 1, "a", "b")

	l.TruncateFrom(1)

	if got := l.LastIndex(); got != 0 {
		t.Errorf("LastIndex() = %d, want 0", got)
	}
	if _, ok := l.TermAt(0); !ok {
		t.Error("sentinel destroyed by truncate")
	}
}

func TestAppendAfterTruncateContinuesFromNewEnd(t *testing.T) {
	l := NewLog()
	appendAt(l, 1, "a", "b", "c")
	l.TruncateFrom(2)

	l.SetTerm(4)
	if got := l.Append([]byte("new")); got != 2 {
		t.Errorf("Append returned index %d, want 2", got)
	}
	if got := l.LastTerm(); got != 4 {
		t.Errorf("LastTerm() = %d, want 4", got)
	}
}

func TestEntriesFrom(t *testing.T) {
	l := NewLog()
	appendAt(l, 1, "a", "b", "c")

	got := l.EntriesFrom(2)
	if len(got) != 2 {
		t.Fatalf("EntriesFrom(2) returned %d entries, want 2", len(got))
	}
	if string(got[0].Command) != "b" || string(got[1].Command) != "c" {
		t.Errorf("EntriesFrom(2) = %q, %q, want %q, %q",
			got[0].Command, got[1].Command, "b", "c")
	}

	if got := l.EntriesFrom(4); len(got) != 0 {
		t.Errorf("EntriesFrom(4) returned %d entries, want 0", len(got))
	}
	if got := l.EntriesFrom(1); len(got) != 3 {
		t.Errorf("EntriesFrom(1) returned %d entries, want 3", len(got))
	}
}

// EntriesFrom must not alias the log's own storage, or a later Append could
// mutate a slice a caller is still transmitting.
func TestEntriesFromReturnsIndependentSlice(t *testing.T) {
	l := NewLog()
	appendAt(l, 1, "a", "b")

	got := l.EntriesFrom(1)
	got[0].Command = []byte("tampered")

	if entry, _ := l.EntryAt(1); string(entry.Command) != "a" {
		t.Errorf("log entry mutated through returned slice: got %q, want %q",
			entry.Command, "a")
	}
}

// The election restriction, Raft paper section 5.4.1.
func TestIsUpToDate(t *testing.T) {
	l := NewLog()
	appendAt(l, 2, "a", "b") // last entry: index 2, term 2

	tests := []struct {
		name      string
		lastIndex Index
		lastTerm  Term
		want      bool
	}{
		{"identical log", 2, 2, true},
		{"higher term wins even with shorter log", 1, 3, true},
		{"lower term loses even with longer log", 5, 1, false},
		{"same term, longer log", 3, 2, true},
		{"same term, shorter log", 1, 2, false},
		{"empty candidate log", 0, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := l.IsUpToDate(tt.lastIndex, tt.lastTerm)
			if got != tt.want {
				t.Errorf("IsUpToDate(%d, %d) = %v, want %v",
					tt.lastIndex, tt.lastTerm, got, tt.want)
			}
		})
	}
}

func TestEmptyLogIsUpToDateWithEmptyCandidate(t *testing.T) {
	l := NewLog()
	if !l.IsUpToDate(0, 0) {
		t.Error("IsUpToDate(0, 0) on empty log = false, want true")
	}
}
