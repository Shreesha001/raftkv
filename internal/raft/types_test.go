package raft

import "testing"

func TestStateString(t *testing.T) {
	tests := []struct {
		state State
		want  string
	}{
		{Follower, "Follower"},
		{Candidate, "Candidate"},
		{Leader, "Leader"},
		{State(99), "Unknown(99)"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("State(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}

func TestNodeIDString(t *testing.T) {
	if got, want := NodeID(3).String(), "node-3"; got != want {
		t.Errorf("NodeID(3).String() = %q, want %q", got, want)
	}
}

// Follower must be the zero value: a node that has just started, or been
// reconstructed from a zero struct, is a follower by default.
func TestFollowerIsZeroValue(t *testing.T) {
	var s State
	if s != Follower {
		t.Errorf("zero State = %v, want Follower", s)
	}
}
