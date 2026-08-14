package raft

import "testing"

// voteRequest builds a candidate's vote request.
func voteRequest(from NodeID, term Term, lastIndex Index, lastTerm Term) Message {
	return Message{
		Type:         MsgRequestVote,
		From:         from,
		To:           1,
		Term:         term,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
}

// onlyMessage returns the node's single outbound message, failing otherwise.
func onlyMessage(t *testing.T, n *Node) Message {
	t.Helper()
	msgs := n.Messages()
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1: %+v", len(msgs), msgs)
	}
	return msgs[0]
}

func TestFollowerGrantsVoteToUpToDateCandidate(t *testing.T) {
	n := newTestNode(1, 2, 3)

	n.Step(voteRequest(2, 1, 0, 0))

	reply := onlyMessage(t, n)
	if reply.Type != MsgRequestVoteResponse {
		t.Fatalf("Type = %v, want MsgRequestVoteResponse", reply.Type)
	}
	if !reply.Granted {
		t.Error("Granted = false, want true")
	}
	if reply.To != 2 {
		t.Errorf("To = %v, want node-2", reply.To)
	}
	if reply.Term != 1 {
		t.Errorf("Term = %d, want 1", reply.Term)
	}
	if got := n.VotedFor(); got != 2 {
		t.Errorf("VotedFor() = %v, want node-2", got)
	}
	if got := n.Term(); got != 1 {
		t.Errorf("Term() = %d, want 1", got)
	}
}

// A request from an old term is from a node that has not noticed the cluster
// moved on. The reply carries our term so it learns and steps down.
func TestFollowerRejectsVoteFromStaleTerm(t *testing.T) {
	n := newTestNode(1, 2, 3)
	n.Step(voteRequest(2, 5, 0, 0)) // advances us to term 5
	n.Messages()

	n.Step(voteRequest(3, 3, 0, 0)) // stale

	reply := onlyMessage(t, n)
	if reply.Granted {
		t.Error("Granted = true for a stale term, want false")
	}
	if reply.Term != 5 {
		t.Errorf("Term = %d, want 5 so the sender learns the current term", reply.Term)
	}
}

// One vote per term. Without this a node could help two different candidates
// reach a majority in the same term, producing two leaders.
func TestFollowerVotesOnlyOncePerTerm(t *testing.T) {
	n := newTestNode(1, 2, 3)
	n.Step(voteRequest(2, 1, 0, 0))
	n.Messages()

	n.Step(voteRequest(3, 1, 0, 0))

	reply := onlyMessage(t, n)
	if reply.Granted {
		t.Error("Granted = true for a second candidate in the same term, want false")
	}
	if got := n.VotedFor(); got != 2 {
		t.Errorf("VotedFor() = %v, want node-2 unchanged", got)
	}
}

// Repeating a vote to the same candidate must be safe: the first reply may
// have been lost, and the candidate will retry.
func TestFollowerRepeatsVoteToSameCandidate(t *testing.T) {
	n := newTestNode(1, 2, 3)
	n.Step(voteRequest(2, 1, 0, 0))
	n.Messages()

	n.Step(voteRequest(2, 1, 0, 0))

	if reply := onlyMessage(t, n); !reply.Granted {
		t.Error("Granted = false on a repeated request from the same candidate, want true")
	}
}

// The election restriction: a candidate behind us must not win, or committed
// entries could be erased.
func TestFollowerRejectsCandidateWithStaleLog(t *testing.T) {
	n := newTestNode(1, 2, 3)
	n.log.SetTerm(2)
	n.log.Append([]byte("a"), []byte("b")) // our log ends at index 2, term 2

	n.Step(voteRequest(2, 3, 1, 1)) // candidate ends at index 1, term 1

	reply := onlyMessage(t, n)
	if reply.Granted {
		t.Error("Granted = true for a candidate with a stale log, want false")
	}
	// We still adopt the higher term even while refusing the vote.
	if got := n.Term(); got != 3 {
		t.Errorf("Term() = %d, want 3", got)
	}
}

func TestFollowerGrantsVoteToCandidateWithLongerLog(t *testing.T) {
	n := newTestNode(1, 2, 3)
	n.log.SetTerm(1)
	n.log.Append([]byte("a"))

	n.Step(voteRequest(2, 2, 5, 3))

	if reply := onlyMessage(t, n); !reply.Granted {
		t.Error("Granted = false for a candidate ahead of us, want true")
	}
}

// Seeing a higher term retires a leader. This is what stops a partitioned
// leader competing when it rejoins.
func TestLeaderStepsDownOnHigherTerm(t *testing.T) {
	n := newTestNode(1, 2, 3)
	for range 100 {
		n.Tick()
		if n.State() == Candidate {
			break
		}
	}
	n.Step(Message{Type: MsgRequestVoteResponse, From: 2, To: 1, Term: n.Term(), Granted: true})
	if n.State() != Leader {
		t.Fatalf("setup: State() = %v, want Leader", n.State())
	}
	n.Messages()

	n.Step(Message{Type: MsgAppendEntries, From: 3, To: 1, Term: n.Term() + 5})

	if got := n.State(); got != Follower {
		t.Errorf("State() = %v, want Follower", got)
	}
	if got := n.VotedFor(); got != None {
		t.Errorf("VotedFor() = %v, want None in a fresh term", got)
	}
}

func TestCandidateBecomesLeaderOnMajority(t *testing.T) {
	n := newTestNode(1, 2, 3)
	tickUntilCandidate(t, n)
	n.Messages()

	// Two of three nodes is a majority: this node plus one peer.
	n.Step(Message{Type: MsgRequestVoteResponse, From: 2, To: 1, Term: n.Term(), Granted: true})

	if got := n.State(); got != Leader {
		t.Fatalf("State() = %v, want Leader", got)
	}

	// A new leader announces itself immediately rather than waiting a
	// heartbeat interval, or followers keep counting down to their own
	// elections.
	msgs := n.Messages()
	if len(msgs) != 2 {
		t.Fatalf("sent %d messages on election, want 2 heartbeats", len(msgs))
	}
	for _, m := range msgs {
		if m.Type != MsgAppendEntries {
			t.Errorf("to %v: Type = %v, want MsgAppendEntries", m.To, m.Type)
		}
	}
}

func TestCandidateStaysCandidateWithoutMajority(t *testing.T) {
	n := newTestNode(1, 2, 3)
	tickUntilCandidate(t, n)
	n.Messages()

	n.Step(Message{Type: MsgRequestVoteResponse, From: 2, To: 1, Term: n.Term(), Granted: false})

	if got := n.State(); got != Candidate {
		t.Errorf("State() = %v, want Candidate", got)
	}
}

// A vote for an election two terms ago must not count towards this one.
func TestCandidateIgnoresStaleVoteResponse(t *testing.T) {
	n := newTestNode(1, 2, 3)
	tickUntilCandidate(t, n)
	n.Messages()

	n.Step(Message{Type: MsgRequestVoteResponse, From: 2, To: 1, Term: n.Term() - 1, Granted: true})

	if got := n.State(); got != Candidate {
		t.Errorf("State() = %v, want Candidate", got)
	}
}

// Duplicate grants from one peer must not be counted twice, or a node could
// reach a "majority" on a single vote.
func TestDuplicateVotesCountOnce(t *testing.T) {
	n := NewNode(Config{
		ID: 1, Peers: []NodeID{2, 3, 4, 5},
		ElectionTick: 10, HeartbeatTick: 1,
		Logger: newTestNode(1).logger,
	})
	for range 100 {
		n.Tick()
		if n.State() == Candidate {
			break
		}
	}
	n.Messages()

	// Five nodes need three votes. Self plus one peer, repeated, is only two.
	for range 5 {
		n.Step(Message{Type: MsgRequestVoteResponse, From: 2, To: 1, Term: n.Term(), Granted: true})
	}

	if got := n.State(); got != Candidate {
		t.Errorf("State() = %v after duplicate votes from one peer, want Candidate", got)
	}
}

// A candidate that hears from a legitimate leader in its own term concedes.
func TestCandidateStepsDownOnLeaderInSameTerm(t *testing.T) {
	n := newTestNode(1, 2, 3)
	tickUntilCandidate(t, n)
	n.Messages()

	n.Step(Message{Type: MsgAppendEntries, From: 2, To: 1, Term: n.Term()})

	if got := n.State(); got != Follower {
		t.Errorf("State() = %v, want Follower", got)
	}
}

// Hearing from a leader restarts the countdown; this is what heartbeats buy.
func TestHeartbeatPreventsElection(t *testing.T) {
	n := newTestNode(1, 2, 3)

	// Well past any election timeout, but interrupted by heartbeats throughout.
	for range 200 {
		for range 5 {
			n.Tick()
		}
		n.Step(Message{Type: MsgAppendEntries, From: 2, To: 1, Term: 1})
	}

	if got := n.State(); got != Follower {
		t.Errorf("State() = %v despite regular heartbeats, want Follower", got)
	}
	if got := n.Term(); got != 1 {
		t.Errorf("Term() = %d, want 1: no election should have occurred", got)
	}
}

// A leader ignores messages from an older term entirely.
func TestStaleAppendEntriesIsRejected(t *testing.T) {
	n := newTestNode(1, 2, 3)
	n.Step(voteRequest(2, 5, 0, 0))
	n.Messages()

	n.Step(Message{Type: MsgAppendEntries, From: 3, To: 1, Term: 2})

	reply := onlyMessage(t, n)
	if reply.Success {
		t.Error("Success = true for a stale leader, want false")
	}
	if reply.Term != 5 {
		t.Errorf("Term = %d, want 5 so the stale leader steps down", reply.Term)
	}
}
