package raft

import "testing"

// leaderOfOne returns a node that has won an election in a three-node cluster,
// with its outbox already drained.
func leaderOfOne(t *testing.T) *Node {
	t.Helper()

	n := newTestNode(1, 2, 3)
	tickUntilCandidate(t, n)
	n.Messages()
	n.Step(Message{Type: MsgRequestVoteResponse, From: 2, To: 1, Term: n.Term(), Granted: true})
	if n.State() != Leader {
		t.Fatalf("setup: State() = %v, want Leader", n.State())
	}
	n.Messages()
	return n
}

// messagesTo indexes a node's outbound messages by recipient.
func messagesTo(n *Node) map[NodeID]Message {
	out := map[NodeID]Message{}
	for _, m := range n.Messages() {
		out[m.To] = m
	}
	return out
}

func TestOnlyLeaderAcceptsProposals(t *testing.T) {
	n := newTestNode(1, 2, 3)

	if _, err := n.Propose([]byte("x")); err == nil {
		t.Error("Propose on a follower: got nil error, want error")
	}

	tickUntilCandidate(t, n)
	if _, err := n.Propose([]byte("x")); err == nil {
		t.Error("Propose on a candidate: got nil error, want error")
	}
}

func TestLeaderAppendsProposalToItsOwnLog(t *testing.T) {
	n := leaderOfOne(t)

	index, err := n.Propose([]byte("set x 1"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if index != 1 {
		t.Errorf("Propose returned index %d, want 1", index)
	}

	entry, ok := n.log.EntryAt(1)
	if !ok {
		t.Fatal("entry 1 missing from the leader's log")
	}
	if string(entry.Command) != "set x 1" {
		t.Errorf("entry command = %q, want %q", entry.Command, "set x 1")
	}
	if entry.Term != n.Term() {
		t.Errorf("entry term = %d, want %d", entry.Term, n.Term())
	}
}

func TestLeaderSendsProposalToEveryFollower(t *testing.T) {
	n := leaderOfOne(t)

	if _, err := n.Propose([]byte("a")); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	msgs := messagesTo(n)
	if len(msgs) != 2 {
		t.Fatalf("sent to %d peers, want 2", len(msgs))
	}
	for _, peer := range []NodeID{2, 3} {
		m := msgs[peer]
		if m.Type != MsgAppendEntries {
			t.Errorf("to %v: Type = %v, want MsgAppendEntries", peer, m.Type)
		}
		if len(m.Entries) != 1 || string(m.Entries[0].Command) != "a" {
			t.Errorf("to %v: Entries = %+v, want one entry %q", peer, m.Entries, "a")
		}
		// A new leader's log is empty, so the entry before this one is the
		// sentinel at index 0.
		if m.PrevLogIndex != 0 || m.PrevLogTerm != 0 {
			t.Errorf("to %v: prev = (%d, %d), want (0, 0)", peer, m.PrevLogIndex, m.PrevLogTerm)
		}
	}
}

func TestFollowerAppendsEntriesFromLeader(t *testing.T) {
	n := newTestNode(1, 2, 3)

	n.Step(Message{
		Type: MsgAppendEntries, From: 2, To: 1, Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Term: 1, Index: 1, Command: []byte("a")},
			{Term: 1, Index: 2, Command: []byte("b")},
		},
	})

	reply := onlyMessage(t, n)
	if !reply.Success {
		t.Fatal("Success = false, want true")
	}
	if got := n.log.LastIndex(); got != 2 {
		t.Errorf("LastIndex() = %d, want 2", got)
	}
	entry, _ := n.log.EntryAt(2)
	if string(entry.Command) != "b" {
		t.Errorf("entry 2 = %q, want %q", entry.Command, "b")
	}
}

// The consistency check: a follower whose log does not match at PrevLogIndex
// refuses the entries rather than creating a hole.
func TestFollowerRejectsEntriesWhenPreviousEntryMissing(t *testing.T) {
	n := newTestNode(1, 2, 3)

	n.Step(Message{
		Type: MsgAppendEntries, From: 2, To: 1, Term: 1,
		PrevLogIndex: 5, PrevLogTerm: 1, // we have nothing at index 5
		Entries: []LogEntry{{Term: 1, Index: 6, Command: []byte("a")}},
	})

	reply := onlyMessage(t, n)
	if reply.Success {
		t.Error("Success = true despite a gap, want false")
	}
	if got := n.log.LastIndex(); got != 0 {
		t.Errorf("LastIndex() = %d, want 0: nothing should have been appended", got)
	}
}

func TestFollowerRejectsEntriesWhenPreviousTermDiffers(t *testing.T) {
	n := newTestNode(1, 2, 3)
	n.log.SetTerm(1)
	n.log.Append([]byte("a"))

	n.Step(Message{
		Type: MsgAppendEntries, From: 2, To: 1, Term: 5,
		PrevLogIndex: 1, PrevLogTerm: 3, // ours is term 1, not 3
		Entries: []LogEntry{{Term: 5, Index: 2, Command: []byte("b")}},
	})

	if reply := onlyMessage(t, n); reply.Success {
		t.Error("Success = true despite a term mismatch, want false")
	}
}

// Where logs diverge, the leader's version wins and the follower discards its
// own. Those entries were never committed, so nothing is lost.
func TestFollowerTruncatesConflictingEntries(t *testing.T) {
	n := newTestNode(1, 2, 3)
	n.log.SetTerm(1)
	n.log.Append([]byte("a"), []byte("wrong"), []byte("alsowrong"))

	n.Step(Message{
		Type: MsgAppendEntries, From: 2, To: 1, Term: 2,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 2, Index: 2, Command: []byte("right")}},
	})

	if reply := onlyMessage(t, n); !reply.Success {
		t.Fatal("Success = false, want true")
	}
	if got := n.log.LastIndex(); got != 2 {
		t.Fatalf("LastIndex() = %d, want 2: index 3 should be gone", got)
	}
	entry, _ := n.log.EntryAt(2)
	if string(entry.Command) != "right" {
		t.Errorf("entry 2 = %q, want %q", entry.Command, "right")
	}
}

// A retransmitted message the follower already applied must not truncate
// entries it has since received. Duplicates are normal on an unreliable link.
func TestFollowerIgnoresDuplicateEntries(t *testing.T) {
	n := newTestNode(1, 2, 3)
	appendMsg := Message{
		Type: MsgAppendEntries, From: 2, To: 1, Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{{Term: 1, Index: 1, Command: []byte("a")}},
	}
	n.Step(appendMsg)
	n.Step(Message{
		Type: MsgAppendEntries, From: 2, To: 1, Term: 1,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 1, Index: 2, Command: []byte("b")}},
	})
	n.Messages()

	n.Step(appendMsg) // the first message arrives again

	if reply := onlyMessage(t, n); !reply.Success {
		t.Fatal("Success = false for a duplicate, want true")
	}
	if got := n.log.LastIndex(); got != 2 {
		t.Errorf("LastIndex() = %d, want 2: a duplicate must not truncate", got)
	}
}

// When a follower refuses, the leader's guess about that follower's log was
// too optimistic. It steps back and tries again from an earlier point.
func TestLeaderRetriesFromEarlierIndexAfterRejection(t *testing.T) {
	n := leaderOfOne(t)
	for _, cmd := range []string{"a", "b", "c"} {
		if _, err := n.Propose([]byte(cmd)); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	// A leader that believes this follower already holds everything, which is
	// what it assumes on taking office.
	n.nextIndex[2] = 4
	n.Messages()

	n.Step(Message{Type: MsgAppendEntriesResponse, From: 2, To: 1, Term: n.Term(), Success: false})

	msgs := messagesTo(n)
	m, ok := msgs[2]
	if !ok {
		t.Fatal("leader did not retry after a rejection")
	}
	if m.PrevLogIndex != 2 {
		t.Errorf("PrevLogIndex = %d, want 2: the leader should step back one entry", m.PrevLogIndex)
	}
	if len(m.Entries) != 1 {
		t.Errorf("sent %d entries, want 1 from index 3 onward", len(m.Entries))
	}
}

// An entry is committed once a majority holds it. Two of three suffices.
func TestLeaderCommitsOnMajorityAcknowledgement(t *testing.T) {
	n := leaderOfOne(t)
	index, err := n.Propose([]byte("a"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	n.Messages()

	if got := n.CommitIndex(); got != 0 {
		t.Fatalf("CommitIndex() = %d before any acknowledgement, want 0", got)
	}

	n.Step(Message{
		Type: MsgAppendEntriesResponse, From: 2, To: 1,
		Term: n.Term(), Success: true, MatchIndex: index,
	})

	if got := n.CommitIndex(); got != index {
		t.Errorf("CommitIndex() = %d after a majority acknowledged, want %d", got, index)
	}
}

func TestLeaderDoesNotCommitWithoutMajority(t *testing.T) {
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
	for _, peer := range []NodeID{2, 3} {
		n.Step(Message{Type: MsgRequestVoteResponse, From: peer, To: 1, Term: n.Term(), Granted: true})
	}
	if n.State() != Leader {
		t.Fatalf("setup: State() = %v, want Leader", n.State())
	}
	index, err := n.Propose([]byte("a"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	n.Messages()

	// Five nodes need three copies. Leader plus one peer is only two.
	n.Step(Message{
		Type: MsgAppendEntriesResponse, From: 2, To: 1,
		Term: n.Term(), Success: true, MatchIndex: index,
	})

	if got := n.CommitIndex(); got != 0 {
		t.Errorf("CommitIndex() = %d with only 2 of 5 copies, want 0", got)
	}
}

// Raft paper section 5.4.2. A leader may not commit an entry from an earlier
// term merely because it is now on a majority: such an entry can still be
// overwritten. It becomes committed only once an entry from the leader's own
// term is committed alongside it.
func TestLeaderDoesNotCommitEntriesFromEarlierTerms(t *testing.T) {
	n := leaderOfOne(t)
	// Plant an entry from an older term, as if inherited from a previous leader.
	n.log.SetTerm(n.Term() - 1)
	n.log.Append([]byte("old"))
	n.log.SetTerm(n.Term())
	n.Messages()

	n.Step(Message{
		Type: MsgAppendEntriesResponse, From: 2, To: 1,
		Term: n.Term(), Success: true, MatchIndex: 1,
	})

	if got := n.CommitIndex(); got != 0 {
		t.Errorf("CommitIndex() = %d for an entry from an earlier term, want 0", got)
	}

	// Committing an entry from the current term carries the older one with it.
	index, err := n.Propose([]byte("current"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	n.Messages()
	n.Step(Message{
		Type: MsgAppendEntriesResponse, From: 2, To: 1,
		Term: n.Term(), Success: true, MatchIndex: index,
	})

	if got := n.CommitIndex(); got != index {
		t.Errorf("CommitIndex() = %d, want %d once a current-term entry committed", got, index)
	}
}

func TestFollowerAdvancesCommitIndexFromLeader(t *testing.T) {
	n := newTestNode(1, 2, 3)

	n.Step(Message{
		Type: MsgAppendEntries, From: 2, To: 1, Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Term: 1, Index: 1, Command: []byte("a")},
			{Term: 1, Index: 2, Command: []byte("b")},
		},
		LeaderCommit: 2,
	})

	if got := n.CommitIndex(); got != 2 {
		t.Errorf("CommitIndex() = %d, want 2", got)
	}
}

// A follower must never claim to have committed further than its own log
// reaches, however far ahead the leader says it is.
func TestFollowerCommitIndexNeverExceedsItsLog(t *testing.T) {
	n := newTestNode(1, 2, 3)

	n.Step(Message{
		Type: MsgAppendEntries, From: 2, To: 1, Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      []LogEntry{{Term: 1, Index: 1, Command: []byte("a")}},
		LeaderCommit: 99,
	})

	if got := n.CommitIndex(); got != 1 {
		t.Errorf("CommitIndex() = %d, want 1: it cannot exceed the log", got)
	}
}

// CommittedEntries hands the application everything newly safe to apply, in
// order, exactly once.
func TestCommittedEntriesDrainsInOrder(t *testing.T) {
	n := newTestNode(1, 2, 3)

	n.Step(Message{
		Type: MsgAppendEntries, From: 2, To: 1, Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Term: 1, Index: 1, Command: []byte("a")},
			{Term: 1, Index: 2, Command: []byte("b")},
		},
		LeaderCommit: 2,
	})

	got := n.CommittedEntries()
	if len(got) != 2 {
		t.Fatalf("CommittedEntries() returned %d entries, want 2", len(got))
	}
	if string(got[0].Command) != "a" || string(got[1].Command) != "b" {
		t.Errorf("entries = %q, %q, want %q, %q", got[0].Command, got[1].Command, "a", "b")
	}
	if again := n.CommittedEntries(); len(again) != 0 {
		t.Errorf("second call returned %d entries, want 0: entries must be applied once", len(again))
	}
}
