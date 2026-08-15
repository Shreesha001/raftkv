package raft

import "errors"

// ErrNotLeader is returned by Propose on a node that is not the leader. The
// caller should redirect the client to whoever is.
var ErrNotLeader = errors.New("raft: not the leader")

// Propose submits a command for replication and returns the log index it was
// assigned.
//
// Returning does not mean the command is committed — only that the leader has
// recorded it and begun replicating. It becomes committed once a majority
// holds it, at which point it appears from CommittedEntries. A caller that
// must not report success early waits for that index to commit.
func (n *Node) Propose(command []byte) (Index, error) {
	if n.state != Leader {
		return 0, ErrNotLeader
	}

	index := n.log.Append(command)
	n.matchIndex[n.id] = index
	n.persist()

	n.logger.V(2).Info("proposal accepted", "index", index, "term", n.currentTerm)

	n.broadcastAppend()
	return index, nil
}

// broadcastAppend sends each follower whatever it is missing.
//
// A follower that is already up to date receives an empty message, which is
// the heartbeat: it carries no data and exists only so the follower knows the
// leader is alive and does not stand for election.
func (n *Node) broadcastAppend() {
	for _, peer := range n.peers {
		n.sendAppend(peer)
	}
}

// sendAppend sends one follower the entries from its nextIndex onward.
func (n *Node) sendAppend(peer NodeID) {
	next := max(n.nextIndex[peer], 1)

	// The entry immediately before what we are sending. The follower accepts
	// only if its own log agrees there, which is what stops the two logs
	// silently diverging.
	prevIndex := next - 1
	prevTerm, ok := n.log.TermAt(prevIndex)
	if !ok {
		// We no longer hold that entry, so we cannot prove continuity. Fall
		// back to the start of the log.
		prevIndex, prevTerm = 0, 0
	}

	entries := n.log.EntriesFrom(next)

	n.logger.V(3).Info("replicating", "to", int(peer), "prevIndex", prevIndex,
		"prevTerm", prevTerm, "entries", len(entries), "commit", n.commitIndex)

	n.send(Message{
		Type:         MsgAppendEntries,
		To:           peer,
		Term:         n.currentTerm,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	})
}

// handleAppendEntries processes a message from the leader.
//
// Reaching here means the term check in Step already passed, so the sender is
// a legitimate leader for this term.
func (n *Node) handleAppendEntries(m Message) {
	// A candidate that hears from a leader in its own term has lost, and
	// concedes rather than splitting the cluster further.
	if n.state == Candidate {
		n.logger.V(1).Info("conceding to leader", "leader", int(m.From), "term", m.Term)
		n.becomeFollower(m.Term, n.votedFor)
	}

	n.leader = m.From

	// Contact from a leader restarts the countdown. This is the entire purpose
	// of heartbeats: a leader that keeps talking keeps its followers from
	// standing for election.
	n.electionElapsed = 0

	// The consistency check. If our log does not match the leader's at
	// PrevLogIndex, accepting these entries would leave a hole or a divergence
	// we could never detect later, so we refuse and let the leader retry from
	// further back.
	if !n.log.Matches(m.PrevLogIndex, m.PrevLogTerm) {
		n.logger.V(2).Info("rejecting entries: log mismatch",
			"prevIndex", m.PrevLogIndex, "prevTerm", m.PrevLogTerm,
			"ourLastIndex", n.log.LastIndex())

		n.send(Message{
			Type:    MsgAppendEntriesResponse,
			To:      m.From,
			Term:    n.currentTerm,
			Success: false,
		})
		return
	}

	if len(m.Entries) > 0 {
		n.appendFromLeader(m.Entries)
		// Durable before acknowledged: the leader may commit on the strength
		// of this reply, so the entries must survive a crash here.
		n.persist()
	}

	// The leader tells us how far it has committed. We may be behind it, so
	// clamp to what we actually hold: claiming to have committed entries we do
	// not have would let us apply them out of thin air.
	if m.LeaderCommit > n.commitIndex {
		n.commitIndex = min(m.LeaderCommit, n.log.LastIndex())
		n.logger.V(2).Info("commit advanced", "commit", n.commitIndex)
	}

	n.send(Message{
		Type:       MsgAppendEntriesResponse,
		To:         m.From,
		Term:       n.currentTerm,
		Success:    true,
		MatchIndex: m.PrevLogIndex + Index(len(m.Entries)),
	})
}

// appendFromLeader stores entries the consistency check has already approved.
//
// Entries we already hold at the same term are skipped: messages get
// retransmitted, and a duplicate must not truncate entries received since.
// The first entry that disagrees marks where our log diverged, so everything
// from there is discarded in favour of the leader's version — safe, because
// entries the leader disagrees with were never committed.
func (n *Node) appendFromLeader(entries []LogEntry) {
	for i, entry := range entries {
		existing, ok := n.log.EntryAt(entry.Index)
		if ok && existing.Term == entry.Term {
			continue // already have it
		}
		if ok {
			n.logger.V(1).Info("truncating conflicting entries",
				"from", entry.Index, "ourTerm", existing.Term, "leaderTerm", entry.Term)
			n.log.TruncateFrom(entry.Index)
		}
		n.log.Restore(entries[i:])
		return
	}
}

// handleAppendEntriesResponse processes a follower's answer.
func (n *Node) handleAppendEntriesResponse(m Message) {
	if n.state != Leader {
		return
	}

	if !m.Success {
		// Our guess about this follower's log was too optimistic. Step back
		// and try again from one entry earlier; repeated rejections walk
		// backwards until we find the last point the two logs agree.
		if n.nextIndex[m.From] > 1 {
			n.nextIndex[m.From]--
		}
		n.logger.V(2).Info("follower rejected entries, retrying earlier",
			"follower", int(m.From), "nextIndex", n.nextIndex[m.From])

		n.sendAppend(m.From)
		return
	}

	// Only ever move a follower's progress forward. Responses can arrive out
	// of order, and an older one must not undo a newer one.
	if m.MatchIndex > n.matchIndex[m.From] {
		n.matchIndex[m.From] = m.MatchIndex
		n.nextIndex[m.From] = m.MatchIndex + 1
	}

	n.advanceCommit()
}

// advanceCommit raises the commit index to the highest entry now held by a
// majority.
//
// The extra condition — that the entry belongs to the leader's own term — is
// section 5.4.2 of the Raft paper, and it is subtle. An entry from an earlier
// term sitting on a majority is *not* safe to commit: a future leader could
// still overwrite it. Only once an entry from the current term commits does
// everything before it become permanent, carried along by the log matching
// property.
func (n *Node) advanceCommit() {
	for index := n.log.LastIndex(); index > n.commitIndex; index-- {
		term, ok := n.log.TermAt(index)
		if !ok || term != n.currentTerm {
			continue
		}

		copies := 1 // the leader itself holds it
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= index {
				copies++
			}
		}

		if copies >= n.quorum() {
			n.commitIndex = index
			n.logger.V(1).Info("entry committed",
				"index", index, "term", term, "copies", copies)
			return
		}
	}
}
