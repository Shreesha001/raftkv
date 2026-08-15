package raft

// Step delivers one message to the node.
//
// Every message is filtered by term before anything else happens, because two
// rules govern the whole protocol and both are about terms:
//
//   - A message from a higher term means this node has fallen behind. It
//     adopts that term and reverts to Follower, whatever it was doing. This is
//     what retires a leader that was partitioned away while the cluster moved
//     on: it cannot keep acting as leader once it sees a newer term.
//   - A message from a lower term is from a node that has itself fallen
//     behind. It is refused, and the refusal carries this node's term so the
//     sender learns and steps down.
func (n *Node) Step(m Message) {
	switch {
	case m.Term > n.currentTerm:
		// A vote request is not itself evidence of a leader, so stepping down
		// leaves the vote unspent and this node free to grant it below.
		n.becomeFollower(m.Term, None)

	case m.Term < n.currentTerm:
		n.rejectStale(m)
		return
	}

	switch m.Type {
	case MsgRequestVote:
		n.handleRequestVote(m)
	case MsgRequestVoteResponse:
		n.handleRequestVoteResponse(m)
	case MsgAppendEntries:
		n.handleAppendEntries(m)
	case MsgAppendEntriesResponse:
		n.handleAppendEntriesResponse(m)
	}
}

// rejectStale answers a message from an older term, telling the sender the
// current one.
func (n *Node) rejectStale(m Message) {
	n.logger.V(3).Info("rejecting stale message",
		"type", m.Type, "from", int(m.From), "theirTerm", m.Term, "ourTerm", n.currentTerm)

	switch m.Type {
	case MsgRequestVote:
		n.send(Message{
			Type:    MsgRequestVoteResponse,
			To:      m.From,
			Term:    n.currentTerm,
			Granted: false,
		})
	case MsgAppendEntries:
		n.send(Message{
			Type:    MsgAppendEntriesResponse,
			To:      m.From,
			Term:    n.currentTerm,
			Success: false,
		})
	}
}

// handleRequestVote decides whether to vote for a candidate.
//
// Two conditions must both hold, and each closes off a way of electing a bad
// leader:
//
//   - This node has not already voted for someone else this term. One vote per
//     term is what stops a node helping two candidates reach a majority
//     simultaneously, which would produce two leaders.
//   - The candidate's log is at least as current as this node's. A candidate
//     needs a majority to win, and a committed entry already sits on a
//     majority, so those two sets always overlap. Refusing candidates that are
//     behind therefore guarantees any winner holds every committed entry —
//     committed data can never be lost to an election.
//
// Granting the same vote twice to the same candidate is safe and necessary:
// the first reply may have been dropped and the candidate will retry.
func (n *Node) handleRequestVote(m Message) {
	alreadyVoted := n.votedFor != None && n.votedFor != m.From
	granted := !alreadyVoted && n.log.IsUpToDate(m.LastLogIndex, m.LastLogTerm)

	if granted {
		n.votedFor = m.From
		// Having just endorsed a candidate, give it time to win before
		// standing against it.
		n.resetElectionTimeout()
	}

	n.logger.V(2).Info("vote requested",
		"candidate", int(m.From), "term", m.Term, "granted", granted,
		"alreadyVoted", alreadyVoted)

	n.send(Message{
		Type:    MsgRequestVoteResponse,
		To:      m.From,
		Term:    n.currentTerm,
		Granted: granted,
	})
}

// handleRequestVoteResponse counts a vote and promotes this node if the count
// reaches a majority.
func (n *Node) handleRequestVoteResponse(m Message) {
	if n.state != Candidate {
		// Already won, already lost, or already stepped down: the answer is
		// no longer relevant.
		return
	}

	// votes is keyed by sender, so a peer that answers twice is counted once.
	n.votes[m.From] = m.Granted

	n.logger.V(2).Info("vote received",
		"from", int(m.From), "granted", m.Granted, "needed", n.quorum())

	n.tallyVotes()
}
