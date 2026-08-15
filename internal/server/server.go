// Package server runs a Raft node: it owns the clock, the goroutine, and the
// wiring between consensus and the key-value store.
package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"

	"github.com/Shreesha001/raftkv/internal/kv"
	"github.com/Shreesha001/raftkv/internal/raft"
	"github.com/Shreesha001/raftkv/internal/transport"
)

// ErrNotLeader is returned when a write reaches a node that does not lead.
var ErrNotLeader = errors.New("server: not the leader")

// ErrStopped is returned once the server has shut down.
var ErrStopped = errors.New("server: stopped")

// Config describes one server.
type Config struct {
	// Node configures the underlying Raft node.
	Node raft.Config
	// Transport delivers messages to peers.
	Transport transport.Transport
	// TickInterval is how much wall-clock time one Raft tick represents.
	TickInterval time.Duration
	// Logger receives server events.
	Logger logr.Logger
}

// Status is a snapshot of a node's role, readable without disturbing the Raft
// goroutine.
type Status struct {
	ID     raft.NodeID
	State  raft.State
	Term   raft.Term
	Leader raft.NodeID
	Commit raft.Index
	// ReadsReady reports whether this node may answer reads. A leader that has
	// just taken office cannot until it has committed an entry of its own term.
	ReadsReady bool
}

// proposal is a client write awaiting commitment.
type proposal struct {
	command []byte
	result  chan error
}

// Server drives a Raft node.
//
// Exactly one goroutine touches the raft.Node, and everything else reaches it
// through channels. The node has no locks of its own, so this single-owner rule
// is what makes concurrent access safe — and it is checkable by inspection,
// unlike a scattering of mutexes.
type Server struct {
	cfg  Config
	node *raft.Node // owned solely by run()

	store *kv.Store

	incoming  chan raft.Message
	proposals chan proposal
	stop      chan struct{}
	stopped   chan struct{}
	stopOnce  sync.Once

	// pending maps a log index to the client waiting for it to commit.
	// Touched only by run().
	pending map[raft.Index]chan error

	// status is published for readers outside the Raft goroutine.
	mu     sync.RWMutex
	status Status

	logger logr.Logger
}

// New builds a server. It does not start it.
func New(cfg Config) (*Server, error) {
	if cfg.Transport == nil {
		return nil, errors.New("server: transport is required")
	}
	if cfg.TickInterval <= 0 {
		return nil, fmt.Errorf("server: TickInterval must be positive, got %v", cfg.TickInterval)
	}

	node := raft.NewNode(cfg.Node)

	s := &Server{
		cfg:   cfg,
		node:  node,
		store: kv.NewStore(),
		// Buffered so a burst of peer traffic does not block HTTP handlers
		// while the Raft goroutine is busy.
		incoming:  make(chan raft.Message, 256),
		proposals: make(chan proposal, 64),
		stop:      make(chan struct{}),
		stopped:   make(chan struct{}),
		pending:   make(map[raft.Index]chan error),
		logger:    cfg.Logger.WithValues("node", int(cfg.Node.ID)),
	}
	s.publishStatus()
	return s, nil
}

// Start begins driving the node. It returns immediately.
func (s *Server) Start() {
	go s.run()
}

// Stop shuts the server down and waits for the Raft goroutine to finish. It is
// safe to call more than once.
func (s *Server) Stop() {
	s.stopOnce.Do(func() { close(s.stop) })
	<-s.stopped
}

// Deliver hands a message from a peer to the Raft goroutine.
//
// It never blocks: if the queue is full the message is dropped, which Raft
// treats as an ordinary lost packet and recovers from by retrying. Blocking
// here would let a slow node stall the peers talking to it.
func (s *Server) Deliver(m raft.Message) {
	select {
	case s.incoming <- m:
	case <-s.stop:
	default:
		s.logger.V(1).Info("inbound queue full; dropping message",
			"from", int(m.From), "type", m.Type)
	}
}

// Status returns the node's current role.
func (s *Server) Status() Status {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.status
}

// Get reads a key.
//
// Reads are served only by a leader that is ready. A follower may be
// arbitrarily far behind, and answering from a stale replica would let a client
// read a value older than one it just wrote. A newly elected leader is refused
// too, until the no-op it appended on taking office commits: before that it
// holds inherited entries it cannot prove are committed and has not applied
// them, so it would report a successfully written key as missing.
//
// This is short of full linearizability: a leader deposed by a partition can
// still answer from stale state until it notices. Closing that gap requires
// confirming leadership with a heartbeat round before each read.
func (s *Server) Get(key string) (string, bool, error) {
	if !s.Status().ReadsReady {
		return "", false, ErrNotLeader
	}

	value, ok := s.store.Get(key)
	return value, ok, nil
}

// Apply replicates a command and returns once it has been committed and
// applied, or the context expires.
//
// Returning nil means a majority of the cluster holds the command durably, so
// it survives the loss of any single node.
func (s *Server) Apply(ctx context.Context, command []byte) error {
	p := proposal{command: command, result: make(chan error, 1)}

	select {
	case s.proposals <- p:
	case <-s.stop:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-p.result:
		return err
	case <-s.stop:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run is the single goroutine owning the Raft node.
func (s *Server) run() {
	defer close(s.stopped)

	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()

	s.logger.V(1).Info("server started", "tickInterval", s.cfg.TickInterval)

	for {
		select {
		case <-ticker.C:
			s.node.Tick()

		case m := <-s.incoming:
			s.node.Step(m)

		case p := <-s.proposals:
			s.handleProposal(p)

		case <-s.stop:
			s.failPending(ErrStopped)
			s.logger.V(1).Info("server stopped")
			return
		}

		s.dispatch()
	}
}

// handleProposal submits a client write to Raft.
func (s *Server) handleProposal(p proposal) {
	index, err := s.node.Propose(p.command)
	if err != nil {
		// Translate at the boundary so callers match on this package's
		// sentinel and never need to know which layer refused. The client
		// reached the wrong node, or leadership changed between its status
		// check and now.
		if errors.Is(err, raft.ErrNotLeader) {
			err = ErrNotLeader
		}
		p.result <- err
		return
	}

	// The client waits until this index is applied, so it learns of success
	// only once the write is genuinely durable across a majority.
	s.pending[index] = p.result
}

// dispatch performs the work that follows any state change: send what the node
// wants sent, apply what it has committed, and republish status.
func (s *Server) dispatch() {
	for _, m := range s.node.Messages() {
		s.cfg.Transport.Send(m)
	}

	for _, entry := range s.node.CommittedEntries() {
		s.applyEntry(entry)
	}

	s.publishStatus()
	s.expirePendingIfNotLeader()
}

// applyEntry hands a committed entry to the state machine and wakes whoever
// proposed it.
func (s *Server) applyEntry(entry raft.LogEntry) {
	// A leader commits an empty entry on taking office, which carries entries
	// inherited from earlier terms into committed state. It holds no command.
	if len(entry.Command) == 0 {
		return
	}

	err := s.store.Apply(entry.Command)
	if err != nil {
		// The entry is committed, so every node will try to apply it and fail
		// identically. That is corruption or a version mismatch rather than a
		// client error, and continuing would let replicas diverge.
		s.logger.Error(err, "cannot apply committed entry", "index", entry.Index)
	}

	if waiter, ok := s.pending[entry.Index]; ok {
		waiter <- err
		delete(s.pending, entry.Index)
	}
}

// expirePendingIfNotLeader fails outstanding writes after a loss of leadership.
//
// Entries proposed but not yet committed may be discarded by the next leader,
// so a client must be told the write did not happen rather than left waiting
// for a commit that will never come.
func (s *Server) expirePendingIfNotLeader() {
	if len(s.pending) == 0 || s.node.State() == raft.Leader {
		return
	}

	s.logger.V(1).Info("lost leadership; failing pending writes", "count", len(s.pending))
	s.failPending(ErrNotLeader)
}

func (s *Server) failPending(err error) {
	for index, waiter := range s.pending {
		waiter <- err
		delete(s.pending, index)
	}
}

// publishStatus copies the node's role where other goroutines can read it.
func (s *Server) publishStatus() {
	status := Status{
		ID:         s.node.ID(),
		State:      s.node.State(),
		Term:       s.node.Term(),
		Leader:     s.node.Leader(),
		Commit:     s.node.CommitIndex(),
		ReadsReady: s.node.CanServeReads(),
	}

	s.mu.Lock()
	s.status = status
	s.mu.Unlock()
}
