// Package api exposes the key-value store and the Raft message endpoint over
// HTTP.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/go-logr/logr"

	"github.com/Shreesha001/raftkv/internal/kv"
	"github.com/Shreesha001/raftkv/internal/raft"
	"github.com/Shreesha001/raftkv/internal/server"
	"github.com/Shreesha001/raftkv/internal/transport"
)

// maxValueBytes caps a request body. Every write is replicated to every node
// and stored forever, so an unbounded body is a way to exhaust the cluster.
const maxValueBytes = 1 << 20 // 1 MiB

// Handler serves the client API and the peer message endpoint.
type Handler struct {
	server *server.Server
	// peerAddresses maps node IDs to base URLs so a follower can redirect a
	// client to the leader rather than merely refusing.
	peerAddresses map[raft.NodeID]string
	logger        logr.Logger
}

// New returns an HTTP handler for the given server.
func New(s *server.Server, peerAddresses map[raft.NodeID]string, logger logr.Logger) http.Handler {
	h := &Handler{server: s, peerAddresses: peerAddresses, logger: logger}

	mux := http.NewServeMux()
	mux.HandleFunc("POST "+transport.MessagePath, h.handleRaftMessage)
	mux.HandleFunc("GET /kv/{key}", h.handleGet)
	mux.HandleFunc("PUT /kv/{key}", h.handlePut)
	mux.HandleFunc("DELETE /kv/{key}", h.handleDelete)
	mux.HandleFunc("GET /status", h.handleStatus)
	return mux
}

// handleRaftMessage receives a message from a peer.
//
// It returns as soon as the message is queued rather than waiting for it to be
// processed: the sender needs no reply, because every Raft response is itself
// a separate message posted back the other way.
func (h *Handler) handleRaftMessage(w http.ResponseWriter, r *http.Request) {
	var m raft.Message
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		http.Error(w, "malformed raft message", http.StatusBadRequest)
		return
	}

	h.server.Deliver(m)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	value, found, err := h.server.Get(key)
	if errors.Is(err, server.ErrNotLeader) {
		h.redirectToLeader(w, r)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !found {
		http.Error(w, fmt.Sprintf("key %q not found", key), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprint(w, value)
}

func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxValueBytes+1))
	if err != nil {
		http.Error(w, "cannot read body", http.StatusBadRequest)
		return
	}
	if len(body) > maxValueBytes {
		http.Error(w, "value too large", http.StatusRequestEntityTooLarge)
		return
	}

	h.replicate(w, r, kv.Command{
		Op:    kv.OpSet,
		Key:   r.PathValue("key"),
		Value: string(body),
	})
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	h.replicate(w, r, kv.Command{Op: kv.OpDelete, Key: r.PathValue("key")})
}

// replicate encodes a command and waits for it to commit.
func (h *Handler) replicate(w http.ResponseWriter, r *http.Request, cmd kv.Command) {
	command, err := cmd.Encode()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// The request context bounds the wait, so a client that gives up does not
	// leave a handler blocked on a cluster that cannot reach a majority.
	err = h.server.Apply(r.Context(), command)
	switch {
	case err == nil:
		w.WriteHeader(http.StatusOK)
	case errors.Is(err, server.ErrNotLeader):
		h.redirectToLeader(w, r)
	case errors.Is(err, server.ErrStopped):
		http.Error(w, "server shutting down", http.StatusServiceUnavailable)
	default:
		// Most often a context deadline: the write may still commit later, so
		// the honest answer is "unknown" rather than "failed".
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	}
}

// redirectToLeader points a client at whoever currently leads.
//
// 307 preserves the method and body, so a redirected PUT stays a PUT. If no
// leader is known — during an election — there is nowhere to send the client,
// and 503 tells it to retry shortly.
func (h *Handler) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	leader := h.server.Status().Leader

	address, ok := h.peerAddresses[leader]
	if leader == raft.None || !ok {
		http.Error(w, "no leader elected; retry shortly", http.StatusServiceUnavailable)
		return
	}

	http.Redirect(w, r, strings.TrimSuffix(address, "/")+r.URL.Path, http.StatusTemporaryRedirect)
}

func (h *Handler) handleStatus(w http.ResponseWriter, _ *http.Request) {
	status := h.server.Status()

	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(map[string]any{
		"id":     int(status.ID),
		"state":  status.State.String(),
		"term":   uint64(status.Term),
		"leader": int(status.Leader),
		"commit": uint64(status.Commit),
	})
	if err != nil {
		h.logger.V(1).Info("cannot write status response", "err", err.Error())
	}
}
