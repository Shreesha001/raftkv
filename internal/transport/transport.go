// Package transport carries Raft messages between nodes.
package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-logr/logr"

	"github.com/Shreesha001/raftkv/internal/raft"
)

// Transport delivers messages to peers.
//
// Send is one-way and gives no indication of success. That is deliberate: Raft
// already assumes an unreliable network and recovers from loss through
// heartbeats, retries, and the nextIndex backoff. Reporting delivery failures
// upward would only duplicate machinery the protocol already has.
type Transport interface {
	// Send delivers a message to m.To. It must not block the caller, because
	// the caller is the goroutine running the Raft state machine.
	Send(m raft.Message)
	// Close releases resources.
	Close() error
}

// HTTP sends messages as JSON POSTs to peers.
//
// HTTP with JSON is used in place of gRPC: the payload is a single struct, the
// message volume is small, and it keeps the project free of a protobuf
// toolchain. The cost is bandwidth and encoding time, neither of which matters
// at this scale, and any transport satisfying the interface can replace it.
type HTTP struct {
	// addresses maps node IDs to base URLs, e.g. "http://localhost:8081".
	addresses map[raft.NodeID]string
	client    *http.Client
	logger    logr.Logger
}

// MessagePath is the endpoint peers post Raft messages to.
const MessagePath = "/raft/message"

// NewHTTP returns a transport that posts to the given peer addresses.
func NewHTTP(addresses map[raft.NodeID]string, logger logr.Logger) *HTTP {
	return &HTTP{
		addresses: addresses,
		client: &http.Client{
			// Bounded so a hung peer cannot tie up a sender indefinitely. It
			// is comfortably above a healthy round trip and well below an
			// election timeout, so a slow peer looks like a lost message
			// rather than stalling the cluster.
			Timeout: 2 * time.Second,
		},
		logger: logger,
	}
}

// Send posts the message to its recipient in the background.
//
// Delivery happens on its own goroutine because the caller is the goroutine
// driving the Raft state machine: blocking it on a network write would stop
// the node processing anything else, including the heartbeats that keep it
// from being replaced.
func (h *HTTP) Send(m raft.Message) {
	address, ok := h.addresses[m.To]
	if !ok {
		h.logger.V(0).Info("no address for peer; dropping message",
			"to", int(m.To), "type", m.Type)
		return
	}

	go func() {
		if err := h.post(address, m); err != nil {
			// Expected whenever a peer is down or unreachable. Raft recovers
			// on its own, so this is routine rather than exceptional.
			h.logger.V(3).Info("send failed",
				"to", int(m.To), "type", m.Type, "err", err.Error())
		}
	}()
}

func (h *HTTP) post(address string, m raft.Message) error {
	body, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("transport: encode message: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		address+MessagePath, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("transport: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("transport: peer returned %s", resp.Status)
	}
	return nil
}

// Close releases idle connections.
func (h *HTTP) Close() error {
	h.client.CloseIdleConnections()
	return nil
}
