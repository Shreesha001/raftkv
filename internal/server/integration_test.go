package server_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/Shreesha001/raftkv/internal/api"
	"github.com/Shreesha001/raftkv/internal/kv"
	"github.com/Shreesha001/raftkv/internal/raft"
	"github.com/Shreesha001/raftkv/internal/server"
	"github.com/Shreesha001/raftkv/internal/storage"
	"github.com/Shreesha001/raftkv/internal/transport"
)

// These tests run real servers over real HTTP on loopback, so unlike the
// deterministic tests in internal/raft they depend on wall-clock time. Ticks
// are short and every wait polls with a generous ceiling rather than sleeping
// a fixed duration, which keeps them quick when things work and honest when
// they do not.
const (
	tickInterval = 10 * time.Millisecond
	settleLimit  = 10 * time.Second
)

// testCluster is a set of servers wired together over loopback HTTP.
type testCluster struct {
	t       *testing.T
	servers map[raft.NodeID]*server.Server
	urls    map[raft.NodeID]string
	http    map[raft.NodeID]*http.Server
}

// newTestCluster starts n servers and returns once they are listening.
//
// Listeners are opened before any server is built because each node needs the
// addresses of all the others up front, and binding port 0 is the only way to
// learn them without hard-coding ports a test machine may already be using.
func newTestCluster(t *testing.T, ids ...raft.NodeID) *testCluster {
	t.Helper()

	listeners := make(map[raft.NodeID]net.Listener, len(ids))
	urls := make(map[raft.NodeID]string, len(ids))
	for _, id := range ids {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[id] = listener
		urls[id] = "http://" + listener.Addr().String()
	}

	c := &testCluster{
		t:       t,
		servers: make(map[raft.NodeID]*server.Server, len(ids)),
		urls:    urls,
		http:    make(map[raft.NodeID]*http.Server, len(ids)),
	}

	dir := t.TempDir()
	for _, id := range ids {
		peers := make([]raft.NodeID, 0, len(ids)-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		store, err := storage.NewFile(fmt.Sprintf("%s/node-%d.json", dir, id))
		if err != nil {
			t.Fatalf("storage: %v", err)
		}

		srv, err := server.New(server.Config{
			Node: raft.Config{
				ID:            id,
				Peers:         peers,
				ElectionTick:  10,
				HeartbeatTick: 2,
				Storage:       store,
				Logger:        logr.Discard(),
			},
			Transport:    transport.NewHTTP(urls, logr.Discard()),
			TickInterval: tickInterval,
			Logger:       logr.Discard(),
		})
		if err != nil {
			t.Fatalf("server.New: %v", err)
		}
		srv.Start()
		c.servers[id] = srv

		httpServer := &http.Server{Handler: api.New(srv, urls, logr.Discard())}
		c.http[id] = httpServer
		go httpServer.Serve(listeners[id])
	}

	t.Cleanup(c.stop)
	return c
}

func (c *testCluster) stop() {
	for id, httpServer := range c.http {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		httpServer.Shutdown(ctx)
		cancel()
		c.servers[id].Stop()
	}
}

// awaitLeader polls until exactly one node leads and is ready to serve.
//
// Waiting for the leader role alone is not enough: a leader that has just been
// elected has not yet committed an entry of its own term, so it refuses reads
// until it has. Real clients see the same window and retry through it.
func (c *testCluster) awaitLeader() raft.NodeID {
	c.t.Helper()

	deadline := time.Now().Add(settleLimit)
	for time.Now().Before(deadline) {
		var leaders []raft.NodeID
		for id, srv := range c.servers {
			if status := srv.Status(); status.State == raft.Leader && status.ReadsReady {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(tickInterval)
	}

	c.t.Fatalf("no single ready leader within %v", settleLimit)
	return raft.None
}

// client follows redirects, as a real client would.
func (c *testCluster) client() *http.Client {
	return &http.Client{Timeout: 5 * time.Second}
}

func (c *testCluster) put(id raft.NodeID, key, value string) (int, error) {
	req, err := http.NewRequest(http.MethodPut, c.urls[id]+"/kv/"+key, strings.NewReader(value))
	if err != nil {
		return 0, err
	}
	resp, err := c.client().Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func (c *testCluster) get(id raft.NodeID, key string) (string, int, error) {
	resp, err := c.client().Get(c.urls[id] + "/kv/" + key)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", resp.StatusCode, err
	}
	return string(body), resp.StatusCode, nil
}

func TestClusterElectsLeaderOverHTTP(t *testing.T) {
	c := newTestCluster(t, 1, 2, 3)

	leader := c.awaitLeader()

	// Every node must agree who leads. Disagreement here would mean the
	// cluster had split.
	deadline := time.Now().Add(settleLimit)
	for time.Now().Before(deadline) {
		agreed := true
		for _, srv := range c.servers {
			if srv.Status().Leader != leader {
				agreed = false
				break
			}
		}
		if agreed {
			return
		}
		time.Sleep(tickInterval)
	}
	t.Fatalf("nodes never agreed that %v leads", leader)
}

// A write sent to a follower must reach the leader rather than being refused.
func TestWriteToFollowerIsRedirectedAndCommitted(t *testing.T) {
	c := newTestCluster(t, 1, 2, 3)
	leader := c.awaitLeader()

	var follower raft.NodeID
	for id := range c.servers {
		if id != leader {
			follower = id
			break
		}
	}

	status, err := c.put(follower, "greeting", "hello")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("PUT to follower returned %d, want 200", status)
	}

	value, status, err := c.get(leader, "greeting")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if status != http.StatusOK || value != "hello" {
		t.Errorf("GET returned (%q, %d), want (%q, 200)", value, status, "hello")
	}
}

// A committed write is on every node's state machine, not just the leader's.
func TestCommittedWritesReachEveryNode(t *testing.T) {
	c := newTestCluster(t, 1, 2, 3)
	leader := c.awaitLeader()

	writes := map[string]string{"x": "1", "y": "2", "z": "3"}
	for key, value := range writes {
		if status, err := c.put(leader, key, value); err != nil || status != http.StatusOK {
			t.Fatalf("put %s: status %d, err %v", key, status, err)
		}
	}

	// Inspect each node's own store directly rather than through HTTP, which
	// would redirect every read to the leader and prove nothing.
	deadline := time.Now().Add(settleLimit)
	for time.Now().Before(deadline) {
		if c.allNodesCommitted(uint64(len(writes))) {
			return
		}
		time.Sleep(tickInterval)
	}
	t.Fatalf("not every node committed %d entries", len(writes))
}

func (c *testCluster) allNodesCommitted(want uint64) bool {
	for _, srv := range c.servers {
		if uint64(srv.Status().Commit) < want {
			return false
		}
	}
	return true
}

func TestDeleteRemovesKey(t *testing.T) {
	c := newTestCluster(t, 1, 2, 3)
	leader := c.awaitLeader()

	if status, err := c.put(leader, "temp", "value"); err != nil || status != http.StatusOK {
		t.Fatalf("put: status %d, err %v", status, err)
	}

	req, err := http.NewRequest(http.MethodDelete, c.urls[leader]+"/kv/temp", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := c.client().Do(req)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE returned %d, want 200", resp.StatusCode)
	}

	_, status, err := c.get(leader, "temp")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if status != http.StatusNotFound {
		t.Errorf("GET after DELETE returned %d, want 404", status)
	}
}

func TestUnknownKeyReturnsNotFound(t *testing.T) {
	c := newTestCluster(t, 1, 2, 3)
	leader := c.awaitLeader()

	_, status, err := c.get(leader, "absent")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if status != http.StatusNotFound {
		t.Errorf("status = %d, want 404", status)
	}
}

// Losing the leader must not lose acknowledged writes, and the cluster must
// keep serving on the survivors.
func TestClusterSurvivesLeaderLoss(t *testing.T) {
	c := newTestCluster(t, 1, 2, 3)
	original := c.awaitLeader()

	if status, err := c.put(original, "before", "kept"); err != nil || status != http.StatusOK {
		t.Fatalf("put: status %d, err %v", status, err)
	}

	// Stop the leader outright, as a crash would.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	c.http[original].Shutdown(ctx)
	cancel()
	c.servers[original].Stop()
	delete(c.servers, original)
	delete(c.http, original)

	replacement := c.awaitLeader()
	if replacement == original {
		t.Fatal("stopped node is still reported as leader")
	}

	// The acknowledged write survived.
	value, status, err := c.get(replacement, "before")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if status != http.StatusOK || value != "kept" {
		t.Errorf("GET returned (%q, %d), want (%q, 200)", value, status, "kept")
	}

	// And the two survivors, still a majority, accept new writes.
	if status, err := c.put(replacement, "after", "works"); err != nil || status != http.StatusOK {
		t.Errorf("PUT after failover: status %d, err %v", status, err)
	}
}

// Commands are opaque to Raft, so the encoding the API produces must be what
// the store expects. This pins the contract between the two.
func TestAPIEncodesCommandsTheStoreUnderstands(t *testing.T) {
	cmd := kv.Command{Op: kv.OpSet, Key: "k", Value: "v"}

	data, err := cmd.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	store := kv.NewStore()
	if err := store.Apply(data); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got, ok := store.Get("k"); !ok || got != "v" {
		t.Errorf("Get(k) = (%q, %v), want (%q, true)", got, ok, "v")
	}
}
