// Command raftkvd runs one node of a replicated key-value store.
//
// A cluster is formed by starting one process per node, each told its own ID
// and the addresses of all members:
//
//	raftkvd -id 1 -peers 1=localhost:8081,2=localhost:8082,3=localhost:8083
//	raftkvd -id 2 -peers 1=localhost:8081,2=localhost:8082,3=localhost:8083
//	raftkvd -id 3 -peers 1=localhost:8081,2=localhost:8082,3=localhost:8083
//
// Then write and read through any node; followers redirect to the leader:
//
//	curl -L -X PUT localhost:8081/kv/greeting -d hello
//	curl -L localhost:8081/kv/greeting
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Shreesha001/raftkv/internal/api"
	"github.com/Shreesha001/raftkv/internal/logging"
	"github.com/Shreesha001/raftkv/internal/raft"
	"github.com/Shreesha001/raftkv/internal/server"
	"github.com/Shreesha001/raftkv/internal/storage"
	"github.com/Shreesha001/raftkv/internal/transport"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "raftkvd: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		id        = flag.Int("id", 0, "this node's ID; must appear in -peers")
		peerList  = flag.String("peers", "", "comma-separated id=host:port list of all cluster members")
		dataDir   = flag.String("data-dir", "data", "directory for persistent state")
		verbosity = flag.Int("v", 1, "log verbosity: 0 errors, 1 elections, 2 replication, 3 every message")

		tickInterval  = flag.Duration("tick", 100*time.Millisecond, "duration of one Raft tick")
		electionTick  = flag.Int("election-ticks", 10, "ticks without a leader before standing for election")
		heartbeatTick = flag.Int("heartbeat-ticks", 3, "ticks between heartbeats; must be below -election-ticks")
	)
	flag.Parse()

	logger, err := logging.New(*verbosity)
	if err != nil {
		return err
	}
	defer logging.Flush()

	peers, err := parsePeers(*peerList)
	if err != nil {
		return err
	}
	self := raft.NodeID(*id)
	address, ok := peers[self]
	if !ok {
		return fmt.Errorf("node ID %d does not appear in -peers", *id)
	}

	others := make([]raft.NodeID, 0, len(peers)-1)
	for peerID := range peers {
		if peerID != self {
			others = append(others, peerID)
		}
	}

	store, err := storage.NewFile(filepath.Join(*dataDir, fmt.Sprintf("node-%d.json", *id)))
	if err != nil {
		return err
	}

	peerURLs := make(map[raft.NodeID]string, len(peers))
	for peerID, addr := range peers {
		peerURLs[peerID] = "http://" + addr
	}
	peerTransport := transport.NewHTTP(peerURLs, logger)
	defer peerTransport.Close()

	srv, err := server.New(server.Config{
		Node: raft.Config{
			ID:            self,
			Peers:         others,
			ElectionTick:  *electionTick,
			HeartbeatTick: *heartbeatTick,
			Storage:       store,
			Logger:        logger,
		},
		Transport:    peerTransport,
		TickInterval: *tickInterval,
		Logger:       logger,
	})
	if err != nil {
		return err
	}

	srv.Start()
	defer srv.Stop()

	httpServer := &http.Server{
		Addr:              address,
		Handler:           api.New(srv, peerURLs, logger),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Shut down on interrupt so an operator stopping a node does not leave
	// requests half-served.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	errs := make(chan error, 1)
	go func() {
		logger.V(0).Info("listening", "node", *id, "address", address,
			"peers", len(others), "electionTimeout", time.Duration(*electionTick)*(*tickInterval))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- err
		}
	}()

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		logger.V(0).Info("shutting down")
	}

	shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	return httpServer.Shutdown(shutdownCtx)
}

// parsePeers reads an "id=host:port,id=host:port" cluster description.
func parsePeers(list string) (map[raft.NodeID]string, error) {
	if strings.TrimSpace(list) == "" {
		return nil, errors.New("-peers is required, e.g. 1=localhost:8081,2=localhost:8082")
	}

	peers := make(map[raft.NodeID]string)
	for _, item := range strings.Split(list, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		idText, address, found := strings.Cut(item, "=")
		if !found {
			return nil, fmt.Errorf("malformed peer %q, want id=host:port", item)
		}
		id, err := strconv.Atoi(strings.TrimSpace(idText))
		if err != nil {
			return nil, fmt.Errorf("malformed peer ID in %q: %w", item, err)
		}
		if address = strings.TrimSpace(address); address == "" {
			return nil, fmt.Errorf("peer %d has no address", id)
		}
		if _, duplicate := peers[raft.NodeID(id)]; duplicate {
			return nil, fmt.Errorf("peer ID %d appears twice", id)
		}
		peers[raft.NodeID(id)] = address
	}

	if len(peers)%2 == 0 {
		return nil, fmt.Errorf("cluster has %d members; use an odd number so a "+
			"majority always exists", len(peers))
	}
	return peers, nil
}
