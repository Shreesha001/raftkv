// Package logging configures klog-backed structured logging with numeric
// verbosity levels.
//
// Raft is chatty: heartbeats fire every few tens of milliseconds per peer, so
// a cluster produces a constant stream of "nothing is wrong" messages.
// Verbosity levels let that detail be switched on for debugging without
// drowning normal operation, and without recompiling.
//
//	0  errors and top-level events
//	1  state transitions: elections, leader changes  (default)
//	2  replication: entries sent, commit index advances
//	3  every heartbeat and RPC — debugging only
//
// Loggers are logr.Logger values, which carry verbosity and contextual fields
// natively:
//
//	log = log.WithValues("node", id)  // every later message carries node=id
//	log.V(3).Info("heartbeat", "peer", peer)
//
// Components receive a logr.Logger at construction rather than reaching for a
// package-level one, so tests can pass Nop and stay silent.
package logging

import (
	"flag"
	"fmt"
	"strconv"

	"github.com/go-logr/logr"
	"k8s.io/klog/v2"
)

// New configures klog to emit messages up to the given verbosity and returns a
// logger honouring it.
//
// Verbosity is a klog flag and therefore process-global: the last call wins,
// and callers changing it concurrently will interfere with each other. Call it
// once, at startup.
func New(verbosity int) (logr.Logger, error) {
	if verbosity < 0 {
		return logr.Discard(), fmt.Errorf("logging: verbosity must be >= 0, got %d", verbosity)
	}

	// klog registers its configuration as command-line flags. Route them
	// through a private FlagSet so they do not appear on the program's own
	// command line, then set the one we care about.
	fs := flag.NewFlagSet("klog", flag.ContinueOnError)
	klog.InitFlags(fs)
	if err := fs.Set("v", strconv.Itoa(verbosity)); err != nil {
		return logr.Discard(), fmt.Errorf("logging: set verbosity: %w", err)
	}

	return klog.Background(), nil
}

// Nop returns a logger that discards everything. Use it in tests.
func Nop() logr.Logger {
	return logr.Discard()
}

// Flush writes out any buffered log entries. Call it before the process exits,
// or messages logged just before a crash are lost.
func Flush() {
	klog.Flush()
}
