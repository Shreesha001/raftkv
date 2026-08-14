package logging

import (
	"bytes"
	"strings"
	"testing"

	"k8s.io/klog/v2"
)

// capture redirects klog output into a buffer for the duration of the test.
//
// klog's verbosity is process-global, so these tests must not run in parallel.
func capture(t *testing.T) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer
	klog.SetOutput(&buf)
	klog.LogToStderr(false)
	t.Cleanup(func() {
		klog.Flush()
		klog.LogToStderr(true)
		klog.SetOutput(nil)
	})
	return &buf
}

func TestVerbosityGating(t *testing.T) {
	log, err := New(1)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	tests := []struct {
		level int
		want  bool
	}{
		{0, true},  // errors and top-level events
		{1, true},  // at the threshold
		{2, false}, // replication detail — too noisy
		{3, false}, // heartbeats — far too noisy
	}

	for _, tt := range tests {
		if got := log.V(tt.level).Enabled(); got != tt.want {
			t.Errorf("V(%d).Enabled() = %v, want %v", tt.level, got, tt.want)
		}
	}
}

func TestVerbosityZeroAllowsOnlyLevelZero(t *testing.T) {
	log, err := New(0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if !log.V(0).Enabled() {
		t.Error("V(0).Enabled() = false, want true")
	}
	if log.V(1).Enabled() {
		t.Error("V(1).Enabled() = true, want false")
	}
}

func TestVerbosityThreeEnablesEverything(t *testing.T) {
	log, err := New(3)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for level := 0; level <= 3; level++ {
		if !log.V(level).Enabled() {
			t.Errorf("V(%d).Enabled() = false, want true", level)
		}
	}
}

func TestNewRejectsNegativeVerbosity(t *testing.T) {
	if _, err := New(-1); err == nil {
		t.Fatal("New(-1): got nil error, want error")
	}
}

func TestSuppressedMessageProducesNoOutput(t *testing.T) {
	buf := capture(t)
	log, err := New(1)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	log.V(3).Info("heartbeat", "peer", 2)
	klog.Flush()

	if got := buf.String(); got != "" {
		t.Errorf("suppressed message produced output: %q", got)
	}
}

func TestEnabledMessageIsWritten(t *testing.T) {
	buf := capture(t)
	log, err := New(1)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	log.V(1).Info("became leader", "term", 4)
	klog.Flush()

	got := buf.String()
	if !strings.Contains(got, "became leader") {
		t.Errorf("output %q does not contain message", got)
	}
	if !strings.Contains(got, "term") || !strings.Contains(got, "4") {
		t.Errorf("output %q does not contain term=4", got)
	}
}

// WithValues attaches fields once so call sites need not repeat the node ID.
func TestWithValuesAttachesFields(t *testing.T) {
	buf := capture(t)
	log, err := New(1)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	log = log.WithValues("node", 7)

	log.V(1).Info("started")
	klog.Flush()

	got := buf.String()
	if !strings.Contains(got, "node") || !strings.Contains(got, "7") {
		t.Errorf("output %q does not contain node=7", got)
	}
}

func TestWithValuesPreservesVerbosity(t *testing.T) {
	log, err := New(1)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	log = log.WithValues("component", "raft")

	if log.V(2).Enabled() {
		t.Error("V(2).Enabled() = true after WithValues, want false")
	}
}

func TestNopDiscardsEverything(t *testing.T) {
	buf := capture(t)
	log := Nop()

	log.V(0).Info("should vanish")
	log.WithValues("node", 1).V(3).Info("also vanishes")
	klog.Flush()

	if got := buf.String(); got != "" {
		t.Errorf("Nop logger produced output: %q", got)
	}
	if log.V(0).Enabled() {
		t.Error("Nop().V(0).Enabled() = true, want false")
	}
}
