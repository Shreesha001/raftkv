package kv

import (
	"fmt"
	"sync"
	"testing"
)

// mustEncode fails the test if the command is invalid.
func mustEncode(t *testing.T, cmd Command) []byte {
	t.Helper()
	data, err := cmd.Encode()
	if err != nil {
		t.Fatalf("Encode(%+v): %v", cmd, err)
	}
	return data
}

func TestStoreAppliesSet(t *testing.T) {
	s := NewStore()
	if err := s.Apply(mustEncode(t, Command{Op: OpSet, Key: "x", Value: "5"})); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	got, ok := s.Get("x")
	if !ok {
		t.Fatal("Get(x): not found, want found")
	}
	if got != "5" {
		t.Errorf("Get(x) = %q, want %q", got, "5")
	}
}

func TestStoreSetOverwrites(t *testing.T) {
	s := NewStore()
	for _, v := range []string{"1", "2", "3"} {
		if err := s.Apply(mustEncode(t, Command{Op: OpSet, Key: "x", Value: v})); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}

	if got, _ := s.Get("x"); got != "3" {
		t.Errorf("Get(x) = %q, want %q", got, "3")
	}
	if got := s.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1", got)
	}
}

func TestStoreAppliesDelete(t *testing.T) {
	s := NewStore()
	if err := s.Apply(mustEncode(t, Command{Op: OpSet, Key: "x", Value: "5"})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := s.Apply(mustEncode(t, Command{Op: OpDelete, Key: "x"})); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if _, ok := s.Get("x"); ok {
		t.Error("Get(x): found after delete, want not found")
	}
}

// Deleting a key that was never set is legal: replaying a log must never fail
// on account of ordering the caller cannot control.
func TestStoreDeleteMissingKeyIsNotAnError(t *testing.T) {
	s := NewStore()
	if err := s.Apply(mustEncode(t, Command{Op: OpDelete, Key: "ghost"})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := s.Len(); got != 0 {
		t.Errorf("Len() = %d, want 0", got)
	}
}

func TestStoreGetMissingKey(t *testing.T) {
	s := NewStore()
	value, ok := s.Get("nope")
	if ok {
		t.Error("Get: ok = true, want false")
	}
	if value != "" {
		t.Errorf("Get: value = %q, want empty", value)
	}
}

func TestStoreApplyRejectsGarbage(t *testing.T) {
	s := NewStore()
	if err := s.Apply([]byte("not json")); err == nil {
		t.Fatal("Apply: got nil error for garbage, want error")
	}
}

// The defining property of state machine replication: two stores fed the same
// commands in the same order end up identical.
func TestStoresConvergeOnIdenticalCommandSequence(t *testing.T) {
	commands := [][]byte{
		mustEncode(t, Command{Op: OpSet, Key: "x", Value: "1"}),
		mustEncode(t, Command{Op: OpSet, Key: "y", Value: "2"}),
		mustEncode(t, Command{Op: OpDelete, Key: "x"}),
		mustEncode(t, Command{Op: OpSet, Key: "x", Value: "9"}),
	}

	a, b := NewStore(), NewStore()
	for _, cmd := range commands {
		if err := a.Apply(cmd); err != nil {
			t.Fatalf("a.Apply: %v", err)
		}
		if err := b.Apply(cmd); err != nil {
			t.Fatalf("b.Apply: %v", err)
		}
	}

	for _, key := range []string{"x", "y"} {
		av, aok := a.Get(key)
		bv, bok := b.Get(key)
		if av != bv || aok != bok {
			t.Errorf("key %q diverged: a=(%q,%v) b=(%q,%v)", key, av, aok, bv, bok)
		}
	}
}

func TestStoreConcurrentAccessIsRaceFree(t *testing.T) {
	s := NewStore()
	var wg sync.WaitGroup

	for i := range 50 {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", i)
			_ = s.Apply(mustEncode(t, Command{Op: OpSet, Key: key, Value: "v"}))
		}(i)
		go func(i int) {
			defer wg.Done()
			s.Get(fmt.Sprintf("k%d", i))
		}(i)
	}
	wg.Wait()
}
