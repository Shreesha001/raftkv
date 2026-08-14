package kv

import "testing"

func TestCommandRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		cmd  Command
	}{
		{"set", Command{Op: OpSet, Key: "x", Value: "5"}},
		{"delete", Command{Op: OpDelete, Key: "x"}},
		{"empty value", Command{Op: OpSet, Key: "x", Value: ""}},
		{"key with spaces", Command{Op: OpSet, Key: "a b", Value: "c"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.cmd.Encode()
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got, err := DecodeCommand(data)
			if err != nil {
				t.Fatalf("DecodeCommand: %v", err)
			}
			if got != tt.cmd {
				t.Errorf("got %+v, want %+v", got, tt.cmd)
			}
		})
	}
}

func TestEncodeRejectsUnknownOp(t *testing.T) {
	cmd := Command{Op: "frobnicate", Key: "x"}
	if _, err := cmd.Encode(); err == nil {
		t.Fatal("Encode: got nil error for unknown op, want error")
	}
}

func TestEncodeRejectsEmptyKey(t *testing.T) {
	cmd := Command{Op: OpSet, Key: "", Value: "v"}
	if _, err := cmd.Encode(); err == nil {
		t.Fatal("Encode: got nil error for empty key, want error")
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := DecodeCommand([]byte("not json")); err == nil {
		t.Fatal("DecodeCommand: got nil error for garbage, want error")
	}
}

func TestDecodeRejectsUnknownOp(t *testing.T) {
	if _, err := DecodeCommand([]byte(`{"op":"frobnicate","key":"x"}`)); err == nil {
		t.Fatal("DecodeCommand: got nil error for unknown op, want error")
	}
}
