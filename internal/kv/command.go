// Package kv implements the replicated state machine: a key-value map built by
// applying commands from the Raft log in order.
package kv

import (
	"encoding/json"
	"fmt"
)

// Op identifies what a Command does.
type Op string

const (
	// OpSet stores Value at Key, overwriting any existing value.
	OpSet Op = "set"
	// OpDelete removes Key. Deleting an absent key is not an error.
	OpDelete Op = "delete"
)

// Command is a single instruction in the replicated log.
//
// Commands travel through Raft as opaque bytes; only this package knows how to
// interpret them. That separation is what keeps the consensus layer reusable
// for state machines other than a key-value map.
type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// validate reports whether the command is well formed.
func (c Command) validate() error {
	switch c.Op {
	case OpSet, OpDelete:
	default:
		return fmt.Errorf("kv: unknown op %q", c.Op)
	}
	if c.Key == "" {
		return fmt.Errorf("kv: command has empty key")
	}
	return nil
}

// Encode serialises the command for storage in the Raft log.
//
// JSON is chosen over a compact binary encoding so a log dump stays readable
// during debugging; the volumes here never make the difference matter.
func (c Command) Encode() ([]byte, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("kv: encode command: %w", err)
	}
	return data, nil
}

// DecodeCommand parses a command previously produced by Encode.
//
// Validation runs on the way in as well as on the way out: entries reaching
// this point have crossed a network and sat on a disk, and a malformed one
// means corruption rather than a caller mistake.
func DecodeCommand(data []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return Command{}, fmt.Errorf("kv: decode command: %w", err)
	}
	if err := c.validate(); err != nil {
		return Command{}, err
	}
	return c, nil
}
