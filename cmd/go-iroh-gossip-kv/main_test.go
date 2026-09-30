package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tmc/go-iroh/key"
)

func TestRun(t *testing.T) {
	var buf bytes.Buffer
	err := run(&buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		// The joining node found the bootstrap address it was given.
		"joined 1 neighbor",
		// The update crossed the topic, verified, and was stored.
		"color=blue signer=",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}

// TestApply pins the merge rule the broadcast medium relies on: verify before
// storing, apply more than once without effect, and never go backwards in
// time.
func TestApply(t *testing.T) {
	sk, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	defer sk.Clear()
	store := make(kvStore)
	scope := sk.Public().Bytes()

	op := signUpdate(sk, []byte("color"), []byte("blue"), 2).encode()
	for range 2 { // duplicate delivery is normal on a gossip topic
		if err := store.apply(op); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	if got := store[scope]["color"]; got.Value != "blue" || got.Timestamp != 2 {
		t.Errorf("after apply: %+v, want value blue timestamp 2", got)
	}

	stale := signUpdate(sk, []byte("color"), []byte("red"), 1).encode()
	if err := store.apply(stale); err != nil {
		t.Fatalf("apply stale: %v", err)
	}
	if got := store[scope]["color"]; got.Value != "blue" {
		t.Errorf("stale update overwrote the newer value: %+v", got)
	}

	// Editing the value invalidates the signature over it.
	tampered := bytes.Replace(op, []byte("blue"), []byte("gold"), 1)
	if bytes.Equal(tampered, op) {
		t.Fatal("test did not modify the update")
	}
	if err := store.apply(tampered); err == nil {
		t.Error("apply accepted an update whose value was edited")
	}
}
