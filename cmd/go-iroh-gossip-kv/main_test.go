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
		// The operation crossed the topic, verified, and was stored.
		"color=blue seq=1 signer=",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}

// TestApply pins the merge rule the broadcast medium relies on: verify before
// storing, apply more than once without effect, and never go backwards in
// sequence.
func TestApply(t *testing.T) {
	sk, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	defer sk.Clear()
	n := &node{signer: sk, store: make(kvStore)}

	op, err := n.signSet("color", "blue", 2)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 { // duplicate delivery is normal on a gossip topic
		if err := n.store.apply(op); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	if got := n.store["color"]; got.Value != "blue" || got.Seq != 2 {
		t.Errorf("after apply: %+v, want value blue seq 2", got)
	}

	stale, err := n.signSet("color", "red", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.store.apply(stale); err != nil {
		t.Fatalf("apply stale: %v", err)
	}
	if got := n.store["color"]; got.Value != "blue" {
		t.Errorf("stale operation overwrote the newer value: %+v", got)
	}

	// Editing the body invalidates the signature over it.
	tampered := bytes.Replace(op, []byte(`"blue"`), []byte(`"gold"`), 1)
	if bytes.Equal(tampered, op) {
		t.Fatal("test did not modify the operation")
	}
	if err := n.store.apply(tampered); err == nil {
		t.Error("apply accepted an operation whose body was edited")
	}
}
