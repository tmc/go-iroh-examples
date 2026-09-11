package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/docs"
	"github.com/tmc/go-iroh/iroh"
)

func TestRun(t *testing.T) {
	var buf bytes.Buffer
	err := run(&buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		"bootstrap: bob reconciled 1 entry he never saw broadcast",
		"gossip: alice's write reached bob without a sync call",
		"gossip: bob's write reached alice the same way",
		"converged: 3 entries on both replicas",
		"persisted: 3 entries read back from the file",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}

// TestBootstrapCatchesUpLateJoiner isolates the half of live sync that is not
// gossip. Every entry here is written before the second replica exists, so
// nothing was broadcast to it and only reconciliation can deliver them. The
// example shows this with one entry; a document's worth is the case that
// matters, since a peer joining an established document is the normal one.
func TestBootstrapCatchesUpLateJoiner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	namespace := docs.NewNamespaceSecret(seed(0xd0))
	lookup := iroh.NewMemoryLookup()

	alice, err := newReplica(ctx, "alice", 0xa1, docs.NewMemoryStore(), lookup)
	if err != nil {
		t.Fatal(err)
	}
	defer alice.close(context.Background())

	const entries = 5
	for i := range entries {
		if err := alice.put(ctx, namespace, string(rune('a'+i)), "value"); err != nil {
			t.Fatal(err)
		}
	}
	if err := alice.startLiveSync(ctx, namespace); err != nil {
		t.Fatal(err)
	}

	bob, err := newReplica(ctx, "bob", 0xb0, docs.NewMemoryStore(), lookup)
	if err != nil {
		t.Fatal(err)
	}
	defer bob.close(context.Background())
	if err := bob.startLiveSync(ctx, namespace, alice.ep.Addr()); err != nil {
		t.Fatal(err)
	}

	if err := bob.await(ctx, entries); err != nil {
		t.Fatalf("bob was not caught up: %v", err)
	}
	if a, b := alice.store.Fingerprint(docs.Range{}), bob.store.Fingerprint(docs.Range{}); a != b {
		t.Error("replicas did not converge on the entries written before bob joined")
	}
}
