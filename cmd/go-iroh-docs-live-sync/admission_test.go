package main

import (
	"context"
	"testing"
	"time"

	"github.com/tmc/go-iroh/docs"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
)

func TestReplicaAdmissionLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	namespace := docs.NewNamespaceSecret(seed(0xd0))
	r, err := newReplica(ctx, "replica", 0xa1, docs.NewMemoryStore(), iroh.NewMemoryLookup())
	if err != nil {
		t.Fatal(err)
	}
	defer r.close(context.Background())
	if r.allowSync(namespace.ID(), key.EndpointID{}) {
		t.Fatal("namespace accepted before live sync starts")
	}
	if err := r.startLiveSync(ctx, namespace); err != nil {
		t.Fatal(err)
	}
	if !r.allowSync(namespace.ID(), key.EndpointID{}) {
		t.Fatal("active namespace rejected")
	}
	other := docs.NewNamespaceSecret(seed(0xd1))
	if r.allowSync(other.ID(), key.EndpointID{}) {
		t.Fatal("unserved namespace accepted")
	}
	r.close(context.Background())
	if r.allowSync(namespace.ID(), key.EndpointID{}) {
		t.Fatal("namespace accepted after close")
	}
}
