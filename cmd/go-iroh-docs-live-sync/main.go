// Command go-iroh-docs-live-sync replicates a document as it is written.
//
// go-iroh-docs-sync reconciles two replicas when it calls [docs.SyncTicket],
// which keeps the sequence observable and is not how a document is usually
// kept up to date. [docs.StartLiveSync] is: replicas subscribe to an
// iroh-gossip topic derived from the namespace, a local insert is broadcast to
// the neighbors as it happens, and a new neighbor is reconciled with the same
// range protocol before the stream of updates begins. Nothing here calls sync.
//
// The two mechanisms answer different halves of one question. Gossip carries
// what changes from now on, and cannot deliver what was written before a peer
// arrived; reconciliation catches a peer up on everything it missed but has to
// be asked. Live sync runs both, which is why a replica that joins late ends
// up with the entries it never saw broadcast.
//
// [docs.LiveSyncOptions] is where the two are configured: Bootstrap names the
// peers to join and reconcile with at startup, Resolver turns the endpoint IDs
// gossip reports into addresses, BlobStore reports which entry content is
// available locally, DownloadPolicy chooses which values to fetch, and OnSync
// observes each reconciliation attempt. This example leaves the policy at its
// zero value, which downloads everything.
//
// Resolver is not optional in the way it looks. A neighbor arrives from gossip
// as an ID and nothing else, so without one the reconciliation half of live
// sync fails with "no reachable address" while the gossip half keeps working —
// updates flow and a late joiner is never caught up.
//
// One replica keeps its entries in a file with [docs.NewFileStore], so the
// document outlives the process: the store appends every successful insert,
// and [docs.LoadMemoryStoreFile] reads it back. The example reopens the file
// at the end to show that what arrived over gossip was persisted like anything
// else, because a live replica that forgets on restart is a cache.
package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"time"

	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/docs"
	"github.com/tmc/go-iroh/gossip"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Fixed seeds keep the output the same from run to run. Real code uses
	// docs.GenerateNamespaceSecret and docs.GenerateAuthor.
	namespace := docs.NewNamespaceSecret(seed(0xd0))

	dir, err := os.MkdirTemp("", "go-iroh-docs-live-sync")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "bob.doc")

	// Live sync learns neighbors from gossip as bare endpoint IDs, so it needs
	// a resolver to turn one into an address before it can reconcile with it.
	// On loopback that is an iroh.MemoryLookup both replicas are added to; a
	// deployed program would resolve through DNS or a pkarr relay.
	lookup := iroh.NewMemoryLookup()

	alice, err := newReplica(ctx, "alice", 0xa1, docs.NewMemoryStore(), lookup)
	if err != nil {
		return err
	}
	defer alice.close(ctx)

	// Bob's entries live in a file, so they survive the process.
	bobStore, err := docs.NewFileStore(path)
	if err != nil {
		return fmt.Errorf("open bob's store: %w", err)
	}
	bob, err := newReplica(ctx, "bob", 0xb0, bobStore, lookup)
	if err != nil {
		return err
	}
	defer bob.close(ctx)

	// Alice writes before bob joins, so this entry can only reach him by
	// reconciliation: it was broadcast to nobody.
	if err := alice.put(ctx, namespace, "before", "written before bob joined"); err != nil {
		return err
	}

	if err := alice.startLiveSync(ctx, namespace); err != nil {
		return err
	}
	// Bob bootstraps to alice, which is both how he finds the topic's members
	// and how he is caught up on what he missed.
	if err := bob.startLiveSync(ctx, namespace, alice.ep.Addr()); err != nil {
		return err
	}

	if err := bob.await(ctx, 1); err != nil {
		return fmt.Errorf("bob was never caught up on the entry written before he joined: %w", err)
	}
	fmt.Println("bootstrap: bob reconciled 1 entry he never saw broadcast")

	// From here the topic carries the writes. Neither side calls sync.
	if err := alice.put(ctx, namespace, "after", "written while both are live"); err != nil {
		return err
	}
	if err := bob.await(ctx, 2); err != nil {
		return fmt.Errorf("alice's live write never reached bob: %w", err)
	}
	fmt.Println("gossip: alice's write reached bob without a sync call")

	// And the other way, which is the point of a multi-writer document.
	if err := bob.put(ctx, namespace, "reply", "bob writes too"); err != nil {
		return err
	}
	if err := alice.await(ctx, 3); err != nil {
		return fmt.Errorf("bob's live write never reached alice: %w", err)
	}
	fmt.Println("gossip: bob's write reached alice the same way")

	if a, b := alice.store.Fingerprint(docs.Range{}), bob.store.Fingerprint(docs.Range{}); a != b {
		return errors.New("replicas did not converge")
	}
	fmt.Println("converged:", alice.store.Len(), "entries on both replicas")

	// Everything bob received was appended to his file as it arrived.
	if err := bob.store.PersistError(); err != nil {
		return fmt.Errorf("bob's store failed to persist: %w", err)
	}
	reopened, err := docs.LoadMemoryStoreFile(path)
	if err != nil {
		return fmt.Errorf("reopen bob's store: %w", err)
	}
	if reopened.Fingerprint(docs.Range{}) != bob.store.Fingerprint(docs.Range{}) {
		return errors.New("bob's store did not survive being reopened")
	}
	fmt.Println("persisted:", reopened.Len(), "entries read back from the file")
	return nil
}

// replica is one peer: an endpoint serving both the docs and the gossip
// protocols, a document store, and a blob store for entry content.
type replica struct {
	name    string
	ep      *iroh.Endpoint
	router  *iroh.Router
	gossip  *gossip.Gossip
	lookup  *iroh.MemoryLookup
	store   *docs.MemoryStore
	content *blobs.MemStore
	author  docs.Author
	live    *docs.LiveSync
}

// newReplica binds an endpoint that answers both protocols live sync needs:
// iroh-gossip carries the updates, and the docs ALPN serves the range
// reconciliation a joining neighbor asks for.
func newReplica(ctx context.Context, name string, authorSeed byte, store *docs.MemoryStore, lookup *iroh.MemoryLookup) (*replica, error) {
	ep, err := bind(ctx, iroh.WithALPNs(docs.ALPN, gossip.ALPN))
	if err != nil {
		return nil, err
	}
	// Publish this replica's address so the other one can resolve it from the
	// ID gossip hands it.
	lookup.AddEndpointAddr(ep.Addr())
	content, err := blobs.NewMemStore()
	if err != nil {
		return nil, err
	}
	g := gossip.NewGossip(ep)
	r := &replica{
		name:    name,
		ep:      ep,
		lookup:  lookup,
		gossip:  g,
		store:   store,
		content: content,
		author:  docs.NewAuthor(seed(authorSeed)),
	}
	r.router, err = iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{
		gossip.ALPN: g.Handler(),
		docs.ALPN: &docs.Handler{
			Store:     store,
			BlobStore: content,
			Config:    docs.DefaultSyncConfig(),
		},
	}, nil)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// startLiveSync joins the namespace's topic, reconciling with bootstrap first.
func (r *replica) startLiveSync(ctx context.Context, namespace docs.NamespaceSecret, bootstrap ...netaddr.EndpointAddr) error {
	live, err := docs.StartLiveSync(ctx, r.ep, r.gossip, namespace.ID(), r.store, docs.LiveSyncOptions{
		Bootstrap: bootstrap,
		Resolver:  r.lookup,
		BlobStore: r.content,
		Config:    docs.DefaultSyncConfig(),
		// OnSync reports each reconciliation attempt, which is the only way to
		// see the half of live sync that is not gossip.
		OnSync: func(res docs.SyncResult) {
			if res.Err != nil {
				fmt.Fprintf(os.Stderr, "%s: sync with %s: %v\n", r.name, res.Addr.ID.Short(), res.Err)
			}
		},
	})
	if err != nil {
		return fmt.Errorf("%s: start live sync: %w", r.name, err)
	}
	r.live = live
	return nil
}

// put writes one entry, which live sync broadcasts to the topic.
func (r *replica) put(ctx context.Context, namespace docs.NamespaceSecret, key, value string) error {
	hash, err := blobs.WriteBlob(ctx, r.content, []byte(value))
	if err != nil {
		return fmt.Errorf("%s: write content for %q: %w", r.name, key, err)
	}
	id := docs.NewRecordIdentifier(namespace.ID(), r.author.ID(), []byte(key))
	record := docs.NewRecord(hash, uint64(len(value)), uint64(time.Now().UnixMicro()))
	entry := docs.NewSignedEntry(docs.NewEntry(id, record), namespace, r.author)
	if outcome := r.store.Put(entry); !outcome.Inserted() {
		return fmt.Errorf("%s: put %q: an equal or newer entry is already stored", r.name, key)
	}
	return nil
}

// await waits for the store to hold n entries. Live sync delivers on its own
// schedule, so a test of it is a wait rather than a call: there is no request
// whose return means the update has arrived.
func (r *replica) await(ctx context.Context, n int) error {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		if r.store.Len() >= n {
			return nil
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			return fmt.Errorf("%s holds %d entries, want %d: %w", r.name, r.store.Len(), n, ctx.Err())
		}
	}
}

func (r *replica) close(ctx context.Context) {
	if r.live != nil {
		r.live.Close()
	}
	r.router.Shutdown(ctx)
	r.ep.Shutdown(ctx)
}

// seed expands a byte into the 32-byte seed the docs constructors take, so the
// example's keys are the same on every run.
func seed(b byte) [32]byte {
	return sha256.Sum256([]byte{b})
}

// bind binds an endpoint to an ephemeral IPv6 loopback port, then applies opts.
// Loopback keeps the example self-contained: no relay, no DNS, no network.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
}
