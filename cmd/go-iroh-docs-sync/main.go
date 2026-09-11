// Command go-iroh-docs-sync replicates a multi-writer document between two peers.
//
// The [docs] package is the Go port of iroh-docs. A document is a namespace of
// signed key-value entries: anyone holding the namespace secret may write to
// it, every replica holds the whole entry set, and two replicas reconcile with
// each other over the /iroh-sync/1 ALPN whenever they can talk. No replica is
// the authority, which is what distinguishes a document from the
// request/response protocols the earlier examples serve.
//
// An entry is identified by the triple (namespace, author, key) rather than by
// the key alone. That is what makes the document multi-writer: when two
// authors write the same key, the result is two entries and reconciliation
// keeps both, so the application chooses between them instead of one write
// silently losing a race. Within a single author a later timestamp replaces an
// earlier one, and an entry shadows every longer key of the same author that
// starts with it — the mechanism upstream uses for prefix deletion, and the
// reason document keys are usually kept prefix-free.
//
// Reconciliation is range based. [docs.Sync] and [docs.Handler] exchange
// fingerprints over ranges of the key space ([docs.MemoryStore.Fingerprint])
// and descend only into the ranges whose fingerprints disagree, so a sync
// costs what the difference costs rather than what the document costs. The
// second sync below exchanges the range holding the one entry that changed,
// not the five the replicas agree on. Equal full-range fingerprints are also
// how a caller checks convergence without comparing entry sets.
//
// A [docs.DocTicket] is what a peer needs to join: a capability for the
// namespace — a read capability is the namespace ID, a write capability is the
// namespace secret — plus the addresses to sync with. It is the
// document-shaped analog of the endpoint ticket in go-iroh-tickets, and
// [docs.Register] adds its decoder to an [endpointticket.Registry] for
// programs that accept more than one kind of ticket.
//
// An entry names its value by BLAKE3 hash and length; the bytes themselves
// live in a blob store and move over the iroh-blobs protocol (go-iroh-blobs-transfer),
// not over this one. Syncing a document is therefore cheap and content is
// fetched on demand: below, each replica ends up with all five entries but
// only with the bytes it wrote itself.
//
// This example syncs on demand to keep the sequence observable.
// [docs.StartLiveSync] drives the same reconciliation from an iroh-gossip
// topic instead, so that replicas push entries as they are written.
package main

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"os"
	"slices"
	"time"

	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/docs"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// replica is one peer's view of a document: the entries it knows about, the
// content it holds locally, and the author it signs its own writes with.
type replica struct {
	name    string
	ep      *iroh.Endpoint
	entries *docs.MemoryStore
	content *blobs.MemStore
	author  docs.Author
}

func newReplica(ctx context.Context, name string, authorSeed byte) (*replica, error) {
	ep, err := bind(ctx, iroh.WithALPNs(docs.ALPN))
	if err != nil {
		return nil, fmt.Errorf("%s: bind: %w", name, err)
	}
	content, err := blobs.NewMemStore()
	if err != nil {
		ep.Shutdown(ctx)
		return nil, fmt.Errorf("%s: blob store: %w", name, err)
	}
	author := docs.NewAuthor(seed(authorSeed))
	return &replica{name: name, ep: ep, entries: docs.NewMemoryStore(), content: content, author: author}, nil
}

// put writes value under key, signed by the replica's author and by the
// namespace secret. The value goes to the blob store and the entry records
// only its hash and length; timestamp orders writes by the same author.
func (r *replica) put(ctx context.Context, namespace docs.NamespaceSecret, key, value string, timestamp uint64) (docs.InsertOutcome, error) {
	hash, err := blobs.WriteBlob(ctx, r.content, []byte(value))
	if err != nil {
		return docs.InsertOutcome{}, fmt.Errorf("%s: write content for %q: %w", r.name, key, err)
	}
	id := docs.NewRecordIdentifier(namespace.ID(), r.author.ID(), []byte(key))
	entry := docs.NewSignedEntry(docs.NewEntry(id, docs.NewRecord(hash, uint64(len(value)), timestamp)), namespace, r.author)
	outcome := r.entries.Put(entry)
	if !outcome.Inserted() {
		return outcome, fmt.Errorf("%s: put %q: an equal or newer entry is already stored", r.name, key)
	}
	return outcome, nil
}

// haveContent counts the entries whose content this replica holds locally.
// Reconciliation moves entries, not bytes, so the count lags the entry count
// until the content is fetched over iroh-blobs.
func (r *replica) haveContent(ctx context.Context) (int, error) {
	n := 0
	for _, entry := range r.entries.Entries() {
		status, err := blobs.Status(ctx, r.content, entry.Entry.ContentHash())
		if err != nil {
			return 0, fmt.Errorf("%s: blob status: %w", r.name, err)
		}
		if status.IsComplete() {
			n++
		}
	}
	return n, nil
}

func run(stdout io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The namespace secret is the document. Whoever holds it can write and can
	// hand out write capabilities; the namespace ID derived from it names the
	// document on the wire.
	//
	// Real code generates these with [docs.GenerateNamespaceSecret] and
	// [docs.GenerateAuthor]. Fixed seeds keep this example's output
	// reproducible, because entry order — and with it the ranges a
	// reconciliation splits into — follows from the namespace and author keys.
	namespace := docs.NewNamespaceSecret(seed(0xd0))

	alice, err := newReplica(ctx, "alice", 0xa1)
	if err != nil {
		return err
	}
	defer alice.ep.Shutdown(ctx)

	bob, err := newReplica(ctx, "bob", 0xb2)
	if err != nil {
		return err
	}
	defer bob.ep.Shutdown(ctx)

	// Alice serves the sync protocol. The handler answers reconciliation
	// requests against her store; its BlobStore is what lets her tell a peer
	// which entry content she already holds.
	router, err := iroh.NewRouter(alice.ep, map[string]iroh.ProtocolHandler{
		docs.ALPN: &docs.Handler{
			Store:     alice.entries,
			BlobStore: alice.content,
			Config:    docs.DefaultSyncConfig(),
		},
	}, nil)
	if err != nil {
		return err
	}
	defer router.Shutdown(ctx)

	// Entry timestamps are microseconds since the Unix epoch. Deriving them
	// from one reading keeps the writes below strictly ordered even when they
	// land within the same microsecond.
	base := uint64(time.Now().UnixMicro())

	for i, w := range []struct{ key, value string }{
		{"greeting", "hello from alice"},
		{"menu/coffee", "espresso"},
		{"menu/tea", "genmaicha"},
	} {
		if _, err := alice.put(ctx, namespace, w.key, w.value, base+uint64(i)); err != nil {
			return err
		}
	}

	// The ticket is the whole join: a write capability plus somewhere to sync
	// with. On a real network the address would carry a relay URL; here it is
	// the loopback socket alice is bound to.
	ticket := docs.NewTicket(docs.NewWriteCapability(namespace), []netaddr.EndpointAddr{alice.ep.Addr()})
	wire := ticket.EncodeString()

	// Everything below this point is what bob can do knowing only the string.
	joined, err := docs.ParseTicket(wire)
	if err != nil {
		return err
	}
	capability := joined.Capability()
	secret, writable := capability.Secret()
	fmt.Fprintln(stdout, "ticket kind:", joined.Kind())
	fmt.Fprintln(stdout, "ticket grants write:", writable)
	fmt.Fprintln(stdout, "ticket nodes:", len(joined.Nodes()))
	fmt.Fprintln(stdout, "same namespace:", capability.NamespaceID() == namespace.ID())

	// Bob writes before he has ever met alice. One of his keys collides with
	// hers; because the author is part of the entry identifier, the collision
	// produces two entries rather than a conflict.
	for i, w := range []struct{ key, value string }{
		{"greeting", "hello from bob"},
		{"menu/juice", "grapefruit"},
	} {
		if _, err := bob.put(ctx, secret, w.key, w.value, base+uint64(10+i)); err != nil {
			return err
		}
	}

	fmt.Fprintln(stdout, "alice before sync:", alice.entries.Len(), "entries")
	fmt.Fprintln(stdout, "bob before sync:", bob.entries.Len(), "entries")

	// One sync is symmetric: bob sends what alice is missing and receives what
	// he is missing, in the same stream.
	first, err := syncOnce(ctx, bob, joined)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "sync 1: sent=%d received=%d\n", first.NumSent, first.NumRecv)
	fmt.Fprintln(stdout, "alice after sync:", alice.entries.Len(), "entries")
	fmt.Fprintln(stdout, "bob after sync:", bob.entries.Len(), "entries")
	fmt.Fprintln(stdout, "fingerprints equal:", converged(alice, bob))

	names := map[string]string{
		alice.author.ID().String(): "alice",
		bob.author.ID().String():   "bob",
	}
	printDocument("alice", alice, names, stdout)
	printDocument("bob", bob, names, stdout)

	// The entries travelled; the bytes did not. Each replica can still name
	// every value by hash, and fetches the ones it wants over iroh-blobs.
	aliceContent, err := alice.haveContent(ctx)
	if err != nil {
		return err
	}
	bobContent, err := bob.haveContent(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "content on alice: %d of %d entries\n", aliceContent, alice.entries.Len())
	fmt.Fprintf(stdout, "content on bob: %d of %d entries\n", bobContent, bob.entries.Len())

	// A newer write by the same author under the same key replaces the older
	// entry rather than adding one.
	outcome, err := alice.put(ctx, namespace, "menu/coffee", "cold brew", base+100)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, "older entries replaced:", outcome.Removed())
	fmt.Fprintln(stdout, "alice after update:", alice.entries.Len(), "entries")

	// The second sync reconciles the same five keys, but the fingerprints
	// agree everywhere except the range holding the changed entry, so only
	// that range is exchanged.
	second, err := syncOnce(ctx, bob, joined)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "sync 2: sent=%d received=%d\n", second.NumSent, second.NumRecv)

	updated, ok := bob.entries.GetExact(namespace.ID(), alice.author.ID(), []byte("menu/coffee"), false)
	if !ok {
		return fmt.Errorf("bob is missing alice's menu/coffee entry")
	}
	fmt.Fprintln(stdout, "bob has alice's update:", updated.Entry.ContentHash() == blobs.NewHash([]byte("cold brew")))

	// An entry carries the namespace and author signatures over its own
	// contents, so bob validates alice's write without trusting the peer he
	// received it from.
	fmt.Fprintln(stdout, "signature verifies:", updated.Verify() == nil)
	fmt.Fprintln(stdout, "fingerprints equal:", converged(alice, bob))
	return nil
}

// syncOnce reconciles r's store with the peers named by ticket and returns the
// outcome of the single peer the ticket carries.
func syncOnce(ctx context.Context, r *replica, ticket docs.DocTicket) (docs.SyncOutcome, error) {
	results := docs.SyncTicket(ctx, r.ep, ticket, r.entries, r.content, docs.DefaultSyncConfig(), nil)
	if err := docs.SyncErrors(results); err != nil {
		return docs.SyncOutcome{}, err
	}
	if len(results) != 1 {
		return docs.SyncOutcome{}, fmt.Errorf("%s: synced %d peers, want 1", r.name, len(results))
	}
	return results[0].Outcome, nil
}

// converged reports whether the two replicas hold the same entries. The zero
// [docs.Range] is the whole key space, so this compares one fingerprint per
// replica instead of the entry sets.
func converged(a, b *replica) bool {
	return a.entries.Fingerprint(docs.Range{}) == b.entries.Fingerprint(docs.Range{})
}

// seed returns a 32-byte key seed of repeated b.
func seed(b byte) [32]byte {
	var out [32]byte
	for i := range out {
		out[i] = b
	}
	return out
}

// printDocument prints one replica's entries, sorted so that the output does
// not depend on store iteration order.
func printDocument(title string, r *replica, names map[string]string, stdout io.Writer) {
	rows := make([]string, 0, r.entries.Len())
	for _, entry := range r.entries.Entries() {
		rows = append(rows, fmt.Sprintf("  %-12s %-5s %2d bytes  %s",
			entry.Entry.Key(),
			names[entry.Entry.Author().String()],
			entry.Entry.ContentLen(),
			entry.Entry.ContentHash().Short()))
	}
	slices.Sort(rows)
	fmt.Fprintf(stdout, "%s document:\n", title)
	for _, row := range rows {
		fmt.Fprintln(stdout, row)
	}
}

// bind binds an endpoint to an ephemeral IPv6 loopback port, then applies opts.
// Loopback keeps the example self-contained: no relay, no DNS, no network.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
}
