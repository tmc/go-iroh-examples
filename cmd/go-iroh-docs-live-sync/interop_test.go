package main

// Interoperability with iroh-docs live sync, the Rust implementation
// [docs.StartLiveSync] ports.
//
// These tests live outside main_test.go on purpose. internal/catalog reads
// main_test.go to decide what an example needs in order to run, and a Rust
// toolchain is a property of the checkout rather than of the example.
//
// The Rust side is interop/docs's docs_peer, an iroh-docs node built from the
// crates.io iroh-docs, iroh-blobs and iroh-gossip. go-iroh-docs-sync's
// interop_test.go covers one-shot reconciliation; this file covers what live
// sync adds: after the initial sync, entries written on either side travel
// over the gossip topic, and the receiver fetches their content over
// iroh-blobs.

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/docs"
	"github.com/tmc/go-iroh/gossip"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// TestInteropLiveSyncWithRust joins a Go replica and a Rust iroh-docs node to
// one document, with either side bootstrapping to the other, and checks that
// the initial sync catches both up and that live writes on each side then
// reach the other over gossip, content included.
//
// The two join orders cover sync initiation in both directions as well as
// content transfer for reconciled entries.
func TestInteropLiveSyncWithRust(t *testing.T) {
	for _, tc := range []struct {
		name    string
		goJoins bool
	}{
		{"go joins rust", true},
		{"rust joins go", false},
	} {
		t.Run(tc.name, func(t *testing.T) { testLiveSync(t, tc.goJoins) })
	}
}

func testLiveSync(t *testing.T, goJoins bool) {
	bin := docsPeerBin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	namespace := docs.NewNamespaceSecret(seed(0xd0))
	rust := startDocsPeer(t, ctx, bin, namespace, docs.NewAuthor(seed(0xb0)), []string{"rust/before=written before go joined"})

	lookup := iroh.NewMemoryLookup()
	lookup.AddEndpointAddr(rust.addr)
	g := newInteropReplica(t, ctx, "go", 0xa1, lookup)
	if err := g.put(ctx, namespace, "go/before", "written before rust joined"); err != nil {
		t.Fatal(err)
	}
	goID := idHex(g.ep.ID())
	if goJoins {
		if err := g.startLiveSync(ctx, namespace, rust.addr); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := g.startLiveSync(ctx, namespace); err != nil {
			t.Fatal(err)
		}
		addr, err := loopbackAddr(g.ep)
		if err != nil {
			t.Fatal(err)
		}
		rust.send(t, "sync", goID, addr.String())
	}
	rust.expectAll(t, ctx, "EVENT NeighborUp "+goID, "EVENT SyncFinished "+goID+" ok")
	if err := g.await(ctx, 2); err != nil {
		t.Fatalf("initial sync: %v", err)
	}

	// Go writes; Rust receives the entry from gossip, not from a sync, and
	// then fetches its content from Go. Rust can report the content before
	// the insert, so the two events are awaited in either order.
	if err := g.put(ctx, namespace, "go/live", "written while both are live"); err != nil {
		t.Fatal(err)
	}
	rust.expectAll(t, ctx,
		"EVENT InsertRemote "+goID+" "+hex.EncodeToString([]byte("go/live")),
		"EVENT ContentReady "+blobs.NewHash([]byte("written while both are live")).String())

	// Rust writes; Go receives it the same way.
	rust.send(t, "put", "rust/live", "rust-writes-too")
	if err := g.await(ctx, 4); err != nil {
		t.Fatalf("rust's live write never reached go: %v", err)
	}
	if err := g.awaitContent(ctx, "rust/live"); err != nil {
		t.Fatal(err)
	}

	rustEntries := rust.dump(t, ctx)
	var rustRows []string
	for _, e := range rustEntries {
		rustRows = append(rustRows, e.row)
		if e.content == nil {
			t.Errorf("rust did not fetch the content of %q", e.key)
		}
	}
	slices.Sort(rustRows)
	var goRows []string
	for _, e := range g.store.Entries() {
		goRows = append(goRows, entryRow(e))
	}
	slices.Sort(goRows)
	if len(goRows) != 4 || !slices.Equal(goRows, rustRows) {
		t.Errorf("replicas differ\ngo:\n  %s\nrust:\n  %s",
			strings.Join(goRows, "\n  "), strings.Join(rustRows, "\n  "))
	}

	// rust/before reached Go by reconciliation, not gossip. Live sync must
	// fetch its content as well as its entry.
	if err := g.awaitContent(ctx, "rust/before"); err != nil {
		t.Fatal(err)
	}
}

// awaitContent waits until the replica holds the content of the entries
// with the given keys, which live sync downloads on its own.
func (r *replica) awaitContent(ctx context.Context, keys ...string) error {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		var missing []string
		for _, e := range r.store.Entries() {
			if !slices.Contains(keys, string(e.Entry.Key())) {
				continue
			}
			st, err := blobs.Status(ctx, r.content, e.Entry.ContentHash())
			if err != nil || !st.IsComplete() {
				missing = append(missing, string(e.Entry.Key()))
			}
		}
		if len(missing) == 0 {
			return nil
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			return fmt.Errorf("%s is missing content for %q: %w", r.name, missing, ctx.Err())
		}
	}
}

// newInteropReplica is newReplica bound to IPv4 loopback, where the Rust peer
// is, and serving its blob store, from which the Rust peer fetches content.
func newInteropReplica(t *testing.T, ctx context.Context, name string, authorSeed byte, lookup *iroh.MemoryLookup) *replica {
	t.Helper()
	ep, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 0)),
		iroh.WithALPNs(docs.ALPN, gossip.ALPN, blobs.ALPN))
	if err != nil {
		t.Fatal(err)
	}
	content, err := blobs.NewMemStore()
	if err != nil {
		t.Fatal(err)
	}
	g := gossip.NewGossip(ep)
	r := &replica{
		name:    name,
		ep:      ep,
		lookup:  lookup,
		gossip:  g,
		store:   docs.NewMemoryStore(),
		content: content,
		author:  docs.NewAuthor(seed(authorSeed)),
	}
	r.router, err = iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{
		gossip.ALPN: g.Handler(),
		docs.ALPN:   &docs.Handler{Store: r.store, BlobStore: content, Config: docs.DefaultSyncConfig(), Allow: r.allowSync},
		blobs.ALPN:  blobHandler{content},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.close(context.Background()) })
	return r
}

// blobHandler serves a blob store over iroh-blobs.
type blobHandler struct{ store blobs.Store }

func (h blobHandler) Accept(ctx context.Context, conn *iroh.Conn) error {
	return blobs.ServeBlobStreams(ctx, func(ctx context.Context) (blobs.BidiStream, error) {
		return conn.AcceptStream(ctx)
	}, h.store)
}

// entryRow formats e the way docs_peer prints an entry, without the content.
func entryRow(e docs.SignedEntry) string {
	author := e.Entry.Author().Bytes()
	asig := e.Signature.Author.Bytes()
	nsig := e.Signature.Namespace.Bytes()
	return fmt.Sprintf("%x %x %s %d %d %x%x",
		author, e.Entry.Key(), e.Entry.ContentHash(), e.Entry.ContentLen(),
		e.Entry.Timestamp(), asig, nsig)
}

// The rest drives docs_peer and is the same as in go-iroh-docs-sync's
// interop_test.go; each example is its own main package.

func docsPeerBin(t *testing.T) string {
	t.Helper()
	path := os.Getenv("IROH_EXAMPLE_RUST_DOCS")
	if path == "" {
		t.Skip("set IROH_EXAMPLE_RUST_DOCS to the Rust docs_peer binary to run live interop")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("IROH_EXAMPLE_RUST_DOCS: %v", err)
	}
	return path
}

type docsPeer struct {
	addr  netaddr.EndpointAddr
	stdin io.Writer
	lines chan string
}

func startDocsPeer(t *testing.T, ctx context.Context, bin string, namespace docs.NamespaceSecret, author docs.Author, kvs []string) *docsPeer {
	t.Helper()
	ns, a := namespace.Bytes(), author.Bytes()
	cmd := exec.CommandContext(ctx, bin, append([]string{hex.EncodeToString(ns[:]), hex.EncodeToString(a[:])}, kvs...)...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stdin.Close()
		done := make(chan struct{})
		go func() { cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			cmd.Process.Kill()
			<-done
		}
	})
	p := &docsPeer{stdin: stdin, lines: make(chan string, 256)}
	go func() {
		defer close(p.lines)
		sc := bufio.NewScanner(stdout)
		sc.Buffer(nil, 1<<20)
		for sc.Scan() {
			p.lines <- sc.Text()
		}
	}()
	id, ap, err := p.readAddr(ctx)
	if err != nil {
		t.Fatalf("reading the Rust peer's address: %v", err)
	}
	p.addr = netaddr.NewEndpointAddr(id, netaddr.IPAddr{Addr: ap})
	t.Logf("rust docs peer %s at %s", id.Short(), ap)
	return p
}

func (p *docsPeer) send(t *testing.T, args ...string) {
	t.Helper()
	if _, err := fmt.Fprintln(p.stdin, strings.Join(args, " ")); err != nil {
		t.Fatalf("write to rust peer: %v", err)
	}
}

func (p *docsPeer) next(ctx context.Context) (string, error) {
	select {
	case line, ok := <-p.lines:
		if !ok {
			return "", errors.New("rust peer exited")
		}
		return line, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (p *docsPeer) expect(t *testing.T, ctx context.Context, prefix string) string {
	t.Helper()
	for {
		line, err := p.next(ctx)
		if err != nil {
			t.Fatalf("waiting for %q: %v", prefix, err)
		}
		t.Logf("rust: %s", line)
		if strings.HasPrefix(line, prefix) {
			return line
		}
		if strings.HasPrefix(line, "EVENT SyncFinished ") && strings.HasPrefix(prefix, "EVENT SyncFinished ") {
			t.Fatalf("rust sync failed: %s", line)
		}
	}
}

// expectAll waits until the peer has printed a line starting with each of
// prefixes, in any order.
func (p *docsPeer) expectAll(t *testing.T, ctx context.Context, prefixes ...string) {
	t.Helper()
	pending := slices.Clone(prefixes)
	for len(pending) > 0 {
		line, err := p.next(ctx)
		if err != nil {
			t.Fatalf("waiting for %q: %v", pending, err)
		}
		t.Logf("rust: %s", line)
		pending = slices.DeleteFunc(pending, func(prefix string) bool {
			return strings.HasPrefix(line, prefix)
		})
	}
}

type rustEntry struct {
	row     string
	key     string
	content []byte
}

func (p *docsPeer) dump(t *testing.T, ctx context.Context) []rustEntry {
	t.Helper()
	p.send(t, "dump")
	var out []rustEntry
	for {
		line, err := p.next(ctx)
		if err != nil {
			t.Fatalf("reading dump: %v", err)
		}
		if line == "END" {
			return out
		}
		rest, ok := strings.CutPrefix(line, "ENTRY ")
		if !ok {
			t.Logf("rust: %s", line)
			continue
		}
		f := strings.Fields(rest)
		if len(f) != 7 {
			t.Fatalf("malformed entry line %q", line)
		}
		k, err := hex.DecodeString(f[1])
		if err != nil {
			t.Fatal(err)
		}
		e := rustEntry{row: strings.Join(f[:6], " "), key: string(k)}
		if f[6] != "-" {
			if e.content, err = hex.DecodeString(f[6]); err != nil {
				t.Fatal(err)
			}
		}
		out = append(out, e)
	}
}

func (p *docsPeer) readAddr(ctx context.Context) (key.EndpointID, netip.AddrPort, error) {
	for {
		line, err := p.next(ctx)
		if err != nil {
			return key.EndpointID{}, netip.AddrPort{}, err
		}
		if line == "READY" {
			return key.EndpointID{}, netip.AddrPort{}, errors.New("no loopback ADDR line before READY")
		}
		f := strings.Fields(line)
		if len(f) != 3 || f[0] != "ADDR" {
			continue
		}
		ap, err := netip.ParseAddrPort(f[2])
		if err != nil || !ap.Addr().IsLoopback() {
			continue
		}
		id, err := key.ParseEndpointID(f[1])
		if err != nil {
			return key.EndpointID{}, netip.AddrPort{}, err
		}
		for line != "READY" {
			if line, err = p.next(ctx); err != nil {
				return key.EndpointID{}, netip.AddrPort{}, err
			}
		}
		return id, ap, nil
	}
}

func idHex(id key.EndpointID) string {
	b := id.Bytes()
	return hex.EncodeToString(b[:])
}

func loopbackAddr(ep *iroh.Endpoint) (netip.AddrPort, error) {
	for _, a := range ep.Addr().Addrs() {
		ip, ok := a.(netaddr.IPAddr)
		if ok && ip.Addr.Addr().IsLoopback() {
			return ip.Addr, nil
		}
	}
	return netip.AddrPort{}, errors.New("endpoint has no loopback address")
}
