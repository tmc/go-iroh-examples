package main

// Interoperability with iroh-docs, the Rust implementation the docs package
// ports.
//
// These tests live outside main_test.go on purpose. internal/catalog reads
// main_test.go to decide what an example needs in order to run, and a Rust
// toolchain is a property of the checkout rather than of the example.
//
// The Rust side is interop/docs's docs_peer: an iroh-docs node built from the
// crates.io iroh-docs, iroh-blobs and iroh-gossip, with an in-memory store.
// Both sides write distinct entries into one namespace, one side syncs with
// the other, and afterwards each must hold the union, with the same
// signatures, authors and timestamps. The Rust node also fetches the content
// of the entries it received over iroh-blobs, as any iroh-docs node does, so
// the Go side serves its blob store too.

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
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// Entries each side writes before the sync. "greeting" is written by both,
// which must survive as two entries because the author is part of the entry
// identifier.
var (
	goWrites   = []string{"greeting=hello from go", "menu/coffee=espresso"}
	rustWrites = []string{"greeting=hello from rust", "menu/tea=genmaicha"}
)

// TestInteropGoSyncsWithRust has the Go replica initiate a sync with a Rust
// iroh-docs node, through the example's syncOnce.
//
// This checks the Go sync initiator against iroh-docs' Init, Sync and Abort
// wire messages.
func TestInteropGoSyncsWithRust(t *testing.T) {
	bin := docsPeerBin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	namespace := docs.NewNamespaceSecret(seed(0xd0))
	g := newInteropReplica(t, ctx, "go", 0xa1, namespace.ID())
	g.write(t, ctx, namespace, goWrites)
	rust := startDocsPeer(t, ctx, bin, namespace, docs.NewAuthor(seed(0xb2)), rustWrites)

	ticket := docs.NewTicket(docs.NewWriteCapability(namespace), []netaddr.EndpointAddr{rust.addr})
	out, err := syncOnce(ctx, g.replica, ticket)
	if err != nil {
		t.Fatalf("sync with rust: %v", err)
	}
	t.Logf("sync: sent=%d received=%d", out.NumSent, out.NumRecv)
	rust.expect(t, ctx, "EVENT SyncFinished "+idHex(g.ep.ID())+" ok")
	rust.expect(t, ctx, "EVENT PendingContentReady")
	checkConverged(t, ctx, g, rust)
}

// TestInteropRustSyncsWithGo has a Rust iroh-docs node initiate a sync with
// the Go replica, which serves [docs.Handler] as the example's alice does.
func TestInteropRustSyncsWithGo(t *testing.T) {
	bin := docsPeerBin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	namespace := docs.NewNamespaceSecret(seed(0xd0))
	g := newInteropReplica(t, ctx, "go", 0xa1, namespace.ID())
	g.write(t, ctx, namespace, goWrites)
	rust := startDocsPeer(t, ctx, bin, namespace, docs.NewAuthor(seed(0xb2)), rustWrites)

	addr, err := loopbackAddr(g.ep)
	if err != nil {
		t.Fatal(err)
	}
	rust.send(t, "sync", idHex(g.ep.ID()), addr.String())
	rust.expect(t, ctx, "EVENT SyncFinished "+idHex(g.ep.ID())+" ok")
	rust.expect(t, ctx, "EVENT PendingContentReady")
	checkConverged(t, ctx, g, rust)
}

// checkConverged checks that g and the Rust node hold the same entries, the
// union of what each wrote, and that each side holds or can fetch the content
// of all of them: Rust by having downloaded Go's content during the sync, Go
// by fetching Rust's content over iroh-blobs now.
func checkConverged(t *testing.T, ctx context.Context, g *interopReplica, rust *docsPeer) {
	t.Helper()
	rustEntries := rust.dump(t, ctx)
	var rustRows []string
	for _, e := range rustEntries {
		rustRows = append(rustRows, e.row)
	}
	slices.Sort(rustRows)
	var goRows []string
	for _, e := range g.entries.Entries() {
		if err := e.Verify(); err != nil {
			t.Errorf("go holds an entry that does not verify: %v", err)
		}
		goRows = append(goRows, entryRow(e))
	}
	slices.Sort(goRows)
	if want := len(goWrites) + len(rustWrites); len(goRows) != want {
		t.Errorf("go holds %d entries, want %d", len(goRows), want)
	}
	if !slices.Equal(goRows, rustRows) {
		t.Errorf("replicas differ\ngo:\n  %s\nrust:\n  %s",
			strings.Join(goRows, "\n  "), strings.Join(rustRows, "\n  "))
	}

	for _, e := range rustEntries {
		if e.content == nil {
			t.Errorf("rust did not fetch the content of %q", e.key)
		} else if blobs.NewHash(e.content) != e.hash {
			t.Errorf("rust holds content for %q that does not match its hash", e.key)
		}
	}

	conn, err := g.ep.Connect(ctx, rust.addr, blobs.ALPN)
	if err != nil {
		t.Fatalf("connect to rust over iroh-blobs: %v", err)
	}
	defer conn.CloseWithError(0, "")
	for _, e := range g.entries.Entries() {
		hash := e.Entry.ContentHash()
		if st, err := blobs.Status(ctx, g.content, hash); err == nil && st.IsComplete() {
			continue
		}
		s, err := conn.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		b, err := blobs.GetBlobBytes(ctx, s, hash)
		if err != nil {
			t.Errorf("fetch content of %q from rust: %v", e.Entry.Key(), err)
			continue
		}
		if uint64(len(b)) != e.Entry.ContentLen() {
			t.Errorf("content of %q: got %d bytes, want %d", e.Entry.Key(), len(b), e.Entry.ContentLen())
		}
	}
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

// interopReplica is a Go replica bound to IPv4 loopback, serving the docs
// protocol and its blob store. The example's own replicas bind ::1, which has
// no route to the Rust peer's 127.0.0.1.
type interopReplica struct {
	*replica
	router *iroh.Router
}

func newInteropReplica(t *testing.T, ctx context.Context, name string, authorSeed byte, namespace docs.NamespaceID) *interopReplica {
	t.Helper()
	ep, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 0)),
		iroh.WithALPNs(docs.ALPN, blobs.ALPN))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ep.Shutdown(context.Background()) })
	content, err := blobs.NewMemStore()
	if err != nil {
		t.Fatal(err)
	}
	r := &replica{name: name, ep: ep, entries: docs.NewMemoryStore(), content: content, author: docs.NewAuthor(seed(authorSeed))}
	router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{
		docs.ALPN:  &docs.Handler{Store: r.entries, BlobStore: r.content, Config: docs.DefaultSyncConfig(), Allow: func(ns docs.NamespaceID, _ key.EndpointID) bool { return ns == namespace }},
		blobs.ALPN: blobHandler{r.content},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { router.Shutdown(context.Background()) })
	return &interopReplica{replica: r, router: router}
}

func (r *interopReplica) write(t *testing.T, ctx context.Context, namespace docs.NamespaceSecret, kvs []string) {
	t.Helper()
	base := uint64(time.Now().UnixMicro())
	for i, kv := range kvs {
		k, v, _ := strings.Cut(kv, "=")
		if _, err := r.put(ctx, namespace, k, v, base+uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
}

// blobHandler serves the replica's content over iroh-blobs, which is how an
// iroh-docs peer fetches the values behind the entries it synced.
type blobHandler struct{ store blobs.Store }

func (h blobHandler) Accept(ctx context.Context, conn *iroh.Conn) error {
	return blobs.ServeBlobStreams(ctx, func(ctx context.Context) (blobs.BidiStream, error) {
		return conn.AcceptStream(ctx)
	}, h.store)
}

// docsPeerBin is the Rust docs_peer built from interop/docs.
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

// docsPeer is a running docs_peer process.
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

// next returns the next line the peer printed, logging it.
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

// expect waits for a line starting with prefix and returns it.
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

// rustEntry is one entry as docs_peer printed it.
type rustEntry struct {
	row     string // author, key, hash, length, timestamp, signature
	key     string
	hash    blobs.Hash
	content []byte // nil if the Rust node does not hold it
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
		hb, err := hex.DecodeString(f[2])
		if err != nil || len(hb) != blobs.HashSize {
			t.Fatalf("malformed hash in %q", line)
		}
		e := rustEntry{row: strings.Join(f[:6], " "), key: string(k), hash: blobs.Hash(hb)}
		if f[6] != "-" {
			if e.content, err = hex.DecodeString(f[6]); err != nil {
				t.Fatal(err)
			}
		}
		out = append(out, e)
	}
}

// readAddr reads the peer's announced loopback address, printed before READY.
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
		// Consume through READY so later reads start at the first event.
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

// loopbackAddr is the endpoint's own loopback address.
func loopbackAddr(ep *iroh.Endpoint) (netip.AddrPort, error) {
	for _, a := range ep.Addr().Addrs() {
		ip, ok := a.(netaddr.IPAddr)
		if ok && ip.Addr.Addr().IsLoopback() {
			return ip.Addr, nil
		}
	}
	return netip.AddrPort{}, errors.New("endpoint has no loopback address")
}
