package main

// Interoperability with n0's iroh-blobs.
//
// The package comment says a blob served here can be fetched by the Rust
// tooling and the reverse. These tests run that claim against the iroh-blobs
// crate itself, in both directions: the Rust getter against this example's
// provider, and this example's getter against the Rust provider.
//
// As in go-iroh-framed-messages, they live outside main_test.go because
// internal/catalog reads that file to decide what an example needs in order to
// run, and what these need is a Rust toolchain: a property of the checkout
// rather than of the example.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// sizes are the blob sizes both directions transfer. The bao tree groups
// 1 KiB chunks into 16 KiB blocks, so these cover a blob of one partial
// chunk, one that crosses a single block boundary, and one spanning several
// blocks; the last two end mid-chunk.
var sizes = []struct {
	name string
	n    int
}{
	{"partial chunk", 27},
	{"crosses one block", 17_000},
	{"several blocks", 100_000},
}

// payload returns n deterministic bytes.
func payload(n int) []byte {
	b := make([]byte, n)
	rand.NewChaCha8([32]byte{byte(n)}).Read(b)
	return b
}

var loopback4 = netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 0)

// peerBin is the Rust peer built from interop/blobs. Without it the live
// tests skip.
func peerBin(t *testing.T) string {
	t.Helper()
	path := os.Getenv("IROH_EXAMPLE_RUST_BLOBS")
	if path == "" {
		t.Skip("set IROH_EXAMPLE_RUST_BLOBS to the Rust blobs_peer binary to run live interop")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("IROH_EXAMPLE_RUST_BLOBS: %v", err)
	}
	return path
}

// TestInteropRustGetsFromGo serves each payload with this example's serve and
// fetches it with iroh-blobs' get_blob, which verifies it chunk by chunk.
func TestInteropRustGetsFromGo(t *testing.T) {
	bin := peerBin(t)
	for _, tt := range sizes {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			data := payload(tt.n)
			store, err := blobs.NewMemStore(data)
			if err != nil {
				t.Fatal(err)
			}
			// The example binds ::1; the Rust peer dials from 127.0.0.1.
			server, err := bind(ctx, iroh.WithBindAddr(loopback4), iroh.WithALPNs(blobs.ALPN))
			if err != nil {
				t.Fatal(err)
			}
			defer server.Shutdown(context.Background())
			served := make(chan error, 1)
			go func() { served <- serve(ctx, server, store) }()

			addr, err := loopbackAddr(server)
			if err != nil {
				t.Fatal(err)
			}
			id := server.ID().Bytes()
			out := filepath.Join(t.TempDir(), "blob")
			b, err := exec.CommandContext(ctx, bin, "get", hex.EncodeToString(id[:]), addr.String(),
				blobs.NewHash(data).String(), out).CombinedOutput()
			if err != nil {
				t.Fatalf("rust get: %v\n%s", err, b)
			}
			if err := <-served; err != nil {
				t.Fatalf("serve: %v", err)
			}
			got, err := os.ReadFile(out)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, data) {
				t.Errorf("Rust fetched %d bytes that differ from the %d served", len(got), len(data))
			}
		})
	}
}

// TestInteropGoGetsFromRust fetches each payload from an iroh-blobs provider
// the way the example does: one stream, blobs.GetBlobBytes, verified against
// the hash the caller already had.
func TestInteropGoGetsFromRust(t *testing.T) {
	bin := peerBin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dir := t.TempDir()
	var files []string
	for _, tt := range sizes {
		p := filepath.Join(dir, fmt.Sprint(tt.n))
		if err := os.WriteFile(p, payload(tt.n), 0o644); err != nil {
			t.Fatal(err)
		}
		files = append(files, p)
	}
	lines := startPeer(t, ctx, bin, append([]string{"provide"}, files...)...)
	provider, err := parseAddr(lines["ADDR"])
	if err != nil {
		t.Fatal(err)
	}

	client, err := bind(ctx, iroh.WithBindAddr(loopback4))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Shutdown(context.Background())

	for _, tt := range sizes {
		t.Run(tt.name, func(t *testing.T) {
			want := payload(tt.n)
			hash := blobs.NewHash(want)
			if got := lines["BLOB "+fmt.Sprint(tt.n)]; got != hash.String() {
				t.Fatalf("Rust hashed %d bytes to %s, want %s", tt.n, got, hash)
			}
			conn, err := client.Connect(ctx, provider, blobs.ALPN)
			if err != nil {
				t.Fatalf("connect to the Rust provider: %v", err)
			}
			defer conn.CloseWithError(0, "")
			s, err := conn.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			got, err := blobs.GetBlobBytes(ctx, s, hash)
			if err != nil {
				t.Fatalf("get blob: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("fetched %d bytes that differ from the %d served", len(got), len(want))
			}
		})
	}
}

// startPeer runs the Rust peer with args and returns what it printed before
// READY: each line keyed by all but its last field, so "BLOB <name> <hash>" is
// lines["BLOB <name>"], except that "ADDR <id> <ip:port>" is lines["ADDR"]. The
// peer is killed when the test ends.
func startPeer(t *testing.T, ctx context.Context, bin string, args ...string) map[string]string {
	t.Helper()
	cmd := exec.CommandContext(ctx, bin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	lines := map[string]string{}
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		line := sc.Text()
		if line == "READY" {
			go io.Copy(io.Discard, stdout)
			return lines
		}
		if rest, ok := strings.CutPrefix(line, "ADDR "); ok {
			lines["ADDR"] = rest
			continue
		}
		if i := strings.LastIndexByte(line, ' '); i > 0 {
			lines[line[:i]] = line[i+1:]
		}
	}
	t.Fatalf("Rust peer exited before READY: %v", sc.Err())
	return nil
}

// parseAddr parses the "<id-hex> <ip:port>" of an ADDR line.
func parseAddr(s string) (netaddr.EndpointAddr, error) {
	idHex, ap, ok := strings.Cut(s, " ")
	if !ok {
		return netaddr.EndpointAddr{}, fmt.Errorf("bad ADDR line %q", s)
	}
	raw, err := hex.DecodeString(idHex)
	if err != nil || len(raw) != 32 {
		return netaddr.EndpointAddr{}, fmt.Errorf("bad endpoint id %q", idHex)
	}
	id, err := key.NewEndpointID([32]byte(raw))
	if err != nil {
		return netaddr.EndpointAddr{}, err
	}
	addr, err := netip.ParseAddrPort(ap)
	if err != nil {
		return netaddr.EndpointAddr{}, err
	}
	return netaddr.NewEndpointAddr(id, netaddr.IPAddr{Addr: addr}), nil
}

// loopbackAddr is the endpoint's own direct address, which is what the Rust
// peer needs to dial it with no relay and no discovery.
func loopbackAddr(ep *iroh.Endpoint) (netip.AddrPort, error) {
	for _, a := range ep.Addr().Addrs() {
		if ip, ok := a.(netaddr.IPAddr); ok && ip.Addr.Addr().IsLoopback() {
			return ip.Addr, nil
		}
	}
	return netip.AddrPort{}, fmt.Errorf("endpoint %s announced no loopback address", ep.ID().Short())
}
