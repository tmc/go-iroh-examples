package main

// Interoperability with n0's iroh-blobs.
//
// The package comment says this is the Go port of iroh-gateway, fetching over
// blobs.ALPN from a provider it reaches by endpoint ID. That provider need not
// be written in Go, and this test puts the gateway in front of one that is
// not: a provider built from the iroh-blobs crate, reached both by its
// configured address and through blob tickets the Rust side printed.
//
// As in go-iroh-framed-messages, it lives outside main_test.go because
// internal/catalog reads that file to decide what an example needs in order to
// run, and what this needs is a Rust toolchain: a property of the checkout
// rather than of the example.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// TestInteropGatewayOverRust serves every gateway route from a Rust provider
// and checks each body against the file the provider was given. The large
// file spans several 16 KiB blocks and ends mid-chunk, and the Range request
// starts inside a block. The gateway fetches whole blobs and slices them
// locally, so that range never becomes a ranged blobs request and known
// go-iroh bug 3 (ExtractBlobRange proofs for ranges starting inside a block)
// is not in play.
func TestInteropGatewayOverRust(t *testing.T) {
	path := os.Getenv("IROH_EXAMPLE_RUST_BLOBS")
	if path == "" {
		t.Skip("set IROH_EXAMPLE_RUST_BLOBS to the Rust blobs_peer binary to run live interop")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	note := []byte("hello gateway over rust iroh-blobs\n")
	big := make([]byte, 100_000)
	rand.NewChaCha8([32]byte{2}).Read(big)
	dir := t.TempDir()
	for name, data := range map[string][]byte{"note.txt": note, "big.bin": big} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// note.txt first: the Rust peer's BLOB_TICKET names the first file.
	lines := startPeer(t, ctx, path, "provide", filepath.Join(dir, "note.txt"), filepath.Join(dir, "big.bin"))
	provider, err := parseAddr(lines["ADDR"])
	if err != nil {
		t.Fatal(err)
	}

	// The example binds ::1; the Rust provider listens on 127.0.0.1.
	client, err := bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Shutdown(context.Background())
	gateway := httptest.NewServer(gatewayHandler{endpoint: client, provider: provider})
	defer gateway.Close()

	root, blobTicket, collTicket := lines["COLLECTION"], lines["BLOB_TICKET"], lines["COLLECTION_TICKET"]
	for _, tt := range []struct {
		path, rangeHeader string
		wantStatus        int
		want              []byte
	}{
		{"/blob/" + lines["BLOB big.bin"], "", 200, big},
		{"/blob/" + lines["BLOB big.bin"], "bytes=20000-40999", 206, big[20000:41000]},
		{"/collection/" + root + "/note.txt", "", 200, note},
		{"/collection/" + root + "/big.bin", "bytes=99000-", 206, big[99000:]},
		{"/ticket/" + blobTicket, "", 200, note},
		{"/ticket/" + collTicket, "", 200, []byte(
			"/ticket/" + collTicket + "/note.txt\n/ticket/" + collTicket + "/big.bin\n")},
		{"/ticket/" + collTicket + "/big.bin", "", 200, big},
	} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, gateway.URL+tt.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if tt.rangeHeader != "" {
			req.Header.Set("Range", tt.rangeHeader)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		name := tt.path[:min(len(tt.path), 40)] + " " + tt.rangeHeader
		if resp.StatusCode != tt.wantStatus {
			t.Errorf("GET %s: %s, want %d: %s", name, resp.Status, tt.wantStatus, bytes.TrimSpace(body[:min(len(body), 200)]))
			continue
		}
		if !bytes.Equal(body, tt.want) {
			t.Errorf("GET %s: %d-byte body differs from the %d bytes the Rust provider holds", name, len(body), len(tt.want))
		}
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
