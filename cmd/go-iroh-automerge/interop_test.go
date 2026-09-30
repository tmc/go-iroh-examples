package main

// Interoperability with n0's iroh-automerge, the Rust example this is a port
// of. The Rust peer is interop/automerge, which compiles upstream's own
// src/protocol.rs; see interop/README.md.
//
// These tests live outside main_test.go on purpose: internal/catalog reads
// main_test.go to decide what an example needs, and a Rust toolchain is a
// property of the checkout, not of the example.

import (
	"bufio"
	"bytes"
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

	automerge "github.com/automerge/automerge-go"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// Vectors printed by `automerge_peer vectors`, from n0-computer/iroh-examples
// at 6a8cfcdccc6a with Rust automerge 0.7.4.
const (
	rustALPNHex = "69726f682f6175746f6d657267652f32"
	// rustEmptySyncHex is the first sync message an empty Rust document
	// sends. The trailing 02 01 02 lists the message versions Rust supports,
	// a field the automerge-go library (Rust automerge 0.5.0) predates.
	rustEmptySyncHex = "42000001000000020102"
)

// TestRustWireFormat pins the ALPN and the framing to bytes from the Rust
// implementation. It needs no Rust toolchain.
func TestRustWireFormat(t *testing.T) {
	if got := hex.EncodeToString([]byte(alpn)); got != rustALPNHex {
		t.Errorf("alpn = %s, want the Rust ALPN %s", got, rustALPNHex)
	}

	msg, err := hex.DecodeString(rustEmptySyncHex)
	if err != nil {
		t.Fatal(err)
	}
	// Rust frames a message as its length in eight little-endian bytes.
	var frame bytes.Buffer
	frame.Write([]byte{byte(len(msg)), 0, 0, 0, 0, 0, 0, 0})
	frame.Write(msg)
	frame.Write(make([]byte, 8)) // a zero length: "nothing more to send"

	state := automerge.NewSyncState(automerge.New())
	done, err := receiveSyncMessage(&frame, state)
	if err != nil || done {
		t.Fatalf("receiveSyncMessage(Rust frame) = %v, %v; want false, nil", done, err)
	}
	done, err = receiveSyncMessage(&frame, state)
	if err != nil || !done {
		t.Fatalf("receiveSyncMessage(zero length) = %v, %v; want true, nil", done, err)
	}

	var out bytes.Buffer
	if err := sendSyncMessage(&out, nil, false); err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(out.Bytes()); got != "0000000000000000" {
		t.Errorf("done frame = %s, want eight zero bytes", got)
	}
}

// peerBin is the Rust peer built from interop/automerge. Without it the live
// tests skip.
func peerBin(t *testing.T) string {
	t.Helper()
	path := os.Getenv("IROH_EXAMPLE_RUST_AUTOMERGE")
	if path == "" {
		t.Skip("set IROH_EXAMPLE_RUST_AUTOMERGE to the Rust automerge_peer binary to run live interop")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("IROH_EXAMPLE_RUST_AUTOMERGE: %v", err)
	}
	return path
}

// goDoc is the document the Go side starts with. The Rust peer starts with
// rust-0 to rust-2 and its own value for "shared", so after a sync each side
// must hold keys it never wrote and agree on which "shared" won.
func goDoc(t *testing.T) *automerge.Doc {
	t.Helper()
	doc := automerge.New()
	for i := range 3 {
		if err := doc.RootMap().Set(fmt.Sprintf("go-%d", i), fmt.Sprintf("from go %d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := doc.RootMap().Set("shared", "go"); err != nil {
		t.Fatal(err)
	}
	return doc
}

// checkMerged checks that the Go and Rust printouts of the document are the
// same and hold both sides' edits.
func checkMerged(t *testing.T, doc *automerge.Doc, rustOut string) {
	t.Helper()
	var buf bytes.Buffer
	if err := printState(doc, &buf); err != nil {
		t.Fatal(err)
	}
	goState := stateLines(buf.String())
	rustState := stateLines(rustOut)
	if !slices.Equal(goState, rustState) {
		t.Errorf("documents differ after sync\ngo:\n%s\nrust:\n%s",
			strings.Join(goState, "\n"), strings.Join(rustState, "\n"))
	}
	for _, want := range []string{
		`go-0 => "from go 0"`, `go-2 => "from go 2"`,
		`rust-0 => "from rust 0"`, `rust-2 => "from rust 2"`,
	} {
		if !slices.Contains(goState, want) {
			t.Errorf("merged document missing %s\n%s", want, buf.String())
		}
	}
	if len(goState) != 7 {
		t.Errorf("merged document has %d keys, want 7\n%s", len(goState), buf.String())
	}
}

// stateLines returns the lines after "State" in a printout.
func stateLines(out string) []string {
	_, after, ok := strings.Cut(out, "State\n")
	if !ok {
		return nil
	}
	var lines []string
	for line := range strings.Lines(after) {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// TestInteropGoDialsRust initiates a sync with a Rust responder.
func TestInteropGoDialsRust(t *testing.T) {
	bin := peerBin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, bin, "listen")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	r := bufio.NewReader(stdout)
	id, addrPort, err := readPeerAddr(r)
	if err != nil {
		t.Fatalf("reading the Rust peer's address: %v", err)
	}

	// The Rust peer announces 127.0.0.1, which an endpoint on ::1 cannot reach.
	client, err := bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Shutdown(context.Background())

	conn, err := client.Connect(ctx, netaddr.NewEndpointAddr(id, netaddr.IPAddr{Addr: addrPort}), alpn)
	if err != nil {
		t.Fatalf("connecting to the Rust peer: %v", err)
	}
	doc := goDoc(t)
	if err := initiateSync(ctx, conn, doc); err != nil {
		t.Fatalf("initiateSync: %v", err)
	}
	// The Rust responder waits for the dialer to close before it reports.
	conn.CloseWithError(0, "thanks, bye")

	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("rust peer: %v\n%s", err, rest)
	}
	checkMerged(t, doc, string(rest))
}

// TestInteropRustDialsGo responds to a sync initiated by Rust.
func TestInteropRustDialsGo(t *testing.T) {
	bin := peerBin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server, err := bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 0)))
	if err != nil {
		t.Fatal(err)
	}
	doc := goDoc(t)
	synced := make(chan *automerge.Doc, 1)
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		alpn: &automergeHandler{doc: doc, synced: synced},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer router.Shutdown(context.Background())

	addrPort, err := loopbackAddr(server)
	if err != nil {
		t.Fatal(err)
	}
	id := server.ID().Bytes()
	out, err := exec.CommandContext(ctx, bin, "connect",
		hex.EncodeToString(id[:]), addrPort.String()).CombinedOutput()
	if err != nil {
		t.Fatalf("rust peer: %v\n%s", err, out)
	}
	select {
	case <-synced:
	case <-ctx.Done():
		t.Fatalf("Go responder did not finish: %v\nrust:\n%s", ctx.Err(), out)
	}
	checkMerged(t, doc, string(out))
}

// readPeerAddr reads lines up to READY and returns the loopback address
// announced before it.
func readPeerAddr(r *bufio.Reader) (key.EndpointID, netip.AddrPort, error) {
	var (
		id    key.EndpointID
		ap    netip.AddrPort
		found bool
	)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return id, ap, fmt.Errorf("read: %w", err)
		}
		line = strings.TrimSpace(line)
		if line == "READY" {
			break
		}
		fields := strings.Fields(strings.TrimPrefix(line, "ADDR "))
		if found || !strings.HasPrefix(line, "ADDR ") || len(fields) != 2 {
			continue
		}
		a, err := netip.ParseAddrPort(fields[1])
		if err != nil || !a.Addr().IsLoopback() {
			continue
		}
		if id, err = key.ParseEndpointID(fields[0]); err != nil {
			return id, ap, err
		}
		ap, found = a, true
	}
	if !found {
		return id, ap, errors.New("no loopback ADDR line before READY")
	}
	return id, ap, nil
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
