package main

// Interoperability with the Rust implementation this example is a port of.
//
// These tests live outside main_test.go on purpose. internal/catalog reads
// main_test.go to decide what an example needs in order to run, and by that
// measure this example needs nothing: its own test runs on loopback. What is
// needed here is a Rust toolchain, which is a property of the checkout rather
// than of the example, so it is kept out of the catalog's view.

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"net/netip"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// wire vectors produced by the Rust implementation, from
// n0-computer/iroh-examples/framed-messages at iroh 1.0. Each is one complete
// frame: a big-endian uint32 length followed by the postcard encoding of a
// Move. postcard writes a (u8, u8) tuple as two bytes, which is why a move is
// four bytes and not a tagged structure.
var rustFrames = map[string]struct {
	mv   move
	wire string
}{
	"white e2e4": {move{From: file{4, 2}, To: file{4, 4}}, "0000000404020404"},
	"white d2d3": {move{From: file{3, 2}, To: file{3, 3}}, "0000000403020303"},
	"black f7f6": {move{From: file{5, 7}, To: file{5, 6}}, "0000000405070506"},
	"black f8f7": {move{From: file{5, 8}, To: file{5, 7}}, "0000000405080507"},
}

// TestRustWireFormat pins this example's framing to bytes the Rust
// implementation produced. It needs no Rust toolchain, so it runs everywhere
// and is the check that actually guards interoperability day to day: a change
// to the frame header or to the move encoding fails here rather than silently
// ending compatibility with a peer nobody tests against.
func TestRustWireFormat(t *testing.T) {
	const rustALPN = "iroh/examples/messages/0"
	if alpn != rustALPN {
		t.Errorf("alpn = %q, want the Rust ALPN %q", alpn, rustALPN)
	}
	for name, tc := range rustFrames {
		var buf strings.Builder
		if err := sendMove(hexWriter{&buf}, tc.mv); err != nil {
			t.Fatalf("%s: sendMove: %v", name, err)
		}
		if got := buf.String(); got != tc.wire {
			t.Errorf("%s: frame = %s, want %s (Rust)", name, got, tc.wire)
		}
		raw, err := hex.DecodeString(tc.wire)
		if err != nil {
			t.Fatal(err)
		}
		got, err := recvMove(strings.NewReader(string(raw)))
		if err != nil {
			t.Fatalf("%s: recvMove on the Rust frame: %v", name, err)
		}
		if got != tc.mv {
			t.Errorf("%s: decoded %+v, want %+v", name, got, tc.mv)
		}
	}
}

// hexWriter renders what is written to it as hex, so a frame can be compared
// with a vector as text and a mismatch reads as bytes rather than as escapes.
type hexWriter struct{ b *strings.Builder }

func (w hexWriter) Write(p []byte) (int, error) {
	w.b.WriteString(hex.EncodeToString(p))
	return len(p), nil
}

// peerBin is the Rust peer built from rust-interop, which speaks this ALPN
// against a real iroh endpoint. Without it the live tests skip: they need a
// Rust toolchain, and the vector test above already covers the wire format.
func peerBin(t *testing.T) string {
	t.Helper()
	path := os.Getenv("IROH_EXAMPLE_RUST_PEER")
	if path == "" {
		t.Skip("set IROH_EXAMPLE_RUST_PEER to the Rust chess peer binary to run live interop")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("IROH_EXAMPLE_RUST_PEER: %v", err)
	}
	return path
}

// TestInteropGoDialsRust plays this example's white against a Rust black.
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

	id, addrPort, err := readPeerAddr(stdout)
	if err != nil {
		t.Fatalf("reading the Rust peer's address: %v", err)
	}
	t.Logf("rust peer %s at %s", id.Short(), addrPort)

	// Bind IPv4 loopback rather than using the example's bind, which takes
	// IPv6: the Rust peer announces 127.0.0.1, and an endpoint bound to ::1
	// has no route to it.
	client, err := iroh.Bind(ctx, iroh.WithBindAddr(
		netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Shutdown(context.Background())

	addr := netaddr.NewEndpointAddr(id, netaddr.IPAddr{Addr: addrPort})
	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		t.Fatalf("connecting to the Rust peer: %v", err)
	}
	defer conn.CloseWithError(0, "")

	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The same two moves the example's own client half plays, against Rust.
	for _, step := range []struct{ send, want move }{
		{move{From: file{4, 2}, To: file{4, 4}}, move{From: file{5, 7}, To: file{5, 6}}},
		{move{From: file{3, 2}, To: file{3, 3}}, move{From: file{5, 8}, To: file{5, 7}}},
	} {
		if err := sendMove(s, step.send); err != nil {
			t.Fatalf("sendMove: %v", err)
		}
		got, err := recvMove(s)
		if err != nil {
			t.Fatalf("recvMove: %v", err)
		}
		if got != step.want {
			t.Errorf("Rust replied %+v, want %+v", got, step.want)
		}
	}
}

// TestInteropRustDialsGo serves this example's black to a Rust white.
func TestInteropRustDialsGo(t *testing.T) {
	bin := peerBin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server, err := bind(ctx, iroh.WithALPNs(alpn))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())

	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		alpn: chessHandler{},
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
	// Rust prints what it received; those are this example's replies as black.
	for _, want := range []string{
		"Move { from: (5, 7), to: (5, 6) }",
		"Move { from: (5, 8), to: (5, 7) }",
	} {
		if !strings.Contains(string(out), want) {
			t.Errorf("Rust peer did not report %s\n%s", want, out)
		}
	}
}

// readPeerAddr reads the peer's announced loopback address, which it prints
// before READY.
func readPeerAddr(r interface{ Read([]byte) (int, error) }) (key.EndpointID, netip.AddrPort, error) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := sc.Text()
		if line == "READY" {
			break
		}
		rest, ok := strings.CutPrefix(line, "ADDR ")
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) != 2 {
			continue
		}
		ap, err := netip.ParseAddrPort(fields[1])
		if err != nil || !ap.Addr().IsLoopback() {
			continue
		}
		id, err := key.ParseEndpointID(fields[0])
		if err != nil {
			return key.EndpointID{}, netip.AddrPort{}, err
		}
		return id, ap, nil
	}
	if err := sc.Err(); err != nil {
		return key.EndpointID{}, netip.AddrPort{}, err
	}
	return key.EndpointID{}, netip.AddrPort{}, errors.New("no loopback ADDR line before READY")
}

// loopbackAddr is the endpoint's own loopback address, which is what a peer on
// this host can dial.
func loopbackAddr(ep *iroh.Endpoint) (netip.AddrPort, error) {
	for _, a := range ep.Addr().Addrs() {
		ip, ok := a.(netaddr.IPAddr)
		if ok && ip.Addr.Addr().IsLoopback() {
			return ip.Addr, nil
		}
	}
	return netip.AddrPort{}, errors.New("endpoint has no loopback address")
}
