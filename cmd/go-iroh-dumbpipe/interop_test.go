package main

// Interoperability with n0's dumbpipe.
//
// The README says this example is wire-compatible with Rust dumbpipe. These
// tests are what makes that a claim about tested behavior: the constants are
// pinned against the dumbpipe crate's own, and the two live tests run the
// protocol against a peer built from it.
//
// As in go-iroh-framed-messages, they live outside main_test.go because
// internal/catalog reads that file to decide what an example needs in order to
// run, and what these need is a Rust toolchain: a property of the checkout
// rather than of the example.

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
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// Hex of dumbpipe::ALPN and dumbpipe::HANDSHAKE at dumbpipe 0.39.0, printed by
// interop's vectors binary. Regenerate with: cargo run --bin vectors.
const (
	rustALPNHex      = "44554d42504950455630"
	rustHandshakeHex = "68656c6c6f"
)

// TestRustProtocolConstants pins the two constants that decide whether this
// example can talk to dumbpipe at all. It needs no Rust toolchain, so it is the
// check that guards compatibility day to day: renaming the ALPN or changing the
// handshake fails here rather than in a shell somebody runs by hand once.
func TestRustProtocolConstants(t *testing.T) {
	for _, tt := range []struct{ name, got, wantHex string }{
		{"ALPN", dumbpipeALPN, rustALPNHex},
		{"handshake", handshake, rustHandshakeHex},
	} {
		want, err := hex.DecodeString(tt.wantHex)
		if err != nil {
			t.Fatal(err)
		}
		if tt.got != string(want) {
			t.Errorf("%s = %q, want the Rust %q", tt.name, tt.got, want)
		}
	}
}

// peerBin is the Rust peer built from interop/, which speaks dumbpipe's
// protocol against a real iroh endpoint. Without it the live tests skip: they
// need a Rust toolchain, and the constant test above already covers the wire.
func peerBin(t *testing.T) string {
	t.Helper()
	path := os.Getenv("IROH_EXAMPLE_RUST_DUMBPIPE")
	if path == "" {
		t.Skip("set IROH_EXAMPLE_RUST_DUMBPIPE to the Rust dumbpipe peer binary to run live interop")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("IROH_EXAMPLE_RUST_DUMBPIPE: %v", err)
	}
	return path
}

// TestInteropGoDialsRust runs this example's dialer half against a Rust
// listener: the handshake goes first, then the payload, and the reply comes
// back on the same stream.
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
	t.Logf("rust dumbpipe peer %s at %s", id.Short(), addrPort)

	// IPv4 loopback, not the example's default bind: the Rust peer announces
	// 127.0.0.1, and an endpoint bound to ::1 has no route to it.
	client, err := iroh.Bind(ctx, iroh.WithBindAddr(
		netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Shutdown(context.Background())

	conn, err := client.Connect(ctx, netaddr.NewEndpointAddr(id, netaddr.IPAddr{Addr: addrPort}), dumbpipeALPN)
	if err != nil {
		t.Fatalf("connecting to the Rust peer: %v", err)
	}
	defer conn.CloseWithError(0, "")
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte(handshake + "from go\n")); err != nil {
		t.Fatal(err)
	}
	if err := closeWrite(stream); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("reading the Rust peer's reply: %v", err)
	}
	if want := "rust echo: from go\n"; string(got) != want {
		t.Errorf("Rust replied %q, want %q", got, want)
	}
}

// TestInteropRustDialsGo runs this example's listener half against a Rust
// dialer, including readHandshake, which is the check dumbpipe's listener makes
// before it pipes anything.
func TestInteropRustDialsGo(t *testing.T) {
	bin := peerBin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 0)),
		iroh.WithALPNs(dumbpipeALPN))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())

	served := make(chan error, 1)
	go func() { served <- serveOneEcho(ctx, server) }()

	addrPort, err := loopbackAddr(server)
	if err != nil {
		t.Fatal(err)
	}
	id := server.ID().Bytes()
	out, err := exec.CommandContext(ctx, bin, "connect",
		hex.EncodeToString(id[:]), addrPort.String(), "from rust\n").CombinedOutput()
	if err != nil {
		t.Fatalf("rust peer: %v\n%s", err, out)
	}
	if err := <-served; err != nil {
		t.Fatalf("serving the Rust dialer: %v", err)
	}
	if want := "go echo: from rust\n"; !strings.Contains(string(out), want) {
		t.Errorf("Rust peer reported %q, want it to contain %q", out, want)
	}
}

// serveOneEcho accepts one dumbpipe stream, checks the handshake the way the
// example's listener does, and echoes the payload back with a prefix.
func serveOneEcho(ctx context.Context, ep *iroh.Endpoint) error {
	conn, err := ep.Accept(ctx)
	if err != nil {
		return err
	}
	defer conn.CloseWithError(0, "")
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	if err := readHandshake(stream); err != nil {
		return err
	}
	body, err := io.ReadAll(stream)
	if err != nil {
		return err
	}
	if _, err := stream.Write([]byte("go echo: " + string(body))); err != nil {
		return err
	}
	if err := closeWrite(stream); err != nil {
		return err
	}
	// Half-closing the stream does not deliver the reply; closing the
	// connection on the way out of this function would cut it off while the
	// dialer is still reading, which the Rust peer reports as "connection
	// lost: closed by peer: 0". Wait for the dialer to close instead, which
	// it does once it has read to EOF.
	select {
	case <-conn.Context().Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// readPeerAddr reads the peer's announced loopback address, which it prints
// before READY.
func readPeerAddr(r io.Reader) (key.EndpointID, netip.AddrPort, error) {
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
		raw, err := hex.DecodeString(fields[0])
		if err != nil {
			return key.EndpointID{}, netip.AddrPort{}, err
		}
		var b [32]byte
		copy(b[:], raw)
		id, err := key.NewEndpointID(b)
		if err != nil {
			return key.EndpointID{}, netip.AddrPort{}, err
		}
		ap, err := netip.ParseAddrPort(fields[1])
		if err != nil {
			return key.EndpointID{}, netip.AddrPort{}, err
		}
		return id, ap, nil
	}
	if err := sc.Err(); err != nil {
		return key.EndpointID{}, netip.AddrPort{}, err
	}
	return key.EndpointID{}, netip.AddrPort{}, errors.New("peer printed no ADDR line")
}

// loopbackAddr is the endpoint's own direct address, which is what the Rust
// peer needs to dial it with no relay and no discovery.
func loopbackAddr(ep *iroh.Endpoint) (netip.AddrPort, error) {
	for _, a := range ep.Addr().Addrs() {
		ip, ok := a.(netaddr.IPAddr)
		if !ok {
			continue
		}
		if ip.Addr.Addr().IsLoopback() {
			return ip.Addr, nil
		}
	}
	return netip.AddrPort{}, fmt.Errorf("endpoint %s announced no loopback address", ep.ID().Short())
}
