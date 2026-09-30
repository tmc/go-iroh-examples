package main

// Interoperability with n0's iroh-ping.
//
// The doc comment says this example speaks iroh-ping's protocol byte for byte.
// These tests are what makes that a claim about tested behavior: the ALPN is
// pinned against the iroh_ping crate's own constant, and the two live tests
// run the protocol against a peer whose listener is the crate's Ping handler
// and whose dialer is its Ping::ping.
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

// rustALPNHex is the hex of iroh_ping::ALPN at iroh-ping 1.0.0, printed by
// interop/ping's peer. Regenerate with: ping_peer vectors.
//
// PING and PONG are byte literals inside the crate's functions rather than
// exported constants, so there is nothing to print for them; the live tests
// are what check them, since the crate asserts on both.
const rustALPNHex = "69726f682f70696e672f30"

// TestRustProtocolConstants pins the ALPN, the one constant that decides
// whether this example can reach an iroh-ping endpoint at all. It needs no
// Rust toolchain, so it guards compatibility day to day.
func TestRustProtocolConstants(t *testing.T) {
	want, err := hex.DecodeString(rustALPNHex)
	if err != nil {
		t.Fatal(err)
	}
	if alpn != string(want) {
		t.Errorf("ALPN = %q, want the Rust %q", alpn, want)
	}
}

// peerBin is the Rust peer built from interop/ping. Without it the live tests
// skip: they need a Rust toolchain, and the constant test above already
// covers the ALPN.
func peerBin(t *testing.T) string {
	t.Helper()
	path := os.Getenv("IROH_EXAMPLE_RUST_PING")
	if path == "" {
		t.Skip("set IROH_EXAMPLE_RUST_PING to the Rust ping peer binary to run live interop")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("IROH_EXAMPLE_RUST_PING: %v", err)
	}
	return path
}

// TestRustVectors checks that the pinned hex is still what the crate says,
// so a stale pin shows up the first time somebody runs with the peer built.
func TestRustVectors(t *testing.T) {
	bin := peerBin(t)
	out, err := exec.Command(bin, "vectors").Output()
	if err != nil {
		t.Fatalf("rust peer: %v", err)
	}
	if got, want := strings.TrimSpace(string(out)), "ALPN "+rustALPNHex; got != want {
		t.Errorf("rust vectors = %q, want %q", got, want)
	}
}

// TestInteropGoDialsRust runs this example's ping against the crate's Ping
// handler, which asserts that the request is exactly PING.
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
	t.Logf("rust ping peer %s at %s", id.Short(), addrPort)

	// IPv4 loopback, not the example's default bind: the Rust peer announces
	// 127.0.0.1, and an endpoint bound to ::1 has no route to it.
	client, err := iroh.Bind(ctx, iroh.WithBindAddr(
		netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Shutdown(context.Background())

	conn, err := client.Connect(ctx, netaddr.NewEndpointAddr(id, netaddr.IPAddr{Addr: addrPort}), alpn)
	if err != nil {
		t.Fatalf("connecting to the Rust peer: %v", err)
	}
	// The crate's handler waits for the dialer to close the connection, as
	// its own Ping::ping does once it has the reply.
	defer conn.CloseWithError(0, "")

	reply, err := ping(ctx, conn)
	if err != nil {
		t.Fatalf("pinging the Rust peer: %v", err)
	}
	if reply != response {
		t.Errorf("Rust replied %q, want %q", reply, response)
	}
}

// TestInteropRustDialsGo runs the crate's Ping::ping against this example's
// handler behind a Router, as main does. Ping::ping asserts that the reply is
// exactly PONG, so a wrong reply fails the Rust process.
func TestInteropRustDialsGo(t *testing.T) {
	bin := peerBin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server, err := iroh.Bind(ctx, iroh.WithBindAddr(
		netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 0)))
	if err != nil {
		t.Fatal(err)
	}
	// The router logs a handler's error rather than returning it, so wrap
	// the handler to learn whether it saw a well-formed ping.
	served := make(chan error, 1)
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		alpn: iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
			err := pingHandler{}.Accept(ctx, conn)
			served <- err
			return err
		}),
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
	case err := <-served:
		if err != nil {
			t.Fatalf("serving the Rust dialer: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("the Go handler never saw the Rust ping")
	}
	if !strings.Contains(string(out), "PONG ") {
		t.Errorf("Rust peer reported %q, want a PONG round trip", out)
	}
	t.Logf("rust: %s", strings.TrimSpace(string(out)))
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
