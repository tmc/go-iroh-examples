package main

// Interoperability with Rust iroh-gossip, which this example claims to speak.
//
// These tests live outside main_test.go on purpose. internal/catalog reads
// main_test.go to decide what an example needs in order to run, and by that
// measure this example needs nothing: its own test runs on loopback. What is
// needed here is a Rust toolchain, which is a property of the checkout rather
// than of the example, so it is kept out of the catalog's view.

import (
	"bufio"
	"context"
	"crypto/sha256"
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

	"github.com/tmc/go-iroh/gossip"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// TestInterop puts one Go node and one Rust iroh-gossip node on a topic and
// has each broadcast once; each must receive the other's message. Which side
// joins which matters to the protocol — the joiner sends the HyParView Join
// and the other side answers with a Neighbor — so both are tried.
func TestInterop(t *testing.T) {
	bin := gossipBin(t)
	for _, tc := range []struct {
		name     string
		rustJoin bool // Rust dials Go; otherwise Go dials Rust
	}{
		{"rust joins go", true},
		{"go joins rust", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			topic := gossip.TopicID(sha256.Sum256([]byte("go-iroh-examples/gossip-topic/interop/" + tc.name)))

			rust := startRust(t, ctx, bin, "topic", hex.EncodeToString(topic[:]))

			// Bind IPv4 rather than using the example's bind, which takes
			// IPv6: the Rust peer announces 127.0.0.1, and an endpoint bound
			// to ::1 has no route to it.
			ep, err := iroh.Bind(ctx, iroh.WithBindAddr(
				netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 0)))
			if err != nil {
				t.Fatal(err)
			}
			names := map[key.EndpointID]string{rust.id: "rust"}
			g, err := serve(ctx, ep, "go", names)
			if err != nil {
				t.Fatal(err)
			}
			defer g.close(context.Background())
			if err := g.subscribe(ctx, topic); err != nil {
				t.Fatal(err)
			}

			if tc.rustJoin {
				goAddr, err := loopbackAddr(ep)
				if err != nil {
					t.Fatal(err)
				}
				rust.send(t, "join %s %s", idHex(ep.ID()), goAddr)
			} else {
				rust := netaddr.NewEndpointAddr(rust.id, netaddr.IPAddr{Addr: rust.addr})
				if err := g.topic.JoinPeers(ctx, []netaddr.EndpointAddr{rust}); err != nil {
					t.Fatalf("join rust: %v", err)
				}
			}

			// Both sides must see the link before broadcasting, or a message
			// can go out while the overlay is still empty.
			if err := g.awaitNeighbor(ctx, "rust"); err != nil {
				t.Fatal(err)
			}
			if _, err := rust.await(ctx, "UP "+idHex(ep.ID())); err != nil {
				t.Fatal(err)
			}

			const fromGo, fromRust = "hello from go", "hello from rust"
			if err := g.topic.Broadcast(ctx, []byte(fromGo)); err != nil {
				t.Fatalf("broadcast: %v", err)
			}
			rust.send(t, "broadcast %s", fromRust)

			ev, err := g.awaitMessage(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if string(ev.Content) != fromRust || ev.DeliveredFrom != rust.id {
				t.Errorf("go received %q from %s, want %q from rust", ev.Content, ev.DeliveredFrom.Short(), fromRust)
			}
			if _, err := rust.await(ctx, fmt.Sprintf("RECV %s %s", idHex(ep.ID()), fromGo)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// gossipBin is the Rust peer built from interop/gossip. Without it the live
// tests skip: they need a Rust toolchain.
func gossipBin(t *testing.T) string {
	t.Helper()
	path := os.Getenv("IROH_EXAMPLE_RUST_GOSSIP")
	if path == "" {
		t.Skip("set IROH_EXAMPLE_RUST_GOSSIP to the Rust gossip_peer binary to run live interop")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("IROH_EXAMPLE_RUST_GOSSIP: %v", err)
	}
	return path
}

// rustPeer is a running gossip_peer: its announced address, its stdin for
// commands, and its stdout as a stream of lines.
type rustPeer struct {
	id    key.EndpointID
	addr  netip.AddrPort
	stdin io.Writer
	lines chan string
}

// startRust starts the Rust peer and waits for it to announce its address.
// The process is killed when the test ends.
func startRust(t *testing.T, ctx context.Context, bin string, args ...string) *rustPeer {
	t.Helper()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	p := &rustPeer{stdin: stdin, lines: make(chan string, 256)}
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			p.lines <- sc.Text()
		}
		close(p.lines)
	}()
	for {
		line, err := p.next(ctx)
		if err != nil {
			t.Fatalf("reading the Rust peer's address: %v", err)
		}
		if line == "READY" {
			break
		}
		if rest, ok := strings.CutPrefix(line, "ADDR "); ok {
			f := strings.Fields(rest)
			if len(f) != 2 {
				continue
			}
			ap, err := netip.ParseAddrPort(f[1])
			if err != nil || !ap.Addr().IsLoopback() {
				continue
			}
			if p.id, err = key.ParseEndpointID(f[0]); err != nil {
				t.Fatal(err)
			}
			p.addr = ap
		}
	}
	if !p.addr.IsValid() {
		t.Fatal("the Rust peer announced no loopback address before READY")
	}
	t.Logf("rust peer %s at %s", p.id.Short(), p.addr)
	return p
}

func (p *rustPeer) next(ctx context.Context) (string, error) {
	select {
	case line, ok := <-p.lines:
		if !ok {
			return "", errors.New("the Rust peer exited")
		}
		return line, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// await reads the peer's output until a line equal to want.
func (p *rustPeer) await(ctx context.Context, want string) (string, error) {
	for {
		line, err := p.next(ctx)
		if err != nil {
			return "", fmt.Errorf("waiting for Rust to print %q: %w", want, err)
		}
		if line == want {
			return line, nil
		}
	}
}

func (p *rustPeer) send(t *testing.T, format string, args ...any) {
	t.Helper()
	if _, err := fmt.Fprintf(p.stdin, format+"\n", args...); err != nil {
		t.Fatalf("writing to the Rust peer: %v", err)
	}
}

// idHex is an endpoint id as Rust prints it.
func idHex(id key.EndpointID) string {
	b := id.Bytes()
	return hex.EncodeToString(b[:])
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
