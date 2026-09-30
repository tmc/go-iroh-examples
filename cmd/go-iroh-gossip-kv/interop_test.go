package main

// Interoperability with n0's iroh-smol-kv, which this example is a port of.
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

// A message produced by iroh-smol-kv 0.4.0's own GossipMessage type
// (gossip_peer kv-vector in interop/gossip): the scope whose secret key is 32
// bytes of 7 writes color=blue at timestamp 1750000000000000000.
const (
	rustScope   = "ea4a6c63e29c520abef5507b132ec5f9954776aebebe7b92421eea691446d22c"
	rustSigning = "05636f6c6f728080bca7a6f7cfa41804626c7565"
	rustMessage = "00" + rustScope + "05636f6c6f728080bca7a6f7cfa41804626c7565" +
		"3bf53d5cba96c9bb781ef8123bd347c0b95fbf48f7fb0c262d991b5a943b6437" +
		"d13b26766ea6dabf87c5f14d08591c88c1f77da1d55a67685426c458b3df400f"
)

// TestRustWireFormat pins the update encoding and the signed bytes to what
// iroh-smol-kv produced. Ed25519 signatures are deterministic, so signing the
// same write here must reproduce the Rust message byte for byte. It needs no
// Rust toolchain and runs everywhere.
func TestRustWireFormat(t *testing.T) {
	var seed [key.SeedSize]byte
	for i := range seed {
		seed[i] = 7
	}
	sk := key.NewSecretKey(seed)
	defer sk.Clear()
	const ts = 1750000000000000000

	if got := hex.EncodeToString(signingData([]byte("color"), ts, []byte("blue"))); got != rustSigning {
		t.Errorf("signing data = %s, want %s (Rust)", got, rustSigning)
	}
	if got := hex.EncodeToString(signUpdate(sk, []byte("color"), []byte("blue"), ts).encode()); got != rustMessage {
		t.Errorf("message = %s\nwant      %s (Rust)", got, rustMessage)
	}

	raw, err := hex.DecodeString(rustMessage)
	if err != nil {
		t.Fatal(err)
	}
	store := make(kvStore)
	if err := store.apply(raw); err != nil {
		t.Fatalf("apply the Rust message: %v", err)
	}
	scope, err := key.ParsePublicKey(rustScope)
	if err != nil {
		t.Fatal(err)
	}
	if got := store[scope.Bytes()]["color"]; got.Value != "blue" || got.Timestamp != ts {
		t.Errorf("after the Rust message: %+v, want blue at %d", got, ts)
	}
}

// TestInterop puts one Go node and one Rust iroh-smol-kv node on a topic and
// writes on each; each write must reach the other's store. Both sides joining
// are tried, because the joiner is the one that dials.
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
			topic := gossip.TopicID(sha256.Sum256([]byte("go-iroh-examples/gossip-kv/interop/" + tc.name)))
			topicHex := hex.EncodeToString(topic[:])

			// Bind IPv4 rather than using the example's bind, which takes
			// IPv6: the Rust peer announces 127.0.0.1, and an endpoint bound
			// to ::1 has no route to it.
			ep, err := iroh.Bind(ctx, iroh.WithBindAddr(
				netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 0)))
			if err != nil {
				t.Fatal(err)
			}
			g, err := serve(ctx, ep)
			if err != nil {
				t.Fatal(err)
			}
			defer g.close(context.Background())

			var (
				rust *rustPeer
				sub  *gossip.Topic
			)
			if tc.rustJoin {
				// Rust gets Go's address on its command line, so Go must be
				// subscribed before Rust starts.
				if sub, err = g.gossip.Subscribe(ctx, topic, nil); err != nil {
					t.Fatal(err)
				}
				goAddr, err := loopbackAddr(ep)
				if err != nil {
					t.Fatal(err)
				}
				rust = startRust(t, ctx, bin, "kv", topicHex, idHex(ep.ID()), goAddr.String())
			} else {
				rust = startRust(t, ctx, bin, "kv", topicHex)
				addr := netaddr.NewEndpointAddr(rust.id, netaddr.IPAddr{Addr: rust.addr})
				if sub, err = g.gossip.Subscribe(ctx, topic, []netaddr.EndpointAddr{addr}); err != nil {
					t.Fatal(err)
				}
			}
			defer sub.Close()

			line, err := rust.await(ctx, "SCOPE ")
			if err != nil {
				t.Fatal(err)
			}
			rustScope, err := key.ParsePublicKey(strings.TrimPrefix(line, "SCOPE "))
			if err != nil {
				t.Fatal(err)
			}

			// Both sides must see the link before writing, or an update can
			// go out while the overlay is still empty.
			if err := sub.Joined(ctx); err != nil {
				t.Fatalf("go never joined: %v", err)
			}
			if _, err := rust.await(ctx, "UP "+idHex(ep.ID())); err != nil {
				t.Fatal(err)
			}

			msg, err := g.put("color", "blue")
			if err != nil {
				t.Fatal(err)
			}
			if err := sub.Broadcast(ctx, msg); err != nil {
				t.Fatalf("broadcast: %v", err)
			}
			rust.send(t, "put shape circle")

			// Rust reports every entry its store accepts, and it accepts only
			// updates whose signature it verified.
			goScope := g.signer.Public()
			if _, err := rust.await(ctx, fmt.Sprintf("ENTRY %s color blue", idHex(goScope.EndpointID()))); err != nil {
				t.Fatal(err)
			}
			if err := awaitValue(ctx, g, sub, rustScope, "shape", "circle"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// awaitValue applies updates from sub to n's store until scope's k is want.
func awaitValue(ctx context.Context, n *node, sub *gossip.Topic, scope key.PublicKey, k, want string) error {
	// Only closing the topic ends its event stream.
	stop := context.AfterFunc(ctx, func() { sub.Close() })
	defer stop()
	for ev, err := range sub.Events() {
		if err != nil {
			return err
		}
		if ev.Kind != gossip.Received {
			continue
		}
		if err := n.store.apply(ev.Content); err != nil {
			return fmt.Errorf("apply update from %s: %w", ev.DeliveredFrom.Short(), err)
		}
		if v := n.store[scope.Bytes()][k]; v.Value == want {
			return nil
		}
	}
	return fmt.Errorf("go never stored %s=%s from rust: %w", k, want, ctx.Err())
}

// gossipBin is the Rust peer built from interop/gossip. Without it the live
// tests skip: they need a Rust toolchain, and the vector test above already
// covers the wire format.
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

// startRust starts the Rust peer and waits for it to announce its loopback
// address. The process is killed when the test ends.
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

// await reads the peer's output until a line that begins with prefix, and
// returns that line.
func (p *rustPeer) await(ctx context.Context, prefix string) (string, error) {
	for {
		line, err := p.next(ctx)
		if err != nil {
			return "", fmt.Errorf("waiting for Rust to print %q: %w", prefix, err)
		}
		if strings.HasPrefix(line, prefix) {
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

// idHex is a key as Rust prints it.
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
