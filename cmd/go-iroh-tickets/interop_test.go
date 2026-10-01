package main

// Interoperability with iroh's Rust tickets.
//
// The package comment says a ticket printed here can be dialed by iroh's Rust
// tooling and the reverse. These tests hand tickets across in both directions
// and dial them: a Go ticket parsed by the iroh-tickets crate, and a ticket
// printed by that crate parsed by endpointticket.Decode. Nothing else carries
// the address; the only thing the dialer is given is the ticket string.
//
// As in go-iroh-framed-messages, they live outside main_test.go because
// internal/catalog reads that file to decide what an example needs in order to
// run, and what these need is a Rust toolchain: a property of the checkout
// rather than of the example.

import (
	"bufio"
	"context"
	"io"
	"net/netip"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/iroh"
)

// families are the loopback addresses the tests bind. The "ipv6x2" case adds
// a second, unreachable IPv6 address to the Go ticket, which is what a Go
// endpoint bound to [::] announces. Before go-iroh v0.2.3, endpointticket
// wrote two extra varints after each IPv6 address; Rust ignores trailing
// bytes after the last address, so only a ticket with two caught it.
var families = []struct {
	name     string
	loopback netip.Addr
	rust     string
	extra    netip.AddrPort // also listed in the Go ticket, if valid
}{
	{"ipv4", netip.AddrFrom4([4]byte{127, 0, 0, 1}), "127.0.0.1:0", netip.AddrPort{}},
	{"ipv6", netip.IPv6Loopback(), "[::1]:0", netip.AddrPort{}},
	{"ipv6x2", netip.IPv6Loopback(), "[::1]:0", netip.MustParseAddrPort("[2001:db8::1]:1")},
}

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

// TestInteropRustDialsGoTicket prints a ticket with endpointticket.Encode, as
// the example does, and has iroh-tickets parse it and dial the example's echo.
func TestInteropRustDialsGoTicket(t *testing.T) {
	bin := peerBin(t)
	for _, fam := range families {
		t.Run(fam.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			server, err := bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(fam.loopback, 0)), iroh.WithALPNs(alpn))
			if err != nil {
				t.Fatal(err)
			}
			defer server.Shutdown(context.Background())
			go func() {
				conn, err := server.Accept(ctx)
				if err != nil {
					return
				}
				_ = echo(ctx, conn)
				<-conn.Context().Done()
			}()

			addr := server.Addr()
			if fam.extra.IsValid() {
				addr = addr.WithIP(fam.extra)
			}
			ticket := endpointticket.Encode(addr)
			out, err := exec.CommandContext(ctx, bin, "ticket-dial", ticket, "from rust").CombinedOutput()
			if err != nil {
				t.Fatalf("rust ticket-dial: %v\n%s", err, out)
			}
			if !strings.Contains(string(out), "ECHO from rust") {
				t.Errorf("Rust peer reported %q, want the echo of %q", out, "from rust")
			}
		})
	}
}

// TestInteropGoDialsRustTicket parses a ticket printed by iroh-tickets with
// endpointticket.Decode, as the example does, and dials the address in it.
func TestInteropGoDialsRustTicket(t *testing.T) {
	bin := peerBin(t)
	for _, fam := range families {
		if fam.extra.IsValid() {
			continue // the Rust ticket lists one address; ipv6 covers it
		}
		t.Run(fam.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			ticket := startTicketPeer(t, ctx, bin, fam.rust)
			addr, err := endpointticket.Decode(ticket)
			if err != nil {
				t.Fatalf("decode Rust ticket: %v", err)
			}
			client, err := bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(fam.loopback, 0)))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Shutdown(context.Background())
			conn, err := client.Connect(ctx, addr, alpn)
			if err != nil {
				t.Fatalf("dial Rust ticket: %v", err)
			}
			defer conn.CloseWithError(0, "")
			reply, err := exchange(ctx, conn, "from go")
			if err != nil {
				t.Fatal(err)
			}
			if reply != "from go" {
				t.Errorf("Rust echoed %q, want %q", reply, "from go")
			}
		})
	}
}

// startTicketPeer runs the Rust ticket listener on bind and returns the ticket
// it printed. The peer is killed when the test ends.
func startTicketPeer(t *testing.T, ctx context.Context, bin, bind string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, bin, "ticket-listen", bind)
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
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		if ticket, ok := strings.CutPrefix(sc.Text(), "TICKET "); ok {
			go io.Copy(io.Discard, stdout)
			return ticket
		}
	}
	t.Fatalf("Rust peer printed no ticket: %v", sc.Err())
	return ""
}
