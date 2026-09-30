package main

// Interoperability with Rust iroh and postcard.
//
// The package comment makes two claims this file tests. The request and
// report are encoded with a postcard codec that is byte-exact with Rust's, so
// the encodings are pinned against bytes printed by the postcard crate from
// serde definitions of the same two structs. And the key-exchange policies
// are about which peers can connect, so the exchange is run live against Rust
// iroh endpoints whose rustls providers offer known groups.
//
// As in go-iroh-framed-messages, they live outside main_test.go because
// internal/catalog reads that file to decide what an example needs in order to
// run, and what the live tests need is a Rust toolchain: a property of the
// checkout rather than of the example.

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/netip"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/postcard"
)

// TestRustPostcardVectors pins request and report against the postcard crate
// 1.1, whose output interop/blobs prints with: blobs_peer kx-vectors. It needs
// no Rust toolchain.
func TestRustPostcardVectors(t *testing.T) {
	for _, tt := range []struct {
		v       any
		rustHex string
	}{
		{request{Nonce: 0}, "00"},
		{request{Nonce: 0x5a5a}, "dab401"},
		{request{Nonce: math.MaxUint64}, "ffffffffffffffffff01"},
		{report{Nonce: 0, Group: ""}, "0000"},
		{report{Nonce: 0x5a5a, Group: "X25519"}, "dab40106583235353139"},
		{report{Nonce: 0x5a5a, Group: "X25519MLKEM768"}, "dab4010e5832353531394d4c4b454d373638"},
		{report{Nonce: math.MaxUint64, Group: "X25519MLKEM768"}, "ffffffffffffffffff010e5832353531394d4c4b454d373638"},
	} {
		b, err := postcard.Marshal(tt.v)
		if err != nil {
			t.Fatal(err)
		}
		if got := hex.EncodeToString(b); got != tt.rustHex {
			t.Errorf("Marshal(%+v) = %s, want the Rust %s", tt.v, got, tt.rustHex)
		}
		rust, _ := hex.DecodeString(tt.rustHex)
		p := reflect.New(reflect.TypeOf(tt.v))
		if err := postcard.Unmarshal(rust, p.Interface()); err != nil {
			t.Errorf("Unmarshal(%s): %v", tt.rustHex, err)
			continue
		}
		if got := p.Elem().Interface(); got != tt.v {
			t.Errorf("Unmarshal(%s) = %+v, want %+v", tt.rustHex, got, tt.v)
		}
	}
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

// The Rust peer's policies, as rustls providers. Rust iroh does not report the
// negotiated group, so a Rust listener reports the one its policy offers:
//
//	default    iroh's stock provider, ring, which has no ML-KEM group
//	classical  aws-lc-rs offering X25519 only
//	pq-only    aws-lc-rs offering X25519MLKEM768 only
//
// "" as the wanted group means the handshake must fail.

var policyNames = map[iroh.KeyExchangePolicy]string{
	iroh.KeyExchangeDefault:   "default",
	iroh.KeyExchangeClassical: "classical",
	iroh.KeyExchangePreferPQ:  "prefer-pq",
	iroh.KeyExchangePQOnly:    "pq-only",
}

// TestInteropGoDialsRust runs the example's probe against Rust listeners.
func TestInteropGoDialsRust(t *testing.T) {
	bin := peerBin(t)
	for _, tt := range []struct {
		rust     string
		goPolicy iroh.KeyExchangePolicy
		group    string
	}{
		{"classical", iroh.KeyExchangeDefault, "X25519"},
		{"pq-only", iroh.KeyExchangeDefault, "X25519MLKEM768"},
		{"pq-only", iroh.KeyExchangePQOnly, "X25519MLKEM768"},
		{"pq-only", iroh.KeyExchangeClassical, ""},
	} {
		t.Run(fmt.Sprintf("rust %s, go %s", tt.rust, policyNames[tt.goPolicy]), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// The example binds ::1, so the Rust listener does too.
			addr := startKXPeer(t, ctx, bin, tt.rust)
			dialed, accepted, err := probe(ctx, addr, tt.goPolicy)
			switch {
			case tt.group == "" && errors.Is(err, iroh.ErrTLSHandshakeFailure):
				return
			case tt.group == "":
				t.Fatalf("probe: %v, want ErrTLSHandshakeFailure", err)
			case err != nil:
				t.Fatalf("probe: %v", err)
			}
			if dialed != tt.group || accepted != tt.group {
				t.Errorf("Go negotiated %s, Rust reported %s, want %s", dialed, accepted, tt.group)
			}
		})
	}
}

// TestInteropRustDialsGo runs the example's serve against Rust dialers,
// including stock Rust iroh, which offers only classical groups.
func TestInteropRustDialsGo(t *testing.T) {
	bin := peerBin(t)
	for _, tt := range []struct {
		goPolicy iroh.KeyExchangePolicy
		rust     string
		group    string
	}{
		{iroh.KeyExchangeDefault, "default", "X25519"},
		{iroh.KeyExchangeDefault, "pq-only", "X25519MLKEM768"},
		{iroh.KeyExchangePQOnly, "pq-only", "X25519MLKEM768"},
		{iroh.KeyExchangePQOnly, "default", ""},
	} {
		t.Run(fmt.Sprintf("go %s, rust %s", policyNames[tt.goPolicy], tt.rust), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			server, err := bind(ctx, iroh.WithALPNs(alpn), iroh.WithKeyExchangePolicy(tt.goPolicy))
			if err != nil {
				t.Fatal(err)
			}
			defer server.Shutdown(context.Background())
			go serve(ctx, server)

			ap, err := loopbackAddr(server)
			if err != nil {
				t.Fatal(err)
			}
			id := server.ID().Bytes()
			out, err := exec.CommandContext(ctx, bin, "kx-connect", tt.rust,
				hex.EncodeToString(id[:]), ap.String(), "23130").CombinedOutput()
			if tt.group == "" {
				// TLS alert 40 is handshake_failure: no group in common.
				if err == nil || !strings.Contains(string(out), "handshake failed: error 40") {
					t.Fatalf("rust kx-connect: %v, want a handshake failure:\n%s", err, out)
				}
				return
			}
			if err != nil {
				t.Fatalf("rust kx-connect: %v\n%s", err, out)
			}
			if want := "REPORT 23130 " + tt.group + "\n"; !strings.Contains(string(out), want) {
				t.Errorf("Rust peer reported %q, want it to contain %q", out, want)
			}
		})
	}
}

// startKXPeer runs a Rust key-exchange listener on [::1] with policy and
// returns its address. The peer is killed when the test ends.
func startKXPeer(t *testing.T, ctx context.Context, bin, policy string) netaddr.EndpointAddr {
	t.Helper()
	cmd := exec.CommandContext(ctx, bin, "kx-listen", policy, "[::1]:0")
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
		rest, ok := strings.CutPrefix(sc.Text(), "ADDR ")
		if !ok {
			continue
		}
		go io.Copy(io.Discard, stdout)
		idHex, ap, _ := strings.Cut(rest, " ")
		raw, err := hex.DecodeString(idHex)
		if err != nil || len(raw) != 32 {
			t.Fatalf("bad endpoint id %q", idHex)
		}
		id, err := key.NewEndpointID([32]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		addr, err := netip.ParseAddrPort(ap)
		if err != nil {
			t.Fatal(err)
		}
		return netaddr.NewEndpointAddr(id, netaddr.IPAddr{Addr: addr})
	}
	t.Fatalf("Rust peer printed no ADDR line: %v", sc.Err())
	return netaddr.EndpointAddr{}
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
