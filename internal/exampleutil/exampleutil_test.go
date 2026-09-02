package exampleutil

import (
	"context"
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// TestTicketGolden pins the wire form of an endpoint ticket against a vector
// produced by the Rust implementation, so a change in go-iroh that breaks
// cross-language tickets fails here first.
func TestTicketGolden(t *testing.T) {
	id, err := key.ParseEndpointID("ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6")
	if err != nil {
		t.Fatal(err)
	}
	relay, err := netaddr.ParseRelayURL("http://derp.me./")
	if err != nil {
		t.Fatal(err)
	}
	addr := netaddr.NewEndpointAddr(id,
		netaddr.RelayAddr{URL: relay},
		netaddr.IPAddr{Addr: netip.MustParseAddrPort("127.0.0.1:1024")},
	)
	wantBytes, err := hex.DecodeString(
		"00" +
			"ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6" +
			"02" +
			"00" +
			"10" +
			"687474703a2f2f646572702e6d652e2f" +
			"01" +
			"00" +
			"7f0000018008")
	if err != nil {
		t.Fatal(err)
	}
	want := "endpoint" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(wantBytes))
	if got := EncodeTicket(addr); got != want {
		t.Fatalf("EncodeTicket = %q, want %q", got, want)
	}
	got, err := DecodeTicket(want)
	if err != nil {
		t.Fatalf("DecodeTicket: %v", err)
	}
	if !got.ID.Equal(addr.ID) {
		t.Fatalf("id = %s, want %s", got.ID, addr.ID)
	}
	if len(got.Addrs()) != len(addr.Addrs()) {
		t.Fatalf("addrs = %v, want %v", got.Addrs(), addr.Addrs())
	}
}

// TestBindExchange covers the helpers every example builds on: two loopback
// endpoints, an address without a discovery service, and one stream exchange.
func TestBindExchange(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	const alpn = "go-iroh-examples/exampleutil-test/1"
	server, err := Bind(ctx, iroh.WithALPNs(alpn))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(ctx)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			return
		}
		_ = Handler{Transform: strings.ToUpper}.Accept(ctx, conn)
	}()

	client, err := Bind(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, Addr(server), alpn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseWithError(0, "")

	got, err := Exchange(ctx, conn, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if got != "HELLO" {
		t.Fatalf("Exchange = %q, want %q", got, "HELLO")
	}
}

func TestCapture(t *testing.T) {
	out, err := Capture(func() error {
		fmt.Println("captured")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if out != "captured\n" {
		t.Fatalf("Capture = %q, want %q", out, "captured\n")
	}
}

func TestEnv(t *testing.T) {
	if got := Env("GO_IROH_EXAMPLES_UNSET", "fallback"); got != "fallback" {
		t.Errorf("Env unset = %q, want %q", got, "fallback")
	}
	t.Setenv("GO_IROH_EXAMPLES_TEST", "set")
	if got := Env("GO_IROH_EXAMPLES_TEST", "fallback"); got != "set" {
		t.Errorf("Env set = %q, want %q", got, "set")
	}
	t.Setenv("GO_IROH_EXAMPLES_TEST_BOOL", "true")
	if !EnvBool("GO_IROH_EXAMPLES_TEST_BOOL", false) {
		t.Error("EnvBool = false, want true")
	}
	if EnvBool("GO_IROH_EXAMPLES_UNSET", true) != true {
		t.Error("EnvBool unset did not fall back")
	}
}
