package exampleutil

import (
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"net/netip"
	"strings"
	"testing"

	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// TestTicketGolden pins the wire form of an endpoint ticket against a vector
// produced by the Rust implementation, so a change in go-iroh that breaks
// cross-language tickets fails here first. The examples hand addresses to
// peers as tickets, so this is the assumption underneath go-iroh-tickets and
// go-iroh-dumbpipe rather than a test of anything in this package.
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
	if got := endpointticket.Encode(addr); got != want {
		t.Fatalf("endpointticket.Encode = %q, want %q", got, want)
	}
	got, err := endpointticket.Decode(want)
	if err != nil {
		t.Fatalf("endpointticket.Decode: %v", err)
	}
	if !got.ID.Equal(addr.ID) {
		t.Fatalf("id = %s, want %s", got.ID, addr.ID)
	}
	if len(got.Addrs()) != len(addr.Addrs()) {
		t.Fatalf("addrs = %v, want %v", got.Addrs(), addr.Addrs())
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
