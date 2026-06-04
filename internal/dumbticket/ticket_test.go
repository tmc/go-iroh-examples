package dumbticket

import (
	"encoding/base32"
	"encoding/hex"
	"net/netip"
	"strings"
	"testing"

	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

func TestEndpointTicketGolden(t *testing.T) {
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
	if got := EncodeEndpoint(addr); got != want {
		t.Fatalf("EncodeEndpoint = %q, want %q", got, want)
	}
	got, err := DecodeEndpoint(want)
	if err != nil {
		t.Fatalf("DecodeEndpoint: %v", err)
	}
	if !got.ID.Equal(addr.ID) {
		t.Fatalf("id = %s, want %s", got.ID, addr.ID)
	}
	if len(got.Addrs()) != len(addr.Addrs()) {
		t.Fatalf("addrs = %v, want %v", got.Addrs(), addr.Addrs())
	}
}
