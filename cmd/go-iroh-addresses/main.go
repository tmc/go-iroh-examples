// Command go-iroh-addresses builds the value a peer is dialed with.
//
// [netaddr.EndpointAddr] pairs an endpoint ID with the paths that endpoint may
// be reached over: the ID says who, the paths say where. A path is either a
// direct UDP socket address or a relay URL, and one address may carry any
// number of each — including none, which is still usable when an address-lookup
// service can supply the paths (go-iroh-memory-discovery).
//
// Direct and relayed paths are alternatives rather than a choice the caller
// makes. Given both, an endpoint races them and settles on whichever works,
// switching later if the network changes; go-iroh-path-upgrade watches that happen.
//
// This example assembles an address by hand, which is what go-iroh-direct-echo does
// on loopback and what nothing does in production. There the same value arrives
// from elsewhere: packed into a ticket (go-iroh-tickets) or resolved
// from DNS or pkarr (go-iroh-dns-resolve, go-iroh-pkarr-publish-resolve).
package main

import (
	"fmt"
	"io"
	"net/netip"
	"os"

	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(stdout io.Writer) error {
	secret, err := key.GenerateSecretKey()
	if err != nil {
		return err
	}

	relayURL, err := netaddr.ParseRelayURL("https://relay.example.com")
	if err != nil {
		return fmt.Errorf("parse relay url: %w", err)
	}

	addr := netaddr.NewEndpointAddr(secret.Public().EndpointID()).
		WithIP(netip.MustParseAddrPort("[::1]:4433")).
		WithRelayURL(relayURL)

	fmt.Fprintln(stdout, "endpoint:", addr.ID.Short())
	fmt.Fprintln(stdout, "direct paths:", len(addr.IPAddrs()))
	fmt.Fprintln(stdout, "relay paths:", len(addr.RelayURLs()))
	for _, a := range addr.Addrs() {
		fmt.Fprintln(stdout, a)
	}
	return nil
}
