// Command 14-dns-resolve turns an endpoint id into an address using DNS.
//
// An endpoint ID is a public key. It says who a peer is and nothing about where
// it is, which is why 12-connect-public has to be handed a UDP address or a
// relay URL alongside it. Endpoint discovery closes that gap with DNS: an
// endpoint publishes its current relay URL and direct addresses as TXT records
// under "_iroh.<z32-endpoint-id>.<origin>", and anyone holding the ID can look
// them up. Because the records are signed with the same key the ID names, a
// hostile resolver can withhold an answer but cannot forge one.
//
// [iroh.NewDNSAddressLookup] performs that query against a discovery origin —
// [dns.N0DNSEndpointOriginProd] by default, or -dns-origin for a private
// deployment. [iroh.DNSAddressLookup.Resolve] returns a stream rather than one
// address because a lookup can produce several results, each carrying its
// [iroh.Item.Provenance], and a caller may want the first usable one; this
// example prints it and stops.
//
// This is the read side. 15-pkarr-publish-resolve is the write side, publishing
// the records this resolves; 05-memory-discovery is the same ID-to-address step
// with an in-process directory; 27-local-infra runs both against its own origin.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("14-dns-resolve", flag.ContinueOnError)
	rawID := fs.String("endpoint-id", exampleutil.Env("IROH_EXAMPLE_ENDPOINT_ID", ""), "published endpoint id to resolve, z32 or hex ($IROH_EXAMPLE_ENDPOINT_ID)")
	origin := fs.String("dns-origin", exampleutil.Env("IROH_EXAMPLE_DNS_ORIGIN", dns.N0DNSEndpointOriginProd), "discovery origin to query ($IROH_EXAMPLE_DNS_ORIGIN)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *rawID == "" {
		fmt.Println("pass -endpoint-id or set IROH_EXAMPLE_ENDPOINT_ID to a published endpoint id")
		return nil
	}

	id, err := parseEndpointID(*rawID)
	if err != nil {
		return fmt.Errorf("parse endpoint id: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lookup := iroh.NewDNSAddressLookup(*origin, &dns.Resolver{})
	for item, err := range lookup.Resolve(ctx, id) {
		if err != nil {
			return fmt.Errorf("resolve endpoint %s: %w", id, err)
		}
		addr := item.Addr()
		fmt.Println("provenance:", item.Provenance())
		fmt.Println("endpoint:", addr.ID.Z32())
		fmt.Println("direct paths:", addr.IPAddrs())
		fmt.Println("relay paths:", addr.RelayURLs())
		return nil
	}
	fmt.Println("no DNS endpoint records found")
	return nil
}

// parseEndpointID accepts either printed form of an endpoint id. As of go-iroh
// v0.1.0 key.ParseEndpointID takes the hex form of key.EndpointID.String, not
// the z-base-32 form of key.EndpointID.Z32 that the other examples print, so
// the z32 form is tried separately.
func parseEndpointID(s string) (key.EndpointID, error) {
	if id, err := key.ParseEndpointID(s); err == nil {
		return id, nil
	}
	return key.ParseEndpointIDZ32(s)
}
