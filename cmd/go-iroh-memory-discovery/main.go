// Command go-iroh-memory-discovery dials a peer known only by its endpoint ID.
//
// go-iroh-direct-echo and go-iroh-router-echo hand the client a complete address because
// the client already knows where the server is. Outside a loopback example it
// does not: an endpoint ID is stable and shareable, while the addresses behind
// it change with the network. An address-lookup service closes that gap, and
// [iroh.MemoryLookup] is the smallest one — a map from ID to address that the
// application fills in from whatever out-of-band channel it already has, such
// as a ticket it was pasted.
//
// [iroh.WithAddressLookup] registers the lookup with both endpoints. In go-iroh
// v0.1.0 the registration feeds the per-remote address state machine but does
// not seed the first dial, so this example resolves the ID itself and passes
// the resulting [netaddr.EndpointAddr] to [iroh.Endpoint.Connect]; the same
// note applies in go-iroh-local-infra.
//
// [iroh.AddressResolver] is one interface with several implementations, and
// only this one is a map: go-iroh-dns-resolve and go-iroh-pkarr-publish-resolve resolve
// through n0's public deployment, go-iroh-local-infra through a pkarr relay in the
// same process, go-iroh-mdns-discovery over the local link.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const alpn = "go-iroh-examples/memory-discovery/1"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lookup := iroh.NewMemoryLookup()
	var lookups iroh.AddressLookupServices
	lookups.AddResolver(lookup)

	server, err := exampleutil.Bind(ctx, iroh.WithAddressLookup(&lookups))
	if err != nil {
		return err
	}
	lookup.AddEndpointAddr(server.Addr())

	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		alpn: exampleutil.Handler{},
	}, nil)
	if err != nil {
		return err
	}
	defer router.Shutdown(ctx)

	client, err := exampleutil.Bind(ctx, iroh.WithAddressLookup(&lookups))
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	// Register the client too. A lookup service is symmetric: the server
	// resolves the dialing endpoint's ID while it accepts, so an entry that
	// only ever names the server leaves the reverse lookup with nothing to
	// return. In go-iroh v0.1.0 that miss crashes the endpoint
	// (iroh/addresslookup.go:303 ranges over the nil sequence
	// MemoryLookup.Resolve returns for an unknown ID).
	lookup.AddEndpointAddr(client.Addr())

	addr, err := resolve(ctx, lookup, server)
	if err != nil {
		return err
	}
	fmt.Println("resolved addresses:", len(addr.Addrs()))

	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", server.ID().Short(), err)
	}
	defer conn.CloseWithError(0, "")

	reply, err := exampleutil.Exchange(ctx, conn, "discovered hello")
	if err != nil {
		return err
	}
	fmt.Println(reply)
	return nil
}

// resolve returns the first address the lookup reports for the server's ID.
// Resolve yields a sequence because a real service may answer more than once,
// from more than one source, and may report an error for one source while
// another still succeeds.
func resolve(ctx context.Context, lookup *iroh.MemoryLookup, server *iroh.Endpoint) (netaddr.EndpointAddr, error) {
	for item, err := range lookup.Resolve(ctx, server.ID()) {
		if err != nil {
			return netaddr.EndpointAddr{}, fmt.Errorf("resolve %s: %w", server.ID().Short(), err)
		}
		return item.Addr(), nil
	}
	return netaddr.EndpointAddr{}, fmt.Errorf("no address for %s", server.ID().Short())
}
