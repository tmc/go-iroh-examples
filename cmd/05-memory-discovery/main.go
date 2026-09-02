package main

import (
	"context"
	"fmt"
	"net/netip"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const alpn = "go-iroh-examples/memory-discovery/1"

	lookup := iroh.NewMemoryLookup()
	var lookups iroh.AddressLookupServices
	lookups.AddResolver(lookup)

	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithAddressLookup(&lookups),
	)
	if err != nil {
		panic(err)
	}
	lookup.AddEndpointAddr(server.Addr())

	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		alpn: exampleutil.Handler{},
	}, nil)
	if err != nil {
		panic(err)
	}
	defer router.Shutdown(ctx)

	client, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithAddressLookup(&lookups),
	)
	if err != nil {
		panic(err)
	}
	defer client.Shutdown(ctx)

	// Register the client too. A lookup service is symmetric: the server
	// resolves the dialing endpoint's ID while it accepts, so an entry that
	// only ever names the server leaves the reverse lookup with nothing to
	// return. In go-iroh v0.1.0 that miss crashes the endpoint
	// (iroh/addresslookup.go:303 ranges over the nil sequence
	// MemoryLookup.Resolve returns for an unknown ID).
	lookup.AddEndpointAddr(client.Addr())

	var addr iroh.Item
	ok := false
	for item, err := range lookup.Resolve(ctx, server.ID()) {
		if err != nil {
			panic(err)
		}
		addr = item
		ok = true
		break
	}
	if !ok {
		panic("memory lookup returned no results")
	}

	conn, err := client.Connect(ctx, addr.Addr(), alpn)
	if err != nil {
		panic(err)
	}
	defer conn.CloseWithError(0, "")

	reply, err := exampleutil.Exchange(ctx, conn, "discovered hello")
	if err != nil {
		panic(err)
	}
	fmt.Println(reply)
}
