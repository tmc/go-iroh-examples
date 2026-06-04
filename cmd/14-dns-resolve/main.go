package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
)

func main() {
	rawID := os.Getenv("IROH_EXAMPLE_ENDPOINT_ID")
	if rawID == "" {
		fmt.Println("set IROH_EXAMPLE_ENDPOINT_ID to a published endpoint id")
		return
	}
	origin := os.Getenv("IROH_EXAMPLE_DNS_ORIGIN")
	if origin == "" {
		origin = dns.N0DNSEndpointOriginProd
	}

	id, err := key.ParseEndpointID(rawID)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lookup := iroh.NewDNSAddressLookup(origin, &dns.Resolver{})
	for item, err := range lookup.Resolve(ctx, id) {
		if err != nil {
			panic(err)
		}
		addr := item.Addr()
		fmt.Println("provenance:", item.Provenance())
		fmt.Println("endpoint:", addr.ID.Z32())
		fmt.Println("direct paths:", addr.IPAddrs())
		fmt.Println("relay paths:", addr.RelayURLs())
		return
	}
}
