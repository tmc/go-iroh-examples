package main

import (
	"fmt"
	"net/netip"

	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

func main() {
	secret, err := key.GenerateSecretKey()
	if err != nil {
		panic(err)
	}

	relayURL, err := netaddr.ParseRelayURL("https://relay.example.com")
	if err != nil {
		panic(err)
	}

	addr := netaddr.NewEndpointAddr(secret.Public().EndpointID()).
		WithIP(netip.MustParseAddrPort("[::1]:4433")).
		WithRelayURL(relayURL)

	fmt.Println("endpoint:", addr.ID.Short())
	fmt.Println("direct paths:", len(addr.IPAddrs()))
	fmt.Println("relay paths:", len(addr.RelayURLs()))
	for _, a := range addr.Addrs() {
		fmt.Println(a)
	}
}
