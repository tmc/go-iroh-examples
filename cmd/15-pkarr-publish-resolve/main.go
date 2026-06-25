package main

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if os.Getenv("GO_IROH_LIVE_PKARR") != "1" {
		fmt.Println("set GO_IROH_LIVE_PKARR=1 to publish to and resolve from the public pkarr relay")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	secret, err := key.GenerateSecretKey()
	if err != nil {
		return err
	}
	publisher, err := iroh.N0PkarrPublisher(secret, &iroh.PkarrPublisherConfig{
		AddrFilter:        func(addrs []netaddr.TransportAddr) []netaddr.TransportAddr { return addrs },
		RepublishInterval: time.Hour,
	})
	if err != nil {
		return err
	}
	defer publisher.Close()

	addr := netip.MustParseAddrPort("203.0.113.10:4433")
	publisher.Publish(dns.EndpointDataFromAddr(
		netaddr.NewEndpointAddr(secret.Public().EndpointID()).WithIP(addr),
	))

	resolver, err := iroh.N0PkarrResolver(nil)
	if err != nil {
		return err
	}

	var lastErr error
	for ctx.Err() == nil {
		for item, err := range resolver.Resolve(ctx, secret.Public().EndpointID()) {
			if err != nil {
				lastErr = err
				continue
			}
			fmt.Println("published endpoint:", item.EndpointID().Z32())
			fmt.Println("resolved direct paths:", item.Addr().IPAddrs())
			return nil
		}
		time.Sleep(time.Second)
	}
	if lastErr != nil {
		return fmt.Errorf("resolve published endpoint: %w", lastErr)
	}
	return errors.New("pkarr resolve timed out")
}
