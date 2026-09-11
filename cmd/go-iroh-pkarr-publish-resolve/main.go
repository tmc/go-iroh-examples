// Command go-iroh-pkarr-publish-resolve publishes an address and reads it back.
//
// This is the write side of endpoint discovery, and pkarr is the mechanism:
// public-key addressable resource records. An endpoint signs a small DNS packet
// describing itself with its own secret key and PUTs it to a pkarr relay, which
// stores packets keyed by the public key that signed them. Anyone holding the
// endpoint ID can GET it back, and verify it, without trusting the relay.
// go-iroh-dns-resolve reads the same records over ordinary DNS, because n0's
// discovery origin serves what its pkarr relay stores.
//
// [iroh.PkarrPublisher.Publish] is fire-and-forget: it hands the data to a
// background goroutine that performs the PUT and republishes on
// RepublishInterval, so this example resolves in a loop until the record shows
// up rather than assuming it is there. The published address here is a
// documentation address (RFC 5737) rather than the host's own, so running the
// example puts nothing real on the public relay.
//
// AddrFilter is the field to notice. It defaults to iroh.RelayOnlyFilter, which
// keeps direct addresses — often private LAN addresses — out of a world-readable
// record. This example passes them through so there is something to see; see
// go-iroh-address-filtering for the choice itself.
//
// The pkarr relay is a real service, so nothing happens without -live.
// go-iroh-local-infra runs this whole round trip against an in-process pkarr relay
// and needs no network.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strconv"
	"time"

	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// publishedAddr is an RFC 5737 documentation address: publishing it says
// nothing about the host running the example.
const publishedAddr = "203.0.113.10:4433"

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		// -h is a request for the usage message, which the flag package has
		// already printed. It is not a failure.
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("go-iroh-pkarr-publish-resolve", flag.ContinueOnError)
	live := fs.Bool("live", envBool("GO_IROH_LIVE_PKARR", false), "publish to and resolve from n0's public pkarr relay ($GO_IROH_LIVE_PKARR)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !*live {
		fmt.Fprintln(stdout, "pass -live or set GO_IROH_LIVE_PKARR=1 to publish to and resolve from the public pkarr relay")
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

	addr := netip.MustParseAddrPort(publishedAddr)
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
			fmt.Fprintln(stdout, "published endpoint:", item.EndpointID().Z32())
			fmt.Fprintln(stdout, "resolved direct paths:", item.Addr().IPAddrs())
			return nil
		}
		time.Sleep(time.Second)
	}
	if lastErr != nil {
		return fmt.Errorf("resolve published endpoint: %w", lastErr)
	}
	return errors.New("pkarr resolve timed out")
}

// envBool returns the boolean value of the environment variable name, or def
// if it is unset or unparseable, so that a flag and a GO_IROH_ variable
// configure the same thing.
func envBool(name string, def bool) bool {
	v, err := strconv.ParseBool(os.Getenv(name))
	if err != nil {
		return def
	}
	return v
}
