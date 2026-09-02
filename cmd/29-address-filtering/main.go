// Command 29-address-filtering publishes only some of an endpoint's addresses.
//
// An endpoint usually has several ways to be reached: a home relay URL, direct
// UDP addresses on whatever interfaces it holds, and any custom transport
// address an application has added. Publishing all of them to a discovery
// service is rarely what is wanted, because a direct address describes the
// host's local network and a public pkarr relay is readable by anyone. An
// [iroh.AddrFilter] decides which ones leave the machine: it is handed the full
// address set and returns the subset to publish, in priority order.
//
// [iroh.PkarrPublisher] applies its filter to every packet it signs. The
// default is [iroh.RelayOnlyFilter], which is why an endpoint on the public
// deployment advertises a relay rather than a LAN address.
// [iroh.IPOnlyFilter] is the other direction — it drops relays and keeps direct
// and custom addresses, so "ip only" below still reports a custom address. Any
// function of the right shape works too; relayAndCustom is one.
//
// The three lines are the same address set — a relay URL, an IP, and a custom
// address — published under each filter to a local [dnsserver.Server] and then
// resolved back, so what is counted is what a peer would actually see.
//
// The fourth line applies a filter one level up. [iroh.AddressLookupServices]
// is the registry of an endpoint's publishers and resolvers, and its
// SetAddrFilter filters what every registered publisher is handed; a publisher
// with a filter of its own then applies that as well.
//
// Compare 15-pkarr-publish-resolve, which publishes to n0's public
// infrastructure, and 27-local-infra, which replaces the default filter with
// one that publishes everything because on loopback there is nothing to hide.
package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/dnsserver"
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A pkarr relay on loopback, so nothing published here leaves the machine.
	srv := dnsserver.New()
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	relayURL, err := netaddr.ParseRelayURL("https://relay.example.com")
	if err != nil {
		return err
	}
	// One address of each kind, so every filter has something to keep and
	// something to drop.
	data := dns.NewEndpointData(
		netaddr.RelayAddr{URL: relayURL},
		netaddr.IPAddr{Addr: netip.MustParseAddrPort("127.0.0.1:4433")},
		netaddr.NewCustomAddr(42, []byte("memory-link")),
	)

	for _, f := range []struct {
		name   string
		filter iroh.AddrFilter
	}{
		{"relay only", iroh.RelayOnlyFilter},
		{"ip only", iroh.IPOnlyFilter},
		{"relay plus custom", relayAndCustom},
	} {
		relay, ip, custom, err := publish(ctx, httpSrv.URL, httpSrv.Client(), data, f.filter)
		if err != nil {
			return fmt.Errorf("%s: %w", f.name, err)
		}
		fmt.Printf("%s: relay=%d ip=%d custom=%d\n", f.name, relay, ip, custom)
	}

	// The same filter, set on the registry instead of on one publisher. The
	// recorder stands in for a publisher and keeps what it was handed.
	var services iroh.AddressLookupServices
	rec := recorder{}
	services.SetAddrFilter(iroh.IPOnlyFilter)
	services.AddPublisher(&rec)
	services.Publish(data)
	_, ip, custom := countAddrs(rec.data.Addrs())
	fmt.Printf("lookup services filter: ip=%d custom=%d\n", ip, custom)
	return nil
}

// publish signs data under a fresh key, publishes it through filter, and
// reports the addresses a resolver reading the packet back can see.
func publish(ctx context.Context, pkarrURL string, httpClient *http.Client, data dns.EndpointData, filter iroh.AddrFilter) (relay, ip, custom int, err error) {
	sk, err := key.GenerateSecretKey()
	if err != nil {
		return 0, 0, 0, err
	}
	publisher, err := iroh.NewPkarrPublisher(sk, pkarrURL, &iroh.PkarrPublisherConfig{
		TTL:               30,
		RepublishInterval: time.Minute,
		AddrFilter:        filter,
		HTTPClient:        httpClient,
	})
	if err != nil {
		return 0, 0, 0, err
	}
	defer publisher.Close()

	publisher.Publish(data)
	resolver, err := iroh.NewPkarrResolver(pkarrURL, &iroh.PkarrResolverConfig{HTTPClient: httpClient})
	if err != nil {
		return 0, 0, 0, err
	}
	info, err := waitResolve(ctx, resolver, sk.Public().EndpointID())
	if err != nil {
		return 0, 0, 0, err
	}
	relay, ip, custom = countAddrs(info.Data.Addrs())
	return relay, ip, custom, nil
}

// recorder is an [iroh.AddressPublisher] that keeps the last data it was given.
type recorder struct {
	data dns.EndpointData
}

func (r *recorder) Publish(data dns.EndpointData) {
	r.data = data
}

// waitResolve polls the pkarr relay until it answers for id. Publication is
// asynchronous, so the first query can arrive before the packet does.
func waitResolve(ctx context.Context, resolver *iroh.PkarrResolver, id key.EndpointID) (dns.EndpointInfo, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		for item, err := range resolver.Resolve(ctx, id) {
			if err != nil {
				last = err
				break
			}
			return item.EndpointInfo(), nil
		}
		select {
		case <-ctx.Done():
			if last != nil {
				return dns.EndpointInfo{}, fmt.Errorf("resolve %s: %w", id, last)
			}
			return dns.EndpointInfo{}, fmt.Errorf("resolve %s: %w", id, ctx.Err())
		case <-ticker.C:
		}
	}
}

// relayAndCustom is an application's own filter: publish the addresses that do
// not describe the local network.
func relayAndCustom(addrs []netaddr.TransportAddr) []netaddr.TransportAddr {
	out := make([]netaddr.TransportAddr, 0, len(addrs))
	for _, addr := range addrs {
		switch addr.(type) {
		case netaddr.RelayAddr, netaddr.CustomAddr:
			out = append(out, addr)
		}
	}
	return out
}

func countAddrs(addrs []netaddr.TransportAddr) (relay, ip, custom int) {
	for _, addr := range addrs {
		switch addr.(type) {
		case netaddr.RelayAddr:
			relay++
		case netaddr.IPAddr:
			ip++
		case netaddr.CustomAddr:
			custom++
		}
	}
	return relay, ip, custom
}
