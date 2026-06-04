package main

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/netip"
	"time"

	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/dnsserver"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv := dnsserver.New()
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	relayURL, err := netaddr.ParseRelayURL("https://relay.example.com")
	if err != nil {
		panic(err)
	}
	data := dns.NewEndpointData(
		netaddr.RelayAddr{URL: relayURL},
		netaddr.IPAddr{Addr: netip.MustParseAddrPort("127.0.0.1:4433")},
		netaddr.NewCustomAddr(42, []byte("memory-link")),
	)

	run := func(name string, filter iroh.AddrFilter) {
		sk, err := key.GenerateSecretKey()
		if err != nil {
			panic(err)
		}
		publisher, err := iroh.NewPkarrPublisher(sk, httpSrv.URL, &iroh.PkarrPublisherConfig{
			TTL:               30,
			RepublishInterval: time.Minute,
			AddrFilter:        filter,
			HTTPClient:        httpSrv.Client(),
		})
		if err != nil {
			panic(err)
		}
		defer publisher.Close()

		publisher.Publish(data)
		resolver, err := iroh.NewPkarrResolver(httpSrv.URL, &iroh.PkarrResolverConfig{HTTPClient: httpSrv.Client()})
		if err != nil {
			panic(err)
		}
		info, err := waitResolve(ctx, resolver, sk.Public().EndpointID())
		if err != nil {
			panic(err)
		}
		relay, ip, custom := countAddrs(info.Data.Addrs())
		fmt.Printf("%s: relay=%d ip=%d custom=%d\n", name, relay, ip, custom)
	}

	run("relay only", iroh.RelayOnlyFilter)
	run("ip only", iroh.IPOnlyFilter)
	run("relay plus custom", relayAndCustom)

	var services iroh.AddressLookupServices
	rec := recorder{}
	services.SetAddrFilter(iroh.IPOnlyFilter)
	services.AddPublisher(&rec)
	services.Publish(data)
	_, ip, custom := countAddrs(rec.data.Addrs())
	fmt.Printf("lookup services filter: ip=%d custom=%d\n", ip, custom)
}

type recorder struct {
	data dns.EndpointData
}

func (r *recorder) Publish(data dns.EndpointData) {
	r.data = data
}

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
				return dns.EndpointInfo{}, last
			}
			return dns.EndpointInfo{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

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
