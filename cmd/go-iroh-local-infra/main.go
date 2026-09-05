// Command go-iroh-local-infra runs a private iroh network on loopback.
//
// The public deployment has two pieces of infrastructure behind it: relay
// servers, which forward packets for endpoints that cannot reach each other
// directly, and a pkarr relay, which stores the signed DNS packets endpoints
// publish so that a peer can be dialed by ID alone. Both ship in this module as
// libraries, so a test, a CI job, or an air-gapped network can run its own.
//
// This example starts [relayserver.Server] and [dnsserver.Server] on httptest
// listeners, points two endpoints at them with [relay.ModeCustomURLs] and a
// [iroh.PkarrPublisher]/[iroh.PkarrResolver] pair, and then dials a peer the
// caller knows only by endpoint ID: the ID goes to the resolver, the resolver
// returns an address, and the address is dialed. Nothing leaves the machine.
//
// Compare go-iroh-pkarr-publish-resolve, which does the same against n0's public
// infrastructure and therefore needs the network.
package main

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/dnsserver"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/metrics"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
	"github.com/tmc/go-iroh/relayserver"
)

const alpn = "go-iroh-examples/local-infra/1"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The two servers. relayserver forwards datagrams between endpoints;
	// dnsserver stores pkarr packets and answers DNS TXT queries for them.
	relaySrv := relayserver.New()
	relayHTTP := httptest.NewServer(relaySrv)
	defer relayHTTP.Close()

	dnsSrv := dnsserver.New()
	dnsHTTP := httptest.NewServer(dnsSrv)
	defer dnsHTTP.Close()

	relayURL, err := netaddr.ParseRelayURL(relayHTTP.URL)
	if err != nil {
		return err
	}
	mode := relay.ModeCustomURLs(relayURL)
	pkarrURL := dnsHTTP.URL + "/pkarr"
	fmt.Println("relay:", relayURL)
	fmt.Println("pkarr:", pkarrURL)

	// The server publishes its own address to the local pkarr relay. The
	// default AddrFilter publishes relay addresses only, which is what a real
	// deployment wants; here it means the client learns the relay URL and
	// reaches the server through it.
	serverKey, err := key.GenerateSecretKey()
	if err != nil {
		return err
	}
	publisher, err := iroh.NewPkarrPublisher(serverKey, pkarrURL, &iroh.PkarrPublisherConfig{
		// The default filter publishes relay addresses only. On loopback there
		// is nothing to hide, and publishing the direct address too lets the
		// client take the direct path once it has one.
		AddrFilter:        func(addrs []netaddr.TransportAddr) []netaddr.TransportAddr { return addrs },
		RepublishInterval: time.Hour,
	})
	if err != nil {
		return err
	}
	defer publisher.Close()

	var serverLookups iroh.AddressLookupServices
	serverLookups.AddPublisher(publisher)

	server, err := bind(ctx,
		iroh.WithSecretKey(serverKey),
		iroh.WithALPNs(alpn),
		iroh.WithRelayMode(mode),
		iroh.WithAddressLookup(&serverLookups),
	)
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	// Online waits until the endpoint has a relay home, which is what makes its
	// published address dialable.
	if err := server.Online(ctx); err != nil {
		return fmt.Errorf("server online: %w", err)
	}

	// The endpoint republishes on its own whenever its address changes; push
	// once here so the first dial does not wait for that.
	publisher.Publish(dns.EndpointDataFromAddr(server.Addr()))

	accepted := make(chan error, 1)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			accepted <- err
			return
		}
		accepted <- echo(ctx, conn)
	}()

	// The client resolves through the same local relay and knows only the ID.
	resolver, err := iroh.NewPkarrResolver(pkarrURL, nil)
	if err != nil {
		return err
	}
	var clientLookups iroh.AddressLookupServices
	clientLookups.AddResolver(resolver)

	client, err := bind(ctx,
		iroh.WithRelayMode(mode),
		iroh.WithAddressLookup(&clientLookups),
	)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)
	if err := client.Online(ctx); err != nil {
		return fmt.Errorf("client online: %w", err)
	}

	// Publication is asynchronous: the endpoint pushes a packet when its own
	// address settles. Wait for the record to appear, the way a peer that just
	// received an ID out of band would retry. Connect resolves the ID itself
	// through the same lookup, but it asks once — a dial before the relay has
	// the record fails with [iroh.ErrNoAddress] rather than waiting for it.
	if err := waitPublished(ctx, resolver, server.ID()); err != nil {
		return err
	}

	conn, err := client.Connect(ctx, netaddr.NewEndpointAddr(server.ID()), alpn)
	if err != nil {
		return fmt.Errorf("dial by id: %w", err)
	}
	defer conn.CloseWithError(0, "")

	reply, err := exchange(ctx, conn, "local infra hello")
	if err != nil {
		return err
	}
	if err := <-accepted; err != nil {
		return err
	}
	fmt.Println("reply:", reply)
	fmt.Println("path:", selectedPathKind(conn.Paths()))

	// Both servers implement the metrics source interface, so a deployment can
	// scrape them through one registry.
	reg := metrics.NewRegistry()
	if err := reg.Register("dnsserver", dnsSrv); err != nil {
		return err
	}
	if err := reg.Register("relayserver", relaySrv); err != nil {
		return err
	}
	if err := reg.WriteOpenMetrics(io.Discard); err != nil {
		return err
	}
	fmt.Println("dns snapshot:", dnsSrv.Snapshot())
	fmt.Println("relay snapshot:", relaySrv.Snapshot())
	return nil
}

// waitPublished polls the pkarr relay until it has a record for id.
func waitPublished(ctx context.Context, r *iroh.PkarrResolver, id key.EndpointID) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		for item, err := range r.Resolve(ctx, id) {
			if err == nil && item.EndpointID().Equal(id) {
				fmt.Println("resolved by id:", len(item.Addr().Addrs()), "address(es)")
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("pkarr relay never published %s: %w", id, ctx.Err())
		case <-ticker.C:
		}
	}
}

// bind binds an endpoint to an ephemeral IPv6 loopback port, which keeps the
// example self-contained: it needs no relay, no DNS, and no network access.
// Additional options are applied after the bind address, so a caller may
// override it.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
}

// echo accepts one bidirectional stream, reads it to EOF, and writes back what
// it read. It is the server half of exchange.
func echo(ctx context.Context, conn *iroh.Conn) error {
	s, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	b, err := io.ReadAll(s)
	if err != nil {
		return err
	}
	if _, err := s.Write(b); err != nil {
		return err
	}
	return s.Close()
}

// exchange opens a bidirectional stream, writes msg, closes the write side, and
// reads the reply until EOF.
func exchange(ctx context.Context, conn *iroh.Conn, msg string) (string, error) {
	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return "", err
	}
	if _, err := s.Write([]byte(msg)); err != nil {
		return "", err
	}
	// Half-close: the peer reads to EOF and replies on the same stream.
	if err := s.CloseWrite(); err != nil {
		return "", err
	}
	b, err := io.ReadAll(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// selectedPathKind names the transport of the selected path: "relay", the
// network of a direct address, "unknown", or "none" if no path is selected.
func selectedPathKind(paths []iroh.PathInfo) string {
	for _, p := range paths {
		if !p.Selected {
			continue
		}
		if p.Relayed {
			return "relay"
		}
		if p.HasAddr {
			return p.Addr.Network()
		}
		return "unknown"
	}
	return "none"
}
