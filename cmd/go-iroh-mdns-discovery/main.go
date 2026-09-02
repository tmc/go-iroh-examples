// Command go-iroh-mdns-discovery finds a peer on the local link with multicast DNS.
//
// An endpoint ID names a peer but says nothing about where it is. The other
// discovery examples close that gap with a service both sides agree on:
// go-iroh-memory-discovery hands addresses around inside one process, which is what
// a test wants; go-iroh-dns-resolve and go-iroh-pkarr-publish-resolve ask n0's public
// DNS and pkarr infrastructure; go-iroh-local-infra runs that same pkarr relay
// privately on loopback. Every one of them needs something in the middle.
//
// [mdns.Discovery] needs nothing. It announces the local endpoint's direct
// addresses to 224.0.0.251 and caches the announcements other endpoints make,
// so two machines on one link find each other with no relay, no DNS server,
// and no pkarr relay — the reason to reach for it is a LAN, a lab bench, or a
// conference wifi with no route to the internet. The cost is its scope: an
// announcement travels one multicast hop and reaches nobody beyond it. It
// implements [iroh.AddressPublisher] and [iroh.AddressResolver], so it
// registers with [iroh.AddressLookupServices] like any other service and can
// sit alongside pkarr, which covers the peers mDNS cannot see.
//
// This example runs both halves: one endpoint announces itself, a second one
// resolves it by ID and dials the address that comes back. Two details of
// go-iroh v0.1.0 shape the code. Announcements carry the addresses passed to
// [mdns.Discovery.Publish], so the announcer republishes on a ticker rather
// than once — [mdns.Discovery] does not answer the query
// [mdns.Discovery.Resolve] sends, and a peer is discovered when its next
// announcement arrives. And the service name is per-run rather than the
// default "irohv1", so that the example neither hears nor disturbs real iroh
// peers sharing the link.
//
// mDNS is the one discovery mechanism that depends on the host's networking:
// it needs an interface that is up and multicast-capable, and it needs UDP port
// 5353. Where that is unavailable — a sandbox, a container with no multicast —
// this example says so and exits 0 rather than failing.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/iroh/mdns"
	"github.com/tmc/go-iroh/key"
)

const (
	alpn = "go-iroh-examples/mdns-discovery/1"

	// discoverWait bounds the lookup. Discovery is best-effort: an announcement
	// can be dropped, and on a host that cannot multicast none ever arrives.
	discoverWait = 15 * time.Second

	// announceInterval is how often the announcer repeats itself while the
	// seeker is listening.
	announceInterval = 250 * time.Millisecond

	// needsMulticast explains every way this example gives up.
	needsMulticast = "mDNS needs a multicast-capable interface"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A service name unique to this run. Real deployments keep the default,
	// which is the name the Rust implementation uses, so that Go and Rust
	// endpoints see each other; a self-contained example that talks only to
	// itself should not join that conversation.
	service, err := uniqueService()
	if err != nil {
		return err
	}
	fmt.Println("default service:", mdns.DefaultServiceName)

	// The announcing side. Discovery needs the endpoint ID up front, so the key
	// is generated before the endpoint is bound.
	announcerKey, err := key.GenerateSecretKey()
	if err != nil {
		return err
	}
	announcer := mdns.New(announcerKey.Public().EndpointID(), mdns.WithServiceName(service))

	// Registering the Discovery as a publisher is what makes announcing
	// automatic: the endpoint pushes its addresses to every publisher whenever
	// they change.
	var announceLookups iroh.AddressLookupServices
	announceLookups.AddPublisher(announcer)

	// exampleutil.Bind binds IPv6 loopback, so the announced address is
	// reachable from this machine and nowhere else. That is deliberate: the two
	// endpoints here share a host, and loopback keeps the dial off the network.
	// A program that wants to be dialed by another machine binds the
	// unspecified address instead, so that its LAN addresses are the ones the
	// announcement carries.
	server, err := exampleutil.Bind(ctx,
		iroh.WithSecretKey(announcerKey),
		iroh.WithALPNs(alpn),
		iroh.WithAddressLookup(&announceLookups),
	)
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	// The seeking side. WithPassive keeps it listening only: it resolves peers
	// without advertising itself, which is what a client that nobody dials
	// wants. WithLookupTimeout bounds Resolve after a cache miss.
	clientKey, err := key.GenerateSecretKey()
	if err != nil {
		return err
	}
	seeker := mdns.New(clientKey.Public().EndpointID(),
		mdns.WithServiceName(service),
		mdns.WithPassive(true),
		mdns.WithLookupTimeout(discoverWait),
	)
	var seekLookups iroh.AddressLookupServices
	seekLookups.AddResolver(seeker)

	client, err := exampleutil.Bind(ctx,
		iroh.WithSecretKey(clientKey),
		iroh.WithAddressLookup(&seekLookups),
	)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	// Start owns the multicast socket: it binds UDP 5353, joins the group on
	// every interface that is up, and reads until ctx ends. Resolve only sees
	// remote announcements while it is running.
	listen := make(chan error, 2)
	go func() { listen <- announcer.Start(ctx) }()
	go func() { listen <- seeker.Start(ctx) }()
	select {
	case err := <-listen:
		if err != nil {
			// A host with no multicast interface, or one where 5353 cannot be
			// shared, fails here rather than silently hearing nothing.
			fmt.Printf("mDNS listener stopped (%v); %s\n", err, needsMulticast)
			return nil
		}
	case <-time.After(announceInterval):
	}

	accepted := make(chan error, 1)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			accepted <- err
			return
		}
		accepted <- exampleutil.Echo(ctx, conn)
	}()

	// What gets announced: the endpoint's own addresses, minus the relay URLs
	// this loopback endpoint does not have.
	data := dns.EndpointDataFromAddr(exampleutil.Addr(server))
	fmt.Println("announcing:", len(data.IPAddrs()), "address(es)")
	go announce(ctx, announcer, data)

	// The seeker knows the ID and nothing else, the way a peer that was handed
	// an ID out of band does. Note that the address has to be resolved before
	// the dial: Connect only tries the addresses in the EndpointAddr it is
	// given.
	item, ok, err := discover(ctx, seeker, server.ID())
	if err != nil {
		return err
	}
	if !ok {
		fmt.Printf("no peer discovered over mDNS in %s; %s\n", discoverWait, needsMulticast)
		return nil
	}
	addr := item.Addr()
	fmt.Println("discovered by id:", len(addr.Addrs()), "address(es)")
	fmt.Println("provenance:", item.Provenance())
	fmt.Println("same endpoint:", addr.ID.Equal(server.ID()))

	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		return fmt.Errorf("dial discovered address: %w", err)
	}
	defer conn.CloseWithError(0, "")

	reply, err := exampleutil.Exchange(ctx, conn, "mdns hello")
	if err != nil {
		return err
	}
	if err := <-accepted; err != nil {
		return err
	}
	fmt.Println("reply:", reply)
	fmt.Println("path:", exampleutil.SelectedPathKind(conn.Paths()))
	return nil
}

// uniqueService returns a DNS-SD service name that only this process uses.
func uniqueService() (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "go-iroh-examples-" + hex.EncodeToString(b[:]), nil
}

// announce republishes data until ctx ends. A single announcement is enough
// only if a listener happens to be resolving when it arrives; repeating is what
// an mDNS responder does, and it is what makes the lookup below reliable.
func announce(ctx context.Context, d *mdns.Discovery, data dns.EndpointData) {
	ticker := time.NewTicker(announceInterval)
	defer ticker.Stop()
	for {
		d.Publish(data)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// discover returns the first item d resolves for id. It reports false if the
// lookup timeout passes with no announcement for id, which is what a host
// without working multicast looks like.
func discover(ctx context.Context, d *mdns.Discovery, id key.EndpointID) (iroh.Item, bool, error) {
	for item, err := range d.Resolve(ctx, id) {
		if err != nil {
			return iroh.Item{}, false, err
		}
		return item, true, nil
	}
	return iroh.Item{}, false, nil
}
