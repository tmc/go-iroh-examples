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
// addresses to the mDNS multicast groups, 224.0.0.251 and ff02::fb, and caches
// the announcements other endpoints make, so two machines that share a link of
// either IP version find each other with no relay, no DNS server, and no pkarr
// relay — the reason to reach for it is a LAN, a lab bench, or a conference
// wifi with no route to the internet. The cost is its scope: an announcement
// travels one multicast hop and reaches nobody beyond it. It implements
// [iroh.AddressPublisher] and [iroh.AddressResolver], so it registers with
// [iroh.AddressLookupServices] like any other service and can sit alongside
// pkarr, which covers the peers mDNS cannot see.
//
// This example runs both halves: one endpoint announces itself, a second one
// resolves it by ID and dials the address that comes back. The announcer
// publishes once. A responder answers the PTR query [mdns.Discovery.Resolve]
// sends with its last announcement, so a peer stays findable between
// announcements rather than only while it happens to be repeating itself. The
// service name is per-run rather than the default "irohv1", so that the example
// neither hears nor disturbs real iroh peers sharing the link.
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
	"io"
	"net/netip"
	"os"
	"time"

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

	// startupGrace is how long to wait for a listener to fail to bind before
	// taking its silence for success.
	startupGrace = 250 * time.Millisecond

	// announcementGap is how long the example lets the one announcement pass
	// before anyone is listening for it, so that the lookup below can only
	// succeed through a query and its answer.
	announcementGap = time.Second

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

	// bind binds IPv6 loopback, so the announced address is
	// reachable from this machine and nowhere else. That is deliberate: the two
	// endpoints here share a host, and loopback keeps the dial off the network.
	// A program that wants to be dialed by another machine binds the
	// unspecified address instead, so that its LAN addresses are the ones the
	// announcement carries.
	server, err := bind(ctx,
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

	client, err := bind(ctx,
		iroh.WithSecretKey(clientKey),
		iroh.WithAddressLookup(&seekLookups),
	)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	// Start owns the multicast socket: it binds UDP 5353, joins the group on
	// every interface that is up, and reads until ctx ends. A Discovery
	// announces, answers, and hears only while it is running.
	listen := make(chan error, 2)
	go func() { listen <- announcer.Start(ctx) }()
	if stopped, err := waitListening(listen); stopped {
		// A host with no multicast interface, or one where 5353 cannot be
		// shared, fails here rather than silently hearing nothing.
		fmt.Printf("mDNS listener stopped (%v); %s\n", err, needsMulticast)
		return nil
	}

	accepted := make(chan error, 1)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			accepted <- err
			return
		}
		accepted <- echo(ctx, conn)
	}()

	// What gets announced: the endpoint's own addresses, minus the relay URLs
	// this loopback endpoint does not have.
	data := dns.EndpointDataFromAddr(server.Addr())
	fmt.Println("announcing:", len(data.IPAddrs()), "address(es)")
	announcer.Publish(data)

	// The seeker starts listening only after that announcement has come and
	// gone, so it has nothing cached and no announcement is coming. It finds the
	// peer anyway: Resolve multicasts a PTR query for the service, and the
	// announcer answers it with the announcement it last built. Being
	// discoverable does not mean repeating yourself until somebody hears.
	time.Sleep(announcementGap)
	go func() { listen <- seeker.Start(ctx) }()
	if stopped, err := waitListening(listen); stopped {
		fmt.Printf("mDNS listener stopped (%v); %s\n", err, needsMulticast)
		return nil
	}

	// The seeker knows the ID and nothing else, the way a peer that was handed
	// an ID out of band does. Resolving explicitly rather than dialing the bare
	// ID is what shows where the answer came from.
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

	reply, err := exchange(ctx, conn, "mdns hello")
	if err != nil {
		return err
	}
	if err := <-accepted; err != nil {
		return err
	}
	fmt.Println("reply:", reply)
	fmt.Println("path:", selectedPathKind(conn.Paths()))
	return nil
}

// waitListening reports whether a listener failed to start. Start blocks until
// ctx ends, so silence for startupGrace is as much confirmation as there is.
func waitListening(listen <-chan error) (stopped bool, err error) {
	select {
	case err := <-listen:
		return true, err
	case <-time.After(startupGrace):
		return false, nil
	}
}

// uniqueService returns a DNS-SD service name that only this process uses.
func uniqueService() (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "go-iroh-examples-" + hex.EncodeToString(b[:]), nil
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
