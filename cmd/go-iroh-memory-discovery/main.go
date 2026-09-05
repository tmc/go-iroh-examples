// Command go-iroh-memory-discovery dials a peer known only by its endpoint ID.
//
// go-iroh-direct-echo and go-iroh-router-echo hand the client a complete address because
// the client already knows where the server is. Outside a loopback example it
// does not: an endpoint ID is stable and shareable, while the addresses behind
// it change with the network. An address-lookup service closes that gap, and
// [iroh.MemoryLookup] is the smallest one — a map from ID to address that the
// application fills in from whatever out-of-band channel it already has, such
// as a ticket it was pasted.
//
// [iroh.WithAddressLookup] registers the lookup with both endpoints, which is
// all it takes: [iroh.Endpoint.Connect] given an address that carries an ID and
// nothing else asks the lookup services for a path and dials the first answer
// that has one. The client here is never told where the server is.
//
// [iroh.AddressResolver] is one interface with several implementations, and
// only this one is a map: go-iroh-dns-resolve and go-iroh-pkarr-publish-resolve resolve
// through n0's public deployment, go-iroh-local-infra through a pkarr relay in the
// same process, go-iroh-mdns-discovery over the local link.
package main

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const alpn = "go-iroh-examples/memory-discovery/1"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lookup := iroh.NewMemoryLookup()
	var lookups iroh.AddressLookupServices
	lookups.AddResolver(lookup)

	server, err := bind(ctx, iroh.WithAddressLookup(&lookups))
	if err != nil {
		return err
	}
	lookup.AddEndpointAddr(server.Addr())

	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		alpn: handler{},
	}, nil)
	if err != nil {
		return err
	}
	defer router.Shutdown(ctx)

	client, err := bind(ctx, iroh.WithAddressLookup(&lookups))
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	// Register the client too. A lookup service is symmetric: the server
	// resolves the dialing endpoint's ID while it accepts, so an entry that
	// only ever names the server leaves the reverse lookup with nothing to
	// return.
	lookup.AddEndpointAddr(client.Addr())

	// Everything the client knows about the server. Connect finds the rest.
	addr := netaddr.NewEndpointAddr(server.ID())
	fmt.Println("addresses given to Connect:", len(addr.Addrs()))

	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", server.ID().Short(), err)
	}
	defer conn.CloseWithError(0, "")

	reply, err := exchange(ctx, conn, "discovered hello")
	if err != nil {
		return err
	}
	fmt.Println(reply)
	return nil
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

// handler echoes one stream per connection.
type handler struct{}

// Accept implements [iroh.ProtocolHandler].
func (handler) Accept(ctx context.Context, conn *iroh.Conn) error {
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
