// Command go-iroh-alpn-negotiation agrees on a protocol version at dial time.
//
// Every other example in this repository hard-codes one ALPN ending in /1. A
// protocol that lives long enough grows a /2, and for a while both versions are
// deployed at once. ALPN is where the two ends agree which one to speak: the
// server names the versions it still accepts with [iroh.WithALPNs], the client
// names the one it wants in [iroh.Endpoint.Connect], and [iroh.Conn.ALPN]
// reports what was agreed. go-iroh-multi-alpn shows one endpoint serving two
// unrelated protocols; this shows two versions of one protocol, and what a
// dialer sees when there is no version in common.
//
// The negotiation is one-sided by construction. Connect takes a single ALPN, not
// a preference list, so a dial offers exactly one protocol and the server either
// accepts it or refuses the connection. There is no server-side preference to
// override the client's, and the order of the strings given to WithALPNs is not
// a ranking: it is the set of protocols the endpoint will accept. Version
// negotiation is therefore a client-side loop — dial the newest version, and on
// refusal try the next one down — which is what preferred does here.
//
// A refusal costs a round trip and arrives as an error from Connect, before any
// stream exists. Unlike a key-exchange mismatch (go-iroh-key-exchange), which
// matches [iroh.ErrTLSHandshakeFailure], an ALPN mismatch has no exported error
// to match: the peer sends TLS alert 120, no_application_protocol, which arrives
// as a QUIC CRYPTO_ERROR 0x178 whose type lives in an internal package. A
// program that must tell this failure from an unreachable peer has to match the
// error text, as noVersionInCommon does, and should treat a text match as a hint
// rather than a contract.
//
// The practical consequence for a protocol author: keep serving the old ALPN
// until the peers that speak it are gone, because a client that only knows the
// old version cannot be told about the new one — it just fails to connect.
package main

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strings"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

// Two versions of one protocol. v2 answers with the version tag the client can
// see; v1 is the older, plainer reply that existing peers still expect.
const (
	v1 = "go-iroh-examples/alpn-negotiation/1"
	v2 = "go-iroh-examples/alpn-negotiation/2"

	// v3 is not served. A client that knows only this version has nothing in
	// common with the server.
	v3 = "go-iroh-examples/alpn-negotiation/3"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The oldest version is listed first on purpose: WithALPNs is a set, and
	// the dials below show the client's choice is what decides.
	server, err := bind(ctx, iroh.WithALPNs(v1, v2))
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)
	go serve(ctx, server)

	cases := []struct {
		name  string
		wants []string // newest acceptable version first
	}{
		{"modern client", []string{v3, v2, v1}},
		{"pinned to v2", []string{v2}},
		{"legacy client", []string{v1}},
		{"future-only client", []string{v3}},
	}
	for _, c := range cases {
		alpn, reply, err := preferred(ctx, server.Addr(), c.wants, "hello")
		if err != nil {
			return fmt.Errorf("%s: %w", c.name, err)
		}
		if alpn == "" {
			fmt.Printf("%s: no version in common with the server\n", c.name)
			continue
		}
		fmt.Printf("%s: negotiated %s, reply %q\n", c.name, version(alpn), reply)
	}
	return nil
}

// preferred dials addr for each ALPN in wants, newest first, and runs one
// exchange over the first that the server accepts. It returns the negotiated
// ALPN and the reply, or an empty ALPN if the server accepted none of them.
// Errors other than a version mismatch end the search, because retrying a lower
// version cannot fix an unreachable peer.
func preferred(ctx context.Context, addr netaddr.EndpointAddr, wants []string, msg string) (alpn, reply string, err error) {
	client, err := bind(ctx)
	if err != nil {
		return "", "", err
	}
	defer client.Shutdown(ctx)

	for _, want := range wants {
		conn, err := client.Connect(ctx, addr, want)
		if noVersionInCommon(err) {
			continue
		}
		if err != nil {
			return "", "", err
		}
		defer conn.CloseWithError(0, "")

		// conn.ALPN is what was agreed. It can only be want here, since that
		// is the single protocol the dial offered, but reading it back is how
		// a handler that did not do the dialing learns the version.
		reply, err := exchange(ctx, conn, msg)
		if err != nil {
			return "", "", err
		}
		return conn.ALPN(), reply, nil
	}
	return "", "", nil
}

// noVersionInCommon reports whether err is a dial refused because the server
// accepts none of the dialed ALPN. go-iroh exports no error for this, so the
// test is on the text of the TLS alert the peer sent; see the package comment.
func noVersionInCommon(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no application protocol")
}

// serve answers on ep until ctx ends or ep shuts down, dispatching on the
// version each connection negotiated.
func serve(ctx context.Context, ep *iroh.Endpoint) {
	for {
		conn, err := ep.Accept(ctx)
		if err != nil {
			return
		}
		go func() {
			// The accepting side learns the version the same way the dialing
			// side does, from the connection.
			_ = transform(ctx, conn, answer(conn.ALPN()))
		}()
	}
}

// answer returns the reply function for a negotiated version. v2 tags its
// replies; v1 predates the tag and must keep answering as it always did.
func answer(alpn string) func(string) string {
	if alpn == v2 {
		return func(msg string) string { return "v2 " + msg }
	}
	return func(msg string) string { return msg }
}

// version shortens an ALPN to its trailing version for printing.
func version(alpn string) string {
	return "v" + alpn[strings.LastIndex(alpn, "/")+1:]
}

// bind binds an endpoint to an ephemeral IPv6 loopback port, which keeps the
// example self-contained: no relay, no DNS, no network access. Options given by
// the caller are applied after the bind address, so they may override it.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
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

// transform accepts one bidirectional stream, reads it to EOF, and writes back
// f applied to what it read. It is the server half of exchange.
func transform(ctx context.Context, conn *iroh.Conn, f func(string) string) error {
	s, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	b, err := io.ReadAll(s)
	if err != nil {
		return err
	}
	if _, err := s.Write([]byte(f(string(b)))); err != nil {
		return err
	}
	return s.Close()
}
