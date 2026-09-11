// Command go-iroh-router-echo serves the same echo through a router.
//
// [iroh.Router] is the accept loop go-iroh-direct-echo writes out by hand. It
// accepts connections on an endpoint, reads the ALPN each one negotiated, and
// runs the [iroh.ProtocolHandler] registered for that ALPN in a goroutine of
// its own. A handler that returns an error ends only its own connection and the
// error is logged; a handler that panics is recovered and logged; the accept
// loop continues either way. go-iroh-direct-echo has to decide all of that itself.
//
// The handler map given to [iroh.NewRouter] also registers the endpoint's
// ALPNs, so there is no [iroh.WithALPNs] here — and passing it as well is an
// error, because the endpoint must not already be listening when the router
// starts.
//
// One handler for one protocol is the degenerate case, shown here so that the
// only difference from go-iroh-direct-echo is the accept loop. go-iroh-multi-alpn
// registers two handlers, which is what a router is for; go-iroh-incoming-filter
// puts admission control in front of one.
package main

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/iroh"
)

const alpn = "go-iroh-examples/router-echo/1"

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(stdout io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server, err := bind(ctx)
	if err != nil {
		return err
	}

	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		alpn: handler{},
	}, nil)
	if err != nil {
		return err
	}
	defer router.Shutdown(ctx)

	client, err := bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, server.Addr(), alpn)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", server.ID().Short(), err)
	}
	defer conn.CloseWithError(0, "")

	reply, err := exchange(ctx, conn, "router hello")
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, reply)
	return nil
}

// bind binds an endpoint to an ephemeral IPv6 loopback port, so that the
// example is self-contained: no relay, no DNS, no network access. Options given
// by the caller are applied after the bind address and may override it.
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

// handler serves one echo exchange per connection.
type handler struct{}

// Accept implements [iroh.ProtocolHandler].
func (handler) Accept(ctx context.Context, conn *iroh.Conn) error {
	return echo(ctx, conn)
}
