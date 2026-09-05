// Command go-iroh-hooks observes dialing from inside the endpoint.
//
// [iroh.EndpointHooks] is the dialing side's counterpart to the accepting-side
// decision points in go-iroh-manual-incoming, go-iroh-source-validation, and
// go-iroh-incoming-filter. [iroh.WithHooks] registers an implementation on an
// endpoint, and its two methods bracket a dial:
//
//   - BeforeConnect runs before [iroh.Endpoint.Connect] resolves an address or
//     sends a packet, with the [netaddr.EndpointAddr] and ALPN the caller
//     asked for. This is intent: the ID in it is the peer the program wants.
//   - AfterHandshake runs once the connection is verified, with the
//     [iroh.Conn]. This is outcome: the ID in it is the peer that proved it
//     holds the matching secret key.
//
// Either may return an error, and the error fails the dial; an AfterHandshake
// error built by [iroh.RejectHandshake] also closes the connection with an
// application code the peer can read (go-iroh-close-codes). Hooks are therefore the
// place for policy that must hold for every dial an endpoint makes — an
// allow-list, a rate limit, an audit trail — rather than a check copied to each
// call site and forgotten at one of them.
//
// The pair is not symmetric. Only a dialing endpoint runs BeforeConnect, but
// AfterHandshake runs for accepted connections as well, so a hook installed on
// a server sees handshakes it never asked for. The endpoint here only dials.
//
// The two lines it prints are the same dial before and after, and they match:
// an endpoint ID is a public key, so reaching a peer other than the one dialed
// is a failed handshake rather than a silent substitution.
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

const alpn = "go-iroh-examples/hooks/1"

// hookLog records what each hook saw. Both hooks run on the goroutine that
// called Connect, so an endpoint that only dials needs no locking here; one
// that also accepts, or that uses the 0-RTT paths, is called from goroutines it
// does not control.
type hookLog struct {
	before string
	after  string
}

func (h *hookLog) BeforeConnect(_ context.Context, addr netaddr.EndpointAddr, alpn string) error {
	h.before = fmt.Sprintf("%s %s", addr.ID.Short(), alpn)
	return nil
}

func (h *hookLog) AfterHandshake(_ context.Context, conn *iroh.Conn) error {
	h.after = fmt.Sprintf("%s %s", conn.RemoteID().Short(), conn.ALPN())
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server, err := bind(ctx, iroh.WithALPNs(alpn))
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	served := make(chan error, 1)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			served <- fmt.Errorf("accept: %w", err)
			return
		}
		served <- echo(ctx, conn)
	}()

	hooks := new(hookLog)
	client, err := bind(ctx, iroh.WithHooks(hooks))
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, server.Addr(), alpn)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", server.ID().Short(), err)
	}
	defer conn.CloseWithError(0, "")

	reply, err := exchange(ctx, conn, "hooked hello")
	if err != nil {
		return err
	}
	if err := <-served; err != nil {
		return fmt.Errorf("server: %w", err)
	}
	fmt.Println(reply)
	fmt.Println("before:", hooks.before)
	fmt.Println("after:", hooks.after)
	return nil
}

// bind binds an endpoint to an ephemeral IPv6 loopback port. Loopback keeps
// the example self-contained: no relay, no DNS, no network access.
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
