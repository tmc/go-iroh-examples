// Command go-iroh-manual-incoming accepts a connection one stage at a time.
//
// [iroh.Endpoint.Accept], used by go-iroh-direct-echo, runs three stages and hands
// back only the result. This example unrolls them:
//
//   - [iroh.Endpoint.AcceptIncoming] returns an [iroh.Incoming] as soon as a
//     handshake arrives. Nothing is verified yet and the peer's identity is
//     unknown; the choice at this point is [iroh.Incoming.Accept],
//     [iroh.Incoming.Refuse], or [iroh.Incoming.Ignore].
//   - [iroh.Incoming.Accept] continues the handshake and yields an
//     [iroh.Accepting].
//   - [iroh.Accepting.ALPN] reports the negotiated protocol, and
//     [iroh.Accepting.Connection] waits for the verified [iroh.Conn].
//
// Take the stages apart when there is a decision to make between them: dropping
// a peer before paying for a handshake, or choosing a code path from the ALPN
// before the connection exists. The cheap facts arrive first and the trustworthy
// ones last — the remote address is available on the [iroh.Incoming] and is
// only as good as source validation makes it (go-iroh-source-validation), while the
// remote ID exists only once [iroh.Accepting.Connection] returns.
//
// A router unrolls the same stages internally. go-iroh-incoming-filter exposes both
// decision points as callbacks, which is the form to prefer over this loop
// unless the sequencing itself is the point.
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

const alpn = "go-iroh-examples/manual-incoming/1"

// inbound is what the accepting side learns, in the order it learns it.
type inbound struct {
	alpn   string
	remote string
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

	seen := make(chan inbound, 1)
	served := make(chan error, 1)
	go func() {
		served <- acceptOne(ctx, server, seen)
	}()

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

	reply, err := exchange(ctx, conn, "manual hello")
	if err != nil {
		return err
	}
	if err := <-served; err != nil {
		return fmt.Errorf("server: %w", err)
	}
	got := <-seen
	fmt.Println(reply)
	fmt.Printf("%s from %s\n", got.alpn, got.remote)
	return nil
}

// acceptOne walks one incoming connection through the three stages, reports
// what each stage revealed on seen, and then echoes.
func acceptOne(ctx context.Context, server *iroh.Endpoint, seen chan<- inbound) error {
	in, err := server.AcceptIncoming(ctx)
	if err != nil {
		return fmt.Errorf("accept incoming: %w", err)
	}
	accepting, err := in.Accept()
	if err != nil {
		return fmt.Errorf("accept from %s: %w", in.RemoteAddr(), err)
	}
	negotiated, err := accepting.ALPN(ctx)
	if err != nil {
		return fmt.Errorf("negotiate alpn: %w", err)
	}
	conn, err := accepting.Connection(ctx)
	if err != nil {
		return fmt.Errorf("await verified connection: %w", err)
	}
	seen <- inbound{alpn: negotiated, remote: conn.RemoteID().Short()}
	return echo(ctx, conn)
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
