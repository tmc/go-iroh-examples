// Command go-iroh-direct-echo connects two endpoints and echoes one message.
//
// This is the smallest complete iroh program: bind two endpoints, dial one from
// the other under an agreed ALPN, open a bidirectional stream, and read the
// reply. Most of the examples that follow are a variation on these thirty
// lines.
//
// The server owns its accept loop. [iroh.Endpoint.Accept] returns the next
// verified connection and the program decides what to do with it, including
// what to do when the handler fails — here the error travels back to run on a
// buffered channel. That shape suits a server that speaks one protocol;
// go-iroh-router-echo hands the same loop, and the error handling around it, to
// [iroh.Router].
//
// [iroh.WithALPNs] declares which protocols the endpoint accepts. A dial naming
// an ALPN the server did not declare fails during the TLS handshake, before
// either side has a connection to reject.
//
// Both endpoints bind IPv6 loopback and the client is told the server's address
// outright, so there is no relay, no discovery, and no network involved.
// go-iroh-memory-discovery drops the assumption that the client already knows where
// the server is.
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

const alpn = "go-iroh-examples/direct-echo/1"

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(stdout io.Writer) error {
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

	reply, err := exchange(ctx, conn, "direct hello")
	if err != nil {
		return err
	}
	if err := <-served; err != nil {
		return fmt.Errorf("server: %w", err)
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
