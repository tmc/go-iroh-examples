// Command go-iroh-multi-alpn serves two protocols from one endpoint.
//
// An endpoint is one socket and one identity, but it is not one protocol. Every
// entry in the map given to [iroh.NewRouter] registers an
// [iroh.ProtocolHandler] under an ALPN, and each incoming connection goes to
// the handler whose ALPN it negotiated. go-iroh-router-echo is this example with one
// entry; the second entry is all that separates them.
//
// Dispatch is by exact string match, decided in the TLS handshake. There is no
// fallback handler and no prefix matching, so a dial for an unregistered ALPN
// fails before a connection exists, and the two protocols can never see each
// other's bytes. A protocol is versioned by putting the version in the string —
// both of these end in /1 — and a server that speaks two versions registers
// both and keeps the old handler until its peers have moved.
//
// The client opens one connection per protocol because that is what an ALPN is:
// chosen at dial time and fixed for the life of the connection. Carrying
// several conversations inside one protocol is the job of streams instead
// (go-iroh-uni-streams, go-iroh-framed-messages).
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
)

const (
	echoALPN  = "go-iroh-examples/multi-alpn/echo/1"
	upperALPN = "go-iroh-examples/multi-alpn/upper/1"
)

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
		echoALPN:  handler{},
		upperALPN: handler{transform: strings.ToUpper},
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

	addr := server.Addr()
	echoConn, err := client.Connect(ctx, addr, echoALPN)
	if err != nil {
		return fmt.Errorf("connect for %s: %w", echoALPN, err)
	}
	defer echoConn.CloseWithError(0, "")

	upperConn, err := client.Connect(ctx, addr, upperALPN)
	if err != nil {
		return fmt.Errorf("connect for %s: %w", upperALPN, err)
	}
	defer upperConn.CloseWithError(0, "")

	// The same request over both connections. Only the ALPN differs, so the
	// two replies are the dispatch.
	echoReply, err := exchange(ctx, echoConn, "multi hello")
	if err != nil {
		return err
	}
	upperReply, err := exchange(ctx, upperConn, "multi hello")
	if err != nil {
		return err
	}

	fmt.Fprintln(stdout, "echo:", echoReply)
	fmt.Fprintln(stdout, "upper:", upperReply)
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

// handler serves one transform exchange per connection. The zero handler
// echoes.
type handler struct {
	// transform maps a request to a response. If nil, the request is echoed.
	transform func(string) string
}

// Accept implements [iroh.ProtocolHandler].
func (h handler) Accept(ctx context.Context, conn *iroh.Conn) error {
	f := h.transform
	if f == nil {
		f = func(s string) string { return s }
	}
	return transform(ctx, conn, f)
}
