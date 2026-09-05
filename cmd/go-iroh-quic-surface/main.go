// Command go-iroh-quic-surface carries streams and datagrams over the quicconn
// adapter.
//
// An HTTP/3 stack does not want an iroh connection. It wants the QUIC surface
// it was written against: open a bidirectional stream, accept a unidirectional
// one, send a datagram, close with a numeric error code. [quicconn.NewConn]
// wraps an [iroh.Conn] in exactly that surface, so such a stack can sit on
// go-iroh without importing its transport internals or linking a second QUIC
// implementation.
//
// The package implements no protocol of its own, so this example is the shapes
// rather than a conversation: a request and response on a [quicconn.BidiStream],
// a one-way push on a [quicconn.SendStream], and an unreliable datagram, each
// in both directions across one connection.
//
// The streams are the same iroh streams underneath — [quicconn.BidiStream]
// returns the one it wraps — so nothing here is a second transport. What the
// adapter adds is the vocabulary: OpenBidi and AcceptUni instead of
// OpenStreamSync and AcceptUniStream, deadlines on the stream, CancelRead and
// CancelWrite for the resets an HTTP/3 stack sends, and a Close that takes the
// error code rather than a string.
//
// Compare go-iroh-stream-netconn, which puts the same streams behind net.Conn
// for code that speaks that instead.
package main

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/quicconn"
)

// alpn names this exchange. An HTTP/3 stack would offer "h3" here; the adapter
// does not care which, because it carries no protocol of its own.
const alpn = "go-iroh-examples/quic-surface/1"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	server, err := bind(ctx, iroh.WithALPNs(alpn))
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	served := make(chan error, 1)
	go func() { served <- serve(ctx, server) }()

	client, err := bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	raw, err := client.Connect(ctx, server.Addr(), alpn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	// From here on the iroh connection is not used directly: conn is the whole
	// surface, and it is the one an HTTP/3 transport would be handed.
	conn := quicconn.NewConn(raw)

	// A bidirectional stream, with a deadline on it. This is the shape of a
	// request and its response.
	bidi, err := conn.OpenBidi(ctx)
	if err != nil {
		return fmt.Errorf("open bidi: %w", err)
	}
	if err := bidi.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}
	if _, err := io.WriteString(bidi, "GET /surface"); err != nil {
		return fmt.Errorf("write request: %w", err)
	}
	// Closing the send side is the end of the request body; the peer's read
	// returns io.EOF and it can answer.
	if err := bidi.Close(); err != nil {
		return fmt.Errorf("close send side: %w", err)
	}
	response, err := io.ReadAll(bidi)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	fmt.Printf("bidi: %q\n", response)

	// A unidirectional stream the other way: the server pushed it before
	// answering, which is what a server push or a control stream looks like.
	uni, err := conn.AcceptUni(ctx)
	if err != nil {
		return fmt.Errorf("accept uni: %w", err)
	}
	pushed, err := io.ReadAll(uni)
	if err != nil {
		return fmt.Errorf("read uni: %w", err)
	}
	fmt.Printf("uni: %q\n", pushed)

	// A datagram, which may be dropped and is not ordered against the streams.
	if err := conn.SendDatagram([]byte("ping")); err != nil {
		return fmt.Errorf("send datagram: %w", err)
	}
	dgram, err := conn.ReceiveDatagram(ctx)
	if err != nil {
		return fmt.Errorf("receive datagram: %w", err)
	}
	fmt.Printf("datagram: %q\n", dgram)

	// The code is the application's, and reaches the peer as one: HTTP/3 spends
	// this field on H3_NO_ERROR and its neighbours.
	const h3NoError = 0x100
	if err := conn.Close(h3NoError, "done"); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	if err := <-served; err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	fmt.Println("closed: application code", fmt.Sprintf("%#x", h3NoError))
	return nil
}

// serve answers one connection through the same adapter the client uses, which
// is the point: both ends see the QUIC surface and neither reaches past it.
func serve(ctx context.Context, ep *iroh.Endpoint) error {
	raw, err := ep.Accept(ctx)
	if err != nil {
		return err
	}
	conn := quicconn.NewConn(raw)

	bidi, err := conn.AcceptBidi(ctx)
	if err != nil {
		return fmt.Errorf("accept bidi: %w", err)
	}
	request, err := io.ReadAll(bidi)
	if err != nil {
		return fmt.Errorf("read request: %w", err)
	}

	// The push goes out before the answer, so the client has something to
	// accept on the unidirectional side.
	uni, err := conn.OpenUni(ctx)
	if err != nil {
		return fmt.Errorf("open uni: %w", err)
	}
	if _, err := io.WriteString(uni, "pushed by the server"); err != nil {
		return fmt.Errorf("write uni: %w", err)
	}
	if err := uni.Close(); err != nil {
		return fmt.Errorf("close uni: %w", err)
	}

	if _, err := fmt.Fprintf(bidi, "answered %s", request); err != nil {
		return fmt.Errorf("write response: %w", err)
	}
	if err := bidi.Close(); err != nil {
		return fmt.Errorf("close response: %w", err)
	}

	dgram, err := conn.ReceiveDatagram(ctx)
	if err != nil {
		return fmt.Errorf("receive datagram: %w", err)
	}
	if err := conn.SendDatagram(append([]byte("pong for "), dgram...)); err != nil {
		return fmt.Errorf("send datagram: %w", err)
	}

	// Wait for the client's close. The code it passed to Conn.Close arrives
	// here as an application error, which is how the peer learns why the
	// connection ended rather than merely that it did.
	<-raw.Context().Done()
	appErr, ok := iroh.AsApplicationError(context.Cause(raw.Context()))
	if !ok {
		return fmt.Errorf("connection ended without an application code: %w", context.Cause(raw.Context()))
	}
	if appErr.Code != 0x100 {
		return fmt.Errorf("peer closed with code %#x, want %#x", appErr.Code, 0x100)
	}
	return nil
}

// bind binds an endpoint to an ephemeral IPv6 loopback port, then applies opts.
// Loopback keeps the example self-contained: no relay, no DNS, no network.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
}
