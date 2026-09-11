// Command go-iroh-stream-netconn uses an iroh stream as a net.Conn.
//
// [iroh.Conn.OpenStreamConn] and [iroh.Conn.AcceptStreamConn] wrap one
// bidirectional QUIC stream as a [net.Conn]. That is what lets code written
// against the standard library — bufio, encoding/json, net/textproto,
// net/http, crypto/tls — run over iroh without being told what is underneath.
//
// The wrapper also changes how cancellation is expressed. An [iroh.Stream]
// takes a [context.Context] per operation, which is what a program written for
// iroh wants; a [net.Conn] instead has SetDeadline, SetReadDeadline, and
// SetWriteDeadline. A passed deadline unblocks a Read or Write with an error
// matching [os.ErrDeadlineExceeded] that reports Timeout as a [net.Error],
// which is what code written for TCP already expects. Deadlines are absolute
// times rather than durations, so a loop that reads repeatedly sets a new one
// before each call rather than once at the top.
//
// The exchange is one line up and an uppercased line back, with both ends
// reading through bufio under a five-second deadline. Nothing here is iroh's
// own API except the two calls that produce the connections.
//
// Half-close comes with the wrapper. [net.Conn] does not declare CloseWrite, so
// the standard library type-asserts for it — that is how net/http and
// httputil.ReverseProxy tell a TCP connection they are done sending without
// tearing the connection down. The connection here answers that assertion, so
// the client ends its write side and the server reads an ordinary EOF while
// the connection stays open.
//
// Compare go-iroh-stream-listener, which wraps the accept side as a [net.Listener]
// so an existing server loop — net/http included — serves iroh peers
// unchanged.
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/tmc/go-iroh/iroh"
)

const alpn = "go-iroh-examples/stream-netconn/1"

// deadline bounds each side's read and write. It is generous: the point is that
// the deadline exists and is enforced by the net.Conn, not how long it is.
const deadline = 5 * time.Second

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

	// The server holds its connection open until release is closed. Closing a
	// QUIC connection can discard data that is still in flight, and the reply
	// is the last thing written.
	release := make(chan struct{})
	releaseServer := sync.OnceFunc(func() { close(release) })
	defer releaseServer()

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- serve(ctx, server, release, stdout)
	}()

	client, err := bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, server.Addr(), alpn)
	if err != nil {
		return err
	}
	defer conn.Close()

	stream, err := conn.OpenStreamConn(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()

	if err := stream.SetDeadline(time.Now().Add(deadline)); err != nil {
		return err
	}
	if _, err := io.WriteString(stream, "deadline example\n"); err != nil {
		return fmt.Errorf("write request: %w", err)
	}
	reply, err := bufio.NewReader(stream).ReadString('\n')
	if err != nil {
		return fmt.Errorf("read reply: %w", err)
	}
	fmt.Fprint(stdout, reply)

	// A net.Conn over iroh half-closes, the same probe net/http and
	// httputil.ReverseProxy make on a *net.TCPConn. Close would end both
	// directions; CloseWrite ends only the send side, which the peer sees as a
	// plain EOF.
	cw, ok := stream.(interface{ CloseWrite() error })
	fmt.Fprintln(stdout, "half-close supported:", ok)
	if ok {
		if err := cw.CloseWrite(); err != nil {
			return fmt.Errorf("close write: %w", err)
		}
	}

	releaseServer()
	if err := <-serverErr; err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

// serve accepts one stream as a net.Conn, uppercases the line it reads, and
// waits for release before letting the connection close.
func serve(ctx context.Context, ep *iroh.Endpoint, release <-chan struct{}, stdout io.Writer) error {
	conn, err := ep.Accept(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	defer func() { <-release }()

	stream, err := conn.AcceptStreamConn(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()

	if err := stream.SetDeadline(time.Now().Add(deadline)); err != nil {
		return err
	}
	line, err := bufio.NewReader(stream).ReadString('\n')
	if err != nil {
		return fmt.Errorf("read request: %w", err)
	}
	if _, err := io.WriteString(stream, strings.ToUpper(line)); err != nil {
		return fmt.Errorf("write reply: %w", err)
	}

	// The client's half-close arrives here as an EOF on a connection that is
	// otherwise still open, which is what lets a protocol say "that is all I
	// am sending" without saying "I am gone".
	rest, err := io.ReadAll(stream)
	if err != nil {
		return fmt.Errorf("read after reply: %w", err)
	}
	fmt.Fprintln(stdout, "peer half-closed after", len(rest), "more bytes")
	return nil
}

// bind binds an endpoint to an ephemeral IPv6 loopback port, then applies opts.
// Loopback binding keeps the example self-contained: no relay, no DNS, no
// network access.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
}
