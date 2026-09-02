// Command go-iroh-ping answers a ping over an iroh connection.
//
// This is the Go port of n0's iroh-ping, and it speaks that program's protocol
// byte for byte: ALPN "iroh/ping/0", the four bytes PING from the dialer, the
// four bytes PONG back. There is no length prefix and no version byte because
// there is nothing to frame — the dialer closes its write side after PING, so
// the responder's read ends exactly where the request does. A Rust iroh-ping
// client can be pointed at a Go endpoint serving this ALPN, and this program's
// client half can be pointed at a Rust listener; neither side can tell.
//
// That is the reason to read this example rather than go-iroh-router-echo, which
// has the same shape — one [iroh.ProtocolHandler] registered with
// [iroh.NewRouter], which dispatches by exact ALPN, and one
// [iroh.Conn.OpenStreamSync] on the dialing side. What differs is that the ALPN
// and the payloads belong to somebody else. An ALPN is an opaque byte string,
// so interoperating with an existing implementation is a matter of using its
// string and writing its bytes.
//
// Closing the write side is what ends the message here, which works because
// there is exactly one message. go-iroh-framed-messages keeps the stream open for
// several and therefore has to frame them.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
)

// The ALPN and the two payloads are n0's iroh-ping. They are wire values, not
// Go identifiers, so they must match byte for byte across implementations.
const (
	alpn     = "iroh/ping/0"
	request  = "PING"
	response = "PONG"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server, err := exampleutil.Bind(ctx)
	if err != nil {
		return err
	}
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		alpn: pingHandler{},
	}, nil)
	if err != nil {
		return err
	}
	defer router.Shutdown(ctx)

	client, err := exampleutil.Bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, exampleutil.Addr(server), alpn)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", server.ID().Short(), err)
	}
	defer conn.CloseWithError(0, "")

	reply, err := ping(ctx, conn)
	if err != nil {
		return err
	}
	fmt.Println(reply)
	return nil
}

// pingHandler answers one ping per connection.
type pingHandler struct{}

// Accept implements [iroh.ProtocolHandler].
func (pingHandler) Accept(ctx context.Context, conn *iroh.Conn) error {
	s, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	// The dialer closed its write side, so this read ends at the end of the
	// request. It is the whole framing of the protocol.
	msg, err := io.ReadAll(s)
	if err != nil {
		return err
	}
	if string(msg) != request {
		return fmt.Errorf("unexpected ping payload %q", msg)
	}
	if _, err := s.Write([]byte(response)); err != nil {
		return err
	}
	return s.Close()
}

// ping sends one request on conn and returns the reply.
func ping(ctx context.Context, conn *iroh.Conn) (string, error) {
	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return "", err
	}
	if _, err := s.Write([]byte(request)); err != nil {
		return "", err
	}
	if err := s.Close(); err != nil {
		return "", err
	}
	reply, err := io.ReadAll(s)
	if err != nil {
		return "", err
	}
	return string(reply), nil
}
