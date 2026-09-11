// Command go-iroh-close-codes reads the code and reason a peer closed with.
//
// QUIC lets the side that closes a connection attach an application error code
// and a reason string, and both reach the other end. That is how a server says
// "quota exceeded" rather than dropping the connection and leaving the client to
// guess between a policy decision, a crash, and a network failure.
//
// The code arrives as the cause of the connection's context.
// [iroh.AsApplicationError] separates an application close from a transport
// failure, and its Remote field says which side closed. A client that retries
// should look at both: a local timeout is worth retrying, a remote code 42 is
// not.
//
// See go-iroh-graceful-shutdown for closing an endpoint without cutting off work in
// flight.
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

const alpn = "go-iroh-examples/close-codes/1"

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

	accepted := make(chan error, 1)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			accepted <- err
			return
		}
		accepted <- conn.CloseWithError(42, "quota exceeded")
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
	defer conn.CloseWithError(0, "")

	select {
	case <-conn.Context().Done():
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := <-accepted; err != nil {
		return err
	}

	appErr, ok := iroh.AsApplicationError(context.Cause(conn.Context()))
	if !ok {
		return fmt.Errorf("close cause: %v", context.Cause(conn.Context()))
	}
	fmt.Fprintln(stdout, "remote:", appErr.Remote)
	fmt.Fprintln(stdout, "code:", appErr.Code)
	fmt.Fprintln(stdout, "reason:", appErr.Reason)
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
