// Command go-iroh-source-validation makes a peer prove its source address.
//
// A QUIC server that answers an initial packet has already spent a handshake on
// an address it has not checked, which turns it into an amplifier for a peer
// that spoofed the source. The defence is Retry: the server sends back a token
// and goes no further until the client echoes it from the same address, which
// an off-path attacker cannot do. The cost is one extra round trip.
//
// [iroh.WithSourceAddressValidation] is the policy. It receives the unvalidated
// remote address and returns whether to send a Retry. Returning true
// unconditionally, as here, charges every connection that round trip; a server
// with real traffic returns true only under load, or for addresses it has no
// reason to trust. The policy may be consulted more than once for one
// connection, so the count printed here is not a count of connections and the
// policy must not be written as though it were.
//
// [iroh.Incoming.RemoteAddrValidated] reports the result on the accepting side.
// It is the earliest thing a server knows about a peer — available before the
// handshake finishes and long before any identity — which is why it is read at
// the first of the stages go-iroh-manual-incoming unrolls, and why it is the fact
// worth filtering on in go-iroh-incoming-filter.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
)

const alpn = "go-iroh-examples/source-validation/1"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The policy runs on the endpoint's own goroutines, so a counter it shares
	// with run is atomic.
	var retryChecks atomic.Int64
	server, err := exampleutil.Bind(ctx,
		iroh.WithALPNs(alpn),
		iroh.WithSourceAddressValidation(func(net.Addr) bool {
			retryChecks.Add(1)
			return true
		}),
	)
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	validated := make(chan bool, 1)
	served := make(chan error, 1)
	go func() {
		served <- acceptOne(ctx, server, validated)
	}()

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

	reply, err := exampleutil.Exchange(ctx, conn, "validated hello")
	if err != nil {
		return err
	}
	if err := <-served; err != nil {
		return fmt.Errorf("server: %w", err)
	}
	fmt.Println(reply)
	fmt.Println("remote address validated:", <-validated)
	fmt.Println("retry checks:", retryChecks.Load())
	return nil
}

// acceptOne accepts one incoming connection, reports whether its source address
// was validated before the handshake completed, and echoes.
func acceptOne(ctx context.Context, server *iroh.Endpoint, validated chan<- bool) error {
	in, err := server.AcceptIncoming(ctx)
	if err != nil {
		return fmt.Errorf("accept incoming: %w", err)
	}
	validated <- in.RemoteAddrValidated()
	accepting, err := in.Accept()
	if err != nil {
		return fmt.Errorf("accept from %s: %w", in.RemoteAddr(), err)
	}
	conn, err := accepting.Connection(ctx)
	if err != nil {
		return fmt.Errorf("await verified connection: %w", err)
	}
	return exampleutil.Echo(ctx, conn)
}
