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
	"os"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
)

const alpn = "go-iroh-examples/direct-echo/1"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server, err := exampleutil.Bind(ctx, iroh.WithALPNs(alpn))
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
		served <- exampleutil.Echo(ctx, conn)
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

	reply, err := exampleutil.Exchange(ctx, conn, "direct hello")
	if err != nil {
		return err
	}
	if err := <-served; err != nil {
		return fmt.Errorf("server: %w", err)
	}
	fmt.Println(reply)
	return nil
}
