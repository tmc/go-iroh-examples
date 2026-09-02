// Command go-iroh-router-echo serves the same echo through a router.
//
// [iroh.Router] is the accept loop go-iroh-direct-echo writes out by hand. It
// accepts connections on an endpoint, reads the ALPN each one negotiated, and
// runs the [iroh.ProtocolHandler] registered for that ALPN in a goroutine of
// its own. A handler that returns an error ends only its own connection and the
// error is logged; a handler that panics is recovered and logged; the accept
// loop continues either way. go-iroh-direct-echo has to decide all of that itself.
//
// The handler map given to [iroh.NewRouter] also registers the endpoint's
// ALPNs, so there is no [iroh.WithALPNs] here — and passing it as well is an
// error, because the endpoint must not already be listening when the router
// starts.
//
// One handler for one protocol is the degenerate case, shown here so that the
// only difference from go-iroh-direct-echo is the accept loop. go-iroh-multi-alpn
// registers two handlers, which is what a router is for; go-iroh-incoming-filter
// puts admission control in front of one.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
)

const alpn = "go-iroh-examples/router-echo/1"

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
		alpn: exampleutil.Handler{},
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

	reply, err := exampleutil.Exchange(ctx, conn, "router hello")
	if err != nil {
		return err
	}
	fmt.Println(reply)
	return nil
}
