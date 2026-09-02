// Command go-iroh-metrics reads an endpoint's counters after a connection.
//
// [iroh.Endpoint.Metrics] returns [iroh.Metrics], a snapshot of what the
// endpoint has done: dials started, accepted, and failed on the connecting
// side, the same triple on the accepting side, and nested socket and net-report
// counters. It is a plain struct returned by value, so reading it is cheap and
// cannot block or perturb a connection in flight.
//
// The counters are the cheapest answer to "is this endpoint doing what I think
// it is". Connects started with none accepted is a reachability problem; accepts
// started running ahead of accepts accepted is a peer failing the handshake, not
// a server refusing it. This example prints the six fields directly because
// after one connection there are few enough to read.
//
// A snapshot is also a metrics source: [iroh.Metrics.WriteOpenMetrics] renders
// it as OpenMetrics text, and go-iroh-local-infra collects several sources —
// endpoint, relay server, DNS server — into one
// [github.com/tmc/go-iroh/metrics.Registry] so that a process exports them
// together. Reach for that when something scrapes; reach for these fields when
// a person is reading.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
)

const alpn = "go-iroh-examples/metrics/1"

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

	reply, err := exampleutil.Exchange(ctx, conn, "metrics hello")
	if err != nil {
		return err
	}
	// Read the snapshots after the exchange has been answered: the accepting
	// side's counters only settle once its handshake has completed.
	if err := <-served; err != nil {
		return fmt.Errorf("server: %w", err)
	}

	cm := client.Metrics()
	sm := server.Metrics()
	fmt.Println(reply)
	fmt.Printf("client connects: started=%d accepted=%d failed=%d\n", cm.ConnectsStarted, cm.ConnectsAccepted, cm.ConnectsFailed)
	fmt.Printf("server accepts: started=%d accepted=%d failed=%d\n", sm.AcceptsStarted, sm.AcceptsAccepted, sm.AcceptsFailed)
	return nil
}
