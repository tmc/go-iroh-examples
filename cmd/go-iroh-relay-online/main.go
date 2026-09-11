// Command go-iroh-relay-online joins the public relay map and waits to be reachable.
//
// [iroh.Bind] is direct-only: the default relay mode is relay.ModeDisabled, so
// a fresh endpoint can be dialed only by a peer that can already reach one of
// its UDP addresses. [relay.ModeDefault] adds n0's public relays, which forward
// packets for endpoints that cannot reach each other directly and give every
// endpoint a stable address that survives a change of network.
//
// Joining is not instant, and this example is about the wait. Relay selection
// probes the map and picks a home relay by latency, and until that connection
// is up the endpoint has no relay address to advertise.
// [iroh.Endpoint.Online] blocks until it does, which is why a server publishes
// its address after Online rather than straight after Bind.
// [iroh.Endpoint.HomeRelayStatus] is the same state as an observer, for a
// program that would rather watch than block.
//
// The relays are a real service on the internet, so this example does nothing
// until -live is passed. go-iroh-local-infra runs the same shape against an
// in-process relay server and needs no network; go-iroh-net-report shows
// what the probing measured; go-iroh-public-endpoint folds this opt-in into a server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/relay"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		// -h is a request for the usage message, which the flag package has
		// already printed. It is not a failure.
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("go-iroh-relay-online", flag.ContinueOnError)
	live := fs.Bool("live", envBool("GO_IROH_LIVE_RELAY", false), "connect to n0's default public relays ($GO_IROH_LIVE_RELAY)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !*live {
		fmt.Fprintln(stdout, "pass -live or set GO_IROH_LIVE_RELAY=1 to connect to the default public relays")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	ep, err := iroh.Bind(ctx, iroh.WithRelayMode(relay.ModeDefault()))
	if err != nil {
		return err
	}
	defer ep.Shutdown(ctx)

	if err := ep.Online(ctx); err != nil {
		return fmt.Errorf("connect to public relay map: %w", err)
	}
	status := ep.HomeRelayStatus().Current()
	if status == nil {
		return errors.New("online reported success with no home relay")
	}
	fmt.Fprintln(stdout, "endpoint id:", ep.ID().Z32())
	fmt.Fprintln(stdout, "home relay:", status.URL)
	fmt.Fprintln(stdout, "connected:", status.IsConnected())
	fmt.Fprintln(stdout, "advertised relays:", ep.Addr().RelayURLs())
	return nil
}

// envBool returns the boolean value of the environment variable name, or def
// if it is unset or unparseable, so that a flag and a GO_IROH_ variable
// configure the same thing.
func envBool(name string, def bool) bool {
	v, err := strconv.ParseBool(os.Getenv(name))
	if err != nil {
		return def
	}
	return v
}
