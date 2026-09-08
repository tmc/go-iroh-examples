// Command go-iroh-net-report prints what the endpoint measured about its network.
//
// A net report is the endpoint's view of its own position on the network: did
// an IPv4 or IPv6 UDP round trip complete, what public address did a relay
// observe the packets coming from, and which relay answered fastest. iroh uses
// it while establishing connections — the observed addresses become candidates
// other peers can try, and the preferred relay becomes the home relay — and
// [iroh.Endpoint.NetReport] exposes the same snapshot to an application, which
// is the first thing to read when a connection stays on a relayed path.
//
// The measurement is made by probing relay servers, so a direct-only endpoint
// has nothing to measure against: an endpoint refreshes the report on its own
// whenever it has relays, and with none the report never becomes available.
// That is what the default run prints. With -live the endpoint joins the public
// relay map, the probes have somewhere to go, and waitReport polls until the
// first report lands. [iroh.WithoutNetReport] stops the refreshes for an
// endpoint that has relays and does not want to spend packets measuring them.
//
// go-iroh-doctor prints these fields and more against an in-process relay, so it
// gives real numbers with no network; go-iroh-relay-online is the relay opt-in on
// its own; go-iroh-path-upgrade shows the paths a connection picks once the report
// has supplied candidates.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/netip"
	"os"
	"strconv"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/relay"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		// -h is a request for the usage message, which the flag package has
		// already printed. It is not a failure.
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("go-iroh-net-report", flag.ContinueOnError)
	live := fs.Bool("live", envBool("GO_IROH_LIVE_RELAY", false), "probe n0's public relay map instead of reporting a direct-only endpoint ($GO_IROH_LIVE_RELAY)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	opts := []iroh.Option{
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
	}
	if *live {
		opts = append(opts, iroh.WithRelayMode(relay.ModeDefault()))
	}

	ep, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return err
	}
	defer ep.Shutdown(ctx)

	var report iroh.NetReport
	var ok bool
	if *live {
		reportCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		report, ok = waitReport(reportCtx, ep)
		cancel()
	} else {
		report, ok = ep.NetReport()
	}
	fmt.Println("live relay:", *live)
	fmt.Println("report available:", ok)
	if !ok {
		fmt.Println("pass -live or set GO_IROH_LIVE_RELAY=1 to run net_report against the public relay map")
		return nil
	}
	fmt.Println("has udp:", report.HasUDP())
	fmt.Println("udp4:", report.UDPv4)
	fmt.Println("udp6:", report.UDPv6)
	fmt.Println("global v4:", report.GlobalV4)
	fmt.Println("global v6:", report.GlobalV6)
	fmt.Println("preferred relay:", report.PreferredRelay)
	return nil
}

// waitReport polls ep for its first net report. It returns false if ctx ends
// before one is available.
func waitReport(ctx context.Context, ep *iroh.Endpoint) (iroh.NetReport, bool) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if report, ok := ep.NetReport(); ok {
			return report, true
		}
		select {
		case <-ctx.Done():
			return iroh.NetReport{}, false
		case <-ticker.C:
		}
	}
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
