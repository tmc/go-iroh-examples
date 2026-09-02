// Command 28-net-report prints what the endpoint measured about its network.
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
// has nothing to measure against: [iroh.WithNetReport] enables the background
// refreshes but the report never becomes available. That is what the default
// run prints. With -live the endpoint joins the public relay map, the probes
// have somewhere to go, and [exampleutil.WaitReport] polls until the first
// report lands.
//
// 37-doctor prints these fields and more against an in-process relay, so it
// gives real numbers with no network; 13-relay-online is the relay opt-in on
// its own; 33-path-upgrade shows the paths a connection picks once the report
// has supplied candidates.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/relay"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("28-net-report", flag.ContinueOnError)
	live := fs.Bool("live", exampleutil.EnvBool("GO_IROH_LIVE_RELAY", false), "probe n0's public relay map instead of reporting a direct-only endpoint ($GO_IROH_LIVE_RELAY)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	opts := []iroh.Option{
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithNetReport(),
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
		report, ok = exampleutil.WaitReport(reportCtx, ep)
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
