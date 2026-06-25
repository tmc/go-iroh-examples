package main

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/relay"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	opts := []iroh.Option{
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithNetReport(),
	}
	live := os.Getenv("GO_IROH_LIVE_RELAY") == "1"
	if live {
		opts = append(opts, iroh.WithRelayMode(relay.ModeDefault()))
	}

	ep, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return err
	}
	defer ep.Shutdown(ctx)

	var report iroh.NetReport
	var ok bool
	if live {
		reportCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		report, ok = waitReport(reportCtx, ep)
		cancel()
	} else {
		report, ok = ep.NetReport()
	}
	fmt.Println("live relay:", live)
	fmt.Println("report available:", ok)
	if !ok {
		fmt.Println("set GO_IROH_LIVE_RELAY=1 to run net_report against the public relay map")
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
