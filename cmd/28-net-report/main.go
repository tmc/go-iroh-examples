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
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

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
		panic(err)
	}
	defer ep.Shutdown(ctx)

	report, ok := waitReport(ctx, ep)
	fmt.Println("live relay:", live)
	fmt.Println("report available:", ok)
	if !ok {
		fmt.Println("set GO_IROH_LIVE_RELAY=1 to run net_report against the public relay map")
		return
	}
	fmt.Println("has udp:", report.HasUDP())
	fmt.Println("udp4:", report.UDPv4)
	fmt.Println("udp6:", report.UDPv6)
	fmt.Println("global v4:", report.GlobalV4)
	fmt.Println("global v6:", report.GlobalV6)
	fmt.Println("preferred relay:", report.PreferredRelay)
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
