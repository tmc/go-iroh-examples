package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"sort"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
	"github.com/tmc/go-iroh/relayserver"
)

const alpn = "go-iroh-examples/doctor/1"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	live := os.Getenv("GO_IROH_LIVE_RELAY") == "1"
	mode, relayURL, cleanup, err := relayMode(live)
	if err != nil {
		return err
	}
	defer cleanup()
	fmt.Println("live relay:", live)
	if !live {
		fmt.Println("set GO_IROH_LIVE_RELAY=1 to diagnose against public relays")
	}

	server, err := iroh.Bind(ctx,
		iroh.WithALPNs(alpn),
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithRelayMode(mode),
		iroh.WithNetReport(),
	)
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	if err := server.Online(ctx); err != nil {
		return fmt.Errorf("server online: %w", err)
	}
	status := server.HomeRelayStatus().Current()
	fmt.Println("home relay connected:", status != nil && status.IsConnected())
	if status != nil {
		fmt.Println("home relay:", status.URL)
		if relayURL.IsZero() {
			relayURL = status.URL
		}
	}

	report, ok := waitReport(ctx, server)
	fmt.Println("net report available:", ok)
	if ok {
		fmt.Println("udp available:", report.HasUDP())
		fmt.Println("preferred relay:", report.PreferredRelay)
		printRelayLatencies(report.RelayLatencies)
	}

	client, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithRelayMode(mode),
	)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)
	if err := client.Online(ctx); err != nil {
		return fmt.Errorf("client online: %w", err)
	}

	accepted := make(chan error, 1)
	go func() {
		conn, err := server.Accept(ctx)
		if err == nil {
			defer conn.CloseWithError(0, "")
		}
		accepted <- err
	}()

	conn, err := client.Connect(ctx, netaddr.NewEndpointAddr(server.ID()).WithRelayURL(relayURL), alpn)
	if err != nil {
		return err
	}
	defer conn.CloseWithError(0, "")
	if err := <-accepted; err != nil {
		return err
	}
	fmt.Println("connection selected:", selectedKind(conn.Paths()))
	return nil
}

func relayMode(live bool) (relay.Mode, netaddr.RelayURL, func(), error) {
	if live {
		return relay.ModeDefault(), netaddr.RelayURL{}, func() {}, nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/", relayserver.New())
	relayHTTP := httptest.NewServer(mux)
	relayURL, err := netaddr.ParseRelayURL(relayHTTP.URL)
	if err != nil {
		relayHTTP.Close()
		return relay.Mode{}, netaddr.RelayURL{}, func() {}, err
	}
	return relay.ModeCustomURLs(relayURL), relayURL, relayHTTP.Close, nil
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

func printRelayLatencies(latencies map[netaddr.RelayURL]time.Duration) {
	urls := make([]netaddr.RelayURL, 0, len(latencies))
	for url := range latencies {
		urls = append(urls, url)
	}
	sort.Slice(urls, func(i, j int) bool {
		return urls[i].String() < urls[j].String()
	})
	fmt.Println("relay latencies:", len(urls))
	for _, url := range urls {
		fmt.Printf("latency %s: %s\n", url, latencies[url].Round(time.Millisecond))
	}
}

func selectedKind(paths []iroh.PathInfo) string {
	for _, p := range paths {
		if !p.Selected {
			continue
		}
		if p.Relayed {
			return "relay"
		}
		if p.HasAddr {
			return p.Addr.Network()
		}
		return "unknown"
	}
	return "none"
}
