// Command go-iroh-doctor reports what an endpoint can see of the network.
//
// It is the Go counterpart of n0's iroh-doctor: the questions to ask when a
// connection is not behaving. Whether the endpoint reached a home relay, what
// [iroh.NetReport] says about UDP and about the addresses the network reports
// back, the round-trip time to each relay, and which path a live connection
// actually selected.
//
// By default it diagnoses against an in-process [relayserver.Server], so it runs
// anywhere and the numbers describe loopback. Pass -live (or set
// GO_IROH_LIVE_RELAY=1) to run the same checks against n0's public relays, which
// is where the answers are worth reading.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
	"github.com/tmc/go-iroh/relayserver"
)

const alpn = "go-iroh-examples/doctor/1"

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
	fs := flag.NewFlagSet("go-iroh-doctor", flag.ContinueOnError)
	live := fs.Bool("live", envBool("GO_IROH_LIVE_RELAY", false),
		"diagnose against n0's public relays instead of an in-process one ($GO_IROH_LIVE_RELAY)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mode, relayURL, cleanup, err := relayMode(*live)
	if err != nil {
		return err
	}
	defer cleanup()
	fmt.Fprintln(stdout, "live relay:", *live)
	if !*live {
		fmt.Fprintln(stdout, "pass -live or set GO_IROH_LIVE_RELAY=1 to diagnose against public relays")
	}

	server, err := iroh.Bind(ctx,
		iroh.WithALPNs(alpn),
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithRelayMode(mode),
	)
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	if err := server.Online(ctx); err != nil {
		return fmt.Errorf("server online: %w", err)
	}
	status := server.HomeRelayStatus().Current()
	fmt.Fprintln(stdout, "home relay connected:", status != nil && status.IsConnected())
	if status != nil {
		fmt.Fprintln(stdout, "home relay:", status.URL)
		if relayURL.IsZero() {
			relayURL = status.URL
		}
	}

	report, ok := waitReport(ctx, server)
	fmt.Fprintln(stdout, "net report available:", ok)
	if ok {
		fmt.Fprintln(stdout, "udp available:", report.HasUDP())
		fmt.Fprintln(stdout, "preferred relay:", report.PreferredRelay)
		printRelayLatencies(report.RelayLatencies, stdout)
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
	fmt.Fprintln(stdout, "connection selected:", selectedPathKind(conn.Paths()))
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

func printRelayLatencies(latencies map[netaddr.RelayURL]time.Duration, stdout io.Writer) {
	urls := make([]netaddr.RelayURL, 0, len(latencies))
	for url := range latencies {
		urls = append(urls, url)
	}
	sort.Slice(urls, func(i, j int) bool {
		return urls[i].String() < urls[j].String()
	})
	fmt.Fprintln(stdout, "relay latencies:", len(urls))
	for _, url := range urls {
		fmt.Fprintf(stdout, "latency %s: %s\n", url, latencies[url].Round(time.Millisecond))
	}
}

// envBool returns the environment variable name parsed as a boolean, or def if
// it is unset or unparseable, so that a flag and an environment variable
// configure the same thing.
func envBool(name string, def bool) bool {
	v, err := strconv.ParseBool(os.Getenv(name))
	if err != nil {
		return def
	}
	return v
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

// selectedPathKind names the transport of the selected path: "relay", the
// network of a direct address, "unknown", or "none" if no path is selected.
func selectedPathKind(paths []iroh.PathInfo) string {
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
