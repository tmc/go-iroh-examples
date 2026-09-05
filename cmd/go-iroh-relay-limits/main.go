// Command go-iroh-relay-limits shapes relayed traffic with a per-client rate limit.
//
// A relay forwards other people's packets, so an operator wants a ceiling on
// what any one client can push through it. [relayserver.WithClientRate] sets
// that ceiling in bytes per second. The limit is applied where the relay reads
// from a client's WebSocket: each accepted relay session gets its own token
// bucket, filled at the configured rate, and the server stops reading from that
// client until the bucket covers the frame it just read. It is therefore per
// client session and per direction — the sender's frames into the relay — not a
// budget shared across the relay.
//
// The bucket starts full and its burst is the relay's maximum frame size, one
// mebibyte, so the first mebibyte a client sends is never delayed. Only a
// transfer larger than the burst shows the limit at all, which is why this
// example sends twice that.
//
// The limit shapes relayed traffic only. Once the same connection upgrades to a
// direct path its packets no longer pass through the relay, and the third
// transfer below runs at full speed over the very relay that throttled the
// first two. Relay rate limits protect the relay; they are not a policy on the
// application.
//
// Both relays here are in-process [relayserver.Server] instances on httptest
// servers, so the example needs no network. Compare go-iroh-path-upgrade for
// the path migration on its own and go-iroh-local-infra for a fuller private
// deployment.
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"net/http/httptest"
	"net/netip"
	"os"
	"strconv"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
	"github.com/tmc/go-iroh/relayserver"
)

const alpn = "go-iroh-examples/relay-limits/1"

func main() {
	fs := flag.NewFlagSet("go-iroh-relay-limits", flag.ExitOnError)
	bytes := fs.Int64("bytes", envInt64("IROH_EXAMPLE_BYTES", 2<<20), "payload size in bytes ($IROH_EXAMPLE_BYTES)")
	rate := fs.Int64("rate", envInt64("IROH_EXAMPLE_RATE", 1<<20), "limited relay's per-client rate in bytes per second ($IROH_EXAMPLE_RATE)")
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	if err := run(*bytes, *rate); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(payload, rate int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	fmt.Printf("payload: %d bytes, limit: %d bytes/s\n", payload, rate)

	unlimited, err := transfer(ctx, relayserver.New(), payload, false)
	if err != nil {
		return fmt.Errorf("unlimited relay: %w", err)
	}
	fmt.Printf("relayed, no limit: %s\n", round(unlimited))

	limited, err := transfer(ctx, relayserver.NewWithOptions(relayserver.WithClientRate(rate)), payload, false)
	if err != nil {
		return fmt.Errorf("limited relay: %w", err)
	}
	fmt.Printf("relayed, limited: %s\n", round(limited))
	fmt.Println("limit slowed the relayed transfer:", limited > unlimited)

	// The same limited relay, but the connection is allowed to find a direct
	// path first. The relay's rate limit never sees these bytes.
	direct, err := transfer(ctx, relayserver.NewWithOptions(relayserver.WithClientRate(rate)), payload, true)
	if err != nil {
		return fmt.Errorf("limited relay, direct path: %w", err)
	}
	fmt.Printf("direct, same limit: %s\n", round(direct))
	fmt.Println("limit applied to the direct transfer:", direct >= limited)
	return nil
}

// transfer streams payload bytes over one connection through srv and returns
// how long the transfer took. If upgrade is set, the endpoints advertise their
// own sockets and the transfer waits for the connection to leave the relay;
// otherwise every byte goes through the relay.
func transfer(ctx context.Context, srv *relayserver.Server, payload int64, upgrade bool) (time.Duration, error) {
	// Without the upgrade, direct IP transports are disabled outright. Both
	// endpoints live on this host, so they would otherwise find a loopback path
	// immediately and never touch the relay at all.
	var transports []iroh.Option
	if !upgrade {
		transports = append(transports, iroh.WithoutIPTransports())
	}
	http := httptest.NewServer(srv)
	defer http.Close()
	relayURL, err := netaddr.ParseRelayURL(http.URL)
	if err != nil {
		return 0, err
	}
	mode := relay.ModeCustomURLs(relayURL)

	server, err := bind(ctx, append(transports, iroh.WithALPNs(alpn), iroh.WithRelayMode(mode))...)
	if err != nil {
		return 0, err
	}
	defer server.Shutdown(ctx)

	client, err := bind(ctx, append(transports, iroh.WithRelayMode(mode))...)
	if err != nil {
		return 0, err
	}
	defer client.Shutdown(ctx)

	if err := server.Online(ctx); err != nil {
		return 0, fmt.Errorf("server online: %w", err)
	}
	if err := client.Online(ctx); err != nil {
		return 0, fmt.Errorf("client online: %w", err)
	}

	sunk := make(chan error, 1)
	go func() { sunk <- sink(ctx, server) }()

	// The address carries only the relay URL, so the connection starts relayed.
	conn, err := client.Connect(ctx, netaddr.NewEndpointAddr(server.ID()).WithRelayURL(relayURL), alpn)
	if err != nil {
		return 0, err
	}
	defer conn.CloseWithError(0, "")

	if upgrade {
		server.AddExternalAddr(server.LocalAddr())
		client.AddExternalAddr(client.LocalAddr())
		if err := waitForDirect(ctx, conn); err != nil {
			return 0, err
		}
	}

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return 0, err
	}
	defer stream.Close()

	start := time.Now()
	if _, err := io.Copy(stream, io.LimitReader(rand.Reader, payload)); err != nil {
		return 0, fmt.Errorf("write payload: %w", err)
	}
	if err := stream.CloseWrite(); err != nil {
		return 0, err
	}
	// The peer replies only after reading every byte, so the round trip
	// measures the whole transfer rather than the local send buffer.
	var ack [1]byte
	if _, err := io.ReadFull(stream, ack[:]); err != nil {
		return 0, fmt.Errorf("read ack: %w", err)
	}
	elapsed := time.Since(start)

	if err := <-sunk; err != nil {
		return 0, err
	}
	return elapsed, nil
}

// sink accepts one connection and one stream, drains it, and acknowledges. It
// deliberately leaves the connection open: closing it here would race the
// peer's read of the acknowledgement. The endpoint's shutdown closes it.
func sink(ctx context.Context, ep *iroh.Endpoint) error {
	conn, err := ep.Accept(ctx)
	if err != nil {
		return err
	}
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	if _, err := io.Copy(io.Discard, stream); err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	if _, err := stream.Write([]byte{0}); err != nil {
		return err
	}
	return stream.CloseWrite()
}

// waitForDirect blocks until the connection selects a non-relayed path.
func waitForDirect(ctx context.Context, conn *iroh.Conn) error {
	watch, err := conn.WatchPaths(ctx)
	if err != nil {
		return err
	}
	for paths := range watch {
		for _, p := range paths {
			if p.Selected && !p.Relayed {
				return nil
			}
		}
	}
	return fmt.Errorf("path watch closed before a direct path was selected")
}

func round(d time.Duration) time.Duration { return d.Round(time.Millisecond) }

// envInt64 returns the environment variable name parsed as an integer, or def
// if it is unset or unparseable.
func envInt64(name string, def int64) int64 {
	v, err := strconv.ParseInt(os.Getenv(name), 10, 64)
	if err != nil {
		return def
	}
	return v
}

// bind binds an endpoint to an ephemeral IPv6 loopback port, which keeps the
// example self-contained: the only relay is the in-process one, and there is no
// DNS and no network access.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
}
