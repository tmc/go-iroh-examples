package main

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
	"github.com/tmc/go-iroh/relayserver"
)

const alpn = "go-iroh-examples/path-upgrade/1"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Second)
	defer cancel()

	relayHTTP := httptest.NewServer(relayserver.New())
	defer relayHTTP.Close()
	relayURL, err := netaddr.ParseRelayURL(relayHTTP.URL)
	if err != nil {
		return err
	}
	mode := relay.ModeCustomURLs(relayURL)

	server, err := iroh.Bind(ctx,
		iroh.WithALPNs(alpn),
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithRelayMode(mode),
	)
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	client, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithRelayMode(mode),
	)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	if err := server.Online(ctx); err != nil {
		return fmt.Errorf("server online: %w", err)
	}
	if err := client.Online(ctx); err != nil {
		return fmt.Errorf("client online: %w", err)
	}

	type acceptResult struct {
		conn *iroh.Conn
		err  error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		conn, err := server.Accept(ctx)
		accepted <- acceptResult{conn: conn, err: err}
	}()

	conn, err := client.Connect(ctx, netaddr.NewEndpointAddr(server.ID()).WithRelayURL(relayURL), alpn)
	if err != nil {
		return err
	}
	defer conn.CloseWithError(0, "")
	result := <-accepted
	if result.err != nil {
		return result.err
	}
	defer result.conn.CloseWithError(0, "")

	watch, err := conn.WatchPaths(ctx)
	if err != nil {
		return err
	}
	initial, ok := <-watch
	if !ok {
		return fmt.Errorf("path watch closed before initial snapshot")
	}
	fmt.Println("initial selected:", selectedKind(initial))

	server.AddExternalAddr(server.LocalAddr())
	client.AddExternalAddr(client.LocalAddr())

	upgraded := waitForDirect(ctx, watch, 70*time.Second)
	fmt.Println("direct upgrade observed:", upgraded)
	fmt.Println("current selected:", selectedKind(conn.Paths()))
	return nil
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

func waitForDirect(ctx context.Context, watch <-chan []iroh.PathInfo, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case paths, ok := <-watch:
			if !ok {
				return false
			}
			if selectedKind(paths) == "ip" {
				return true
			}
		case <-timer.C:
			return false
		case <-ctx.Done():
			return false
		}
	}
}
