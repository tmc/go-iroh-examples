// Command go-iroh-path-upgrade watches a connection move from a relay to a direct
// path.
//
// A dial to a peer known only by its relay URL starts relayed: every packet
// costs a round trip through the relay server. In the background the endpoints
// probe each other's addresses, and when a direct path works the connection
// switches to it without interrupting the streams already running on it. That
// switch is the reason a relay is a fallback rather than a proxy, and
// [iroh.Conn.WatchPaths] is how an application observes it.
//
// The relay here is an in-process [relayserver.Server], so the example needs no
// network. The direct path is discovered only after each endpoint advertises
// its own socket with AddExternalAddr, which stands in for the address
// discovery a real deployment gets from QAD or a lookup service.
package main

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
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
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
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
	fmt.Println("initial selected:", exampleutil.SelectedPathKind(initial))

	server.AddExternalAddr(server.LocalAddr())
	client.AddExternalAddr(client.LocalAddr())

	upgraded := waitForDirect(ctx, watch, 20*time.Second)
	fmt.Println("direct upgrade observed:", upgraded)
	fmt.Println("current selected:", exampleutil.SelectedPathKind(conn.Paths()))
	return nil
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
			if exampleutil.SelectedPathKind(paths) == "ip" {
				return true
			}
		case <-timer.C:
			return false
		case <-ctx.Done():
			return false
		}
	}
}
