// Command 12-connect-public dials a peer given its ID and its coordinates.
//
// It is the dialing half of 11-public-server, and it shows what a
// [netaddr.EndpointAddr] is made of: an endpoint ID, which names the peer and
// is the only part that authenticates it, plus the direct UDP addresses and
// relay URLs that say where to look for it. The ID comes from -peer-id, the
// coordinates from -peer-ip and -peer-relay, which is how addressing works
// before any discovery service is involved.
//
// Either coordinate alone is enough. A direct address is dialed straight; a
// relay URL is a rendezvous, and reaching a peer through one requires the
// dialing endpoint to speak to that relay too, which is why -peer-relay also
// puts the URL into [relay.ModeCustomURLs]. Passing both lets iroh race them
// and then upgrade to the direct path.
//
// 14-dns-resolve and 15-pkarr-publish-resolve build the same address from an ID
// alone by asking a discovery service; 38-app-envelope-ticket carries it as one
// pasteable ticket. Reach for those once the coordinates stop fitting on a
// command line.
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
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
)

const defaultALPN = "go-iroh-examples/public-server/1"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("12-connect-public", flag.ContinueOnError)
	peerID := fs.String("peer-id", exampleutil.Env("IROH_EXAMPLE_PEER_ID", ""), "endpoint id of the peer to dial, z32 or hex ($IROH_EXAMPLE_PEER_ID)")
	peerIP := fs.String("peer-ip", exampleutil.Env("IROH_EXAMPLE_PEER_IP", ""), "direct UDP address of the peer, host:port ($IROH_EXAMPLE_PEER_IP)")
	peerRelay := fs.String("peer-relay", exampleutil.Env("IROH_EXAMPLE_PEER_RELAY", ""), "relay URL the peer is reachable through ($IROH_EXAMPLE_PEER_RELAY)")
	alpn := fs.String("alpn", exampleutil.Env("IROH_EXAMPLE_ALPN", defaultALPN), "ALPN to negotiate, matching the server's ($IROH_EXAMPLE_ALPN)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *peerID == "" || (*peerIP == "" && *peerRelay == "") {
		fmt.Println("pass -peer-id and -peer-ip or -peer-relay (or set IROH_EXAMPLE_PEER_ID, IROH_EXAMPLE_PEER_IP, IROH_EXAMPLE_PEER_RELAY)")
		return nil
	}

	id, err := parseEndpointID(*peerID)
	if err != nil {
		return fmt.Errorf("parse peer id: %w", err)
	}
	addr := netaddr.NewEndpointAddr(id)
	var relayURLs []netaddr.RelayURL
	if *peerIP != "" {
		ap, err := netip.ParseAddrPort(*peerIP)
		if err != nil {
			return fmt.Errorf("parse peer ip: %w", err)
		}
		addr = addr.WithIP(ap)
	}
	if *peerRelay != "" {
		u, err := netaddr.ParseRelayURL(*peerRelay)
		if err != nil {
			return fmt.Errorf("parse peer relay: %w", err)
		}
		addr = addr.WithRelayURL(u)
		relayURLs = append(relayURLs, u)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := []iroh.Option{}
	if len(relayURLs) > 0 {
		opts = append(opts, iroh.WithRelayMode(relay.ModeCustomURLs(relayURLs...)))
	}
	client, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, addr, *alpn)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", addr.ID, err)
	}
	defer conn.CloseWithError(0, "")

	reply, err := exampleutil.Exchange(ctx, conn, "public hello")
	if err != nil {
		return err
	}
	fmt.Println(reply)
	fmt.Println("remote:", conn.RemoteID().Short())
	return nil
}

// parseEndpointID accepts either printed form of an endpoint id. As of go-iroh
// v0.1.0 key.ParseEndpointID takes the hex form of key.EndpointID.String, not
// the z-base-32 form of key.EndpointID.Z32 that the other examples print, so
// the z32 form is tried separately.
func parseEndpointID(s string) (key.EndpointID, error) {
	if id, err := key.ParseEndpointID(s); err == nil {
		return id, nil
	}
	return key.ParseEndpointIDZ32(s)
}
