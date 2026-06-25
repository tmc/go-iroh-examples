package main

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	peerID := os.Getenv("IROH_EXAMPLE_PEER_ID")
	peerIP := os.Getenv("IROH_EXAMPLE_PEER_IP")
	peerRelay := os.Getenv("IROH_EXAMPLE_PEER_RELAY")
	alpn := os.Getenv("IROH_EXAMPLE_ALPN")
	if peerID == "" || (peerIP == "" && peerRelay == "") {
		fmt.Println("set IROH_EXAMPLE_PEER_ID and IROH_EXAMPLE_PEER_IP or IROH_EXAMPLE_PEER_RELAY")
		return nil
	}
	if alpn == "" {
		alpn = "go-iroh-examples/public-server/1"
	}

	id, err := key.ParseEndpointID(peerID)
	if err != nil {
		return fmt.Errorf("parse IROH_EXAMPLE_PEER_ID: %w", err)
	}
	addr := netaddr.NewEndpointAddr(id)
	var relayURLs []netaddr.RelayURL
	if peerIP != "" {
		ap, err := netip.ParseAddrPort(peerIP)
		if err != nil {
			return fmt.Errorf("parse IROH_EXAMPLE_PEER_IP: %w", err)
		}
		addr = addr.WithIP(ap)
	}
	if peerRelay != "" {
		u, err := netaddr.ParseRelayURL(peerRelay)
		if err != nil {
			return fmt.Errorf("parse IROH_EXAMPLE_PEER_RELAY: %w", err)
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

	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", addr.ID, err)
	}
	defer conn.CloseWithError(0, "")

	reply, err := exchange(ctx, conn, "public hello")
	if err != nil {
		return err
	}
	fmt.Println(reply)
	fmt.Println("remote:", conn.RemoteID().Short())
	return nil
}
