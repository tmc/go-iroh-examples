package main

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"strconv"
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	const alpn = "go-iroh-examples/public-server/1"
	port := uint16(4433)
	if s := os.Getenv("IROH_EXAMPLE_PORT"); s != "" {
		n, err := strconv.ParseUint(s, 10, 16)
		if err != nil {
			return fmt.Errorf("parse IROH_EXAMPLE_PORT: %w", err)
		}
		port = uint16(n)
	}

	opts := []iroh.Option{
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv4Unspecified(), port)),
		iroh.WithALPNs(alpn),
	}
	if os.Getenv("GO_IROH_LIVE_RELAY") == "1" {
		opts = append(opts, iroh.WithRelayMode(relay.ModeDefault()))
	}

	ep, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return err
	}
	defer ep.Shutdown(ctx)

	if os.Getenv("GO_IROH_LIVE_RELAY") == "1" {
		onlineCtx, cancelOnline := context.WithTimeout(ctx, 30*time.Second)
		if err := ep.Online(onlineCtx); err != nil {
			cancelOnline()
			return fmt.Errorf("connect to public relay map: %w", err)
		}
		cancelOnline()
	}

	fmt.Println("endpoint id:", ep.ID().Z32())
	fmt.Println("alpn:", alpn)
	fmt.Println("direct paths:", ep.Addr().IPAddrs())
	fmt.Println("relay paths:", ep.Addr().RelayURLs())
	fmt.Println("local udp:", ep.LocalAddr())

	if os.Getenv("IROH_EXAMPLE_SERVE") != "1" {
		fmt.Println("set IROH_EXAMPLE_SERVE=1 to keep serving echo connections")
		return nil
	}
	for {
		conn, err := ep.Accept(ctx)
		if err != nil {
			return err
		}
		go func() {
			_ = echoOnce(ctx, conn)
		}()
	}
}
