package main

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const alpn = "go-iroh-examples/close-codes/1"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithALPNs(alpn),
	)
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	accepted := make(chan error, 1)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			accepted <- err
			return
		}
		accepted <- conn.CloseWithError(42, "quota exceeded")
	}()

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		return err
	}
	defer conn.CloseWithError(0, "")

	select {
	case <-conn.Context().Done():
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := <-accepted; err != nil {
		return err
	}

	appErr, ok := iroh.AsApplicationError(context.Cause(conn.Context()))
	if !ok {
		return fmt.Errorf("close cause: %v", context.Cause(conn.Context()))
	}
	fmt.Println("remote:", appErr.Remote)
	fmt.Println("code:", appErr.Code)
	fmt.Println("reason:", appErr.Reason)
	return nil
}
