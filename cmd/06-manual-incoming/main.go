package main

import (
	"context"
	"fmt"
	"net/netip"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const alpn = "go-iroh-examples/manual-incoming/1"

	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithALPNs(alpn),
	)
	if err != nil {
		panic(err)
	}
	defer server.Shutdown(ctx)

	accepted := make(chan string, 1)
	go func() {
		in, err := server.AcceptIncoming(ctx)
		if err != nil {
			accepted <- err.Error()
			return
		}
		accepting, err := in.Accept()
		if err != nil {
			accepted <- err.Error()
			return
		}
		got, err := accepting.ALPN(ctx)
		if err != nil {
			accepted <- err.Error()
			return
		}
		conn, err := accepting.Connection(ctx)
		if err != nil {
			accepted <- err.Error()
			return
		}
		accepted <- fmt.Sprintf("%s from %s", got, conn.RemoteID().Short())
		_ = echoOnce(ctx, conn)
	}()

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		panic(err)
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		panic(err)
	}
	defer conn.CloseWithError(0, "")

	reply, err := exchange(ctx, conn, "manual hello")
	if err != nil {
		panic(err)
	}
	fmt.Println(reply)
	fmt.Println(<-accepted)
}
