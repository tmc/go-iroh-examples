package main

import (
	"context"
	"fmt"
	"net/netip"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

type hookLog struct {
	before string
	after  string
}

func (h *hookLog) BeforeConnect(_ context.Context, addr netaddr.EndpointAddr, alpn string) error {
	h.before = fmt.Sprintf("%s %s", addr.ID.Short(), alpn)
	return nil
}

func (h *hookLog) AfterHandshake(_ context.Context, conn *iroh.Conn) error {
	h.after = fmt.Sprintf("%s %s", conn.RemoteID().Short(), conn.ALPN())
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const alpn = "go-iroh-examples/hooks/1"

	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithALPNs(alpn),
	)
	if err != nil {
		panic(err)
	}
	defer server.Shutdown(ctx)

	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			return
		}
		_ = echoOnce(ctx, conn)
	}()

	hooks := new(hookLog)
	client, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithHooks(hooks),
	)
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

	reply, err := exchange(ctx, conn, "hooked hello")
	if err != nil {
		panic(err)
	}
	fmt.Println(reply)
	fmt.Println("before:", hooks.before)
	fmt.Println("after:", hooks.after)
}
