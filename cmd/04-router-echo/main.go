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

	const alpn = "go-iroh-examples/router-echo/1"

	server, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		panic(err)
	}

	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		alpn: echoHandler{},
	}, nil)
	if err != nil {
		panic(err)
	}
	defer router.Shutdown(ctx)

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

	reply, err := exchange(ctx, conn, "router hello")
	if err != nil {
		panic(err)
	}
	fmt.Println(reply)
}
