package main

import (
	"context"
	"fmt"
	"net/netip"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const (
		echoALPN  = "go-iroh-examples/multi-alpn/echo/1"
		upperALPN = "go-iroh-examples/multi-alpn/upper/1"
	)

	server, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		panic(err)
	}

	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		echoALPN:  exampleutil.EchoHandler{},
		upperALPN: exampleutil.UpperHandler{},
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
	echoConn, err := client.Connect(ctx, addr, echoALPN)
	if err != nil {
		panic(err)
	}
	defer echoConn.CloseWithError(0, "")

	upperConn, err := client.Connect(ctx, addr, upperALPN)
	if err != nil {
		panic(err)
	}
	defer upperConn.CloseWithError(0, "")

	echoReply, err := exampleutil.Exchange(ctx, echoConn, "multi hello")
	if err != nil {
		panic(err)
	}
	upperReply, err := exampleutil.Exchange(ctx, upperConn, "multi hello")
	if err != nil {
		panic(err)
	}

	fmt.Println("echo:", echoReply)
	fmt.Println("upper:", upperReply)
}
