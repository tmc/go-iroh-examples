package main

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const alpn = "go-iroh-examples/source-validation/1"

	var retryChecks int
	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithALPNs(alpn),
		iroh.WithSourceAddressValidation(func(net.Addr) bool {
			retryChecks++
			return true
		}),
	)
	if err != nil {
		panic(err)
	}
	defer server.Shutdown(ctx)

	validated := make(chan bool, 1)
	go func() {
		in, err := server.AcceptIncoming(ctx)
		if err != nil {
			validated <- false
			return
		}
		validated <- in.RemoteAddrValidated()
		accepting, err := in.Accept()
		if err != nil {
			return
		}
		conn, err := accepting.Connection(ctx)
		if err != nil {
			return
		}
		_ = exampleutil.EchoOnce(ctx, conn)
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

	reply, err := exampleutil.Exchange(ctx, conn, "validated hello")
	if err != nil {
		panic(err)
	}
	fmt.Println(reply)
	fmt.Println("remote address validated:", <-validated)
	fmt.Println("retry checks:", retryChecks)
}
