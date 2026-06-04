package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/netip"
	"strings"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const alpn = "go-iroh-examples/stream-netconn-deadline/1"

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithALPNs(alpn),
	)
	if err != nil {
		panic(err)
	}
	defer server.Shutdown(ctx)

	done := make(chan struct{})
	release := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := server.Accept(ctx)
		if err != nil {
			return
		}
		defer conn.Close()
		defer func() { <-release }()

		stream, err := conn.AcceptStreamConn(ctx)
		if err != nil {
			return
		}
		defer stream.Close()

		if err := stream.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return
		}
		line, err := bufio.NewReader(stream).ReadString('\n')
		if err != nil {
			return
		}
		_, _ = io.WriteString(stream, strings.ToUpper(line))
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
	defer conn.Close()

	stream, err := conn.OpenStreamConn(ctx)
	if err != nil {
		panic(err)
	}
	defer stream.Close()

	if err := stream.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		panic(err)
	}
	if _, err := io.WriteString(stream, "deadline example\n"); err != nil {
		panic(err)
	}
	reply, err := bufio.NewReader(stream).ReadString('\n')
	if err != nil {
		panic(err)
	}
	fmt.Print(reply)

	close(release)
	<-done
}
