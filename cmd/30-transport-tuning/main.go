package main

import (
	"context"
	"fmt"
	"net/netip"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const alpn = "go-iroh-examples/transport-tuning/1"

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tuning := &iroh.QUICTransportConfig{
		KeepAlivePeriod: 250 * time.Millisecond,
		MaxIdleTimeout:  3 * time.Second,
	}
	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithALPNs(alpn),
		iroh.WithTransportConfig(tuning),
	)
	if err != nil {
		panic(err)
	}
	defer server.Shutdown(ctx)

	done := make(chan error, 1)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			done <- err
			return
		}
		for range 2 {
			p, err := conn.ReadDatagram(ctx)
			if err != nil {
				done <- err
				return
			}
			if err := conn.SendDatagram(p); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	client, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithTransportConfig(tuning),
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
	defer conn.Close()

	first, err := datagramExchange(ctx, conn, "first")
	if err != nil {
		panic(err)
	}
	time.Sleep(500 * time.Millisecond)
	second, err := datagramExchange(ctx, conn, "second")
	if err != nil {
		panic(err)
	}
	if err := <-done; err != nil {
		panic(err)
	}

	fmt.Println("keepalive:", tuning.KeepAlivePeriod)
	fmt.Println("max idle:", tuning.MaxIdleTimeout)
	fmt.Println(first)
	fmt.Println(second)
	fmt.Println("default direct idle:", iroh.PathMaxIdleTimeout)
	fmt.Println("default relay idle:", iroh.RelayPathMaxIdleTimeout)
}

func datagramExchange(ctx context.Context, conn *iroh.Conn, msg string) (string, error) {
	if err := conn.SendDatagram([]byte(msg)); err != nil {
		return "", err
	}
	p, err := conn.ReadDatagram(ctx)
	if err != nil {
		return "", err
	}
	return string(p), nil
}
