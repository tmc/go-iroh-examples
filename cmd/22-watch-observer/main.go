package main

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const alpn = "go-iroh-examples/watch-observer/1"

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ep, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithALPNs(alpn),
	)
	if err != nil {
		panic(err)
	}
	defer ep.Shutdown(ctx)
	done := serve(ctx, ep, 2)

	obs := ep.WatchAddr()
	current := obs.Current()
	fmt.Println("current addrs:", len(current.IPAddrs()))
	fmt.Println("first reply:", dial(ctx, current, "first"))

	updated := make(chan error, 1)
	go func() {
		addr, err := obs.Updated(ctx)
		if err != nil {
			updated <- err
			return
		}
		fmt.Println("updated has external:", containsAddr(addr.IPAddrs(), externalAddr()))
		updated <- nil
	}()

	ep.AddExternalAddr(externalAddr())
	if err := <-updated; err != nil {
		panic(err)
	}
	fmt.Println("second reply:", dial(ctx, current, "second"))

	streamCtx, stopStream := context.WithCancel(ctx)
	defer stopStream()
	seen := 0
	for addr := range ep.WatchAddr().Stream(streamCtx) {
		fmt.Println("stream addrs:", len(addr.IPAddrs()))
		seen++
		if seen == 2 {
			break
		}
		ep.AddExternalAddr(netip.MustParseAddrPort("192.0.2.56:12345"))
	}
	if err := <-done; err != nil {
		panic(err)
	}
}

func externalAddr() netip.AddrPort {
	return netip.MustParseAddrPort("192.0.2.55:12345")
}

func containsAddr(addrs []netip.AddrPort, want netip.AddrPort) bool {
	for _, addr := range addrs {
		if addr == want {
			return true
		}
	}
	return false
}

func serve(ctx context.Context, ep *iroh.Endpoint, n int) <-chan error {
	done := make(chan error, 1)
	go func() {
		for range n {
			conn, err := ep.Accept(ctx)
			if err != nil {
				done <- err
				return
			}
			stream, err := conn.AcceptStream(ctx)
			if err != nil {
				done <- err
				return
			}
			if _, err := io.Copy(stream, stream); err != nil {
				done <- err
				return
			}
			if err := stream.Close(); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	return done
}

func dial(ctx context.Context, addr netaddr.EndpointAddr, msg string) string {
	ep, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		panic(err)
	}
	defer ep.Shutdown(ctx)

	conn, err := ep.Connect(ctx, addr, alpn)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		panic(err)
	}
	if _, err := io.WriteString(stream, msg); err != nil {
		panic(err)
	}
	if err := stream.Close(); err != nil {
		panic(err)
	}
	reply, err := io.ReadAll(stream)
	if err != nil {
		panic(err)
	}
	return string(reply)
}
