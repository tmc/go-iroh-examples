package main

import (
	"context"
	"fmt"
	"net/netip"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

const alpn = "go-iroh-examples/memory-mesh/1"

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lookup := iroh.NewMemoryLookup()
	var lookups iroh.AddressLookupServices
	lookups.AddResolver(lookup)

	nodes := make([]*iroh.Endpoint, 3)
	routers := make([]*iroh.Router, 0, len(nodes)-1)
	delivered := make(chan string, len(nodes)-1)
	for i := range nodes {
		ep, err := iroh.Bind(ctx,
			iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
			iroh.WithAddressLookup(&lookups),
		)
		if err != nil {
			panic(err)
		}
		defer ep.Shutdown(ctx)
		nodes[i] = ep
		lookup.AddEndpointAddr(ep.Addr())

		if i == 0 {
			continue
		}
		router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{
			alpn: meshHandler{index: i, delivered: delivered},
		}, nil)
		if err != nil {
			panic(err)
		}
		defer router.Shutdown(ctx)
		routers = append(routers, router)
	}
	_ = routers

	conns := make([]*iroh.Conn, 0, len(nodes)-1)
	for i := 1; i < len(nodes); i++ {
		addr := resolve(ctx, lookup, nodes[i].ID())
		conn, err := nodes[0].Connect(ctx, addr, alpn)
		if err != nil {
			panic(err)
		}
		if err := send(ctx, conn, fmt.Sprintf("broadcast to node %d", i)); err != nil {
			panic(err)
		}
		conns = append(conns, conn)
	}
	for range len(nodes) - 1 {
		fmt.Println(<-delivered)
	}
	for _, conn := range conns {
		conn.Close()
	}
}

type meshHandler struct {
	index     int
	delivered chan<- string
}

func (h meshHandler) Accept(ctx context.Context, conn *iroh.Conn) error {
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	var b [256]byte
	n, err := stream.Read(b[:])
	if err != nil && n == 0 {
		return err
	}
	h.delivered <- fmt.Sprintf("node %d got %q", h.index, b[:n])
	return nil
}

func resolve(ctx context.Context, lookup *iroh.MemoryLookup, id key.EndpointID) netaddr.EndpointAddr {
	for item, err := range lookup.Resolve(ctx, id) {
		if err != nil {
			panic(err)
		}
		return item.Addr()
	}
	panic("memory lookup returned no results")
}

func send(ctx context.Context, conn *iroh.Conn, msg string) error {
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	if _, err := stream.Write([]byte(msg)); err != nil {
		stream.Close()
		return err
	}
	return stream.Close()
}
