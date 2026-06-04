package main

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
)

func main() {
	peerID := os.Getenv("IROH_EXAMPLE_PEER_ID")
	peerIP := os.Getenv("IROH_EXAMPLE_PEER_IP")
	peerRelay := os.Getenv("IROH_EXAMPLE_PEER_RELAY")
	alpn := os.Getenv("IROH_EXAMPLE_ALPN")
	if peerID == "" || (peerIP == "" && peerRelay == "") {
		fmt.Println("set IROH_EXAMPLE_PEER_ID and IROH_EXAMPLE_PEER_IP or IROH_EXAMPLE_PEER_RELAY")
		return
	}
	if alpn == "" {
		alpn = "go-iroh-examples/public-server/1"
	}

	id, err := key.ParseEndpointID(peerID)
	if err != nil {
		panic(err)
	}
	addr := netaddr.NewEndpointAddr(id)
	if peerIP != "" {
		ap, err := netip.ParseAddrPort(peerIP)
		if err != nil {
			panic(err)
		}
		addr = addr.WithIP(ap)
	}
	if peerRelay != "" {
		u, err := netaddr.ParseRelayURL(peerRelay)
		if err != nil {
			panic(err)
		}
		addr = addr.WithRelayURL(u)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := []iroh.Option{}
	if peerRelay != "" {
		opts = append(opts, iroh.WithRelayMode(relay.ModeDefault()))
	}
	client, err := iroh.Bind(ctx, opts...)
	if err != nil {
		panic(err)
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		panic(err)
	}
	defer conn.CloseWithError(0, "")

	reply, err := exampleutil.Exchange(ctx, conn, "public hello")
	if err != nil {
		panic(err)
	}
	fmt.Println(reply)
	fmt.Println("remote:", conn.RemoteID().Short())
}
