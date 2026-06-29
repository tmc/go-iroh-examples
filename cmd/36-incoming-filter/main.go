// Command 36-incoming-filter demonstrates router admission control.
//
// It shows the two parts of go-iroh's custom-protocol API that decide whether
// and how an incoming connection is handled, before any application data flows:
//
//   - RouterConfig.IncomingFilter inspects each [iroh.Incoming] and returns an
//     admission outcome. It runs at QUIC admission time, before ALPN
//     negotiation, so it is the place to drop unwanted peers as early and
//     cheaply as possible — for a maintenance gate, an allow-list, or
//     back-pressure. The filter may be consulted more than once per connection
//     (for example before and after address validation), so its decision must be
//     a pure function of the [iroh.Incoming] and external state — do not count
//     calls to limit connections.
//   - AcceptingHandler.OnAccepting intercepts the verified-but-not-yet-converted
//     [iroh.Accepting] so a handler can observe the negotiated ALPN and remote
//     address (e.g. to log or annotate) before producing the [iroh.Conn].
//
// The filter here is a maintenance gate: it accepts while the server is "open"
// and rejects once the example flips it "closed". The first connection succeeds;
// after the gate closes, a second connection is refused before reaching a
// handler.
package main

import (
	"context"
	"fmt"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const alpn = "go-iroh-examples/incoming-filter/1"

	server, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		panic(err)
	}

	// open gates admission. While true the filter accepts; once false it rejects.
	// The decision is idempotent, so it is safe for the filter to run more than
	// once per connection.
	var open atomic.Bool
	open.Store(true)
	filter := func(in *iroh.Incoming) iroh.IncomingFilterOutcome {
		if open.Load() {
			return iroh.FilterAccept
		}
		fmt.Printf("filter: rejecting %s (server closed for maintenance)\n", in.RemoteAddr())
		return iroh.FilterReject
	}

	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		alpn: &loggingEchoHandler{},
	}, &iroh.RouterConfig{IncomingFilter: filter})
	if err != nil {
		panic(err)
	}
	defer router.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		panic(err)
	}
	defer client.Shutdown(ctx)

	// First connection: admitted, completes an echo exchange.
	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		panic(err)
	}
	reply, err := exchange(ctx, conn, "filtered hello")
	if err != nil {
		panic(err)
	}
	fmt.Println("first client reply:", reply)
	conn.CloseWithError(0, "")

	// Close the gate; further connections are refused by the filter. A filtered
	// connection may complete the optimistic QUIC handshake, so the refusal is
	// observed when the client first tries to use the connection.
	open.Store(false)

	second, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		fmt.Println("second client refused at connect:", err)
		return
	}
	defer second.CloseWithError(0, "")
	if _, err := exchange(ctx, second, "after maintenance"); err != nil {
		fmt.Println("second client refused:", err)
	} else {
		fmt.Println("second client unexpectedly admitted")
	}
}
