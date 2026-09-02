// Command 49-custom-transport runs an iroh connection over a transport of its own.
//
// An endpoint normally reaches its peers over UDP, either directly or through a
// relay. [iroh.WithCustomTransport] adds a third kind of carrier: any code that
// can move a datagram from one endpoint to another. Everything above it is
// unchanged — the same QUIC connection, the same endpoint-ID authentication, the
// same path selection — so a Bluetooth link, a serial line, an overlay network
// that already exists, or, as here, a queue inside one process becomes something
// iroh connections can run on. Choose it when the carrier is one iroh does not
// know about; it is not a way to tune UDP, which is what
// [iroh.WithTransportConfig] is for.
//
// A transport implements [iroh.CustomTransport]: Send hands a datagram to the
// carrier, and Serve delivers what arrives back to the endpoint as an
// [iroh.CustomDatagram]. Peers are named by [netaddr.CustomAddr], a freely
// chosen 64-bit transport id plus opaque bytes that only the transport
// interprets; the id distinguishes transports that share a network, and n0
// keeps a registry of well-known values. A transport that also implements
// [iroh.AdvertisingCustomTransport] returns its local addresses from
// LocalCustomAddrs, which puts them in [iroh.Endpoint.Addr] so that tickets and
// discovery carry them like any other address. Without that method the peer has
// to be told the [netaddr.CustomAddr] some other way.
//
// The transport here is a bus: a map from address to a buffered Go channel, with
// counters. Two endpoints attach to it, both with [iroh.WithoutIPTransports] and
// [iroh.WithoutRelayTransports] so that the bus is the only way for them to
// reach each other, and one dials the other by the address the bus advertised.
// The output shows what the connection ran on: the advertised address is a
// custom one, the selected path is "custom", and the bus counters move while the
// application exchange happens. Nothing binds a socket and nothing leaves the
// process.
//
// Two obligations fall on a real implementation and are visible below. Send must
// copy the packet it is given, because the endpoint reuses the buffer as soon as
// Send returns. And Serve must keep running when recv reports false: a false
// result means the endpoint's receive queue is full, which is a dropped
// datagram, not a reason to stop the transport. Both directions may lose
// datagrams — QUIC handles the loss.
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const alpn = "go-iroh-examples/custom-transport/1"

// transportID identifies this transport in a [netaddr.CustomAddr]. The value is
// the implementation's to choose; n0 lists the well-known ones at
// https://github.com/n0-computer/iroh/blob/main/TRANSPORTS.md. Two endpoints
// only ever exchange datagrams through a transport that shares the id.
const transportID = 0x676f2d69726f68 // "go-iroh"

// bus is an in-process packet switch: every attached port has a buffered
// channel, and a datagram addressed to a port is copied into it.
type bus struct {
	mu    sync.Mutex
	ports map[string]chan iroh.CustomDatagram

	delivered map[string]*atomic.Int64
	dropped   atomic.Int64
}

func newBus() *bus {
	return &bus{
		ports:     make(map[string]chan iroh.CustomDatagram),
		delivered: make(map[string]*atomic.Int64),
	}
}

// attach returns a port named name. The name is the address data a peer dials.
func (b *bus) attach(name string) *port {
	p := &port{
		bus:  b,
		addr: netaddr.NewCustomAddr(transportID, []byte(name)),
		in:   make(chan iroh.CustomDatagram, 64),
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ports[p.addr.String()] = p.in
	b.delivered[name] = new(atomic.Int64)
	return p
}

// delivered counts the datagrams the bus has handed to the port named name.
func (b *bus) count(name string) int64 { return b.delivered[name].Load() }

// send queues one datagram for to. It reports whether the bus took it; a full
// queue or an unknown address is a drop, which QUIC recovers from.
func (b *bus) send(from, to netaddr.CustomAddr, data []byte) bool {
	b.mu.Lock()
	in, ok := b.ports[to.String()]
	b.mu.Unlock()
	if !ok {
		b.dropped.Add(1)
		return false
	}
	// The endpoint reuses data once Send returns, so the bus owns a copy.
	d := iroh.CustomDatagram{
		Remote:   from,
		Local:    to,
		HasLocal: true,
		Data:     append([]byte(nil), data...),
	}
	select {
	case in <- d:
		b.delivered[string(to.Data())].Add(1)
		return true
	default:
		b.dropped.Add(1)
		return false
	}
}

// port is one endpoint's attachment to a [bus].
type port struct {
	bus  *bus
	addr netaddr.CustomAddr
	in   chan iroh.CustomDatagram
}

var (
	_ iroh.CustomTransport            = (*port)(nil)
	_ iroh.AdvertisingCustomTransport = (*port)(nil)
)

// Serve delivers datagrams to the endpoint until ctx ends.
func (p *port) Serve(ctx context.Context, recv func(iroh.CustomDatagram) bool) {
	for {
		select {
		case <-ctx.Done():
			return
		case d := <-p.in:
			// A false result is a full receive queue: the datagram is lost, the
			// transport keeps running.
			if !recv(d) {
				p.bus.dropped.Add(1)
			}
		}
	}
}

// Send hands one datagram to the bus. local is the local address the path
// selected, which a transport with several of them would send from; this one
// has exactly one.
func (p *port) Send(remote netaddr.CustomAddr, local *netaddr.CustomAddr, data []byte) bool {
	return p.bus.send(p.addr, remote, data)
}

// LocalCustomAddrs makes this port's address part of [iroh.Endpoint.Addr].
func (p *port) LocalCustomAddrs(context.Context) ([]netaddr.CustomAddr, error) {
	return []netaddr.CustomAddr{p.addr}, nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := newBus()

	// Both endpoints refuse IP and relay transports, so the bus is the only
	// carrier left. Neither binds a UDP socket, which is why these do not use
	// the loopback bind the other examples share.
	server, err := iroh.Bind(ctx,
		iroh.WithALPNs(alpn),
		iroh.WithCustomTransport(b.attach("server")),
		iroh.WithoutIPTransports(),
		iroh.WithoutRelayTransports(),
	)
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			return
		}
		_ = exampleutil.Echo(ctx, conn)
	}()

	client, err := iroh.Bind(ctx,
		iroh.WithCustomTransport(b.attach("client")),
		iroh.WithoutIPTransports(),
		iroh.WithoutRelayTransports(),
	)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	// The address the server advertises comes from LocalCustomAddrs. It holds
	// no IP address at all, so a peer that receives it as a ticket can only
	// reach the server through a transport with the same id.
	addr := server.Addr()
	for _, a := range addr.Addrs() {
		fmt.Println("advertised:", a.Network(), a.String())
	}
	fmt.Println("advertised ip addresses:", len(addr.IPAddrs()))

	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.CloseWithError(0, "")
	fmt.Println("handshake reached the server:", conn.RemoteID() == server.ID())
	fmt.Println("path kind:", exampleutil.SelectedPathKind(conn.Paths()))

	// The handshake already crossed the bus; the counters below prove the
	// application data does too. Exact packet counts depend on QUIC's pacing,
	// so the example compares them rather than printing them.
	toServer, toClient := b.count("server"), b.count("client")
	reply, err := exampleutil.Exchange(ctx, conn, "over the bus")
	if err != nil {
		return err
	}
	fmt.Println("reply:", reply)
	fmt.Println("bus carried client to server:", b.count("server") > toServer)
	fmt.Println("bus carried server to client:", b.count("client") > toClient)
	fmt.Println("bus dropped:", b.dropped.Load())
	return nil
}
