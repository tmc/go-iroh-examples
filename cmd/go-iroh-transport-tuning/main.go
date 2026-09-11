// Command go-iroh-transport-tuning tunes an endpoint's QUIC transport settings.
//
// [iroh.WithTransportConfig] takes the fields of [iroh.QUICTransportConfig]:
// KeepAlivePeriod, MaxIdleTimeout, InitialPacketSize, and MaxIncomingStreams.
// Those are the settings go-iroh commits to; the rest of the QUIC stack stays
// private to the endpoint, so that tuning a program does not pin it to one
// version's internals. A zero field keeps the default.
//
// KeepAlivePeriod and MaxIdleTimeout are one decision made twice. A connection
// with nothing on it is closed after MaxIdleTimeout, and the keep-alive is what
// stops it from being idle, so the period must be comfortably shorter than the
// timeout — 250ms against 3s here. A zero period lets an idle connection lapse,
// which is the right choice when reconnecting is cheap; a short timeout gives
// up on a path that is merely slow.
//
// The example leaves the connection idle for 500ms between two datagram
// exchanges: longer than the keep-alive period, shorter than the idle timeout.
// The second exchange succeeding is the evidence that the pair is consistent.
// That pause is also why the program takes about half a second.
//
// The last two lines are separate from all of the above.
// [iroh.PathMaxIdleTimeout] and [iroh.RelayPathMaxIdleTimeout] are iroh's own
// defaults for how long one path to a peer is kept, which is not the same
// question as how long the QUIC connection over those paths survives. Relay
// paths are held longer than direct ones.
//
// Compare go-iroh-datagram-vs-stream for what a datagram is good for, and
// go-iroh-path-upgrade for the paths those last two constants are about.
package main

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/iroh"
)

const alpn = "go-iroh-examples/transport-tuning/1"

// idle is how long the connection is left with nothing on it. It sits between
// the keep-alive period and the idle timeout below.
const idle = 500 * time.Millisecond

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(stdout io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Both ends are tuned the same way. Each end's settings govern its own
	// side, so a program that only tunes the dialer gets half of this.
	tuning := &iroh.QUICTransportConfig{
		KeepAlivePeriod: 250 * time.Millisecond,
		MaxIdleTimeout:  3 * time.Second,
	}
	server, err := bind(ctx,
		iroh.WithALPNs(alpn),
		iroh.WithTransportConfig(tuning),
	)
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- reflectDatagrams(ctx, server, 2)
	}()

	client, err := bind(ctx, iroh.WithTransportConfig(tuning))
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, server.Addr(), alpn)
	if err != nil {
		return err
	}
	defer conn.Close()

	first, err := datagramExchange(ctx, conn, "first")
	if err != nil {
		return fmt.Errorf("first exchange: %w", err)
	}
	// Nothing is sent for longer than the keep-alive period. The keep-alive is
	// what carries the connection across this gap.
	time.Sleep(idle)
	second, err := datagramExchange(ctx, conn, "second")
	if err != nil {
		return fmt.Errorf("exchange after %v idle: %w", idle, err)
	}
	if err := <-serverErr; err != nil {
		return fmt.Errorf("reflect: %w", err)
	}

	fmt.Fprintln(stdout, "keepalive:", tuning.KeepAlivePeriod)
	fmt.Fprintln(stdout, "max idle:", tuning.MaxIdleTimeout)
	fmt.Fprintln(stdout, first)
	fmt.Fprintln(stdout, second)
	fmt.Fprintln(stdout, "default direct idle:", iroh.PathMaxIdleTimeout)
	fmt.Fprintln(stdout, "default relay idle:", iroh.RelayPathMaxIdleTimeout)
	return nil
}

// reflectDatagrams accepts one connection and sends back the next n datagrams
// it receives.
func reflectDatagrams(ctx context.Context, ep *iroh.Endpoint, n int) error {
	conn, err := ep.Accept(ctx)
	if err != nil {
		return err
	}
	for range n {
		p, err := conn.ReadDatagram(ctx)
		if err != nil {
			return err
		}
		if err := conn.SendDatagram(p); err != nil {
			return err
		}
	}
	return nil
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

// bind binds an endpoint to an ephemeral IPv6 loopback port, then applies opts.
// Loopback binding keeps the example self-contained: no relay, no DNS, no
// network access.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
}
