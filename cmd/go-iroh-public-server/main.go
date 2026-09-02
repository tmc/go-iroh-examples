// Command go-iroh-public-server binds an endpoint on a routable UDP port.
//
// Examples 01 through 10 bind an ephemeral loopback port, which is all a demo
// running both halves in one process needs. A server that other machines dial
// needs the two things this example shows: a fixed UDP port, so the address
// survives a restart and a firewall rule can name it, and an address a dialer
// can be handed out of band. The address is printed in pieces — endpoint ID,
// ALPN, direct paths, relay paths — because those are the fields
// go-iroh-connect-public takes as flags. For the same address as one pasteable
// string, see go-iroh-tickets.
//
// [iroh.Bind] is direct-only by default: a UDP socket and no relay. -live adds
// [relay.ModeDefault] and waits for [iroh.Endpoint.Online], so that the printed
// relay paths give a dialer a way in when the direct address sits behind a NAT.
// go-iroh-relay-online is that opt-in on its own.
//
// Without -serve the example prints the address and exits. With -serve it
// accepts connections and echoes each one, which is what go-iroh-connect-public
// expects to find on the other end.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/netip"
	"os"
	"strconv"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/relay"
)

const defaultALPN = "go-iroh-examples/public-server/1"

func main() {
	if err := run(os.Args[1:]); err != nil {
		// -h is a request for the usage message, which the flag package has
		// already printed. It is not a failure.
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	defPort, err := strconv.ParseUint(exampleutil.Env("IROH_EXAMPLE_PORT", "4433"), 10, 16)
	if err != nil {
		return fmt.Errorf("parse IROH_EXAMPLE_PORT: %w", err)
	}

	fs := flag.NewFlagSet("go-iroh-public-server", flag.ContinueOnError)
	port := fs.Uint("port", uint(defPort), "UDP port to bind on every IPv4 interface ($IROH_EXAMPLE_PORT)")
	alpn := fs.String("alpn", exampleutil.Env("IROH_EXAMPLE_ALPN", defaultALPN), "ALPN to accept ($IROH_EXAMPLE_ALPN)")
	serve := fs.Bool("serve", exampleutil.EnvBool("IROH_EXAMPLE_SERVE", false), "keep accepting echo connections instead of exiting after printing the address ($IROH_EXAMPLE_SERVE)")
	live := fs.Bool("live", exampleutil.EnvBool("GO_IROH_LIVE_RELAY", false), "advertise a public relay and wait for relay connectivity ($GO_IROH_LIVE_RELAY)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *port > 65535 {
		return fmt.Errorf("port %d out of range", *port)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	opts := []iroh.Option{
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv4Unspecified(), uint16(*port))),
		iroh.WithALPNs(*alpn),
	}
	if *live {
		opts = append(opts, iroh.WithRelayMode(relay.ModeDefault()))
	}

	ep, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return err
	}
	defer ep.Shutdown(ctx)

	if *live {
		onlineCtx, cancelOnline := context.WithTimeout(ctx, 30*time.Second)
		err := ep.Online(onlineCtx)
		cancelOnline()
		if err != nil {
			return fmt.Errorf("connect to public relay map: %w", err)
		}
	}

	fmt.Println("endpoint id:", ep.ID().Z32())
	fmt.Println("alpn:", *alpn)
	fmt.Println("direct paths:", ep.Addr().IPAddrs())
	fmt.Println("relay paths:", ep.Addr().RelayURLs())
	fmt.Println("local udp:", ep.LocalAddr())

	if !*serve {
		fmt.Println("pass -serve or set IROH_EXAMPLE_SERVE=1 to keep serving echo connections")
		return nil
	}
	for {
		conn, err := ep.Accept(ctx)
		if err != nil {
			return err
		}
		go func() {
			_ = exampleutil.Echo(ctx, conn)
		}()
	}
}
