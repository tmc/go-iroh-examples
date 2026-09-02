// Command go-iroh-watch-observer follows values that change over time.
//
// go-iroh reports mutable state through [watch.Value] and the [watch.Observer]
// it hands out. An observer offers three ways to read the same value, and which
// one to use is the whole lesson:
//
//   - Current returns the value now and never blocks.
//   - Updated blocks until the value differs from the last one the caller saw.
//   - Stream ranges over every distinct value until its context ends.
//
// The example uses a plain [watch.Value] to show the three shapes, then the same
// three against [iroh.Endpoint.WatchAddr], whose value is the endpoint's own
// address as paths and external addresses come and go. An endpoint's address is
// not fixed at bind time, so anything that publishes or shares it wants the
// observer rather than a single read.
package main

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/watch"
)

const alpn = "go-iroh-examples/watch-observer/1"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := watchValue(ctx); err != nil {
		return err
	}
	return watchEndpointAddr(ctx)
}

// watchValue exercises the three observer shapes on a bare [watch.Value],
// with no endpoint involved.
func watchValue(ctx context.Context) error {
	value := watch.NewValue("starting")
	obs := value.Watch()
	fmt.Println("current:", obs.Current())

	updated := make(chan error, 1)
	go func() {
		next, err := obs.Updated(ctx)
		if err != nil {
			updated <- err
			return
		}
		fmt.Println("updated:", next)
		updated <- nil
	}()
	value.Set("ready")
	if err := <-updated; err != nil {
		return err
	}

	// NewValueFunc takes the equality used to decide what counts as a change.
	// Setting 1 twice in a row produces one stream value, not two.
	unique := watch.NewValueFunc(0, func(a, b int) bool { return a == b })
	streamCtx, stopStream := context.WithCancel(ctx)
	defer stopStream()
	seen := 0
	for n := range unique.Watch().Stream(streamCtx) {
		fmt.Println("stream:", n)
		seen++
		if seen == 3 {
			break
		}
		switch n {
		case 0:
			unique.Set(1)
		case 1:
			unique.Set(1)
			unique.Set(2)
		}
	}
	return nil
}

// watchEndpointAddr runs the same three shapes against an endpoint's own
// address, which changes as external addresses are learned.
func watchEndpointAddr(ctx context.Context) error {
	ep, err := exampleutil.Bind(ctx, iroh.WithALPNs(alpn))
	if err != nil {
		return err
	}
	defer ep.Shutdown(ctx)
	done := serve(ctx, ep, 2)

	obs := ep.WatchAddr()
	current := obs.Current()
	fmt.Println("current addrs:", len(current.IPAddrs()))
	reply, err := dial(ctx, current, "first")
	if err != nil {
		return err
	}
	fmt.Println("first reply:", reply)

	updated := make(chan error, 1)
	go func() {
		// Updated reports the next value, whatever it is. An endpoint's
		// address can change for reasons the caller did not ask for, so a
		// caller waiting for one specific change keeps reading until it sees
		// it rather than assuming the first update is the one it wanted.
		for {
			addr, err := obs.Updated(ctx)
			if err != nil {
				updated <- err
				return
			}
			if containsAddr(addr.IPAddrs(), externalAddr()) {
				fmt.Println("updated has external:", true)
				updated <- nil
				return
			}
		}
	}()

	ep.AddExternalAddr(externalAddr())
	if err := <-updated; err != nil {
		return err
	}
	reply, err = dial(ctx, current, "second")
	if err != nil {
		return err
	}
	fmt.Println("second reply:", reply)

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
	return <-done
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

// serve echoes one stream on each of the next n connections.
func serve(ctx context.Context, ep *iroh.Endpoint, n int) <-chan error {
	done := make(chan error, 1)
	go func() {
		for range n {
			conn, err := ep.Accept(ctx)
			if err != nil {
				done <- err
				return
			}
			if err := exampleutil.Echo(ctx, conn); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	return done
}

func dial(ctx context.Context, addr netaddr.EndpointAddr, msg string) (string, error) {
	ep, err := exampleutil.Bind(ctx)
	if err != nil {
		return "", err
	}
	defer ep.Shutdown(ctx)

	conn, err := ep.Connect(ctx, addr, alpn)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return exampleutil.Exchange(ctx, conn, msg)
}
