package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
)

const (
	dumbpipeALPN = "DUMBPIPEV0"
	handshake    = "hello"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	args := os.Args[1:]
	if len(args) == 0 {
		return demo()
	}
	switch args[0] {
	case "listen":
		fs := flag.NewFlagSet("listen", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		bind := fs.String("bind", bindAddrDefault(), "UDP address to bind")
		advertise := fs.String("advertise", "", "direct address to put in the printed ticket")
		useRelay := fs.Bool("relay", false, "advertise a public relay address")
		if err := fs.Parse(args[1:]); err != nil {
			return usage()
		}
		if fs.NArg() != 0 {
			return usage()
		}
		return listen(*bind, *advertise, *useRelay)
	case "connect":
		fs := flag.NewFlagSet("connect", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		bind := fs.String("bind", bindAddrDefault(), "UDP address to bind")
		if err := fs.Parse(args[1:]); err != nil {
			return usage()
		}
		if fs.NArg() != 1 {
			return usage()
		}
		return connect(*bind, fs.Arg(0))
	default:
		return usage()
	}
}

func usage() error {
	return errors.New(`usage:
  dumbpipe listen [-relay] [-bind addr:port] [-advertise addr:port]
  dumbpipe connect [-bind addr:port] <endpoint-ticket>

The default bind address is loopback for local demos. For another machine, use
listen -relay, or bind to a reachable UDP address and advertise that address.`)
}

func demo() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	input := []byte("pipe hello\n")
	if s := os.Getenv("IROH_EXAMPLE_PIPE_INPUT"); s != "" {
		input = []byte(s)
	}

	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithALPNs(dumbpipeALPN),
	)
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	done := make(chan error, 1)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			done <- err
			return
		}
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			done <- err
			return
		}
		if err := readHandshake(stream); err != nil {
			done <- err
			return
		}
		_, err = io.Copy(os.Stdout, stream)
		done <- err
	}()

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr()), dumbpipeALPN)
	if err != nil {
		return err
	}
	defer conn.Close()

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	if _, err := stream.Write([]byte(handshake)); err != nil {
		return err
	}
	if _, err := stream.Write(input); err != nil {
		return err
	}
	if err := stream.Close(); err != nil {
		return err
	}
	if err := <-done; err != nil {
		return err
	}
	fmt.Println("bytes piped:", len(input))
	return nil
}

func listen(bind, advertise string, useRelay bool) error {
	ctx := context.Background()
	bindAddr, err := parseBindAddr(bind)
	if err != nil {
		return err
	}
	opts := []iroh.Option{
		iroh.WithBindAddr(bindAddr),
		iroh.WithALPNs(dumbpipeALPN),
	}
	if os.Getenv("GO_IROH_LIVE_RELAY") == "1" {
		useRelay = true
	}
	if useRelay {
		opts = append(opts, iroh.WithRelayMode(relay.ModeDefault()))
	}
	ep, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return err
	}
	defer ep.Shutdown(ctx)
	if useRelay {
		onlineCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
		if err := ep.Online(onlineCtx); err != nil {
			cancel()
			return fmt.Errorf("connect to public relay map: %w", err)
		}
		cancel()
	}

	addr := ep.Addr()
	if advertise == "" {
		advertise = os.Getenv("GO_IROH_DUMBPIPE_ADVERTISE_ADDR")
	}
	if advertise != "" {
		ap, err := netip.ParseAddrPort(advertise)
		if err != nil {
			return fmt.Errorf("parse GO_IROH_DUMBPIPE_ADVERTISE_ADDR: %w", err)
		}
		addr = netaddr.NewEndpointAddr(ep.ID()).WithIP(ap)
	}
	ticket := encodeEndpointTicket(addr)
	fmt.Fprintf(os.Stderr, "Listening.\nGo:   go run ./cmd/17-dumbpipe connect %s\nRust: dumbpipe connect %s\n", ticket, ticket)
	if len(addr.RelayURLs()) == 0 && loopbackOnly(addr) {
		fmt.Fprintln(os.Stderr, "This ticket only contains loopback addresses. It works on this machine only; use -relay or -advertise for another machine.")
	}

	conn, err := ep.Accept(ctx)
	if err != nil {
		return err
	}
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	if err := readHandshake(stream); err != nil {
		return err
	}
	return forward(os.Stdin, os.Stdout, stream)
}

func connect(bind, ticket string) error {
	ctx := context.Background()
	addr, err := decodeEndpointTicket(ticket)
	if err != nil {
		return err
	}
	bindAddr, err := parseBindAddr(bind)
	if err != nil {
		return err
	}
	opts := []iroh.Option{iroh.WithBindAddr(bindAddr)}
	if relays := addr.RelayURLs(); len(relays) > 0 {
		opts = append(opts, iroh.WithRelayMode(relay.ModeCustomURLs(relays...)))
	}
	ep, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return err
	}
	defer ep.Shutdown(ctx)
	if len(addr.RelayURLs()) > 0 && len(addr.IPAddrs()) == 0 {
		onlineCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
		if err := ep.Online(onlineCtx); err != nil {
			cancel()
			return err
		}
		cancel()
	}

	conn, err := ep.Connect(ctx, addr, dumbpipeALPN)
	if err != nil {
		return fmt.Errorf("connect to %s: %w%s", addr.ID, err, connectHint(addr))
	}
	defer conn.Close()
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	if _, err := stream.Write([]byte(handshake)); err != nil {
		return err
	}
	return forward(os.Stdin, os.Stdout, stream)
}

func bindAddrDefault() string {
	s := os.Getenv("GO_IROH_DUMBPIPE_BIND_ADDR")
	if s == "" {
		return "[::1]:0"
	}
	return s
}

func parseBindAddr(s string) (netip.AddrPort, error) {
	addr, err := netip.ParseAddrPort(s)
	if err != nil {
		return netip.AddrPort{}, fmt.Errorf("parse bind address: %w", err)
	}
	return addr, nil
}

func loopbackOnly(addr netaddr.EndpointAddr) bool {
	ips := addr.IPAddrs()
	if len(ips) == 0 {
		return false
	}
	for _, ap := range ips {
		if !ap.Addr().IsLoopback() {
			return false
		}
	}
	return true
}

func connectHint(addr netaddr.EndpointAddr) string {
	if len(addr.RelayURLs()) == 0 && loopbackOnly(addr) {
		return "\nreceived a loopback-only ticket; it is usable only on the same machine. Start the listener with -relay for public relay connectivity, or with -bind/-advertise for a reachable direct UDP address."
	}
	return ""
}

func readHandshake(r io.Reader) error {
	buf := make([]byte, len(handshake))
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	if string(buf) != handshake {
		return fmt.Errorf("invalid dumbpipe handshake %q", string(buf))
	}
	return nil
}

func forward(stdin io.Reader, stdout io.Writer, stream io.ReadWriteCloser) error {
	errc := make(chan error, 2)
	go func() {
		_, err := io.Copy(stream, stdin)
		if closeErr := stream.Close(); err == nil {
			err = closeErr
		}
		errc <- err
	}()
	go func() {
		_, err := io.Copy(stdout, stream)
		errc <- err
	}()
	var firstErr error
	for range 2 {
		if err := <-errc; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
