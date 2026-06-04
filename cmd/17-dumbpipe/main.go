package main

import (
	"context"
	"errors"
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
		panic(err)
	}
}

func run() error {
	args := os.Args[1:]
	if len(args) == 0 {
		return demo()
	}
	switch args[0] {
	case "listen":
		return listen()
	case "connect":
		if len(args) != 2 {
			return errors.New("usage: go run ./cmd/17-dumbpipe connect <endpoint-ticket>")
		}
		return connect(args[1])
	default:
		return errors.New("usage: go run ./cmd/17-dumbpipe [listen|connect <endpoint-ticket>]")
	}
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

func listen() error {
	ctx := context.Background()
	bind, err := bindAddr()
	if err != nil {
		return err
	}
	opts := []iroh.Option{
		iroh.WithBindAddr(bind),
		iroh.WithALPNs(dumbpipeALPN),
	}
	if os.Getenv("GO_IROH_LIVE_RELAY") == "1" {
		opts = append(opts, iroh.WithRelayMode(relay.ModeDefault()))
	}
	ep, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return err
	}
	defer ep.Shutdown(ctx)
	if os.Getenv("GO_IROH_LIVE_RELAY") == "1" {
		onlineCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
		if err := ep.Online(onlineCtx); err != nil {
			cancel()
			return err
		}
		cancel()
	}

	addr := ep.Addr()
	if advertised := os.Getenv("GO_IROH_DUMBPIPE_ADVERTISE_ADDR"); advertised != "" {
		ap, err := netip.ParseAddrPort(advertised)
		if err != nil {
			return fmt.Errorf("parse GO_IROH_DUMBPIPE_ADVERTISE_ADDR: %w", err)
		}
		addr = netaddr.NewEndpointAddr(ep.ID()).WithIP(ap)
	}
	ticket := encodeEndpointTicket(addr)
	fmt.Fprintf(os.Stderr, "Listening. To connect with Rust dumbpipe, use:\ndumbpipe connect %s\n", ticket)

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

func connect(ticket string) error {
	ctx := context.Background()
	addr, err := decodeEndpointTicket(ticket)
	if err != nil {
		return err
	}
	bind, err := bindAddr()
	if err != nil {
		return err
	}
	opts := []iroh.Option{iroh.WithBindAddr(bind)}
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
	return forward(os.Stdin, os.Stdout, stream)
}

func bindAddr() (netip.AddrPort, error) {
	s := os.Getenv("GO_IROH_DUMBPIPE_BIND_ADDR")
	if s == "" {
		return netip.AddrPortFrom(netip.IPv6Loopback(), 0), nil
	}
	addr, err := netip.ParseAddrPort(s)
	if err != nil {
		return netip.AddrPort{}, fmt.Errorf("parse GO_IROH_DUMBPIPE_BIND_ADDR: %w", err)
	}
	return addr, nil
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
