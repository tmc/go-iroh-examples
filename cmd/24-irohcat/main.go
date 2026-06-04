package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strings"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
)

const alpn = "IROHCATV0"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return demo()
	}
	switch args[0] {
	case "listen":
		fs := flag.NewFlagSet("listen", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		relayMode := fs.Bool("relay", false, "advertise a public relay address")
		bind := fs.String("bind", "[::1]:0", "UDP address to bind")
		advertise := fs.String("advertise", "", "direct address to put in the printed ticket")
		keyPath := fs.String("key", "", "endpoint secret key file")
		ticketPath := fs.String("ticket", "", "file to write the current endpoint ticket")
		if err := fs.Parse(args[1:]); err != nil {
			return usage()
		}
		if fs.NArg() != 0 {
			return usage()
		}
		return listen(*bind, *advertise, *keyPath, *ticketPath, *relayMode)
	case "connect":
		fs := flag.NewFlagSet("connect", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		bind := fs.String("bind", "[::1]:0", "UDP address to bind")
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
  irohcat listen [-relay] [-bind addr:port] [-advertise addr:port] [-key file] [-ticket file]
  irohcat connect [-bind addr:port] <ticket>`)
}

func demo() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithALPNs(alpn),
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
		_, err = io.Copy(os.Stdout, stream)
		done <- err
	}()

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		return err
	}
	defer conn.Close()
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(stream, "irohcat hello\n"); err != nil {
		return err
	}
	if err := stream.Close(); err != nil {
		return err
	}
	return <-done
}

func listen(bind, advertise, keyPath, ticketPath string, useRelay bool) error {
	ctx := context.Background()
	bindAddr, err := netip.ParseAddrPort(bind)
	if err != nil {
		return fmt.Errorf("parse bind address: %w", err)
	}
	opts := []iroh.Option{
		iroh.WithBindAddr(bindAddr),
		iroh.WithALPNs(alpn),
	}
	if keyPath != "" {
		sk, err := loadOrCreateSecretKey(keyPath)
		if err != nil {
			return err
		}
		opts = append(opts, iroh.WithSecretKey(sk))
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
			return err
		}
		cancel()
	}

	addr := ep.Addr()
	if advertise != "" {
		ap, err := netip.ParseAddrPort(advertise)
		if err != nil {
			return fmt.Errorf("parse advertise address: %w", err)
		}
		addr = netaddr.NewEndpointAddr(ep.ID()).WithIP(ap)
	}
	ticket := encodeEndpointTicket(addr)
	if ticketPath != "" {
		if err := os.WriteFile(ticketPath, []byte(ticket+"\n"), 0o644); err != nil {
			return fmt.Errorf("write ticket: %w", err)
		}
	}
	fmt.Fprintln(os.Stderr, ticket)

	conn, err := ep.Accept(ctx)
	if err != nil {
		return err
	}
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	return forward(os.Stdin, os.Stdout, stream)
}

func loadOrCreateSecretKey(path string) (key.SecretKey, error) {
	b, err := os.ReadFile(path)
	if err == nil {
		sk, err := key.ParseSecretKey(strings.TrimSpace(string(b)))
		if err != nil {
			return key.SecretKey{}, fmt.Errorf("parse key file: %w", err)
		}
		return sk, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return key.SecretKey{}, fmt.Errorf("read key file: %w", err)
	}
	sk, err := key.GenerateSecretKey()
	if err != nil {
		return key.SecretKey{}, err
	}
	seed := sk.Bytes()
	if err := os.WriteFile(path, []byte(hex.EncodeToString(seed[:])+"\n"), 0o600); err != nil {
		return key.SecretKey{}, fmt.Errorf("write key file: %w", err)
	}
	return sk, nil
}

func connect(bind, ticket string) error {
	ctx := context.Background()
	addr, err := decodeEndpointTicket(ticket)
	if err != nil {
		return err
	}
	bindAddr, err := netip.ParseAddrPort(bind)
	if err != nil {
		return fmt.Errorf("parse bind address: %w", err)
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

	conn, err := ep.Connect(ctx, addr, alpn)
	if err != nil {
		return err
	}
	defer conn.Close()
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	return forward(os.Stdin, os.Stdout, stream)
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
