package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const alpn = "go-iroh-examples/sendme-file/1"
	payload := []byte("sendme-style file contents\n")
	if path := os.Getenv("IROH_EXAMPLE_FILE"); path != "" {
		var err error
		payload, err = os.ReadFile(path)
		if err != nil {
			panic(err)
		}
	}

	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithALPNs(alpn),
	)
	if err != nil {
		panic(err)
	}
	defer server.Shutdown(ctx)

	done := make(chan error, 1)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			done <- err
			return
		}
		s, err := conn.AcceptStream(ctx)
		if err != nil {
			done <- err
			return
		}
		if _, err := io.Copy(io.Discard, s); err != nil {
			done <- err
			return
		}
		_, err = s.Write(payload)
		if closeErr := s.Close(); err == nil {
			err = closeErr
		}
		done <- err
	}()

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		panic(err)
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		panic(err)
	}
	if err := s.Close(); err != nil {
		panic(err)
	}
	got, err := io.ReadAll(s)
	if err != nil {
		panic(err)
	}
	if err := <-done; err != nil {
		panic(err)
	}

	sum := sha256.Sum256(got)
	fmt.Println("bytes:", len(got))
	fmt.Printf("sha256: %x\n", sum)
}
