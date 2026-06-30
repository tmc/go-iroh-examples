package main

import (
	"bytes"
	"context"
	"fmt"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	payload := []byte("sendme-style file contents\n")
	if path := os.Getenv("IROH_EXAMPLE_FILE"); path != "" {
		var err error
		payload, err = os.ReadFile(path)
		if err != nil {
			panic(err)
		}
	}
	store, err := blobs.NewBytesMap(payload)
	if err != nil {
		panic(err)
	}
	hash := blobs.NewHash(payload)

	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithALPNs(blobs.ALPN),
	)
	if err != nil {
		panic(err)
	}
	defer server.Shutdown(ctx)

	serverErr := make(chan error, 1)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			serverErr <- err
			return
		}
		s, err := conn.AcceptStream(ctx)
		if err != nil {
			serverErr <- err
			return
		}
		serverErr <- blobs.ServeBlob(ctx, s, store.Store())
	}()

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		panic(err)
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	conn, err := client.Connect(ctx, addr, blobs.ALPN)
	if err != nil {
		panic(err)
	}
	defer conn.CloseWithError(0, "")

	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		panic(err)
	}
	got, err := blobs.GetBlobBytes(ctx, s, hash)
	if err != nil {
		panic(err)
	}
	if err := <-serverErr; err != nil {
		panic(err)
	}
	if !bytes.Equal(got, payload) {
		panic("blob mismatch")
	}

	fmt.Println("bytes:", len(got))
	fmt.Println("blake3:", hash.Short())
}
