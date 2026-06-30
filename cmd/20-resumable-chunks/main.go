package main

import (
	"bytes"
	"context"
	"fmt"
	"net/netip"
	"time"

	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	payload := bytes.Repeat([]byte("verified resumable blob transfer\n"), 96)
	store, err := blobs.NewBytesMap(payload)
	if err != nil {
		panic(err)
	}
	hash := blobs.NewHash(payload)
	size := uint64(len(payload))

	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithALPNs(blobs.ALPN),
	)
	if err != nil {
		panic(err)
	}
	defer server.Shutdown(ctx)

	serverErr := make(chan error, 4)
	go serveBlobs(ctx, server, store.Store(), serverErr)

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		panic(err)
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	prefix, err := getRange(ctx, client, addr, hash, blobs.RangeChunks(0, 2), size)
	if err != nil {
		select {
		case serverErr := <-serverErr:
			panic(serverErr)
		default:
		}
		panic(err)
	}
	suffix, err := getRange(ctx, client, addr, hash, blobs.RangeChunks(2, chunkCount(size)), size)
	if err != nil {
		select {
		case serverErr := <-serverErr:
			panic(serverErr)
		default:
		}
		panic(err)
	}
	got := append(prefix, suffix...)
	if !bytes.Equal(got, payload) {
		panic("resumed blob mismatch")
	}

	fmt.Println("bytes:", len(got))
	fmt.Println("blake3:", hash.Short())
	fmt.Println("ranges: prefix + resumed suffix")
}

func serveBlobs(ctx context.Context, ep *iroh.Endpoint, store blobs.Store, errc chan<- error) {
	for {
		conn, err := ep.Accept(ctx)
		if err != nil {
			return
		}
		go func() {
			stream, err := conn.AcceptStream(ctx)
			if err != nil {
				conn.Close()
				return
			}
			if err := blobs.ServeBlob(ctx, stream, store); err != nil {
				select {
				case errc <- err:
				default:
				}
			}
		}()
	}
}

func getRange(ctx context.Context, ep *iroh.Endpoint, addr netaddr.EndpointAddr, hash blobs.Hash, ranges blobs.ChunkRanges, size uint64) ([]byte, error) {
	conn, err := ep.Connect(ctx, addr, blobs.ALPN)
	if err != nil {
		return nil, fmt.Errorf("connect provider: %w", err)
	}
	defer conn.CloseWithError(0, "")
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, fmt.Errorf("open blob stream: %w", err)
	}
	data, err := blobs.GetBlobRangeBytes(ctx, stream, hash, ranges, size)
	if err != nil {
		return nil, fmt.Errorf("get range: %w", err)
	}
	return data, nil
}

func chunkCount(size uint64) uint64 {
	return (size + blobs.ChunkSize - 1) / blobs.ChunkSize
}
