// Command go-iroh-blobs-ranges fetches a blob in two chunk ranges.
//
// A transfer that fails halfway should not have to start over. An iroh-blobs
// request carries the range of the blob it wants, so a receiver that already
// holds the first part asks only for the rest: [blobs.GetBlobRangeBytes] takes
// a [blobs.ChunkRanges] and the verified blob size and returns those bytes.
//
// The ranges are chunk-aligned because the verification is. Content is hashed
// as a BLAKE3 tree over [blobs.ChunkSize] blocks, and the provider sends the
// tree nodes covering the requested range next to the data, so each range is
// checked against the same root hash on its own. Nothing has to be trusted or
// carried over from the session that fetched the earlier part.
//
// This example asks for chunks [0,2) on one connection and the remainder on a
// second connection, which is the shape of a resumed download: all the second
// fetch has is the hash, the blob size, and how far the first one got. Both
// pieces are then concatenated and compared with the original.
//
// Compare go-iroh-blobs-transfer, which requests the whole blob over the same protocol
// from the same kind of store. Read the two together: the difference between a
// download and a resumed download is the range in the request.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	payload := bytes.Repeat([]byte("verified resumable blob transfer\n"), 96)
	store, err := blobs.NewMemStore(payload)
	if err != nil {
		return err
	}
	// A resuming receiver has these two out of band, or from the response that
	// was interrupted: the hash names the content, the size bounds the ranges.
	hash := blobs.NewHash(payload)
	size := uint64(len(payload))

	server, err := exampleutil.Bind(ctx, iroh.WithALPNs(blobs.ALPN))
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	serverErr := make(chan error, 1)
	go serveBlobs(ctx, server, store, serverErr)

	client, err := exampleutil.Bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	addr := exampleutil.Addr(server)
	prefix, err := getRange(ctx, client, addr, hash, blobs.RangeChunks(0, 2), size)
	if err != nil {
		return preferServerErr(serverErr, err)
	}
	// The second connection is a new one on purpose: nothing but hash, size,
	// and offset carries over from the first.
	suffix, err := getRange(ctx, client, addr, hash, blobs.RangeChunks(2, chunkCount(size)), size)
	if err != nil {
		return preferServerErr(serverErr, err)
	}
	got := append(prefix, suffix...)
	if !bytes.Equal(got, payload) {
		return errors.New("resumed blob does not match the served payload")
	}

	fmt.Println("bytes:", len(got))
	fmt.Println("blake3:", hash.Short())
	fmt.Println("ranges: prefix + resumed suffix")
	return nil
}

// serveBlobs answers blob requests until ctx ends or the endpoint stops
// accepting. It reports the first serving failure on errc, which the client
// side uses to explain its own error.
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

// getRange fetches one chunk range of hash over its own connection.
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

// preferServerErr returns the provider's error if it has one, since it usually
// says more about a failed fetch than the receiver's end of it does.
func preferServerErr(serverErr <-chan error, err error) error {
	select {
	case serveErr := <-serverErr:
		return fmt.Errorf("serve blob: %w", serveErr)
	default:
		return err
	}
}

// chunkCount is the number of BLAKE3 chunks a blob of size bytes occupies.
func chunkCount(size uint64) uint64 {
	return (size + blobs.ChunkSize - 1) / blobs.ChunkSize
}
