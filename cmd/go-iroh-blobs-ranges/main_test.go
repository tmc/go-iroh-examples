package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/iroh"
)

func TestRun(t *testing.T) {
	var buf bytes.Buffer
	err := run(&buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		// The whole blob, reassembled from two separately fetched ranges. run
		// returns an error if the pieces do not concatenate to the original, so
		// reaching this length is the assertion that resumption worked.
		"bytes: 67584",
		"ranges: prefix + resumed suffix",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}

func TestChunkCount(t *testing.T) {
	for _, tt := range []struct {
		name string
		size uint64
		want uint64
	}{
		{"empty", 0, 0},
		{"partial chunk", 1, 1},
		{"exact chunk", blobs.ChunkSize, 1},
		{"one byte over", blobs.ChunkSize + 1, 2},
		{"example payload", 3168, 4},
	} {
		if got := chunkCount(tt.size); got != tt.want {
			t.Errorf("chunkCount(%d) = %d, want %d (%s)", tt.size, got, tt.want, tt.name)
		}
	}
}

// TestRangePastFirstBlock checks that a range can begin at a later verification
// block and still verify against the original blob hash.
func TestRangePastFirstBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const blockChunks = 16 * 1024 / blobs.ChunkSize
	payload := make([]byte, 64*1024)
	for i := range payload {
		payload[i] = byte(i*7 + i/251)
	}
	store, err := blobs.NewMemStore(payload)
	if err != nil {
		t.Fatal(err)
	}
	hash := blobs.NewHash(payload)
	size := uint64(len(payload))

	server, err := bind(ctx, iroh.WithALPNs(blobs.ALPN))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(ctx)
	go serveBlobs(ctx, server, store, make(chan error, 1))

	client, err := bind(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Shutdown(ctx)
	addr := server.Addr()

	got, err := getRange(ctx, client, addr, hash, blobs.RangeChunks(0, blockChunks), size)
	if err != nil {
		t.Fatalf("range inside the first block: %v", err)
	}
	if !bytes.Equal(got, payload[:16*1024]) {
		t.Error("range inside the first block returned the wrong bytes")
	}

	got, err = getRange(ctx, client, addr, hash, blobs.RangeChunks(blockChunks, 2*blockChunks), size)
	if err != nil {
		t.Fatalf("range beginning at the second block: %v", err)
	}
	if !bytes.Equal(got, payload[16*1024:32*1024]) {
		t.Error("range beginning at the second block returned the wrong bytes")
	}
}
