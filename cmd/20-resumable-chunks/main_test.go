package main

import (
	"strings"
	"testing"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/blobs"
)

func TestRun(t *testing.T) {
	out, err := exampleutil.Capture(run)
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		// The whole blob, reassembled from two separately fetched ranges. run
		// returns an error if the pieces do not concatenate to the original, so
		// reaching this length is the assertion that resumption worked.
		"bytes: 3168",
		"blake3: 0c8d748520",
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
