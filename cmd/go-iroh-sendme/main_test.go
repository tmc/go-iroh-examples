package main

import (
	"bytes"
	"context"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/tmc/go-iroh/blobs"
)

// TestRunDemo sends a directory and receives it over loopback. The collection
// hash is fixed by the three files and their names, so it is the evidence that
// the collection was built the way sendme builds one.
func TestRunDemo(t *testing.T) {
	var buf bytes.Buffer
	err := run(nil, &buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		"imported directory photos, 61 B, hash 5f4f51f62ddbf573262350413348c4c905d5d10e5f87c72771c937948bcd569e",
		"sendme receive blob",
		"getting collection 5f4f51f62ddbf573262350413348c4c905d5d10e5f87c72771c937948bcd569e 3 files",
		"exporting to photos",
		`received photos/a.txt: "the first file\n"`,
		`received photos/trip/b.txt: "a file in a subdirectory\n"`,
		`received photos/trip/c/d.txt: "and one further down\n"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}

// TestRunUsage checks that an unusable command line is reported as such rather
// than acted on.
func TestRunUsage(t *testing.T) {
	for _, args := range [][]string{
		{"nonsense"},
		{"send"},                           // missing path
		{"receive", "one", "two"},          // too many arguments
		{"send", "-no-such-flag", "x"},     // unknown flag
		{"recv", "-format", "base32", "x"}, // unknown format
	} {
		if err := run(args, io.Discard); err != errUsage {
			t.Errorf("run(%q, io.Discard) = %v, want errUsage", args, err)
		}
	}
}

func TestParseTicketType(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want ticketType
	}{
		{"relay-and-addresses", relayAndAddresses},
		{"RelayAndAddresses", relayAndAddresses},
		{"id", idOnly},
		{"Id", idOnly},
		{"relay", relayOnly},
		{"addresses", addressesOnly},
	} {
		got, err := parseTicketType(tt.in)
		if err != nil || got != tt.want {
			t.Errorf("parseTicketType(%q) = %v, %v, want %v", tt.in, got, err, tt.want)
		}
	}
	if _, err := parseTicketType("everything"); err == nil {
		t.Error("parseTicketType accepted an unknown type")
	}
}

// TestExportPath checks that a collection name from the sender cannot place a
// file outside the directory it is received into.
func TestExportPath(t *testing.T) {
	for _, tt := range []struct {
		name string
		ok   bool
	}{
		{"a.txt", true},
		{"photos/trip/b.jpg", true},
		{"../escape", false},
		{"photos/../../escape", false},
		{"/absolute", false},
		{"photos//b", false},
		{`photos\b`, false},
		{".", false},
	} {
		_, err := exportPath("/root", tt.name)
		if (err == nil) != tt.ok {
			t.Errorf("exportPath(%q) error = %v, want ok=%v", tt.name, err, tt.ok)
		}
	}
}

func TestHumanBytes(t *testing.T) {
	for _, tt := range []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{1023, "1023 B"},
		{1024, "1.00 KiB"},
		{1536, "1.50 KiB"},
		{5 << 20, "5.00 MiB"},
	} {
		if got := humanBytes(tt.n); got != tt.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

// TestEncodeRanges checks the range encoder against go-iroh: the tree hash
// against the blob's hash, and the encoding of ranges that start and end on
// block boundaries, where iroh-blobs and blobs.ExtractBlobRange agree, against
// ExtractBlobRange. Ranges inside a block are checked against the Rust
// receiver in interop_test.go.
func TestEncodeRanges(t *testing.T) {
	ctx := context.Background()
	store, err := blobs.NewFSStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{1, 1024, 1025, 16 << 10, 100_000, 300_000} {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i * 7)
		}
		h, err := store.Add(data)
		if err != nil {
			t.Fatal(err)
		}
		if got := selection(nil).encodeBlock(new([]byte), 0, data, true); got != h {
			t.Errorf("size %d: tree hash %s, want %s", size, got, h)
		}
		blob, err := store.Open(ctx, h)
		if err != nil {
			t.Fatal(err)
		}
		ob, err := blob.Outboard(ctx)
		if err != nil {
			t.Fatal(err)
		}
		chunks := uint64(size+blobs.ChunkSize-1) / blobs.ChunkSize
		for _, r := range []blobs.ChunkRanges{
			blobs.RangeAll(),
			blobs.RangeChunks(16, 48),
			blobs.RangeChunks(32, 1<<20),
			blobs.RangeLastChunk(),
		} {
			var got bytes.Buffer
			if err := encodeRanges(&got, h, uint64(size), bytes.NewReader(data), ob, r); err != nil {
				t.Errorf("size %d, %v: %v", size, r, err)
				continue
			}
			sel := newSelection(r, chunks)
			if len(sel) == 0 || sel[0][0]%16 != 0 || sel[0][1] != ^uint64(0) && sel[0][1]%16 != 0 {
				continue // not block-aligned for this size
			}
			lo := min(sel[0][0]*blobs.ChunkSize, uint64(size))
			hi := uint64(size)
			if sel[0][1] != ^uint64(0) {
				hi = min(sel[0][1]*blobs.ChunkSize, hi)
			}
			var want bytes.Buffer
			if err := blobs.ExtractBlobRange(&want, bytes.NewReader(data), io.NewSectionReader(ob, 0, ob.Size()), lo, hi-lo); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got.Bytes(), want.Bytes()) {
				t.Errorf("size %d, chunks %v: encoding differs from ExtractBlobRange (%d bytes, want %d)", size, sel, got.Len(), want.Len())
			}
		}
		ob.Close()
	}
}

// TestNewSelection checks the rule that a range reaching past the end of a
// blob selects its last chunk.
func TestNewSelection(t *testing.T) {
	const open = ^uint64(0)
	for _, tt := range []struct {
		name   string
		r      blobs.ChunkRanges
		chunks uint64
		want   selection
	}{
		{"all", blobs.RangeAll(), 10, selection{{0, open}}},
		{"last chunk", blobs.RangeLastChunk(), 10, selection{{9, open}}},
		{"inside", blobs.RangeChunks(2, 5), 10, selection{{2, 5}}},
		{"to the end", blobs.RangeChunks(2, 10), 10, selection{{2, open}}},
		{"past the end", blobs.RangeChunks(12, 20), 10, selection{{9, open}}},
		{"empty blob", blobs.RangeLastChunk(), 0, selection{{0, open}}},
	} {
		got := newSelection(tt.r, tt.chunks)
		if !slices.Equal(got, tt.want) {
			t.Errorf("%s: newSelection = %v, want %v", tt.name, got, tt.want)
		}
	}
}
