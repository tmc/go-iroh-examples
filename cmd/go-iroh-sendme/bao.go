package main

// This file answers a range request the way iroh-blobs does, which
// blobs.ExtractBlobRange in go-iroh v0.2.1 does not for every range.
//
// A blob's hash is the root of a BLAKE3 tree over 1 KiB chunks, and a response
// to a request for some chunks is a walk of that tree in pre-order: the pair
// of child hashes at every parent above the requested chunks, then the chunks.
// iroh-blobs stores the parents only down to 16-chunk blocks and sends a fully
// requested block as bare data. A partly requested block it opens up,
// computing the parents inside it from the data and sending only the
// requested chunks. ExtractBlobRange sends the whole block instead, which the
// Rust receiver rejects, and sendme's first request, for the last chunk of
// every file, is a partly requested block whenever a file's last block holds
// more than one chunk.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"

	"github.com/tmc/go-iroh/blobs"
)

const blockChunks = blobs.BlockSize / blobs.ChunkSize

// encodeRanges writes the response to a request for ranges of the blob with
// hash h: its size, then the parents and chunks the ranges select. data holds
// the blob's size bytes; outboard holds the parents above block level, in
// pre-order after an 8-byte size, as blobs.FSStore writes them.
func encodeRanges(w io.Writer, h blobs.Hash, size uint64, data, outboard io.ReaderAt, ranges blobs.ChunkRanges) error {
	chunks := (size + blobs.ChunkSize - 1) / blobs.ChunkSize
	e := &encoder{
		w:        bufio.NewWriter(w),
		data:     data,
		outboard: outboard,
		size:     size,
		sel:      newSelection(ranges, chunks),
	}
	binary.Write(e.w, binary.LittleEndian, size)
	if chunks > 0 {
		if err := e.subtree(0, chunks, true, 8, h); err != nil {
			return err
		}
	}
	return e.w.Flush()
}

type encoder struct {
	w        *bufio.Writer
	data     io.ReaderAt
	outboard io.ReaderAt
	size     uint64
	sel      selection
}

// subtree writes the part of the response for the n chunks starting at start,
// whose hash is want and whose parents begin at offset off in the outboard.
func (e *encoder) subtree(start, n uint64, root bool, off int64, want blobs.Hash) error {
	selected := e.sel.count(start, start+n)
	if selected == 0 {
		return nil
	}
	if n <= blockChunks {
		lo := start * blobs.ChunkSize
		buf := make([]byte, min(n*blobs.ChunkSize, e.size-lo))
		if _, err := e.data.ReadAt(buf, int64(lo)); err != nil {
			return fmt.Errorf("read block at chunk %d: %w", start, err)
		}
		if selected == n {
			_, err := e.w.Write(buf)
			return err
		}
		var out []byte
		if got := e.sel.encodeBlock(&out, start, buf, root); got != want {
			return fmt.Errorf("block at chunk %d does not match its hash", start)
		}
		_, err := e.w.Write(out)
		return err
	}
	var pair [64]byte
	if _, err := e.outboard.ReadAt(pair[:], off); err != nil {
		return fmt.Errorf("read outboard: %w", err)
	}
	e.w.Write(pair[:])
	left := leftChunks(n)
	leftParents := int64((left+blockChunks-1)/blockChunks - 1)
	if err := e.subtree(start, left, false, off+64, blobs.Hash(pair[:32])); err != nil {
		return err
	}
	return e.subtree(start+left, n-left, false, off+64+64*leftParents, blobs.Hash(pair[32:]))
}

// encodeBlock appends to out the response for the selected chunks of one
// block, whose first chunk is start, and returns the hash of the block. A
// parent inside the block is sent only if its subtree is partly selected.
func (sel selection) encodeBlock(out *[]byte, start uint64, data []byte, root bool) blobs.Hash {
	if len(data) <= blobs.ChunkSize {
		if sel.count(start, start+1) > 0 {
			*out = append(*out, data...)
		}
		return chunkHash(data, start, root)
	}
	n := (uint64(len(data)) + blobs.ChunkSize - 1) / blobs.ChunkSize
	selected := sel.count(start, start+n)
	at := -1
	if selected > 0 && selected < n {
		at = len(*out)
		*out = append(*out, make([]byte, 64)...)
	}
	left := leftChunks(n)
	l := sel.encodeBlock(out, start, data[:left*blobs.ChunkSize], false)
	r := sel.encodeBlock(out, start+left, data[left*blobs.ChunkSize:], false)
	if at >= 0 {
		copy((*out)[at:], l[:])
		copy((*out)[at+32:], r[:])
	}
	return parentHash(l, r, root)
}

// leftChunks returns the number of chunks in the left subtree of a tree of
// n > 1 chunks: the largest power of two less than n.
func leftChunks(n uint64) uint64 {
	return 1 << (bits.Len64(n-1) - 1)
}

// A selection is the set of chunks a request selects from a blob, as sorted
// half-open ranges.
type selection [][2]uint64

// newSelection returns the chunks of a blob of the given number of chunks that
// r selects. As in iroh-blobs, a range that reaches or passes the last chunk
// selects the last chunk, so that "the chunk at infinity" is a request for a
// proof of the blob's size.
func newSelection(r blobs.ChunkRanges, chunks uint64) selection {
	var in selection
	for _, c := range r.Ranges() {
		in = append(in, [2]uint64{c.Start, c.End})
	}
	if s, ok := r.OpenStart(); ok {
		in = append(in, [2]uint64{s, math.MaxUint64})
	}
	last := max(chunks, 1) - 1
	var sel selection
	for _, c := range in {
		if c[1] > last {
			sel = append(sel, [2]uint64{min(c[0], last), math.MaxUint64})
			break
		}
		sel = append(sel, c)
	}
	return sel
}

// count returns how many chunks in [lo, hi) sel selects.
func (sel selection) count(lo, hi uint64) uint64 {
	var n uint64
	for _, c := range sel {
		if a, b := max(c[0], lo), min(c[1], hi); a < b {
			n += b - a
		}
	}
	return n
}

// BLAKE3, as far as a tree of chunks needs it: the chaining value of a chunk
// at a given position and of a parent, either of which may be the root.

const (
	flagChunkStart = 1 << 0
	flagChunkEnd   = 1 << 1
	flagParent     = 1 << 2
	flagRoot       = 1 << 3
)

var blake3IV = [8]uint32{
	0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A,
	0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19,
}

var blake3Permutation = [16]int{2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8}

func chunkHash(data []byte, counter uint64, root bool) blobs.Hash {
	cv := blake3IV
	for i := 0; i == 0 || i < len(data); i += 64 {
		block := data[i:min(i+64, len(data))]
		var flags uint32
		if i == 0 {
			flags |= flagChunkStart
		}
		if i+64 >= len(data) {
			flags |= flagChunkEnd
			if root {
				flags |= flagRoot
			}
		}
		var buf [64]byte
		copy(buf[:], block)
		cv = compress(cv, buf, counter, uint32(len(block)), flags)
	}
	return cvBytes(cv)
}

func parentHash(l, r blobs.Hash, root bool) blobs.Hash {
	var buf [64]byte
	copy(buf[:], l[:])
	copy(buf[32:], r[:])
	flags := uint32(flagParent)
	if root {
		flags |= flagRoot
	}
	return cvBytes(compress(blake3IV, buf, 0, 64, flags))
}

func cvBytes(cv [8]uint32) blobs.Hash {
	var h blobs.Hash
	for i, w := range cv {
		binary.LittleEndian.PutUint32(h[4*i:], w)
	}
	return h
}

func compress(cv [8]uint32, block [64]byte, counter uint64, blockLen, flags uint32) [8]uint32 {
	var m [16]uint32
	for i := range m {
		m[i] = binary.LittleEndian.Uint32(block[4*i:])
	}
	s := [16]uint32{
		cv[0], cv[1], cv[2], cv[3], cv[4], cv[5], cv[6], cv[7],
		blake3IV[0], blake3IV[1], blake3IV[2], blake3IV[3],
		uint32(counter), uint32(counter >> 32), blockLen, flags,
	}
	for round := range 7 {
		g(&s, 0, 4, 8, 12, m[0], m[1])
		g(&s, 1, 5, 9, 13, m[2], m[3])
		g(&s, 2, 6, 10, 14, m[4], m[5])
		g(&s, 3, 7, 11, 15, m[6], m[7])
		g(&s, 0, 5, 10, 15, m[8], m[9])
		g(&s, 1, 6, 11, 12, m[10], m[11])
		g(&s, 2, 7, 8, 13, m[12], m[13])
		g(&s, 3, 4, 9, 14, m[14], m[15])
		if round < 6 {
			var p [16]uint32
			for i, j := range blake3Permutation {
				p[i] = m[j]
			}
			m = p
		}
	}
	var out [8]uint32
	for i := range out {
		out[i] = s[i] ^ s[i+8]
	}
	return out
}

func g(s *[16]uint32, a, b, c, d int, mx, my uint32) {
	s[a] += s[b] + mx
	s[d] = bits.RotateLeft32(s[d]^s[a], -16)
	s[c] += s[d]
	s[b] = bits.RotateLeft32(s[b]^s[c], -12)
	s[a] += s[b] + my
	s[d] = bits.RotateLeft32(s[d]^s[a], -8)
	s[c] += s[d]
	s[b] = bits.RotateLeft32(s[b]^s[c], -7)
}
