package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"net/netip"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const alpn = "go-iroh-examples/resumable-chunks/1"

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	payload := []byte("chunk zero\nchunk one\nchunk two\nchunk three\n")
	chunks := split(payload, 12)
	want := make([][32]byte, len(chunks))
	for i, chunk := range chunks {
		want[i] = sha256.Sum256(chunk)
	}

	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithALPNs(alpn),
	)
	if err != nil {
		panic(err)
	}
	defer server.Shutdown(ctx)

	received := make(chan []byte, 1)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			return
		}
		buf := make([][]byte, len(chunks))
		for got := 0; got < len(chunks); {
			stream, err := conn.AcceptStream(ctx)
			if err != nil {
				return
			}
			index, data, err := readChunk(stream)
			_ = stream.Close()
			if err != nil || index >= len(buf) {
				return
			}
			if sha256.Sum256(data) != want[index] {
				return
			}
			if buf[index] == nil {
				got++
			}
			buf[index] = data
		}
		received <- bytes.Join(buf, nil)
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

	for _, index := range []int{0, 2, 1, 2, 3} {
		if err := sendChunk(ctx, conn, uint32(index), chunks[index]); err != nil {
			panic(err)
		}
	}

	got := <-received
	fmt.Println("bytes:", len(got))
	fmt.Println("sha256:", fmt.Sprintf("%x", sha256.Sum256(got))[:16])
}

func split(b []byte, size int) [][]byte {
	var chunks [][]byte
	for len(b) > 0 {
		n := min(size, len(b))
		chunks = append(chunks, b[:n])
		b = b[n:]
	}
	return chunks
}

func sendChunk(ctx context.Context, conn *iroh.Conn, index uint32, data []byte) error {
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[:4], index)
	binary.BigEndian.PutUint32(hdr[4:], uint32(len(data)))
	if _, err := stream.Write(hdr[:]); err != nil {
		stream.Close()
		return err
	}
	if _, err := stream.Write(data); err != nil {
		stream.Close()
		return err
	}
	return stream.Close()
}

func readChunk(r io.Reader) (int, []byte, error) {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	index := binary.BigEndian.Uint32(hdr[:4])
	size := binary.BigEndian.Uint32(hdr[4:])
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return 0, nil, err
	}
	return int(index), data, nil
}
