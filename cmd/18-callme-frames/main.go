// Command 18-callme-frames sends application frames as QUIC datagrams.
//
// Datagrams are the transport for data that is worthless once it is late: an
// audio packet, a video frame, a cursor position. They are not retransmitted
// and not ordered, so an application that sends more than one kind of thing has
// to put its own header on the wire — here a one-byte kind and a four-byte
// sequence number, which is enough to tell an audio frame from a video frame and
// to notice a gap.
//
// The framing is this example's own, not a protocol anyone else speaks. What is
// worth copying is the shape: fixed-size header, payload, and a decoder that
// rejects anything too short, because a datagram arrives whole or not at all and
// there is no stream to resynchronize against.
//
// See 39-datagram-vs-stream for choosing between the two, and 34-uni-streams for
// the ordered, reliable alternative.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
)

const alpn = "go-iroh-examples/datagram-frames/1"

// frame is the application's own datagram format: kind, sequence, payload.
type frame struct {
	Kind byte
	Seq  uint32
	Data []byte
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server, err := exampleutil.Bind(ctx, iroh.WithALPNs(alpn))
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	frames := []frame{
		{Kind: 'a', Seq: 1, Data: []byte("audio opus packet 1")},
		{Kind: 'v', Seq: 1, Data: []byte("video keyframe")},
		{Kind: 'a', Seq: 2, Data: []byte("audio opus packet 2")},
		{Kind: 'v', Seq: 2, Data: []byte("video delta frame")},
	}

	received := make(chan frame, len(frames))
	errs := make(chan error, 1)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			errs <- err
			return
		}
		for range len(frames) {
			b, err := conn.ReadDatagram(ctx)
			if err != nil {
				errs <- err
				return
			}
			f, err := decodeFrame(b)
			if err != nil {
				errs <- err
				return
			}
			received <- f
		}
		close(received)
	}()

	client, err := exampleutil.Bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, exampleutil.Addr(server), alpn)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Every frame has to fit in one datagram; there is no fragmentation.
	n, ok := conn.MaxDatagramSize()
	if !ok {
		return fmt.Errorf("peer did not negotiate datagram support")
	}
	for _, f := range frames {
		b := encodeFrame(f)
		if len(b) > n {
			return fmt.Errorf("frame %c%d is %d bytes, over the %d-byte datagram limit", f.Kind, f.Seq, len(b), n)
		}
		if err := conn.SendDatagram(b); err != nil {
			return err
		}
	}

	for {
		select {
		case f, ok := <-received:
			if !ok {
				return nil
			}
			fmt.Printf("%c%d %s\n", f.Kind, f.Seq, f.Data)
		case err := <-errs:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func encodeFrame(f frame) []byte {
	b := make([]byte, 5+len(f.Data))
	b[0] = f.Kind
	binary.BigEndian.PutUint32(b[1:5], f.Seq)
	copy(b[5:], f.Data)
	return b
}

func decodeFrame(b []byte) (frame, error) {
	if len(b) < 5 {
		return frame{}, fmt.Errorf("short frame: %d bytes", len(b))
	}
	return frame{
		Kind: b[0],
		Seq:  binary.BigEndian.Uint32(b[1:5]),
		Data: b[5:],
	}, nil
}
