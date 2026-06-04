package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/netip"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const alpn = "go-iroh-examples/callme-frames/1"

type mediaFrame struct {
	Kind byte
	Seq  uint32
	Data []byte
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithALPNs(alpn),
	)
	if err != nil {
		panic(err)
	}
	defer server.Shutdown(ctx)

	received := make(chan mediaFrame, 4)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			close(received)
			return
		}
		for range 4 {
			b, err := conn.ReadDatagram(ctx)
			if err != nil {
				close(received)
				return
			}
			frame, err := decodeFrame(b)
			if err != nil {
				close(received)
				return
			}
			received <- frame
		}
		close(received)
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

	frames := []mediaFrame{
		{Kind: 'a', Seq: 1, Data: []byte("audio opus packet 1")},
		{Kind: 'v', Seq: 1, Data: []byte("video keyframe")},
		{Kind: 'a', Seq: 2, Data: []byte("audio opus packet 2")},
		{Kind: 'v', Seq: 2, Data: []byte("video delta frame")},
	}
	for _, frame := range frames {
		if err := conn.SendDatagram(encodeFrame(frame)); err != nil {
			panic(err)
		}
	}

	for frame := range received {
		fmt.Printf("%c%d %s\n", frame.Kind, frame.Seq, frame.Data)
	}
}

func encodeFrame(frame mediaFrame) []byte {
	b := make([]byte, 5+len(frame.Data))
	b[0] = frame.Kind
	binary.BigEndian.PutUint32(b[1:5], frame.Seq)
	copy(b[5:], frame.Data)
	return b
}

func decodeFrame(b []byte) (mediaFrame, error) {
	if len(b) < 5 {
		return mediaFrame{}, fmt.Errorf("short frame: %d bytes", len(b))
	}
	return mediaFrame{
		Kind: b[0],
		Seq:  binary.BigEndian.Uint32(b[1:5]),
		Data: b[5:],
	}, nil
}
