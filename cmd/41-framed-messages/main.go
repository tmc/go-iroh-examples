package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const (
	alpn            = "iroh/examples/messages/0"
	maxMessageSize  = 1000
	moveMessageSize = 4
)

type move struct {
	From file
	To   file
}

type file struct {
	X byte
	Y byte
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		panic(err)
	}
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		alpn: chessHandler{},
	}, nil)
	if err != nil {
		panic(err)
	}
	defer router.Shutdown(ctx)

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
	defer conn.CloseWithError(0, "bye")

	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		panic(err)
	}
	defer s.Close()

	if err := sendMove(s, move{From: file{4, 2}, To: file{4, 4}}); err != nil {
		panic(err)
	}
	mv, err := recvMove(s)
	if err != nil {
		panic(err)
	}
	fmt.Printf("received move: %+v\n", mv)

	if err := sendMove(s, move{From: file{3, 2}, To: file{3, 3}}); err != nil {
		panic(err)
	}
	mv, err = recvMove(s)
	if err != nil {
		panic(err)
	}
	fmt.Printf("received move: %+v\n", mv)
}

type chessHandler struct{}

func (chessHandler) Accept(ctx context.Context, conn *iroh.Conn) error {
	s, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	defer s.Close()

	mv, err := recvMove(s)
	if err != nil {
		return err
	}
	fmt.Printf("got move: %+v\n", mv)
	if err := sendMove(s, move{From: file{5, 7}, To: file{5, 6}}); err != nil {
		return err
	}

	mv, err = recvMove(s)
	if err != nil {
		return err
	}
	fmt.Printf("got move: %+v\n", mv)
	return sendMove(s, move{From: file{5, 8}, To: file{5, 7}})
}

func sendMove(w io.Writer, mv move) error {
	return writeFrame(w, []byte{mv.From.X, mv.From.Y, mv.To.X, mv.To.Y})
}

func recvMove(r io.Reader) (move, error) {
	b, err := readFrame(r)
	if err != nil {
		return move{}, err
	}
	if len(b) != moveMessageSize {
		return move{}, fmt.Errorf("move: frame length %d", len(b))
	}
	return move{
		From: file{X: b[0], Y: b[1]},
		To:   file{X: b[2], Y: b[3]},
	}, nil
}

func writeFrame(w io.Writer, payload []byte) error {
	if len(payload) > maxMessageSize {
		return fmt.Errorf("frame too large: %d > %d", len(payload), maxMessageSize)
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > maxMessageSize {
		return nil, fmt.Errorf("frame too large: %d > %d", n, maxMessageSize)
	}
	if n == 0 {
		return nil, errors.New("empty frame")
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}
