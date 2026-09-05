// Command go-iroh-framed-messages exchanges chess moves as length-prefixed frames.
//
// This is the Go port of the framed-messages example from n0's iroh-examples,
// and it answers the question a reader has after go-iroh-ping: how do two peers
// send more than one message on a stream? A QUIC stream is a stream of bytes.
// It preserves order and delivers everything written to it, but it carries no
// message boundaries, so a reader may see one move split across two reads or
// two moves in one. 40 avoids the problem by closing its write side after a
// single message; a stream that stays open for a conversation cannot.
//
// The framing is the conventional one: a four-byte big-endian length, then that
// many bytes of payload. The length arrives from the peer, so [readFrame]
// checks it against maxMessageSize before allocating, and [writeFrame] refuses
// to write a payload the peer would reject. Both sides then read whole
// messages, never partial ones.
//
// ALPN "iroh/examples/messages/0" and the frame layout are the Rust example's.
// A move is four bytes — two squares — and the two halves alternate on one
// stream that stays open for the whole exchange.
//
// go-iroh-uni-streams takes the other approach: one stream per message, with the
// stream's own close acting as the frame. That needs no length prefix, but it
// costs a stream per message and leaves messages unordered relative to each
// other. Framing one long-lived stream keeps a single order.
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/iroh"
)

const (
	alpn = "iroh/examples/messages/0"
	// maxMessageSize bounds a frame in both directions. It is the only thing
	// standing between a peer-supplied length and an allocation.
	maxMessageSize  = 1000
	moveMessageSize = 4
)

// move is one chess move: the square a piece left and the square it reached.
type move struct {
	From file
	To   file
}

// file is a board square, numbered from one.
type file struct {
	X byte
	Y byte
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

	server, err := bind(ctx)
	if err != nil {
		return err
	}
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		alpn: chessHandler{},
	}, nil)
	if err != nil {
		return err
	}
	defer router.Shutdown(ctx)

	client, err := bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, server.Addr(), alpn)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", server.ID().Short(), err)
	}
	defer conn.CloseWithError(0, "bye")

	// One stream carries the whole game, so every message needs its own frame.
	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	defer s.Close()

	if err := sendMove(s, move{From: file{4, 2}, To: file{4, 4}}); err != nil {
		return err
	}
	mv, err := recvMove(s)
	if err != nil {
		return err
	}
	fmt.Printf("received move: %+v\n", mv)

	if err := sendMove(s, move{From: file{3, 2}, To: file{3, 3}}); err != nil {
		return err
	}
	mv, err = recvMove(s)
	if err != nil {
		return err
	}
	fmt.Printf("received move: %+v\n", mv)
	return nil
}

// chessHandler plays the black side of a two-move opening.
type chessHandler struct{}

// Accept implements [iroh.ProtocolHandler].
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

// sendMove writes mv as one frame.
func sendMove(w io.Writer, mv move) error {
	return writeFrame(w, []byte{mv.From.X, mv.From.Y, mv.To.X, mv.To.Y})
}

// recvMove reads one frame and decodes it as a move.
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

// writeFrame writes payload prefixed by its length.
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

// readFrame reads one length-prefixed frame. The length is checked before the
// payload is allocated, because it comes from the peer.
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

// bind binds an endpoint to an ephemeral IPv6 loopback port, then applies opts.
// Loopback binding keeps the example self-contained: no relay, no DNS, no
// network access.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
}
