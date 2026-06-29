package main

import (
	"context"
	"fmt"
	"io"

	"github.com/tmc/go-iroh/iroh"
)

// loggingEchoHandler echoes a single message. It implements
// [iroh.AcceptingHandler] so it can observe the negotiated ALPN and remote
// address before the connection is converted to an [iroh.Conn]. Implementing
// OnAccepting is optional; a handler that does not need to intercept can omit it
// and the router uses [iroh.Accepting.Connection] by default.
type loggingEchoHandler struct{}

// OnAccepting logs the accepted connection, then completes the handshake by
// returning the verified connection. Returning an error here refuses the
// connection without invoking Accept.
func (loggingEchoHandler) OnAccepting(ctx context.Context, accepting *iroh.Accepting) (*iroh.Conn, error) {
	alpn, err := accepting.ALPN(ctx)
	if err != nil {
		return nil, err
	}
	fmt.Printf("on-accepting: alpn=%q remote=%s\n", alpn, accepting.RemoteAddr())
	return accepting.Connection(ctx)
}

// Accept handles the verified connection by echoing one message.
func (loggingEchoHandler) Accept(ctx context.Context, conn *iroh.Conn) error {
	return echoOnce(ctx, conn)
}

// echoOnce accepts one stream, reads it to EOF, and writes the bytes back.
func echoOnce(ctx context.Context, conn *iroh.Conn) error {
	s, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	defer s.Close()
	b, err := io.ReadAll(s)
	if err != nil {
		return err
	}
	if _, err := s.Write(b); err != nil {
		return err
	}
	return s.Close()
}

// exchange opens a stream, sends msg, and returns the echoed reply.
func exchange(ctx context.Context, conn *iroh.Conn, msg string) (string, error) {
	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return "", err
	}
	if _, err := s.Write([]byte(msg)); err != nil {
		return "", err
	}
	if err := s.Close(); err != nil {
		return "", err
	}
	b, err := io.ReadAll(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
