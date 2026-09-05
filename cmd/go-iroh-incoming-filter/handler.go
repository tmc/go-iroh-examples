package main

import (
	"context"
	"fmt"

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
	return echo(ctx, conn)
}
