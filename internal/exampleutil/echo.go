package exampleutil

import (
	"context"
	"io"
	"strings"

	"github.com/tmc/go-iroh/iroh"
)

// EchoOnce accepts one bidirectional stream, reads it to EOF, writes the same
// bytes back, and closes the stream.
func EchoOnce(ctx context.Context, conn *iroh.Conn) error {
	s, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	b, err := io.ReadAll(s)
	if err != nil {
		return err
	}
	if _, err := s.Write(b); err != nil {
		return err
	}
	return s.Close()
}

// EchoHandler dispatches one stream echo for iroh.Router examples.
type EchoHandler struct{}

func (EchoHandler) Accept(ctx context.Context, conn *iroh.Conn) error {
	return EchoOnce(ctx, conn)
}

// UpperHandler dispatches one stream that replies with the upper-case request.
type UpperHandler struct{}

func (UpperHandler) Accept(ctx context.Context, conn *iroh.Conn) error {
	return TransformOnce(ctx, conn, strings.ToUpper)
}

// TransformOnce accepts one bidirectional stream, transforms the request, and
// writes the response.
func TransformOnce(ctx context.Context, conn *iroh.Conn, f func(string) string) error {
	s, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	b, err := io.ReadAll(s)
	if err != nil {
		return err
	}
	if _, err := s.Write([]byte(f(string(b)))); err != nil {
		return err
	}
	return s.Close()
}

// Exchange writes msg on a new stream and returns the peer's reply.
func Exchange(ctx context.Context, conn *iroh.Conn, msg string) (string, error) {
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
