package main

import (
	"context"
	"io"
	"strings"

	"github.com/tmc/go-iroh/iroh"
)

func echoOnce(ctx context.Context, conn *iroh.Conn) error {
	return transformOnce(ctx, conn, func(s string) string { return s })
}

func transformOnce(ctx context.Context, conn *iroh.Conn, f func(string) string) error {
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

type echoHandler struct{}

func (echoHandler) Accept(ctx context.Context, conn *iroh.Conn) error {
	return echoOnce(ctx, conn)
}

type upperHandler struct{}

func (upperHandler) Accept(ctx context.Context, conn *iroh.Conn) error {
	return transformOnce(ctx, conn, strings.ToUpper)
}
