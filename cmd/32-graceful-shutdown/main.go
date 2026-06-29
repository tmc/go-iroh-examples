package main

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const alpn = "go-iroh-examples/graceful-shutdown/1"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	server, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		return err
	}

	handler := newGracefulHandler()
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		alpn: handler,
	}, nil)
	if err != nil {
		return err
	}

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		return err
	}
	defer client.Shutdown(context.Background())

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		return err
	}
	defer conn.CloseWithError(0, "")

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(stream, "drain this request"); err != nil {
		return err
	}
	if err := stream.Close(); err != nil {
		return err
	}

	if err := handler.WaitStarted(ctx); err != nil {
		return err
	}
	fmt.Println("handler: request in flight")

	stop()
	<-ctx.Done()
	fmt.Println("signal: shutdown requested")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := router.Shutdown(shutdownCtx); err != nil {
		return err
	}
	fmt.Println("shutdown: router drained")
	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	fmt.Println("shutdown: endpoint closed")
	return nil
}

type gracefulHandler struct {
	started chan struct{}
	release chan struct{}
	done    chan struct{}
	once    sync.Once
}

func newGracefulHandler() *gracefulHandler {
	return &gracefulHandler{
		started: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (h *gracefulHandler) Accept(ctx context.Context, conn *iroh.Conn) error {
	defer close(h.done)

	s, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	if _, err := io.ReadAll(s); err != nil {
		return err
	}
	close(h.started)

	select {
	case <-h.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.Close()
}

func (h *gracefulHandler) Shutdown(ctx context.Context) {
	h.once.Do(func() {
		close(h.release)
	})
	select {
	case <-h.done:
	case <-ctx.Done():
	}
}

func (h *gracefulHandler) WaitStarted(ctx context.Context) error {
	select {
	case <-h.started:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
