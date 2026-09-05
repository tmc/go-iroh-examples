// Command go-iroh-graceful-shutdown drains in-flight work before closing an endpoint.
//
// Closing an endpoint while a handler is still running loses whatever that
// handler had not finished. The ordered teardown is: stop accepting, let the
// handlers that are already running reach a stopping point, then close the
// endpoint. [iroh.Router.Shutdown] does the first two, and a handler that
// implements Shutdown(ctx) is told when to stop taking new work — go-iroh calls
// it as part of the router's drain.
//
// Everything else in this repository closes with a deferred Shutdown, which is
// correct for a program that is about to exit anyway. This example is what a
// server does instead: a signal arrives, the request in flight completes, and
// only then does the endpoint go away.
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

	server, err := bind(ctx)
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

	client, err := bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(context.Background())

	conn, err := client.Connect(ctx, server.Addr(), alpn)
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
	if err := stream.CloseWrite(); err != nil {
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

// bind binds an endpoint to an ephemeral IPv6 loopback port, then applies opts.
// Loopback binding keeps the example self-contained: no relay, no DNS, no
// network access.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
}
