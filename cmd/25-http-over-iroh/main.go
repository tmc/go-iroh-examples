package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const alpn = "go-iroh-examples/http-over-iroh/1"

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

	listener := newStreamListener(net.UDPAddrFromAddrPort(server.LocalAddr()))
	defer listener.Close()

	httpServer := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			name := r.URL.Query().Get("name")
			if name == "" {
				name = "iroh"
			}
			fmt.Fprintf(w, "hello %s over %s\n", name, r.Proto)
		}),
	}
	defer httpServer.Shutdown(ctx)

	go acceptStreams(ctx, server, listener)
	go func() {
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		panic(err)
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				return client.Dial(ctx, addr, alpn)
			},
		},
		Timeout: 5 * time.Second,
	}

	resp, err := httpClient.Get("http://iroh.local/greet?name=net-http")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	fmt.Print(string(body))
}

func acceptStreams(ctx context.Context, endpoint *iroh.Endpoint, listener *streamListener) {
	conn, err := endpoint.Accept(ctx)
	if err != nil {
		listener.Close()
		return
	}
	defer conn.Close()

	for {
		stream, err := conn.AcceptStreamConn(ctx)
		if err != nil {
			listener.Close()
			return
		}
		if err := listener.push(ctx, stream); err != nil {
			stream.Close()
			return
		}
	}
}

type streamListener struct {
	addr   net.Addr
	conns  chan net.Conn
	done   chan struct{}
	closed bool
	mu     sync.Mutex
}

func newStreamListener(addr net.Addr) *streamListener {
	return &streamListener{
		addr:  addr,
		conns: make(chan net.Conn),
		done:  make(chan struct{}),
	}
}

func (l *streamListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *streamListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.closed = true
		close(l.done)
	}
	return nil
}

func (l *streamListener) Addr() net.Addr {
	return l.addr
}

func (l *streamListener) push(ctx context.Context, conn net.Conn) error {
	select {
	case l.conns <- conn:
		return nil
	case <-l.done:
		return net.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}
