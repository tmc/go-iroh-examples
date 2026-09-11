// Command go-iroh-stream-listener serves ordinary net.Listener code over iroh.
//
// A QUIC stream is already a net.Conn, so anything written against
// net.Listener — net/http, a line protocol, an RPC server — runs unchanged over
// an iroh endpoint once something turns accepted streams into a listener.
// go-iroh ships two ways to do that, and the difference is who owns the accept
// loop:
//
//   - [iroh.Endpoint.ListenStreams] takes the endpoint's accept loop and hands
//     back a net.Listener. Use it when the endpoint speaks one protocol.
//   - [iroh.NewStreamListener] produces a listener plus an
//     [iroh.ProtocolHandler] to register with a [iroh.Router]. Use it when the
//     endpoint multiplexes several ALPNs and only one of them is net.Listener
//     shaped.
//
// The example runs net/http over the first and a line protocol over the second.
// Neither needs the 50 lines of channel plumbing an application would otherwise
// write to adapt AcceptStreamConn to net.Listener.
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strings"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const (
	httpALPN   = "go-iroh-examples/stream-listener/http/1"
	routerALPN = "go-iroh-examples/stream-listener/router/1"
)

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(stdout io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := serveHTTP(ctx, stdout); err != nil {
		return err
	}
	return serveRouterProtocol(ctx, stdout)
}

func serveHTTP(ctx context.Context, stdout io.Writer) error {
	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithALPNs(httpALPN),
	)
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	ln, err := server.ListenStreams()
	if err != nil {
		return err
	}
	defer ln.Close()
	var _ net.Listener = ln

	httpServer := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			name := r.URL.Query().Get("name")
			if name == "" {
				name = "listener"
			}
			fmt.Fprintf(w, "hello %s over %s\n", name, r.Proto)
		}),
	}
	done := make(chan error, 1)
	go func() {
		err := httpServer.Serve(ln)
		if err == http.ErrServerClosed {
			err = nil
		}
		done <- err
	}()
	defer httpServer.Close()

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				return client.Dial(ctx, addr, httpALPN)
			},
		},
		Timeout: 5 * time.Second,
	}

	resp, err := httpClient.Get("http://iroh.local/greet?name=listener")
	if err != nil {
		return err
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	httpServer.Close()
	if err := <-done; err != nil {
		return err
	}
	fmt.Fprint(stdout, string(body))
	fmt.Fprintln(stdout, "http listener:", ln.Addr())
	return nil
}

func serveRouterProtocol(ctx context.Context, stdout io.Writer) error {
	server, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	ln := iroh.NewStreamListener()
	defer ln.Close()
	var _ net.Listener = ln

	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		routerALPN: ln.Handler(),
	}, nil)
	if err != nil {
		return err
	}
	defer router.Shutdown(ctx)

	done := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		defer c.Close()
		line, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			done <- err
			return
		}
		_, err = io.WriteString(c, strings.ToUpper(line))
		done <- err
	}()

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	c, err := client.Dial(ctx, addr, routerALPN)
	if err != nil {
		return err
	}
	defer c.Close()
	var _ net.Conn = c

	if _, err := io.WriteString(c, "router listener hello\n"); err != nil {
		return err
	}
	reply, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return err
	}
	if err := <-done; err != nil {
		return err
	}
	fmt.Fprint(stdout, reply)
	fmt.Fprintln(stdout, "router listener:", ln.Addr())
	return nil
}
