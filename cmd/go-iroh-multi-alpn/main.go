// Command go-iroh-multi-alpn serves two protocols from one endpoint.
//
// An endpoint is one socket and one identity, but it is not one protocol. Every
// entry in the map given to [iroh.NewRouter] registers an
// [iroh.ProtocolHandler] under an ALPN, and each incoming connection goes to
// the handler whose ALPN it negotiated. go-iroh-router-echo is this example with one
// entry; the second entry is all that separates them.
//
// Dispatch is by exact string match, decided in the TLS handshake. There is no
// fallback handler and no prefix matching, so a dial for an unregistered ALPN
// fails before a connection exists, and the two protocols can never see each
// other's bytes. A protocol is versioned by putting the version in the string —
// both of these end in /1 — and a server that speaks two versions registers
// both and keeps the old handler until its peers have moved.
//
// The client opens one connection per protocol because that is what an ALPN is:
// chosen at dial time and fixed for the life of the connection. Carrying
// several conversations inside one protocol is the job of streams instead
// (go-iroh-uni-streams, go-iroh-framed-messages).
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
)

const (
	echoALPN  = "go-iroh-examples/multi-alpn/echo/1"
	upperALPN = "go-iroh-examples/multi-alpn/upper/1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server, err := exampleutil.Bind(ctx)
	if err != nil {
		return err
	}

	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		echoALPN:  exampleutil.Handler{},
		upperALPN: exampleutil.Handler{Transform: strings.ToUpper},
	}, nil)
	if err != nil {
		return err
	}
	defer router.Shutdown(ctx)

	client, err := exampleutil.Bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	addr := exampleutil.Addr(server)
	echoConn, err := client.Connect(ctx, addr, echoALPN)
	if err != nil {
		return fmt.Errorf("connect for %s: %w", echoALPN, err)
	}
	defer echoConn.CloseWithError(0, "")

	upperConn, err := client.Connect(ctx, addr, upperALPN)
	if err != nil {
		return fmt.Errorf("connect for %s: %w", upperALPN, err)
	}
	defer upperConn.CloseWithError(0, "")

	// The same request over both connections. Only the ALPN differs, so the
	// two replies are the dispatch.
	echoReply, err := exampleutil.Exchange(ctx, echoConn, "multi hello")
	if err != nil {
		return err
	}
	upperReply, err := exampleutil.Exchange(ctx, upperConn, "multi hello")
	if err != nil {
		return err
	}

	fmt.Println("echo:", echoReply)
	fmt.Println("upper:", upperReply)
	return nil
}
