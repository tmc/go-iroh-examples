// Command 39-datagram-vs-stream sends small messages as QUIC datagrams and
// falls back to streams when a payload is too large for one datagram.
package main

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strings"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const alpn = "go-iroh-examples/datagram-vs-stream/1"

type receivedMessage struct {
	Via string
	Len int
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

	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithALPNs(alpn),
	)
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	received := make(chan receivedMessage, 4)
	serverErr := make(chan error, 1)
	go acceptOne(ctx, server, received, serverErr)

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		return err
	}
	defer conn.CloseWithError(0, "")

	small := []byte("fits in one datagram")
	large := []byte(strings.Repeat("stream fallback ", 5000))

	via, err := sendMessage(ctx, conn, small)
	if err != nil {
		return err
	}
	fmt.Printf("small sent via %s (%d bytes)\n", via, len(small))

	via, err = sendMessage(ctx, conn, large)
	if err != nil {
		return err
	}
	fmt.Printf("large sent via %s (%d bytes)\n", via, len(large))

	for range 2 {
		select {
		case msg := <-received:
			fmt.Printf("server received %s (%d bytes)\n", msg.Via, msg.Len)
		case err := <-serverErr:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func acceptOne(ctx context.Context, endpoint *iroh.Endpoint, received chan<- receivedMessage, errs chan<- error) {
	conn, err := endpoint.Accept(ctx)
	if err != nil {
		errs <- err
		return
	}
	defer conn.CloseWithError(0, "")

	go readDatagrams(ctx, conn, received)

	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go readStream(stream, received)
	}
}

func readDatagrams(ctx context.Context, conn *iroh.Conn, received chan<- receivedMessage) {
	for {
		b, err := conn.ReadDatagram(ctx)
		if err != nil {
			return
		}
		received <- receivedMessage{Via: "datagram", Len: len(b)}
	}
}

func readStream(stream *iroh.Stream, received chan<- receivedMessage) {
	defer stream.Close()
	b, err := io.ReadAll(stream)
	if err != nil {
		return
	}
	received <- receivedMessage{Via: "stream", Len: len(b)}
}

func sendMessage(ctx context.Context, conn *iroh.Conn, b []byte) (string, error) {
	if err := conn.SendDatagram(b); err == nil {
		return "datagram", nil
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return "", err
	}
	if _, err := stream.Write(b); err != nil {
		stream.Close()
		return "", err
	}
	if err := stream.Close(); err != nil {
		return "", err
	}
	return "stream", nil
}
