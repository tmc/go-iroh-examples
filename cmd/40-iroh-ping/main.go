package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const alpn = "iroh/ping/0"

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		panic(err)
	}
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		alpn: pingHandler{},
	}, nil)
	if err != nil {
		panic(err)
	}
	defer router.Shutdown(ctx)

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		panic(err)
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		panic(err)
	}
	defer conn.CloseWithError(0, "")

	reply, err := ping(ctx, conn)
	if err != nil {
		panic(err)
	}
	fmt.Println(reply)
}

type pingHandler struct{}

func (pingHandler) Accept(ctx context.Context, conn *iroh.Conn) error {
	s, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	msg, err := io.ReadAll(s)
	if err != nil {
		return err
	}
	if string(msg) != "PING" {
		return errors.New("unexpected ping payload")
	}
	if _, err := s.Write([]byte("PONG")); err != nil {
		return err
	}
	return s.Close()
}

func ping(ctx context.Context, conn *iroh.Conn) (string, error) {
	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return "", err
	}
	if _, err := s.Write([]byte("PING")); err != nil {
		return "", err
	}
	if err := s.Close(); err != nil {
		return "", err
	}
	reply, err := io.ReadAll(s)
	if err != nil {
		return "", err
	}
	return string(reply), nil
}
