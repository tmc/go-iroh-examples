package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const alpn = "go-iroh-examples/uni-streams/1"

type telemetryEvent struct {
	Seq    int    `json:"seq"`
	Metric string `json:"metric"`
	Value  int    `json:"value"`
}

type telemetryResult struct {
	events []telemetryEvent
	err    error
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

	received := make(chan telemetryResult, 1)
	go func() {
		events, err := receiveTelemetry(ctx, server, 3)
		received <- telemetryResult{events: events, err: err}
	}()

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
	defer conn.Close()

	events := []telemetryEvent{
		{Seq: 1, Metric: "cpu", Value: 42},
		{Seq: 2, Metric: "memory", Value: 73},
		{Seq: 3, Metric: "queue", Value: 5},
	}
	for _, event := range events {
		if err := publishEvent(ctx, conn, event); err != nil {
			return err
		}
	}

	result := <-received
	if result.err != nil {
		return result.err
	}
	for _, event := range result.events {
		fmt.Printf("event %d: %s=%d\n", event.Seq, event.Metric, event.Value)
	}
	return nil
}

func publishEvent(ctx context.Context, conn *iroh.Conn, event telemetryEvent) error {
	stream, err := conn.OpenUniStreamSync(ctx)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(stream).Encode(event); err != nil {
		stream.Close()
		return err
	}
	return stream.Close()
}

func receiveTelemetry(ctx context.Context, ep *iroh.Endpoint, n int) ([]telemetryEvent, error) {
	conn, err := ep.Accept(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	events := make([]telemetryEvent, 0, n)
	for len(events) < n {
		stream, err := conn.AcceptUniStream(ctx)
		if err != nil {
			return nil, err
		}
		event, err := readEvent(stream)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func readEvent(r io.Reader) (telemetryEvent, error) {
	var event telemetryEvent
	if err := json.NewDecoder(r).Decode(&event); err != nil {
		return telemetryEvent{}, err
	}
	return event, nil
}
