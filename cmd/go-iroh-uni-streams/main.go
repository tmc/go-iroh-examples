// Command go-iroh-uni-streams publishes events over unidirectional streams.
//
// A unidirectional stream carries bytes one way and needs no reply, so the
// sender never waits for the receiver and the receiver never has to correlate a
// response. That makes one stream per message a reasonable design for telemetry,
// logs, and notifications: each message is independently ordered and framed by
// the stream's own close, so a slow consumer of one message does not stall the
// next.
//
// Compare go-iroh-direct-echo, where a bidirectional stream is a request and its
// reply, and go-iroh-datagram-frames, where a datagram is used because a late message
// is worth less than a dropped one. A unidirectional stream is the middle case:
// reliable and ordered, but one-way.
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
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(stdout io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server, err := bind(ctx, iroh.WithALPNs(alpn))
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	received := make(chan telemetryResult, 1)
	go func() {
		events, err := receiveTelemetry(ctx, server, 3)
		received <- telemetryResult{events: events, err: err}
	}()

	client, err := bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, server.Addr(), alpn)
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
		fmt.Fprintf(stdout, "event %d: %s=%d\n", event.Seq, event.Metric, event.Value)
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

// bind binds an endpoint to an ephemeral IPv6 loopback port, then applies opts.
// Loopback binding keeps the example self-contained: no relay, no DNS, no
// network access.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
}
