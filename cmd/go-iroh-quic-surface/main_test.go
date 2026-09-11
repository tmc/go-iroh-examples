package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/quicconn"
)

func TestRun(t *testing.T) {
	var buf bytes.Buffer
	err := run(&buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		`bidi: "answered GET /surface"`,
		`uni: "pushed by the server"`,
		`datagram: "pong for ping"`,
		"closed: application code 0x100",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}

// TestStreamIsTheIrohStream checks the claim the doc comment makes: the adapter
// is a vocabulary over the same streams, not a second transport. If IrohStream
// returned something else, code mixing the two surfaces would silently split
// its data across two streams.
func TestStreamIsTheIrohStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	server, err := bind(ctx, iroh.WithALPNs(alpn))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())

	echoed := make(chan error, 1)
	go func() { echoed <- echoOneStream(ctx, server) }()

	client, err := bind(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Shutdown(context.Background())

	raw, err := client.Connect(ctx, server.Addr(), alpn)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.CloseWithError(0, "")

	bidi, err := quicconn.NewConn(raw).OpenBidi(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Write through the adapter, then through the stream it says it wraps.
	// The peer echoes what it reads, so both halves arriving proves they are
	// one stream and not two.
	if _, err := io.WriteString(bidi, "through the adapter, "); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(bidi.IrohStream(), "through the stream"); err != nil {
		t.Fatal(err)
	}
	if err := bidi.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(bidi)
	if err != nil {
		t.Fatal(err)
	}
	if want := "through the adapter, through the stream"; string(got) != want {
		t.Errorf("echoed %q, want %q", got, want)
	}
	// Close before waiting: the echo side stays up until the client goes away,
	// so that its deferred close cannot race this test's last read.
	raw.CloseWithError(0, "")
	if err := <-echoed; err != nil {
		t.Fatalf("echo: %v", err)
	}
}

func echoOneStream(ctx context.Context, ep *iroh.Endpoint) error {
	conn, err := ep.Accept(ctx)
	if err != nil {
		return err
	}
	defer conn.CloseWithError(0, "")
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	body, err := io.ReadAll(stream)
	if err != nil {
		return err
	}
	if _, err := stream.Write(body); err != nil {
		return err
	}
	if err := stream.CloseWrite(); err != nil {
		return err
	}
	// Hold the connection open until the reader is finished with it.
	<-conn.Context().Done()
	return nil
}
