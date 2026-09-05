// Command go-iroh-qlog-tracing records a qlog trace of a QUIC connection.
//
// go-iroh-hooks and go-iroh-metrics watch an endpoint from the outside: hooks report the
// dials and handshakes an application makes, and counters total them up. Both
// answer "what did my program do". Neither answers "what did QUIC do" — which
// packets went out, which were lost, how the congestion window moved, why the
// handshake took as long as it did.
//
// qlog does. It is the QUIC working group's standard trace format, and every
// go-iroh endpoint installs the tracer, so setting the QLOGDIR environment
// variable records every connection in the process with no code change at all:
//
//	QLOGDIR=./traces go run ./cmd/go-iroh-direct-echo
//
// That is process-wide, which is the problem this example is about. Several
// endpoints in one process — and every example here binds at least two — write
// into one directory keyed only by connection id, so the traces interleave and
// nothing says which endpoint produced which file. [iroh.WithQLOG] replaces the
// variable for a single endpoint with a sink: a function called once per
// connection, before the handshake, returning the [io.WriteCloser] that
// connection's trace is written to. Returning nil traces that connection not at
// all, which is how a sink samples.
//
// The two endpoints here take the two shapes a sink comes in. The client uses
// [iroh.QLOGDir], which reproduces QLOGDIR's file layout —
// <connection id>_client.sqlog — in a directory of its own choosing. The server
// keeps its trace in memory instead, which is the point of a sink taking an
// io.WriteCloser rather than a path: a trace can go to a ring buffer a support
// bundle picks up, or to an upload, without touching the disk.
//
// Both files describe one connection from opposite ends. [iroh.QLOGConnection]
// carries the original destination connection id, which the two ends share, and
// whether this endpoint dialed; that is what lets the two traces be paired
// afterwards, and it is why the id is part of the API rather than an
// implementation detail of the file name.
//
// A trace holds frame metadata, loss and congestion events, and the handshake.
// It does not hold payload bytes, which this example checks rather than
// asserts: the message the two endpoints exchange does not appear in either
// trace. To see payload bytes at all, log the TLS secrets with
// [iroh.WithKeyLogWriter] and decrypt a capture in Wireshark — a debugging tool
// that gives away the confidentiality of every connection the endpoint makes.
//
// The traces are JSON-seq (RFC 7464): one JSON object per record, each preceded
// by a 0x1e byte. qvis (https://qvis.quictools.info) draws them.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tmc/go-iroh/iroh"
)

const (
	alpn = "go-iroh-examples/qlog-tracing/1"

	// payload is checked for in both traces. qlog records what QUIC did, not
	// what the application said.
	payload = "qlog-tracing-payload"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dir, err := os.MkdirTemp("", "qlog")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	// The server keeps its traces in memory. A sink is called once per
	// connection and owns what it returns, so the buffer is created here and
	// closed by the endpoint when the connection ends.
	var traces sinkRecorder
	server, err := bind(ctx,
		iroh.WithALPNs(alpn),
		iroh.WithQLOG(traces.open),
	)
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	accepted := make(chan error, 1)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			accepted <- err
			return
		}
		accepted <- echo(ctx, conn)
	}()

	// The client writes files, the layout QLOGDIR produces and qvis expects.
	client, err := bind(ctx, iroh.WithQLOG(iroh.QLOGDir(dir)))
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, server.Addr(), alpn)
	if err != nil {
		return err
	}
	reply, err := exchange(ctx, conn, payload)
	if err != nil {
		return err
	}
	if err := <-accepted; err != nil {
		return err
	}
	fmt.Println("reply:", reply)

	// Closing the connection is what closes the trace: the sink's writer is
	// closed once, when the connection ends, so a trace read before that is
	// truncated.
	conn.CloseWithError(0, "done")

	clientTrace, name, err := waitTrace(ctx, dir)
	if err != nil {
		return err
	}
	serverTrace, ok := traces.wait(ctx)
	if !ok {
		return fmt.Errorf("server recorded no trace")
	}

	// The file name is the connection id and the side, which is how the two
	// ends of one connection are paired after the fact.
	id, side, _ := strings.Cut(strings.TrimSuffix(name, ".sqlog"), "_")
	fmt.Println("client trace side:", side)
	fmt.Println("both ends share the connection id:", id == serverTrace.id)

	for _, t := range []struct {
		name string
		data []byte
	}{
		{"client", clientTrace},
		{"server", serverTrace.buf.Bytes()},
	} {
		names, err := eventNames(t.data)
		if err != nil {
			return fmt.Errorf("%s trace: %w", t.name, err)
		}
		fmt.Printf("%s trace: %d records, %d event kinds\n", t.name, len(names), countKinds(names))
		fmt.Printf("%s trace has packet_sent: %v\n", t.name, has(names, "packet_sent"))
		fmt.Printf("%s trace contains the payload: %v\n", t.name, bytes.Contains(t.data, []byte(payload)))
	}
	return nil
}

// sinkRecorder is a [iroh.WithQLOG] sink that keeps each connection's trace in
// memory. A real one would cap the buffer or forward it somewhere; the shape is
// what matters, which is that a sink hands back an io.WriteCloser and nothing
// else is required of it.
type sinkRecorder struct {
	mu     sync.Mutex
	traces []*memTrace
}

// open is the sink. It runs before the handshake, on the endpoint's goroutine,
// so it does as little as possible.
func (r *sinkRecorder) open(_ context.Context, c iroh.QLOGConnection) io.WriteCloser {
	t := &memTrace{id: c.ConnectionID}
	r.mu.Lock()
	r.traces = append(r.traces, t)
	r.mu.Unlock()
	return t
}

// wait returns the first complete trace the sink recorded.
func (r *sinkRecorder) wait(ctx context.Context) (*memTrace, bool) {
	for {
		r.mu.Lock()
		for _, t := range r.traces {
			if t.done() {
				r.mu.Unlock()
				return t, true
			}
		}
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// memTrace is one connection's trace, held in memory. The endpoint writes to it
// from the connection's goroutine and closes it once, so it locks.
type memTrace struct {
	id string

	mu     sync.Mutex
	buf    bytes.Buffer
	closed bool
}

func (t *memTrace) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.buf.Write(p)
}

func (t *memTrace) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}

func (t *memTrace) done() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

// waitTrace returns the contents and base name of the first .sqlog file to
// appear in dir. QLOGDir creates the file when the connection starts, so the
// wait is for content rather than for the file.
func waitTrace(ctx context.Context, dir string) ([]byte, string, error) {
	for {
		names, err := filepath.Glob(filepath.Join(dir, "*.sqlog"))
		if err != nil {
			return nil, "", err
		}
		for _, name := range names {
			b, err := os.ReadFile(name)
			if err != nil {
				return nil, "", err
			}
			if len(b) > 0 {
				return b, filepath.Base(name), nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, "", fmt.Errorf("no qlog trace in %s: %w", dir, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// eventNames returns the "name" of every record in a JSON-seq qlog trace, in
// order. RFC 7464 puts a 0x1e byte before each record, so the split is on that
// and not on newlines: a record may contain them.
func eventNames(trace []byte) ([]string, error) {
	var names []string
	for _, rec := range bytes.Split(trace, []byte{0x1e}) {
		rec = bytes.TrimSpace(rec)
		if len(rec) == 0 {
			continue
		}
		var ev struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(rec, &ev); err != nil {
			return nil, fmt.Errorf("record %d: %w", len(names), err)
		}
		names = append(names, ev.Name)
	}
	return names, nil
}

func has(names []string, want string) bool {
	for _, n := range names {
		if n == want || strings.HasSuffix(n, ":"+want) {
			return true
		}
	}
	return false
}

// countKinds returns how many distinct event names the trace holds.
func countKinds(names []string) int {
	seen := map[string]bool{}
	for _, n := range names {
		seen[n] = true
	}
	return len(seen)
}

// bind binds an endpoint to an ephemeral IPv6 loopback port. Loopback keeps
// the example self-contained: no relay, no DNS, no network access.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
}

// echo accepts one bidirectional stream, reads it to EOF, and writes back what
// it read. It is the server half of exchange.
func echo(ctx context.Context, conn *iroh.Conn) error {
	s, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	b, err := io.ReadAll(s)
	if err != nil {
		return err
	}
	if _, err := s.Write(b); err != nil {
		return err
	}
	return s.Close()
}

// exchange opens a bidirectional stream, writes msg, closes the write side, and
// reads the reply until EOF.
func exchange(ctx context.Context, conn *iroh.Conn, msg string) (string, error) {
	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return "", err
	}
	if _, err := s.Write([]byte(msg)); err != nil {
		return "", err
	}
	// Half-close: the peer reads to EOF and replies on the same stream.
	if err := s.CloseWrite(); err != nil {
		return "", err
	}
	b, err := io.ReadAll(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
