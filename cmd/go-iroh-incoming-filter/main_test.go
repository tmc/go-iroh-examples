package main

import (
	"bytes"
	"strings"
	"sync"
	"testing"
)

func TestRun(t *testing.T) {
	var buf lockedBuffer
	err := run(&buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		// OnAccepting sees the negotiated ALPN before the connection exists.
		// The remote address that follows it is an ephemeral port, so only the
		// stable part is asserted.
		`on-accepting: alpn="` + alpn + `"`,
		// The gate is open, so the first connection reaches the handler.
		"first client reply: filtered hello",
		// The gate is closed, so the second is dropped by the filter. The
		// refusal is reported either at connect or at first use, and both
		// messages start this way.
		"filter: rejecting ",
		"second client refused",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
	if strings.Contains(out, "second client unexpectedly admitted") {
		t.Errorf("filter admitted a connection while closed\n%s", out)
	}
}

// lockedBuffer is a [bytes.Buffer] that may be written from several goroutines
// at once. run prints from the protocol handler as well as from the dialing
// side, so the writer it is handed has to tolerate what os.Stdout does.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
