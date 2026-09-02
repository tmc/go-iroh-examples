package main

import (
	"strings"
	"testing"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
)

func TestRun(t *testing.T) {
	out, err := exampleutil.Capture(run)
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
