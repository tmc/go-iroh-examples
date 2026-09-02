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
	// The reply is the whole protocol: a handler that echoed the request, or
	// wrote nothing, would still exit 0.
	if got := strings.TrimSpace(out); got != response {
		t.Errorf("reply = %q, want %q", got, response)
	}
}
