package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	var buf bytes.Buffer
	err := run(&buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	// The reply is the whole protocol: a handler that echoed the request, or
	// wrote nothing, would still exit 0.
	if got := strings.TrimSpace(out); got != response {
		t.Errorf("reply = %q, want %q", got, response)
	}
}
