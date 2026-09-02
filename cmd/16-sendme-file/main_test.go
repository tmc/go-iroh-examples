package main

import (
	"strings"
	"testing"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
)

func TestRun(t *testing.T) {
	out, err := exampleutil.Capture(func() error { return run(nil) })
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		"bytes: 27",
		// The hash of the built-in payload. It is what the receiver asked for,
		// so printing it unchanged is the evidence that the verified transfer
		// returned that content and not something else.
		"blake3: 726c5e0e4a",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
