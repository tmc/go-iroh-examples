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
	// The address count proves the lookup answered rather than the dial
	// falling back on something the client already had; the reply proves the
	// address it answered with was dialable.
	for _, want := range []string{
		"resolved addresses: 1",
		"discovered hello",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
