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
	// The uppercased line proves the request crossed the net.Conn in both
	// directions within the deadline; a deadline that fired instead would end
	// run with a timeout error.
	for _, want := range []string{
		"DEADLINE EXAMPLE\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
