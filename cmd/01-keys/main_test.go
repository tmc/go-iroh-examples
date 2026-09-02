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
	// The seed is fixed, so the identity it derives is too: these strings pin
	// both the key derivation and the two renderings of an endpoint ID.
	for _, want := range []string{
		"endpoint id: 221fe5f685",
		"z32: rex6m7wfuepr95d3yi6j4cse4x6dk5owjcq48c83yfzxphwatdpy",
		"signature valid: true",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
