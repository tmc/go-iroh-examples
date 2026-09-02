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
	// The ID is freshly generated, so only the paths are stable. Assert both
	// accessors and both rendered transport addresses: a direct path and a
	// relay path are separately reachable and separately counted.
	for _, want := range []string{
		"direct paths: 1",
		"relay paths: 1",
		"relay:https://relay.example.com/",
		"ip:[::1]:4433",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
	if !strings.Contains(out, "endpoint: ") {
		t.Errorf("output missing endpoint id line\n%s", out)
	}
}
