package main

import (
	"strings"
	"testing"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
)

// TestRun exercises the default path, which diagnoses an in-process relay and
// needs no network. The -live path is not tested here; it depends on n0's
// public relays being reachable.
func TestRun(t *testing.T) {
	out, err := exampleutil.Capture(func() error { return run(nil) })
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		"live relay: false",
		"home relay connected: true",
		"net report available: true",
		"relay latencies: 1",
		"udp available:",
		"preferred relay:",
		"connection selected: relay",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
