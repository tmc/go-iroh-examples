package main

import (
	"strings"
	"testing"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
)

// TestRun exercises the in-process relay, which needs no network. -live is
// passed false rather than left to its default, which comes from
// GO_IROH_LIVE_RELAY: the assertions below describe a single local relay, and
// an exported GO_IROH_LIVE_RELAY would diagnose n0's public ones instead. The
// live path is not tested here; it depends on those relays being reachable.
func TestRun(t *testing.T) {
	out, err := exampleutil.Capture(func() error { return run([]string{"-live=false"}) })
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
