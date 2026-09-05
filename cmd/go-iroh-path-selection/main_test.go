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
	// Only what the policy itself guarantees. Whether a direct path is
	// discovered, and so whether the default selector would have diverged from
	// relayFirst, is a race the test deliberately does not assert on; that the
	// selector was consulted, was offered the relay, and pinned every decision
	// to it is not.
	for _, want := range []string{
		"selector consulted: true",
		"policy chose relay every time: true",
		"selected path kind: relay",
		"select: candidates=[relay:",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
	if !strings.Contains(out, "candidate kinds offered: relay") &&
		!strings.Contains(out, "candidate kinds offered: ip,relay") {
		t.Errorf("output missing a relay candidate\n%s", out)
	}
}
