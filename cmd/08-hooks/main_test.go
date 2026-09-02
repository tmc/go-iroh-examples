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
	if !strings.Contains(out, "hooked hello") {
		t.Errorf("output missing %q\n%s", "hooked hello", out)
	}
	// The peer ID is fresh each run, so assert the property instead: both
	// hooks fired, and the peer that answered is the peer that was dialed,
	// under the ALPN that was asked for.
	before := field(t, out, "before: ")
	after := field(t, out, "after: ")
	if before != after {
		t.Errorf("before %q != after %q\n%s", before, after, out)
	}
	if !strings.HasSuffix(before, " "+alpn) {
		t.Errorf("hooks reported %q, want alpn %q\n%s", before, alpn, out)
	}
}

// field returns the rest of the line that follows label.
func field(t *testing.T, out, label string) string {
	t.Helper()
	_, rest, ok := strings.Cut(out, label)
	if !ok {
		t.Fatalf("output missing %q\n%s", label, out)
	}
	line, _, _ := strings.Cut(rest, "\n")
	return strings.TrimSpace(line)
}
