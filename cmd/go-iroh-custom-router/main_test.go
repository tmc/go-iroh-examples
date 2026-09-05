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
	// The whole point is the order: the same ALPN is served, then dropped,
	// while the other goes the other way, and an unadvertised ALPN never gets
	// past negotiation at all.
	want := []string{
		`dial alpn1: served, echoed "hello"`,
		"dial alpn2: connected, then closed with code 1 (no handler for alpn)",
		"dial alpn3: refused during negotiation, not advertised",
		"remove alpn1: true",
		"dial alpn1: connected, then closed with code 1 (no handler for alpn)",
		"dial alpn2: connected, then closed with code 1 (no handler for alpn)",
		"add alpn2",
		"dial alpn1: connected, then closed with code 1 (no handler for alpn)",
		`dial alpn2: served, echoed "hello"`,
	}
	rest := out
	for _, w := range want {
		i := strings.Index(rest, w)
		if i < 0 {
			t.Fatalf("output missing %q after the lines before it\n%s", w, out)
		}
		rest = rest[i+len(w):]
	}
}
