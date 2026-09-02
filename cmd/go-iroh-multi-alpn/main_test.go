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
	// One request, two ALPNs, two different answers: the differing replies are
	// the only evidence that the router dispatched by ALPN rather than running
	// whichever handler it happened to register first.
	for _, want := range []string{
		"echo: multi hello",
		"upper: MULTI HELLO",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
