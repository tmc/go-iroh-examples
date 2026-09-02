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
	// The ALPN line comes from Accepting.ALPN and the short ID from the
	// verified Conn, so printing both proves the two later stages ran and
	// reported the right connection rather than a default.
	for _, want := range []string{
		"manual hello",
		alpn + " from ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
