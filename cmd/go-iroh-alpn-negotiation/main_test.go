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
	for _, want := range []string{
		`modern client: negotiated v2, reply "v2 hello"`,
		`pinned to v2: negotiated v2, reply "v2 hello"`,
		`legacy client: negotiated v1, reply "hello"`,
		"future-only client: no version in common with the server",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
