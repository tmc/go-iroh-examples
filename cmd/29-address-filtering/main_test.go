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
	// Each line is a filter's decision about the same three addresses, read
	// back out of the resolver rather than out of the filter, so a change in
	// what gets published shows up here.
	for _, want := range []string{
		"relay only: relay=1 ip=0 custom=0",
		"ip only: relay=0 ip=1 custom=1",
		"relay plus custom: relay=1 ip=0 custom=1",
		"lookup services filter: ip=1 custom=1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
