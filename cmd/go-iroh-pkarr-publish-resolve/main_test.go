package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestRun covers the offline path: without the opt-in nothing is published and
// the example names the switch that would publish it.
func TestRun(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	var buf bytes.Buffer
	err := run([]string{"-live=false"}, &buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	want := "pass -live or set GO_IROH_LIVE_PKARR=1 to publish to and resolve from the public pkarr relay"
	if !strings.Contains(out, want) {
		t.Errorf("output missing %q\n%s", want, out)
	}
}

// TestRunLive performs the round trip against the public pkarr relay. The
// address that comes back must be the documentation address that was published,
// which is what proves the packet made it through the relay unmodified.
func TestRunLive(t *testing.T) {
	if testing.Short() {
		t.Skip("publishes to the public pkarr relay")
	}
	if !envBool("GO_IROH_LIVE_PKARR", false) {
		t.Skip("set GO_IROH_LIVE_PKARR=1 to use n0's public pkarr relay")
	}
	var buf bytes.Buffer
	err := run([]string{"-live"}, &buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		"published endpoint: ",
		"resolved direct paths: [" + publishedAddr + "]",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
