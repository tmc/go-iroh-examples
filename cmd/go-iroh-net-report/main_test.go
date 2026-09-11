package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestRun runs the direct-only path, which binds a loopback endpoint and needs
// no network. A direct-only endpoint has no relay to probe, so the point of the
// assertion is that the example says so instead of printing an empty report.
func TestRun(t *testing.T) {
	if testing.Short() {
		t.Skip("binds a loopback endpoint")
	}
	var buf bytes.Buffer
	err := run([]string{"-live=false"}, &buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if want := "live relay: false"; !strings.Contains(out, want) {
		t.Errorf("output missing %q\n%s", want, out)
	}
	switch {
	case strings.Contains(out, "report available: false"):
		want := "pass -live or set GO_IROH_LIVE_RELAY=1 to run net_report against the public relay map"
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	case strings.Contains(out, "report available: true"):
		// Unexpected without relays, but then the report must be printed.
		for _, want := range []string{"has udp:", "udp4:", "udp6:", "preferred relay:"} {
			if !strings.Contains(out, want) {
				t.Errorf("output missing %q\n%s", want, out)
			}
		}
	default:
		t.Errorf("output does not say whether a report was available\n%s", out)
	}
}

// TestRunLive probes the public relay map, where a report is expected to arrive
// and to name the relay that answered. The UDP fields are not asserted: the
// example binds a loopback socket, so the QAD probes have nowhere to go and
// report false even when the relays answer over HTTPS.
func TestRunLive(t *testing.T) {
	if testing.Short() {
		t.Skip("probes the public relay map")
	}
	if !envBool("GO_IROH_LIVE_RELAY", false) {
		t.Skip("set GO_IROH_LIVE_RELAY=1 to probe n0's public relay map")
	}
	var buf bytes.Buffer
	err := run([]string{"-live"}, &buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		"live relay: true",
		"report available: true",
		"has udp: ",
		"preferred relay: https://",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
