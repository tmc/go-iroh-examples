package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestRun covers the offline path: without the opt-in the example names the
// switch and exits 0, so nothing here contacts the relays.
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
	want := "pass -live or set GO_IROH_LIVE_RELAY=1 to connect to the default public relays"
	if !strings.Contains(out, want) {
		t.Errorf("output missing %q\n%s", want, out)
	}
}

// TestRunLive joins the public relay map for real and checks that the wait
// produced what it promises: a connected home relay that the endpoint then
// advertises.
func TestRunLive(t *testing.T) {
	if testing.Short() {
		t.Skip("contacts the public relay map")
	}
	if !envBool("GO_IROH_LIVE_RELAY", false) {
		t.Skip("set GO_IROH_LIVE_RELAY=1 to join n0's public relay map")
	}
	var buf bytes.Buffer
	err := run([]string{"-live"}, &buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		"home relay: https://",
		"connected: true",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
	if strings.Contains(out, "advertised relays: []") {
		t.Errorf("endpoint is online but advertises no relay\n%s", out)
	}
}
