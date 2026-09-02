package main

import (
	"strings"
	"testing"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
)

// TestRun covers the path that needs nothing outside this machine: with no peer
// coordinates the example says which ones it wants and exits 0. Empty flags
// override any IROH_EXAMPLE_PEER_* already in the environment.
func TestRun(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	out, err := exampleutil.Capture(func() error {
		return run([]string{"-peer-id=", "-peer-ip=", "-peer-relay="})
	})
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	want := "pass -peer-id and -peer-ip or -peer-relay (or set IROH_EXAMPLE_PEER_ID, IROH_EXAMPLE_PEER_IP, IROH_EXAMPLE_PEER_RELAY)"
	if !strings.Contains(out, want) {
		t.Errorf("output missing %q\n%s", want, out)
	}
}

// TestRunLive dials the peer named in the environment, which must be running
// 11-public-server or another endpoint echoing on the same ALPN.
func TestRunLive(t *testing.T) {
	if testing.Short() {
		t.Skip("dials a remote peer")
	}
	id := exampleutil.Env("IROH_EXAMPLE_PEER_ID", "")
	ip := exampleutil.Env("IROH_EXAMPLE_PEER_IP", "")
	relayURL := exampleutil.Env("IROH_EXAMPLE_PEER_RELAY", "")
	if id == "" || (ip == "" && relayURL == "") {
		t.Skip("set IROH_EXAMPLE_PEER_ID and IROH_EXAMPLE_PEER_IP or IROH_EXAMPLE_PEER_RELAY to dial a live peer")
	}
	out, err := exampleutil.Capture(func() error { return run(nil) })
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if want := "public hello"; !strings.Contains(out, want) { // the server echoed the request back
		t.Errorf("output missing %q\n%s", want, out)
	}
	want, err := parseEndpointID(id)
	if err != nil {
		t.Fatalf("parse IROH_EXAMPLE_PEER_ID: %v", err)
	}
	if !strings.Contains(out, "remote: "+want.Short()) {
		t.Errorf("dialed %s but connected to another peer\n%s", id, out)
	}
}
