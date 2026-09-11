package main

import (
	"bytes"
	"flag"
	"io"
	"strings"
	"testing"
)

// TestListen runs the self-contained half of the listener: bind, print the
// address, exit. Binding an unspecified IPv4 address needs a socket but no
// reachable network, so this is the assertion that still runs offline. Port 0
// keeps the test off the example's default 4433, which may be in use.
func TestListen(t *testing.T) {
	if testing.Short() {
		t.Skip("binds a UDP socket")
	}
	var buf bytes.Buffer
	err := run([]string{"listen", "-port=0", "-alpn=" + defaultALPN, "-serve=false", "-live=false"}, &buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		"alpn: go-iroh-examples/public-endpoint/1",
		"relay paths: []", // direct-only by default: no relay is advertised
		"local udp: ",
		"pass -serve or set IROH_EXAMPLE_SERVE=1 to keep serving echo connections",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
	if id, ok := field(out, "endpoint id: "); !ok || len(id) != 52 {
		t.Errorf("endpoint id is not a 52-character z32 id: %q\n%s", id, out)
	}
}

// TestListenLive is the same listener against n0's public relay map, which is
// what -live adds: the endpoint waits for a home relay and then advertises it.
func TestListenLive(t *testing.T) {
	if testing.Short() {
		t.Skip("contacts the public relay map")
	}
	if !envBool("GO_IROH_LIVE_RELAY", false) {
		t.Skip("set GO_IROH_LIVE_RELAY=1 to dial n0's public relays")
	}
	var buf bytes.Buffer
	err := run([]string{"listen", "-port=0", "-serve=false", "-live"}, &buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if !strings.Contains(out, "endpoint id: ") {
		t.Errorf("output missing %q\n%s", "endpoint id: ", out)
	}
	if strings.Contains(out, "relay paths: []") {
		t.Errorf("online endpoint advertised no relay path\n%s", out)
	}
}

// TestConnect covers the dialing path that needs nothing outside this machine:
// with no peer coordinates the example says which ones it wants and exits 0.
// Empty flags override any IROH_EXAMPLE_PEER_* already in the environment.
func TestConnect(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	var buf bytes.Buffer
	err := run([]string{"connect", "-peer-id=", "-peer-ip=", "-peer-relay="}, &buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	want := "pass -peer-id and -peer-ip or -peer-relay (or set IROH_EXAMPLE_PEER_ID, IROH_EXAMPLE_PEER_IP, IROH_EXAMPLE_PEER_RELAY)"
	if !strings.Contains(out, want) {
		t.Errorf("output missing %q\n%s", want, out)
	}
}

// TestConnectLive dials the peer named in the environment, which must be
// running "go-iroh-public-endpoint listen -serve" or another endpoint echoing
// on the same ALPN.
func TestConnectLive(t *testing.T) {
	if testing.Short() {
		t.Skip("dials a remote peer")
	}
	id := env("IROH_EXAMPLE_PEER_ID", "")
	ip := env("IROH_EXAMPLE_PEER_IP", "")
	relayURL := env("IROH_EXAMPLE_PEER_RELAY", "")
	if id == "" || (ip == "" && relayURL == "") {
		t.Skip("set IROH_EXAMPLE_PEER_ID and IROH_EXAMPLE_PEER_IP or IROH_EXAMPLE_PEER_RELAY to dial a live peer")
	}
	var buf bytes.Buffer
	err := run([]string{"connect"}, &buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if want := "public hello"; !strings.Contains(out, want) { // the listener echoed the request back
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

// TestRunUsage checks that an unusable command line is reported as such rather
// than acted on.
func TestRunUsage(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"nonsense"},
		{"listen", "-no-such-flag"},
		{"connect", "stray-argument"},
	} {
		if err := run(args, io.Discard); err != errUsage {
			t.Errorf("run(%q, io.Discard) = %v, want errUsage", args, err)
		}
	}
}

// TestRunHelp checks that -h is answered with the usage text rather than
// treated as a failure.
func TestRunHelp(t *testing.T) {
	if err := run([]string{"-h"}, io.Discard); err != flag.ErrHelp {
		t.Errorf("run(-h, io.Discard) = %v, want flag.ErrHelp", err)
	}
	for _, sub := range []string{"listen", "connect"} {
		if err := run([]string{sub, "-h"}, io.Discard); err != flag.ErrHelp {
			t.Errorf("run(%s -h, io.Discard) = %v, want flag.ErrHelp", sub, err)
		}
	}
}

// field returns the remainder of the first line of out starting with prefix.
func field(out, prefix string) (string, bool) {
	for line := range strings.SplitSeq(out, "\n") {
		if rest, ok := strings.CutPrefix(line, prefix); ok {
			return rest, true
		}
	}
	return "", false
}
