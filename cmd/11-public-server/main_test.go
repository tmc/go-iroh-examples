package main

import (
	"strings"
	"testing"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
)

// TestRun runs the self-contained half of the example: bind, print the address,
// exit. Binding an unspecified IPv4 address needs a socket but no reachable
// network, so this is the assertion that still runs offline. Port 0 keeps the
// test off the example's default 4433, which may be in use.
func TestRun(t *testing.T) {
	if testing.Short() {
		t.Skip("binds a UDP socket")
	}
	out, err := exampleutil.Capture(func() error {
		return run([]string{"-port=0", "-alpn=" + defaultALPN, "-serve=false", "-live=false"})
	})
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		"alpn: go-iroh-examples/public-server/1",
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

// TestRunLive is the same example against n0's public relay map, which is what
// -live adds: the endpoint waits for a home relay and then advertises it.
func TestRunLive(t *testing.T) {
	if testing.Short() {
		t.Skip("contacts the public relay map")
	}
	if !exampleutil.EnvBool("GO_IROH_LIVE_RELAY", false) {
		t.Skip("set GO_IROH_LIVE_RELAY=1 to dial n0's public relays")
	}
	out, err := exampleutil.Capture(func() error {
		return run([]string{"-port=0", "-serve=false", "-live"})
	})
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

// field returns the remainder of the first line of out starting with prefix.
func field(out, prefix string) (string, bool) {
	for line := range strings.SplitSeq(out, "\n") {
		if rest, ok := strings.CutPrefix(line, prefix); ok {
			return rest, true
		}
	}
	return "", false
}
