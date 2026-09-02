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
	// mDNS is the one example that depends on the host's networking. Where the
	// multicast socket or the announcement does not survive the environment,
	// run reports that and exits cleanly; there is nothing to assert on.
	if strings.Contains(out, needsMulticast) {
		t.Skipf("mDNS unavailable in this environment:\n%s", out)
	}
	for _, want := range []string{
		"default service: irohv1",
		"announcing: 1 address(es)",
		"discovered by id: 1 address(es)",
		"provenance: mdns",
		"same endpoint: true",
		"reply: mdns hello",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
