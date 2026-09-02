package main

import (
	"strconv"
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
		"validated hello",
		// The whole point: with a policy that always retries, the connection
		// reaches AcceptIncoming already validated.
		"remote address validated: true",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
	// How often qng consults the policy is its business, so assert only that
	// it was consulted at all.
	if n := retryChecks(t, out); n < 1 {
		t.Errorf("retry checks = %d, want at least 1\n%s", n, out)
	}
}

func retryChecks(t *testing.T, out string) int {
	t.Helper()
	const label = "retry checks: "
	_, rest, ok := strings.Cut(out, label)
	if !ok {
		t.Fatalf("output missing %q\n%s", label, out)
	}
	field, _, _ := strings.Cut(rest, "\n")
	n, err := strconv.Atoi(strings.TrimSpace(field))
	if err != nil {
		t.Fatalf("parse %q: %v", field, err)
	}
	return n
}
