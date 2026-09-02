package main

import (
	"strings"
	"testing"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
)

// TestRun covers the path that issues no query: with no endpoint id there is
// nothing to look up, so the example names the flag and exits 0.
func TestRun(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	out, err := exampleutil.Capture(func() error { return run([]string{"-endpoint-id="}) })
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	want := "pass -endpoint-id or set IROH_EXAMPLE_ENDPOINT_ID to a published endpoint id"
	if !strings.Contains(out, want) {
		t.Errorf("output missing %q\n%s", want, out)
	}
}

// TestRunLive resolves the configured id against the configured origin and
// checks that the answer is about the endpoint that was asked for.
func TestRunLive(t *testing.T) {
	if testing.Short() {
		t.Skip("queries a DNS discovery origin")
	}
	id := exampleutil.Env("IROH_EXAMPLE_ENDPOINT_ID", "")
	if id == "" {
		t.Skip("set IROH_EXAMPLE_ENDPOINT_ID to a published endpoint id (and IROH_EXAMPLE_DNS_ORIGIN for a non-default origin)")
	}
	out, err := exampleutil.Capture(func() error { return run(nil) })
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	if strings.Contains(out, "no DNS endpoint records found") {
		t.Fatalf("endpoint %s has no records at the queried origin\n%s", id, out)
	}
	for _, want := range []string{
		"provenance: ",
		"direct paths: ",
		"relay paths: ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
	// The resolved record must describe the endpoint that was looked up.
	want, err := parseEndpointID(id)
	if err != nil {
		t.Fatalf("parse IROH_EXAMPLE_ENDPOINT_ID: %v", err)
	}
	if !strings.Contains(out, "endpoint: "+want.Z32()) {
		t.Errorf("resolved an endpoint other than %s\n%s", want.Z32(), out)
	}
}
