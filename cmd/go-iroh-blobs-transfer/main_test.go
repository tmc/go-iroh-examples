package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestRun serves the built-in payload. -file is passed empty rather than left
// to its default, which comes from IROH_EXAMPLE_FILE: the hash below is the
// hash of the built-in payload, so an exported IROH_EXAMPLE_FILE would serve a
// different file and fail a test that is not about files at all.
func TestRun(t *testing.T) {
	var buf bytes.Buffer
	err := run([]string{"-file="}, &buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		"bytes: 27",
		// The hash of the built-in payload. It is what the receiver asked for,
		// so printing it unchanged is the evidence that the verified transfer
		// returned that content and not something else.
		"blake3: 726c5e0e4a",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
