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
	// The uppercased line proves the request crossed the net.Conn in both
	// directions within the deadline; a deadline that fired instead would end
	// run with a timeout error.
	//
	// The half-close lines prove the second half: the assertion for CloseWrite
	// succeeds on a net.Conn from iroh, and the server saw the resulting EOF
	// rather than a connection error.
	for _, want := range []string{
		"DEADLINE EXAMPLE\n",
		"half-close supported: true",
		"peer half-closed after 0 more bytes",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
