package main

import (
	"strings"
	"testing"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
)

// TestRunDemo covers the no-argument path: both halves of the pipe in one
// process over loopback.
func TestRunDemo(t *testing.T) {
	out, err := exampleutil.Capture(func() error { return run(nil) })
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		"pipe hello",
		"bytes piped: 11",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}

// TestRunUsage checks that an unusable command line is reported as such rather
// than acted on.
func TestRunUsage(t *testing.T) {
	for _, args := range [][]string{
		{"nonsense"},
		{"connect"},                 // missing ticket
		{"connect", "one", "two"},   // too many arguments
		{"listen", "-no-such-flag"}, // unknown flag
	} {
		if err := run(args); err != errUsage {
			t.Errorf("run(%q) = %v, want errUsage", args, err)
		}
	}
}

// TestHandshake pins the dumbpipe handshake check, which is what makes a ticket
// from this program dialable by the Rust implementation.
func TestHandshake(t *testing.T) {
	if err := readHandshake(strings.NewReader(handshake)); err != nil {
		t.Errorf("readHandshake(%q) = %v, want nil", handshake, err)
	}
	if err := readHandshake(strings.NewReader("byebye")); err == nil {
		t.Error("readHandshake accepted a wrong handshake")
	}
	if err := readHandshake(strings.NewReader("hi")); err == nil {
		t.Error("readHandshake accepted a short handshake")
	}
}
