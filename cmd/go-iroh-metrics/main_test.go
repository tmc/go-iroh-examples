package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	var buf bytes.Buffer
	err := run(&buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	// Exactly one connection is made, so every counter is pinned: a started
	// that never becomes an accepted, or a stray failure, is the regression
	// these counters exist to show.
	for _, want := range []string{
		"metrics hello",
		"client connects: started=1 accepted=1 failed=0",
		"server accepts: started=1 accepted=1 failed=0",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
