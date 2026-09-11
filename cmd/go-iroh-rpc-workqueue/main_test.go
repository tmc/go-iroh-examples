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
	// The jobs finish in whatever order the server answers them, so assert on
	// the three results rather than on the order they print in. Each is a
	// different task, which is what proves the request reached the handler
	// intact and the response came back matched to its own call.
	for _, want := range []string{
		"job 1: FIRST JOB",
		"job 2: boj dnoces",
		"job 3: 9 bytes",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
