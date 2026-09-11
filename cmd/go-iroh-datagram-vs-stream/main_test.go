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
	for _, want := range []string{
		"datagrams negotiated: true",
		"small fits in a datagram: true",
		"large fits in a datagram: false",
		"small sent via datagram (20 bytes)",
		"large sent via stream (80000 bytes)",
		"server received datagram (20 bytes)",
		"server received stream (80000 bytes)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
