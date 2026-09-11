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
		"ticket prefix: true",
		"same endpoint: true",
		"addresses: 1",
		"reply: ticket hello",
		"room: room-7",
		"query: kind=photo tag=sunset",
		"envelope round trip: true",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
