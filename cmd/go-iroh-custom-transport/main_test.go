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
		"advertised: custom 676f2d69726f68_736572766572",
		"advertised ip addresses: 0",
		"handshake reached the server: true",
		"path kind: custom",
		"reply: over the bus",
		"bus carried client to server: true",
		"bus carried server to client: true",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
