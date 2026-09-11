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
		"keepalive: 250ms",
		"max idle: 3s",
		"first\n",
		// The second reflection arrives after the connection has been idle for
		// longer than the keep-alive period, so this line is what shows the
		// keep-alive held the connection open.
		"second\n",
		"default direct idle: 15s",
		"default relay idle: 30s",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
