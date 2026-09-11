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
		"current: starting",
		"updated: ready",
		"stream: 0",
		"stream: 1",
		"stream: 2",
		"current addrs: 1",
		"first reply: first",
		"updated has external: true",
		"second reply: second",
		"stream addrs: 2",
		"stream addrs: 3",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
