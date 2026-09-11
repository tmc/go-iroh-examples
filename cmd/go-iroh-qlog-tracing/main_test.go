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
	// The record counts vary from run to run, so the assertions are on what
	// does not: that both sinks produced a trace of the same connection, that
	// each holds QUIC events, and that neither holds the payload.
	for _, want := range []string{
		"reply: " + payload,
		"client trace side: client",
		"both ends share the connection id: true",
		"client trace has packet_sent: true",
		"server trace has packet_sent: true",
		"client trace contains the payload: false",
		"server trace contains the payload: false",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
