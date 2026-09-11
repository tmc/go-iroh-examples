package main

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
	"time"
)

// The limited relay sends 2 MiB through a 1 MiB/s per-client limit. The first
// mebibyte is free — the token bucket starts full and its burst is the relay's
// maximum frame size — so the transfer cannot finish in less than a second.
// The assertion is a generous fraction of that floor: the point is that the
// limit is enforced, not what the exact throughput was. The same transfer
// without a limit takes tens of milliseconds.
const (
	payload  = 2 << 20
	rate     = 1 << 20
	minLimit = 700 * time.Millisecond
)

var limitedRE = regexp.MustCompile(`relayed, limited: (\S+)`)

func TestRun(t *testing.T) {
	var buf bytes.Buffer
	err := run(payload, rate, &buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		"limit slowed the relayed transfer: true",
		"limit applied to the direct transfer: false",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
	m := limitedRE.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("output has no limited duration\n%s", out)
	}
	limited, err := time.ParseDuration(m[1])
	if err != nil {
		t.Fatalf("parse %q: %v", m[1], err)
	}
	if limited < minLimit {
		t.Errorf("limited transfer took %s, want at least %s\n%s", limited, minLimit, out)
	}
}
