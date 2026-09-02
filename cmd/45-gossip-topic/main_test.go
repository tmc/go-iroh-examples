package main

import (
	"strings"
	"testing"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
)

func TestRun(t *testing.T) {
	out, err := exampleutil.Capture(run)
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		"topic prefix: 2d96ebc44bd0619a",
		"A neighbors: B\n",
		"B neighbors: A C\n",
		"C neighbors: B\n",
		`A broadcast: "one broadcast, two receivers"`,
		`B received "one broadcast, two receivers" from A at round 0 over the swarm`,
		`C received "one broadcast, two receivers" from B at round 1 over the swarm`,
		"A has connection state for C: false",
		"A data messages sent=1 received=0",
		"B data messages sent=1 received=1",
		"C data messages sent=0 received=1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
