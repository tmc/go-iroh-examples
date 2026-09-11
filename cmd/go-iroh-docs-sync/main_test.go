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
		"ticket kind: doc",
		"ticket grants write: true",
		"same namespace: true",
		// One stream carries the difference both ways: bob sends the two
		// entries alice lacks and receives the three he lacks.
		"sync 1: sent=2 received=3",
		"alice after sync: 5 entries",
		"bob after sync: 5 entries",
		"fingerprints equal: true",
		// Bob ends up with alice's keys, with the same content hashes, and
		// both authors' "greeting" entries survive.
		"bob document:\n" +
			"  greeting     alice 16 bytes  e974fc1fc0\n" +
			"  greeting     bob   14 bytes  331875c560\n" +
			"  menu/coffee  alice  8 bytes  a55527015d\n" +
			"  menu/juice   bob   10 bytes  71f5ff7a99\n" +
			"  menu/tea     alice  9 bytes  a4649dd601\n",
		// The entries synced but the content did not: bob holds bytes only
		// for the two entries he wrote.
		"content on alice: 3 of 5 entries",
		"content on bob: 2 of 5 entries",
		"older entries replaced: 1",
		"alice after update: 5 entries",
		// The second sync exchanges the changed range, not the document.
		"sync 2: sent=1 received=2",
		"bob has alice's update: true",
		"signature verifies: true",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
