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
		// The Range header was honoured rather than the whole blob returned.
		"206 Partial Content",
		"bytes 6-10/29",
		// bytes 6-10 of "hello gateway over iroh blobs".
		`range body: "gatew"`,
		// The collection entry resolved by name under its root hash.
		"/note.txt 200 OK",
		// A raw ticket names one blob, and the gateway fetched it from the
		// address inside the ticket rather than its configured provider.
		`ticket blob: "hello gateway over iroh blobs"`,
		// A hash-sequence ticket names a collection, so the same route
		// answered with a one-entry index.
		"ticket index entries: 1",
		// That index entry, fetched through the second ticket route.
		`ticket entry: "hello gateway over iroh blobs"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
