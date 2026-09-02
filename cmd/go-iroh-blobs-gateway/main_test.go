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
		// The Range header was honoured rather than the whole blob returned.
		"206 Partial Content",
		"bytes 6-10/29",
		// bytes 6-10 of "hello gateway over iroh blobs".
		`range body: "gatew"`,
		// The collection entry resolved by name under its root hash.
		"/note.txt 200 OK",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
