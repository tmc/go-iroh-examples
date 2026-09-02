package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
)

func TestRun(t *testing.T) {
	out, err := exampleutil.Capture(run)
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	// The document printed is the receiver's, which started empty. Every key
	// in it arrived over the sync protocol, so a partial sync fails here
	// rather than producing an error.
	want := []string{"State"}
	for i := range 5 {
		want = append(want, fmt.Sprintf("key-%d => %q", i, fmt.Sprintf("value-%d", i)))
	}
	for _, w := range want {
		if !strings.Contains(out, w) {
			t.Errorf("output missing %q\n%s", w, out)
		}
	}
}
