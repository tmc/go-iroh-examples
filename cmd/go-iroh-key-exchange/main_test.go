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
		"default server, default client: X25519MLKEM768, both ends agree: true",
		"default server, classical client: X25519, both ends agree: true",
		"pq-only server, default client: X25519MLKEM768, both ends agree: true",
		"pq-only server, classical client: refused: CRYPTO_ERROR 0x128",
		"tls: handshake failure",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}
