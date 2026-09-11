// Command go-iroh-keys derives an endpoint's identity from its secret key.
//
// An iroh endpoint is named by an Ed25519 public key. There is no registry and
// no certificate authority, so generating a key is the whole of creating an
// identity, and the [key.EndpointID] a peer dials is that public key. Two
// renderings of it recur through this repository and through iroh's tooling:
// [key.EndpointID.Short], the abbreviated hex that fits in a log line, and
// [key.EndpointID.Z32], the full z-base-32 form that appears on the wire, in
// tickets, and in DNS records.
//
// The same key signs. Because the endpoint ID is a public key, a peer that
// knows only the ID can verify anything the endpoint signed, with no key
// exchange of its own; go-iroh-gossip-kv builds its updates on that.
//
// The seed here is fixed so the printed identity is the same on every run.
// Endpoints that are not examples use [key.GenerateSecretKey], as go-iroh-addresses
// does.
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/tmc/go-iroh/key"
)

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(stdout io.Writer) error {
	seed := [key.SeedSize]byte{1, 2, 3}
	secret := key.NewSecretKey(seed)
	pub := secret.Public()
	id := pub.EndpointID()

	msg := []byte("hello go-iroh")
	sig := secret.Sign(msg)

	fmt.Fprintln(stdout, "endpoint id:", id.Short())
	fmt.Fprintln(stdout, "z32:", id.Z32())
	fmt.Fprintln(stdout, "signature valid:", pub.Verify(msg, sig) == nil)
	return nil
}
