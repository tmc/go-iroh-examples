package main

import (
	"fmt"

	"github.com/tmc/go-iroh/key"
)

func main() {
	seed := [key.SeedSize]byte{1, 2, 3}
	secret := key.NewSecretKey(seed)
	pub := secret.Public()
	id := pub.EndpointID()

	msg := []byte("hello go-iroh")
	sig := secret.Sign(msg)

	fmt.Println("endpoint id:", id.Short())
	fmt.Println("z32:", id.Z32())
	fmt.Println("signature valid:", pub.Verify(msg, sig) == nil)
}
