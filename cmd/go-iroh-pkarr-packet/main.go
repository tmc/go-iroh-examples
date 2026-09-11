// Command go-iroh-pkarr-packet builds, signs, and verifies a pkarr packet.
//
// go-iroh-pkarr-publish-resolve puts records on n0's pkarr relay and reads them
// back, and never sees the bytes in between. This example is the other half:
// the [pkarr] package is the codec those bytes are in, exported so that a
// program which is not an iroh endpoint — a relay, a directory, a test — can
// produce and check the same records.
//
// A pkarr packet is a DNS packet signed by the key it describes, so the key is
// both the name and the authority for it: anyone holding the public key can
// verify the records without trusting whoever handed them over.
//
// Four things happen here, and none of them touch the network. A packet is
// signed with [pkarr.FromTxtStrings] and read back with [pkarr.FromBytes],
// which verifies the signature; one byte of it is flipped, and parsing fails
// with [pkarr.ErrSignature] while [pkarr.FromBytesUnchecked] still returns the
// records, because "what does this say" and "should I believe it" are separate
// questions. The relay encoding is a third form of the same packet, and
// [pkarr.FromRelayPayload] reconstructs it from the public key. Finally two
// packets from one key are compared with MoreRecentThan, which is how a
// resolver decides that what it just fetched supersedes what it had.
package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/pkarr"
)

// The seed is fixed so the example prints the same key every time. A real
// program uses key.GenerateSecretKey, or loads a key it already has.
var seed = [32]byte{
	0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60,
	0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec, 0x2c, 0xc4,
	0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19,
	0x70, 0x3b, 0xac, 0x03, 0x1c, 0xae, 0x7f, 0x60,
}

// name is the label the records live under. iroh publishes its addresses under
// _iroh; a packet may carry any names its author chooses.
const name = "_iroh"

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(stdout io.Writer) error {
	sk := key.NewSecretKey(seed)
	values := []string{"relay=https://relay.example/", "addrpath=1"}

	// The packet is signed as it is built: the secret key is needed here and
	// nowhere else, and every later step works from the public key alone.
	packet, err := pkarr.FromTxtStrings(sk, name, values, 30)
	if err != nil {
		return fmt.Errorf("sign packet: %w", err)
	}
	wire := packet.Bytes()
	fmt.Fprintln(stdout, "key:", packet.PublicKey().Short())
	fmt.Fprintln(stdout, "signed:", len(wire), "bytes")
	fmt.Fprintln(stdout, "records:", packet.TxtRecords(name))

	// FromBytes verifies the signature before it returns anything, so a packet
	// that parses is a packet the named key vouched for.
	got, err := pkarr.FromBytes(wire)
	if err != nil {
		return fmt.Errorf("parse packet: %w", err)
	}
	if !bytes.Equal(got.Bytes(), wire) {
		return errors.New("packet does not round-trip through its own bytes")
	}
	if !got.PublicKey().Equal(sk.Public()) {
		return errors.New("parsed packet names a different key")
	}
	fmt.Fprintln(stdout, "verified:", len(got.AllTxtRecords()), "TXT records")

	// Flipping a byte of the records invalidates the signature over them.
	// Unchecked parsing still reads the packet, which is what a tool that
	// wants to show a bad record rather than hide it needs.
	tampered := bytes.Clone(wire)
	tampered[len(tampered)-1] ^= 0x01
	if _, err := pkarr.FromBytes(tampered); !errors.Is(err, pkarr.ErrSignature) {
		return fmt.Errorf("tampered packet: got %v, want %v", err, pkarr.ErrSignature)
	}
	if _, err := pkarr.FromBytesUnchecked(tampered); err != nil {
		return fmt.Errorf("unchecked parse of a tampered packet: %w", err)
	}
	fmt.Fprintln(stdout, "tampered: signature rejected, unchecked parse still reads it")

	// A pkarr relay stores the signature and the DNS packet without the public
	// key, because the key is the URL the payload was PUT to.
	payload := packet.RelayPayload()
	relayed, err := pkarr.FromRelayPayload(sk.Public(), payload)
	if err != nil {
		return fmt.Errorf("parse relay payload: %w", err)
	}
	if !bytes.Equal(relayed.Bytes(), wire) {
		return errors.New("relay payload does not reconstruct the packet")
	}
	fmt.Fprintln(stdout, "relay payload:", len(payload), "bytes, reconstructs the packet")

	// Timestamps are strictly monotonic, so a resolver can order two packets
	// from one key and keep the later one.
	newer, err := pkarr.FromTxtStrings(sk, name, []string{"relay=https://other.example/"}, 30)
	if err != nil {
		return fmt.Errorf("sign second packet: %w", err)
	}
	if !newer.MoreRecentThan(packet) {
		return errors.New("the second packet is not more recent than the first")
	}
	fmt.Fprintln(stdout, "supersedes: the later packet wins")
	return nil
}
