package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/pkarr"
)

func TestRun(t *testing.T) {
	var buf bytes.Buffer
	err := run(&buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		// The key is derived from a fixed seed, so it is the same every run.
		"key: d75a980182",
		"records: [relay=https://relay.example/ addrpath=1]",
		"verified: 2 TXT records",
		"tampered: signature rejected, unchecked parse still reads it",
		"relay payload: 206 bytes, reconstructs the packet",
		"supersedes: the later packet wins",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}

// TestSignatureCoversEveryByte checks the property the example asserts about
// one byte: the signature covers the whole packet, so flipping any bit of it
// makes the packet unverifiable. run only tampers with the last byte, which
// would also pass if the signature covered nothing but a prefix.
func TestSignatureCoversEveryByte(t *testing.T) {
	sk := key.NewSecretKey(seed)
	packet, err := pkarr.FromTxtStrings(sk, name, []string{"relay=https://relay.example/"}, 30)
	if err != nil {
		t.Fatal(err)
	}
	wire := packet.Bytes()
	for i := range wire {
		tampered := bytes.Clone(wire)
		tampered[i] ^= 0x80
		if _, err := pkarr.FromBytes(tampered); err == nil {
			t.Errorf("byte %d of %d: a tampered packet verified", i, len(wire))
		}
	}
}

// TestUncheckedParseKeepsRecords is the other half of that: unchecked parsing
// is useful only if it returns what the packet says, so a tool can show a
// record it has decided not to trust.
func TestUncheckedParseKeepsRecords(t *testing.T) {
	sk := key.NewSecretKey(seed)
	packet, err := pkarr.FromTxtStrings(sk, name, []string{"relay=https://relay.example/"}, 30)
	if err != nil {
		t.Fatal(err)
	}
	tampered := bytes.Clone(packet.Bytes())
	// Flip a signature byte, which leaves the key and the DNS packet intact.
	// The packet begins with the 32-byte public key, so the signature starts
	// at 32; touching a byte before that fails as a bad key instead.
	tampered[40] ^= 0x01
	if _, err := pkarr.FromBytes(tampered); !errors.Is(err, pkarr.ErrSignature) {
		t.Fatalf("FromBytes on a tampered signature: got %v, want %v", err, pkarr.ErrSignature)
	}
	got, err := pkarr.FromBytesUnchecked(tampered)
	if err != nil {
		t.Fatalf("FromBytesUnchecked: %v", err)
	}
	if want := []string{"relay=https://relay.example/"}; !equal(got.TxtRecords(name), want) {
		t.Errorf("records = %v, want %v", got.TxtRecords(name), want)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
