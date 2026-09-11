package main

import (
	"bytes"
	"encoding/binary"
	"strings"
	"sync"
	"testing"
)

func TestRun(t *testing.T) {
	var buf lockedBuffer
	err := run(&buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	// Both moves in both directions. A framing bug shows up here as a move
	// decoded from the wrong four bytes, not as an error.
	for _, want := range []string{
		"got move: {From:{X:4 Y:2} To:{X:4 Y:4}}",
		"received move: {From:{X:5 Y:7} To:{X:5 Y:6}}",
		"got move: {From:{X:3 Y:2} To:{X:3 Y:3}}",
		"received move: {From:{X:5 Y:8} To:{X:5 Y:7}}",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}

// TestFrame pins the framing itself: two moves written back to back must be
// read back as two moves, which is the property a raw stream does not have.
func TestFrame(t *testing.T) {
	var buf bytes.Buffer
	want := []move{
		{From: file{4, 2}, To: file{4, 4}},
		{From: file{5, 7}, To: file{5, 6}},
	}
	for _, mv := range want {
		if err := sendMove(&buf, mv); err != nil {
			t.Fatalf("sendMove(%+v): %v", mv, err)
		}
	}
	for _, mv := range want {
		got, err := recvMove(&buf)
		if err != nil {
			t.Fatalf("recvMove: %v", err)
		}
		if got != mv {
			t.Errorf("recvMove = %+v, want %+v", got, mv)
		}
	}

	// A length from the peer is checked before anything is allocated.
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], maxMessageSize+1)
	if _, err := readFrame(bytes.NewReader(hdr[:])); err == nil {
		t.Error("readFrame accepted a length over maxMessageSize")
	}
	if err := writeFrame(&buf, make([]byte, maxMessageSize+1)); err == nil {
		t.Error("writeFrame accepted a payload over maxMessageSize")
	}
}

// lockedBuffer is a [bytes.Buffer] that may be written from several goroutines
// at once. run prints from the protocol handler as well as from the dialing
// side, so the writer it is handed has to tolerate what os.Stdout does.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
