package main

// Interoperability with n0's sendme.
//
// These tests run the Rust sendme binary against this example in both
// directions: Rust receiving what Go sends, and Go receiving what Rust sends.
// They live outside main_test.go because internal/catalog reads that file to
// decide what an example needs in order to run, and what these need is a Rust
// build: a property of the checkout rather than of the example.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"io/fs"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/key"
)

// sendmeBin is the Rust sendme binary. Build it with:
//
//	cargo install --locked --no-default-features --root interop/target/cli sendme@0.36.0
//
// The default "clipboard" feature makes "sendme send" read key presses from
// the terminal, and with no terminal it aborts right after printing its
// ticket (fixed upstream after 0.36.0 by n0-computer/sendme#134). The
// feature only adds that key handling; sending and receiving are unchanged.
func sendmeBin(t *testing.T) string {
	t.Helper()
	path := os.Getenv("IROH_EXAMPLE_RUST_SENDME")
	if path == "" {
		t.Skip("set IROH_EXAMPLE_RUST_SENDME to the Rust sendme binary to run live interop")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("IROH_EXAMPLE_RUST_SENDME: %v", err)
	}
	return path
}

// writeTree creates the directory both tests send. It holds an empty file, a
// file of one partial chunk, and one spanning several 16 KiB chunk groups and
// ending mid-chunk, so that the size proofs sendme asks for cover each shape.
func writeTree(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	big := make([]byte, 100_000)
	r := rand.NewChaCha8([32]byte{1})
	r.Read(big)
	files := map[string][]byte{
		"photos/empty":           nil,
		"photos/small.txt":       []byte("hello from the other side\n"),
		"photos/trip/large.bin":  big,
		"photos/trip/deep/z.txt": []byte("z\n"),
	}
	for name, body := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return files
}

// checkTree compares what was received with what was sent.
func checkTree(t *testing.T, dir string, want map[string][]byte) {
	t.Helper()
	got := map[string][]byte{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		if strings.HasPrefix(rel, ".sendme-") {
			t.Errorf("receiver left %s behind", rel)
			return nil
		}
		got[filepath.ToSlash(rel)], err = os.ReadFile(p)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range want {
		b, ok := got[name]
		switch {
		case !ok:
			t.Errorf("%s was not received", name)
		case !bytes.Equal(b, body):
			t.Errorf("%s: received %d bytes (sha256 %x), want %d (sha256 %x)",
				name, len(b), sha256.Sum256(b), len(body), sha256.Sum256(body))
		}
		delete(got, name)
	}
	for name := range got {
		t.Errorf("received unexpected %s", name)
	}
}

// TestInteropRustReceivesFromGo serves a directory with this example's sender
// and fetches it with "sendme receive". The Rust receiver asks for every
// file's size proof before any content, so this is the test of serveRequest.
func TestInteropRustReceivesFromGo(t *testing.T) {
	bin := sendmeBin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	src, dst := t.TempDir(), t.TempDir()
	want := writeTree(t, src)
	o := options{relay: "disabled", ipv4: "127.0.0.1:0", jobs: 2, ticketType: relayAndAddresses}
	var err error
	if o.secret, err = key.GenerateSecretKey(); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	s, err := startSend(ctx, filepath.Join(src, "photos"), filepath.Join(src, ".sendme-send-test"), o, &out)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	t.Logf("go sender:\n%s", out.String())

	cmd := exec.CommandContext(ctx, bin, "receive", "--relay", "disabled", "--no-progress", s.ticket.String())
	cmd.Dir = dst
	b, err := cmd.CombinedOutput()
	t.Logf("sendme receive:\n%s", b)
	if err != nil {
		t.Fatalf("sendme receive: %v", err)
	}
	checkTree(t, dst, want)
}

// TestInteropGoReceivesFromRust fetches a directory served by "sendme send"
// with this example's receiver.
func TestInteropGoReceivesFromRust(t *testing.T) {
	bin := sendmeBin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	src, dst := t.TempDir(), t.TempDir()
	want := writeTree(t, src)
	cmd := exec.CommandContext(ctx, bin, "send", "--relay", "disabled", "--no-progress", "--magic-ipv4-addr", "127.0.0.1:0", "photos")
	cmd.Dir = src
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		// SIGINT is how sendme is told to stop; it deletes its store on the
		// way out.
		cmd.Process.Signal(syscall.SIGINT)
		cmd.Wait()
	}()
	var ticket string
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		t.Logf("sendme send: %s", sc.Text())
		if rest, ok := strings.CutPrefix(sc.Text(), "sendme receive "); ok {
			ticket = rest
			break
		}
	}
	if ticket == "" {
		t.Fatalf("sendme send printed no ticket: %v", sc.Err())
	}
	exited := make(chan struct{})
	go func() {
		for sc.Scan() {
		}
		close(exited)
		cancel()
	}()

	if bt, err := blobs.ParseTicket(ticket); err == nil {
		t.Logf("ticket addresses: %v", bt.Addr().IPAddrs())
	}
	o := options{relay: "disabled", jobs: 2, ipv4: "127.0.0.1:0"}
	if o.secret, err = key.GenerateSecretKey(); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := receive(ctx, ticket, dst, o, &out, &out); err != nil {
		select {
		case <-exited:
			t.Fatalf("sendme send exited during the transfer; was it built with the clipboard feature? (see sendmeBin)")
		default:
		}
		t.Fatalf("receive: %v\n%s", err, out.String())
	}
	t.Logf("go receiver:\n%s", out.String())
	checkTree(t, dst, want)
}
