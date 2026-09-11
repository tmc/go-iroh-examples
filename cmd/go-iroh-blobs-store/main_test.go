package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/tmc/go-iroh/blobs"
)

// TestRun runs the example against a store directory the test owns, so the
// output does not depend on IROH_EXAMPLE_DIR or on what an earlier run left
// behind, and then inspects that directory: the point of the example is what
// is on disk when the process is gone.
func TestRun(t *testing.T) {
	dir := t.TempDir()
	var buf bytes.Buffer
	err := run([]string{"-dir=" + dir}, &buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	keep := blobs.NewHash(keepData)
	remote := blobs.NewHash(remoteData)
	scratch := blobs.NewHash(scratchData)
	for _, want := range []string{
		"stored: " + keep.Short(),
		// The tag and its blob were read back from a second store opened on
		// the same directory, not from the one that wrote them.
		"reopened: " + keep.Short() + " still tagged keep",
		"downloaded: " + remote.Short(),
		// Exactly the one blob no tag names was reclaimed.
		"gc deleted: 1",
		"gc reclaimed: " + scratch.Short(),
		"untagged blob present: false",
		"tag keep: " + keep.Short() + " present=true",
		"tag remote: " + remote.Short() + " present=true",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}

	store, err := blobs.NewFSStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	tags, err := store.Tags()
	if err != nil {
		t.Fatalf("tags: %v", err)
	}
	var names []string
	for _, tag := range tags {
		names = append(names, tag.Name)
	}
	if got, want := strings.Join(names, ","), "keep,remote"; got != want {
		t.Errorf("tags after run = %q, want %q", got, want)
	}
	if _, err := store.Open(context.Background(), scratch); err == nil {
		t.Error("collected blob is still readable")
	}
}
