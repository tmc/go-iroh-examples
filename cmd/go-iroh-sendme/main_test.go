package main

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// TestRunDemo sends a directory and receives it over loopback. The collection
// hash is fixed by the three files and their names, so it is the evidence that
// the collection was built the way sendme builds one.
func TestRunDemo(t *testing.T) {
	var buf bytes.Buffer
	err := run(nil, &buf)
	out := buf.String()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	for _, want := range []string{
		"imported directory photos, 61 B, hash 5f4f51f62ddbf573262350413348c4c905d5d10e5f87c72771c937948bcd569e",
		"sendme receive blob",
		"getting collection 5f4f51f62ddbf573262350413348c4c905d5d10e5f87c72771c937948bcd569e 3 files",
		"exporting to photos",
		`received photos/a.txt: "the first file\n"`,
		`received photos/trip/b.txt: "a file in a subdirectory\n"`,
		`received photos/trip/c/d.txt: "and one further down\n"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}

// TestRunUsage checks that an unusable command line is reported as such rather
// than acted on.
func TestRunUsage(t *testing.T) {
	for _, args := range [][]string{
		{"nonsense"},
		{"send"},                           // missing path
		{"receive", "one", "two"},          // too many arguments
		{"send", "-no-such-flag", "x"},     // unknown flag
		{"recv", "-format", "base32", "x"}, // unknown format
	} {
		if err := run(args, io.Discard); err != errUsage {
			t.Errorf("run(%q, io.Discard) = %v, want errUsage", args, err)
		}
	}
}

func TestParseTicketType(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want ticketType
	}{
		{"relay-and-addresses", relayAndAddresses},
		{"RelayAndAddresses", relayAndAddresses},
		{"id", idOnly},
		{"Id", idOnly},
		{"relay", relayOnly},
		{"addresses", addressesOnly},
	} {
		got, err := parseTicketType(tt.in)
		if err != nil || got != tt.want {
			t.Errorf("parseTicketType(%q) = %v, %v, want %v", tt.in, got, err, tt.want)
		}
	}
	if _, err := parseTicketType("everything"); err == nil {
		t.Error("parseTicketType accepted an unknown type")
	}
}

// TestExportPath checks that a collection name from the sender cannot place a
// file outside the directory it is received into.
func TestExportPath(t *testing.T) {
	for _, tt := range []struct {
		name string
		ok   bool
	}{
		{"a.txt", true},
		{"photos/trip/b.jpg", true},
		{"../escape", false},
		{"photos/../../escape", false},
		{"/absolute", false},
		{"photos//b", false},
		{`photos\b`, false},
		{".", false},
	} {
		_, err := exportPath("/root", tt.name)
		if (err == nil) != tt.ok {
			t.Errorf("exportPath(%q) error = %v, want ok=%v", tt.name, err, tt.ok)
		}
	}
}

func TestHumanBytes(t *testing.T) {
	for _, tt := range []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{1023, "1023 B"},
		{1024, "1.00 KiB"},
		{1536, "1.50 KiB"},
		{5 << 20, "5.00 MiB"},
	} {
		if got := humanBytes(tt.n); got != tt.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}
