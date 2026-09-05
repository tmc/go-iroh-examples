package catalog

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite examples.json from the tree")

const (
	root   = "../.."
	module = "github.com/tmc/go-iroh-examples"
)

func load(t *testing.T) *Catalog {
	t.Helper()
	c, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Examples) == 0 {
		t.Fatal("no examples found")
	}
	return c
}

// TestConventions holds every example to the shape the rest of the repository
// assumes: a doc comment that names the command, and a flag whose default comes
// from the environment saying so in its usage, which is the only reason -h can
// list it.
func TestConventions(t *testing.T) {
	for _, e := range load(t).Examples {
		if e.Summary == "" {
			t.Errorf("%s: doc comment has no first sentence", e.Dir)
		}
		if strings.HasSuffix(e.Summary, ".") {
			t.Errorf("%s: summary %q keeps its period", e.Dir, e.Summary)
		}
		for _, f := range e.Flags {
			if f.Env == "" {
				continue
			}
			if !strings.Contains(f.Usage, "($"+f.Env+")") {
				t.Errorf("%s: -%s defaults to $%s but its usage does not name it: %q",
					e.Dir, f.Name, f.Env, f.Usage)
			}
		}
	}
}

// TestStandalone is the property the examples exist for: a main.go is a whole
// program. A reader copies one file, and it has to compile against go-iroh and
// the standard library alone, with nothing interesting hidden behind a name
// that exists only in this repository. Tests may import internal/exampleutil,
// because a reader copies the example and not its test.
func TestStandalone(t *testing.T) {
	for _, e := range load(t).Examples {
		for _, path := range e.Imports {
			if path == module || strings.HasPrefix(path, module+"/") {
				t.Errorf("%s/main.go imports %s; an example is standalone", e.Dir, path)
			}
		}
	}
}

// TestNaming keeps the directory names namespaced. `go install ./cmd/...`
// names each binary after its directory and Go offers no other way to name it,
// so a bare name would put a program called "doctor" or "dumbpipe" on the
// user's PATH — and this repository's README tells a reader to install Rust
// dumbpipe and run it alongside these examples.
func TestNaming(t *testing.T) {
	const prefix = "go-iroh-"
	for _, e := range load(t).Examples {
		if !strings.HasPrefix(e.Dir, prefix) {
			t.Errorf("%s does not start with %q", e.Dir, prefix)
		}
		if !nameRE.MatchString(e.Dir) {
			t.Errorf("%s is not a lowercase hyphenated name", e.Dir)
		}
	}
}

var nameRE = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// TestExamplesJSON keeps examples.json equal to what the tree and the README
// say together. The documentation site reads that file, so a stale one is how a
// published page comes to describe examples that no longer exist. Rewrite it
// with -run TestExamplesJSON -update.
func TestExamplesJSON(t *testing.T) {
	got, err := json.MarshalIndent(load(t), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')

	path := filepath.Join(root, "examples.json")
	if *update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Error("examples.json is stale: go test ./internal/catalog -run TestExamplesJSON -update")
	}
}

// TestNetworkTable checks that the README's table of examples needing the
// network holds exactly those whose tests skip without one. Load has already
// checked that the progression lists the tree, so what is left is the second
// table.
//
// Neither check reads the prose: the "Shows" column is an index of the APIs an
// example reaches for, which is worth writing by hand.
func TestNetworkTable(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	i := strings.Index(text, "## Examples that need the network")
	if i < 0 {
		t.Fatal("README has no network section")
	}
	network := map[string]bool{}
	for _, m := range rowRE.FindAllStringSubmatch(text[i:], -1) {
		network[m[1]] = true
	}
	for _, e := range load(t).Examples {
		if e.Network() && !network[e.Dir] {
			t.Errorf("%s skips without %v but is not in the network table", e.Dir, e.SkipEnv)
		}
		if !e.Network() && network[e.Dir] {
			t.Errorf("%s is in the network table but its test needs no environment", e.Dir)
		}
	}
}
