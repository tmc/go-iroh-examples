package catalog

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite examples.json from the tree")

const (
	root   = "../.."
	module = "github.com/tmc/go-iroh-examples"

	// goIrohModule is what the examples exist to show.
	goIrohModule = "github.com/tmc/go-iroh"
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

// TestStandalone is the property the examples exist for: an example is a whole
// program. A reader copies one file, and it has to compile against go-iroh and
// the standard library alone, with nothing interesting hidden behind a name
// that exists only in this repository. The tests hold to the same rule: an
// example's output is asserted by passing run a [bytes.Buffer], so a reader who
// copies the test gets a test that still builds.
func TestStandalone(t *testing.T) {
	for _, e := range load(t).Examples {
		for _, path := range e.Imports {
			if path == module || strings.HasPrefix(path, module+"/") {
				t.Errorf("%s/main.go imports %s; an example is standalone", e.Dir, path)
			}
		}
	}
	files, err := filepath.Glob(filepath.Join(root, "cmd", "*", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		f, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range f.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if path == module || strings.HasPrefix(path, module+"/") {
				t.Errorf("%s imports %s; an example is standalone", file, path)
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

// TestPackagesHaveExamples keeps every package go-iroh exports either reachable
// from an example or named in the README's "What is not here". The
// repository's claim is that it covers go-iroh, and a new package arriving in a
// dependency bump is the way that claim quietly stops being true: nothing else
// in the suite notices a package nobody imports.
//
// Declining is a real answer, so the README is the escape hatch rather than a
// list in this file: a package mentioned in backticks under "What is not here"
// passes, and the reason sits where a reader looking for the missing example
// will find it.
//
// Only whole packages are checked, not every exported name. Most of what a
// package exports is reached by using it, and an example that demonstrated
// every option would be a worse example.
func TestPackagesHaveExamples(t *testing.T) {
	out, err := exec.Command("go", "list", goIrohModule+"/...").Output()
	if err != nil {
		t.Skipf("go list %s/...: %v", goIrohModule, err)
	}
	imported := map[string]bool{}
	for _, e := range load(t).Examples {
		for _, path := range e.Imports {
			imported[path] = true
		}
	}
	excused, err := notHere(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, pkg := range strings.Fields(string(out)) {
		// Internal packages are not API, and go-iroh's own commands are
		// programs rather than something to build on.
		if strings.Contains(pkg, "/internal/") || strings.Contains(pkg, "/cmd/") {
			continue
		}
		if imported[pkg] || excused[path.Base(pkg)] {
			continue
		}
		t.Errorf("no example imports %s; either write one or say why not in the README's %q section", pkg, notHereHeading)
	}
}

const notHereHeading = "## What is not here"

// notHere returns the names the README's "What is not here" section mentions in
// backticks, which is how the repository declines to write an example for
// something rather than leaving it unexplained.
func notHere(readme string) (map[string]bool, error) {
	data, err := os.ReadFile(readme)
	if err != nil {
		return nil, err
	}
	text := string(data)
	i := strings.Index(text, notHereHeading)
	if i < 0 {
		return nil, fmt.Errorf("%s has no %q section", readme, notHereHeading)
	}
	section := text[i+len(notHereHeading):]
	if j := strings.Index(section, "\n## "); j >= 0 {
		section = section[:j]
	}
	out := map[string]bool{}
	for _, m := range codeRE.FindAllStringSubmatch(section, -1) {
		out[m[1]] = true
	}
	return out, nil
}

var codeRE = regexp.MustCompile("`([^`]+)`")
