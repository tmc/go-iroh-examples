// Package catalog reads the example programs in cmd and reports what they are.
//
// The examples describe themselves. Each one is a directory holding a main.go
// whose doc comment opens "Command <directory> ", and the first sentence of
// that comment is the summary, and the flags and environment variables are the
// ones the program declares. Nothing here is a second copy of any of that.
//
// The reading order and the grouping are the exception: they are editorial, so
// they live in the README's progression and are read from it. A catalog exists
// so that the README, examples.json, and the documentation site are one
// description checked against the tree rather than four transcribed from it.
package catalog

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// A Group is one section of the reading order, from a "###" heading in the
// README's progression.
type Group struct {
	Key   string `json:"key"`
	Title string `json:"title"`
}

// A Flag is a command-line flag an example declares.
type Flag struct {
	Name  string `json:"name"`
	Usage string `json:"usage"`
	Env   string `json:"env,omitempty"` // environment variable the default is read from
}

// An Example is one program in cmd.
type Example struct {
	Dir     string `json:"dir"`
	Group   string `json:"group"`
	Summary string `json:"summary"` // first sentence of the doc comment, without the "Command <dir> " prefix
	Doc     string `json:"doc"`     // the whole doc comment, as Markdown
	Flags   []Flag `json:"flags,omitempty"`

	// SkipEnv lists the environment variables the example's test consults to
	// decide whether to skip. An example with none runs on loopback with no
	// configuration; one with any needs something outside the machine.
	SkipEnv []string `json:"skipEnv,omitempty"`

	// Imports are the packages main.go imports. An example imports nothing from
	// this module, so that a reader who copies the file has copied a program.
	Imports []string `json:"-"`
}

// Network reports whether the example needs a relay, DNS, a pkarr relay, or a
// peer outside this machine.
func (e Example) Network() bool { return len(e.SkipEnv) > 0 }

// A Catalog is the whole reading order: the groups in the order the README
// lists them, and the examples in the order they are read.
type Catalog struct {
	Groups   []Group   `json:"groups"`
	Examples []Example `json:"examples"`
}

// Load reads the README's progression and the examples it lists. It reports an
// error if the two disagree, which is the check that keeps the README from
// describing a tree that has moved on.
func Load(root string) (*Catalog, error) {
	groups, order, group, err := progression(filepath.Join(root, "README.md"))
	if err != nil {
		return nil, err
	}
	dirs, err := exampleDirs(filepath.Join(root, "cmd"))
	if err != nil {
		return nil, err
	}
	if err := agree(order, dirs); err != nil {
		return nil, err
	}

	c := &Catalog{Groups: groups}
	for _, name := range order {
		ex, err := scanDir(filepath.Join(root, "cmd", name), name)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		ex.Group = group[name]
		c.Examples = append(c.Examples, ex)
	}
	return c, nil
}

var (
	progressionRE = regexp.MustCompile(`(?m)^## Progression\s*$`)
	nextTopRE     = regexp.MustCompile(`(?m)^## `)
	sectionRE     = regexp.MustCompile(`(?m)^### (.+?)\s*$`)
	rowRE         = regexp.MustCompile("(?m)^\\| `([a-z0-9][a-z0-9-]*)` \\|")
	slugRE        = regexp.MustCompile(`[^a-z0-9]+`)
)

// progression reads the README's reading order: the groups under "## Progression"
// in the order they appear, the example directories in the order they are
// listed, and the group each one is listed under.
func progression(path string) ([]Group, []string, map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, nil, err
	}
	text := string(data)
	loc := progressionRE.FindStringIndex(text)
	if loc == nil {
		return nil, nil, nil, fmt.Errorf("%s has no \"## Progression\" section", path)
	}
	body := text[loc[1]:]
	if stop := nextTopRE.FindStringIndex(body); stop != nil {
		body = body[:stop[0]]
	}

	var groups []Group
	var order []string
	group := map[string]string{}
	sections := sectionRE.FindAllStringSubmatchIndex(body, -1)
	for i, s := range sections {
		title := body[s[2]:s[3]]
		key := slug(title)
		groups = append(groups, Group{Key: key, Title: title})
		end := len(body)
		if i+1 < len(sections) {
			end = sections[i+1][0]
		}
		for _, m := range rowRE.FindAllStringSubmatch(body[s[1]:end], -1) {
			if _, dup := group[m[1]]; dup {
				return nil, nil, nil, fmt.Errorf("%s lists %s twice", path, m[1])
			}
			order = append(order, m[1])
			group[m[1]] = key
		}
	}
	if len(groups) == 0 {
		return nil, nil, nil, fmt.Errorf("%s progression has no groups", path)
	}
	return groups, order, group, nil
}

// slug turns a group title into the key the documentation site uses.
func slug(title string) string {
	return strings.Trim(slugRE.ReplaceAllString(strings.ToLower(title), "-"), "-")
}

// exampleDirs lists the directories under cmd, sorted.
func exampleDirs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

// agree reports the first disagreement between the README's progression and the
// tree.
func agree(order []string, dirs []string) error {
	listed := map[string]bool{}
	for _, name := range order {
		listed[name] = true
	}
	present := map[string]bool{}
	for _, name := range dirs {
		present[name] = true
	}
	var missing, extra []string
	for _, name := range dirs {
		if !listed[name] {
			missing = append(missing, name)
		}
	}
	for _, name := range order {
		if !present[name] {
			extra = append(extra, name)
		}
	}
	switch {
	case len(missing) > 0 && len(extra) > 0:
		return fmt.Errorf("README lists %v, which are not in cmd, and omits %v, which are", extra, missing)
	case len(missing) > 0:
		return fmt.Errorf("README progression omits %v", missing)
	case len(extra) > 0:
		return fmt.Errorf("README progression lists %v, which are not in cmd", extra)
	}
	return nil
}

func scanDir(dir, name string) (Example, error) {
	ex := Example{Dir: name}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filepath.Join(dir, "main.go"), nil, parser.ParseComments)
	if err != nil {
		return Example{}, err
	}
	ex.Doc = strings.TrimSpace(file.Doc.Text())
	prefix := "Command " + name + " "
	if !strings.HasPrefix(ex.Doc, prefix) {
		return Example{}, fmt.Errorf("doc comment does not open %q", prefix)
	}
	ex.Summary = firstSentence(strings.TrimPrefix(ex.Doc, prefix))
	ex.Flags = scanFlags(file)
	ex.Imports = imports(file)

	// A test may be absent while an example is being written; treat that as no
	// skips rather than an error, so the catalog still reports the example.
	testFile, err := parser.ParseFile(fset, filepath.Join(dir, "main_test.go"), nil, 0)
	if err == nil {
		ex.SkipEnv = scanSkipEnv(testFile)
	} else if !os.IsNotExist(err) {
		return Example{}, err
	}
	return ex, nil
}

// imports returns the import paths of a file, in source order.
func imports(file *ast.File) []string {
	var out []string
	for _, spec := range file.Imports {
		if path, ok := stringLit(spec.Path); ok {
			out = append(out, path)
		}
	}
	return out
}

// firstSentence returns text up to the first period that ends a line or is
// followed by a space, with the doc comment's line wrapping removed.
func firstSentence(text string) string {
	text = strings.Join(strings.Fields(text), " ")
	for i := 0; i < len(text); i++ {
		if text[i] != '.' {
			continue
		}
		if i+1 == len(text) || text[i+1] == ' ' {
			return text[:i]
		}
	}
	return text
}

// scanFlags reports the flags declared on a flag.FlagSet, in declaration
// order. The environment variable is the one named in the usage string, which
// is where the examples record it for -h.
func scanFlags(file *ast.File) []Flag {
	var out []Flag
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 3 {
			return true
		}
		switch calleeName(call) {
		case "String", "Bool", "Int", "Uint", "Int64", "Uint64", "Float64", "Duration":
		default:
			return true
		}
		name, ok := stringLit(call.Args[0])
		if !ok {
			return true
		}
		usage, ok := stringLit(call.Args[2])
		if !ok {
			return true
		}
		out = append(out, Flag{Name: name, Usage: usage, Env: defaultEnv(call.Args[1])})
		return true
	})
	return out
}

// defaultEnv returns the environment variable a flag's default is read from,
// which is the flag's own declaration rather than a claim made about it.
func defaultEnv(def ast.Expr) string {
	call, ok := def.(*ast.CallExpr)
	if !ok || len(call.Args) == 0 {
		return ""
	}
	if !envCall(calleeName(call)) {
		return ""
	}
	name, _ := stringLit(call.Args[0])
	return name
}

// scanSkipEnv reports the environment variables a test consults, which are the
// ones a reader has to set for the example to do anything.
func scanSkipEnv(file *ast.File) []string {
	seen := map[string]bool{}
	var out []string
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		if !envCall(calleeName(call)) {
			return true
		}
		name, ok := stringLit(call.Args[0])
		if !ok || seen[name] {
			return true
		}
		seen[name] = true
		out = append(out, name)
		return true
	})
	sort.Strings(out)
	return out
}

// envCall reports whether a function of this name reads an environment
// variable named by its first argument. Examples define their own env, envBool,
// and envUint now that nothing is shared between them, so any name that opens
// "env" counts, as do the standard library's two.
func envCall(name string) bool {
	switch name {
	case "Getenv", "LookupEnv":
		return true
	}
	return strings.HasPrefix(strings.ToLower(name), "env")
}

// calleeName returns the identifier a call names, whether it is qualified by a
// package or a receiver ("flag.String", "fs.String") or not ("env").
func calleeName(call *ast.CallExpr) string {
	switch fun := call.Fun.(type) {
	case *ast.SelectorExpr:
		return fun.Sel.Name
	case *ast.Ident:
		return fun.Name
	}
	return ""
}

func stringLit(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	return s, true
}
