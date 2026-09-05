// Package exampleutil holds what the go-iroh examples' tests need and the
// examples themselves do not.
//
// Every example is a single main.go that a reader can copy, and it compiles
// against go-iroh and the standard library alone: nothing in cmd imports this
// package, and internal/catalog's TestStandalone keeps it that way. What is
// left here is test plumbing — reading the output an example printed, and the
// environment variables that decide whether a test can run at all.
//
// Nothing in this package is part of go-iroh's API.
package exampleutil

import (
	"os"
	"strconv"
)

// Env returns the environment variable name, or def if it is unset or empty.
// Tests use it to read the IROH_EXAMPLE_ variables that say what infrastructure
// is available.
func Env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// EnvBool is [Env] for a boolean, parsed by [strconv.ParseBool]. An
// unparseable value yields def.
func EnvBool(name string, def bool) bool {
	v, err := strconv.ParseBool(Env(name, ""))
	if err != nil {
		return def
	}
	return v
}
