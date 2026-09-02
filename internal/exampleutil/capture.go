package exampleutil

import (
	"io"
	"os"
)

// Capture runs f with os.Stdout replaced by a pipe and returns everything f
// wrote to it. It is how each example's self-test asserts on the output the
// README promises; nothing in an example needs it.
//
// Capture is not safe for concurrent use: it swaps a process-wide variable.
func Capture(f func() error) (string, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return "", err
	}
	saved := os.Stdout
	os.Stdout = w

	done := make(chan struct{})
	var buf []byte
	go func() {
		defer close(done)
		buf, _ = io.ReadAll(r)
	}()

	runErr := f()

	os.Stdout = saved
	closeErr := w.Close()
	<-done
	r.Close()

	if runErr != nil {
		return string(buf), runErr
	}
	return string(buf), closeErr
}
