package main

// Command-line parity with n0's dumbpipe.
//
// interop_test.go checks the protocol against a peer built from the dumbpipe
// crate. These tests check the program: the released dumbpipe binary on one
// end and this example, built as a binary, on the other, driven with the same
// command lines. Go's flag package accepts "--ipv4-addr" as readily as
// "-ipv4-addr", so the only argument the two differ in is -no-relay, which
// keeps the Go side off the public relays.

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// dumbpipeBin is the Rust dumbpipe binary. Build it with:
//
//	cargo install --locked --root interop/target/cli dumbpipe@0.39.0
func dumbpipeBin(t *testing.T) string {
	t.Helper()
	path := os.Getenv("IROH_EXAMPLE_RUST_DUMBPIPE_CLI")
	if path == "" {
		t.Skip("set IROH_EXAMPLE_RUST_DUMBPIPE_CLI to the Rust dumbpipe binary to run live interop")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("IROH_EXAMPLE_RUST_DUMBPIPE_CLI: %v", err)
	}
	return path
}

var buildOnce struct {
	sync.Once
	path string
	err  error
	out  []byte
}

// goBin builds this example once per test run.
func goBin(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "dumbpipe-interop")
		if err != nil {
			buildOnce.err = err
			return
		}
		buildOnce.path = filepath.Join(dir, "go-iroh-dumbpipe")
		buildOnce.out, buildOnce.err = exec.Command("go", "build", "-o", buildOnce.path, ".").CombinedOutput()
	})
	if buildOnce.err != nil {
		t.Fatalf("go build: %v\n%s", buildOnce.err, buildOnce.out)
	}
	return buildOnce.path
}

// An impl is one implementation of the dumbpipe command line.
type impl struct {
	name string
	cmd  func(ctx context.Context, args ...string) *exec.Cmd
}

func impls(t *testing.T) (goImpl, rustImpl impl) {
	rust, gobin := dumbpipeBin(t), goBin(t)
	goImpl = impl{"go", func(ctx context.Context, args ...string) *exec.Cmd {
		return exec.CommandContext(ctx, gobin, append(args, "-no-relay")...)
	}}
	rustImpl = impl{"rust", func(ctx context.Context, args ...string) *exec.Cmd {
		// dumbpipe logs to stdout, which here is the pipe.
		cmd := exec.CommandContext(ctx, rust, args...)
		cmd.Env = append(os.Environ(), "RUST_LOG=off")
		return cmd
	}}
	return goImpl, rustImpl
}

// pairs returns both directions: each implementation listening for the other.
func pairs(t *testing.T) [][2]impl {
	g, r := impls(t)
	return [][2]impl{{g, r}, {r, g}}
}

// startListener starts a listening command and returns the ticket it prints.
// Both implementations print it on stderr as the last word of a line that
// starts "dumbpipe connect".
func startListener(t *testing.T, cmd *exec.Cmd) string {
	t.Helper()
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	tickets := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			line := sc.Text()
			t.Logf("listener: %s", line)
			if f := strings.Fields(line); len(f) > 1 && f[0] == "dumbpipe" && strings.HasPrefix(f[1], "connect") {
				select {
				case tickets <- f[len(f)-1]:
				default:
				}
			}
		}
	}()
	select {
	case ticket := <-tickets:
		return ticket
	case <-time.After(30 * time.Second):
		t.Fatal("listener printed no ticket")
		return ""
	}
}

var loopback = []string{"--ipv4-addr", "127.0.0.1:0"}

// TestInteropCLIStdio pipes stdin to stdout in both directions at once, with
// each implementation listening for the other, under dumbpipe's ALPN and
// under a custom one.
func TestInteropCLIStdio(t *testing.T) {
	for _, p := range pairs(t) {
		for _, alpn := range [][]string{nil, {"--custom-alpn", "utf8:MYAPPV0"}} {
			l, c := p[0], p[1]
			name := l.name + "-listens/" + c.name + "-connects"
			if alpn != nil {
				name += "/custom-alpn"
			}
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()

				listener := l.cmd(ctx, append([]string{"listen"}, append(loopback, alpn...)...)...)
				var lout bytes.Buffer
				listener.Stdout = &lout
				lin, err := listener.StdinPipe()
				if err != nil {
					t.Fatal(err)
				}
				ticket := startListener(t, listener)

				connector := c.cmd(ctx, append([]string{"connect"}, append(append(loopback, alpn...), ticket)...)...)
				connector.Stdin = strings.NewReader("from the connector\n")
				var cout bytes.Buffer
				connector.Stdout = &cout
				connector.Stderr = &logWriter{t: t, prefix: "connector: "}
				if err := connector.Start(); err != nil {
					t.Fatal(err)
				}
				io.WriteString(lin, "from the listener\n")
				lin.Close()
				if err := connector.Wait(); err != nil {
					t.Fatalf("connector: %v", err)
				}
				if err := listener.Wait(); err != nil {
					t.Fatalf("listener: %v", err)
				}
				if got, want := cout.String(), "from the listener\n"; got != want {
					t.Errorf("connector received %q, want %q", got, want)
				}
				if got, want := lout.String(), "from the connector\n"; got != want {
					t.Errorf("listener received %q, want %q", got, want)
				}
			})
		}
	}
}

// TestInteropCLITCP tunnels a TCP echo service: listen-tcp forwards to it,
// connect-tcp accepts local connections and carries each over iroh.
func TestInteropCLITCP(t *testing.T) {
	for _, p := range pairs(t) {
		l, c := p[0], p[1]
		t.Run(l.name+"-listens/"+c.name+"-connects", func(t *testing.T) {
			echo := echoServer(t, "tcp", "127.0.0.1:0")
			front := freePort(t)
			testForward(t,
				l.cmd(t.Context(), append([]string{"listen-tcp", "--host", echo}, loopback...)...),
				func(ticket string) *exec.Cmd {
					return c.cmd(t.Context(), append(append([]string{"connect-tcp", "--addr", front}, loopback...), ticket)...)
				},
				"tcp", front, 2)
		})
	}
}

// TestInteropCLIUnix is TestInteropCLITCP for Unix sockets.
func TestInteropCLIUnix(t *testing.T) {
	for _, p := range pairs(t) {
		l, c := p[0], p[1]
		t.Run(l.name+"-listens/"+c.name+"-connects", func(t *testing.T) {
			// Socket paths are limited to about 100 bytes, which t.TempDir
			// can exceed.
			dir, err := os.MkdirTemp("", "dp")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.RemoveAll(dir) })
			echo := echoServer(t, "unix", filepath.Join(dir, "echo.sock"))
			front := filepath.Join(dir, "front.sock")
			// Rust dumbpipe 0.39's connect-unix carries every local
			// connection as a stream on one iroh connection, but its
			// listen-unix serves one stream per connection and then
			// closes it, so only the first local connection works.
			n := 2
			if l.name == "rust" {
				n = 1
			}
			testForward(t,
				l.cmd(t.Context(), append([]string{"listen-unix", "--socket-path", echo}, loopback...)...),
				func(ticket string) *exec.Cmd {
					return c.cmd(t.Context(), append(append([]string{"connect-unix", "--socket-path", front}, loopback...), ticket)...)
				},
				"unix", front, n)
		})
	}
}

// testForward starts listener, then the connector for its ticket, then makes
// n connections through the tunnel at front and checks each is echoed.
func testForward(t *testing.T, listener *exec.Cmd, connector func(ticket string) *exec.Cmd, network, front string, n int) {
	t.Helper()
	ticket := startListener(t, listener)
	c := connector(ticket)
	c.Stderr = &logWriter{t: t, prefix: "connector: "}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- c.Wait() }()
	t.Cleanup(func() {
		c.Process.Kill()
		<-exited
	})
	for i := range n {
		msg := fmt.Sprintf("connection %d\n", i)
		conn := dialRetry(t, network, front, exited)
		if _, err := io.WriteString(conn, msg); err != nil {
			t.Fatal(err)
		}
		// Read the echo before half-closing. Rust dumbpipe 0.39's listen-tcp
		// and listen-unix drop the connection as soon as both directions
		// finish, which discards an echo still in flight.
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		got := make([]byte, len(msg))
		if _, err := io.ReadFull(conn, got); err != nil {
			t.Fatalf("connection %d: %v", i, err)
		}
		if string(got) != msg {
			t.Errorf("connection %d echoed %q, want %q", i, got, msg)
		}
		conn.(interface{ CloseWrite() error }).CloseWrite()
		rest, err := io.ReadAll(conn)
		conn.Close()
		if err != nil || len(rest) > 0 {
			t.Errorf("connection %d after half-close: read %q, %v; want EOF", i, rest, err)
		}
	}
}

// echoServer serves connections that echo what they read until EOF.
func echoServer(t *testing.T, network, addr string) string {
	t.Helper()
	ln, err := net.Listen(network, addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				io.Copy(conn, conn)
			}()
		}
	}()
	return ln.Addr().String()
}

// freePort returns a loopback TCP address nothing is listening on.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// dialRetry dials addr until the connector has started listening on it,
// giving up if the connector exits first.
func dialRetry(t *testing.T, network, addr string, exited chan error) net.Conn {
	t.Helper()
	deadline := time.After(30 * time.Second)
	for {
		conn, err := net.Dial(network, addr)
		if err == nil {
			return conn
		}
		select {
		case werr := <-exited:
			exited <- werr
			t.Fatalf("connector exited before listening on %s: %v", addr, werr)
		case <-deadline:
			t.Fatalf("dial %s: %v", addr, err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// logWriter sends a command's output to the test log.
type logWriter struct {
	t      *testing.T
	prefix string
}

func (w *logWriter) Write(p []byte) (int, error) {
	for line := range strings.SplitSeq(strings.TrimRight(string(p), "\n"), "\n") {
		w.t.Logf("%s%s", w.prefix, line)
	}
	return len(p), nil
}
