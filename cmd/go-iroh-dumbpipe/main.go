// Command go-iroh-dumbpipe pipes stdin to stdout over an iroh connection.
//
// It speaks the wire protocol of n0's dumbpipe: ALPN "DUMBPIPEV0", a five-byte
// "hello" written by the dialer before any payload, then raw bytes in both
// directions until either side closes. A ticket printed by "go-iroh-dumbpipe listen"
// can be given to the Rust "dumbpipe connect", and the reverse, so this is the
// example to read when the question is cross-implementation compatibility.
//
// -alpn changes the negotiated protocol. Any value other than DUMBPIPEV0 skips
// the handshake and pipes bytes directly, which is the whole of what a
// netcat-over-iroh looks like: the pipe is not the interesting part, the ALPN
// and the ticket are.
//
// With no arguments it runs both halves in one process over loopback, so that
// "go run ./cmd/go-iroh-dumbpipe" is a complete demo. Otherwise:
//
//	go-iroh-dumbpipe listen [-alpn v] [-no-relay] [-bind addr:port] [-advertise addr:port] [-key file] [-ticket file]
//	go-iroh-dumbpipe connect [-alpn v] [-bind addr:port] <endpoint-ticket>
//
// The listener advertises a public relay by default, so the printed ticket is
// dialable from another machine. -no-relay keeps everything on loopback.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strings"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
)

const (
	// dumbpipeALPN and handshake are n0's dumbpipe wire protocol. The dialer
	// writes handshake before anything else; the listener refuses the stream if
	// the first bytes are something else.
	dumbpipeALPN = "DUMBPIPEV0"
	handshake    = "hello"
)

// errUsage reports a command line the program cannot act on. Usage text is
// already on stderr by the time it is returned.
var errUsage = errors.New("usage")

const usageText = `usage:
  50-dumbpipe listen [-alpn v] [-no-relay] [-bind addr:port] [-advertise addr:port] [-key file] [-ticket file]
  50-dumbpipe connect [-alpn v] [-bind addr:port] <endpoint-ticket>

With no arguments both halves run in one process over loopback.
The listen command advertises a public relay by default. Use -no-relay for a
direct-only local demo, or -bind and -advertise for a reachable direct UDP
address.
`

func main() {
	err := run(os.Args[1:])
	switch {
	case err == nil:
	case errors.Is(err, flag.ErrHelp):
		// The usage text was asked for, so it is the output, not an error.
		fmt.Print(usageText)
	case errors.Is(err, errUsage):
		os.Exit(1)
	default:
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return demo()
	}
	switch args[0] {
	case "-h", "-help", "--help":
		return flag.ErrHelp
	case "listen":
		fs := newFlagSet("listen")
		alpn := fs.String("alpn", dumbpipeALPN, "ALPN to accept")
		bind := fs.String("bind", exampleutil.Env("GO_IROH_DUMBPIPE_BIND_ADDR", "[::1]:0"), "UDP address to bind")
		advertise := fs.String("advertise", exampleutil.Env("GO_IROH_DUMBPIPE_ADVERTISE_ADDR", ""), "direct address to put in the printed ticket")
		relayFlag := fs.Bool("relay", true, "advertise a public relay address")
		noRelay := fs.Bool("no-relay", false, "disable public relay advertising")
		keyPath := fs.String("key", "", "endpoint secret key file, created if missing")
		ticketPath := fs.String("ticket", "", "file to write the printed ticket to")
		if err := fs.Parse(args[1:]); err != nil {
			return help(err)
		}
		if fs.NArg() != 0 {
			return usage()
		}
		useRelay := (*relayFlag && !*noRelay) || exampleutil.EnvBool("GO_IROH_LIVE_RELAY", false)
		return listen(listenConfig{
			alpn:       *alpn,
			bind:       *bind,
			advertise:  *advertise,
			keyPath:    *keyPath,
			ticketPath: *ticketPath,
			useRelay:   useRelay,
		})
	case "connect":
		fs := newFlagSet("connect")
		alpn := fs.String("alpn", dumbpipeALPN, "ALPN to negotiate")
		bind := fs.String("bind", exampleutil.Env("GO_IROH_DUMBPIPE_BIND_ADDR", "[::1]:0"), "UDP address to bind")
		if err := fs.Parse(args[1:]); err != nil {
			return help(err)
		}
		if fs.NArg() != 1 {
			return usage()
		}
		return connect(*alpn, *bind, fs.Arg(0))
	default:
		return usage()
	}
}

func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

func usage() error {
	fmt.Fprint(os.Stderr, usageText)
	return errUsage
}

// help distinguishes the one flag error that is not a mistake. The subcommand
// flag sets discard their own output, so -h reaches main as flag.ErrHelp and is
// answered there with the usage text on stdout.
func help(err error) error {
	if errors.Is(err, flag.ErrHelp) {
		return flag.ErrHelp
	}
	return usage()
}

// demo runs a listener and a dialer in one process, over loopback.
func demo() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	input := []byte("pipe hello\n")

	server, err := exampleutil.Bind(ctx, iroh.WithALPNs(dumbpipeALPN))
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	done := make(chan error, 1)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			done <- err
			return
		}
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			done <- err
			return
		}
		if err := readHandshake(stream); err != nil {
			done <- err
			return
		}
		_, err = io.Copy(os.Stdout, stream)
		done <- err
	}()

	client, err := exampleutil.Bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, exampleutil.Addr(server), dumbpipeALPN)
	if err != nil {
		return err
	}
	defer conn.Close()

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	if _, err := stream.Write([]byte(handshake)); err != nil {
		return err
	}
	if _, err := stream.Write(input); err != nil {
		return err
	}
	if err := stream.Close(); err != nil {
		return err
	}
	if err := <-done; err != nil {
		return err
	}
	fmt.Println("bytes piped:", len(input))
	return nil
}

type listenConfig struct {
	alpn       string
	bind       string
	advertise  string
	keyPath    string
	ticketPath string
	useRelay   bool
}

func listen(cfg listenConfig) error {
	ctx := context.Background()
	bindAddr, err := netip.ParseAddrPort(cfg.bind)
	if err != nil {
		return fmt.Errorf("parse bind address: %w", err)
	}
	opts := []iroh.Option{
		iroh.WithBindAddr(bindAddr),
		iroh.WithALPNs(cfg.alpn),
	}
	// A persistent key keeps the endpoint ID, and so the ticket's identity,
	// stable across restarts.
	if cfg.keyPath != "" {
		sk, err := loadOrCreateSecretKey(cfg.keyPath)
		if err != nil {
			return err
		}
		opts = append(opts, iroh.WithSecretKey(sk))
	}
	if cfg.useRelay {
		opts = append(opts, iroh.WithRelayMode(relay.ModeDefault()))
	}
	ep, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return err
	}
	defer ep.Shutdown(ctx)
	if cfg.useRelay {
		if err := online(ctx, ep); err != nil {
			return fmt.Errorf("connect to public relay map: %w", err)
		}
	}

	addr := ep.Addr()
	if cfg.advertise != "" {
		ap, err := netip.ParseAddrPort(cfg.advertise)
		if err != nil {
			return fmt.Errorf("parse advertise address: %w", err)
		}
		addr = netaddr.NewEndpointAddr(ep.ID()).WithIP(ap)
	}
	ticket := exampleutil.EncodeTicket(addr)
	if cfg.ticketPath != "" {
		if err := os.WriteFile(cfg.ticketPath, []byte(ticket+"\n"), 0o644); err != nil {
			return fmt.Errorf("write ticket: %w", err)
		}
	}
	fmt.Fprintf(os.Stderr, "Listening.\nGo:   go run ./cmd/50-dumbpipe connect %s\n", ticket)
	if cfg.alpn == dumbpipeALPN {
		fmt.Fprintf(os.Stderr, "Rust: dumbpipe connect %s\n", ticket)
	}
	if len(addr.RelayURLs()) == 0 && loopbackOnly(addr) {
		fmt.Fprintln(os.Stderr, "This ticket only contains loopback addresses. It works on this machine only; omit -no-relay or use -advertise for another machine.")
	}

	conn, err := ep.Accept(ctx)
	if err != nil {
		return err
	}
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	if cfg.alpn == dumbpipeALPN {
		if err := readHandshake(stream); err != nil {
			return err
		}
	}
	return exampleutil.Forward(os.Stdin, os.Stdout, stream)
}

func connect(alpn, bind, ticket string) error {
	ctx := context.Background()
	addr, err := exampleutil.DecodeTicket(ticket)
	if err != nil {
		return err
	}
	bindAddr, err := netip.ParseAddrPort(bind)
	if err != nil {
		return fmt.Errorf("parse bind address: %w", err)
	}
	opts := []iroh.Option{iroh.WithBindAddr(bindAddr)}
	if relays := addr.RelayURLs(); len(relays) > 0 {
		opts = append(opts, iroh.WithRelayMode(relay.ModeCustomURLs(relays...)))
	}
	ep, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return err
	}
	defer ep.Shutdown(ctx)
	// A relay-only ticket is dialable only once this endpoint has a relay home
	// of its own, which is what Online waits for.
	if len(addr.RelayURLs()) > 0 && len(addr.IPAddrs()) == 0 {
		if err := online(ctx, ep); err != nil {
			return err
		}
	}

	conn, err := ep.Connect(ctx, addr, alpn)
	if err != nil {
		return fmt.Errorf("connect to %s: %w%s", addr.ID, err, connectHint(addr))
	}
	defer conn.Close()
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	if alpn == dumbpipeALPN {
		if _, err := stream.Write([]byte(handshake)); err != nil {
			return err
		}
	}
	return exampleutil.Forward(os.Stdin, os.Stdout, stream)
}

func online(ctx context.Context, ep *iroh.Endpoint) error {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	return ep.Online(ctx)
}

// loadOrCreateSecretKey reads a hex-encoded seed from path, creating one if the
// file does not exist.
func loadOrCreateSecretKey(path string) (key.SecretKey, error) {
	b, err := os.ReadFile(path)
	if err == nil {
		sk, err := key.ParseSecretKey(strings.TrimSpace(string(b)))
		if err != nil {
			return key.SecretKey{}, fmt.Errorf("parse key file: %w", err)
		}
		return sk, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return key.SecretKey{}, fmt.Errorf("read key file: %w", err)
	}
	sk, err := key.GenerateSecretKey()
	if err != nil {
		return key.SecretKey{}, err
	}
	seed := sk.Bytes()
	if err := os.WriteFile(path, []byte(hex.EncodeToString(seed[:])+"\n"), 0o600); err != nil {
		return key.SecretKey{}, fmt.Errorf("write key file: %w", err)
	}
	return sk, nil
}

func loopbackOnly(addr netaddr.EndpointAddr) bool {
	ips := addr.IPAddrs()
	if len(ips) == 0 {
		return false
	}
	for _, ap := range ips {
		if !ap.Addr().IsLoopback() {
			return false
		}
	}
	return true
}

func connectHint(addr netaddr.EndpointAddr) string {
	if len(addr.RelayURLs()) == 0 && loopbackOnly(addr) {
		return "\nreceived a loopback-only ticket; it is usable only on the same machine. Start the listener without -no-relay for public relay connectivity, or with -bind/-advertise for a reachable direct UDP address."
	}
	return ""
}

func readHandshake(r io.Reader) error {
	buf := make([]byte, len(handshake))
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	if string(buf) != handshake {
		return fmt.Errorf("invalid dumbpipe handshake %q", string(buf))
	}
	return nil
}
