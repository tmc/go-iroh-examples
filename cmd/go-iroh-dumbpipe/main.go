// Command go-iroh-dumbpipe pipes bytes between two machines over iroh, the way
// n0's dumbpipe does.
//
// It is dumbpipe's command line and dumbpipe's wire protocol: ALPN
// "DUMBPIPEV0", a five-byte "hello" written by the dialer before any payload,
// then raw bytes in both directions until either side closes. A ticket printed
// by one implementation is accepted by the other, in both directions, which
// interop_test.go checks against the Rust binary.
//
// The commands are dumbpipe's. listen and connect pipe stdin and stdout.
// listen-tcp forwards every incoming iroh connection to a TCP address, and
// connect-tcp accepts TCP connections and carries each over a new iroh
// connection, which together tunnel a TCP service between machines; the -unix
// pair does the same for Unix sockets. generate-ticket prints a ticket holding
// only the endpoint ID of IROH_SECRET, which stays valid across restarts.
//
// -custom-alpn negotiates another protocol ("utf8:" followed by text, or hex)
// and drops the handshake, so the pipe carries whatever that protocol speaks:
// netcat over iroh. The pipe is not the interesting part; the ALPN and the
// ticket are.
//
// With no arguments it runs both halves in one process over loopback, so that
// "go run ./cmd/go-iroh-dumbpipe" is a complete demo. Otherwise:
//
//	go-iroh-dumbpipe generate-ticket
//	go-iroh-dumbpipe listen [-recv-only] [flags]
//	go-iroh-dumbpipe connect [-recv-only] [flags] <ticket>
//	go-iroh-dumbpipe listen-tcp -host host:port [flags]
//	go-iroh-dumbpipe connect-tcp -addr addr:port [flags] <ticket>
//	go-iroh-dumbpipe listen-unix -socket-path path [flags]
//	go-iroh-dumbpipe connect-unix -socket-path path [flags] <ticket>
//
// The flags every command takes are dumbpipe's -ipv4-addr, -ipv6-addr,
// -custom-alpn, and -v, and one of this program's own: -no-relay, which
// keeps an endpoint off the public relays, for use without a network.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/tmc/go-iroh/endpointticket"
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

// onlineTimeout is how long a listener waits for its home relay before
// printing a ticket without one, as dumbpipe does.
const onlineTimeout = 5 * time.Second

// lingerTimeout bounds how long a finished pipe waits for its peer to close
// the connection. See linger.
const lingerTimeout = 3 * time.Second

// errUsage reports a command line the program cannot act on. Usage text is
// already on stderr by the time it is returned.
var errUsage = errors.New("usage")

const usageText = `usage:
  go-iroh-dumbpipe generate-ticket
  go-iroh-dumbpipe listen [-recv-only] [flags]
  go-iroh-dumbpipe connect [-recv-only] [flags] <ticket>
  go-iroh-dumbpipe listen-tcp -host host:port [flags]
  go-iroh-dumbpipe connect-tcp -addr addr:port [flags] <ticket>
  go-iroh-dumbpipe listen-unix -socket-path path [flags]
  go-iroh-dumbpipe connect-unix -socket-path path [flags] <ticket>

Flags: -ipv4-addr addr:port, -ipv6-addr addr:port, -custom-alpn utf8:<text>|<hex>,
-no-relay, -v. IROH_SECRET sets the secret key.
With no arguments both halves run in one process over loopback.
`

func main() {
	err := run(os.Args[1:], os.Stdout)
	switch {
	case err == nil:
	case errors.Is(err, flag.ErrHelp):
		// The usage text was asked for, so it is the output, not an error.
		fmt.Fprint(os.Stdout, usageText)
	case errors.Is(err, errUsage):
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// config is a parsed command line.
type config struct {
	cmd        string
	ipv4, ipv6 string
	customALPN string
	noRelay    bool
	verbose    bool
	recvOnly   bool
	host       string // listen-tcp
	addr       string // connect-tcp
	socketPath string // listen-unix, connect-unix
	ticket     netaddr.EndpointAddr
}

func run(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return demo(stdout)
	}
	cfg := config{cmd: args[0]}
	fs := flag.NewFlagSet(cfg.cmd, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.ipv4, "ipv4-addr", "", "IPv4 UDP address to bind")
	fs.StringVar(&cfg.ipv6, "ipv6-addr", "", "IPv6 UDP address to bind")
	fs.StringVar(&cfg.customALPN, "custom-alpn", "", "ALPN to use instead of DUMBPIPEV0, as utf8:<text> or hex; drops the handshake")
	fs.BoolVar(&cfg.noRelay, "no-relay", false, "do not use the public relays")
	fs.BoolVar(&cfg.verbose, "v", false, "also print a short ticket")
	wantTicket := false
	switch cfg.cmd {
	case "-h", "-help", "--help":
		return flag.ErrHelp
	case "generate-ticket":
		return generateTicket(stdout)
	case "listen":
		fs.BoolVar(&cfg.recvOnly, "recv-only", false, "ignore stdin")
	case "connect":
		fs.BoolVar(&cfg.recvOnly, "recv-only", false, "ignore stdin")
		wantTicket = true
	case "listen-tcp":
		fs.StringVar(&cfg.host, "host", "", "TCP address to forward connections to")
	case "connect-tcp":
		fs.StringVar(&cfg.addr, "addr", "", "TCP address to accept connections on")
		wantTicket = true
	case "listen-unix":
		fs.StringVar(&cfg.socketPath, "socket-path", "", "Unix socket to forward connections to")
	case "connect-unix":
		fs.StringVar(&cfg.socketPath, "socket-path", "", "Unix socket to accept connections on")
		wantTicket = true
	default:
		return usage()
	}
	pos, err := parseArgs(fs, args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return usage()
	}
	switch {
	case wantTicket && len(pos) != 1, !wantTicket && len(pos) != 0:
		return usage()
	case cfg.cmd == "listen-tcp" && cfg.host == "",
		cfg.cmd == "connect-tcp" && cfg.addr == "",
		strings.HasSuffix(cfg.cmd, "-unix") && cfg.socketPath == "":
		return usage()
	}
	if wantTicket {
		if cfg.ticket, err = endpointticket.Decode(pos[0]); err != nil {
			return fmt.Errorf("parse ticket: %w", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	switch cfg.cmd {
	case "listen":
		return listen(ctx, cfg, stdout)
	case "connect":
		return connect(ctx, cfg, stdout)
	case "listen-tcp", "listen-unix":
		return listenForward(ctx, cfg)
	case "connect-tcp":
		return connectTCP(ctx, cfg)
	default:
		return connectUnix(ctx, cfg)
	}
}

func usage() error {
	fmt.Fprint(os.Stderr, usageText)
	return errUsage
}

// parseArgs parses flags that may come before or after the positional
// arguments, as they may for dumbpipe, and returns the positional ones.
func parseArgs(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			return pos, nil
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

// alpn is the protocol to negotiate, and whether the dumbpipe handshake goes
// with it.
func (cfg config) alpn() (alpn string, handshake bool, err error) {
	if cfg.customALPN == "" {
		return dumbpipeALPN, true, nil
	}
	if text, ok := strings.CutPrefix(cfg.customALPN, "utf8:"); ok {
		return text, false, nil
	}
	b, err := hex.DecodeString(cfg.customALPN)
	if err != nil {
		return "", false, fmt.Errorf("parse -custom-alpn: %w", err)
	}
	return string(b), false, nil
}

// secretKey reads IROH_SECRET, or generates a key and prints it so that the
// next run can reuse the endpoint ID.
func secretKey() (key.SecretKey, error) {
	if s := os.Getenv("IROH_SECRET"); s != "" {
		sk, err := key.ParseSecretKey(s)
		if err != nil {
			return key.SecretKey{}, fmt.Errorf("invalid secret: %w", err)
		}
		return sk, nil
	}
	sk, err := key.GenerateSecretKey()
	if err != nil {
		return key.SecretKey{}, err
	}
	seed := sk.Bytes()
	fmt.Fprintf(os.Stderr, "using secret key %s\n", hex.EncodeToString(seed[:]))
	return sk, nil
}

func generateTicket(stdout io.Writer) error {
	sk, err := secretKey()
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, endpointticket.Encode(netaddr.NewEndpointAddr(sk.Public().EndpointID())))
	return nil
}

// bind creates the endpoint. With no bind address it binds a dual-stack socket
// on every interface, as dumbpipe does. A dialer whose ticket names relays
// uses those as its own, so that it can reach the listener through them.
func bind(ctx context.Context, cfg config, alpns ...string) (*iroh.Endpoint, error) {
	sk, err := secretKey()
	if err != nil {
		return nil, err
	}
	opts := []iroh.Option{iroh.WithSecretKey(sk)}
	if len(alpns) > 0 {
		opts = append(opts, iroh.WithALPNs(alpns...))
	}
	switch {
	case cfg.ipv4 != "" && cfg.ipv6 != "":
		return nil, errors.New("go-iroh binds one socket: give -ipv4-addr or -ipv6-addr, not both")
	case cfg.ipv4 != "" || cfg.ipv6 != "":
		ap, err := netip.ParseAddrPort(cfg.ipv4 + cfg.ipv6)
		if err != nil {
			return nil, fmt.Errorf("parse bind address: %w", err)
		}
		opts = append(opts, iroh.WithBindAddr(ap))
	}
	switch relays := cfg.ticket.RelayURLs(); {
	case cfg.noRelay:
	case len(relays) > 0:
		opts = append(opts, iroh.WithRelayMode(relay.ModeCustomURLs(relays...)))
	default:
		opts = append(opts, iroh.WithRelayMode(relay.ModeDefault()))
	}
	return iroh.Bind(ctx, opts...)
}

// bindListener binds a listening endpoint and prints its ticket to stderr, in
// the words dumbpipe uses, with the ticket last.
func bindListener(ctx context.Context, cfg config) (*iroh.Endpoint, error) {
	alpn, _, err := cfg.alpn()
	if err != nil {
		return nil, err
	}
	ep, err := bind(ctx, cfg, alpn)
	if err != nil {
		return nil, err
	}
	if !cfg.noRelay {
		octx, cancel := context.WithTimeout(ctx, onlineTimeout)
		if ep.Online(octx) != nil {
			fmt.Fprintln(os.Stderr, "Warning: Failed to connect to the home relay")
		}
		cancel()
	}
	addr := ticketAddr(ep)
	ticket, short := endpointticket.Encode(addr), endpointticket.Short(addr)
	switch cfg.cmd {
	case "listen":
		fmt.Fprintf(os.Stderr, "Listening. To connect, use:\ndumbpipe connect %s\n", ticket)
		if cfg.verbose {
			fmt.Fprintf(os.Stderr, "or:\ndumbpipe connect %s\n", short)
		}
	case "listen-tcp":
		fmt.Fprintf(os.Stderr, "Forwarding incoming requests to '%s'.\nTo connect, use e.g.:\ndumbpipe connect-tcp %s\n", cfg.host, ticket)
		if cfg.verbose {
			fmt.Fprintf(os.Stderr, "or:\ndumbpipe connect-tcp %s\n", short)
		}
	case "listen-unix":
		fmt.Fprintf(os.Stderr, "Forwarding incoming requests to '%s'.\nTo connect, use e.g.:\n", cfg.socketPath)
		fmt.Fprintf(os.Stderr, "dumbpipe connect-unix --socket-path /path/to/client.sock %s\n", ticket)
		fmt.Fprintf(os.Stderr, "dumbpipe connect-tcp --addr 127.0.0.1:8080 %s\n", ticket)
	}
	return ep, nil
}

// ticketAddr is the endpoint's address as a ticket should carry it. go-iroh
// leaves a socket bound to the unspecified address out of [iroh.Endpoint.Addr],
// so that socket is advertised at each of the machine's interface addresses,
// as dumbpipe's tickets are.
func ticketAddr(ep *iroh.Endpoint) netaddr.EndpointAddr {
	addr := netaddr.NewEndpointAddr(ep.ID())
	for _, u := range ep.Addr().RelayURLs() {
		addr = addr.WithRelayURL(u)
	}
	for _, ap := range ep.Addr().IPAddrs() {
		addr = addr.WithIP(ap)
	}
	if bound := ep.LocalAddr(); bound.Addr().IsUnspecified() {
		for _, ip := range interfaceAddrs(bound.Addr().Is4()) {
			addr = addr.WithIP(netip.AddrPortFrom(ip, bound.Port()))
		}
	}
	return addr
}

// interfaceAddrs lists the addresses of the interfaces that are up, leaving out
// link-local addresses, which are useless without a zone.
func interfaceAddrs(v4only bool) []netip.Addr {
	var out []netip.Addr
	ifaces, _ := net.Interfaces()
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, _ := ifc.Addrs()
		for _, a := range addrs {
			pfx, err := netip.ParsePrefix(a.String())
			if err != nil || pfx.Addr().IsLinkLocalUnicast() || v4only && !pfx.Addr().Is4() {
				continue
			}
			out = append(out, pfx.Addr())
		}
	}
	return out
}

// acceptPipe accepts the first stream of an incoming connection and checks
// the handshake.
func acceptPipe(ctx context.Context, conn *iroh.Conn, withHandshake bool) (*iroh.Stream, error) {
	s, err := conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	if withHandshake {
		if err := readHandshake(s); err != nil {
			s.CancelRead(0)
			s.CancelWrite(0)
			return nil, err
		}
	}
	return s, nil
}

// openPipe opens a stream on conn and writes the handshake. The dialer writes
// first because a QUIC stream does not exist for the peer until it carries
// data, and stdin may have nothing to say yet.
func openPipe(ctx context.Context, conn *iroh.Conn, withHandshake bool) (*iroh.Stream, error) {
	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	if withHandshake {
		if _, err := s.Write([]byte(handshake)); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// listen pipes stdin and stdout to the first stream of the first connection.
func listen(ctx context.Context, cfg config, stdout io.Writer) error {
	ep, err := bindListener(ctx, cfg)
	if err != nil {
		return err
	}
	defer ep.Shutdown(context.Background())
	_, hs, _ := cfg.alpn()
	for {
		conn, err := ep.Accept(ctx)
		if err != nil {
			return err
		}
		s, err := acceptPipe(ctx, conn, hs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error accepting stream: %v\n", err)
			continue
		}
		defer conn.Close()
		err = forward(stdin(cfg), stdout, s)
		linger(conn)
		return err
	}
}

// connect pipes stdin and stdout to a stream on a new connection.
func connect(ctx context.Context, cfg config, stdout io.Writer) error {
	alpn, hs, err := cfg.alpn()
	if err != nil {
		return err
	}
	ep, err := bind(ctx, cfg)
	if err != nil {
		return err
	}
	defer ep.Shutdown(context.Background())
	conn, err := dial(ctx, ep, cfg.ticket, alpn)
	if err != nil {
		return err
	}
	defer conn.Close()
	s, err := openPipe(ctx, conn, hs)
	if err != nil {
		return err
	}
	err = forward(stdin(cfg), stdout, s)
	linger(conn)
	return err
}

// linger waits for the peer to close conn, or for lingerTimeout. Closing a
// QUIC connection discards whatever it has not yet delivered, so the side that
// finishes first must not close under the last bytes it wrote. dumbpipe
// relies on the same thing: it exits once its own pipe is done, which closes
// the connection for the peer.
func linger(conn *iroh.Conn) {
	select {
	case <-conn.Context().Done():
	case <-time.After(lingerTimeout):
	}
}

func stdin(cfg config) io.Reader {
	if cfg.recvOnly {
		return strings.NewReader("")
	}
	return os.Stdin
}

// dial connects to addr. A ticket with only relay addresses is reachable only
// once this endpoint has a home relay of its own, which is what Online waits
// for.
func dial(ctx context.Context, ep *iroh.Endpoint, addr netaddr.EndpointAddr, alpn string) (*iroh.Conn, error) {
	if len(addr.RelayURLs()) > 0 && len(addr.IPAddrs()) == 0 {
		octx, cancel := context.WithTimeout(ctx, 45*time.Second)
		err := ep.Online(octx)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("connect to relay: %w", err)
		}
	}
	conn, err := ep.Connect(ctx, addr, alpn)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", addr.ID.Short(), err)
	}
	return conn, nil
}

// listenForward serves listen-tcp and listen-unix: each stream of each incoming
// connection is joined to a new connection to the local service. dumbpipe
// takes only the first stream of a connection, which is all its connect-tcp
// opens; accepting every stream also serves its connect-unix, which opens a
// stream per local client on one connection.
func listenForward(ctx context.Context, cfg config) error {
	network, target := "tcp", cfg.host
	if cfg.cmd == "listen-unix" {
		network, target = "unix", cfg.socketPath
	}
	ep, err := bindListener(ctx, cfg)
	if err != nil {
		return err
	}
	defer ep.Shutdown(context.Background())
	_, hs, _ := cfg.alpn()
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		conn, err := ep.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		wg.Go(func() {
			defer conn.Close()
			for {
				s, err := acceptPipe(ctx, conn, hs)
				if err != nil {
					// The peer closing the connection ends it; anything
					// else is worth a word.
					if conn.Context().Err() == nil && ctx.Err() == nil {
						fmt.Fprintf(os.Stderr, "error accepting stream: %v\n", err)
					}
					return
				}
				wg.Go(func() {
					if err := forwardLocal(ctx, network, target, s); err != nil {
						fmt.Fprintf(os.Stderr, "error handling connection: %v\n", err)
					}
				})
			}
		})
	}
}

// forwardLocal joins s to a new connection to the local service at target.
func forwardLocal(ctx context.Context, network, target string, s *iroh.Stream) error {
	var d net.Dialer
	local, err := d.DialContext(ctx, network, target)
	if err != nil {
		s.CancelRead(0)
		s.CancelWrite(0)
		return err
	}
	defer local.Close()
	return forward(local, local, s)
}

// connectTCP accepts TCP connections and carries each over a new iroh
// connection, as dumbpipe's connect-tcp does.
func connectTCP(ctx context.Context, cfg config) error {
	alpn, hs, err := cfg.alpn()
	if err != nil {
		return err
	}
	ep, err := bind(ctx, cfg)
	if err != nil {
		return err
	}
	defer ep.Shutdown(context.Background())
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.addr)
	if err != nil {
		return err
	}
	return acceptLocal(ctx, ln, func(local net.Conn) error {
		conn, err := dial(ctx, ep, cfg.ticket, alpn)
		if err != nil {
			return err
		}
		defer conn.Close()
		s, err := openPipe(ctx, conn, hs)
		if err != nil {
			return err
		}
		err = forward(local, local, s)
		linger(conn)
		return err
	})
}

// connectUnix accepts Unix socket connections and carries each over a new
// stream on one iroh connection, made before the socket is created, as
// dumbpipe's connect-unix does.
func connectUnix(ctx context.Context, cfg config) error {
	alpn, hs, err := cfg.alpn()
	if err != nil {
		return err
	}
	ep, err := bind(ctx, cfg)
	if err != nil {
		return err
	}
	defer ep.Shutdown(context.Background())
	if err := os.Remove(cfg.socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove existing socket file: %w", err)
	}
	conn, err := dial(ctx, ep, cfg.ticket, alpn)
	if err != nil {
		return err
	}
	defer conn.Close()
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "unix", cfg.socketPath)
	if err != nil {
		return err
	}
	return acceptLocal(ctx, ln, func(local net.Conn) error {
		s, err := openPipe(ctx, conn, hs)
		if err != nil {
			return err
		}
		return forward(local, local, s)
	})
}

// acceptLocal runs handle for each connection ln accepts until ctx is done,
// then closes ln, which for a Unix socket also removes the socket file.
func acceptLocal(ctx context.Context, ln net.Listener, handle func(net.Conn) error) error {
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		local, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		wg.Go(func() {
			defer local.Close()
			if err := handle(local); err != nil {
				fmt.Fprintf(os.Stderr, "error handling connection: %v\n", err)
			}
		})
	}
}

// demo runs a listener and a dialer in one process, over loopback.
func demo(stdout io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	input := []byte("pipe hello\n")

	server, err := bindLoopback(ctx, iroh.WithALPNs(dumbpipeALPN))
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
		stream, err := acceptPipe(ctx, conn, true)
		if err != nil {
			done <- err
			return
		}
		_, err = io.Copy(stdout, stream)
		done <- err
	}()

	client, err := bindLoopback(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, server.Addr(), dumbpipeALPN)
	if err != nil {
		return err
	}
	defer conn.Close()

	stream, err := openPipe(ctx, conn, true)
	if err != nil {
		return err
	}
	if _, err := stream.Write(input); err != nil {
		return err
	}
	if err := stream.CloseWrite(); err != nil {
		return err
	}
	if err := <-done; err != nil {
		return err
	}
	fmt.Fprintln(stdout, "bytes piped:", len(input))
	return nil
}

// bindLoopback binds an endpoint to an ephemeral IPv6 loopback port, which
// keeps the in-process demo self-contained: no relay, no DNS, no network
// access. Options given by the caller are applied after the bind address, so
// they may override it.
func bindLoopback(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
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

// forward copies src into stream and stream into dst until both directions
// end. When src is exhausted the stream's write side is closed, and when the
// stream is, dst's write side is, so a half-close travels end to end. It
// returns the first error from either direction.
//
// stream is an [io.ReadWriteCloser] rather than an [iroh.Stream] so that the
// half-close is expressed the way the standard library expresses it: probe for
// CloseWrite, and fall back to Close for something with no separate write side.
func forward(src io.Reader, dst io.Writer, stream io.ReadWriteCloser) error {
	errc := make(chan error, 2)
	go func() {
		_, err := io.Copy(stream, src)
		if closeErr := closeWrite(stream); err == nil {
			err = closeErr
		}
		errc <- err
	}()
	go func() {
		_, err := io.Copy(dst, stream)
		if c, ok := dst.(interface{ CloseWrite() error }); ok {
			c.CloseWrite()
		}
		errc <- err
	}()
	var firstErr error
	for range 2 {
		if err := <-errc; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// closeWrite ends c's write side without disturbing its read side, so that a
// peer reading to EOF sees one while the reply is still on its way back.
func closeWrite(c io.Closer) error {
	if cw, ok := c.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return c.Close()
}
