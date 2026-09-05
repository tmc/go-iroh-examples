// Command go-iroh-public-endpoint binds a reachable endpoint and dials one.
//
// The other examples bind an ephemeral loopback port, which is all a demo
// running both halves in one process needs. Two machines need more, and this
// example is both halves of that: "listen" binds a server other machines can
// reach, and "connect" dials one given its address.
//
// A server that other machines dial needs a fixed UDP port, so the address
// survives a restart and a firewall rule can name it, and an address a dialer
// can be handed out of band. "listen" prints that address in pieces — endpoint
// ID, ALPN, direct paths, relay paths — because those are the fields "connect"
// takes as flags. For the same address as one pasteable string, see
// go-iroh-tickets.
//
// Those pieces are what a [netaddr.EndpointAddr] is made of: an endpoint ID,
// which names the peer and is the only part that authenticates it, plus the
// direct UDP addresses and relay URLs that say where to look for it. The ID
// comes from -peer-id, the coordinates from -peer-ip and -peer-relay, which is
// how addressing works before any discovery service is involved.
//
// Either coordinate alone is enough. A direct address is dialed straight; a
// relay URL is a rendezvous, and reaching a peer through one requires the
// dialing endpoint to speak to that relay too, which is why -peer-relay also
// puts the URL into [relay.ModeCustomURLs]. Passing both lets iroh race them
// and then upgrade to the direct path.
//
// [iroh.Bind] is direct-only by default: a UDP socket and no relay. On the
// listening side -live adds [relay.ModeDefault] and waits for
// [iroh.Endpoint.Online], so that the printed relay paths give a dialer a way
// in when the direct address sits behind a NAT. go-iroh-relay-online is that
// opt-in on its own.
//
// Without -serve "listen" prints the address and exits. With -serve it accepts
// connections and echoes each one, which is what "connect" expects to find on
// the other end.
//
//	go-iroh-public-endpoint listen [-port n] [-alpn v] [-serve] [-live]
//	go-iroh-public-endpoint connect [-peer-id id] [-peer-ip host:port] [-peer-relay url] [-alpn v]
//
// go-iroh-dns-resolve and go-iroh-pkarr-publish-resolve build the same address
// from an ID alone by asking a discovery service; go-iroh-tickets carries it as
// one pasteable ticket. Reach for those once the coordinates stop fitting on a
// command line.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strconv"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
)

const defaultALPN = "go-iroh-examples/public-endpoint/1"

// errUsage reports a command line the program cannot act on. Usage text is
// already on stderr by the time it is returned.
var errUsage = errors.New("usage")

const usageText = `usage:
  go-iroh-public-endpoint listen [-port n] [-alpn v] [-serve] [-live]
  go-iroh-public-endpoint connect [-peer-id id] [-peer-ip host:port] [-peer-relay url] [-alpn v]

listen binds a fixed UDP port on every IPv4 interface and prints the address a
dialer needs. connect takes those pieces as flags and echoes one message off
the listener.
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
		return usage()
	}
	switch args[0] {
	case "-h", "-help", "--help":
		return flag.ErrHelp
	case "listen":
		fs := flag.NewFlagSet("listen", flag.ContinueOnError)
		port := fs.Uint("port", envUint("IROH_EXAMPLE_PORT", 4433), "UDP port to bind on every IPv4 interface ($IROH_EXAMPLE_PORT)")
		alpn := fs.String("alpn", env("IROH_EXAMPLE_ALPN", defaultALPN), "ALPN to accept ($IROH_EXAMPLE_ALPN)")
		serve := fs.Bool("serve", envBool("IROH_EXAMPLE_SERVE", false), "keep accepting echo connections instead of exiting after printing the address ($IROH_EXAMPLE_SERVE)")
		live := fs.Bool("live", envBool("GO_IROH_LIVE_RELAY", false), "advertise a public relay and wait for relay connectivity ($GO_IROH_LIVE_RELAY)")
		if err := fs.Parse(args[1:]); err != nil {
			return help(err)
		}
		if fs.NArg() != 0 {
			return usage()
		}
		return listen(*port, *alpn, *serve, *live)
	case "connect":
		fs := flag.NewFlagSet("connect", flag.ContinueOnError)
		peerID := fs.String("peer-id", env("IROH_EXAMPLE_PEER_ID", ""), "endpoint id of the peer to dial, z32 or hex ($IROH_EXAMPLE_PEER_ID)")
		peerIP := fs.String("peer-ip", env("IROH_EXAMPLE_PEER_IP", ""), "direct UDP address of the peer, host:port ($IROH_EXAMPLE_PEER_IP)")
		peerRelay := fs.String("peer-relay", env("IROH_EXAMPLE_PEER_RELAY", ""), "relay URL the peer is reachable through ($IROH_EXAMPLE_PEER_RELAY)")
		alpn := fs.String("alpn", env("IROH_EXAMPLE_ALPN", defaultALPN), "ALPN to negotiate, matching the listener's ($IROH_EXAMPLE_ALPN)")
		if err := fs.Parse(args[1:]); err != nil {
			return help(err)
		}
		if fs.NArg() != 0 {
			return usage()
		}
		return connect(*peerID, *peerIP, *peerRelay, *alpn)
	default:
		return usage()
	}
}

func usage() error {
	fmt.Fprint(os.Stderr, usageText)
	return errUsage
}

// help distinguishes the one flag error that is not a mistake: -h is a request
// for the usage message, not a failure.
func help(err error) error {
	if errors.Is(err, flag.ErrHelp) {
		return flag.ErrHelp
	}
	return usage()
}

func listen(port uint, alpn string, serve, live bool) error {
	if port > 65535 {
		return fmt.Errorf("port %d out of range", port)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	opts := []iroh.Option{
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv4Unspecified(), uint16(port))),
		iroh.WithALPNs(alpn),
	}
	if live {
		opts = append(opts, iroh.WithRelayMode(relay.ModeDefault()))
	}

	ep, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return err
	}
	defer ep.Shutdown(ctx)

	if live {
		onlineCtx, cancelOnline := context.WithTimeout(ctx, 30*time.Second)
		err := ep.Online(onlineCtx)
		cancelOnline()
		if err != nil {
			return fmt.Errorf("connect to public relay map: %w", err)
		}
	}

	fmt.Println("endpoint id:", ep.ID().Z32())
	fmt.Println("alpn:", alpn)
	fmt.Println("direct paths:", ep.Addr().IPAddrs())
	fmt.Println("relay paths:", ep.Addr().RelayURLs())
	fmt.Println("local udp:", ep.LocalAddr())

	if !serve {
		fmt.Println("pass -serve or set IROH_EXAMPLE_SERVE=1 to keep serving echo connections")
		return nil
	}
	for {
		conn, err := ep.Accept(ctx)
		if err != nil {
			return err
		}
		go func() {
			_ = echo(ctx, conn)
		}()
	}
}

func connect(peerID, peerIP, peerRelay, alpn string) error {
	if peerID == "" || (peerIP == "" && peerRelay == "") {
		fmt.Println("pass -peer-id and -peer-ip or -peer-relay (or set IROH_EXAMPLE_PEER_ID, IROH_EXAMPLE_PEER_IP, IROH_EXAMPLE_PEER_RELAY)")
		return nil
	}

	id, err := parseEndpointID(peerID)
	if err != nil {
		return fmt.Errorf("parse peer id: %w", err)
	}
	addr := netaddr.NewEndpointAddr(id)
	var relayURLs []netaddr.RelayURL
	if peerIP != "" {
		ap, err := netip.ParseAddrPort(peerIP)
		if err != nil {
			return fmt.Errorf("parse peer ip: %w", err)
		}
		addr = addr.WithIP(ap)
	}
	if peerRelay != "" {
		u, err := netaddr.ParseRelayURL(peerRelay)
		if err != nil {
			return fmt.Errorf("parse peer relay: %w", err)
		}
		addr = addr.WithRelayURL(u)
		relayURLs = append(relayURLs, u)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := []iroh.Option{}
	if len(relayURLs) > 0 {
		opts = append(opts, iroh.WithRelayMode(relay.ModeCustomURLs(relayURLs...)))
	}
	client, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", addr.ID, err)
	}
	defer conn.CloseWithError(0, "")

	reply, err := exchange(ctx, conn, "public hello")
	if err != nil {
		return err
	}
	fmt.Println(reply)
	fmt.Println("remote:", conn.RemoteID().Short())
	return nil
}

// parseEndpointID accepts either printed form of an endpoint id.
// key.ParseEndpointID reads the hex form of key.EndpointID.String and the RFC
// 4648 base32 form upstream iroh prints; key.ParseEndpointIDZ32 reads the
// z-base-32 form of key.EndpointID.Z32 that these examples print.
//
// The two base32 flavours are both 52 characters and differ only in their
// alphabets, so neither can be recognized: roughly one z-base-32 id in five
// hundred is also valid RFC 4648 base32 and decodes to a different, equally
// well-formed id. The order below is therefore a choice and not a detection —
// z-base-32 first, because that is the form this repository prints. A program
// with one source of ids should call the one function that matches it.
func parseEndpointID(s string) (key.EndpointID, error) {
	if id, err := key.ParseEndpointIDZ32(s); err == nil {
		return id, nil
	}
	return key.ParseEndpointID(s)
}

// env returns the environment variable name, or def if it is unset or empty.
// The flags above use it for their defaults so that a flag and an
// IROH_EXAMPLE_ variable configure the same thing.
func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// envUint is env for an unsigned number, parsed by [strconv.ParseUint]. An
// unparseable value yields def, so that a mistyped variable still leaves -h
// able to print the usage that names it.
func envUint(name string, def uint) uint {
	v, err := strconv.ParseUint(env(name, ""), 10, 16)
	if err != nil {
		return def
	}
	return uint(v)
}

// envBool is env for a boolean. An unparseable value yields def.
func envBool(name string, def bool) bool {
	v, err := strconv.ParseBool(env(name, ""))
	if err != nil {
		return def
	}
	return v
}

// echo accepts one bidirectional stream, reads it to EOF, and writes it back.
func echo(ctx context.Context, conn *iroh.Conn) error {
	s, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	b, err := io.ReadAll(s)
	if err != nil {
		return err
	}
	if _, err := s.Write(b); err != nil {
		return err
	}
	return s.Close()
}

// exchange opens a bidirectional stream, writes msg, closes the write side, and
// reads the reply until EOF.
func exchange(ctx context.Context, conn *iroh.Conn, msg string) (string, error) {
	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return "", err
	}
	if _, err := s.Write([]byte(msg)); err != nil {
		return "", err
	}
	// Half-close: the peer reads to EOF and replies on the same stream.
	if err := s.CloseWrite(); err != nil {
		return "", err
	}
	b, err := io.ReadAll(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
