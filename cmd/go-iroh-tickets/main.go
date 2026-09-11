// Command go-iroh-tickets hands an address to a peer as a ticket.
//
// An endpoint ID names a peer but says nothing about how to reach it. A ticket
// is the normal way to close that gap out of band: it packs the ID together with
// the relay URLs and direct addresses the peer is currently reachable at into
// one base32 string that can be pasted into a chat window. Ticket strings are
// the same on both sides of the Rust/Go boundary, so a ticket printed here can
// be dialed by iroh's Rust tooling and the reverse.
//
// The example does the round trip — encode an address, hand the string over,
// decode it, and dial — and then shows the second half of the pattern: an
// application that needs to carry its own fields alongside the address wraps
// the ticket in its own envelope rather than inventing a new address format.
package main

import (
	"context"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strings"
	"time"

	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/iroh"
)

const (
	alpn           = "go-iroh-examples/tickets/1"
	envelopePrefix = "roomticket"
)

var base32NoPad = base32.StdEncoding.WithPadding(base32.NoPadding)

// envelope is an application's own out-of-band format. It carries whatever the
// application needs plus the endpoint ticket, unmodified.
type envelope struct {
	Room   string `json:"room"`
	Query  string `json:"query"`
	Ticket string `json:"ticket"`
}

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(stdout io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server, err := bind(ctx, iroh.WithALPNs(alpn))
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			return
		}
		_ = echo(ctx, conn)
	}()

	// This is the string a peer would be sent. It starts with "endpoint" and is
	// lowercase base32, so it survives a paste anywhere.
	ticket := endpointticket.Encode(server.Addr())
	fmt.Fprintln(stdout, "ticket prefix:", strings.HasPrefix(ticket, endpointticket.Kind))

	// The receiving side knows only the string.
	addr, err := endpointticket.Decode(ticket)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, "same endpoint:", addr.ID == server.ID())
	fmt.Fprintln(stdout, "addresses:", len(addr.Addrs()))

	client, err := bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		return err
	}
	defer conn.CloseWithError(0, "")

	reply, err := exchange(ctx, conn, "ticket hello")
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, "reply:", reply)

	// An application with its own fields to carry wraps the ticket instead of
	// replacing it, so the address stays in the one format every iroh
	// implementation already parses.
	wrapped, err := wrapTicket("room-7", "kind=photo tag=sunset", ticket)
	if err != nil {
		return err
	}
	room, query, unwrapped, err := unwrapTicket(wrapped)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, "room:", room)
	fmt.Fprintln(stdout, "query:", query)
	fmt.Fprintln(stdout, "envelope round trip:", unwrapped == ticket)
	return nil
}

// wrapTicket encodes room, query, and ticket as one prefixed base32 string, the
// same shape as a ticket so that it can be pasted the same way.
func wrapTicket(room, query, ticket string) (string, error) {
	b, err := json.Marshal(envelope{Room: room, Query: query, Ticket: ticket})
	if err != nil {
		return "", err
	}
	return envelopePrefix + strings.ToLower(base32NoPad.EncodeToString(b)), nil
}

// unwrapTicket reverses [wrapTicket], validating the ticket it carries.
func unwrapTicket(s string) (room, query, ticket string, err error) {
	rest, ok := strings.CutPrefix(s, envelopePrefix)
	if !ok {
		return "", "", "", fmt.Errorf("app envelope: missing %q prefix", envelopePrefix)
	}
	b, err := base32NoPad.DecodeString(strings.ToUpper(rest))
	if err != nil {
		return "", "", "", fmt.Errorf("app envelope: decode base32: %w", err)
	}
	var env envelope
	if err := json.Unmarshal(b, &env); err != nil {
		return "", "", "", fmt.Errorf("app envelope: decode json: %w", err)
	}
	if _, err := endpointticket.Decode(env.Ticket); err != nil {
		return "", "", "", fmt.Errorf("app envelope: parse ticket: %w", err)
	}
	return env.Room, env.Query, env.Ticket, nil
}

// bind binds an endpoint to an ephemeral IPv6 loopback port, so that the
// example is self-contained: no relay, no DNS, no network access. Options given
// by the caller are applied after the bind address and may override it.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
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

// echo accepts one bidirectional stream, reads it to EOF, and writes back what
// it read. It is the server half of exchange.
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
