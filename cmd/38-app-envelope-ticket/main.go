package main

import (
	"encoding/base32"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"strings"

	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

const envelopePrefix = "roomticket"

var base32NoPad = base32.StdEncoding.WithPadding(base32.NoPadding)

type envelope struct {
	Room   string `json:"room"`
	Query  string `json:"query"`
	Ticket string `json:"ticket"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	seed := [key.SeedSize]byte{3, 8, 38}
	secret := key.NewSecretKey(seed)
	addr := netaddr.NewEndpointAddr(secret.Public().EndpointID()).
		WithIP(netip.MustParseAddrPort("[::1]:4433"))

	ticket := endpointticket.New(addr)
	if endpointticket.Encode(addr) != ticket.String() {
		return fmt.Errorf("endpointticket Encode and Ticket.String disagree")
	}
	wrapped, err := wrapTicket("room-7", "kind=photo tag=sunset", ticket)
	if err != nil {
		return err
	}
	room, query, parsed, err := unwrapTicket(wrapped)
	if err != nil {
		return err
	}

	fmt.Println("room:", room)
	fmt.Println("query:", query)
	fmt.Println("ticket prefix:", strings.HasPrefix(parsed.String(), "endpoint"))
	fmt.Println("same endpoint:", parsed.Addr().ID == addr.ID)
	fmt.Println("addresses:", len(parsed.Addr().Addrs()))
	return nil
}

func wrapTicket(room, query string, ticket endpointticket.Ticket) (string, error) {
	env := envelope{
		Room:   room,
		Query:  query,
		Ticket: ticket.String(),
	}
	b, err := json.Marshal(env)
	if err != nil {
		return "", err
	}
	return envelopePrefix + strings.ToLower(base32NoPad.EncodeToString(b)), nil
}

func unwrapTicket(s string) (string, string, endpointticket.Ticket, error) {
	rest, ok := strings.CutPrefix(s, envelopePrefix)
	if !ok {
		return "", "", endpointticket.Ticket{}, fmt.Errorf("app envelope: missing %q prefix", envelopePrefix)
	}
	b, err := base32NoPad.DecodeString(strings.ToUpper(rest))
	if err != nil {
		return "", "", endpointticket.Ticket{}, fmt.Errorf("app envelope: decode base32: %w", err)
	}
	var env envelope
	if err := json.Unmarshal(b, &env); err != nil {
		return "", "", endpointticket.Ticket{}, fmt.Errorf("app envelope: decode json: %w", err)
	}
	ticket, err := endpointticket.Parse(env.Ticket)
	if err != nil {
		return "", "", endpointticket.Ticket{}, fmt.Errorf("app envelope: parse ticket: %w", err)
	}
	return env.Room, env.Query, ticket, nil
}
