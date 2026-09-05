// Command go-iroh-key-exchange selects the TLS key-exchange groups an endpoint offers.
//
// Every iroh connection is a QUIC connection whose TLS 1.3 handshake
// authenticates both peers by endpoint ID. That handshake must also agree on a
// key-exchange group, and the choice decides how long the traffic stays secret:
// a classical X25519 exchange captured today can be opened later by a quantum
// computer, while the hybrid X25519MLKEM768 exchange cannot.
// [iroh.WithKeyExchangePolicy] chooses which groups the endpoint offers, and
// [iroh.Conn.KeyExchangeGroup] reports which one the handshake picked.
//
// The policy is not a peer allow-list. [iroh.KeyExchangePolicy] names four sets
// of groups — [iroh.KeyExchangeDefault], [iroh.KeyExchangeClassical],
// [iroh.KeyExchangePreferPQ], and [iroh.KeyExchangePQOnly] — and the only way it
// can refuse a peer is by sharing no group with it. Deciding which peers may
// connect at all is a different job, done by an incoming filter; see
// go-iroh-incoming-filter.
//
// The example dials two servers, one on the default policy and one on
// KeyExchangePQOnly, from clients on the default and the classical policy. Three
// of the four dials complete, and the printed group shows what each negotiated:
// the default policy prefers X25519MLKEM768 but keeps X25519 as a fallback, so a
// classical peer still connects to it. The fourth dial, classical to PQ-only,
// has no group in common and fails in the handshake. That failure arrives as an
// error from [iroh.Endpoint.Connect], not as a dropped connection later, so a
// caller sees it at the dial site and can report it. It matches
// [iroh.ErrTLSHandshakeFailure], which is how a program tells a policy mismatch
// from every other reason a dial can fail without reading error text.
//
// Choose KeyExchangePQOnly for a closed deployment where every peer is known to
// speak MLKEM and downgrade must be impossible; the cost is that an older peer
// cannot connect at all. Choose KeyExchangeClassical only to interoperate with a
// peer that has no MLKEM support. Otherwise leave the policy at its zero value.
//
// The application payload here is encoded with [postcard], go-iroh's
// Rust-compatible codec, rather than JSON. It is the same codec that carries
// tickets and iroh-docs entries, so an application that already links go-iroh
// gets a compact, byte-exact-with-Rust encoding without adding a dependency or a
// second wire format. postcard is not self-describing: both ends must agree on
// the fields and their order, which is why the encoding is small and why a
// mismatch is an error rather than a silently missing field.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/postcard"
)

const alpn = "go-iroh-examples/key-exchange/1"

// request is what a client sends: a value the server must echo back, so that a
// reply cannot be confused with a stale or empty one.
type request struct {
	Nonce uint64
}

// report is what a server sends back: the nonce and the key-exchange group the
// accepting side negotiated. Comparing it with the dialing side's group shows
// that both ends of a handshake see the same result.
type report struct {
	Nonce uint64
	Group string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// One server on the package default, one that requires post-quantum key
	// exchange. Nothing else differs between them.
	flexible, err := bind(ctx, iroh.WithALPNs(alpn))
	if err != nil {
		return err
	}
	defer flexible.Shutdown(ctx)
	go serve(ctx, flexible)

	strict, err := bind(ctx,
		iroh.WithALPNs(alpn),
		iroh.WithKeyExchangePolicy(iroh.KeyExchangePQOnly),
	)
	if err != nil {
		return err
	}
	defer strict.Shutdown(ctx)
	go serve(ctx, strict)

	cases := []struct {
		name   string
		server *iroh.Endpoint
		policy iroh.KeyExchangePolicy
	}{
		{"default server, default client", flexible, iroh.KeyExchangeDefault},
		{"default server, classical client", flexible, iroh.KeyExchangeClassical},
		{"pq-only server, default client", strict, iroh.KeyExchangeDefault},
		{"pq-only server, classical client", strict, iroh.KeyExchangeClassical},
	}
	for _, c := range cases {
		dialed, accepted, err := probe(ctx, c.server.Addr(), c.policy)
		if errors.Is(err, iroh.ErrTLSHandshakeFailure) {
			// The two policies share no group, so the peer sent TLS alert 40,
			// handshake_failure. Every other dial error is a real failure and
			// ends the run.
			fmt.Printf("%s: refused: no key exchange group in common\n", c.name)
			continue
		}
		if err != nil {
			return fmt.Errorf("%s: %w", c.name, err)
		}
		fmt.Printf("%s: %s, both ends agree: %v\n", c.name, dialed, dialed == accepted)
	}
	return nil
}

// probe dials addr from a fresh endpoint using policy and runs one exchange. It
// returns the key-exchange group as seen by the dialing side and as reported by
// the accepting side.
func probe(ctx context.Context, addr netaddr.EndpointAddr, policy iroh.KeyExchangePolicy) (dialed, accepted string, err error) {
	client, err := bind(ctx, iroh.WithKeyExchangePolicy(policy))
	if err != nil {
		return "", "", err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		return "", "", err
	}
	defer conn.CloseWithError(0, "")

	req, err := postcard.Marshal(request{Nonce: 0x5a5a})
	if err != nil {
		return "", "", err
	}
	// The exchange is bytes, not text; postcard output is binary.
	reply, err := exchange(ctx, conn, string(req))
	if err != nil {
		return "", "", err
	}
	var rep report
	if err := postcard.Unmarshal([]byte(reply), &rep); err != nil {
		return "", "", fmt.Errorf("decode report: %w", err)
	}
	if rep.Nonce != 0x5a5a {
		return "", "", fmt.Errorf("report nonce = %#x, want 0x5a5a", rep.Nonce)
	}
	return conn.KeyExchangeGroup(), rep.Group, nil
}

// serve answers requests on ep until ctx ends or ep shuts down.
func serve(ctx context.Context, ep *iroh.Endpoint) {
	for {
		conn, err := ep.Accept(ctx)
		if err != nil {
			return
		}
		go func() {
			_ = transform(ctx, conn, func(req string) string {
				return string(answer([]byte(req), conn.KeyExchangeGroup()))
			})
		}()
	}
}

// answer decodes a request and encodes the matching report. A malformed request
// yields no bytes, which the dialer reports as a decode failure rather than
// mistaking it for a reply.
func answer(req []byte, group string) []byte {
	var r request
	if err := postcard.Unmarshal(req, &r); err != nil {
		return nil
	}
	b, err := postcard.Marshal(report{Nonce: r.Nonce, Group: group})
	if err != nil {
		return nil
	}
	return b
}

// bind binds an endpoint to an ephemeral IPv6 loopback port, which keeps the
// example self-contained: no relay, no DNS, no network access. Options given by
// the caller are applied after the bind address, so they may override it.
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

// transform accepts one bidirectional stream, reads it to EOF, and writes back
// f applied to what it read. It is the server half of exchange.
func transform(ctx context.Context, conn *iroh.Conn, f func(string) string) error {
	s, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	b, err := io.ReadAll(s)
	if err != nil {
		return err
	}
	if _, err := s.Write([]byte(f(string(b)))); err != nil {
		return err
	}
	return s.Close()
}
