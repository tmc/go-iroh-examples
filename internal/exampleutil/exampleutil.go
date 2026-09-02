// Package exampleutil holds the plumbing the go-iroh examples share.
//
// Every example in this repository is a single main.go that a reader can copy.
// What is worth copying is the protocol the example teaches; what is not is
// binding a loopback endpoint, addressing it without a discovery service, or
// moving one string over one stream. Those live here so that each main.go is
// only the interesting part.
//
// Nothing in this package is part of go-iroh's API.
package exampleutil

import (
	"context"
	"io"
	"net/netip"
	"os"
	"strconv"
	"time"

	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

// Bind binds an endpoint to an ephemeral IPv6 loopback port. Additional options
// are applied after the bind address, so a caller may override it.
//
// Loopback binding keeps the examples self-contained: they need no relay, no
// DNS, and no network access.
func Bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
}

// Addr returns the address of a loopback endpoint: its ID plus the socket it is
// bound to. On a real network a peer would learn this from a ticket or a
// discovery service instead; see the ticket and discovery examples.
func Addr(ep *iroh.Endpoint) netaddr.EndpointAddr {
	return netaddr.NewEndpointAddr(ep.ID()).WithIP(ep.LocalAddr())
}

// Exchange opens a bidirectional stream, writes msg, closes the write side, and
// reads the reply until EOF.
func Exchange(ctx context.Context, conn *iroh.Conn, msg string) (string, error) {
	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return "", err
	}
	if _, err := s.Write([]byte(msg)); err != nil {
		return "", err
	}
	if err := s.Close(); err != nil {
		return "", err
	}
	b, err := io.ReadAll(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Transform accepts one bidirectional stream, reads it to EOF, and writes back
// f applied to what it read. It is the server half of [Exchange].
func Transform(ctx context.Context, conn *iroh.Conn, f func(string) string) error {
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

// Echo is Transform with the identity function.
func Echo(ctx context.Context, conn *iroh.Conn) error {
	return Transform(ctx, conn, func(s string) string { return s })
}

// Handler serves one [Transform] exchange per connection. The zero Handler
// echoes.
type Handler struct {
	// Transform maps a request to a response. If nil, the request is echoed.
	Transform func(string) string
}

// Accept implements [iroh.ProtocolHandler].
func (h Handler) Accept(ctx context.Context, conn *iroh.Conn) error {
	f := h.Transform
	if f == nil {
		f = func(s string) string { return s }
	}
	return Transform(ctx, conn, f)
}

// Forward copies stdin into stream and stream into stdout until both directions
// end, closing the stream's write side when stdin is exhausted. It returns the
// first error from either direction.
func Forward(stdin io.Reader, stdout io.Writer, stream io.ReadWriteCloser) error {
	errc := make(chan error, 2)
	go func() {
		_, err := io.Copy(stream, stdin)
		if closeErr := stream.Close(); err == nil {
			err = closeErr
		}
		errc <- err
	}()
	go func() {
		_, err := io.Copy(stdout, stream)
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

// EncodeTicket returns the Rust-compatible endpoint ticket for addr.
func EncodeTicket(addr netaddr.EndpointAddr) string {
	return endpointticket.Encode(addr)
}

// DecodeTicket parses an endpoint ticket produced by [EncodeTicket] or by the
// Rust tooling.
func DecodeTicket(s string) (netaddr.EndpointAddr, error) {
	return endpointticket.Decode(s)
}

// WaitReport polls ep for its first net report. It returns false if ctx ends
// before one is available.
func WaitReport(ctx context.Context, ep *iroh.Endpoint) (iroh.NetReport, bool) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if report, ok := ep.NetReport(); ok {
			return report, true
		}
		select {
		case <-ctx.Done():
			return iroh.NetReport{}, false
		case <-ticker.C:
		}
	}
}

// SelectedPathKind names the transport of the selected path: "relay", the
// network of a direct address, "unknown", or "none" if no path is selected.
func SelectedPathKind(paths []iroh.PathInfo) string {
	for _, p := range paths {
		if !p.Selected {
			continue
		}
		if p.Relayed {
			return "relay"
		}
		if p.HasAddr {
			return p.Addr.Network()
		}
		return "unknown"
	}
	return "none"
}

// Env returns the environment variable name, or def if it is unset or empty.
// Examples use it for flag defaults so that a flag and an IROH_EXAMPLE_
// variable configure the same thing.
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

// EnvDuration is [Env] for a duration, parsed by [time.ParseDuration]. An
// unparseable value yields def.
func EnvDuration(name string, def time.Duration) time.Duration {
	v, err := time.ParseDuration(Env(name, ""))
	if err != nil {
		return def
	}
	return v
}
