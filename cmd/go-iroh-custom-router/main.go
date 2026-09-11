// Command go-iroh-custom-router adds and removes protocols while it is running.
//
// [iroh.Router] fixes its handler map at [iroh.NewRouter]: the type exposes
// only Endpoint, IsShutdown, and Shutdown, so there is no way to register a
// protocol later or to stop serving one. When a program needs that, it writes
// its own accept loop; this example writes the smallest one that does. The
// router here is a few dozen lines over [iroh.Endpoint.Accept] — a handler map
// guarded by a mutex, and a loop that dispatches on [iroh.Conn.ALPN].
//
// The subtlety worth the example is that two different sets are at play, and
// only one of them is dynamic:
//
//   - The endpoint advertises a fixed set of ALPNs in the TLS handshake. It is
//     decided by [iroh.WithALPNs] at bind time. [iroh.Endpoint.SetALPNs] can
//     replace it, but not while an accept loop is running, and this router's
//     loop never stops — so for a running router the advertised set is frozen.
//   - The router dispatches on a subset that changes at runtime.
//
// The two failure modes look nothing alike, and both appear in the output:
//
//   - An ALPN the endpoint does not advertise is refused during negotiation.
//     The dial itself fails; [iroh.Endpoint.Connect] returns an error whose
//     text ends in "tls: no application protocol", the peer's TLS alert 120
//     (no_application_protocol) arriving as CRYPTO_ERROR 0x178. go-iroh exports
//     no sentinel for it, so recognizing it means matching that text.
//   - An advertised ALPN with no handler negotiates fine. The dial succeeds,
//     and the router then closes the connection with a code of its choosing.
//     That one is recognizable: [iroh.AsApplicationError] reports the code and
//     reason, here noHandlerCode and "no handler for alpn".
//
// A protocol removed from a live router therefore does not vanish from the
// wire; it becomes a connection that is accepted and then dropped. Advertise
// every ALPN the router might ever serve, and treat the handler map as the
// authority on which of them are served now.
//
// go-iroh-router-echo shows the built-in router, and go-iroh-manual-incoming the
// accept stages this loop collapses.
package main

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const (
	alpn1 = "go-iroh-examples/custom-router/1"
	alpn2 = "go-iroh-examples/custom-router/2"
	// alpn3 is never advertised, to show the other failure mode.
	alpn3 = "go-iroh-examples/custom-router/3"

	// noHandlerCode closes a connection whose ALPN the endpoint advertises but
	// the router does not currently serve.
	noHandlerCode = 1
)

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(stdout io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The advertised set is every ALPN this router might ever serve, even the
	// one it does not serve yet.
	server, err := bind(ctx, iroh.WithALPNs(alpn1, alpn2))
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	r := newRouter(server)
	r.handle(alpn1, echo)
	go r.serve(ctx)

	client, err := bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	// Serving alpn1 only.
	report(ctx, client, server.Addr(), alpn1, stdout)
	report(ctx, client, server.Addr(), alpn2, stdout)
	report(ctx, client, server.Addr(), alpn3, stdout)

	// Stop serving alpn1. It stays advertised, so the dial still gets through
	// the handshake and is dropped by the router instead.
	fmt.Fprintf(stdout, "remove %s: %v\n", short(alpn1), r.remove(alpn1))
	report(ctx, client, server.Addr(), alpn1, stdout)
	report(ctx, client, server.Addr(), alpn2, stdout)

	// Start serving alpn2, on the same running router.
	fmt.Fprintf(stdout, "add %s\n", short(alpn2))
	r.handle(alpn2, echo)
	report(ctx, client, server.Addr(), alpn1, stdout)
	report(ctx, client, server.Addr(), alpn2, stdout)

	return nil
}

// router dispatches accepted connections to the handler registered for their
// negotiated ALPN. Unlike [iroh.Router] its handler map may change while the
// accept loop is running.
type router struct {
	ep *iroh.Endpoint

	mu       sync.Mutex
	handlers map[string]iroh.ProtocolHandler
}

func newRouter(ep *iroh.Endpoint) *router {
	return &router{ep: ep, handlers: make(map[string]iroh.ProtocolHandler)}
}

// handle registers h for alpn, replacing any handler already there.
func (r *router) handle(alpn string, h iroh.ProtocolHandlerFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[alpn] = h
}

// remove stops serving alpn and reports whether a handler was registered.
// The endpoint keeps advertising alpn either way; see the package comment.
func (r *router) remove(alpn string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.handlers[alpn]
	delete(r.handlers, alpn)
	return ok
}

func (r *router) lookup(alpn string) (iroh.ProtocolHandler, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.handlers[alpn]
	return h, ok
}

// serve accepts connections until ctx is done or the endpoint closes.
func (r *router) serve(ctx context.Context) {
	for {
		conn, err := r.ep.Accept(ctx)
		if err != nil {
			return
		}
		h, ok := r.lookup(conn.ALPN())
		if !ok {
			// Negotiation already succeeded, so the only refusal left is a
			// close the dialer can read a code off.
			conn.CloseWithError(noHandlerCode, "no handler for alpn")
			continue
		}
		// Like [iroh.Router], the handler owns the connection: it runs for the
		// connection's lifetime and the loop moves on.
		go func() {
			if err := h.Accept(ctx, conn); err != nil {
				fmt.Fprintf(os.Stderr, "handler %s: %v\n", conn.ALPN(), err)
			}
		}()
	}
}

// report dials alpn and prints which of the three outcomes it got.
func report(ctx context.Context, client *iroh.Endpoint, addr netaddr.EndpointAddr, alpn string, stdout io.Writer) {
	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		if strings.Contains(err.Error(), "no application protocol") {
			fmt.Fprintf(stdout, "dial %s: refused during negotiation, not advertised\n", short(alpn))
			return
		}
		fmt.Fprintf(stdout, "dial %s: %v\n", short(alpn), err)
		return
	}
	defer conn.CloseWithError(0, "")

	reply, err := exchange(ctx, conn, "hello")
	if err == nil {
		fmt.Fprintf(stdout, "dial %s: served, echoed %q\n", short(alpn), reply)
		return
	}
	if app, ok := iroh.AsApplicationError(err); ok && app.Remote && app.Code == noHandlerCode {
		fmt.Fprintf(stdout, "dial %s: connected, then closed with code %d (%s)\n", short(alpn), app.Code, app.Reason)
		return
	}
	fmt.Fprintf(stdout, "dial %s: %v\n", short(alpn), err)
}

// short is the trailing version of an ALPN, which is what varies here.
func short(alpn string) string {
	if i := strings.LastIndex(alpn, "/"); i >= 0 {
		return "alpn" + alpn[i+1:]
	}
	return alpn
}

// bind binds an endpoint to an ephemeral IPv6 loopback port, which keeps the
// example self-contained: no relay, no DNS, no network access.
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
