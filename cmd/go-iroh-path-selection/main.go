// Command go-iroh-path-selection replaces the policy that picks a connection's
// network path.
//
// Two decisions decide which path a connection ends up on. [iroh.WithRelayFirstDial]
// decides what Connect tries first: relay addresses before direct ones, with the
// direct addresses still registered as QNT candidates afterwards, so a connection
// that establishes through a relay can still migrate to a validated direct path.
// [iroh.WithPathSelector] decides where traffic goes once several paths are open.
//
// The selector is consulted whenever the set of paths or their round-trip times
// changes: it is handed the currently selected address and every candidate, and
// returns the one to use, or ok=false to leave the selection alone. The default,
// [iroh.BiasedRttPathSelector], prefers direct paths over relayed ones and the
// lowest round-trip time within a tier — which is why go-iroh-path-upgrade sees a
// connection leave the relay on its own.
//
// The relayFirst selector here inverts that: it pins traffic to the relay and
// never returns a direct address, so no amount of direct-path probing moves the
// connection off the relay. A deployment might do this to keep traffic on a path
// it can observe, or to hold a stable source address for a peer behind an ACL.
// Every decision is recorded and printed, next to what the default would have
// chosen from the same candidates.
//
// The relay is an in-process [relayserver.Server], so the example needs no
// network.
package main

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/netip"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
	"github.com/tmc/go-iroh/relayserver"
)

const alpn = "go-iroh-examples/path-selection/1"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	relayHTTP := httptest.NewServer(relayserver.New())
	defer relayHTTP.Close()
	relayURL, err := netaddr.ParseRelayURL(relayHTTP.URL)
	if err != nil {
		return err
	}
	mode := relay.ModeCustomURLs(relayURL)

	server, err := bind(ctx,
		iroh.WithALPNs(alpn),
		iroh.WithRelayMode(mode),
	)
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	// The client keeps its own policy; the server stays on the default, so the
	// two sides of the same connection can disagree about which path to use.
	selector := new(relayFirst)
	client, err := bind(ctx,
		iroh.WithRelayMode(mode),
		iroh.WithPathSelector(selector),
		iroh.WithRelayFirstDial(),
	)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	if err := server.Online(ctx); err != nil {
		return fmt.Errorf("server online: %w", err)
	}
	if err := client.Online(ctx); err != nil {
		return fmt.Errorf("client online: %w", err)
	}

	type acceptResult struct {
		conn *iroh.Conn
		err  error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		conn, err := server.Accept(ctx)
		accepted <- acceptResult{conn: conn, err: err}
	}()

	// The address carries both a relay URL and the server's direct socket, so
	// Connect has a real ordering choice to make. WithRelayFirstDial takes the
	// relay first and leaves the direct address as a QNT candidate.
	addr := netaddr.NewEndpointAddr(server.ID()).
		WithRelayURL(relayURL).
		WithIP(server.LocalAddr())
	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		return err
	}
	defer conn.CloseWithError(0, "")
	result := <-accepted
	if result.err != nil {
		return result.err
	}
	defer result.conn.CloseWithError(0, "")

	// Advertising each socket gives the endpoints a direct path to probe, the
	// same way go-iroh-path-upgrade does. Under the default selector that path
	// would win; here it only ever shows up as a candidate.
	server.AddExternalAddr(server.LocalAddr())
	client.AddExternalAddr(client.LocalAddr())
	waitForCandidate(ctx, conn, "ip", 10*time.Second)

	decisions := selector.decisions()
	fmt.Println("selector consulted:", len(decisions) > 0)
	fmt.Println("candidate kinds offered:", strings.Join(kinds(decisions), ","))
	fmt.Println("policy chose relay every time:", choseOnly(decisions, "relay"))
	for _, d := range distinct(decisions) {
		fmt.Printf("select: candidates=[%s] relayFirst=%s default=%s\n",
			strings.Join(d.candidates, " "), d.chose, d.byDefault)
	}
	fmt.Println("selected path kind:", selectedPathKind(conn.Paths()))
	return nil
}

// decision is one call into the selector, with what the default would have said.
type decision struct {
	candidates []string
	chose      string
	byDefault  string
}

// relayFirst is a [iroh.PathSelector] that pins traffic to a relay path,
// ignoring direct paths however fast they are. Select runs on the endpoint's
// path-selection loop and must not block: it records its decisions for the
// caller to print rather than printing them itself.
type relayFirst struct {
	mu   sync.Mutex
	seen []decision
}

func (s *relayFirst) Select(current netaddr.TransportAddr, candidates []iroh.PathCandidate) (netaddr.TransportAddr, bool) {
	// Lowest-sorting relay address, so repeated calls over the same candidates
	// give the same answer and the connection does not flap between relays.
	var chosen netaddr.TransportAddr
	for _, c := range candidates {
		if c.Addr.Network() != "relay" {
			continue
		}
		if chosen == nil || c.Addr.Compare(chosen) < 0 {
			chosen = c.Addr
		}
	}

	byDefault, _ := iroh.BiasedRttPathSelector{}.Select(current, candidates)
	s.record(candidates, chosen, byDefault)

	if chosen == nil {
		// No relay to pin to: leave the selection as it is rather than
		// falling back to a path this policy exists to avoid.
		return nil, false
	}
	return chosen, true
}

func (s *relayFirst) record(candidates []iroh.PathCandidate, chose, byDefault netaddr.TransportAddr) {
	d := decision{chose: addrString(chose), byDefault: addrString(byDefault)}
	for _, c := range candidates {
		d.candidates = append(d.candidates, c.Addr.String())
	}
	sort.Strings(d.candidates)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen = append(s.seen, d)
}

func (s *relayFirst) decisions() []decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]decision(nil), s.seen...)
}

func addrString(addr netaddr.TransportAddr) string {
	if addr == nil {
		return "none"
	}
	return addr.String()
}

// distinct drops repeats: the selector runs on every heartbeat, and the same
// candidate set answered the same way is not a new decision.
func distinct(ds []decision) []decision {
	var out []decision
	seen := make(map[string]bool)
	for _, d := range ds {
		key := strings.Join(d.candidates, " ") + "|" + d.chose + "|" + d.byDefault
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, d)
	}
	return out
}

// kinds returns the sorted transport kinds that appeared as candidates.
func kinds(ds []decision) []string {
	seen := make(map[string]bool)
	for _, d := range ds {
		for _, c := range d.candidates {
			if kind, _, ok := strings.Cut(c, ":"); ok {
				seen[kind] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// choseOnly reports whether every decision picked a path of the given kind.
func choseOnly(ds []decision, kind string) bool {
	for _, d := range ds {
		if !strings.HasPrefix(d.chose, kind+":") {
			return false
		}
	}
	return len(ds) > 0
}

// waitForCandidate waits until a path of the given kind is open on conn, so the
// selector has been offered one, or until d elapses. A direct path is not
// guaranteed to appear; the example reports the candidates it actually saw.
func waitForCandidate(ctx context.Context, conn *iroh.Conn, kind string, d time.Duration) {
	watch, err := conn.WatchPaths(ctx)
	if err != nil {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case paths, ok := <-watch:
			if !ok {
				return
			}
			for _, p := range paths {
				if p.HasAddr && p.Addr.Network() == kind {
					return
				}
			}
		case <-timer.C:
			return
		case <-ctx.Done():
			return
		}
	}
}

// selectedPathKind names the transport of the selected path: "relay", the
// network of a direct address, "unknown", or "none" if no path is selected.
func selectedPathKind(paths []iroh.PathInfo) string {
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

// bind binds an endpoint to an ephemeral IPv6 loopback port, which keeps the
// example self-contained: no DNS, no network access, and the only relay is the
// in-process one started above.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
}
