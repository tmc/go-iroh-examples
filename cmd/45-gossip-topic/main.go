// Command 45-gossip-topic broadcasts one message to a three-node gossip topic.
//
// Point-to-point connections do not scale to a group: sending the same bytes to
// every member means one stream per member, and every member has to know every
// other member first. [gossip.Gossip] replaces that with a topic. Endpoints
// subscribe to a 32-byte [gossip.TopicID], the subscribers organize themselves
// into a random overlay, and a message handed to [gossip.Topic.Broadcast] is
// relayed along that overlay until every subscriber has it. The sender does not
// address anyone, does not learn the membership, and does not scale its own
// work with the size of the group.
//
// This example makes the relaying visible by wiring three nodes into a chain.
// A is seeded with nothing, B is seeded with A's address, and C is seeded with
// B's address, so C has never heard of A. A then calls Broadcast once. B
// receives the message directly from A, and C receives the same message
// relayed by B, at PlumTree round 1 — the message reached a node its sender
// never had a connection to. The example prints that A still has no connection
// state for C afterwards.
//
// Everything the caller learns arrives on [gossip.Topic.Events]: NeighborUp and
// NeighborDown as the overlay changes shape, and Received for application
// messages. The event carries who relayed it ([gossip.Event.DeliveredFrom]),
// how far it travelled ([gossip.Event.Round]), and whether it came over the
// epidemic overlay or straight from a neighbor ([gossip.Event.Scope]).
// [gossip.Topic.Neighbors] is the same membership as a snapshot, for code that
// wants to ask rather than watch.
//
// Note that [gossip.Topic.Joined] and [gossip.Topic.Events] read the same
// subscription queue in v0.1.0: a running Events iterator consumes the
// NeighborUp that Joined is waiting for, so Joined — and therefore
// [gossip.Gossip.SubscribeAndJoin] — never returns. A program that watches
// events should wait for membership on the event stream, as this one does,
// or poll [gossip.Topic.IsJoined].
//
// The topic runs over the iroh-gossip ALPN, so each endpoint serves
// [gossip.Gossip.Handler] from a router and interoperates with Rust iroh-gossip
// peers on the same topic. Nothing here leaves the machine: the three endpoints
// are on IPv6 loopback and address each other directly.
package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/gossip"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// payload is the one message broadcast in this example.
const payload = "one broadcast, two receivers"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// A TopicID is 32 opaque bytes that every subscriber must agree on and
	// that nothing else derives meaning from, so hashing a name into it is the
	// usual way to pick one.
	topic := gossip.TopicID(sha256.Sum256([]byte("go-iroh-examples/gossip-topic/1")))

	nodes := make([]*node, 3)
	names := map[key.EndpointID]string{}
	for i, name := range []string{"A", "B", "C"} {
		n, err := newNode(ctx, name, names)
		if err != nil {
			return err
		}
		defer n.close(ctx)
		nodes[i] = n
	}
	a, b, c := nodes[0], nodes[1], nodes[2]

	// Subscribing is local: it allocates the topic state and starts the
	// overlay, but nobody is reachable until the node is given a peer to dial.
	for _, n := range nodes {
		if err := n.subscribe(ctx, topic); err != nil {
			return fmt.Errorf("subscribe %s: %w", n.name, err)
		}
	}
	fmt.Printf("topic prefix: %x\n", topic[:8])

	// The chain. Each JoinPeers dials one bootstrap peer; the overlay is what
	// the nodes build out of those two links.
	if err := b.topic.JoinPeers(ctx, []netaddr.EndpointAddr{exampleutil.Addr(a.ep)}); err != nil {
		return fmt.Errorf("seed B with A: %w", err)
	}
	if err := c.topic.JoinPeers(ctx, []netaddr.EndpointAddr{exampleutil.Addr(b.ep)}); err != nil {
		return fmt.Errorf("seed C with B: %w", err)
	}

	// Membership arrives as NeighborUp events. Waiting for the four links of
	// the chain also makes the run deterministic: the broadcast below cannot
	// race the overlay into existence.
	for _, w := range []struct {
		n    *node
		want string
	}{
		{a, "B"}, {b, "A"}, {b, "C"}, {c, "B"},
	} {
		if err := w.n.awaitNeighbor(ctx, w.want); err != nil {
			return err
		}
	}
	for _, n := range nodes {
		fmt.Printf("%s neighbors: %s\n", n.name, strings.Join(n.neighbors(), " "))
	}
	fmt.Println("A has connection state for C:", a.knows(c))

	// One call, addressed to no one.
	if err := a.topic.Broadcast(ctx, []byte(payload)); err != nil {
		return fmt.Errorf("broadcast: %w", err)
	}
	fmt.Printf("A broadcast: %q\n", payload)

	// Both other nodes get it. B is A's neighbor, so it is delivered at round
	// 0; C is not, so B relays it and C sees round 1.
	for _, n := range []*node{b, c} {
		ev, err := n.awaitMessage(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("%s received %q from %s at round %d over the %s\n",
			n.name, ev.Content, n.names[ev.DeliveredFrom], ev.Round, scope(ev.Scope))
	}
	fmt.Println("A has connection state for C:", a.knows(c))

	// The counters agree: A sent the message once and received nothing, B both
	// received and forwarded it, C only received it.
	for _, n := range nodes {
		m := n.g.Metrics()
		fmt.Printf("%s data messages sent=%d received=%d\n", n.name, m.MsgsDataSent, m.MsgsDataRecv)
	}
	return nil
}

// node is one endpoint subscribed to the topic. It keeps the membership and the
// received messages its event stream reported so that the rest of the program
// can wait on them.
type node struct {
	name  string
	names map[key.EndpointID]string // shared, written before any event arrives

	ep     *iroh.Endpoint
	router *iroh.Router
	g      *gossip.Gossip
	topic  *gossip.Topic

	mu   sync.Mutex
	up   map[string]bool
	seen chan gossip.Event
	// changed is closed and replaced whenever up changes.
	changed chan struct{}
}

// newNode binds a loopback endpoint and serves gossip on it. Gossip is an
// ordinary iroh protocol: it is reachable because the router answers its ALPN.
func newNode(ctx context.Context, name string, names map[key.EndpointID]string) (*node, error) {
	ep, err := exampleutil.Bind(ctx)
	if err != nil {
		return nil, err
	}
	g := gossip.NewGossip(ep)
	router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{gossip.ALPN: g.Handler()}, nil)
	if err != nil {
		ep.Shutdown(ctx)
		return nil, err
	}
	names[ep.ID()] = name
	return &node{
		name:    name,
		names:   names,
		ep:      ep,
		router:  router,
		g:       g,
		up:      make(map[string]bool),
		seen:    make(chan gossip.Event, 8),
		changed: make(chan struct{}),
	}, nil
}

func (n *node) close(ctx context.Context) {
	if n.topic != nil {
		n.topic.Close()
	}
	n.g.Shutdown(ctx)
	n.router.Shutdown(ctx)
}

// subscribe joins the topic with no bootstrap peers and starts draining the
// topic's event stream in the background.
func (n *node) subscribe(ctx context.Context, topic gossip.TopicID) error {
	t, err := n.g.Subscribe(ctx, topic, nil)
	if err != nil {
		return err
	}
	n.topic = t
	go n.consume()
	return nil
}

// consume records membership changes and received messages until the topic
// closes. Every gossip program has a loop of this shape.
func (n *node) consume() {
	for ev, err := range n.topic.Events() {
		if err != nil {
			return
		}
		switch ev.Kind {
		case gossip.NeighborUp:
			n.setNeighbor(n.names[ev.Peer], true)
		case gossip.NeighborDown:
			n.setNeighbor(n.names[ev.Peer], false)
		case gossip.Received:
			select {
			case n.seen <- ev:
			default:
			}
		}
	}
}

func (n *node) setNeighbor(name string, up bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.up[name] = up
	close(n.changed)
	n.changed = make(chan struct{})
}

// neighbors returns the sorted names of the node's current direct neighbors, as
// its event stream reported them.
func (n *node) neighbors() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []string
	for name, up := range n.up {
		if up {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// awaitNeighbor waits until want appears in the node's neighbor set.
func (n *node) awaitNeighbor(ctx context.Context, want string) error {
	for {
		n.mu.Lock()
		up, changed := n.up[want], n.changed
		n.mu.Unlock()
		if up {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("node %s never saw neighbor %s: %w", n.name, want, ctx.Err())
		}
	}
}

// awaitMessage waits for the next application message on the topic.
func (n *node) awaitMessage(ctx context.Context) (gossip.Event, error) {
	select {
	case ev := <-n.seen:
		return ev, nil
	case <-ctx.Done():
		return gossip.Event{}, fmt.Errorf("node %s never received the broadcast: %w", n.name, ctx.Err())
	}
}

// knows reports whether n's endpoint holds any connection state for other. It
// is how this example shows that a relayed message needs no connection between
// its sender and its recipient.
func (n *node) knows(other *node) bool {
	_, ok := n.ep.RemoteInfo(other.ep.ID())
	return ok
}

// scope names a delivery: the epidemic overlay, or a direct neighbor send from
// [gossip.Topic.BroadcastNeighbors].
func scope(s gossip.DeliveryScope) string {
	if s == gossip.DeliverySwarm {
		return "swarm"
	}
	return "neighbor"
}
