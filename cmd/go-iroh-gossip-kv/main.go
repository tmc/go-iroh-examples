// Command go-iroh-gossip-kv replicates a signed key-value store over gossip.
//
// This is the Go port of n0's iroh-smol-kv, and it speaks that crate's wire
// format, so a Go node and a Rust iroh-smol-kv node on the same topic share one
// store. It is an application built on gossip rather than an introduction to
// it: for the primitive itself — subscribing to a [gossip.TopicID],
// bootstrapping into a swarm from a known address, and reading the event
// stream — read go-iroh-gossip-topic first. What is here is the layer above.
//
// Gossip is a broadcast medium. A message reaches everyone subscribed to the
// topic, in no fixed order, possibly more than once, and it arrives from
// whichever neighbor relayed it rather than from its author. So the payload has
// to stand on its own. The store is a two-level map: a scope, which is the
// public key of the writer, then a key within it. Each update carries its
// scope, the key, the value, a timestamp in nanoseconds, and an Ed25519
// signature by the scope's key over the key, timestamp and value.
// [kvStore.apply] verifies the signature before it stores anything and keeps
// the value with the later timestamp when it already has one for the key.
// Applying an update twice, or out of order, therefore reaches the same state
// as applying it once in order, which is what a medium with those properties
// requires — and it is why the store needs no leader and no acknowledgement.
//
// The endpoint key and the signing key are deliberately distinct. The endpoint
// key authenticates a connection; the scope key authenticates an update, which
// outlives the connection it arrived on and will be seen by peers that never
// spoke to its author.
//
// iroh-smol-kv also expires entries after a horizon and periodically
// rebroadcasts what it holds so that late joiners catch up. This port keeps
// only the replication rule.
//
// The example runs two nodes on loopback. One subscribes to the topic, the
// other joins using the first as its bootstrap address, broadcasts one signed
// update, and the first prints the store it converged on.
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"sort"
	"time"

	"github.com/tmc/go-iroh/gossip"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(stdout io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var topicID gossip.TopicID
	copy(topicID[:], "go-iroh-smol-kv")

	a, err := newNode(ctx)
	if err != nil {
		return err
	}
	defer a.close(ctx)
	b, err := newNode(ctx)
	if err != nil {
		return err
	}
	defer b.close(ctx)

	aTopic, err := a.gossip.Subscribe(ctx, topicID, nil)
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	defer aTopic.Close()

	bTopic, err := b.gossip.SubscribeAndJoin(ctx, topicID, []netaddr.EndpointAddr{a.endpoint.Addr()})
	if err != nil {
		return fmt.Errorf("subscribe and join: %w", err)
	}
	defer bTopic.Close()

	sender, receiver := bTopic.Split()
	applied := make(chan error, 1)
	go func() {
		applied <- a.applyEvents(ctx, aTopic)
	}()

	msg, err := b.put("color", "blue")
	if err != nil {
		return err
	}
	if err := sender.Broadcast(ctx, msg); err != nil {
		return fmt.Errorf("broadcast: %w", err)
	}

	select {
	case err := <-applied:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	fmt.Fprintf(stdout, "node %s joined %d neighbor\n", b.endpoint.ID().Short(), len(receiver.Neighbors()))
	a.store.print(stdout)
	return nil
}

// node is one member of the swarm: an endpoint carrying gossip, a signing key
// for its scope, and the store it has converged on.
type node struct {
	endpoint *iroh.Endpoint
	router   *iroh.Router
	gossip   *gossip.Gossip
	signer   key.SecretKey
	store    kvStore
}

func newNode(ctx context.Context) (*node, error) {
	ep, err := bind(ctx)
	if err != nil {
		return nil, fmt.Errorf("bind endpoint: %w", err)
	}
	return serve(ctx, ep)
}

// serve runs gossip on ep and gives the node a fresh signing key.
func serve(ctx context.Context, ep *iroh.Endpoint) (*node, error) {
	g := gossip.NewGossip(ep)
	r, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{
		gossip.ALPN: g.Handler(),
	}, nil)
	if err != nil {
		ep.Shutdown(ctx)
		return nil, fmt.Errorf("new router: %w", err)
	}
	// Separate from the endpoint key: this one signs updates, not
	// connections.
	sk, err := key.GenerateSecretKey()
	if err != nil {
		r.Shutdown(ctx)
		return nil, fmt.Errorf("generate key: %w", err)
	}
	return &node{
		endpoint: ep,
		router:   r,
		gossip:   g,
		signer:   sk,
		store:    make(kvStore),
	}, nil
}

func (n *node) close(ctx context.Context) {
	n.gossip.Shutdown(ctx)
	n.router.Shutdown(ctx)
	n.signer.Clear()
}

// put signs name=value in the node's own scope, stores it, and returns the
// message to broadcast.
func (n *node) put(name, value string) ([]byte, error) {
	u := signUpdate(n.signer, []byte(name), []byte(value), uint64(time.Now().UnixNano()))
	msg := u.encode()
	if err := n.store.apply(msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// applyEvents applies the first update broadcast on topic. A real node would
// not return after one; the example does so that it terminates.
func (n *node) applyEvents(ctx context.Context, topic *gossip.Topic) error {
	for ev, err := range topic.Events() {
		if err != nil {
			return err
		}
		if ev.Kind != gossip.Received {
			continue
		}
		return n.store.apply(ev.Content)
	}
	return ctx.Err()
}

// update is one write: iroh-smol-kv's GossipMessage::SignedValue.
type update struct {
	Scope     key.PublicKey
	Key       []byte
	Timestamp uint64 // nanoseconds since the Unix epoch
	Value     []byte
	Signature key.Signature
}

// signUpdate signs key=value at timestamp in the scope of sk.
func signUpdate(sk key.SecretKey, k, value []byte, timestamp uint64) update {
	return update{
		Scope:     sk.Public(),
		Key:       k,
		Timestamp: timestamp,
		Value:     value,
		Signature: sk.Sign(signingData(k, timestamp, value)),
	}
}

// The encodings below are postcard, the serde format iroh-smol-kv uses:
// integers and lengths are unsigned LEB128 varints, byte strings are a length
// then the bytes, and fixed-size arrays (the public key, the signature) are
// the bytes alone.

// signingData is the message the signature covers: iroh-smol-kv's
// SigningData, the postcard encoding of (key, timestamp, value).
func signingData(k []byte, timestamp uint64, value []byte) []byte {
	b := appendBytes(nil, k)
	b = binary.AppendUvarint(b, timestamp)
	return appendBytes(b, value)
}

// encode returns the gossip message: variant 0 of the GossipMessage enum, then
// the scope, the key, and the SignedValue (timestamp, value, signature).
func (u update) encode() []byte {
	b := []byte{0}
	scope := u.Scope.Bytes()
	b = append(b, scope[:]...)
	b = appendBytes(b, u.Key)
	b = binary.AppendUvarint(b, u.Timestamp)
	b = appendBytes(b, u.Value)
	sig := u.Signature.Bytes()
	return append(b, sig[:]...)
}

func appendBytes(b, p []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(p)))
	return append(b, p...)
}

var errShort = errors.New("message too short")

// decodeUpdate parses a gossip message produced by encode or by iroh-smol-kv.
// It does not verify the signature.
func decodeUpdate(b []byte) (update, error) {
	var u update
	if len(b) == 0 || b[0] != 0 {
		return u, errors.New("not a SignedValue message")
	}
	b = b[1:]
	if len(b) < key.PublicKeySize {
		return u, errShort
	}
	scope, err := key.PublicKeyFromSlice(b[:key.PublicKeySize])
	if err != nil {
		return u, fmt.Errorf("parse scope: %w", err)
	}
	u.Scope = scope
	b = b[key.PublicKeySize:]
	if u.Key, b, err = readBytes(b); err != nil {
		return u, fmt.Errorf("read key: %w", err)
	}
	ts, n := binary.Uvarint(b)
	if n <= 0 {
		return u, errors.New("bad timestamp")
	}
	u.Timestamp = ts
	b = b[n:]
	if u.Value, b, err = readBytes(b); err != nil {
		return u, fmt.Errorf("read value: %w", err)
	}
	if len(b) != key.SignatureSize {
		return u, fmt.Errorf("signature is %d bytes, want %d", len(b), key.SignatureSize)
	}
	if u.Signature, err = key.SignatureFromSlice(b); err != nil {
		return u, fmt.Errorf("parse signature: %w", err)
	}
	return u, nil
}

func readBytes(b []byte) (p, rest []byte, err error) {
	n, w := binary.Uvarint(b)
	if w <= 0 {
		return nil, nil, errors.New("bad length")
	}
	b = b[w:]
	if uint64(len(b)) < n {
		return nil, nil, errShort
	}
	return b[:n], b[n:], nil
}

// kvStore maps a scope, then a key, to the latest value written there.
type kvStore map[[key.PublicKeySize]byte]map[string]kvValue

type kvValue struct {
	Value     string
	Timestamp uint64
	Signer    string
}

// apply verifies one update and merges it. It is idempotent and independent
// of the order updates arrive in, which is what lets it run against gossip.
func (s kvStore) apply(msg []byte) error {
	u, err := decodeUpdate(msg)
	if err != nil {
		return fmt.Errorf("decode update: %w", err)
	}
	if err := u.Scope.Verify(signingData(u.Key, u.Timestamp, u.Value), u.Signature); err != nil {
		return fmt.Errorf("verify update: %w", err)
	}
	scope := u.Scope.Bytes()
	keys := s[scope]
	if keys == nil {
		keys = make(map[string]kvValue)
		s[scope] = keys
	}
	if old, ok := keys[string(u.Key)]; ok && old.Timestamp >= u.Timestamp {
		return nil
	}
	keys[string(u.Key)] = kvValue{
		Value:     string(u.Value),
		Timestamp: u.Timestamp,
		Signer:    u.Scope.Short(),
	}
	return nil
}

func (s kvStore) print(stdout io.Writer) {
	var lines []string
	for _, keys := range s {
		for k, v := range keys {
			lines = append(lines, fmt.Sprintf("%s=%s signer=%s", k, v.Value, v.Signer))
		}
	}
	sort.Strings(lines)
	for _, l := range lines {
		fmt.Fprintln(stdout, l)
	}
}

// bind binds an endpoint to an ephemeral IPv6 loopback port, then applies opts.
// Loopback keeps the example self-contained: no relay, no DNS, no network.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
}
