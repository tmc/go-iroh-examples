// Command go-iroh-gossip-kv replicates a signed key-value store over gossip.
//
// This is the Go port of n0's iroh-smol-kv. It is an application built on
// gossip rather than an introduction to it: for the primitive itself —
// subscribing to a [gossip.TopicID], bootstrapping into a swarm from a known
// address, and reading the event stream — read go-iroh-gossip-topic first. What is
// here is the layer above.
//
// Gossip is a broadcast medium. A message reaches everyone subscribed to the
// topic, in no fixed order, possibly more than once, and it arrives from
// whichever neighbor relayed it rather than from its author. So the payload has
// to stand on its own. Each operation carries its author's public key, a
// sequence number, and an Ed25519 signature over the encoded body.
// [kvStore.apply] verifies the signature before it stores anything and keeps
// the value with the higher sequence number when it already has one for the
// key. Applying an operation twice, or out of order, therefore reaches the same
// state as applying it once in order, which is what a medium with those
// properties requires — and it is why the store needs no leader and no
// acknowledgement.
//
// The endpoint key and the signing key are deliberately distinct. The endpoint
// key authenticates a connection; the author key authenticates an operation,
// which outlives the connection it arrived on and will be seen by peers that
// never spoke to its author.
//
// The example runs two nodes on loopback. One subscribes to the topic, the
// other joins using the first as its bootstrap address, broadcasts one signed
// set, and the first prints the store it converged on.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
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

	op, err := b.signSet("color", "blue", 1)
	if err != nil {
		return err
	}
	if err := sender.Broadcast(ctx, op); err != nil {
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

	fmt.Printf("node %s joined %d neighbor\n", b.endpoint.ID().Short(), len(receiver.Neighbors()))
	a.store.print()
	return nil
}

// node is one member of the swarm: an endpoint carrying gossip, a signing key
// for the operations it authors, and the store it has converged on.
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
	g := gossip.NewGossip(ep)
	r, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{
		gossip.ALPN: g.Handler(),
	}, nil)
	if err != nil {
		ep.Shutdown(ctx)
		return nil, fmt.Errorf("new router: %w", err)
	}
	// Separate from the endpoint key: this one signs operations, not
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

// signSet encodes a set operation and signs it. The signature covers the
// encoded body, so a receiver must re-encode the body it decoded to check it.
func (n *node) signSet(name, value string, seq uint64) ([]byte, error) {
	body := kvBody{
		Author: n.signer.Public().String(),
		Key:    name,
		Value:  value,
		Seq:    seq,
	}
	msg, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal op body: %w", err)
	}
	sig := n.signer.Sign(msg)
	wire := kvOp{
		Body:      body,
		Signature: hex.EncodeToString(sig.Ed25519()),
	}
	out, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("marshal op: %w", err)
	}
	return out, nil
}

// applyEvents applies the first operation broadcast on topic. A real node would
// not return after one; the example does so that it terminates.
func (n *node) applyEvents(ctx context.Context, topic *gossip.Topic) error {
	for ev, err := range topic.Events() {
		if err != nil {
			return err
		}
		if ev.Kind != gossip.Received {
			continue
		}
		if err := n.store.apply(ev.Content); err != nil {
			return err
		}
		return nil
	}
	return ctx.Err()
}

// kvBody is the signed part of an operation.
type kvBody struct {
	Author string `json:"author"`
	Key    string `json:"key"`
	Value  string `json:"value"`
	Seq    uint64 `json:"seq"`
}

// kvOp is what goes on the wire: a body and a signature over its encoding.
type kvOp struct {
	Body      kvBody `json:"body"`
	Signature string `json:"signature"`
}

type kvStore map[string]kvValue

type kvValue struct {
	Value  string
	Seq    uint64
	Author string
}

// apply verifies one operation and merges it. It is idempotent and independent
// of the order operations arrive in, which is what lets it run against gossip.
func (s kvStore) apply(data []byte) error {
	var op kvOp
	if err := json.Unmarshal(data, &op); err != nil {
		return fmt.Errorf("decode op: %w", err)
	}
	body, err := json.Marshal(op.Body)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}
	pub, err := key.ParsePublicKey(op.Body.Author)
	if err != nil {
		return fmt.Errorf("parse author: %w", err)
	}
	sigBytes, err := hex.DecodeString(op.Signature)
	if err != nil {
		return fmt.Errorf("decode signature: %w", err)
	}
	sig, err := key.SignatureFromEd25519(sigBytes)
	if err != nil {
		return fmt.Errorf("parse signature: %w", err)
	}
	if err := pub.Verify(body, sig); err != nil {
		return fmt.Errorf("verify op: %w", err)
	}
	if old, ok := s[op.Body.Key]; ok && old.Seq > op.Body.Seq {
		return nil
	}
	s[op.Body.Key] = kvValue{
		Value:  op.Body.Value,
		Seq:    op.Body.Seq,
		Author: pub.Short(),
	}
	return nil
}

func (s kvStore) print() {
	keys := make([]string, 0, len(s))
	for key := range s {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		v := s[key]
		fmt.Printf("%s=%s seq=%d signer=%s\n", key, v.Value, v.Seq, v.Author)
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
