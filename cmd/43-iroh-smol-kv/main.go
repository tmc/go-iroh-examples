package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"sort"
	"time"

	"github.com/tmc/go-iroh/gossip"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var topicID gossip.TopicID
	copy(topicID[:], "go-iroh-smol-kv")

	a, err := newNode(ctx)
	if err != nil {
		panic(err)
	}
	defer a.close(ctx)
	b, err := newNode(ctx)
	if err != nil {
		panic(err)
	}
	defer b.close(ctx)

	aTopic, err := a.gossip.Subscribe(ctx, topicID, nil)
	if err != nil {
		panic(err)
	}
	defer aTopic.Close()

	aAddr := netaddr.NewEndpointAddr(a.endpoint.ID()).WithIP(a.endpoint.LocalAddr())
	bTopic, err := b.gossip.SubscribeAndJoin(ctx, topicID, []netaddr.EndpointAddr{aAddr})
	if err != nil {
		panic(err)
	}
	defer bTopic.Close()

	sender, receiver := bTopic.Split()
	applied := make(chan error, 1)
	go func() {
		applied <- a.applyEvents(ctx, aTopic)
	}()

	op, err := b.signSet("color", "blue", 1)
	if err != nil {
		panic(err)
	}
	if err := sender.Broadcast(ctx, op); err != nil {
		panic(err)
	}

	select {
	case err := <-applied:
		if err != nil {
			panic(err)
		}
	case <-ctx.Done():
		panic(ctx.Err())
	}

	fmt.Printf("node %s joined %d neighbor\n", b.endpoint.ID().Short(), len(receiver.Neighbors()))
	a.store.print()
}

type node struct {
	endpoint *iroh.Endpoint
	router   *iroh.Router
	gossip   *gossip.Gossip
	signer   key.SecretKey
	store    kvStore
}

func newNode(ctx context.Context) (*node, error) {
	ep, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
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

type kvBody struct {
	Author string `json:"author"`
	Key    string `json:"key"`
	Value  string `json:"value"`
	Seq    uint64 `json:"seq"`
}

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
