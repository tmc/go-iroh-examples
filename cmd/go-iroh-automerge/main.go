// Command go-iroh-automerge synchronizes an Automerge document between peers.
//
// This is the Go port of n0's iroh-automerge. Automerge is a CRDT library with
// its own sync protocol: each side keeps a sync state, repeatedly asks it for
// the next message to send, feeds it whatever the peer sent, and stops when
// neither side has anything left to say. That protocol is specified over a
// reliable, ordered, bidirectional byte pipe and says nothing about where the
// pipe comes from.
//
// An iroh stream is such a pipe, which is the point of the example: carrying a
// foreign sync protocol over iroh needs no adapter. The transport supplies
// identity — the peer is its public key — plus NAT traversal and encryption,
// and the protocol supplies everything above that. The only glue is the glue
// every message protocol needs, a length prefix. Here it is eight bytes
// little-endian to match the Rust example, and a length of zero means "I am
// done", which is how each side learns the other has converged.
//
// The two halves are [initiateSync] and [respondSync]. They differ only in who
// speaks first, because the dialer must send before the responder has anything
// to answer. The sender starts with five keys and the receiver starts empty;
// after the exchange the receiver prints what it now holds.
//
// github.com/automerge/automerge-go is the only third-party dependency in this
// repository. Compare go-iroh-framed-messages, which frames messages of its own
// design over the same kind of stream, and go-iroh-gossip-kv, which reaches
// convergence a different way: broadcast operations with a last-writer-wins
// rule rather than a point-to-point sync protocol.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/netip"
	"os"
	"sort"
	"time"

	automerge "github.com/automerge/automerge-go"
	"github.com/tmc/go-iroh/iroh"
)

const (
	alpn = "iroh/automerge/2"
	// maxSyncMessageSize bounds a peer-supplied length before it is allocated.
	maxSyncMessageSize = 16 << 20
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

	receiverDoc := automerge.New()
	synced := make(chan *automerge.Doc, 1)

	server, err := bind(ctx)
	if err != nil {
		return err
	}
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		alpn: &automergeHandler{doc: receiverDoc, synced: synced},
	}, nil)
	if err != nil {
		return err
	}
	defer router.Shutdown(ctx)

	client, err := bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	senderDoc := automerge.New()
	for i := range 5 {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := senderDoc.RootMap().Set(key, value); err != nil {
			return fmt.Errorf("set %s: %w", key, err)
		}
	}

	conn, err := client.Connect(ctx, server.Addr(), alpn)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", server.ID().Short(), err)
	}
	if err := initiateSync(ctx, conn, senderDoc); err != nil {
		return err
	}
	conn.CloseWithError(0, "thanks, bye")

	select {
	case doc := <-synced:
		return printState(doc, stdout)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// automergeHandler syncs doc with each peer that connects and publishes the
// result on synced.
type automergeHandler struct {
	doc    *automerge.Doc
	synced chan<- *automerge.Doc
}

// Accept implements [iroh.ProtocolHandler].
func (h *automergeHandler) Accept(ctx context.Context, conn *iroh.Conn) error {
	if err := respondSync(ctx, conn, h.doc); err != nil {
		return err
	}
	select {
	case h.synced <- h.doc:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// initiateSync runs the dialing half of the Automerge sync protocol: send
// first, then alternate until both sides report they are done.
func initiateSync(ctx context.Context, conn *iroh.Conn, doc *automerge.Doc) error {
	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	defer s.Close()

	state := automerge.NewSyncState(doc)
	for {
		msg, ok := state.GenerateMessage()
		if err := sendSyncMessage(s, msg, ok); err != nil {
			return err
		}
		localDone := !ok

		remoteDone, err := receiveSyncMessage(s, state)
		if err != nil {
			return err
		}
		if localDone && remoteDone {
			return nil
		}
	}
}

// respondSync runs the accepting half: read first, then answer.
func respondSync(ctx context.Context, conn *iroh.Conn, doc *automerge.Doc) error {
	s, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	defer s.Close()

	state := automerge.NewSyncState(doc)
	for {
		remoteDone, err := receiveSyncMessage(s, state)
		if err != nil {
			return err
		}

		msg, ok := state.GenerateMessage()
		if err := sendSyncMessage(s, msg, ok); err != nil {
			return err
		}
		if remoteDone && !ok {
			return nil
		}
	}
}

// sendSyncMessage writes msg with its length in front. A zero length stands for
// "nothing more to send", the signal that ends the exchange.
func sendSyncMessage(w io.Writer, msg *automerge.SyncMessage, ok bool) error {
	if !ok {
		var zero [8]byte
		_, err := w.Write(zero[:])
		return err
	}
	b := msg.Bytes()
	var hdr [8]byte
	binary.LittleEndian.PutUint64(hdr[:], uint64(len(b)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

// receiveSyncMessage reads one length-prefixed message and applies it to state.
// It reports whether the peer signalled that it has nothing more to send.
func receiveSyncMessage(r io.Reader, state *automerge.SyncState) (done bool, err error) {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return false, err
	}
	n := binary.LittleEndian.Uint64(hdr[:])
	if n == 0 {
		return true, nil
	}
	if n > maxSyncMessageSize {
		return false, fmt.Errorf("automerge sync message too large: %d", n)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return false, err
	}
	_, err = state.ReceiveMessage(b)
	return false, err
}

// printState prints doc's root map in key order.
func printState(doc *automerge.Doc, stdout io.Writer) error {
	keys, err := doc.RootMap().Keys()
	if err != nil {
		return fmt.Errorf("read keys: %w", err)
	}
	sort.Strings(keys)
	fmt.Fprintln(stdout, "State")
	for _, key := range keys {
		value, err := automerge.As[string](doc.RootMap().Get(key))
		if err != nil {
			return fmt.Errorf("read %s: %w", key, err)
		}
		fmt.Fprintf(stdout, "%s => %q\n", key, value)
	}
	return nil
}

// bind binds an endpoint to an ephemeral IPv6 loopback port, then applies opts.
// Loopback keeps the example self-contained: no relay, no DNS, no network.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
}
