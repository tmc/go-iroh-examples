package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/netip"
	"sort"
	"time"

	automerge "github.com/automerge/automerge-go"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const alpn = "iroh/automerge/2"

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	receiverDoc := automerge.New()
	synced := make(chan *automerge.Doc, 1)

	server, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		panic(err)
	}
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		alpn: &automergeHandler{doc: receiverDoc, synced: synced},
	}, nil)
	if err != nil {
		panic(err)
	}
	defer router.Shutdown(ctx)

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		panic(err)
	}
	defer client.Shutdown(ctx)

	senderDoc := automerge.New()
	for i := range 5 {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := senderDoc.RootMap().Set(key, value); err != nil {
			panic(err)
		}
	}

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		panic(err)
	}
	if err := initiateSync(ctx, conn, senderDoc); err != nil {
		panic(err)
	}
	conn.CloseWithError(0, "thanks, bye")

	select {
	case doc := <-synced:
		printState(doc)
	case <-ctx.Done():
		panic(ctx.Err())
	}
}

type automergeHandler struct {
	doc    *automerge.Doc
	synced chan<- *automerge.Doc
}

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

func receiveSyncMessage(r io.Reader, state *automerge.SyncState) (done bool, err error) {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return false, err
	}
	n := binary.LittleEndian.Uint64(hdr[:])
	if n == 0 {
		return true, nil
	}
	if n > 16*1024*1024 {
		return false, fmt.Errorf("automerge sync message too large: %d", n)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return false, err
	}
	_, err = state.ReceiveMessage(b)
	return false, err
}

func printState(doc *automerge.Doc) {
	keys, err := doc.RootMap().Keys()
	if err != nil {
		panic(err)
	}
	sort.Strings(keys)
	fmt.Println("State")
	for _, key := range keys {
		value, err := automerge.As[string](doc.RootMap().Get(key))
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s => %q\n", key, value)
	}
}
