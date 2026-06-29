package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"time"

	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, collectionRoot, blobHash, err := newStore()
	if err != nil {
		panic(err)
	}

	provider, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		panic(err)
	}
	router, err := iroh.NewRouter(provider, map[string]iroh.ProtocolHandler{
		blobs.ALPN: blobHandler{store: store.Store()},
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

	providerAddr := netaddr.NewEndpointAddr(provider.ID()).WithIP(provider.LocalAddr())
	gateway := httptest.NewServer(gatewayHandler{
		endpoint: client,
		provider: providerAddr,
	})
	defer gateway.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gateway.URL+"/blob/"+blobHash.String(), nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Range", "bytes=6-10")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	fmt.Printf("GET /blob/%s %s %s\n", blobHash.Short(), resp.Status, resp.Header.Get("Content-Range"))
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	fmt.Printf("range body: %q\n", body)

	req, err = http.NewRequestWithContext(ctx, http.MethodGet, gateway.URL+"/collection/"+collectionRoot.String()+"/note.txt", nil)
	if err != nil {
		panic(err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	fmt.Printf("GET /collection/%s/note.txt %s\n", collectionRoot.Short(), resp.Status)
}

func newStore() (*blobs.BytesMap, blobs.Hash, blobs.Hash, error) {
	store, err := blobs.NewBytesMap()
	if err != nil {
		return nil, blobs.Hash{}, blobs.Hash{}, fmt.Errorf("new bytes map: %w", err)
	}
	data := []byte("hello gateway over iroh blobs")
	hash, err := store.Add(data)
	if err != nil {
		return nil, blobs.Hash{}, blobs.Hash{}, fmt.Errorf("add blob: %w", err)
	}
	collection := blobs.NewCollection([]blobs.CollectionEntry{{Name: "note.txt", Hash: hash}})
	if _, err := store.Add(collection.MetadataBytes()); err != nil {
		return nil, blobs.Hash{}, blobs.Hash{}, fmt.Errorf("add collection metadata: %w", err)
	}
	if _, err := store.Add(collection.HashSequence().Bytes()); err != nil {
		return nil, blobs.Hash{}, blobs.Hash{}, fmt.Errorf("add collection root: %w", err)
	}
	return store, collection.Root(), hash, nil
}

type blobHandler struct {
	store blobs.Store
}

func (h blobHandler) Accept(ctx context.Context, conn *iroh.Conn) error {
	s, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	return blobs.ServeBlob(ctx, s, h.store)
}

type gatewayHandler struct {
	endpoint *iroh.Endpoint
	provider netaddr.EndpointAddr
}

func (h gatewayHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method != http.MethodGet:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	case strings.HasPrefix(r.URL.Path, "/blob/"):
		h.serveBlob(w, r)
	case strings.HasPrefix(r.URL.Path, "/collection/"):
		h.serveCollection(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h gatewayHandler) serveBlob(w http.ResponseWriter, r *http.Request) {
	hashText := strings.TrimPrefix(r.URL.Path, "/blob/")
	hash, err := blobs.ParseHash(hashText)
	if err != nil {
		http.Error(w, "bad blob hash", http.StatusBadRequest)
		return
	}
	data, err := h.getBlob(r.Context(), hash)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	http.ServeContent(w, r, hash.String(), time.Time{}, bytes.NewReader(data))
}

func (h gatewayHandler) serveCollection(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/collection/")
	rootText, name, ok := strings.Cut(rest, "/")
	if !ok || name == "" {
		http.Error(w, "bad collection path", http.StatusBadRequest)
		return
	}
	root, err := blobs.ParseHash(rootText)
	if err != nil {
		http.Error(w, "bad collection hash", http.StatusBadRequest)
		return
	}
	collection, data, err := h.getCollection(r.Context(), root)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	for i, entry := range collection.Entries() {
		if entry.Name == name {
			http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(data[i]))
			return
		}
	}
	http.NotFound(w, r)
}

func (h gatewayHandler) getBlob(ctx context.Context, hash blobs.Hash) ([]byte, error) {
	conn, err := h.endpoint.Connect(ctx, h.provider, blobs.ALPN)
	if err != nil {
		return nil, fmt.Errorf("connect provider: %w", err)
	}
	defer conn.CloseWithError(0, "")
	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, fmt.Errorf("open blob stream: %w", err)
	}
	data, err := blobs.GetBlobBytes(ctx, s, hash)
	if err != nil {
		return nil, fmt.Errorf("get blob: %w", err)
	}
	return data, nil
}

func (h gatewayHandler) getCollection(ctx context.Context, root blobs.Hash) (blobs.Collection, [][]byte, error) {
	conn, err := h.endpoint.Connect(ctx, h.provider, blobs.ALPN)
	if err != nil {
		return blobs.Collection{}, nil, fmt.Errorf("connect provider: %w", err)
	}
	defer conn.CloseWithError(0, "")
	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return blobs.Collection{}, nil, fmt.Errorf("open collection stream: %w", err)
	}
	collection, data, err := blobs.GetCollectionBytes(ctx, s, root)
	if err != nil {
		return blobs.Collection{}, nil, fmt.Errorf("get collection: %w", err)
	}
	return collection, data, nil
}
