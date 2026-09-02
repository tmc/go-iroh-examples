// Command go-iroh-blobs-gateway serves iroh blobs over HTTP.
//
// This is the Go port of n0's iroh-gateway: an HTTP front door for content that
// lives in a blob store on some other endpoint. A browser, a curl, or a video
// player speaks ordinary HTTP to the gateway; the gateway speaks [blobs.ALPN]
// to a provider it reaches by endpoint ID. Nothing on the HTTP side has to know
// iroh exists, which is the reason to run one: it is how hash-addressed content
// reaches clients that can only fetch URLs.
//
// Two routes are served. /blob/<hash> returns one blob — the transfer
// go-iroh-blobs-transfer does with no HTTP in front of it — and
// /collection/<root>/<name> returns one named entry from a collection, which is
// how a directory of files is addressed by a single hash. Both routes hand the
// fetched bytes to [http.ServeContent], which is what makes Range
// requests work: the example asks for bytes 6-10 and gets 206 Partial Content
// with a Content-Range header, the request a media player makes when a viewer
// seeks.
//
// The gateway fetches the whole blob and lets ServeContent slice it, which is
// the right trade only while blobs are small. A deployment serving large files
// would map the HTTP range onto a ranged blobs request instead, so that only
// the requested bytes cross the wire; go-iroh-blobs-ranges shows that request
// with [blobs.RangeChunks] and [blobs.GetBlobRangeBytes]. Verification is per
// chunk either way, so a range is checked against the hash without fetching the
// rest.
//
// A gateway is a trust boundary as well as a protocol boundary: its HTTP
// clients get no proof of anything, because the BLAKE3 verification happens on
// the gateway's side of the wire. That is a property to be aware of before
// putting one in front of content whose integrity matters to the client.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/iroh"
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

	store, collectionRoot, blobHash, err := newStore()
	if err != nil {
		return err
	}

	provider, err := exampleutil.Bind(ctx)
	if err != nil {
		return err
	}
	router, err := iroh.NewRouter(provider, map[string]iroh.ProtocolHandler{
		blobs.ALPN: blobHandler{store: store},
	}, nil)
	if err != nil {
		return err
	}
	defer router.Shutdown(ctx)

	client, err := exampleutil.Bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	// The gateway is a plain net/http server whose handler happens to fetch
	// its bodies over iroh.
	gateway := httptest.NewServer(gatewayHandler{
		endpoint: client,
		provider: exampleutil.Addr(provider),
	})
	defer gateway.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gateway.URL+"/blob/"+blobHash.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", "bytes=6-10")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("get blob: %w", err)
	}
	defer resp.Body.Close()
	fmt.Printf("GET /blob/%s %s %s\n", blobHash.Short(), resp.Status, resp.Header.Get("Content-Range"))
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	fmt.Printf("range body: %q\n", body)

	req, err = http.NewRequestWithContext(ctx, http.MethodGet, gateway.URL+"/collection/"+collectionRoot.String()+"/note.txt", nil)
	if err != nil {
		return err
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("get collection entry: %w", err)
	}
	defer resp.Body.Close()
	fmt.Printf("GET /collection/%s/note.txt %s\n", collectionRoot.Short(), resp.Status)
	return nil
}

// newStore builds the content the provider serves: one blob, and a collection
// naming it. It returns the store, the collection root hash, and the blob hash.
func newStore() (*blobs.MemStore, blobs.Hash, blobs.Hash, error) {
	store, err := blobs.NewMemStore()
	if err != nil {
		return nil, blobs.Hash{}, blobs.Hash{}, fmt.Errorf("new bytes map: %w", err)
	}
	data := []byte("hello gateway over iroh blobs")
	hash, err := store.Add(data)
	if err != nil {
		return nil, blobs.Hash{}, blobs.Hash{}, fmt.Errorf("add blob: %w", err)
	}
	// A collection is itself two blobs: the names, and the hash sequence they
	// index. Both have to be in the store for a peer to fetch the collection.
	collection := blobs.NewCollection([]blobs.CollectionEntry{{Name: "note.txt", Hash: hash}})
	if _, err := store.Add(collection.MetadataBytes()); err != nil {
		return nil, blobs.Hash{}, blobs.Hash{}, fmt.Errorf("add collection metadata: %w", err)
	}
	if _, err := store.Add(collection.HashSequence().Bytes()); err != nil {
		return nil, blobs.Hash{}, blobs.Hash{}, fmt.Errorf("add collection root: %w", err)
	}
	return store, collection.Root(), hash, nil
}

// blobHandler is the provider side: it answers blobs requests from store.
type blobHandler struct {
	store blobs.Store
}

// Accept implements [iroh.ProtocolHandler].
func (h blobHandler) Accept(ctx context.Context, conn *iroh.Conn) error {
	s, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	return blobs.ServeBlob(ctx, s, h.store)
}

// gatewayHandler translates HTTP requests into blobs requests to provider.
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
	// ServeContent handles Range, If-Range, and the 206/416 replies. The blob
	// is content-addressed and therefore immutable, so it has no modification
	// time to report.
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
