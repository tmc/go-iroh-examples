// Command go-iroh-blobs-gateway serves iroh blobs over HTTP.
//
// This is the Go port of n0's iroh-gateway: an HTTP front door for content that
// lives in a blob store on some other endpoint. A browser, a curl, or a video
// player speaks ordinary HTTP to the gateway; the gateway speaks [blobs.ALPN]
// to a provider it reaches by endpoint ID. Nothing on the HTTP side has to know
// iroh exists, which is the reason to run one: it is how hash-addressed content
// reaches clients that can only fetch URLs.
//
// Four routes are served, the same set as the Rust gateway. /blob/<hash>
// returns one blob — the transfer go-iroh-blobs-transfer does with no HTTP in
// front of it — and /collection/<root>/<name> returns one named entry from a
// collection, which is how a directory of files is addressed by a single hash.
// Both resolve against one provider the gateway was configured with, so the URL
// names content and nothing else.
//
// /ticket/<ticket> and /ticket/<ticket>/<name> lift that restriction. A blob
// ticket ([blobs.Ticket]) packs a provider's address together with the hash and
// the blob format, so a URL carrying one names both what to fetch and who from,
// and the gateway serves content it had no prior connection to. The format in
// the ticket decides what /ticket/<ticket> means: a raw ticket is one blob, and
// a hash-sequence ticket is a collection, answered with its index so that the
// entries can be linked to under /ticket/<ticket>/<name>.
//
// The blob routes hand the fetched bytes to [http.ServeContent], which is what
// makes Range requests work: the example asks for bytes 6-10 and gets 206
// Partial Content with a Content-Range header, the request a media player makes
// when a viewer seeks.
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
	"net/netip"
	"os"
	"strings"
	"time"

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

	provider, err := bind(ctx)
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

	client, err := bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	// The gateway is a plain net/http server whose handler happens to fetch
	// its bodies over iroh.
	gateway := httptest.NewServer(gatewayHandler{
		endpoint: client,
		provider: provider.Addr(),
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

	// The ticket routes are the same fetches addressed differently: the URL
	// carries the provider's address, so a gateway that was never told about
	// this provider serves them just the same.
	blobTicket := blobs.NewTicket(provider.Addr(), blobHash, blobs.Raw)
	body, err = get(ctx, gateway.URL+"/ticket/"+blobTicket.EncodeString())
	if err != nil {
		return fmt.Errorf("get ticket blob: %w", err)
	}
	fmt.Printf("ticket blob: %q\n", body)

	// A hash-sequence ticket names a collection, so the same route answers
	// with its index instead of a blob body.
	collectionTicket := blobs.NewTicket(provider.Addr(), collectionRoot, blobs.HashSeq)
	body, err = get(ctx, gateway.URL+"/ticket/"+collectionTicket.EncodeString())
	if err != nil {
		return fmt.Errorf("get ticket index: %w", err)
	}
	fmt.Println("ticket index entries:", len(strings.Fields(string(body))))

	// Each index line is a URL of the second ticket route, which serves one
	// entry of the collection.
	body, err = get(ctx, gateway.URL+strings.Fields(string(body))[0])
	if err != nil {
		return fmt.Errorf("get ticket entry: %w", err)
	}
	fmt.Printf("ticket entry: %q\n", body)
	return nil
}

// get fetches url and returns its body, reporting any status other than 200.
func get(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s: %s", url, resp.Status, bytes.TrimSpace(body))
	}
	return body, nil
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
	case strings.HasPrefix(r.URL.Path, "/ticket/"):
		h.serveTicket(w, r)
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
	data, err := h.getBlob(r.Context(), h.provider, hash)
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
	h.serveEntry(w, r, h.provider, root, name)
}

// serveTicket answers the two ticket routes. Unlike the hash routes, the
// address to fetch from comes out of the URL, so these serve content the
// gateway was never configured with.
func (h gatewayHandler) serveTicket(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/ticket/")
	ticketText, name, hasName := strings.Cut(rest, "/")
	ticket, err := blobs.DecodeString(ticketText)
	if err != nil {
		http.Error(w, "bad ticket", http.StatusBadRequest)
		return
	}
	switch {
	case hasName && name != "":
		h.serveEntry(w, r, ticket.Addr(), ticket.Hash(), name)
	case ticket.Format() == blobs.Raw:
		data, err := h.getBlob(r.Context(), ticket.Addr(), ticket.Hash())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		http.ServeContent(w, r, ticket.Hash().String(), time.Time{}, bytes.NewReader(data))
	default:
		h.serveIndex(w, r, ticket)
	}
}

// serveIndex lists the entries of the collection a hash-sequence ticket names,
// linking each to the ticket route that serves it. The Rust gateway writes
// HTML here; the links are the part that matters, and they are the same.
func (h gatewayHandler) serveIndex(w http.ResponseWriter, r *http.Request, ticket blobs.Ticket) {
	collection, _, err := h.getCollection(r.Context(), ticket.Addr(), ticket.Hash())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	for _, entry := range collection.Entries() {
		fmt.Fprintf(w, "/ticket/%s/%s\n", ticket.EncodeString(), entry.Name)
	}
}

// serveEntry serves the entry named name from the collection rooted at root on
// provider.
func (h gatewayHandler) serveEntry(w http.ResponseWriter, r *http.Request, provider netaddr.EndpointAddr, root blobs.Hash, name string) {
	collection, data, err := h.getCollection(r.Context(), provider, root)
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

func (h gatewayHandler) getBlob(ctx context.Context, provider netaddr.EndpointAddr, hash blobs.Hash) ([]byte, error) {
	conn, err := h.endpoint.Connect(ctx, provider, blobs.ALPN)
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

func (h gatewayHandler) getCollection(ctx context.Context, provider netaddr.EndpointAddr, root blobs.Hash) (blobs.Collection, [][]byte, error) {
	conn, err := h.endpoint.Connect(ctx, provider, blobs.ALPN)
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

// bind binds an endpoint to an ephemeral IPv6 loopback port, then applies opts.
// Loopback keeps the example self-contained: no relay, no DNS, no network.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
}
