// Command go-iroh-blobs-store keeps blobs on disk, downloads them from several
// providers, and collects the ones nothing names.
//
// The other blobs examples use [blobs.NewMemStore], which is enough to show a
// transfer and nothing else: a memory store never fills a disk, never survives
// a restart, and never has to decide which blobs to throw away. This example is
// about the store rather than the wire, and uses [blobs.NewFSStore], the
// on-disk store, for all three of those questions.
//
// Persistence. A [blobs.FSStore] is a directory: blob data under data/, the
// BLAKE3 outboard under outboard/, and the tag table beside them. It holds no
// open file handles between calls, so "closing" it is dropping the value. The
// example writes a blob, forgets the store, opens the same directory again, and
// reads the blob back through the tag that named it.
//
// Downloading from more than one provider. [blobs.NewDownloader] takes a
// [blobs.Sink] to write into and a [blobs.BlobConnector] to reach providers
// with, and races a list of addresses for a hash: the first provider to deliver
// verified content wins and the rest are cancelled. Content addressing is what
// makes this safe to do without trusting any of them — every provider is
// offering the same hash, and a provider that sends something else fails
// verification rather than corrupting the store.
//
// Tags and garbage collection. A blob on disk is alive only while something
// names it. A persistent tag ([blobs.FSStore.SetTag]) is a durable name that
// outlives the process; a [blobs.TempTag] is a process-local name that lasts
// until it is closed, and exists to cover the window between storing a blob and
// naming it. That window is why [blobs.BlobWriter.Commit] and
// [blobs.Downloader.Download] return a TempTag rather than a bare hash: with
// only a hash in hand, a concurrent [blobs.FSStore.GC] could delete the blob
// before the caller could tag it. [blobs.FSStore.GCWithEvents] then deletes
// every blob that no tag reaches, reporting [blobs.GCEvent] progress and a
// [blobs.GCResult] count.
//
// The -dir flag, or IROH_EXAMPLE_DIR, chooses the store directory. The default
// is a temporary directory that is removed on exit, so a plain run leaves
// nothing behind; point -dir at a real path and run twice to watch the tagged
// blob outlive the process.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"time"

	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

// The three payloads have different fates: keepData is tagged and survives,
// scratchData is never named and is collected, remoteData starts out only on
// the providers and arrives by download.
var (
	keepData    = []byte("tagged blob, outlives the process\n")
	scratchData = []byte("untagged blob, collected by gc\n")
	remoteData  = bytes.Repeat([]byte("remote payload chunk\n"), 200)
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		// -h is a request for the usage message, which the flag package has
		// already printed. It is not a failure.
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("go-iroh-blobs-store", flag.ContinueOnError)
	dir := fs.String("dir", env("IROH_EXAMPLE_DIR", ""), "store directory, default a temporary one removed on exit ($IROH_EXAMPLE_DIR)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	storeDir := *dir
	if storeDir == "" {
		tmp, err := os.MkdirTemp("", "go-iroh-blobs-store")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmp)
		storeDir = tmp
	}

	if err := write(ctx, storeDir); err != nil {
		return err
	}
	store, err := reopen(ctx, storeDir)
	if err != nil {
		return err
	}
	if err := download(ctx, store, storeDir); err != nil {
		return err
	}
	return collect(ctx, store)
}

// write stores the two local blobs in a fresh store and tags one of them, then
// returns, leaving the store behind on disk.
func write(ctx context.Context, dir string) error {
	store, err := blobs.NewFSStore(dir)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}

	// NewBlob streams content to a temporary file that GC cannot see, and
	// Commit installs it under its root hash together with a temp tag. The tag
	// is what keeps the blob alive until SetTag gives it a durable name.
	w, err := store.NewBlob(ctx)
	if err != nil {
		return fmt.Errorf("new blob: %w", err)
	}
	defer w.Close()
	if _, err := w.Write(keepData); err != nil {
		return fmt.Errorf("write blob: %w", err)
	}
	tag, err := w.Commit()
	if err != nil {
		return fmt.Errorf("commit blob: %w", err)
	}
	defer tag.Close()
	if err := store.SetTag("keep", blobs.RawHash(tag.Hash())); err != nil {
		return fmt.Errorf("set tag: %w", err)
	}

	// Add is the shorthand for content already in memory. It returns a hash
	// and nothing that protects it, which is exactly what makes this blob the
	// one GC reclaims later.
	if _, err := store.Add(scratchData); err != nil {
		return fmt.Errorf("add blob: %w", err)
	}

	fmt.Println("stored:", tag.Hash().Short())
	return nil
}

// reopen opens the same directory again in a store that shares nothing with the
// one write used, and reads the tagged blob back out of it.
func reopen(ctx context.Context, dir string) (*blobs.FSStore, error) {
	store, err := blobs.NewFSStore(dir)
	if err != nil {
		return nil, fmt.Errorf("reopen store: %w", err)
	}
	value, ok, err := store.Tag("keep")
	if err != nil {
		return nil, fmt.Errorf("read tag: %w", err)
	}
	if !ok {
		return nil, errors.New("tag keep did not survive the reopen")
	}
	data, err := blobs.ReadBlob(ctx, store, value.Hash)
	if err != nil {
		return nil, fmt.Errorf("read blob %s: %w", value.Hash.Short(), err)
	}
	if !bytes.Equal(data, keepData) {
		return nil, errors.New("reopened blob does not match what was stored")
	}
	fmt.Println("reopened:", value.Hash.Short(), "still tagged keep")
	return store, nil
}

// download fetches remoteData into store, racing two providers that hold it.
func download(ctx context.Context, store *blobs.FSStore, dir string) error {
	hash := blobs.NewHash(remoteData)

	// Two providers serving the same content. Neither is trusted: the hash the
	// downloader asks for is what decides whether an answer is acceptable.
	var providers []netaddr.EndpointAddr
	for i := range 2 {
		router, err := serve(ctx, filepath.Join(dir, fmt.Sprintf("provider%d", i)), remoteData)
		if err != nil {
			return err
		}
		defer router.Shutdown(ctx)
		providers = append(providers, router.Endpoint().Addr())
	}

	client, err := bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	connect := func(ctx context.Context, addr netaddr.EndpointAddr, alpn string) (blobs.BlobConn, error) {
		conn, err := client.Connect(ctx, addr, alpn)
		if err != nil {
			return nil, err
		}
		// BlobConnFunc adapts a stream opener to blobs.BlobConn. The
		// downloader opens one stream per attempt on the connection it caches
		// per provider.
		return blobs.BlobConnFunc(func(ctx context.Context) (blobs.BidiStream, error) {
			return conn.OpenStreamSync(ctx)
		}), nil
	}

	var tried, completed int
	d := blobs.NewDownloader(store, blobs.BlobConnectorFunc(connect), blobs.DownloaderOptions{
		Concurrency: len(providers),
		OnEvent: func(ev blobs.DownloadEvent) {
			switch ev.Kind {
			case blobs.DownloadTryProvider:
				tried++
			case blobs.DownloadComplete:
				completed++
			}
		},
	})
	defer d.Close()

	// Download returns the temp tag protecting what it stored. Nothing else
	// names the blob yet, so dropping the tag before SetTag would leave it for
	// the next GC.
	tag, err := d.Download(ctx, hash, providers)
	if err != nil {
		return fmt.Errorf("download %s: %w", hash.Short(), err)
	}
	defer tag.Close()
	if err := store.SetTag("remote", tag.Value()); err != nil {
		return fmt.Errorf("set tag: %w", err)
	}
	// The counters are the evidence that the downloader really raced the
	// providers rather than the example fetching the blob some other way.
	// Which provider wins is a race, so the winner is not printed.
	if tried == 0 || completed == 0 {
		return errors.New("downloader reported no provider attempt")
	}
	fmt.Println("providers offered:", len(providers))
	fmt.Println("downloaded:", tag.Hash().Short())

	return nil
}

// collect runs a garbage collection sweep and reports what it reclaimed.
func collect(ctx context.Context, store *blobs.FSStore) error {
	scratch := blobs.NewHash(scratchData)
	var deleted []blobs.Hash
	result, err := store.GCWithEvents(ctx, func(ev blobs.GCEvent) {
		if ev.Kind == blobs.GCEventDelete {
			deleted = append(deleted, ev.Hash)
		}
	})
	if err != nil {
		return fmt.Errorf("gc: %w", err)
	}
	fmt.Println("gc deleted:", result.Deleted)
	for _, hash := range deleted {
		fmt.Println("gc reclaimed:", hash.Short())
	}

	status, err := blobs.Status(ctx, store, scratch)
	if err != nil {
		return fmt.Errorf("status: %w", err)
	}
	fmt.Println("untagged blob present:", status.State != blobs.BlobNotFound)

	tags, err := store.Tags()
	if err != nil {
		return fmt.Errorf("tags: %w", err)
	}
	for _, tag := range tags {
		status, err := blobs.Status(ctx, store, tag.Value.Hash)
		if err != nil {
			return fmt.Errorf("status: %w", err)
		}
		fmt.Printf("tag %s: %s present=%v\n", tag.Name, tag.Value.Hash.Short(), status.State != blobs.BlobNotFound)
	}
	return nil
}

// serve starts an endpoint that answers blob requests for data out of its own
// on-disk store in dir. A provider is a store plus an accept loop; nothing
// about it is specific to this example.
func serve(ctx context.Context, dir string, data []byte) (*iroh.Router, error) {
	store, err := blobs.NewFSStore(dir)
	if err != nil {
		return nil, fmt.Errorf("open provider store: %w", err)
	}
	if _, err := store.Add(data); err != nil {
		return nil, fmt.Errorf("add provider blob: %w", err)
	}
	ep, err := bind(ctx)
	if err != nil {
		return nil, err
	}
	router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{
		blobs.ALPN: blobHandler{store: store},
	}, nil)
	if err != nil {
		ep.Shutdown(ctx)
		return nil, err
	}
	return router, nil
}

// blobHandler answers every stream of a connection from store.
type blobHandler struct {
	store blobs.Store
}

// Accept implements [iroh.ProtocolHandler].
func (h blobHandler) Accept(ctx context.Context, conn *iroh.Conn) error {
	accept := func(ctx context.Context) (blobs.BidiStream, error) {
		return conn.AcceptStream(ctx)
	}
	return blobs.ServeBlobStreams(ctx, accept, h.store)
}

// bind binds an endpoint to an ephemeral IPv6 loopback port, which keeps the
// example self-contained: no relay, no DNS, no network access.
func bind(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := make([]iroh.Option, 0, len(opts)+1)
	all = append(all, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	all = append(all, opts...)
	return iroh.Bind(ctx, all...)
}

// env returns the environment variable name, or def if it is unset or empty, so
// that the flag and the variable configure the same thing.
func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}
