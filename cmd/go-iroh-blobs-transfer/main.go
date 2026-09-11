// Command go-iroh-blobs-transfer serves one blob and fetches it whole.
//
// This is the shape of n0's sendme: one endpoint holds some content, another
// knows only its BLAKE3 hash, and the bytes are verified as they arrive. The
// [blobs] package implements the raw subset of the iroh-blobs provider protocol
// on ALPN [blobs.ALPN], so a blob served here can be fetched by the Rust
// tooling and the reverse.
//
// Verification is the reason to use this rather than a stream of bytes. A blob
// is named by the root of its BLAKE3 hash tree, and the provider sends enough
// of that tree alongside the data for [blobs.GetBlobBytes] to check each chunk
// against the hash the caller already had. Content that does not hash to what
// was asked for is rejected by the receiver, so the provider does not have to
// be trusted or even identified.
//
// The provider side is [blobs.ServeBlob] over one stream, reading from a
// [blobs.Store]. This example uses [blobs.NewMemStore]; a real provider would
// keep blobs on disk, and nothing else about the exchange changes.
//
// Compare go-iroh-blobs-ranges, which asks for a chunk range of a blob instead
// of all of it. Read the two together: a resumed download is this same request
// with a range in it, and a range carries the hash-tree nodes needed to verify
// it on its own.
//
// The -file flag, or IROH_EXAMPLE_FILE, serves a file instead of the built-in
// payload.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/iroh"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		// -h is a request for the usage message, which the flag package has
		// already printed. It is not a failure.
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("go-iroh-blobs-transfer", flag.ContinueOnError)
	file := fs.String("file", env("IROH_EXAMPLE_FILE", ""), "file to serve instead of the built-in payload ($IROH_EXAMPLE_FILE)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	payload := []byte("sendme-style file contents\n")
	if *file != "" {
		b, err := os.ReadFile(*file)
		if err != nil {
			return err
		}
		payload = b
	}

	// The store holds the content; the hash is what the receiver is given out
	// of band. Both sides derive it from the same bytes here.
	store, err := blobs.NewMemStore(payload)
	if err != nil {
		return err
	}
	hash := blobs.NewHash(payload)

	server, err := bind(ctx, iroh.WithALPNs(blobs.ALPN))
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- serve(ctx, server, store)
	}()

	client, err := bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, server.Addr(), blobs.ALPN)
	if err != nil {
		return fmt.Errorf("connect to provider: %w", err)
	}
	defer conn.CloseWithError(0, "")

	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	got, err := blobs.GetBlobBytes(ctx, s, hash)
	if err != nil {
		return fmt.Errorf("get blob %s: %w", hash.Short(), err)
	}
	if err := <-serverErr; err != nil {
		return fmt.Errorf("serve blob: %w", err)
	}
	// GetBlobBytes has already checked the content against hash; this only
	// guards against the example itself hashing the wrong thing.
	if !bytes.Equal(got, payload) {
		return errors.New("fetched blob does not match the served payload")
	}

	fmt.Fprintln(stdout, "bytes:", len(got))
	fmt.Fprintln(stdout, "blake3:", hash.Short())
	return nil
}

// serve answers one blob request on one stream of one connection.
func serve(ctx context.Context, ep *iroh.Endpoint, store blobs.Store) error {
	conn, err := ep.Accept(ctx)
	if err != nil {
		return err
	}
	s, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	return blobs.ServeBlob(ctx, s, store)
}

// bind binds an endpoint to an ephemeral IPv6 loopback port, then applies opts.
// Loopback keeps the example self-contained: no relay, no DNS, no network.
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
