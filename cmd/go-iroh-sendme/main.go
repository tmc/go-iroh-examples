// Command go-iroh-sendme sends a file or directory to another machine, the way
// n0's sendme does.
//
// It is sendme's command line and sendme's wire protocol. "send" imports a path
// into a blob store, wraps the files in an iroh-blobs collection, and prints a
// blob ticket; "receive" (or "recv") dials the ticket, fetches the collection,
// verifies every byte against the BLAKE3 hash tree as it arrives, and writes
// the files into the current directory. A ticket printed by one implementation
// is accepted by the other in both directions, which interop_test.go checks
// against the Rust binary.
//
// A collection is two blobs plus the files. The root is a hash sequence, the
// concatenated hashes of its children; the first child is a metadata blob
// naming the files, and the rest are the files in name order. The ticket names
// the root in format [blobs.HashSeq], so one hash stands for the whole tree.
// Names are paths relative to the parent of what was sent, with "/" as the
// separator whatever the operating system, so "sendme send photos" produces
// photos/a.jpg and a receiver creates a photos directory.
//
// The Rust receiver opens with one request for the root and the last chunk of
// every child: a proof of each file's size, fetched before any content.
// [blobs.ServeBlob] answers it.
//
// With no arguments it sends a small directory to itself over loopback, so
// that "go run ./cmd/go-iroh-sendme" is a complete demo. Otherwise:
//
//	go-iroh-sendme send [flags] <path>
//	go-iroh-sendme receive [flags] <ticket>
//
// The flags are sendme's: -relay (default, disabled, or a relay URL),
// -ticket-type (id, relay-and-addresses, relay, or addresses), -magic-ipv4-addr
// and -magic-ipv6-addr, -format, -jobs, -show-secret, -no-progress, and -v.
// IROH_SECRET sets the endpoint's secret key, as it does for sendme.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
)

// maxMetaSize bounds the two blobs a receiver must hold in memory, the hash
// sequence and the name list, as sendme bounds the hash sequence.
const maxMetaSize = 32 << 20

// errUsage reports a command line the program cannot act on. Usage text is
// already on stderr by the time it is returned.
var errUsage = errors.New("usage")

const usageText = `usage:
  go-iroh-sendme send [flags] <path>
  go-iroh-sendme receive [flags] <ticket>

With no arguments a directory is sent and received over loopback.
Flags: -relay default|disabled|<url>, -ticket-type id|relay-and-addresses|relay|addresses (send),
-magic-ipv4-addr addr:port, -magic-ipv6-addr addr:port, -format hex|cid, -jobs n,
-show-secret, -no-progress, -v. IROH_SECRET sets the secret key.
`

func main() {
	err := run(os.Args[1:], os.Stdout)
	switch {
	case err == nil:
	case errors.Is(err, flag.ErrHelp):
		fmt.Fprint(os.Stdout, usageText)
	case errors.Is(err, errUsage):
		os.Exit(1)
	default:
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return demo(stdout)
	}
	var cmd string
	switch args[0] {
	case "-h", "-help", "--help":
		return flag.ErrHelp
	case "send":
		cmd = "send"
	case "receive", "recv":
		cmd = "receive"
	default:
		return usage()
	}

	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var o options
	fs.StringVar(&o.relay, "relay", "default", "relay mode: default, disabled, or a relay URL")
	fs.StringVar(&o.ipv4, "magic-ipv4-addr", "", "IPv4 UDP address to bind")
	fs.StringVar(&o.ipv6, "magic-ipv6-addr", "", "IPv6 UDP address to bind")
	fs.StringVar(&o.format, "format", "hex", "hash format: hex or cid")
	fs.IntVar(&o.jobs, "jobs", runtime.NumCPU(), "files to import or download at once")
	fs.BoolVar(&o.showSecret, "show-secret", false, "print the secret key")
	fs.BoolVar(&o.verbose, "v", false, "print each file and transfer statistics")
	fs.Bool("no-progress", false, "accepted for compatibility; there is no progress display")
	ticketType := fs.String("ticket-type", "relay-and-addresses", "addresses to put in the ticket: id, relay-and-addresses, relay, or addresses")
	pos, err := parseArgs(fs, args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return usage()
	}
	if len(pos) != 1 || o.format != "hex" && o.format != "cid" || o.jobs < 1 {
		return usage()
	}
	if o.secret, err = secretKey(o.showSecret || o.verbose); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if cmd == "receive" {
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		return receive(ctx, pos[0], cwd, o, stdout, os.Stderr)
	}

	if o.ticketType, err = parseTicketType(*ticketType); err != nil {
		return err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	path, err := filepath.Abs(pos[0])
	if err != nil {
		return err
	}
	if path == cwd {
		return errors.New("can not share from the current directory")
	}
	suffix := make([]byte, 16)
	rand.Read(suffix)
	s, err := startSend(ctx, path, filepath.Join(cwd, ".sendme-send-"+hex.EncodeToString(suffix)), o, stdout)
	if err != nil {
		return err
	}
	<-ctx.Done()
	fmt.Fprintln(stdout, "shutting down")
	return s.Close()
}

func usage() error {
	fmt.Fprint(os.Stderr, usageText)
	return errUsage
}

// parseArgs parses flags that may come before or after the positional
// arguments, as they may for sendme, and returns the positional ones.
func parseArgs(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			return pos, nil
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

// options are the flags send and receive share, plus -ticket-type.
type options struct {
	relay      string
	ipv4, ipv6 string
	format     string
	jobs       int
	showSecret bool
	verbose    bool
	ticketType ticketType
	secret     key.SecretKey
}

// ticketType selects which of the sender's addresses go in the ticket.
type ticketType int

const (
	relayAndAddresses ticketType = iota
	idOnly
	relayOnly
	addressesOnly
)

// parseTicketType accepts sendme's spellings: the Rust variant names, in any
// case, with or without hyphens.
func parseTicketType(s string) (ticketType, error) {
	switch strings.ToLower(strings.ReplaceAll(s, "-", "")) {
	case "relayandaddresses":
		return relayAndAddresses, nil
	case "id":
		return idOnly, nil
	case "relay":
		return relayOnly, nil
	case "addresses":
		return addressesOnly, nil
	}
	return 0, fmt.Errorf("invalid ticket type %q", s)
}

// secretKey reads IROH_SECRET, or generates a key and, if show is set, prints
// it so that the next run can reuse the endpoint ID.
func secretKey(show bool) (key.SecretKey, error) {
	if s := os.Getenv("IROH_SECRET"); s != "" {
		sk, err := key.ParseSecretKey(s)
		if err != nil {
			return key.SecretKey{}, fmt.Errorf("invalid secret: %w", err)
		}
		return sk, nil
	}
	sk, err := key.GenerateSecretKey()
	if err != nil {
		return key.SecretKey{}, err
	}
	if show {
		seed := sk.Bytes()
		fmt.Fprintf(os.Stderr, "using secret key %s\n", hex.EncodeToString(seed[:]))
	}
	return sk, nil
}

// bind creates the endpoint both commands use. With no bind address it binds
// a dual-stack socket on every interface, as sendme does.
func bind(ctx context.Context, o options, opts ...iroh.Option) (*iroh.Endpoint, error) {
	opts = append(opts, iroh.WithSecretKey(o.secret))
	switch {
	case o.ipv4 != "" && o.ipv6 != "":
		return nil, errors.New("go-iroh binds one socket: give -magic-ipv4-addr or -magic-ipv6-addr, not both")
	case o.ipv4 != "" || o.ipv6 != "":
		ap, err := netip.ParseAddrPort(o.ipv4 + o.ipv6)
		if err != nil {
			return nil, fmt.Errorf("parse bind address: %w", err)
		}
		opts = append(opts, iroh.WithBindAddr(ap))
	}
	mode, err := relayMode(o.relay)
	if err != nil {
		return nil, err
	}
	opts = append(opts, iroh.WithRelayMode(mode))
	return iroh.Bind(ctx, opts...)
}

func relayMode(s string) (relay.Mode, error) {
	switch s {
	case "default":
		return relay.ModeDefault(), nil
	case "disabled":
		return relay.ModeDisabled(), nil
	}
	u, err := netaddr.ParseRelayURL(s)
	if err != nil {
		return relay.Mode{}, fmt.Errorf("invalid relay %q: %w", s, err)
	}
	return relay.ModeCustomURLs(u), nil
}

// A sender serves one collection until it is closed.
type sender struct {
	ep       *iroh.Endpoint
	storeDir string
	ticket   blobs.Ticket
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// startSend imports path into a store at storeDir, serves it, and prints the
// ticket to stdout.
func startSend(ctx context.Context, path, storeDir string, o options, stdout io.Writer) (*sender, error) {
	files, err := walk(path)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(storeDir); err == nil {
		return nil, fmt.Errorf("can not share twice from the same directory: %s", storeDir)
	}
	store, err := blobs.NewFSStore(storeDir)
	if err != nil {
		return nil, err
	}
	s := &sender{storeDir: storeDir}
	root, size, err := importFiles(store, files, o.jobs)
	if err != nil {
		os.RemoveAll(storeDir)
		return nil, err
	}

	opts := []iroh.Option{iroh.WithALPNs(blobs.ALPN)}
	if o.ticketType == idOnly {
		// A ticket holding only an ID is dialable only if the sender publishes
		// where it is, which sendme does to n0's pkarr relay.
		pub, err := iroh.N0PkarrPublisher(o.secret, nil)
		if err != nil {
			return nil, err
		}
		var lookup iroh.AddressLookupServices
		lookup.AddPublisher(pub)
		opts = append(opts, iroh.WithAddressLookup(&lookup))
	}
	s.ep, err = bind(ctx, o, opts...)
	if err != nil {
		os.RemoveAll(storeDir)
		return nil, err
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.wg.Go(func() { serve(serveCtx, s.ep, store) })

	if o.relay != "disabled" {
		octx, ocancel := context.WithTimeout(ctx, 30*time.Second)
		_ = s.ep.Online(octx)
		ocancel()
	}
	s.ticket = blobs.NewTicket(ticketAddr(s.ep, o.ticketType), root, blobs.HashSeq)

	kind := "directory"
	if fi, err := os.Stat(path); err == nil && fi.Mode().IsRegular() {
		kind = "file"
	}
	fmt.Fprintf(stdout, "imported %s %s, %s, hash %s\n", kind, filepath.Base(path), humanBytes(size), root)
	if o.verbose {
		for _, f := range files {
			fmt.Fprintf(stdout, "    %s\n", f.name)
		}
	}
	fmt.Fprintln(stdout, "to get this data, use")
	fmt.Fprintf(stdout, "sendme receive %s\n", s.ticket)
	return s, nil
}

// Close stops serving and deletes the store. The files that were sent are
// untouched: the store holds links or copies of them.
func (s *sender) Close() error {
	s.cancel()
	err := s.ep.Shutdown(context.Background())
	s.wg.Wait()
	if rmErr := os.RemoveAll(s.storeDir); err == nil {
		err = rmErr
	}
	return err
}

// A file is one entry of the collection being sent.
type file struct {
	name string // collection name, "/"-separated
	path string // on disk
}

// walk lists the regular files under path, named relative to path's parent.
func walk(path string) ([]file, error) {
	parent := filepath.Dir(path)
	var files []file
	err := filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(parent, p)
		if err != nil {
			return err
		}
		parts := strings.Split(rel, string(filepath.Separator))
		for _, part := range parts {
			if strings.ContainsAny(part, `/\`) {
				return fmt.Errorf("invalid path component %q", part)
			}
		}
		files = append(files, file{name: strings.Join(parts, "/"), path: p})
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(files, func(a, b file) int { return strings.Compare(a.name, b.name) })
	return files, nil
}

// importFiles adds each file to store and stores the collection naming them,
// returning its root hash and the total size of the files.
func importFiles(store *blobs.FSStore, files []file, jobs int) (blobs.Hash, int64, error) {
	entries := make([]blobs.CollectionEntry, len(files))
	var size int64
	var mu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup
	sem := make(chan struct{}, jobs)
	for i, f := range files {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			h, err := store.ImportFile(f.path, blobs.ImportTryReference)
			var n int64
			if fi, statErr := os.Stat(f.path); err == nil {
				err = statErr
				if fi != nil {
					n = fi.Size()
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("error importing %s: %w", f.name, err)
			}
			entries[i] = blobs.CollectionEntry{Name: f.name, Hash: h}
			size += n
		})
	}
	wg.Wait()
	if firstErr != nil {
		return blobs.Hash{}, 0, firstErr
	}
	c := blobs.NewCollection(entries)
	if _, err := store.Add(c.MetadataBytes()); err != nil {
		return blobs.Hash{}, 0, err
	}
	root, err := store.Add(c.HashSequence().Bytes())
	if err != nil {
		return blobs.Hash{}, 0, err
	}
	return root, size, nil
}

// ticketAddr is the endpoint's address, trimmed to what the ticket type asks
// for. A socket bound to the unspecified address is advertised at each of the
// machine's interface addresses, which is what sendme's tickets carry.
func ticketAddr(ep *iroh.Endpoint, tt ticketType) netaddr.EndpointAddr {
	addr := netaddr.NewEndpointAddr(ep.ID())
	if tt == idOnly {
		return addr
	}
	if tt != addressesOnly {
		for _, u := range ep.Addr().RelayURLs() {
			addr = addr.WithRelayURL(u)
		}
	}
	if tt != relayOnly {
		for _, ap := range directAddrs(ep) {
			addr = addr.WithIP(ap)
		}
	}
	return addr
}

// directAddrs is the endpoint's direct addresses. go-iroh leaves a socket
// bound to the unspecified address out of [iroh.Endpoint.Addr], so that socket
// is advertised at each of the machine's interface addresses, as sendme's
// tickets are.
func directAddrs(ep *iroh.Endpoint) []netip.AddrPort {
	out := ep.Addr().IPAddrs()
	bound := ep.LocalAddr()
	if !bound.Addr().IsUnspecified() {
		return out
	}
	ifaces, _ := net.Interfaces()
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, _ := ifc.Addrs()
		for _, a := range addrs {
			pfx, err := netip.ParsePrefix(a.String())
			if err != nil || pfx.Addr().IsLinkLocalUnicast() || bound.Addr().Is4() && !pfx.Addr().Is4() {
				continue
			}
			out = append(out, netip.AddrPortFrom(pfx.Addr(), bound.Port()))
		}
	}
	return out
}

// serve answers blob requests on every connection until ctx is done.
func serve(ctx context.Context, ep *iroh.Endpoint, store blobs.Store) {
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		conn, err := ep.Accept(ctx)
		if err != nil {
			return
		}
		wg.Go(func() {
			defer conn.Close()
			for {
				s, err := conn.AcceptStream(ctx)
				if err != nil {
					return
				}
				wg.Go(func() {
					if err := blobs.ServeBlob(ctx, s, store); err != nil {
						s.CancelRead(1)
						s.CancelWrite(1)
					}
				})
			}
		})
	}
}

// receive fetches the collection a ticket names and writes its files under dst.
// Progress goes to log, the export message to stdout, as sendme does.
func receive(ctx context.Context, ticketStr, dst string, o options, stdout, log io.Writer) error {
	t, err := blobs.ParseTicket(ticketStr)
	if err != nil {
		return fmt.Errorf("parse ticket: %w", err)
	}
	if !t.Format().IsHashSeq() {
		return errors.New("ticket names a raw blob, not a collection")
	}
	addr := t.Addr()
	var opts []iroh.Option
	if len(addr.IPAddrs()) == 0 && len(addr.RelayURLs()) == 0 {
		// A ticket holding only an ID: find the sender through n0's DNS.
		var lookup iroh.AddressLookupServices
		lookup.AddResolver(iroh.N0DNSAddressLookup(&dns.Resolver{}))
		opts = append(opts, iroh.WithAddressLookup(&lookup))
	}
	ep, err := bind(ctx, o, opts...)
	if err != nil {
		return err
	}
	defer ep.Shutdown(context.Background())
	if len(addr.IPAddrs()) == 0 && o.relay != "disabled" {
		// Dialing through a relay needs a home relay of our own first.
		octx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_ = ep.Online(octx)
		cancel()
	}

	start := time.Now()
	conn, err := ep.Connect(ctx, addr, blobs.ALPN)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", addr.ID.Short(), err)
	}
	defer conn.Close()

	root := t.Hash()
	rootBytes, err := fetchSmall(ctx, conn, root)
	if err != nil {
		return fmt.Errorf("fetch collection root: %w", err)
	}
	hs, err := blobs.ParseHashSequence(rootBytes)
	if err != nil {
		return err
	}
	metaHash, ok := hs.At(0)
	if !ok {
		return errors.New("empty collection")
	}
	meta, err := fetchSmall(ctx, conn, metaHash)
	if err != nil {
		return fmt.Errorf("fetch collection names: %w", err)
	}
	c, err := blobs.ParseCollection(hs, meta)
	if err != nil {
		return err
	}
	entries := c.Entries()
	targets := make([]string, len(entries))
	for i, e := range entries {
		if targets[i], err = exportPath(dst, e.Name); err != nil {
			return err
		}
		if _, err := os.Lstat(targets[i]); err == nil {
			return fmt.Errorf("target %s already exists", targets[i])
		}
	}
	fmt.Fprintf(log, "getting collection %s %d files\n", root, len(entries))

	tmp := filepath.Join(dst, ".sendme-recv-"+root.String())
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	size, err := download(ctx, conn, entries, tmp, o.jobs)
	if err != nil {
		return err
	}

	if len(entries) > 0 {
		fmt.Fprintf(stdout, "exporting to %s\n", strings.Split(entries[0].Name, "/")[0])
	}
	for i, e := range entries {
		if o.verbose {
			fmt.Fprintf(stdout, "    %s %s\n", e.Hash, e.Name)
		}
		if _, err := os.Lstat(targets[i]); err == nil {
			return fmt.Errorf("target %s already exists", targets[i])
		}
		if err := os.MkdirAll(filepath.Dir(targets[i]), 0o755); err != nil {
			return err
		}
		if err := os.Rename(filepath.Join(tmp, fmt.Sprint(i)), targets[i]); err != nil {
			return err
		}
	}
	if o.verbose {
		d := time.Since(start)
		fmt.Fprintf(stdout, "downloaded %d files, %s. took %s (%s/s)\n",
			len(entries), humanBytes(size), d.Round(time.Millisecond), humanBytes(int64(float64(size)/d.Seconds())))
	}
	return nil
}

// fetchSmall fetches a blob that must fit in memory.
func fetchSmall(ctx context.Context, conn *iroh.Conn, h blobs.Hash) ([]byte, error) {
	var buf bytes.Buffer
	if err := fetch(ctx, conn, h, &limitWriter{w: &buf, n: maxMetaSize}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// fetch requests one blob on its own stream and writes it to w as it is
// verified.
func fetch(ctx context.Context, conn *iroh.Conn, h blobs.Hash, w io.Writer) error {
	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	defer s.Close()
	return blobs.DownloadBlob(ctx, s, h, w)
}

// download fetches each entry into dir/<index>, jobs at a time, and returns
// the total size.
func download(ctx context.Context, conn *iroh.Conn, entries []blobs.CollectionEntry, dir string, jobs int) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	var size int64
	var firstErr error
	var wg sync.WaitGroup
	sem := make(chan struct{}, jobs)
	for i, e := range entries {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			n, err := downloadFile(ctx, conn, e.Hash, filepath.Join(dir, fmt.Sprint(i)))
			mu.Lock()
			defer mu.Unlock()
			size += n
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("download %s: %w", e.Name, err)
				cancel()
			}
		})
	}
	wg.Wait()
	return size, firstErr
}

func downloadFile(ctx context.Context, conn *iroh.Conn, h blobs.Hash, path string) (int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	err = fetch(ctx, conn, h, f)
	n, _ := f.Seek(0, io.SeekCurrent)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return n, err
}

// exportPath joins a collection name onto root. Names come from the sender,
// so a component that would climb out of root is refused.
func exportPath(root, name string) (string, error) {
	parts := strings.Split(name, "/")
	for _, p := range parts {
		if p == "" || p == "." || p == ".." || strings.ContainsRune(p, '\\') {
			return "", fmt.Errorf("invalid name %q in collection", name)
		}
	}
	return filepath.Join(append([]string{root}, parts...)...), nil
}

// limitWriter fails a write that would take it past n bytes.
type limitWriter struct {
	w io.Writer
	n int64
}

func (l *limitWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > l.n {
		return 0, errors.New("blob too large")
	}
	l.n -= int64(len(p))
	return l.w.Write(p)
}

// humanBytes formats n the way sendme does, in binary units.
func humanBytes(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	f := float64(n)
	for _, unit := range []string{"KiB", "MiB", "GiB", "TiB"} {
		f /= 1024
		if f < 1024 || unit == "TiB" {
			return fmt.Sprintf("%.2f %s", f, unit)
		}
	}
	panic("unreachable")
}

// demo sends a small directory and receives it into another, over loopback
// with relays disabled.
func demo(stdout io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	work, err := os.MkdirTemp("", "go-iroh-sendme-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(work)

	src := filepath.Join(work, "src", "photos")
	for name, body := range map[string]string{
		"a.txt":        "the first file\n",
		"trip/b.txt":   "a file in a subdirectory\n",
		"trip/c/d.txt": "and one further down\n",
	} {
		p := filepath.Join(src, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			return err
		}
	}

	o := options{relay: "disabled", ipv6: "[::1]:0", jobs: 4}
	if o.secret, err = key.GenerateSecretKey(); err != nil {
		return err
	}
	s, err := startSend(ctx, src, filepath.Join(work, ".sendme-send"), o, stdout)
	if err != nil {
		return err
	}
	defer s.Close()

	dst := filepath.Join(work, "dst")
	if err := os.Mkdir(dst, 0o755); err != nil {
		return err
	}
	if o.secret, err = key.GenerateSecretKey(); err != nil {
		return err
	}
	if err := receive(ctx, s.ticket.String(), dst, o, stdout, stdout); err != nil {
		return err
	}
	return filepath.WalkDir(dst, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dst, p)
		fmt.Fprintf(stdout, "received %s: %q\n", filepath.ToSlash(rel), b)
		return nil
	})
}
