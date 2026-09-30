package main

// Interoperability with n0's mDNS address lookup, the Rust crate
// iroh-mdns-address-lookup that this example's default service name comes
// from.
//
// These tests live outside main_test.go on purpose. internal/catalog reads
// main_test.go to decide what an example needs in order to run; a Rust
// toolchain is a property of the checkout, not of the example.
//
// Unlike the example, which picks a per-run service name so that it talks only
// to itself, every test here uses the default. That default is the claim under
// test: a Go endpoint and a Rust endpoint that leave the service name alone
// find each other by ID.

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/netip"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/iroh/mdns"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// TestRustConstants pins the names Go shares with iroh-mdns-address-lookup
// 0.6.0. The Rust service name is the private constant N0_SERVICE_NAME
// (src/lib.rs:83) and so is copied here rather than read from Rust; the live
// tests below are what check it on the wire. NAME, the provenance of every
// item Rust resolves, is public, and TestInteropRustResolvesGo reads it from a
// Rust resolve.
func TestRustConstants(t *testing.T) {
	for _, tc := range []struct{ name, got, want string }{
		{"service name", mdns.DefaultServiceName, "irohv1"},
		{"provenance", mdns.Provenance, "mdns"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want the Rust %q", tc.name, tc.got, tc.want)
		}
	}
}

// mdnsPeer is the Rust peer built from interop/mdns. Without it the live tests
// skip.
func mdnsPeer(t *testing.T) string {
	t.Helper()
	path := os.Getenv("IROH_EXAMPLE_RUST_MDNS")
	if path == "" {
		t.Skip("set IROH_EXAMPLE_RUST_MDNS to the Rust mdns_peer binary to run live interop")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("IROH_EXAMPLE_RUST_MDNS: %v", err)
	}
	return path
}

// startPeer runs the Rust peer with args and returns the ID it prints before
// READY. The peer is killed when the test ends.
func startPeer(t *testing.T, ctx context.Context, bin string, args ...string) key.EndpointID {
	t.Helper()
	cmd := exec.CommandContext(ctx, bin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	id, err := readPeerID(stdout)
	if err != nil {
		t.Fatalf("reading the Rust peer's id: %v", err)
	}
	// Keep draining so the peer never blocks on a full pipe.
	go io.Copy(io.Discard, stdout)
	return id
}

// readPeerID reads the "ID <id>" line the peer prints before READY.
func readPeerID(r io.Reader) (key.EndpointID, error) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := sc.Text()
		if line == "READY" {
			break
		}
		if rest, ok := strings.CutPrefix(line, "ID "); ok {
			return key.ParseEndpointID(rest)
		}
	}
	if err := sc.Err(); err != nil {
		return key.EndpointID{}, err
	}
	return key.EndpointID{}, errors.New("no ID line before READY")
}

// startMDNS runs d until the test ends, failing the test if it cannot join
// the multicast group.
func startMDNS(t *testing.T, ctx context.Context, d *mdns.Discovery) {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	listen := make(chan error, 1)
	go func() { listen <- d.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-listen
	})
	if stopped, err := waitListening(listen); stopped {
		t.Fatalf("mDNS listener stopped: %v; %s", err, needsMulticast)
	}
}

// bind4 binds IPv4 loopback rather than using the example's bind, which takes
// IPv6: the Rust peer binds 127.0.0.1, and an endpoint on ::1 has no route to
// it.
func bind4(ctx context.Context, opts ...iroh.Option) (*iroh.Endpoint, error) {
	all := append([]iroh.Option{iroh.WithBindAddr(
		netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 0))}, opts...)
	return iroh.Bind(ctx, all...)
}

// TestInteropGoDialsRust dials a Rust endpoint knowing only its ID. The Go
// endpoint has no relay and no address lookup but mDNS, so the address it
// dials can only have come from Rust's mDNS announcement or its answer to
// Go's query.
func TestInteropGoDialsRust(t *testing.T) {
	bin := mdnsPeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	id := startPeer(t, ctx, bin, "listen")
	t.Logf("rust peer %s", id.Short())

	clientKey, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	seeker := mdns.New(clientKey.Public().EndpointID(),
		mdns.WithPassive(true),
		mdns.WithLookupTimeout(discoverWait),
	)
	var lookups iroh.AddressLookupServices
	lookups.AddResolver(seeker)
	client, err := bind4(ctx, iroh.WithSecretKey(clientKey), iroh.WithAddressLookup(&lookups))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Shutdown(context.Background())
	startMDNS(t, ctx, seeker)

	conn, err := client.Connect(ctx, netaddr.NewEndpointAddr(id), alpn)
	if err != nil {
		t.Fatalf("dialing the Rust peer by id: %v", err)
	}
	defer conn.CloseWithError(0, "")
	reply, err := exchange(ctx, conn, "mdns hello")
	if err != nil {
		t.Fatalf("exchange: %v", err)
	}
	if reply != "mdns hello" {
		t.Errorf("Rust echoed %q, want %q", reply, "mdns hello")
	}
}

// TestInteropRustDialsGo has a Rust endpoint dial this Go endpoint knowing
// only its ID, which it can resolve only through Go's mDNS announcement.
//
// With go-iroh v0.2.1 it fails: Rust hears nothing it can use. Go writes the
// A and AAAA records in the answer section, where swarm-discovery does not
// look for them, and an announcement with no relay URL or user data carries
// an empty TXT record, which hickory-proto rejects outright.
func TestInteropRustDialsGo(t *testing.T) {
	bin := mdnsPeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	serverKey, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	announcer := mdns.New(serverKey.Public().EndpointID())
	var lookups iroh.AddressLookupServices
	lookups.AddPublisher(announcer)
	server, err := bind4(ctx,
		iroh.WithSecretKey(serverKey),
		iroh.WithALPNs(alpn),
		iroh.WithAddressLookup(&lookups),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())
	startMDNS(t, ctx, announcer)
	announcer.Publish(dns.EndpointDataFromAddr(server.Addr()))

	accepted := make(chan error, 1)
	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			accepted <- err
			return
		}
		accepted <- echo(ctx, conn)
	}()

	out, err := exec.CommandContext(ctx, bin, "dial", server.ID().String()).CombinedOutput()
	if err != nil {
		t.Fatalf("rust peer: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "reply: mdns hello") {
		t.Errorf("Rust peer did not get the echo\n%s", out)
	}
	if err := <-accepted; err != nil {
		t.Errorf("serving the Rust peer: %v", err)
	}
}

// What the Rust peer's announce command publishes. The relay URL and user data
// travel in TXT attributes, so resolving them checks the attribute keys, which
// are private constants in Rust. The two addresses have different ports, which
// Rust announces as two SRV records for one instance, as it does for any
// endpoint bound to both 0.0.0.0 and [::].
var rustAnnounceAddrs = []string{"127.0.0.1:4242", "[::1]:4243"}

const (
	rustAnnounceRelay    = "https://relay.example.com./"
	rustAnnounceUserData = "rust-announce"
)

// TestInteropGoResolvesRust resolves a Rust announcement that carries two
// ports, a relay URL, and user data.
//
// With go-iroh v0.2.1 it fails on the addresses: parseAnnouncement keeps one
// SRV record per instance, so only the last port survives.
func TestInteropGoResolvesRust(t *testing.T) {
	bin := mdnsPeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	id := startPeer(t, ctx, bin, "announce")
	seekerKey, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	seeker := mdns.New(seekerKey.Public().EndpointID(),
		mdns.WithPassive(true),
		mdns.WithLookupTimeout(discoverWait),
	)
	startMDNS(t, ctx, seeker)

	item, ok, err := discover(ctx, seeker, id)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("no mDNS answer for the Rust peer in %s", discoverWait)
	}
	data := item.EndpointInfo().Data
	var got []string
	for _, ap := range data.IPAddrs() {
		got = append(got, ap.String())
	}
	slices.Sort(got)
	if !slices.Equal(got, rustAnnounceAddrs) {
		t.Errorf("addrs = %v, want %v", got, rustAnnounceAddrs)
	}
	if got := data.RelayURLs(); len(got) != 1 || got[0].String() != rustAnnounceRelay {
		t.Errorf("relay urls = %v, want [%s]", got, rustAnnounceRelay)
	}
	if u, ok := item.UserData(); !ok || u.String() != rustAnnounceUserData {
		t.Errorf("user data = %q, %v, want %q", u, ok, rustAnnounceUserData)
	}
}

// TestInteropRustResolvesGo has Rust resolve a Go announcement that carries an
// address, a relay URL, and user data.
//
// With go-iroh v0.2.1 it fails because Go writes the A record in the answer
// section; see TestInteropRustDialsGo.
func TestInteropRustResolvesGo(t *testing.T) {
	bin := mdnsPeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	k, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	relay, err := netaddr.ParseRelayURL("https://relay.example.org./")
	if err != nil {
		t.Fatal(err)
	}
	user, err := dns.NewUserData("go-announce")
	if err != nil {
		t.Fatal(err)
	}
	data := dns.NewEndpointData().
		WithIPAddrs(netip.MustParseAddrPort("127.0.0.1:4343")).
		WithRelayURL(relay).
		WithUserData(&user)

	announcer := mdns.New(k.Public().EndpointID())
	startMDNS(t, ctx, announcer)
	announcer.Publish(data)

	out, err := exec.CommandContext(ctx, bin, "resolve", k.Public().EndpointID().String()).CombinedOutput()
	if err != nil {
		t.Fatalf("rust peer: %v\n%s", err, out)
	}
	for _, want := range []string{
		"provenance: " + mdns.Provenance,
		"addr: 127.0.0.1:4343",
		"relay: " + relay.String(),
		"user-data: go-announce",
	} {
		if !strings.Contains(string(out), want+"\n") {
			t.Errorf("Rust resolve missing %q\n%s", want, out)
		}
	}
}
