# Rust interoperability harness

The Go examples claim to speak protocols the Rust implementation defines. This
directory is how that claim is tested against the Rust implementation itself
rather than against another Go program.

Nothing here is a copy of upstream. `framed-messages` is a git dependency on
[n0-computer/iroh-examples](https://github.com/n0-computer/iroh-examples), so
the peer is upstream's own code and `Cargo.lock` records which commit was
verified; `dumbpipe` is the published crate, and the ALPN and handshake come
from `dumbpipe::ALPN` and `dumbpipe::HANDSHAKE` rather than being written out
again here.

Three binaries:

    cargo build

- `vectors` prints hex: each chess move's wire encoding, and dumbpipe's ALPN and
  handshake. Those bytes are pinned in the two `interop_test.go` files, so the
  Go tests keep checking the wire format with no Rust toolchain present.
- `peer` is a live iroh peer speaking ALPN `iroh/examples/messages/0` on
  loopback with relays disabled:

      peer listen                  print "ADDR <id> <ip:port>", then play black
      peer connect <id> <ip:port>  dial that address and play white

- `dumbpipe_peer` is a live iroh peer speaking ALPN `DUMBPIPEV0`, the protocol
  `go-iroh-dumbpipe` implements. The dialer writes the handshake before any
  payload and the listener refuses the stream without it, so each direction
  tests the check the other side makes:

      dumbpipe_peer listen                       echo one stream back
      dumbpipe_peer connect <id> <ip:port> <s>   dial, send s, print the reply

To run the live tests, point the Go tests at the built binary:

    cargo build --manifest-path interop/Cargo.toml
    IROH_EXAMPLE_RUST_PEER=$PWD/interop/target/debug/peer \
        go test ./cmd/go-iroh-framed-messages/ -run TestInterop -v
    IROH_EXAMPLE_RUST_DUMBPIPE=$PWD/interop/target/debug/dumbpipe_peer \
        go test ./cmd/go-iroh-dumbpipe/ -run TestInterop -v

Both directions are covered for both protocols: Go dialing a Rust server, and
Rust dialing a Go server. Without the variables the live tests skip and the
pinned-vector tests still run.

The command-line examples are also run against the released Rust tools, so
that `go-iroh-sendme` and `go-iroh-dumbpipe` are tested as programs, not only as
protocols. Install the pinned versions into `interop/target`, which is ignored:

    cargo install --locked --root interop/target/cli sendme@0.36.0 dumbpipe@0.39.0
    IROH_EXAMPLE_RUST_SENDME=$PWD/interop/target/cli/bin/sendme \
        go test ./cmd/go-iroh-sendme/ -run TestInterop -v
    IROH_EXAMPLE_RUST_DUMBPIPE_CLI=$PWD/interop/target/cli/bin/dumbpipe \
        go test ./cmd/go-iroh-dumbpipe/ -run TestInteropCLI -v

The sendme tests send a directory each way, including an empty file and a file
that ends mid-chunk, and compare every received byte. The dumbpipe tests pipe
stdio each way and tunnel TCP and Unix sockets each way, with the Rust binary
on one end and the Go code on the other.

Note that both peers bind `127.0.0.1`, so a Go endpoint dialing one must bind
IPv4 too — an endpoint on `::1` has no route to an IPv4 loopback peer.

## iroh-ping

The crates from here on are standalone, each with its own `Cargo.lock`, so
one example's dependencies never move another's. The commands below build
each into its own `target`; pointing `CARGO_TARGET_DIR` at one shared
directory instead lets them compile iroh once (adjust the binary paths to
match).

`ping/` is a standalone crate, built separately from the one above, holding a
peer made from n0's `iroh-ping` crate (1.0.0, on iroh 1.x). Its listener is
the crate's `Ping` handler behind a `Router` and its dialer is `Ping::ping`,
both of which assert on the PING and PONG bytes, so `go-iroh-ping` is checked
against upstream's code rather than a restatement of it.

    ping_peer vectors                 print iroh_ping::ALPN as hex
    ping_peer listen                  print "ADDR <id> <ip:port>", serve ping
    ping_peer connect <id> <ip:port>  ping that endpoint, print "PONG <rtt>"

To build it and run the live tests:

    cargo build --manifest-path interop/ping/Cargo.toml
    IROH_EXAMPLE_RUST_PING=$PWD/interop/ping/target/debug/ping_peer \
        go test ./cmd/go-iroh-ping/ -run 'TestInterop|TestRust' -v

Without the variable only the pinned ALPN test runs.

## Automerge

`automerge/` is a separate crate for `go-iroh-automerge`, which ports n0's
[iroh-automerge](https://github.com/n0-computer/iroh-examples/tree/main/iroh-automerge)
(ALPN `iroh/automerge/2`, each sync message behind an eight-byte
little-endian length, zero meaning "done"). iroh-automerge is a binary crate,
so cargo will fetch it as a git dependency but not link it (hence the
"missing a lib target" warning); `build.rs` copies its `src/protocol.rs`
from cargo's checkout at the commit `automerge/Cargo.lock` records, and the
peer compiles that file unchanged. Rust automerge is 0.7.4, as upstream's
lock has it; the Go side's automerge-go embeds Rust automerge 0.5.0.

    automerge_peer listen                  print "ADDR <id> <ip:port>", respond to one sync
    automerge_peer connect <id> <ip:port>  dial that address and initiate a sync
    automerge_peer vectors                 print the ALPN and an empty document's first sync message

Each side starts with its own keys plus a conflicting `shared` key; the tests
check that after one sync both print the same seven-key document.

    cargo build --manifest-path interop/automerge/Cargo.toml
    IROH_EXAMPLE_RUST_AUTOMERGE=$PWD/interop/automerge/target/debug/automerge_peer \
        go test ./cmd/go-iroh-automerge/ -run 'TestInterop|TestRust' -v

## iroh-blobs, tickets and key exchange

`blobs/` is a separate crate with one binary, `blobs_peer`, for the four
examples whose claims rest on iroh-blobs 0.103, iroh-tickets 1.0 and postcard
1.1, on the same iroh (1.1.0) that `Cargo.lock` pins:

- `go-iroh-blobs-transfer`: iroh-blobs' `get_blob` fetches from the example's
  provider, and the example fetches from iroh-blobs' `BlobsProtocol`, for blobs
  of one partial chunk, one crossing a 16 KiB block, and several blocks.
- `go-iroh-blobs-gateway`: all four HTTP routes, and Range requests, in front
  of a Rust provider, reached by address and by Rust-printed blob tickets.
- `go-iroh-tickets`: each side dials a ticket the other printed. IPv4 passes;
  the IPv6 cases skip, naming go-iroh's endpointticket IPv6 bug.
- `go-iroh-key-exchange`: the request and report postcard encodings are pinned
  against `blobs_peer kx-vectors` (no Rust needed), and the exchange runs live
  against Rust endpoints on stock iroh (ring, classical groups only) and on
  aws-lc-rs providers offering only X25519 or only X25519MLKEM768.

To run them:

    cargo build --manifest-path interop/blobs/Cargo.toml
    IROH_EXAMPLE_RUST_BLOBS=$PWD/interop/blobs/target/debug/blobs_peer \
        go test ./cmd/go-iroh-blobs-transfer/ ./cmd/go-iroh-blobs-gateway/ \
            ./cmd/go-iroh-tickets/ ./cmd/go-iroh-key-exchange/ -run 'Interop|Rust' -v

## Gossip

`gossip/` is a separate crate, so iroh-gossip and iroh-smol-kv build only when
these tests are wanted. Both come from crates.io (`iroh-gossip` 0.101,
`iroh-smol-kv` 0.4, on iroh 1.1); `gossip/Cargo.lock` records the versions
verified. One binary, `gossip_peer`, binds `127.0.0.1` with relays disabled
and prints `ADDR <id> <ip:port>` then `READY`:

    gossip_peer topic <topic-hex> [<id> <ip:port>]  an iroh-gossip topic
    gossip_peer kv <topic-hex> [<id> <ip:port>]     an iroh-smol-kv store on it
    gossip_peer kv-vector                           one smol-kv message, as hex

The live modes join the optional bootstrap peer, report `UP`, `RECV` and (for
kv) `ENTRY` lines on stdout, and take `join`, `broadcast` and `put` commands
on stdin. The tests wait on those lines rather than sleeping.

    cargo build --manifest-path interop/gossip/Cargo.toml
    IROH_EXAMPLE_RUST_GOSSIP=$PWD/interop/gossip/target/debug/gossip_peer \
        go test ./cmd/go-iroh-gossip-topic/ ./cmd/go-iroh-gossip-kv/ -run TestInterop -v

Each test runs both ways round, Rust joining Go and Go joining Rust: for
gossip-topic each side must receive the other's broadcast, and for gossip-kv
each side's signed write must land in the other's store. `kv-vector`'s output
is pinned in `cmd/go-iroh-gossip-kv/interop_test.go`, so the update encoding
is checked without Rust too.

## mDNS discovery

`interop/mdns` is a separate crate, so its dependencies do not touch the
main one. It builds `mdns_peer` on n0's `iroh-mdns-address-lookup` 0.6.0
(swarm-discovery 0.6.3, iroh 1.1.0), which is where iroh's mDNS lookup lives
since 1.0. Every endpoint has relays disabled and mDNS as its only address
lookup, on the default service name `irohv1`, so a dial by ID alone succeeds
only if mDNS supplied the address:

    mdns_peer listen          print "ID <id>", then echo one stream
    mdns_peer dial <id>       dial <id> by ID alone and echo "mdns hello"
    mdns_peer announce        announce two ports, a relay URL and user data
    mdns_peer resolve <id>    print what mDNS resolves for <id>

    cargo build --manifest-path interop/mdns/Cargo.toml
    IROH_EXAMPLE_RUST_MDNS=$PWD/interop/mdns/target/debug/mdns_peer \
        go test ./cmd/go-iroh-mdns-discovery/ -run 'Rust' -v

The tests need UDP 5353 and a multicast-capable default interface. Rust
multicasts on the default interface only, and the two processes hear each
other through multicast loopback. With go-iroh v0.2.1, Go finds and dials
Rust, but Rust cannot resolve Go and Go keeps only one of Rust's ports; the
test comments name the go-iroh bugs.
