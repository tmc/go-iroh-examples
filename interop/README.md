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
