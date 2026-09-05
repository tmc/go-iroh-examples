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

Note that both peers bind `127.0.0.1`, so a Go endpoint dialing one must bind
IPv4 too — an endpoint on `::1` has no route to an IPv4 loopback peer.
