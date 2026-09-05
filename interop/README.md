# Rust interoperability harness

The Go examples claim to speak protocols the Rust implementation defines. This
directory is how that claim is tested against the Rust implementation itself
rather than against another Go program.

`framed-messages` here is a git dependency on
[n0-computer/iroh-examples](https://github.com/n0-computer/iroh-examples), not a
vendored copy, so the peer is upstream's own code and `Cargo.lock` records which
commit was verified.

Two binaries:

    cargo build

- `vectors` prints the wire encoding of each chess move as hex. Those bytes are
  pinned in `cmd/go-iroh-framed-messages/interop_test.go`, so the Go tests keep
  checking the frame format with no Rust toolchain present.
- `peer` is a live iroh peer speaking ALPN `iroh/examples/messages/0` on
  loopback with relays disabled:

      peer listen                  print "ADDR <id> <ip:port>", then play black
      peer connect <id> <ip:port>  dial that address and play white

To run the live tests, point the Go tests at the built binary:

    cargo build --manifest-path interop/Cargo.toml
    IROH_EXAMPLE_RUST_PEER=$PWD/interop/target/debug/peer \
        go test ./cmd/go-iroh-framed-messages/ -run TestInterop -v

Both directions are covered: Go dialing a Rust server, and Rust dialing a Go
server. Without the variable the live tests skip and the pinned-vector test
still runs.

Note that `peer` binds `127.0.0.1`, so a Go endpoint dialing it must bind IPv4
too — an endpoint on `::1` has no route to an IPv4 loopback peer.
