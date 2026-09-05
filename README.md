# go-iroh examples

Runnable examples for [`github.com/tmc/go-iroh`](https://github.com/tmc/go-iroh),
built against the version pinned in `go.mod`.

Run one:

```sh
go run ./cmd/go-iroh-direct-echo
```

Run all of them:

```sh
go test ./... -count=1
```

Every example is a test. `go test` runs each one on loopback and checks what it
prints, so the suite is the examples rather than a description of them. The
suite also holds the examples to the conventions this file describes and checks
that the tables below still list the tree, so a page about these examples can
be generated instead of transcribed; `examples.json` is that generated
description. The handful that need a public relay, DNS, or pkarr relay skip
themselves unless the matching switch in
[Examples that need the network](#examples-that-need-the-network) is set.

Each `main.go` is a whole program: it imports go-iroh and the standard library
and nothing from this repository, so a reader who copies one file has copied
something that compiles. The tests do share helpers, because a reader copies
the example and not its test. `go test` checks that too.

## Progression

The tables below are the reading order. Start at the top and stop wherever the
answer you came for is.

They are also where that order is defined: `internal/catalog` reads the
progression from this section, and `go test` fails if the tree and these tables
disagree. An example is added by creating its directory and adding a row, and
moved by moving its row. Nothing in a directory name encodes a position, so
`cmd/README.md` records the two renamings there have been and there is no
reason to expect a third.

### Identity and addressing

| Example | Shows |
|---|---|
| `go-iroh-keys` | endpoint identity: `key.SecretKey`, `key.EndpointID`, signatures |
| `go-iroh-addresses` | building a `netaddr.EndpointAddr` from an ID, IPs, and relay URLs |
| `go-iroh-tickets` | handing an address to a peer as a Rust-compatible ticket, and wrapping one in an application envelope |

### Connections

| Example | Shows |
|---|---|
| `go-iroh-direct-echo` | two loopback endpoints and one bidirectional stream |
| `go-iroh-router-echo` | ALPN dispatch through `iroh.Router` |
| `go-iroh-multi-alpn` | one router serving two application protocols |
| `go-iroh-alpn-negotiation` | agreeing on a protocol version, and what a mismatch looks like |
| `go-iroh-custom-router` | dispatching ALPNs yourself, so protocols can come and go at runtime |
| `go-iroh-manual-incoming` | owning the accept loop: `AcceptIncoming`, `Accepting.ALPN` |
| `go-iroh-incoming-filter` | admission control with `RouterConfig.IncomingFilter` and `AcceptingHandler.OnAccepting` |
| `go-iroh-source-validation` | QUIC Retry source-address validation |

### Observing a connection

| Example | Shows |
|---|---|
| `go-iroh-hooks` | observing dials and handshakes with `EndpointHooks` |
| `go-iroh-metrics` | endpoint counters after a connection |
| `go-iroh-close-codes` | reading a peer's application close code with `AsApplicationError` |
| `go-iroh-watch-observer` | `watch.Value` and `Endpoint.WatchAddr`: Current, Updated, Stream |
| `go-iroh-qlog-tracing` | per-endpoint QUIC traces with `WithQLOG`, `QLOGDir`, and `QLOGDIR` |

### Moving bytes

| Example | Shows |
|---|---|
| `go-iroh-uni-streams` | one unidirectional stream per event |
| `go-iroh-datagram-frames` | an application's own framing over QUIC datagrams |
| `go-iroh-datagram-vs-stream` | `Conn.MaxDatagramSize` and falling back to a stream |
| `go-iroh-framed-messages` | length-prefixed messages on one bidirectional stream |
| `go-iroh-stream-netconn` | streams as `net.Conn`, with deadlines |
| `go-iroh-stream-listener` | `Endpoint.ListenStreams` and `iroh.NewStreamListener` under `net/http` |
| `go-iroh-transport-tuning` | keepalive and idle timeout with `QUICTransportConfig` |
| `go-iroh-graceful-shutdown` | draining router handlers on SIGINT before closing |

### Finding peers

| Example | Shows |
|---|---|
| `go-iroh-memory-discovery` | `iroh.MemoryLookup`, the in-process lookup a test wants |
| `go-iroh-mdns-discovery` | finding a peer on the local link, with no infrastructure at all |
| `go-iroh-address-filtering` | `RelayOnlyFilter`, `IPOnlyFilter`, and a filter of your own |
| `go-iroh-dns-resolve` | resolving an ID through n0's DNS origin |
| `go-iroh-pkarr-publish-resolve` | publishing to and resolving from n0's pkarr relay |
| `go-iroh-pkarr-packet` | building and verifying a pkarr signed packet, with no relay involved |
| `go-iroh-relay-online` | the default relay map and `Endpoint.Online` |
| `go-iroh-path-upgrade` | watching a relayed connection move to a direct path |
| `go-iroh-path-selection` | replacing the policy that chooses a path, and dialing relay-first |
| `go-iroh-net-report` | what `Endpoint.NetReport` says about the local network |
| `go-iroh-local-infra` | running your own relay and pkarr relay on loopback |
| `go-iroh-relay-limits` | rate-limiting a relay you run |

### Protocols

| Example | Shows |
|---|---|
| `go-iroh-blobs-transfer` | BAO-verified blob transfer, the `sendme` shape |
| `go-iroh-blobs-ranges` | resumable byte-range fetches from a blob |
| `go-iroh-blobs-store` | an on-disk store, downloads from several providers, tags and GC |
| `go-iroh-blobs-gateway` | an HTTP Range gateway backed by blobs |
| `go-iroh-gossip-topic` | broadcasting to a topic with `gossip.Gossip` |
| `go-iroh-gossip-kv` | signed key-value updates over a gossip topic |
| `go-iroh-docs-sync` | multi-writer documents and range sync with `docs` |
| `go-iroh-rpc-workqueue` | `irpc.Call` and `irpc.Handler` |
| `go-iroh-ping` | the smallest custom protocol: ALPN `iroh/ping/0` |
| `go-iroh-automerge` | Automerge CRDT sync over a protocol handler |

### Tools

| Example | Shows |
|---|---|
| `go-iroh-dumbpipe` | piping stdin to stdout over iroh, wire-compatible with Rust `dumbpipe` |
| `go-iroh-doctor` | relay status, net report, latencies, and selected path |
| `go-iroh-public-endpoint` | binding a reachable UDP address, and dialing one by ID plus coordinates |

### Configuration

| Example | Shows |
|---|---|
| `go-iroh-key-exchange` | choosing TLS key-exchange groups with `WithKeyExchangePolicy` |
| `go-iroh-custom-transport` | carrying iroh datagrams over a transport of your own |

## Examples that need the network

Everything except the six below runs entirely on loopback. These need something
outside the machine, take their configuration from flags whose defaults come
from the environment, and their tests skip with a message naming what to set.

| Example | Flags | Environment |
|---|---|---|
| `go-iroh-dns-resolve` | `-endpoint-id`, `-dns-origin` | `IROH_EXAMPLE_ENDPOINT_ID`, `IROH_EXAMPLE_DNS_ORIGIN` |
| `go-iroh-pkarr-publish-resolve` | `-live` | `GO_IROH_LIVE_PKARR` |
| `go-iroh-relay-online` | `-live` | `GO_IROH_LIVE_RELAY` |
| `go-iroh-net-report` | `-live` | `GO_IROH_LIVE_RELAY` |
| `go-iroh-public-endpoint` | `-port`, `-alpn`, `-serve`, `-live`, `-peer-id`, `-peer-ip`, `-peer-relay` | `IROH_EXAMPLE_PORT`, `IROH_EXAMPLE_ALPN`, `IROH_EXAMPLE_SERVE`, `GO_IROH_LIVE_RELAY`, `IROH_EXAMPLE_PEER_ID`, `IROH_EXAMPLE_PEER_IP`, `IROH_EXAMPLE_PEER_RELAY` |

Flags win over the environment, and `-h` lists each flag with the variable it
falls back to. Two loopback examples also take configuration:
`go-iroh-blobs-transfer` takes `-file` (`IROH_EXAMPLE_FILE`) to serve a real file
instead of its embedded payload, and `go-iroh-dumbpipe` takes `-alpn`, `-bind`
(`GO_IROH_DUMBPIPE_BIND_ADDR`), `-advertise`
(`GO_IROH_DUMBPIPE_ADVERTISE_ADDR`), `-relay`, `-no-relay`, `-key`, and
`-ticket`.

`go-iroh-doctor` also takes `-live` (`GO_IROH_LIVE_RELAY`), but its default path
diagnoses an in-process relay and needs no network.

Serve from one machine and dial from another:

```sh
go run ./cmd/go-iroh-public-endpoint listen -port 4433 -serve
go run ./cmd/go-iroh-public-endpoint connect -peer-id <id> -peer-ip <host:port>
```

## Rust interoperability

Ticket strings and several ALPNs are shared with the Rust implementation, so
these examples interoperate with n0's tools rather than imitating them.

`go-iroh-dumbpipe` speaks ALPN `DUMBPIPEV0`, the `hello` stream handshake, and
`iroh-tickets` endpoint tickets. Go listener, Rust dialer:

```sh
go run ./cmd/go-iroh-dumbpipe listen
# copy the printed ticket, then in another shell:
printf 'hello from rust\n' | dumbpipe connect <ticket>
```

Rust listener, Go dialer:

```sh
dumbpipe listen
printf 'hello from go\n' | go run ./cmd/go-iroh-dumbpipe connect <ticket>
```

Both directions are tested against Rust `dumbpipe` rather than checked by hand:
`interop/` builds a peer from the published `dumbpipe` crate, taking the ALPN
and the handshake from the crate's own constants. The listener advertises a
public relay by default, so the printed ticket is usable
from another machine; `-no-relay` keeps it local. `-alpn` pipes bytes under any
other protocol name and skips the dumbpipe handshake, which is netcat over iroh:

```sh
go run ./cmd/go-iroh-dumbpipe listen -alpn MYAPPV0 -key ./pipe.key -ticket ./pipe.ticket
go run ./cmd/go-iroh-dumbpipe connect -alpn MYAPPV0 "$(cat ./pipe.ticket)"
```

`-key` keeps the endpoint ID stable across restarts; `-ticket` writes the
current ticket to a file.

Ported from n0's corpus: `go-iroh-blobs-transfer` (sendme), `go-iroh-blobs-gateway`
(iroh-gateway), `go-iroh-gossip-kv` (iroh-smol-kv), `go-iroh-ping` (iroh-ping),
`go-iroh-automerge` (iroh-automerge), `go-iroh-doctor` (iroh-doctor), and
`go-iroh-framed-messages`. `go-iroh-docs-sync` speaks `/iroh-sync/1`, and the three
blobs examples speak `/iroh-bytes/4`.

`go-iroh-framed-messages` and `go-iroh-dumbpipe` are checked against the Rust
implementations rather than described as compatible with them: `interop/` builds
n0's own `framed-messages` crate and the published `dumbpipe` crate as live
peers, and both directions of each are tested. See
[interop/README.md](interop/README.md).

## What is not here

Some exported APIs are configuration knobs rather than workflows, and are left
to package documentation: `WithBindAddrOpts` and `NewSessionCache`. `WithNATPMP`
needs a gateway to talk to, and every example here runs on loopback.
`WithDNSResolver` is a wrapper around `WithAddressLookup`, which
`go-iroh-dns-resolve` uses directly.

Cross-host examples that need two machines live in `go-iroh-experiments`. The
compatibility matrix that runs unmodified upstream binaries in pinned Docker
images lives in the go-iroh repository; `interop/` here is the narrower thing,
one protocol checked against upstream's own code.
