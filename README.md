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
something that compiles. The tests hold to the same rule: an example's output
is asserted by handing `run` a `bytes.Buffer`, so a reader who copies the test
has copied something that compiles too. `go test` checks that.

## Progression

The tables below are the reading order. Start at the top and stop wherever the
answer you came for is.

They are also where that order is defined: `internal/catalog` reads the
progression from this section, and `go test` fails if the tree and these tables
disagree. An example is added by creating its directory and adding a row, and
moved by moving its row. Nothing in a directory name encodes a position, so
adding an example displaces nothing and the names are stable. The `go-iroh-`
prefix is there for one reason: `go install ./cmd/...` names each binary after
its directory, and a bare `doctor` or `dumbpipe` on somebody's PATH is not
this repository's to claim.

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
| `go-iroh-quic-surface` | `quicconn`: iroh streams and datagrams as the surface an HTTP/3 stack expects |
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
| `go-iroh-blobs-transfer` | BAO-verified blob transfer, the core of `sendme` |
| `go-iroh-blobs-ranges` | resumable byte-range fetches from a blob |
| `go-iroh-blobs-store` | an on-disk store, downloads from several providers, tags and GC |
| `go-iroh-blobs-gateway` | an HTTP Range gateway backed by blobs |
| `go-iroh-gossip-topic` | broadcasting to a topic with `gossip.Gossip` |
| `go-iroh-gossip-kv` | signed key-value updates over a gossip topic |
| `go-iroh-docs-sync` | multi-writer documents and range sync with `docs` |
| `go-iroh-docs-live-sync` | `docs.StartLiveSync`: replicas that update as they are written, and a store on disk |
| `go-iroh-rpc-workqueue` | `irpc.Call` and `irpc.Handler` |
| `go-iroh-ping` | the smallest custom protocol: ALPN `iroh/ping/0` |
| `go-iroh-automerge` | Automerge CRDT sync over a protocol handler |

### Tools

| Example | Shows |
|---|---|
| `go-iroh-dumbpipe` | n0's `dumbpipe`: stdio, TCP, and Unix-socket pipes over iroh |
| `go-iroh-sendme` | n0's `sendme`: sending a file or directory as an iroh-blobs collection |
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
instead of its embedded payload. `go-iroh-dumbpipe` and `go-iroh-sendme` run a
loopback demo with no arguments and otherwise take the Rust tools' commands and
flags, listed under Rust interoperability below; `IROH_SECRET` sets the
endpoint's secret key for both, as it does for the Rust tools.

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

`go-iroh-dumbpipe` and `go-iroh-sendme` are n0's
[`dumbpipe`](https://github.com/n0-computer/dumbpipe) 0.39 and
[`sendme`](https://github.com/n0-computer/sendme) 0.36: the same commands, the
same flags spelled Go's way, the same output, and the same wire protocols.
Tickets cross between the implementations, with one exception noted below. Go
listener, Rust dialer:

```sh
go run ./cmd/go-iroh-dumbpipe listen
# copy the printed ticket, then in another shell:
printf 'hello from rust\n' | dumbpipe connect <ticket>
```

Rust sender, Go receiver:

```sh
sendme send ./photos
go run ./cmd/go-iroh-sendme receive <ticket>
```

| `dumbpipe` | `go-iroh-dumbpipe` |
|---|---|
| `generate-ticket` | `generate-ticket` |
| `listen [--recv-only]` | `listen [-recv-only]` |
| `connect [--recv-only] <ticket>` | `connect [-recv-only] <ticket>` |
| `listen-tcp --host <addr>` | `listen-tcp -host <addr>` |
| `connect-tcp --addr <addr> <ticket>` | `connect-tcp -addr <addr> <ticket>` |
| `listen-unix --socket-path <path>` | `listen-unix -socket-path <path>` |
| `connect-unix --socket-path <path> <ticket>` | `connect-unix -socket-path <path> <ticket>` |
| `--ipv4-addr`, `--ipv6-addr` | `-ipv4-addr`, `-ipv6-addr` |
| `--custom-alpn utf8:<text>` or hex | `-custom-alpn utf8:<text>` or hex |
| `-v` (short ticket) | `-v` |
| `IROH_SECRET` | `IROH_SECRET` |
| (none) | `-no-relay`, to stay off the public relays |

| `sendme` | `go-iroh-sendme` |
|---|---|
| `send <path>` | `send <path>` |
| `receive <ticket>`, `recv` | `receive <ticket>`, `recv` |
| `--relay default\|disabled\|<url>` | `-relay default\|disabled\|<url>` |
| `--ticket-type id\|relay-and-addresses\|relay\|addresses` | `-ticket-type`, same values |
| `--magic-ipv4-addr`, `--magic-ipv6-addr` | `-magic-ipv4-addr` or `-magic-ipv6-addr` |
| `--format hex\|cid` | `-format hex\|cid` |
| `-j`, `--jobs` | `-jobs` |
| `--show-secret`, `-v`, `--no-progress` | `-show-secret`, `-v`, `-no-progress` |
| `IROH_SECRET` | `IROH_SECRET` |

The differences are small. go-iroh binds one UDP socket, so the Go tools take
an IPv4 or an IPv6 bind address but not both. There are no progress bars;
`-no-progress` is accepted and does nothing. Rust sendme 0.36 prints hashes in
hex for both `--format` values, and so does the Go version. Go's `listen-tcp`
and `listen-unix` accept every stream on a connection where dumbpipe takes the
first, so that they also serve dumbpipe's `connect-unix`, which opens a stream
per local client on one connection.

The exception: with go-iroh v0.2.1, `go-iroh-dumbpipe connect` cannot read a
ticket from Rust `dumbpipe listen` that lists an IPv6 address, which on a
machine with IPv6 is every one. go-iroh's `endpointticket` writes and expects
an IPv6 flow label and scope ID after the port; iroh's tickets carry neither.
Rust reading Go tickets is unaffected while the Go listener binds IPv4.
`go-iroh-sendme` is unaffected: blob tickets are decoded correctly.

`go-iroh-sendme` carries its own provider rather than using `blobs.ServeBlob`.
The Rust receiver opens by asking for the root and the last chunk of every
file, a proof of each file's size. `ServeBlob` answers a request spanning
several blobs with the root alone, and `blobs.ExtractBlobRange` cannot prove a
range that starts inside a 16 KiB block, which the last chunk of most files
does; `bao.go` in the example encodes ranges the way iroh-blobs does.

`-custom-alpn` pipes bytes under any other protocol name and skips the
dumbpipe handshake, which is netcat over iroh:

```sh
go run ./cmd/go-iroh-dumbpipe listen -custom-alpn utf8:MYAPPV0
go run ./cmd/go-iroh-dumbpipe connect -custom-alpn utf8:MYAPPV0 <ticket>
```

Ported from n0's corpus: `go-iroh-blobs-gateway` (iroh-gateway),
`go-iroh-gossip-kv` (iroh-smol-kv), `go-iroh-ping` (iroh-ping),
`go-iroh-automerge` (iroh-automerge), `go-iroh-doctor` (iroh-doctor), and
`go-iroh-framed-messages`. `go-iroh-docs-sync` speaks `/iroh-sync/1`, and
`go-iroh-sendme` and the blobs examples speak `/iroh-bytes/4`.

`go-iroh-framed-messages`, `go-iroh-dumbpipe`, and `go-iroh-sendme` are checked
against the Rust implementations rather than described as compatible with them:
`interop/` builds n0's own `framed-messages` crate, a peer from the published
`dumbpipe` crate, and the released `sendme` and `dumbpipe` binaries, and both
directions of each are tested. See
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
