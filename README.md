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
handful that need a public relay, DNS, or pkarr relay skip themselves unless the
matching switch in [Examples that need the network](#examples-that-need-the-network)
is set.

## Progression

The numbers are a reading order, not a version. Start at 01 and stop wherever
the answer you came for is. Gaps between groups are room for later insertions;
`cmd/README.md` maps the numbers examples used to have.

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
| `go-iroh-manual-incoming` | owning the accept loop: `AcceptIncoming`, `Accepting.ALPN` |
| `go-iroh-incoming-filter` | admission control with `RouterConfig.IncomingFilter` and `AcceptingHandler.OnAccepting` |
| `go-iroh-source-validation` | QUIC Retry source-address validation |
| `go-iroh-hooks` | observing dials and handshakes with `EndpointHooks` |
| `go-iroh-metrics` | endpoint counters after a connection |
| `go-iroh-close-codes` | reading a peer's application close code with `AsApplicationError` |
| `go-iroh-watch-observer` | `watch.Value` and `Endpoint.WatchAddr`: Current, Updated, Stream |

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
| `go-iroh-relay-online` | the default relay map and `Endpoint.Online` |
| `go-iroh-path-upgrade` | watching a relayed connection move to a direct path |
| `go-iroh-net-report` | what `Endpoint.NetReport` says about the local network |
| `go-iroh-local-infra` | running your own relay and pkarr relay on loopback |

### Protocols

| Example | Shows |
|---|---|
| `go-iroh-blobs-transfer` | BAO-verified blob transfer, the `sendme` shape |
| `go-iroh-blobs-ranges` | resumable byte-range fetches from a blob |
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
instead of its embedded payload, and `go-iroh-dumbpipe` takes `-alpn`, `-bind`,
`-advertise`, `-no-relay`, `-key`, and `-ticket`.

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

Both directions have been verified against Rust `dumbpipe` on loopback. The
listener advertises a public relay by default, so the printed ticket is usable
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
`go-iroh-framed-messages`. `go-iroh-docs-sync` speaks `/iroh-sync/1` and `40`/`41`/`42`
speak `/iroh-bytes/4`.

## What is not here

Some exported APIs are configuration knobs rather than workflows, and are left
to package documentation: `WithKeyLogWriter`, `WithBindAddrOpts`,
`WithoutIPTransports`, `WithoutRelayTransports`, and `NewSessionCache`.

Cross-host examples that need two machines live in `go-iroh-experiments`;
live Rust interop gates live in the go-iroh repository.
