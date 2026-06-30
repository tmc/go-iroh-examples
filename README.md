# go-iroh examples

This repository contains small runnable examples for
[`github.com/tmc/go-iroh`](https://github.com/tmc/go-iroh). The examples build
against the `github.com/tmc/go-iroh` version pinned in `go.mod`.

Run an example from this directory:

```sh
go run ./cmd/03-direct-echo
```

Run all default examples and package tests:

```sh
go test ./... -count=1
```

## Progression

| Example | Shows |
|---|---|
| `01-keys` | endpoint identity: `key.SecretKey`, `key.EndpointID`, signatures |
| `02-addresses` | address construction with `netaddr.EndpointAddr` |
| `03-direct-echo` | two localhost endpoints exchanging a QUIC stream |
| `04-router-echo` | ALPN dispatch through `iroh.Router` |
| `05-memory-discovery` | connecting by endpoint id through `iroh.MemoryLookup` |
| `06-manual-incoming` | manual `AcceptIncoming`, `Accepting.ALPN`, and connection verification |
| `07-source-validation` | local QUIC Retry source-address validation |
| `08-hooks` | observing outbound dials and handshakes with `EndpointHooks` |
| `09-metrics` | endpoint counter snapshots after a connection |
| `10-multi-alpn` | one router dispatching multiple application protocols |
| `11-public-server` | binding a server on a public UDP address and advertising its endpoint address |
| `12-connect-public` | connecting to a peer described by endpoint id plus public IP or relay URL |
| `13-relay-online` | opting into the default public relay map and waiting for relay connectivity |
| `14-dns-resolve` | resolving a published endpoint id through DNS endpoint discovery |
| `15-pkarr-publish-resolve` | publishing endpoint data to pkarr and resolving it back |
| `16-sendme-file` | `sendme`-style BAO-verified blob transfer |
| `17-dumbpipe` | `dumbpipe`-style byte piping over an iroh stream |
| `18-callme-frames` | `callme`-style realtime media frame transport with datagrams |
| `19-rpc-workqueue` | concurrent postcard RPC work with `irpc.Call` and `irpc.Handler` |
| `20-resumable-chunks` | resumable BAO-verified blob range transfer |
| `21-memory-mesh` | multi-node loopback mesh broadcast using memory endpoint discovery |
| `22-watch-observer` | observing endpoint address changes with `watch.Observer` |
| `23-watch-value` | using `watch.Value` and observer streams directly |
| `24-irohcat` | `nc`-style stdin/stdout piping over an iroh stream |
| `25-http-over-iroh` | serving `net/http` over stream-backed iroh `net.Conn` values |
| `26-stream-netconn-deadline` | using `Conn.OpenStreamConn`, `Conn.AcceptStreamConn`, and deadlines |
| `27-local-infra` | embedding local DNS, relay, and metrics infrastructure packages |
| `28-net-report` | reading endpoint network reports, with live relay probing opt-in |
| `29-address-filtering` | publishing filtered DNS/pkarr address sets locally |
| `30-transport-tuning` | tuning stable QUIC keepalive and idle timeout settings |
| `31-stream-listener` | serving stream-backed `net.Listener` values directly and through a router |
| `32-graceful-shutdown` | draining router handlers and closing endpoints after SIGINT/SIGTERM |
| `33-path-upgrade` | watching selected paths as a relay connection advertises direct candidates |
| `34-uni-streams` | publishing independent telemetry events over unidirectional streams |
| `35-close-codes` | decoding application close codes and reasons from peer shutdown |
| `36-incoming-filter` | router admission control with `RouterConfig.IncomingFilter` and `AcceptingHandler` |
| `37-doctor` | printing local relay, net-report, latency, and path diagnostics |
| `38-app-envelope-ticket` | wrapping endpoint tickets with app metadata in a base32 envelope |
| `39-datagram-vs-stream` | sending small payloads as datagrams and falling back to streams |
| `40-iroh-ping` | minimal custom protocol over ALPN `iroh/ping/0`: `PING` to `PONG` |
| `41-framed-messages` | length-delimited messages over one bidirectional stream |
| `42-iroh-automerge` | Automerge CRDT sync messages over an iroh protocol handler |
| `43-iroh-smol-kv` | signed key-value updates over a joined gossip topic |
| `44-iroh-gateway` | HTTP Range gateway for verified blobs fetched over iroh |

Examples `01` through `10` use loopback direct paths and avoid live relay/DNS
dependencies. Examples `11` through `15` demonstrate non-local workflows and
either print their required environment variables or require an explicit live
network opt-in.

## Live Examples

`11-public-server` binds UDP on all IPv4 interfaces. Set `IROH_EXAMPLE_PORT` to
choose the port, `GO_IROH_LIVE_RELAY=1` to also advertise a public relay, and
`IROH_EXAMPLE_SERVE=1` to keep accepting echo connections.

`12-connect-public` connects to a peer from `11-public-server` or another iroh
endpoint:

```sh
IROH_EXAMPLE_PEER_ID=<z32-or-hex-id> \
IROH_EXAMPLE_PEER_IP=<host:port> \
go run ./cmd/12-connect-public
```

Use `IROH_EXAMPLE_PEER_RELAY=<relay-url>` instead of, or in addition to,
`IROH_EXAMPLE_PEER_IP` for relay-addressed peers.

`13-relay-online` connects to the default public relay map only when
`GO_IROH_LIVE_RELAY=1` is set.

`14-dns-resolve` resolves a published endpoint id:

```sh
IROH_EXAMPLE_ENDPOINT_ID=<z32-or-hex-id> go run ./cmd/14-dns-resolve
```

Set `IROH_EXAMPLE_DNS_ORIGIN` to query a non-default discovery origin.

`15-pkarr-publish-resolve` publishes temporary endpoint data to the number0
pkarr relay and resolves it back only when `GO_IROH_LIVE_PKARR=1` is set.

`28-net-report` reads the endpoint's most recent net report. The default run
does not contact live relays and usually reports that no net report is available.
Set `GO_IROH_LIVE_RELAY=1` to opt into public relay probing:

```sh
GO_IROH_LIVE_RELAY=1 go run ./cmd/28-net-report
```

## Coverage Notes

The examples cover the main public feature groups: endpoint identity and
addresses, direct connections, routers and ALPN dispatch, manual incoming
admission, source-address validation, hooks, metrics, memory/DNS/pkarr address
lookup, address filtering, relay opt-in, network reports, streams, datagrams,
multi-stream transfers, `watch` observers, stream-backed `net.Conn` values,
stream-backed `net.Listener` values, `net/http` over iroh, stable transport
tuning, graceful shutdown, unidirectional streams, application close codes, and
path observation, local diagnostics, and app-level ticket envelopes. `17-dumbpipe` and
`24-irohcat` use the public `endpointticket` package for Rust-compatible
endpoint tickets;
`38-app-envelope-ticket` wraps those tickets with application metadata.

Some exported APIs are low-level configuration hooks rather than separate
workflows. `WithKeyLogWriter`, `WithBindAddrOpts`, `WithoutIPTransports`,
`WithoutRelayTransports`, and `NewSessionCache` are intentionally left to
package documentation and tests unless an example needs that specific tuning.

Custom transport examples are deferred until the `go-iroh-alt-transports` API
lands in main. The current main API exposes the low-level datagram hook; the
branch is still settling the practical address-publication, capability, policy,
and stream/memory transport shape that a copyable example should teach.

`25-http-over-iroh` shows how to adapt individual accepted stream `net.Conn`
values into a small local listener. `31-stream-listener` uses go-iroh's public
`Endpoint.ListenStreams` and router-native `StreamListener` APIs.
`36-incoming-filter` shows the router's admission-control surface:
`RouterConfig.IncomingFilter` to accept or reject connections before ALPN
negotiation, and `AcceptingHandler.OnAccepting` to inspect a connection before
it is handled.

`39-datagram-vs-stream` shows a transport fallback pattern: try a datagram for a
small message, and use a reliable stream when the datagram send cannot carry the
payload. The server reads datagrams and accepts streams concurrently on the same
connection.

## Rust Docs Equivalents

The examples at <https://docs.iroh.computer/examples> currently highlight
`sendme`, `callme`, and `dumbpipe`.

`16-sendme-file` is the go-iroh equivalent of the `sendme` shape: one endpoint
serves a content-addressed blob over iroh and the receiver verifies it with the
blob's BLAKE3/BAO hash. Set `IROH_EXAMPLE_FILE` to serve a real file instead of
the embedded sample payload.

`17-dumbpipe` is the go-iroh equivalent of `dumbpipe`: a QUIC stream carries raw
bytes from one endpoint to another. It speaks Rust dumbpipe's default transport
protocol: ALPN `DUMBPIPEV0`, the `hello` stream handshake, and
`iroh-tickets` endpoint tickets.

Run Go as the listener and Rust as the connector:

```sh
go run ./cmd/17-dumbpipe listen
# copy the printed ticket, then in another shell:
printf 'hello from rust\n' | dumbpipe connect <ticket>
```

Run Rust as the listener and Go as the connector:

```sh
dumbpipe listen
# copy the printed ticket, then in another shell:
printf 'hello from go\n' | go run ./cmd/17-dumbpipe connect <ticket>
```

Both directions use the Rust endpoint ticket format and have been verified
against Rust `dumbpipe` on loopback.

The listener advertises a public relay by default, so the printed ticket is
usable from another machine when relay connectivity is available:

```sh
go run ./cmd/17-dumbpipe listen
```

Use `-no-relay` for direct-only local demos. Use `-bind` and `-advertise` when
you want to publish a directly reachable UDP address. `GO_IROH_LIVE_RELAY=1`,
`GO_IROH_DUMBPIPE_BIND_ADDR`, and `GO_IROH_DUMBPIPE_ADVERTISE_ADDR` remain as
environment-variable aliases for scripted runs.

`18-callme-frames` is the transport-side go-iroh equivalent of `callme`: it
sends small audio/video-labeled frames over QUIC datagrams. go-iroh does not
provide media capture, encoding, or playback APIs; those belong above the iroh
transport layer.

## Netcat-Style Pipe

`24-irohcat` is a small `nc`-style tool over iroh. The listener prints an
endpoint ticket on stderr; the connector takes the ticket and pipes stdin/stdout
over one bidirectional stream.

```sh
go run ./cmd/24-irohcat listen
# copy the printed ticket, then in another shell:
go run ./cmd/24-irohcat connect <ticket>
```

For multi-machine use, run the listener normally; it advertises a public relay
by default:

```sh
go run ./cmd/24-irohcat listen
```

Use `-key` to keep the same endpoint identity across listener restarts, and
`-ticket` to update a stable file with the listener's current ticket:

```sh
go run ./cmd/24-irohcat listen -key ./irohcat.key -ticket ./irohcat.ticket
go run ./cmd/24-irohcat connect "$(cat ./irohcat.ticket)"
```

Pass `-no-relay` to either listener when you want a same-machine or directly
routed local-only ticket.
