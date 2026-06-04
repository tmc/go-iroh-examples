# go-iroh examples

This repository contains small runnable examples for
[`github.com/tmc/go-iroh`](https://github.com/tmc/go-iroh). The examples build
against the sibling checkout through this module's `replace` directive:

```sh
replace github.com/tmc/go-iroh => ../go-iroh
```

Run them from this directory:

```sh
go run ./cmd/01-keys
go run ./cmd/02-addresses
go run ./cmd/03-direct-echo
go run ./cmd/04-router-echo
go run ./cmd/05-memory-discovery
go run ./cmd/06-manual-incoming
go run ./cmd/07-source-validation
go run ./cmd/08-hooks
go run ./cmd/09-metrics
go run ./cmd/10-multi-alpn
go run ./cmd/11-public-server
go run ./cmd/12-connect-public
go run ./cmd/13-relay-online
go run ./cmd/14-dns-resolve
go run ./cmd/15-pkarr-publish-resolve
go run ./cmd/16-sendme-file
go run ./cmd/17-dumbpipe
go run ./cmd/18-callme-frames
go run ./cmd/19-rpc-workqueue
go run ./cmd/20-resumable-chunks
go run ./cmd/21-memory-mesh
go run ./cmd/24-irohcat
```

## Progression

| Example | Shows |
|---|---|
| `01-keys` | endpoint identity: `key.SecretKey`, `key.EndpointId`, signatures |
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
| `16-sendme-file` | `sendme`-style content transfer over an iroh stream |
| `17-dumbpipe` | `dumbpipe`-style byte piping over an iroh stream |
| `18-callme-frames` | `callme`-style realtime media frame transport with datagrams |
| `19-rpc-workqueue` | concurrent JSON RPC-style work over multiple streams on one connection |
| `20-resumable-chunks` | out-of-order chunk transfer with per-chunk hash validation |
| `21-memory-mesh` | multi-node loopback mesh broadcast using memory endpoint discovery |
| `24-irohcat` | `nc`-style stdin/stdout piping over an iroh stream |

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

## Rust Docs Equivalents

The examples at <https://docs.iroh.computer/examples> currently highlight
`sendme`, `callme`, and `dumbpipe`.

`16-sendme-file` is the go-iroh equivalent of the `sendme` shape: one endpoint
serves file bytes over an iroh stream and the receiver verifies the byte count
and digest. Set `IROH_EXAMPLE_FILE` to serve a real file instead of the embedded
sample payload.

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

Set `GO_IROH_LIVE_RELAY=1` for Go listener tickets that include a public relay.

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

For multi-machine use, opt the listener into the public relay map:

```sh
go run ./cmd/24-irohcat listen -relay
```

Use `-key` to keep the same endpoint identity across listener restarts, and
`-ticket` to update a stable file with the listener's current ticket:

```sh
go run ./cmd/24-irohcat listen -relay -key ./irohcat.key -ticket ./irohcat.ticket
go run ./cmd/24-irohcat connect "$(cat ./irohcat.ticket)"
```
