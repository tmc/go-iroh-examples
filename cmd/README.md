# Where the examples went

The examples were renamed once, and the numbers they carried went away with
the rename. A number in a directory name encoded a position, so inserting an
example moved names that had nothing to do with the change, and the tens digit
could only describe ten groups of ten. The `go-iroh-` prefix does the one job
the directory name has to do, which is to keep `go install ./cmd/...` from
putting a program called `doctor` or `dumbpipe` on somebody's PATH. The reading
order lives in the README instead, where it can be edited without renaming
anything.

Names are stable now. Nothing in a name encodes a position, so no future
example can displace one.

Nothing imports these paths — every one is `package main` — so the only thing a
rename breaks is a bookmark or a copied command line.

| Was | Is | Note |
|---|---|---|
| `01-keys` | `go-iroh-keys` |  |
| `02-addresses` | `go-iroh-addresses` |  |
| `03-direct-echo` | `go-iroh-direct-echo` |  |
| `04-router-echo` | `go-iroh-router-echo` |  |
| `05-memory-discovery` | `go-iroh-memory-discovery` |  |
| `06-manual-incoming` | `go-iroh-manual-incoming` |  |
| `07-source-validation` | `go-iroh-source-validation` |  |
| `08-hooks` | `go-iroh-hooks` |  |
| `09-metrics` | `go-iroh-metrics` |  |
| `10-multi-alpn` | `go-iroh-multi-alpn` |  |
| `11-public-server` | `go-iroh-public-endpoint` | merged, below |
| `12-connect-public` | `go-iroh-public-endpoint` | merged, below |
| `13-relay-online` | `go-iroh-relay-online` |  |
| `14-dns-resolve` | `go-iroh-dns-resolve` |  |
| `15-pkarr-publish-resolve` | `go-iroh-pkarr-publish-resolve` |  |
| `16-sendme-file` | `go-iroh-blobs-transfer` |  |
| `17-dumbpipe` | `go-iroh-dumbpipe` | absorbed 24-irohcat behind -alpn |
| `18-callme-frames` | `go-iroh-datagram-frames` | renamed: it never spoke callme |
| `19-rpc-workqueue` | `go-iroh-rpc-workqueue` |  |
| `20-resumable-chunks` | `go-iroh-blobs-ranges` |  |
| `22-watch-observer` | `go-iroh-watch-observer` | absorbed 23-watch-value |
| `26-stream-netconn-deadline` | `go-iroh-stream-netconn` |  |
| `27-local-infra` | `go-iroh-local-infra` | rewritten around a local relay and pkarr relay |
| `28-net-report` | `go-iroh-net-report` |  |
| `29-address-filtering` | `go-iroh-address-filtering` |  |
| `30-transport-tuning` | `go-iroh-transport-tuning` |  |
| `31-stream-listener` | `go-iroh-stream-listener` | absorbed 25-http-over-iroh |
| `32-graceful-shutdown` | `go-iroh-graceful-shutdown` |  |
| `33-path-upgrade` | `go-iroh-path-upgrade` |  |
| `34-uni-streams` | `go-iroh-uni-streams` |  |
| `35-close-codes` | `go-iroh-close-codes` |  |
| `36-incoming-filter` | `go-iroh-incoming-filter` |  |
| `37-doctor` | `go-iroh-doctor` |  |
| `38-app-envelope-ticket` | `go-iroh-tickets` | now dials with the ticket it builds |
| `39-datagram-vs-stream` | `go-iroh-datagram-vs-stream` |  |
| `40-iroh-ping` | `go-iroh-ping` |  |
| `41-framed-messages` | `go-iroh-framed-messages` |  |
| `42-iroh-automerge` | `go-iroh-automerge` |  |
| `43-iroh-smol-kv` | `go-iroh-gossip-kv` |  |
| `44-iroh-gateway` | `go-iroh-blobs-gateway` |  |

Merged:

| Was | Into | Why |
|---|---|---|
| `11-public-server`, `12-connect-public` | `go-iroh-public-endpoint` | two halves of one exercise: neither ran alone, both needed a real network, and the server printed an address in pieces only because the dialer took those pieces as flags |

Removed:

| Was | Why |
|---|---|
| `21-memory-mesh` | not a mesh and not a broadcast: one unicast stream per peer. `go-iroh-gossip-topic` is the honest version |
| `23-watch-value` | folded into `go-iroh-watch-observer`, which now shows the primitive before the endpoint that uses it |
| `24-irohcat` | the same program as `go-iroh-dumbpipe`, which absorbed it behind `-alpn` |
| `25-http-over-iroh` | a hand-rolled net.Listener that `go-iroh-stream-listener` makes unnecessary |

Added:

| Example | Fills |
|---|---|
| `go-iroh-tickets` | tickets as the normal way to hand an address to a peer (grew out of `38-app-envelope-ticket`) |
| `go-iroh-mdns-discovery` | `iroh/mdns`, the discovery path that needs no infrastructure |
| `go-iroh-gossip-topic` | `gossip` directly, rather than under a key-value protocol |
| `go-iroh-docs-sync` | `docs`, a whole Rust-compatible protocol with no example |
| `go-iroh-key-exchange` | `WithKeyExchangePolicy`, and `postcard` |
| `go-iroh-custom-transport` | `WithCustomTransport`, which the README said was deferred |
| `go-iroh-qlog-tracing` | `WithQLOG` and `QLOGDir`, added in go-iroh v0.1.1 |
| `go-iroh-alpn-negotiation` | how a protocol version is agreed, and how a mismatch surfaces |
