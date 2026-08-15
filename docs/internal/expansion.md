# go-iroh-examples expansion

Internal planning note: where the example progression should grow next.

## Design invariants

- Numbered progression: each example teaches one concept, builds on earlier
  numbers, and stays runnable with `go run ./cmd/NN-name`.
- Examples are self-contained localhost demos by default; anything needing
  external infrastructure states so up front.
- Shared plumbing lives in `internal/exampleutil`, never duplicated per
  example.

## Coverage audit (against go-iroh main, 2026-08)

The 44 examples cover the connectivity layer thoroughly: keys, addresses,
streams, datagrams, discovery (memory/DNS/pkarr), relays, path upgrade,
metrics, hooks, shutdown, and embedded local infrastructure
(`27-local-infra` runs `relayserver`/`dnsserver` in-process).

Gaps, by go-iroh package:

| Package | Coverage |
|---|---|
| `gossip` | only indirectly, inside `43-iroh-smol-kv` |
| `docs` | none |
| `http3` | none (`25-http-over-iroh` uses `net/http` over streams, not the `http3` adapter) |
| `iroh` key-exchange policy | none (landed 2026-07, commit 1389198) |

## Planned additions, in order

### 45-gossip-topic

The first direct gossip example: two (or three) in-process endpoints join a
topic, exchange broadcasts, and print membership changes. Gossip is a
headline protocol and currently has no dedicated example; `43-iroh-smol-kv`
uses it but teaches KV replication, not gossip itself.

### 46-docs-sync

Two in-process endpoints share an iroh-docs replica: one writes keys, the
other syncs and reads them back. Demonstrates author keys, namespaces, and
range sync. This is the only ported protocol with zero example coverage.

### 47-http3-adapter

Serve HTTP/3 over an iroh connection with the `http3` package, contrasted
with `25-http-over-iroh` (HTTP/1 over a stream). One page of code showing
when to reach for which.

### 48-key-exchange

Configure the key-exchange policy on both sides, connect, and print the
negotiated group. Small, but it is the newest public API knob and the
natural place to document its interaction with Rust interop.

## Not planned

- Cross-host examples requiring two machines: `directpath` in
  go-iroh-experiments owns that.
- Anything needing cargo or a Rust checkout: live interop belongs to the
  go-iroh repo's opt-in gates.

## Infrastructure debt

- Most examples have no test files; the pattern used by `17-dumbpipe` and
  `24-irohcat` (run the example against itself in-process) should extend to
  new additions as they land, protocol examples first.
