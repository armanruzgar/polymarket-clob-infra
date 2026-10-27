# Changelog

## 0.1.0 (2026-10-27)

First release. Extracted from a Rust market maker on Polymarket's CLOB
(`polybot-rs` at `f11511f`, 2026-10-26); `tools/drift.sh` reports how far the
shared modules have drifted since.

### Offline (no feature)

- `market_data`: one atomic snapshot of every subscribed book; integer
  millionths; out-of-order and future-dated messages refused.
- `ws::feed_merge`: N connections merged into one book per market, each venue
  event applied once in the venue's order; a disagreement rebuilds the market.
- `ws::feed_lag`: lag per connection over a rolling window, the clock-free
  comparison between connections, the venue clock check.
- `polymarket::signing`: EIP-712 order signing checked against the official
  SDK's vectors; L2 HMAC.
- `fees`: the V2 taker fee curve and a conservative BUY reservation.
- `accounting`, `receipts`, `execution`: the execution ledger, checked against
  on-chain receipts in integer units; a missing fee stays unknown.

### With the `net` feature (default)

- `ws::connect`: one websocket connection (subscribe, pings, frames with
  their receive instant).
- `ws::feed`: the running redundant feed: connections opened and replaced
  (diverged, failed, late by the lag rules, closed), merged into one
  `MarketData`, with per-connection statistics.
- `ws::fill_stream`: the private fill stream, with refused logins told apart
  from outages.
- `polymarket::rest`: the REST client (place, cancel, cancel-all, cancel by
  market, order reads, the public book and clock) with L2 headers.
- `polymarket::auth`: API key derivation.

### Examples, tests, benchmarks

- `examples/replay.rs`: the recorded market over three simulated connections.
  Fixed: its book comparison was vacuous (the books were never marked
  connected, so nothing was applied on either side); it now checks that the
  reference took the stream.
- `examples/live.rs`: the feed on the venue, with each connection's numbers.
- `examples/order_latency_probe.rs`: the order path timed on the public feed.
- `tests/merge_prop.rs`: a property test of the merge over the recorded
  market with random delays, slow spells, lost prints and venue repeats.
- `benches/core.rs`: the book apply benchmark now applies (same fix as the
  replay); its number in the README changed accordingly.
- `docs/measurements.md` with the data behind the README's numbers.
