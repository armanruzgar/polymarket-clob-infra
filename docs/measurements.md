# Measurements

The numbers the README and the module docs quote, with the data behind them.
They were taken while building a market maker on this venue in October 2026,
from hosts in Europe. They describe the venue as it was then; measure again
before relying on them.

## 1. Feed delivery lag

**What was measured.** A probe opened one websocket connection to the market
channel (`wss://ws-subscriptions-clob.polymarket.com/ws/market`) for two
minutes, subscribed to the active 5-minute crypto markets, and recorded for
every `price_change`, `book` and `last_trade_price` message its local receive
time minus the venue timestamp the message carries. The local clock was NTP
disciplined (offset and delay in the data). Runs were repeated back to back,
from several hosts. The probe was written in Python, so its own parsing is in
the tail; the Rust feed measures lower tails on the same host.

**Data.** [`data/feed_lag_runs.csv`](data/feed_lag_runs.csv): one row per
run (8,623 runs), with the host, the venue edge that answered (`edge`: the
CDN's point of presence), the clock offset, the message counts and the lag
quantiles per message type, and the round trips of the REST calls the probe
made. [`data/feed_lag_pooled.json`](data/feed_lag_pooled.json): the lag
quantiles pooled over every event of selected run sets.

**Per run, median over runs** (`price_change` lag, ms; the REST round trips
are the probe's own `GET /book`, `POST /order` and `DELETE /order`):

| source | host | edge | runs | events | p50 | p90 | p99 | GET | POST | DELETE |
|---|---|---|---|---|---|---|---|---|---|---|
| 24 h, 10-09/10 | AWS Dublin c7gn | DUB | 140 | 6,505,791 | 10.0 | 36.1 | 190.6 | 24.7 | | |
| 24 h, 10-09/10 | Railway Amsterdam | AMS | 6,512 | 324,284,182 | 10.6 | 32.6 | 156.5 | 26.3 | | |
| 24 h, 10-09/10 | Railway Amsterdam | FRA | 1,700 | 78,170,567 | 11.4 | 36.1 | 217.0 | 39.3 | | |
| Sat night, 10-10/11 | AWS Frankfurt c7g | FRA | 37 | 2,020,147 | 11.5 | 19.0 | 69.6 | 29.6 | 10.5 | 34.8 |
| Sat night | AWS Stockholm c7g | ARN | 37 | 1,629,891 | 20.4 | 29.2 | 88.3 | 43.7 | 46.8 | 46.8 |
| Sat night | AWS Dublin c7g | DUB | 37 | 2,253,683 | 8.8 | 19.7 | 80.9 | 22.7 | 25.8 | 25.6 |
| Sat night | AWS London c7g | LHR | 37 | 2,139,743 | 4.8 | 12.6 | 66.1 | 16.1 | 9.3 | 19.2 |
| Sat night | AWS Paris c7g | CDG | 37 | 1,792,031 | 11.5 | 19.7 | 57.1 | 23.9 | 10.2 | 30.3 |
| Sat night | DigitalOcean AMS3 | AMS | 34 | 2,471,139 | 9.1 | 18.3 | 69.4 | 22.6 | 26.4 | 26.4 |
| Sat night | Railway Amsterdam | AMS | 29 | 314,683 | 9.1 | 16.7 | 39.0 | 21.2 | 25.0 | 24.7 |

The Railway host was answered by two edges (AMS, FRA) over the 24 hours; the
rows are split by edge. London's low numbers are a reference only: the venue
refuses orders from the UK (HTTP 403 on `POST /order`), as it does from
France.

**Pooled over every event, Dublin** (`price_change`, ms):

| set | runs | events | p50 | p75 | p90 | p95 | p99 | p99.9 |
|---|---|---|---|---|---|---|---|---|
| weekday, 10-09 11:00 to 10-10 10:50 ET | 140 | 6,505,791 | 10.8 | 24.4 | 82.6 | 129.5 | 370.5 | 1,010 |
| weekday, 12:00-15:00 ET only | 17 | 1.2 M | 14.4 | | 117 | | 708 | |
| Saturday night, 10-10 19:06 to 10-11 01:06 ET | 37 | 2,253,683 | 10.0 | 14.9 | 28.6 | 51.8 | 168.0 | 462 |

The tail grows with activity: over the 140 weekday runs, Spearman(run
activity, run p90) is +0.42 to +0.49, and 15 of the 140 runs have a p90 above
100 ms.

**Connections late for their whole life.** Of the 140 Dublin runs, 5 (3.6%)
had a median lag of 79-88 ms, against 10 ms for the rest, whatever the
activity in that run: the connection itself was late, not the market. A
long-lived socket can sit 80 ms behind without any sign on that socket. This
is why `ws::feed_lag` compares connections against each other.

## 2. Event order across connections

**What was measured.** Two recorders subscribed to the same 26 markets for 22
minutes, one on a shared socket for all markets and one with a socket per
market, and their streams were compared event by event by content.

**Found.**
- Book events (`price_change`, the `book` sent after a trade,
  `tick_size_change`) came in the same order on both: 1,393,161 common
  events, no swap.
- `last_trade_price` floats against the book events: 2,757 inversions, each
  of them a print moved across book events.
- The venue repeats single messages verbatim, within ten messages, on every
  connection: in one recorded hour (10-07 14:00 UTC), 1,105 repeats in 6.5
  million book events, never three copies in a row. A repeat is part of the
  stream, applied twice.
- The venue sent no crossed book in 6.8 million price changes and 59,000
  books.

These are the facts `ws::feed_merge` is built on: book events merge by
position in a shared log, prints are counted by copy, and a connection lines
up only after `ALIGN_RUN` (4) consecutive matches, since a shorter match can
be an earlier copy of a repeated message.

## 3. What a third connection adds

**What was measured.** On the live market maker's host (DigitalOcean
Amsterdam, 2 vCPU), a probe ran three market-channel connections with the
bot's subscription for 12.5 minutes (10-14, 7:31-7:43 PM ET), stamping every
message with the kernel receive time; 1.25 M messages arrived on all three.
The gain of a third connection is the first-copy delay over two connections
(mean of the three pairs) minus over three.

| gain of 2 -> 3 connections (ms) | p50 | p90 | p99 |
|---|---|---|---|
| all messages | 0.3 | 1.2 | 6.0 |
| own market (BTC 5m) | 0.2 | 1.1 | 5.6 |
| own market, busiest 5% of seconds | 0.5 | 3.1 | 26.6 |
| own market, fastest 1% of seconds (mid moved >= 9c in 1 s) | 0.4 | 6.0 | 30.6 |

The third connection was first by more than 1 ms for 13.5% of messages, by
more than 5 ms for 2.0%, by more than 20 ms for 0.37%. It cost about 5% of a
vCPU and raised connection drops from 18 to 27 per hour. A stall that hits the
host itself (CPU steal after each 5-minute mark on that shared box) hits every
connection at once, and no number of connections helps there.

## 4. The order path

**What was measured.** The order latency probe (`examples/order_latency_probe.rs`)
sends post-only 1c bids of the minimum size and times, on the public feed,
when the order appears and when its cancel removes it. From AWS Dublin, 63
orders in one quiet hour (10-10):

| Dublin | p10 | p50 | p90 | max |
|---|---|---|---|---|
| POST send to on the book (feed) | 21 | 33 | 53 | 134 ms |
| DELETE send to off the book (feed) | 7 | 17 | 68 | 191 ms |
| POST round trip | | 38 | 57 | |

**A DELETE can overtake its own POST.** The venue keeps a DELETE that arrives
before its order; the order is then refused with HTTP 400 "Already canceled"
and never rests. For a DELETE sent `g` ms after its POST, the share overtaken
fits `1 / (1 + exp((g - 5.75) / 3.25))`: 85% at 0 ms, 56% at 5 ms, 21% at 10
ms, about 0 from 20 ms (48 orders from Railway Amsterdam;
[`data/delete_overtakes_post.json`](data/delete_overtakes_post.json)). A
market maker that cancels at once after posting must hold the cancel for
longer than this, or sign the order so the cancel can be matched to it.

**Taker delay.** Since 2026-09-04 the venue delays taking orders in the
crypto Up/Down markets by 150 ms; measured from send, a taking order reaches
the book about 190-200 ms later. Post-only orders are not delayed.

## 5. Fees on chain

From 12,038 `matchOrders` transactions over 8 BTC 5-minute markets: the taker
fee is `0.07 * C * p * (1 - p)` (median of paid over formula 1.0000, p10
0.9994, p90 1.0001); makers paid no fee on any fill; the maker rebate was
19.9% of the maker's own fee-equivalent on two days, paid the next day in
cash. The `fees` module reserves collateral for a BUY with that curve, and
the `accounting` ledger takes the fee a fill paid only from its receipt.

## Not measured, or measured poorly

- The feed probe was Python: its parsing is in the tails above. The Rust feed
  on the same host reports per-connection p90 of 14 ms (median over 30-second
  windows) against the probe's 36 ms.
- The order path has 63 orders from one quiet hour and 48 pipelined ones from
  another host. Nothing here says how it behaves under load.
- No measurement from outside Europe.
