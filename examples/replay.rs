//! Replays a recorded Polymarket market through `MarketMerge` over three
//! simulated websocket connections, each with its own latency, jitter and a
//! slow spell. One connection also skips a book event (an integrity failure).
//!
//! It checks that the merged book equals the book built from the venue's own
//! stream, and reports how often each connection delivered first, how much
//! earlier the merged feed saw events than the best single connection, and the
//! merge's cost per message.
//!
//!     cargo run --release --example replay
use polymarket_clob_infra::market_data::MarketData;
use polymarket_clob_infra::ws::feed_merge::{Applied, ConnId, EventKey, Expired, Lost, MarketMerge, Step};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader};
use std::time::{Duration, Instant};

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/recorded_market.jsonl.gz");

/// One simulated connection: a base delay, exponential jitter, and a window of
/// the recording (as fractions of it) during which it runs `slow_ms` late.
struct Conn {
    id: ConnId,
    base_ms: f64,
    jitter_ms: f64,
    slow: (f64, f64),
    slow_ms: f64,
    /// Skips the book event at this fraction of the recording.
    skip_at: Option<f64>,
}

struct Delivery {
    at_us: u64,
    conn: ConnId,
    msg: usize,
}

fn load() -> Vec<(u64, Value)> {
    let file = std::fs::File::open(FIXTURE).expect("fixture");
    let mut out = Vec::new();
    for line in BufReader::new(flate2::read::GzDecoder::new(file)).lines() {
        let frame: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let rx = frame["rx"].as_u64().unwrap();
        let value: Value = serde_json::from_str(frame["m"].as_str().unwrap()).unwrap();
        match value {
            Value::Array(list) => out.extend(list.into_iter().map(|m| (rx, m))),
            m => out.push((rx, m)),
        }
    }
    out
}

fn is_book_event(msg: &Value) -> bool {
    matches!(
        msg["event_type"].as_str(),
        Some("book" | "price_change" | "tick_size_change")
    )
}

fn quantile(sorted: &[f64], q: f64) -> f64 {
    sorted[((sorted.len() - 1) as f64 * q).round() as usize]
}

fn main() {
    let msgs = load();
    let tokens: Vec<String> = msgs
        .iter()
        .take(2)
        .map(|(_, m)| m["asset_id"].as_str().unwrap().to_string())
        .collect();
    let (first_rx, last_rx) = (msgs[0].0, msgs.last().unwrap().0);
    let span = (last_rx - first_rx).max(1) as f64;

    // The reference: the venue's stream as one connection received it.
    let reference = MarketData::default();
    reference.reset(&tokens);
    reference.connected();
    let mut changes = 0u64;
    for (_, m) in &msgs {
        changes += reference.ingest_any(m) as u64;
    }
    assert!(
        changes > 10_000,
        "the reference book took only {changes} of the stream's messages"
    );

    let conns = [
        Conn {
            id: 1,
            base_ms: 10.0,
            jitter_ms: 3.0,
            slow: (0.40, 0.55),
            slow_ms: 80.0,
            skip_at: None,
        },
        Conn {
            id: 2,
            base_ms: 12.0,
            jitter_ms: 4.0,
            slow: (0.70, 0.80),
            slow_ms: 120.0,
            skip_at: None,
        },
        Conn {
            id: 3,
            base_ms: 11.0,
            jitter_ms: 6.0,
            slow: (0.10, 0.20),
            slow_ms: 60.0,
            skip_at: Some(0.60),
        },
    ];
    let mut rng = StdRng::seed_from_u64(7);
    let mut deliveries = Vec::new();
    let mut arrival: HashMap<(ConnId, usize), u64> = HashMap::new();
    for c in &conns {
        let mut last = 0u64;
        let mut skipped = false;
        for (i, (rx, m)) in msgs.iter().enumerate() {
            let at = (rx - first_rx) as f64 / span;
            if let Some(skip) = c.skip_at {
                if !skipped && at >= skip && m["event_type"] == "price_change" {
                    skipped = true;
                    continue;
                }
            }
            let jitter = -c.jitter_ms * (1.0 - rng.random::<f64>()).ln();
            let slow = if (c.slow.0..c.slow.1).contains(&at) {
                c.slow_ms
            } else {
                0.0
            };
            // A connection delivers in order: never before its previous message.
            let at_us = (((rx - first_rx) as f64 + c.base_ms + jitter + slow) * 1000.0) as u64;
            last = last.max(at_us);
            deliveries.push(Delivery {
                at_us: last,
                conn: c.id,
                msg: i,
            });
            arrival.insert((c.id, i), last);
        }
    }
    deliveries.sort_by_key(|d| (d.at_us, d.conn));

    let start = Instant::now();
    let merged = MarketData::default();
    merged.reset(&tokens);
    merged.connected();
    let mut merge = MarketMerge::default();
    for c in &conns {
        merge.subscribe(c.id, start);
    }
    let mut snapshotted: HashSet<(ConnId, usize)> = HashSet::new();
    let mut counts: HashMap<&'static str, u64> = HashMap::new();
    let mut first_by: HashMap<ConnId, u64> = HashMap::new();
    let mut applied_at: HashMap<usize, u64> = HashMap::new();
    let mut msg_index: HashMap<EventKey, Vec<usize>> = HashMap::new();
    for (i, (_, m)) in msgs.iter().enumerate() {
        msg_index.entry(EventKey::of(m)).or_default().push(i);
    }
    let mut dropped = Vec::new();
    let mut next_tick = 0u64;
    let mut merge_time = Duration::ZERO;

    let mut apply = |list: Vec<Applied>, at_us: u64, applied_at: &mut HashMap<usize, u64>| {
        for a in list {
            merged.ingest_any(&a.msg);
            if !a.snapshot {
                *first_by.entry(a.conn).or_default() += 1;
                // The first not-yet-applied copy of this message in the recording.
                if let Some(i) = msg_index[&EventKey::of(&a.msg)]
                    .iter()
                    .find(|i| !applied_at.contains_key(i))
                {
                    applied_at.insert(*i, at_us);
                }
            }
        }
    };

    for d in &deliveries {
        if dropped.contains(&d.conn) {
            continue;
        }
        let now = start + Duration::from_micros(d.at_us);
        if d.at_us >= next_tick {
            for expired in merge.tick(now, &|_| false) {
                *counts.entry("tick events").or_default() += 1;
                if let Expired::Aligned(_, list) | Expired::Rebuild(_, list) = expired {
                    apply(list, d.at_us, &mut applied_at);
                }
            }
            next_tick = d.at_us + 250_000;
        }
        let msg = &msgs[d.msg].1;
        let timer = Instant::now();
        let step = match msg["event_type"].as_str() {
            Some("book") => {
                let index = tokens.iter().position(|t| msg["asset_id"] == **t).unwrap();
                if snapshotted.insert((d.conn, index)) {
                    merge.snapshot(d.conn, index, msg, now)
                } else {
                    merge.book_event(d.conn, EventKey::of(msg), msg, now)
                }
            }
            _ if is_book_event(msg) => merge.book_event(d.conn, EventKey::of(msg), msg, now),
            _ => merge.other(d.conn, EventKey::of(msg), msg, now),
        };
        merge_time += timer.elapsed();
        match step {
            Step::Apply(list) => {
                *counts.entry("applied").or_default() += list.len() as u64;
                apply(list, d.at_us, &mut applied_at);
            }
            Step::Duplicate => *counts.entry("duplicates dropped").or_default() += 1,
            Step::Held => *counts.entry("held while lining up").or_default() += 1,
            Step::Ignored => *counts.entry("ignored").or_default() += 1,
            Step::Diverged | Step::Failed => {
                // In production the session layer replaces the connection.
                println!("connection {} diverged from the applied history: dropped", d.conn);
                dropped.push(d.conn);
                assert_eq!(merge.drop_conn(d.conn, now), Lost::Kept);
            }
        }
    }

    println!(
        "{} messages, {} connections, {:.0} s of market",
        msgs.len(),
        conns.len(),
        span / 1000.0
    );
    let mut keys: Vec<_> = counts.iter().collect();
    keys.sort();
    for (k, v) in keys {
        println!("  {k:>22}: {v}");
    }
    let total: u64 = first_by.values().sum();
    for c in &conns {
        let n = first_by.get(&c.id).copied().unwrap_or(0);
        println!(
            "  connection {} delivered first: {:.1}%",
            c.id,
            100.0 * n as f64 / total as f64
        );
    }

    // How much earlier the merged feed applied each event than each single connection got it.
    for c in &conns {
        let mut lead: Vec<f64> = applied_at
            .iter()
            .filter_map(|(i, at)| {
                arrival
                    .get(&(c.id, *i))
                    .map(|single| (*single as f64 - *at as f64) / 1000.0)
            })
            .collect();
        lead.sort_by(f64::total_cmp);
        println!(
            "  merged lead over connection {} alone: mean {:.1} ms, p99 {:.1} ms, max {:.1} ms",
            c.id,
            lead.iter().sum::<f64>() / lead.len() as f64,
            quantile(&lead, 0.99),
            lead.last().unwrap()
        );
    }
    println!(
        "  merge cost: {:.0} ns per delivered message",
        merge_time.as_nanos() as f64 / deliveries.len() as f64
    );

    for t in &tokens {
        let (_, _, book_time, depth, _) = reference.state_of(t).expect("the reference has a book");
        assert!(book_time > 0 && depth.bid_levels().count() + depth.ask_levels().count() > 0);
        assert_eq!(merged.state_of(t), reference.state_of(t), "merged book differs on {t}");
    }
    println!("merged book == single-stream book on both tokens: OK");
}
