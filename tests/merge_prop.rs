//! Property test of the merge: however the venue's recorded stream is delivered
//! over several connections (each with its own delay and jitter, some copies
//! lost, some messages repeated), the merged book equals the book built from
//! the venue's own stream, and every event is applied exactly once.
//!
//! A connection that skips a *book* event is an integrity failure the merge
//! detects (examples/replay.rs shows one); this test keeps the book events of
//! every connection complete and lets prints and whole frames be dropped or
//! repeated, which the venue does.
use polymarket_clob_infra::market_data::MarketData;
use polymarket_clob_infra::ws::feed_merge::{Applied, ConnId, EventKey, Expired, Lost, MarketMerge, Step};
use proptest::prelude::*;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/recorded_market.jsonl.gz");
/// The first messages of the recording are enough: the merge's rules are
/// exercised within seconds of market.
const MESSAGES: usize = 4_000;

fn load() -> &'static Vec<(u64, Value)> {
    static MSGS: OnceLock<Vec<(u64, Value)>> = OnceLock::new();
    MSGS.get_or_init(|| {
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
            if out.len() >= MESSAGES {
                break;
            }
        }
        out
    })
}

fn is_book_event(msg: &Value) -> bool {
    matches!(
        msg["event_type"].as_str(),
        Some("book" | "price_change" | "tick_size_change")
    )
}

#[derive(Clone, Debug)]
struct Conn {
    base_ms: f64,
    jitter_ms: f64,
    /// A window of the recording (fractions) during which it runs `slow_ms` late.
    slow: (f64, f64),
    slow_ms: f64,
    /// Prints it never delivers (indices into the stream).
    drop_prints: Vec<usize>,
}

fn conn() -> impl Strategy<Value = Conn> {
    (
        5.0f64..40.0,
        0.0f64..10.0,
        (0.0f64..1.0, 0.0f64..1.0),
        0.0f64..150.0,
        prop::collection::vec(0usize..MESSAGES, 0..20),
    )
        .prop_map(|(base_ms, jitter_ms, (a, b), slow_ms, drop_prints)| Conn {
            base_ms,
            jitter_ms,
            slow: (a.min(b), a.max(b)),
            slow_ms,
            drop_prints,
        })
}

struct Delivery {
    at_us: u64,
    conn: ConnId,
    msg: usize,
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    #[test]
    fn merged_book_equals_the_venue_stream(
        conns in prop::collection::vec(conn(), 1..=4),
        // Messages the venue sends twice in a row, verbatim, to every connection.
        repeats in prop::collection::vec(2usize..MESSAGES, 0..20),
        seed in any::<u64>(),
    ) {
        let recording = load();
        let mut msgs: Vec<(u64, &Value)> = Vec::with_capacity(recording.len() + repeats.len());
        for (i, (rx, m)) in recording.iter().enumerate() {
            msgs.push((*rx, m));
            if repeats.contains(&i) {
                msgs.push((*rx, m));
            }
        }
        let msgs = &msgs;
        let tokens: Vec<String> = msgs
            .iter()
            .take(2)
            .map(|(_, m)| m["asset_id"].as_str().unwrap().to_string())
            .collect();
        let (first_rx, last_rx) = (msgs[0].0, msgs.last().unwrap().0);
        let span = (last_rx - first_rx).max(1) as f64;

        let reference = MarketData::default();
        reference.reset(&tokens);
        reference.connected();
        for (_, m) in msgs.iter() {
            reference.ingest_any(m);
        }

        use rand::{rngs::StdRng, Rng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(seed);
        let mut deliveries = Vec::new();
        for (c, conn) in conns.iter().enumerate() {
            let id = c as ConnId + 1;
            let mut last = 0u64;
            for (i, (rx, m)) in msgs.iter().enumerate() {
                if !is_book_event(m) && conn.drop_prints.contains(&i) {
                    continue;
                }
                let at = (rx - first_rx) as f64 / span;
                let jitter = -conn.jitter_ms * (1.0 - rng.random::<f64>()).ln();
                let slow = if (conn.slow.0..conn.slow.1).contains(&at) { conn.slow_ms } else { 0.0 };
                let at_us = (((rx - first_rx) as f64 + conn.base_ms + jitter + slow) * 1000.0) as u64;
                last = last.max(at_us);
                deliveries.push(Delivery { at_us: last, conn: id, msg: i });
            }
        }
        deliveries.sort_by_key(|d| (d.at_us, d.conn));

        let start = Instant::now();
        let merged = MarketData::default();
        merged.reset(&tokens);
        merged.connected();
        let mut merge = MarketMerge::default();
        for c in 0..conns.len() {
            merge.subscribe(c as ConnId + 1, start);
        }
        let mut snapshotted: HashSet<(ConnId, usize)> = HashSet::new();
        let mut applied_keys: HashMap<EventKey, u32> = HashMap::new();
        let mut next_tick = 0u64;
        let mut apply = |list: Vec<Applied>| {
            for a in list {
                merged.ingest_any(&a.msg);
                if !a.snapshot {
                    *applied_keys.entry(EventKey::of(&a.msg)).or_default() += 1;
                }
            }
        };
        for d in &deliveries {
            let now = start + Duration::from_micros(d.at_us);
            if d.at_us >= next_tick {
                for expired in merge.tick(now, &|_| false) {
                    match expired {
                        Expired::Aligned(_, list) | Expired::Rebuild(_, list) => apply(list),
                        Expired::Failed(_) => {}
                    }
                }
                next_tick = d.at_us + 250_000;
            }
            let msg = msgs[d.msg].1;
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
            match step {
                Step::Apply(list) => apply(list),
                Step::Duplicate | Step::Held | Step::Ignored => {}
                Step::Diverged | Step::Failed => {
                    prop_assert!(false, "connection {} diverged or failed with complete book events", d.conn);
                }
            }
        }
        // A connection may still hold its tail at the end: let it line up.
        let end = start + Duration::from_micros(deliveries.last().unwrap().at_us + 1);
        for expired in merge.tick(end, &|_| false) {
            if let Expired::Aligned(_, list) | Expired::Rebuild(_, list) = expired {
                apply(list);
            }
        }
        for c in 0..conns.len() {
            prop_assert!(!matches!(merge.drop_conn(c as ConnId + 1, end), Lost::Rebuild(_)));
        }

        for t in &tokens {
            prop_assert!(reference.state_of(t).is_some(), "no book for {t}");
            prop_assert_eq!(merged.state_of(t), reference.state_of(t), "merged book differs on {}", t);
        }
        // Every book event of the stream applied as many times as the venue sent
        // it (the two subscription snapshots are applied as snapshots, not events).
        let mut venue: HashMap<EventKey, u32> = HashMap::new();
        for (_, m) in msgs.iter().skip(2).filter(|(_, m)| is_book_event(m)) {
            *venue.entry(EventKey::of(m)).or_default() += 1;
        }
        for (key, n) in &venue {
            prop_assert_eq!(applied_keys.get(key).copied().unwrap_or(0), *n, "book event applied a different number of times");
        }
    }
}
