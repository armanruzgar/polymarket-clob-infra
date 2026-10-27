//! Hot-path costs: building the book from the venue's stream, keying and
//! merging feed messages, and signing an order.
//!
//!     cargo bench
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use polymarket_clob_infra::market_data::MarketData;
use polymarket_clob_infra::polymarket::signing::{hmac_signature, unsigned_order, Wallet, BUY};
use polymarket_clob_infra::ws::feed_merge::{EventKey, MarketMerge};
use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::time::Instant;

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/recorded_market.jsonl.gz");

fn load() -> Vec<Value> {
    let file = std::fs::File::open(FIXTURE).expect("fixture");
    let mut out = Vec::new();
    for line in BufReader::new(flate2::read::GzDecoder::new(file)).lines() {
        let frame: Value = serde_json::from_str(&line.unwrap()).unwrap();
        match serde_json::from_str(frame["m"].as_str().unwrap()).unwrap() {
            Value::Array(list) => out.extend(list),
            m => out.push(m),
        }
    }
    out
}

fn feed(c: &mut Criterion) {
    let msgs = load();
    let tokens: Vec<String> = msgs[..2]
        .iter()
        .map(|m| m["asset_id"].as_str().unwrap().to_string())
        .collect();
    let mut g = c.benchmark_group("feed");
    g.throughput(Throughput::Elements(msgs.len() as u64));
    g.bench_function("book: apply recorded market", |b| {
        b.iter(|| {
            let book = MarketData::default();
            book.reset(&tokens);
            book.connected();
            for m in &msgs {
                black_box(book.ingest_any(m));
            }
        })
    });
    g.bench_function("merge: event key", |b| {
        b.iter(|| {
            for m in &msgs {
                black_box(EventKey::of(m));
            }
        })
    });
    g.bench_function("merge: one connection, recorded market", |b| {
        b.iter(|| {
            let now = Instant::now();
            let mut merge = MarketMerge::default();
            merge.subscribe(1, now);
            for (i, m) in msgs.iter().enumerate() {
                let step = match m["event_type"].as_str() {
                    Some("book") if i < 2 => merge.snapshot(1, i, m, now),
                    Some("book" | "price_change" | "tick_size_change") => merge.book_event(1, EventKey::of(m), m, now),
                    _ => merge.other(1, EventKey::of(m), m, now),
                };
                black_box(step);
            }
        })
    });
    g.finish();
}

fn signing(c: &mut Criterion) {
    // Public deterministic test key (scalar 1), never used for trading.
    let wallet: Wallet = format!("{:064x}", 1).parse().unwrap();
    let funder = "0x1111111111111111111111111111111111111111";
    let mut g = c.benchmark_group("signing");
    g.bench_function("order: EIP-712 hash + ECDSA", |b| {
        b.iter(|| {
            let order = unsigned_order(
                &wallet,
                "12345678901234567890",
                2_500_000,
                5_000_000,
                BUY,
                funder,
                false,
                1,
            )
            .unwrap();
            black_box(order.sign(&wallet).unwrap())
        })
    });
    let secret = "c2VjcmV0LXNlY3JldC1zZWNyZXQtc2VjcmV0LXNlY3I=";
    g.bench_function("request: L2 HMAC", |b| {
        b.iter(|| black_box(hmac_signature(secret, "1700000000", "POST", "/order", r#"{"order":{}}"#).unwrap()))
    });
    g.finish();
}

criterion_group!(benches, feed, signing);
criterion_main!(benches);
