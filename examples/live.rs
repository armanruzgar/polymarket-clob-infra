//! Runs the redundant feed on the venue for a while and reports what each
//! connection delivered: the share of events it delivered first, its lag over
//! its life and over the last window, the merge's counts, and the merged
//! book. Reads public data only; sends nothing.
//!
//!     cargo run --release --example live -- [seconds] [slug]
//!
//! `seconds` defaults to 60. The market is the current BTC 5-minute one
//! (slug `btc-updown-5m-<start>`) unless a Gamma slug is given.
//! `FEED_CONNECTIONS` (1-4, default 3) and the other `FEED_*` settings apply
//! (ws::feed_lag::FeedSettings).
use anyhow::{Context, Result};
use polymarket_clob_infra::polymarket::rest::{market_tokens, GAMMA_URL};
use polymarket_clob_infra::ws::feed::Feed;
use polymarket_clob_infra::ws::feed_lag::FeedSettings;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn lag(p: Option<i64>) -> String {
    p.map_or("-".to_string(), |v| format!("{v} ms"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let seconds: u64 = args.next().map(|s| s.parse()).transpose()?.unwrap_or(60);
    let http = reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?;
    let (slug, tokens) = match args.next() {
        Some(slug) => {
            let tokens = market_tokens(&http, GAMMA_URL, &slug)
                .await
                .with_context(|| format!("market {slug}"))?;
            (slug, tokens)
        }
        None => {
            // The current 5-minute window, or the next one when this one has
            // under two minutes left (and the venue already lists it).
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            let current = now / 300 * 300;
            let mut picked = None;
            for start in [current + 300, current] {
                if start == current + 300 && now < current + 180 {
                    continue;
                }
                let slug = format!("btc-updown-5m-{start}");
                if let Ok(tokens) = market_tokens(&http, GAMMA_URL, &slug).await {
                    picked = Some((slug, tokens));
                    break;
                }
            }
            picked.context("no current BTC 5-minute market on Gamma; pass a slug")?
        }
    };
    let settings = FeedSettings::from_env()?;
    println!(
        "market {slug}: tokens ..{} / ..{}",
        &tokens[0][tokens[0].len() - 6..],
        &tokens[1][tokens[1].len() - 6..]
    );
    println!("{}", settings.describe());
    let feed = Feed::start(settings, vec![[tokens[0].clone(), tokens[1].clone()]])?;
    let started = Instant::now();
    let mut report = tokio::time::interval(Duration::from_secs(10));
    report.tick().await;
    while started.elapsed() < Duration::from_secs(seconds) {
        tokio::select! {
            _ = report.tick() => {}
            _ = tokio::signal::ctrl_c() => break,
        }
        let stats = feed.stats();
        let data = feed.data();
        let book = |t: &str| {
            data.state_of(t).map_or("-".to_string(), |(bid, ask, _, _, _)| {
                format!("{:.3} / {:.3}", bid.unwrap_or(f64::NAN), ask.unwrap_or(f64::NAN))
            })
        };
        println!(
            "[{:>4} s] applied {} dup {} held {} replaced {} dropped {} rebuilt {} | Up {} | Down {} | clock {}",
            started.elapsed().as_secs(),
            stats.applied,
            stats.duplicates,
            stats.held,
            stats.replaced,
            stats.dropped,
            stats.rebuilt,
            book(&tokens[0]),
            book(&tokens[1]),
            stats
                .clock_offset
                .map_or("unchecked".to_string(), |(lo, hi)| format!("[{lo}, {hi}] ms")),
        );
        let first_total: u64 = stats.conns.iter().map(|c| c.first).sum::<u64>().max(1);
        for c in &stats.conns {
            println!(
                "    conn {} (slot {}): {} | first {:.1}% | window p50 {} p90 {} ({} samples) | life p50 {} p90 {} ({})",
                c.conn,
                c.slot,
                c.since.map_or("connecting".to_string(), |at| format!("up {} s", at.elapsed().as_secs())),
                100.0 * c.first as f64 / first_total as f64,
                lag(c.lag_p50),
                lag(c.lag_p90),
                c.lag_samples,
                lag(c.life_p50),
                lag(c.life_p90),
                c.life_samples,
            );
            if let Some(why) = &c.last_closed {
                println!("        previous connection ended: {why}");
            }
        }
    }
    let stats = feed.stats();
    match feed.snapshot() {
        Ok(snapshot) => println!(
            "snapshot: Up {:?}/{:?} Down {:?}/{:?}, {} ms since the stream last proved it current",
            snapshot.best_bid(&tokens[0]),
            snapshot.best_ask(&tokens[0]),
            snapshot.best_bid(&tokens[1]),
            snapshot.best_ask(&tokens[1]),
            snapshot.max_age_ms()
        ),
        Err(error) => println!("snapshot refused: {error:#}"),
    }
    println!(
        "{} connections, {} s: {} applied, {} duplicates dropped, {} replaced, {} dropped",
        stats.conns.len(),
        started.elapsed().as_secs(),
        stats.applied,
        stats.duplicates,
        stats.replaced,
        stats.dropped
    );
    feed.stop().await;
    Ok(())
}
