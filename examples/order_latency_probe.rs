//! Live order-latency probe: how long a real order takes to rest on the book
//! and how long a cancel takes to remove it, measured on the public feed.
//!
//! Safety:
//! - It only sends post-only GTC BUY orders at the minimum price tick (1c) and
//!   at the market's minimum size, on a token whose best bid is at least 20c.
//!   A post-only order that would cross is refused by the venue, so nothing
//!   can take liquidity.
//! - At most PROBE_ORDERS orders are sent (default 30, hard cap 50). Each one
//!   costs at most price x size (about $0.05) if a seller ever hits it.
//! - Each order ID is registered before it is sent, every cancel is checked
//!   against the venue's `canceled` list, and the probe stops if a cancel is
//!   not confirmed. On error, timeout or SIGINT/SIGTERM it cancels every order
//!   it sent and exits.
//! - If anything is unresolved at the end, it keeps cancelling it and all of
//!   this account's orders on the probed tokens until the last probed market
//!   ends (at least 60 s). Run it only while no other bot trades this account.
//! - It refuses to run unless PROBE_LIVE_CONFIRM=yes, or from a geoblocked host.
//!
//! PROBE_MODE=pipeline times a fire-and-forget order path instead: each
//! order's DELETE leaves 0, 2, 5, 10, 15 or 20 ms after its POST was sent,
//! without waiting for the POST's answer. It reports whether that first
//! DELETE cancels the order or reaches the venue before the order does (the
//! venue then refuses the POST as "Already canceled"), and both round trips.
//!
//! Credentials: PRIVATE_KEY, FUNDER_ADDRESS, SIGNATURE_TYPE (default 1), and
//! optionally POLYMARKET_API_KEY, POLYMARKET_API_SECRET and
//! POLYMARKET_API_PASSPHRASE, derived from the wallet key when absent.
//! Nothing secret is printed.
//!
//!     PROBE_LIVE_CONFIRM=yes cargo run --release --example order_latency_probe
use anyhow::{bail, ensure, Context, Result};
use parking_lot::Mutex;
use polymarket_clob_infra::polymarket::auth::derive_api_creds;
use polymarket_clob_infra::polymarket::rest::{self, ApiCreds, Client, NewOrder, Placed, CLOB_URL, GAMMA_URL};
use polymarket_clob_infra::polymarket::signing::{Wallet, BUY};
use polymarket_clob_infra::ws::connect::{self, Connection, WSS_URL};
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_ORDERS: usize = 50;
const PRICE: f64 = 0.01;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// PROBE_MODE=pipeline: how long after each POST its DELETE is sent, cycled.
const PIPELINE_DELAYS_MS: [u64; 6] = [0, 2, 5, 10, 15, 20];

fn env(name: &str) -> Result<String> {
    std::env::var(name)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .with_context(|| format!("{name} is required"))
}

struct Account {
    client: Client,
    wallet: Wallet,
    funder: String,
    signature_type: u8,
}

async fn account(http: &reqwest::Client) -> Result<Account> {
    let wallet: Wallet = env("PRIVATE_KEY")?.parse().context("Invalid private key")?;
    let funder: polymarket_clob_infra::abi::Address = env("FUNDER_ADDRESS")?
        .trim()
        .parse()
        .context("Invalid FUNDER_ADDRESS")?;
    let signature_type: u8 = std::env::var("SIGNATURE_TYPE").unwrap_or_else(|_| "1".into()).parse()?;
    ensure!(signature_type <= 2, "Unsupported SIGNATURE_TYPE");
    let creds = match ApiCreds::from_env()? {
        Some(creds) => creds,
        None => {
            println!("API credentials not set: deriving them from the wallet key");
            let (key, secret, passphrase) = derive_api_creds(http, &wallet, CLOB_URL).await?;
            ApiCreds::new(key, secret, passphrase)
        }
    };
    Ok(Account {
        client: Client::with_http(http.clone(), CLOB_URL, &wallet, creds),
        wallet,
        funder: funder.to_checksum(None),
        signature_type,
    })
}

fn now_ms() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64() * 1000.0
}

struct Market {
    slug: String,
    token: String,
    best_bid: f64,
    min_size: f64,
    neg_risk: bool,
    end: u64,
}

/// The BTC 5m market with at least `min_left` seconds to go, and a token whose
/// best bid is >= 20c.
async fn pick_market(client: &Client, min_left: u64, not_before: u64) -> Result<Market> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let mut start = now / 300 * 300;
    if start + 300 < now + min_left {
        start += 300;
    }
    start = start.max(not_before);
    let slug = format!("btc-updown-5m-{start}");
    let tokens = rest::market_tokens(client.http(), GAMMA_URL, &slug).await?;
    for token in tokens {
        let book = client.book(&token).await?;
        let best_bid = rest::best_bid(&book);
        let tick: f64 = book["tick_size"].as_str().unwrap_or("0.01").parse()?;
        let min_size: f64 = book["min_order_size"].as_str().unwrap_or("5").parse()?;
        ensure!(
            min_size > 0.0 && min_size <= 20.0,
            "unexpected minimum order size {min_size}"
        );
        let neg_risk = book["neg_risk"].as_bool().unwrap_or(false);
        if best_bid >= 0.20 && (tick - PRICE).abs() < 1e-9 {
            return Ok(Market {
                slug,
                token,
                best_bid,
                min_size,
                neg_risk,
                end: start + 300,
            });
        }
    }
    bail!("no token of {slug} has a best bid of at least 20c and a 1c tick")
}

async fn best_bid(client: &Client, token: &str) -> Result<f64> {
    Ok(rest::best_bid(&client.book(token).await?))
}

/// A 1c bid-level size observed on the feed: local arrival time, venue timestamp (ms), size.
#[derive(Clone, Copy)]
struct Obs {
    at: Instant,
    venue_ms: f64,
    size: f64,
}

#[derive(Default)]
struct Feed {
    obs: Vec<Obs>,
    closed: bool,
    last_rx: Option<Instant>,
}

fn level_size(event: &Value, token: &str) -> Option<(f64, f64)> {
    let ts: f64 = event["timestamp"].as_str()?.parse().ok()?;
    let is_1c = |p: &Value| {
        p.as_str()
            .and_then(|p| p.parse::<f64>().ok())
            .is_some_and(|p| (p - PRICE).abs() < 1e-9)
    };
    match event["event_type"].as_str()? {
        "book" if event["asset_id"].as_str()? == token => {
            let size = event["bids"]
                .as_array()?
                .iter()
                .find(|l| is_1c(&l["price"]))
                .and_then(|l| l["size"].as_str()?.parse().ok())
                .unwrap_or(0.0);
            Some((size, ts))
        }
        "price_change" => event["price_changes"]
            .as_array()?
            .iter()
            .find(|ch| {
                ch["asset_id"].as_str() == Some(token) && ch["side"].as_str() == Some("BUY") && is_1c(&ch["price"])
            })
            .and_then(|ch| Some((ch["size"].as_str()?.parse().ok()?, ts))),
        _ => None,
    }
}

/// Reads the market channel in its own task, stamping each event on arrival.
async fn start_feed(token: String) -> Result<(Arc<Mutex<Feed>>, tokio::task::JoinHandle<()>)> {
    let mut connection: Connection = connect::connect(WSS_URL, std::slice::from_ref(&token)).await?;
    let feed = Arc::new(Mutex::new(Feed::default()));
    let f = feed.clone();
    let task = tokio::spawn(async move {
        while let Some(frame) = connection.next().await {
            f.lock().last_rx = Some(frame.received);
            let Ok(v) = serde_json::from_str::<Value>(&frame.text) else {
                continue;
            };
            for e in v.as_array().cloned().unwrap_or_else(|| vec![v]) {
                if let Some((size, venue_ms)) = level_size(&e, &token) {
                    f.lock().obs.push(Obs {
                        at: frame.received,
                        venue_ms,
                        size,
                    });
                }
            }
        }
        f.lock().closed = true;
    });
    Ok((feed, task))
}

fn current_level(feed: &Arc<Mutex<Feed>>) -> Option<f64> {
    feed.lock().obs.last().map(|o| o.size)
}

/// First observation after `from` where the level moved by exactly `delta`
/// from the level before it. None (sample dropped) if any other change hit the
/// 1c level in the window, or if the matching event's venue timestamp is
/// earlier than the send (allowing 20 ms of clock error).
async fn wait_for_change(
    feed: &Arc<Mutex<Feed>>,
    from: Instant,
    from_wall_ms: f64,
    before: f64,
    delta: f64,
) -> Option<Obs> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        {
            let f = feed.lock();
            for o in f.obs.iter().filter(|o| o.at >= from) {
                if (o.size - before).abs() < 1e-9 {
                    continue; // unchanged level (for example a full book snapshot)
                }
                if (o.size - before - delta).abs() < 1e-6 && o.venue_ms >= from_wall_ms - 20.0 {
                    return Some(*o);
                }
                return None; // some other change: ambiguous, drop the sample
            }
            if f.closed {
                return None;
            }
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// Cancels and confirms from the venue's `canceled` list or a non-resting
/// order status. Returns the DELETE round trip in ms.
async fn cancel_confirmed(client: &Client, id: &str) -> Result<f64> {
    let start = Instant::now();
    let cancelled = client.cancel(id).await?;
    let rtt = start.elapsed().as_secs_f64() * 1000.0;
    if cancelled.cancelled(id) || client.order_closed(id).await? {
        return Ok(rtt);
    }
    bail!("cancel of {id} not confirmed: {:?}", cancelled.not_canceled)
}

fn pct(v: &mut [f64], q: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((q * v.len() as f64) as usize).min(v.len() - 1)]
}

fn fmt(v: Option<f64>) -> String {
    v.map(|x| format!("{x:.1} ms")).unwrap_or_else(|| "-".into())
}

type Placed_ = Arc<Mutex<Vec<(String, bool)>>>;
type Probed = Arc<Mutex<Vec<(String, u64)>>>;

/// The probe's order on `m`, `i`-th of the run: a distinctive size (min + 0.05..0.94
/// shares) identifies it on the public feed.
fn probe_order(a: &Account, m: &Market, i: usize) -> Result<(NewOrder, f64)> {
    let size = m.min_size + ((i % 90) as f64 + 5.0) / 100.0;
    let shares = (size * 1_000_000.0).round() as u64;
    let cash = shares * (PRICE * 1_000_000.0).round() as u64 / 1_000_000;
    ensure!(cash <= 60_000, "order notional {cash} USDC units exceeds the $0.06 cap");
    let order = NewOrder::gtc(&m.token, BUY, cash, shares, &a.funder)
        .post_only()
        .neg_risk(m.neg_risk)
        .signature_type(a.signature_type);
    Ok((order, size))
}

fn register(placed: &Placed_, probed: &Probed, m: &Market, order_id: &str) {
    let mut tokens = probed.lock();
    if !tokens.iter().any(|(t, _)| *t == m.token) {
        tokens.push((m.token.clone(), m.end));
    }
    placed.lock().push((order_id.to_string(), false));
}

fn mark_closed(placed: &Placed_, ids: &[&str]) {
    for entry in placed.lock().iter_mut().filter(|(x, _)| ids.contains(&x.as_str())) {
        entry.1 = true;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    ensure!(
        std::env::var("PROBE_LIVE_CONFIRM").as_deref() == Ok("yes"),
        "Set PROBE_LIVE_CONFIRM=yes to send live orders"
    );
    let n: usize = std::env::var("PROBE_ORDERS").unwrap_or_else(|_| "30".into()).parse()?;
    ensure!((1..=MAX_ORDERS).contains(&n), "PROBE_ORDERS must be 1 to {MAX_ORDERS}");
    let http = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_nodelay(true)
        .build()?;
    let a = account(&http).await?;
    for _ in 0..5 {
        a.client.server_time().await?; // warm DNS, TCP and TLS
    }
    let geo: Value = http
        .get("https://polymarket.com/api/geoblock")
        .send()
        .await?
        .json()
        .await
        .unwrap_or(Value::Null);
    println!("geoblock: blocked={} country={}", geo["blocked"], geo["country"]);
    // Polymarket blocks Ireland and the Netherlands on the website only; the
    // trading API is open there (docs.polymarket.com/api-reference/geoblock).
    // Anywhere else, "blocked" means no trading.
    let api_open = matches!(geo["country"].as_str(), Some("NL") | Some("IE"));
    ensure!(
        geo["blocked"].as_bool() == Some(false) || api_open,
        "This host is geoblocked; refusing to trade"
    );

    // IDs of every order sent, registered before its POST; true = confirmed closed.
    let placed: Placed_ = Arc::default();
    let probed: Probed = Arc::default();
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let pipeline = match std::env::var("PROBE_MODE").as_deref() {
        Err(_) | Ok("") | Ok("default") => false,
        Ok("pipeline") => true,
        Ok(other) => bail!("Unknown PROBE_MODE {other:?}; use pipeline or leave it unset"),
    };
    let result = tokio::select! {
        r = async {
            if pipeline { run_pipeline(&a, n, &placed, &probed).await } else { run(&a, n, &placed, &probed).await }
        } => r,
        _ = tokio::signal::ctrl_c() => Err(anyhow::anyhow!("interrupted")),
        _ = terminate.recv() => Err(anyhow::anyhow!("terminated")),
    };
    // Cleanup: cancel everything this run sent; report anything not confirmed.
    // If the run was cut short, a POST may still be in flight: give it the full
    // request timeout to land before trying to cancel.
    if result.is_err() {
        tokio::time::sleep(REQUEST_TIMEOUT + Duration::from_secs(1)).await;
    }
    // Each round cancels unresolved IDs first (newest first), then sweeps all
    // of this account's orders on the probed tokens. A submission whose answer
    // was lost may be accepted late, so while anything is unresolved the rounds
    // continue every 5 s until 10 s after the last probed market ends: the
    // venue closes a market's orders when it ends.
    let last_end = probed.lock().iter().map(|(_, end)| *end).max().unwrap_or(0);
    let now_s = || {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    };
    let deadline = (now_s() + 60).max(last_end + 10);
    loop {
        let unresolved: Vec<String> = placed
            .lock()
            .iter()
            .rev()
            .filter(|(_, closed)| !closed)
            .map(|(id, _)| id.clone())
            .collect();
        if unresolved.is_empty() {
            break;
        }
        for id in &unresolved {
            if let Ok(cancelled) = a.client.cancel(id).await {
                // Confirmed only by the canceled list or a non-resting order status.
                if cancelled.cancelled(id) || a.client.order_closed(id).await.unwrap_or(false) {
                    mark_closed(&placed, &[id]);
                }
            }
        }
        let tokens: Vec<String> = probed.lock().iter().map(|(t, _)| t.clone()).collect();
        for token in tokens {
            let _ = a.client.cancel_market(&token).await;
        }
        if now_s() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    let unconfirmed: Vec<String> = placed
        .lock()
        .iter()
        .filter(|(_, closed)| !closed)
        .map(|(id, _)| id.clone())
        .collect();
    if !unconfirmed.is_empty() {
        eprintln!("WARNING: could not confirm these orders are closed; cancel them on polymarket.com: {unconfirmed:?}");
    }
    result
}

async fn run(a: &Account, n: usize, placed: &Placed_, probed: &Probed) -> Result<()> {
    let (mut ack, mut on_book, mut on_book_venue, mut cancel_ack, mut off_book, mut off_book_venue) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);
    let mut i = 0;
    let mut next_window = 0;
    while i < n {
        let m = pick_market(&a.client, 150, next_window).await?;
        next_window = m.end; // each window gets at most one batch
        println!(
            "market {} token ..{} best_bid {} min_size {}",
            m.slug,
            &m.token[m.token.len() - 6..],
            m.best_bid,
            m.min_size
        );
        let (feed, task) = start_feed(m.token.clone()).await?;
        let wait_start = Instant::now();
        while current_level(&feed).is_none() && wait_start.elapsed() < Duration::from_secs(10) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        ensure!(current_level(&feed).is_some(), "no initial book from the market feed");
        // Up to 10 orders per market window while at least 60 s remain, then the next window.
        let mut k = 0;
        while k < 10 && i < n && SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() + 60 < m.end {
            {
                let f = feed.lock();
                ensure!(!f.closed, "market feed closed");
                ensure!(
                    f.last_rx.is_some_and(|t| t.elapsed() < Duration::from_secs(12)),
                    "market feed silent for 12 s"
                );
            }
            let bid = best_bid(&a.client, &m.token).await?;
            if bid < 0.20 {
                println!("best bid fell to {bid}; moving to the next window");
                break;
            }
            let (order, size) = probe_order(a, &m, i)?;
            let before = current_level(&feed).unwrap_or(0.0);
            let t0_wall = now_ms();
            let t0 = Instant::now();
            // The order ID is known before the POST: registered first, so a lost
            // answer still gets its cancel.
            let signed = polymarket_clob_infra::polymarket::signing::sign_order(
                &a.wallet,
                &order.token_id,
                order.maker_amount,
                order.taker_amount,
                order.side,
                &order.funder,
                order.neg_risk,
                order.signature_type,
            )
            .await?;
            register(placed, probed, &m, &signed.order_id);
            let body = rest::order_body(&order, &signed, a.client.address(), a.client.creds().key());
            let answer = a.client.post_order(&body).await;
            let t_ack = t0.elapsed().as_secs_f64() * 1000.0;
            let answer = answer?; // a lost answer stays registered for cleanup
            let placed_order = Placed {
                order_id: answer["orderID"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .unwrap_or(&signed.order_id)
                    .to_string(),
                accepted: answer["success"].as_bool() != Some(false),
                status: String::new(),
                signed,
                answer,
            };
            let id = placed_order.order_id.clone();
            if id != placed_order.signed.order_id {
                placed.lock().push((id.clone(), false));
            }
            let seen = if placed_order.accepted {
                wait_for_change(&feed, t0, t0_wall, before, size).await
            } else {
                None
            };
            let level_now = current_level(&feed).unwrap_or(before);
            let t1_wall = now_ms();
            let t1 = Instant::now();
            // An explicit refusal (success=false) never rested on the book.
            let t_cancel = if placed_order.accepted {
                Some(cancel_confirmed(&a.client, &id).await?)
            } else {
                None
            };
            mark_closed(placed, &[&id, &placed_order.signed.order_id]);
            let removed = if seen.is_some() {
                wait_for_change(&feed, t1, t1_wall, level_now, -size).await
            } else {
                None
            };
            let visible = seen.map(|o| o.at.duration_since(t0).as_secs_f64() * 1000.0);
            let visible_venue = seen.map(|o| o.venue_ms - t0_wall);
            let gone = removed.map(|o| o.at.duration_since(t1).as_secs_f64() * 1000.0);
            let gone_venue = removed.map(|o| o.venue_ms - t1_wall);
            println!(
                "order {:2}: post ack {:6.1} ms | on book (feed here) {:>9} | venue stamp {:>9} | cancel ack {:>9} | off book {:>9} | venue stamp {:>9} | {}",
                i + 1,
                t_ack,
                fmt(visible),
                fmt(visible_venue),
                fmt(t_cancel),
                fmt(gone),
                fmt(gone_venue),
                if placed_order.accepted {
                    placed_order.answer["status"].as_str().unwrap_or("ok").to_string()
                } else {
                    format!("refused: {}", placed_order.answer["errorMsg"])
                }
            );
            ack.push(t_ack);
            cancel_ack.extend(t_cancel);
            on_book.extend(visible);
            on_book_venue.extend(visible_venue);
            off_book.extend(gone);
            off_book_venue.extend(gone_venue);
            i += 1;
            k += 1;
            tokio::time::sleep(Duration::from_millis(1500)).await;
        }
        task.abort();
    }
    println!("\nSUMMARY over {n} orders (ms): p10 / p50 / p90. Venue stamps use this host's clock; read them only as a rough cross-check.");
    for (name, v) in [
        ("POST /order round trip", &mut ack),
        ("send -> on book (feed here)", &mut on_book),
        ("send -> venue book stamp", &mut on_book_venue),
        ("DELETE round trip", &mut cancel_ack),
        ("cancel -> off book (feed here)", &mut off_book),
        ("cancel -> venue book stamp", &mut off_book_venue),
    ] {
        println!(
            "  {:32} n={:2}  {:7.1} / {:7.1} / {:7.1}",
            name,
            v.len(),
            pct(v, 0.1),
            pct(v, 0.5),
            pct(v, 0.9)
        );
    }
    Ok(())
}

/// PROBE_MODE=pipeline: see the header.
async fn run_pipeline(a: &Account, n: usize, placed: &Placed_, probed: &Probed) -> Result<()> {
    // Per delay: POST and DELETE round trips; of accepted orders, those the first DELETE
    // cancelled and those whose POST was answered first; accepted, refused, and refused
    // because the DELETE arrived first.
    #[derive(Clone, Default)]
    struct Stat {
        post: Vec<f64>,
        delete: Vec<f64>,
        cancelled: usize,
        post_first: usize,
        accepted: usize,
        refused: usize,
        beaten: usize,
    }
    let mut stats: Vec<Stat> = vec![Stat::default(); PIPELINE_DELAYS_MS.len()];
    let mut i = 0;
    let mut next_window = 0;
    while i < n {
        let m = pick_market(&a.client, 150, next_window).await?;
        next_window = m.end; // each window gets at most one batch
        println!(
            "market {} token ..{} best_bid {} min_size {}",
            m.slug,
            &m.token[m.token.len() - 6..],
            m.best_bid,
            m.min_size
        );
        let mut k = 0;
        while k < 10 && i < n && SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() + 60 < m.end {
            let bid = best_bid(&a.client, &m.token).await?;
            if bid < 0.20 {
                println!("best bid fell to {bid}; moving to the next window");
                break;
            }
            let (order, _) = probe_order(a, &m, i)?;
            let signed = polymarket_clob_infra::polymarket::signing::sign_order(
                &a.wallet,
                &order.token_id,
                order.maker_amount,
                order.taker_amount,
                order.side,
                &order.funder,
                order.neg_risk,
                order.signature_type,
            )
            .await?;
            register(placed, probed, &m, &signed.order_id);
            let body = rest::order_body(&order, &signed, a.client.address(), a.client.creds().key());
            let slot = i % PIPELINE_DELAYS_MS.len();
            let delay = PIPELINE_DELAYS_MS[slot];
            let id = signed.order_id.clone();
            let t0 = Instant::now();
            let post = async {
                let answer = a.client.post_order(&body).await;
                (Instant::now(), answer)
            };
            let delete = async {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                let sent = Instant::now();
                let answer = a.client.cancel(&id).await;
                (sent, Instant::now(), answer)
            };
            let ((post_done, post_answer), (delete_sent, delete_done, delete_answer)) = tokio::join!(post, delete);
            // The venue keeps a DELETE that arrives before its order: the order
            // is then refused ("Already canceled") and never rests. Any other
            // failure stays registered for the cleanup.
            let (post_answer, beaten) = match post_answer {
                Ok(answer) => (answer, false),
                Err(error)
                    if error
                        .downcast_ref::<rest::HttpStatus>()
                        .and_then(rest::HttpStatus::message)
                        .is_some_and(|m| m.contains("Already canceled")) =>
                {
                    (
                        serde_json::json!({"success": false, "errorMsg": "already canceled"}),
                        true,
                    )
                }
                Err(error) => return Err(error),
            };
            let accepted = post_answer["success"].as_bool() != Some(false);
            let first_cancelled = delete_answer.as_ref().is_ok_and(|c| c.cancelled(&id));
            let reason = match &delete_answer {
                Ok(c) => c
                    .not_canceled
                    .iter()
                    .find(|(x, _)| *x == id)
                    .map(|(_, why)| why.clone())
                    .unwrap_or_default(),
                Err(error) => format!("error: {error}"),
            };
            // Missed by the first DELETE (it arrived first, or failed): the order
            // may rest now that its POST is answered; cancel it again and confirm.
            if accepted && !first_cancelled {
                cancel_confirmed(&a.client, &id).await?;
            }
            mark_closed(placed, &[&id]);
            let ms = |a: Instant, b: Instant| a.saturating_duration_since(b).as_secs_f64() * 1000.0;
            let (post_ms, delete_ms) = (ms(post_done, t0), ms(delete_done, delete_sent));
            println!(
                "order {:2}: delete +{:2} ms | post ack {:6.1} ms | delete sent +{:5.1} ms, ack {:6.1} ms | first delete {} | {}",
                i + 1,
                delay,
                post_ms,
                ms(delete_sent, t0),
                delete_ms,
                if beaten {
                    "arrived first".to_string()
                } else if !accepted {
                    "-".to_string()
                } else if first_cancelled {
                    "cancelled".to_string()
                } else {
                    format!("missed ({reason})")
                },
                if beaten {
                    "post refused: already canceled".to_string()
                } else if accepted {
                    "accepted".to_string()
                } else {
                    format!("refused: {}", post_answer["errorMsg"])
                }
            );
            let entry = &mut stats[slot];
            entry.post.push(post_ms);
            entry.delete.push(delete_ms);
            if beaten {
                entry.beaten += 1;
            } else if !accepted {
                entry.refused += 1;
            } else {
                entry.cancelled += first_cancelled as usize;
                entry.post_first += (post_done <= delete_done) as usize;
                entry.accepted += 1;
            }
            i += 1;
            k += 1;
            tokio::time::sleep(Duration::from_millis(1500)).await;
        }
    }
    println!("\nSUMMARY by DELETE delay after the POST (ms: p50 / p90):");
    for (delay, s) in PIPELINE_DELAYS_MS.iter().zip(stats.iter_mut()) {
        println!(
            "  +{:2} ms: DELETE first (POST refused) {:2} | accepted {:2}, first DELETE cancelled {:2}, missed {:2} | other refusals {:2} | POST {:6.1} / {:6.1} | DELETE {:6.1} / {:6.1}",
            delay,
            s.beaten,
            s.accepted,
            s.cancelled,
            s.accepted - s.cancelled,
            s.refused,
            pct(&mut s.post, 0.5),
            pct(&mut s.post, 0.9),
            pct(&mut s.delete, 0.5),
            pct(&mut s.delete, 0.9)
        );
    }
    Ok(())
}
