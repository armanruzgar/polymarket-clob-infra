//! The redundant market-data feed: [`FeedSettings::connections`] connections
//! carrying the same subscription, merged into one book per market
//! ([`MarketMerge`]), every connection's delivery lag watched ([`LagWindow`])
//! and a late one replaced.
//!
//! One task owns it all. Each slot runs a connection ([`super::connect`]);
//! its frames come to the task with the instant they arrived. A frame's
//! messages go through the market's merge: applied (the first copy, in the
//! venue's order), dropped as a duplicate, or held until the connection lines
//! up. What is applied goes into one [`MarketData`], the decision snapshot.
//!
//! A connection is replaced when
//! - its book events disagree with the applied history ([`Step::Diverged`]),
//!   or it fell too far behind to line up ([`Step::Failed`]);
//! - its median lag over the window stays [`FeedSettings::lag_excess_ms`] above
//!   the best other connection's for [`FeedSettings::lag_sustain`] (no clock
//!   needed: the local clock's error cancels), or above
//!   [`FeedSettings::lag_limit_ms`] while the venue's clock check bounds the
//!   local clock's offset within [`FeedSettings::max_clock_offset_ms`];
//! - the venue closes it, it goes silent, or the socket fails.
//!
//! A replaced slot reopens at once; a slot whose connections fail in a row
//! waits [`FeedSettings::reconnect_base`], doubled each time up to
//! [`FeedSettings::reconnect_max`]. A market stays live while any connection
//! is in line for it; the merge rebuilds it from a fresh snapshot otherwise.
//! [`Feed::snapshot`] refuses while a market has no connection that can serve
//! it.
use super::connect::{self, Closed, Connection, Frame};
use super::feed_lag::{FeedSettings, LagWindow, OffsetBounds};
use super::feed_merge::{Applied, ConnId, EventKey, Expired, Lost, MarketMerge, Step};
use crate::market_data::{MarketData, MarketSnapshot};
use anyhow::{ensure, Result};
use parking_lot::Mutex;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Notify};

/// Merge upkeep and the lag rules run this often.
pub const TICK: Duration = Duration::from_millis(250);
/// The venue's clock is checked this often (the absolute lag limit needs it).
pub const CLOCK_CHECK_EVERY: Duration = Duration::from_secs(900);
const CLOCK_ROUNDS: usize = 6;
/// A connection that lived this long resets its slot's failure streak.
const STABLE: Duration = Duration::from_secs(30);

/// One connection's state and numbers.
#[derive(Clone, Debug)]
pub struct ConnStats {
    pub slot: usize,
    pub conn: ConnId,
    /// When the handshake completed; None while connecting.
    pub since: Option<Instant>,
    pub frames: u64,
    pub messages: u64,
    /// Book events and prints it delivered first (applied from it).
    pub first: u64,
    pub duplicates: u64,
    /// Lag over the rolling window: samples, p50, p90 (ms).
    pub lag_samples: u64,
    pub lag_p50: Option<i64>,
    pub lag_p90: Option<i64>,
    /// Its lag over its whole life: samples, p50, p90 (ms).
    pub life_samples: u64,
    pub life_p50: Option<i64>,
    pub life_p90: Option<i64>,
    /// Why the slot's previous connection ended.
    pub last_closed: Option<String>,
}

/// The feed's numbers since it started.
#[derive(Clone, Debug, Default)]
pub struct FeedStats {
    pub conns: Vec<ConnStats>,
    pub applied: u64,
    pub duplicates: u64,
    pub held: u64,
    pub ignored: u64,
    /// Connections replaced by the merge (diverged, failed) or the lag rules.
    pub replaced: u64,
    /// Connections that ended on their own (closed, silent, lost) or failed to open.
    pub dropped: u64,
    /// Markets rebuilt from a fresh snapshot after an integrity failure.
    pub rebuilt: u64,
    /// The local clock's offset from the venue's, [lo, hi] ms, once checked.
    pub clock_offset: Option<(i64, i64)>,
    pub markets_servable: usize,
    pub markets: usize,
}

struct Slot {
    conn: ConnId,
    task: Option<tokio::task::JoinHandle<()>>,
    since: Option<Instant>,
    failures: u32,
    frames: u64,
    messages: u64,
    first: u64,
    duplicates: u64,
    lag: LagWindow,
    life: super::feed_lag::LagHistogram,
    late_since: Option<Instant>,
    /// (market, token) snapshots this connection delivered since it opened.
    snapshotted: std::collections::HashSet<(usize, usize)>,
    last_closed: Option<String>,
}

struct State {
    merges: Vec<MarketMerge>,
    slots: Vec<Slot>,
    stats: FeedStats,
    clock: Option<OffsetBounds>,
}

struct Shared {
    data: MarketData,
    state: Mutex<State>,
    markets: Vec<[String; 2]>,
    /// Token -> (market, index).
    tokens: HashMap<String, (usize, usize)>,
}

enum Event {
    Opened(usize, ConnId, Instant),
    Frame(usize, ConnId, Frame),
    Closed(usize, ConnId, String),
    Reopen(usize),
}

/// The running feed. Dropping it stops every connection.
pub struct Feed {
    shared: Arc<Shared>,
    stop: Arc<Notify>,
    core: Option<tokio::task::JoinHandle<()>>,
}

impl Feed {
    /// Starts the feed on the current tokio runtime: `markets` are pairs of
    /// outcome tokens (Up, Down). Every connection subscribes all of them.
    pub fn start(settings: FeedSettings, markets: Vec<[String; 2]>) -> Result<Self> {
        ensure!(!markets.is_empty(), "no market to subscribe");
        ensure!((1..=8).contains(&settings.connections), "1 to 8 connections");
        let mut tokens = HashMap::new();
        for (m, pair) in markets.iter().enumerate() {
            for (i, token) in pair.iter().enumerate() {
                ensure!(
                    tokens.insert(token.clone(), (m, i)).is_none(),
                    "token {token} subscribed twice"
                );
            }
        }
        let data = MarketData::default();
        let all: Vec<String> = markets.iter().flat_map(|p| p.iter().cloned()).collect();
        data.reset(&all);
        data.connected();
        let shared = Arc::new(Shared {
            data,
            state: Mutex::new(State {
                merges: markets.iter().map(|_| MarketMerge::default()).collect(),
                slots: (0..settings.connections)
                    .map(|_| Slot {
                        conn: 0,
                        task: None,
                        since: None,
                        failures: 0,
                        frames: 0,
                        messages: 0,
                        first: 0,
                        duplicates: 0,
                        lag: LagWindow::new(settings.lag_window),
                        life: Default::default(),
                        late_since: None,
                        snapshotted: Default::default(),
                        last_closed: None,
                    })
                    .collect(),
                stats: FeedStats {
                    markets: markets.len(),
                    ..Default::default()
                },
                clock: None,
            }),
            markets,
            tokens,
        });
        let stop = Arc::new(Notify::new());
        let core = tokio::spawn(run(shared.clone(), settings, stop.clone()));
        Ok(Self {
            shared,
            stop,
            core: Some(core),
        })
    }

    /// The merged books: one decision snapshot for every subscribed token.
    pub fn data(&self) -> MarketData {
        self.shared.data.clone()
    }

    /// The decision snapshot, refused while a market has no connection that
    /// can serve it (no book until a fresh snapshot) or the feed is silent.
    pub fn snapshot(&self) -> Result<MarketSnapshot> {
        {
            let state = self.shared.state.lock();
            for (m, merge) in state.merges.iter().enumerate() {
                ensure!(
                    merge.servable(),
                    "market {} has no connection in line (awaiting a fresh snapshot)",
                    self.shared.markets[m][0]
                );
            }
        }
        self.shared.data.snapshot()
    }

    pub fn stats(&self) -> FeedStats {
        let now = Instant::now();
        let mut state = self.shared.state.lock();
        let mut stats = state.stats.clone();
        stats.clock_offset = state.clock.map(|b| (b.lo, b.hi));
        stats.markets_servable = state.merges.iter().filter(|m| m.servable()).count();
        stats.conns = state
            .slots
            .iter_mut()
            .enumerate()
            .map(|(slot, s)| {
                s.lag.expire(now);
                ConnStats {
                    slot,
                    conn: s.conn,
                    since: s.since,
                    frames: s.frames,
                    messages: s.messages,
                    first: s.first,
                    duplicates: s.duplicates,
                    lag_samples: s.lag.count(),
                    lag_p50: s.lag.quantile(0.5),
                    lag_p90: s.lag.quantile(0.9),
                    life_samples: s.life.count(),
                    life_p50: s.life.quantile(0.5),
                    life_p90: s.life.quantile(0.9),
                    last_closed: s.last_closed.clone(),
                }
            })
            .collect();
        stats
    }

    /// Closes every connection and waits for the feed's task.
    pub async fn stop(mut self) {
        self.stop.notify_one();
        if let Some(core) = self.core.take() {
            let _ = core.await;
        }
        self.shared.data.disconnect();
    }
}

impl Drop for Feed {
    fn drop(&mut self) {
        self.stop.notify_one();
        if let Some(core) = self.core.take() {
            core.abort();
        }
        let mut state = self.shared.state.lock();
        for slot in &mut state.slots {
            if let Some(task) = slot.task.take() {
                task.abort();
            }
        }
    }
}

fn is_book_event(event: &str) -> bool {
    matches!(event, "book" | "price_change" | "tick_size_change")
}

/// The market a message belongs to, and its token's index for `book`.
fn market_of(shared: &Shared, msg: &Value) -> Option<(usize, Option<usize>)> {
    let asset = msg["asset_id"]
        .as_str()
        .or_else(|| msg["price_changes"][0]["asset_id"].as_str())?;
    let (m, i) = *shared.tokens.get(asset)?;
    Some((m, (msg["event_type"] == "book").then_some(i)))
}

fn timestamp_ms(msg: &Value) -> Option<i64> {
    msg.get("timestamp")
        .and_then(|v| v.as_i64().or_else(|| v.as_str()?.parse().ok()))
}

async fn run(shared: Arc<Shared>, settings: FeedSettings, stop: Arc<Notify>) {
    let (tx, mut rx) = mpsc::channel::<Event>(4_096);
    let all: Vec<String> = shared.markets.iter().flat_map(|p| p.iter().cloned()).collect();
    let mut next_conn: ConnId = 0;
    let slots = settings.connections;
    for slot in 0..slots {
        next_conn += 1;
        open(&shared, &settings, &all, &tx, slot, next_conn);
    }
    let clock_task = settings.clock_url.clone().map(|url| {
        let shared = shared.clone();
        tokio::spawn(async move {
            loop {
                if let Ok(bounds) = super::feed_lag::check_venue_clock(&url, CLOCK_ROUNDS).await {
                    shared.state.lock().clock = Some(bounds);
                }
                tokio::time::sleep(CLOCK_CHECK_EVERY).await;
            }
        })
    });
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = stop.notified() => break,
            _ = tick.tick() => {
                let now = Instant::now();
                let replace = upkeep(&shared, &settings, now);
                for (slot, why) in replace {
                    next_conn += 1;
                    replace_slot(&shared, &settings, &all, &tx, slot, next_conn, why, now);
                }
            }
            event = rx.recv() => {
                let Some(event) = event else { break };
                match event {
                    Event::Opened(slot, conn, at) => {
                        let mut state = shared.state.lock();
                        let s = &mut state.slots[slot];
                        if s.conn != conn { continue; }
                        s.since = Some(at);
                        for merge in &mut state.merges {
                            merge.subscribe(conn, at);
                        }
                    }
                    Event::Frame(slot, conn, frame) => {
                        if let Some(why) = on_frame(&shared, slot, conn, frame) {
                            next_conn += 1;
                            replace_slot(&shared, &settings, &all, &tx, slot, next_conn, why, Instant::now());
                        }
                    }
                    Event::Closed(slot, conn, why) => {
                        let now = Instant::now();
                        let delay = {
                            let mut state = shared.state.lock();
                            if state.slots[slot].conn != conn { continue; }
                            let lived = state.slots[slot].since.map(|at| now.saturating_duration_since(at));
                            state.stats.dropped += 1;
                            end_conn(&shared, &mut state, slot, conn, why, now);
                            let s = &mut state.slots[slot];
                            if lived.is_some_and(|l| l >= STABLE) {
                                s.failures = 0;
                            }
                            s.failures += 1;
                            if s.failures <= 1 {
                                Duration::ZERO
                            } else {
                                settings.reconnect_base
                                    .saturating_mul(1u32 << (s.failures - 2).min(16))
                                    .min(settings.reconnect_max)
                            }
                        };
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(delay).await;
                            let _ = tx.send(Event::Reopen(slot)).await;
                        });
                    }
                    Event::Reopen(slot) => {
                        next_conn += 1;
                        open(&shared, &settings, &all, &tx, slot, next_conn);
                    }
                }
            }
        }
    }
    if let Some(task) = clock_task {
        task.abort();
    }
    let mut state = shared.state.lock();
    for slot in &mut state.slots {
        if let Some(task) = slot.task.take() {
            task.abort();
        }
    }
}

/// Opens slot `slot`'s connection as `conn`: a task that connects, then
/// forwards every frame, then says why it ended.
fn open(
    shared: &Arc<Shared>,
    settings: &FeedSettings,
    tokens: &[String],
    tx: &mpsc::Sender<Event>,
    slot: usize,
    conn: ConnId,
) {
    let url = settings.url.clone();
    let tokens = tokens.to_vec();
    let tx = tx.clone();
    let task = tokio::spawn(async move {
        let mut connection: Connection = match connect::connect(&url, &tokens).await {
            Ok(c) => c,
            Err(error) => {
                let _ = tx.send(Event::Closed(slot, conn, format!("{error:#}"))).await;
                return;
            }
        };
        if tx.send(Event::Opened(slot, conn, connection.opened)).await.is_err() {
            return;
        }
        while let Some(frame) = connection.next().await {
            if tx.send(Event::Frame(slot, conn, frame)).await.is_err() {
                return;
            }
        }
        let why = connection.closed().await.unwrap_or(Closed::ByUs).to_string();
        let _ = tx.send(Event::Closed(slot, conn, why)).await;
    });
    let mut state = shared.state.lock();
    let s = &mut state.slots[slot];
    if let Some(old) = s.task.take() {
        old.abort();
    }
    s.conn = conn;
    s.task = Some(task);
    s.since = None;
    s.frames = 0;
    s.messages = 0;
    s.first = 0;
    s.duplicates = 0;
    s.lag = LagWindow::new(settings.lag_window);
    s.life = Default::default();
    s.late_since = None;
    s.snapshotted.clear();
}

/// The slot's connection ended (or is replaced): its lines leave every market.
fn end_conn(shared: &Arc<Shared>, state: &mut State, slot: usize, conn: ConnId, why: String, now: Instant) {
    let s = &mut state.slots[slot];
    if let Some(task) = s.task.take() {
        task.abort();
    }
    s.since = None;
    s.last_closed = Some(why);
    let mut rebuilt = Vec::new();
    for merge in &mut state.merges {
        if let Lost::Rebuild(list) = merge.drop_conn(conn, now) {
            rebuilt.push(list);
        }
    }
    for list in rebuilt {
        state.stats.rebuilt += 1;
        apply(shared, state, list);
    }
}

#[allow(clippy::too_many_arguments)]
fn replace_slot(
    shared: &Arc<Shared>,
    settings: &FeedSettings,
    tokens: &[String],
    tx: &mpsc::Sender<Event>,
    slot: usize,
    conn: ConnId,
    why: String,
    now: Instant,
) {
    {
        let mut state = shared.state.lock();
        let old = state.slots[slot].conn;
        state.stats.replaced += 1;
        end_conn(shared, &mut state, slot, old, why, now);
        state.slots[slot].failures = 0;
    }
    open(shared, settings, tokens, tx, slot, conn);
}

fn apply(shared: &Shared, state: &mut State, list: Vec<Applied>) {
    for a in list {
        shared.data.ingest_any(&a.msg);
        state.stats.applied += 1;
        if !a.snapshot {
            if let Some(s) = state.slots.iter_mut().find(|s| s.conn == a.conn) {
                s.first += 1;
            }
        }
    }
}

/// One frame of a connection. Some(reason): the connection is to be replaced.
fn on_frame(shared: &Arc<Shared>, slot: usize, conn: ConnId, frame: Frame) -> Option<String> {
    shared.data.heartbeat();
    let mut state = shared.state.lock();
    if state.slots[slot].conn != conn {
        return None;
    }
    state.slots[slot].frames += 1;
    if frame.text == "PONG" {
        return None;
    }
    let Ok(value) = serde_json::from_str::<Value>(&frame.text) else {
        return Some("it sent a frame that is not JSON".to_string());
    };
    let messages = match value {
        Value::Array(list) => list,
        other => vec![other],
    };
    let now = frame.received;
    for msg in messages {
        state.slots[slot].messages += 1;
        let Some(event) = msg["event_type"].as_str() else {
            continue;
        };
        let Some((m, token_index)) = market_of(shared, &msg) else {
            continue;
        };
        let snapshot = token_index.is_some_and(|i| state.slots[slot].snapshotted.insert((m, i)));
        let step = if let (true, Some(i)) = (snapshot, token_index) {
            state.merges[m].snapshot(conn, i, &msg, now)
        } else if is_book_event(event) {
            state.merges[m].book_event(conn, EventKey::of(&msg), &msg, now)
        } else {
            state.merges[m].other(conn, EventKey::of(&msg), &msg, now)
        };
        if !snapshot {
            if let Some(ts) = timestamp_ms(&msg) {
                let lag = frame.wall_ms as i64 - ts;
                let s = &mut state.slots[slot];
                s.lag.record(now, lag);
                s.life.add(lag);
            }
        }
        match step {
            Step::Apply(list) => apply(shared, &mut state, list),
            Step::Duplicate => {
                state.stats.duplicates += 1;
                state.slots[slot].duplicates += 1;
            }
            Step::Held => state.stats.held += 1,
            Step::Ignored => state.stats.ignored += 1,
            Step::Diverged => return Some("its book events disagree with the applied history".to_string()),
            Step::Failed => return Some("it fell too far behind to line up".to_string()),
        }
    }
    None
}

/// Merge upkeep and the lag rules: the slots to replace, with why.
fn upkeep(shared: &Arc<Shared>, settings: &FeedSettings, now: Instant) -> Vec<(usize, String)> {
    let mut state = shared.state.lock();
    let mut replace = Vec::new();
    for m in 0..state.merges.len() {
        let pair = shared.markets[m].clone();
        let same_view = |messages: &[&Value]| -> bool {
            let view = MarketData::default();
            view.reset(&pair);
            view.connected();
            for msg in messages {
                view.ingest_any(msg);
            }
            pair.iter().all(|t| view.state_of(t) == shared.data.state_of(t))
        };
        let expired = state.merges[m].tick(now, &same_view);
        for e in expired {
            match e {
                Expired::Aligned(_, list) => apply(shared, &mut state, list),
                Expired::Rebuild(_, list) => {
                    state.stats.rebuilt += 1;
                    apply(shared, &mut state, list);
                }
                Expired::Failed(conn) => {
                    if let Some(slot) = state.slots.iter().position(|s| s.conn == conn && s.since.is_some()) {
                        if !replace.iter().any(|(s, _)| *s == slot) {
                            replace.push((slot, "it could not line up with the applied history".to_string()));
                        }
                    }
                }
            }
        }
    }
    // The lag rules: against the best other connection (no clock), and against
    // the absolute limit while the clock check allows it.
    let clock_ok = state
        .clock
        .is_some_and(|b| b.consistent() && b.worst() <= settings.max_clock_offset_ms);
    let medians: Vec<Option<i64>> = state
        .slots
        .iter_mut()
        .map(|s| {
            s.lag.expire(now);
            (s.since.is_some() && s.lag.count() >= settings.min_lag_samples)
                .then(|| s.lag.quantile(0.5))
                .flatten()
        })
        .collect();
    let live = state.slots.iter().filter(|s| s.since.is_some()).count();
    for slot in 0..state.slots.len() {
        let Some(median) = medians[slot] else {
            state.slots[slot].late_since = None;
            continue;
        };
        let best_other = medians
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != slot)
            .filter_map(|(_, m)| *m)
            .min();
        let excess = best_other.is_some_and(|best| median - best > settings.lag_excess_ms);
        let absolute = clock_ok && median > settings.lag_limit_ms;
        let s = &mut state.slots[slot];
        if !(excess || absolute) {
            s.late_since = None;
            continue;
        }
        let since = *s.late_since.get_or_insert(now);
        if now.saturating_duration_since(since) >= settings.lag_sustain && live > 1 && replace.is_empty() {
            let why = match best_other {
                Some(best) if excess => format!("lag: median {median} ms, best other connection {best} ms"),
                _ => format!("lag: median {median} ms over the {} ms limit", settings.lag_limit_ms),
            };
            replace.push((slot, why));
        }
    }
    replace
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use serde_json::json;
    use tokio_tungstenite::tungstenite::Message;

    fn book(token: &str, ts: u64, bid: &str, ask: &str) -> Value {
        json!({"event_type": "book", "asset_id": token, "market": "0xm", "timestamp": ts.to_string(), "hash": "h",
            "bids": [{"price": bid, "size": "10"}], "asks": [{"price": ask, "size": "10"}]})
    }
    fn change(token: &str, ts: u64, bid: &str, ask: &str) -> Value {
        json!({"event_type": "price_change", "market": "0xm", "timestamp": ts.to_string(), "price_changes": [
            {"asset_id": token, "side": "BUY", "price": bid, "size": "7", "best_bid": bid, "best_ask": ask},
        ]})
    }

    /// A local venue: every connection gets the same stream (snapshots, then
    /// events, one of them repeated). The merged book is the stream's; the
    /// copies are dropped; the feed reports every connection.
    #[tokio::test]
    async fn two_connections_merge_into_one_book() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let now = crate::market_data::now_ms();
        let stream = vec![
            json!([
                book("1", now - 5_000, "0.40", "0.60"),
                book("2", now - 5_000, "0.40", "0.60")
            ]),
            json!([change("1", now - 4_000, "0.41", "0.60")]),
            json!([change("1", now - 3_000, "0.42", "0.60")]),
            json!([change("1", now - 3_000, "0.42", "0.60")]),
            json!([change("2", now - 2_000, "0.39", "0.60")]),
        ];
        tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let stream = stream.clone();
                tokio::spawn(async move {
                    let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
                    let _subscription = ws.next().await;
                    for frame in &stream {
                        ws.send(Message::Text(frame.to_string().into())).await.unwrap();
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    while let Some(Ok(message)) = ws.next().await {
                        if let Message::Text(text) = message {
                            if text == "PING" {
                                let _ = ws.send(Message::Text("PONG".into())).await;
                            }
                        }
                    }
                });
            }
        });
        let settings = FeedSettings {
            connections: 2,
            url,
            clock_url: None,
            ..FeedSettings::default()
        };
        let feed = Feed::start(settings, vec![["1".into(), "2".into()]]).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let stats = loop {
            let stats = feed.stats();
            if stats.applied >= 6 && stats.conns.iter().all(|c| c.messages >= 6) {
                break stats;
            }
            assert!(Instant::now() < deadline, "{stats:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let data = feed.data();
        let (bid, ask, _, _, _) = data.state_of("1").unwrap();
        assert_eq!((bid, ask), (Some(0.42), Some(0.60)));
        assert_eq!(data.state_of("2").unwrap().0, Some(0.39));
        // Two snapshots and three events applied once, the repeat applied as the
        // venue's stream has it. The second connection's six messages are held
        // while it lines up behind the first, then dropped as copies; none applied.
        assert_eq!(stats.applied, 6, "{stats:?}");
        assert_eq!(stats.duplicates + stats.held, 6, "{stats:?}");
        assert_eq!(stats.conns.iter().map(|c| c.first).sum::<u64>(), 4);
        assert!(
            stats.conns.iter().all(|c| c.since.is_some() && c.lag_samples >= 3),
            "{stats:?}"
        );
        assert_eq!(stats.markets_servable, 1);
        assert!(feed.snapshot().is_ok());
        feed.stop().await;
    }

    #[tokio::test]
    async fn a_dead_venue_keeps_reopening_and_reports_it() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        drop(listener);
        let settings = FeedSettings {
            connections: 1,
            url,
            clock_url: None,
            reconnect_base: Duration::from_millis(10),
            reconnect_max: Duration::from_millis(20),
            ..FeedSettings::default()
        };
        let feed = Feed::start(settings, vec![["1".into(), "2".into()]]).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let stats = feed.stats();
            if stats.dropped >= 3 {
                assert!(stats.conns[0].last_closed.is_some());
                assert_eq!(stats.markets_servable, 0);
                assert!(feed.snapshot().is_err());
                break;
            }
            assert!(Instant::now() < deadline, "{stats:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        feed.stop().await;
    }
}
