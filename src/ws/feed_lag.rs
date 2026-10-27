//! The market feed's lag rules: each connection's delivery lag
//! over a rolling window, the settings of the redundant connections, and the
//! clock check that an absolute lag limit needs.
//!
//! Lag is the local wall time a message arrived minus the venue timestamp it
//! carries. Measured from a European cloud host (140
//! two-minute probe runs): median 10 ms, but p90 about 62-83 ms on weekdays and
//! p99 about 370 ms, and 5 of 140 connections (3.6%) were 75-87 ms late for their
//! whole life whatever the activity. A long-lived connection can therefore sit
//! 80 ms late without knowing it. The feed measures every connection's rolling
//! median and replaces one that stays late (FeedSettings).
//!
//! Comparing two connections' lags needs no clock: the local clock's error cancels.
//! An absolute limit does, so it applies only while the venue's clock check says
//! the local clock is within `max_clock_offset_ms` of the venue's. The venue's
//! `GET /time` answers whole seconds; the check times its requests across the
//! venue's second boundaries to bound the offset to about one round trip (25 ms
//! from that host). The box's own time source is the host's NTP.
use anyhow::{bail, ensure, Context, Result};
use std::{
    collections::VecDeque,
    future::Future,
    time::{Duration, Instant},
};

/// The venue's market channel.
pub const WSS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
/// The venue's clock, in whole seconds (a public endpoint).
pub const CLOCK_URL: &str = "https://clob.polymarket.com/time";

/// Lag counts by exact millisecond value, signed: a local clock behind the venue's
/// makes every lag read low (even negative), and comparing connections must still
/// work then; nothing is capped, so no limit can be hidden.
#[derive(Clone, Debug, Default)]
pub struct LagHistogram {
    counts: std::collections::BTreeMap<i64, u32>,
    count: u64,
}

impl LagHistogram {
    pub fn add(&mut self, lag_ms: i64) {
        *self.counts.entry(lag_ms).or_insert(0) += 1;
        self.count += 1;
    }
    fn remove(&mut self, lag_ms: i64) {
        if let Some(n) = self.counts.get_mut(&lag_ms) {
            *n -= 1;
            if *n == 0 {
                self.counts.remove(&lag_ms);
            }
            self.count -= 1;
        }
    }
    pub fn count(&self) -> u64 {
        self.count
    }
    /// The lag at quantile `q` (nearest rank); None when empty.
    pub fn quantile(&self, q: f64) -> Option<i64> {
        if self.count == 0 {
            return None;
        }
        let rank = ((q.clamp(0.0, 1.0) * self.count as f64).ceil() as u64).max(1);
        let mut seen = 0;
        for (lag, n) in &self.counts {
            seen += *n as u64;
            if seen >= rank {
                return Some(*lag);
            }
        }
        self.counts.keys().next_back().copied()
    }
    pub fn clear(&mut self) {
        self.counts.clear();
        self.count = 0;
    }
}

/// One connection's lags over the last `window` of receive time.
#[derive(Clone, Debug)]
pub struct LagWindow {
    window: Duration,
    samples: VecDeque<(Instant, i64)>,
    histogram: LagHistogram,
}

impl LagWindow {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            samples: VecDeque::new(),
            histogram: LagHistogram::default(),
        }
    }
    pub fn record(&mut self, now: Instant, lag_ms: i64) {
        self.expire(now);
        self.samples.push_back((now, lag_ms));
        self.histogram.add(lag_ms);
    }
    /// Drops the samples received more than `window` before `now`.
    pub fn expire(&mut self, now: Instant) {
        while let Some(&(at, lag)) = self.samples.front() {
            if now.saturating_duration_since(at) <= self.window {
                break;
            }
            self.samples.pop_front();
            self.histogram.remove(lag);
        }
    }
    pub fn count(&self) -> u64 {
        self.histogram.count()
    }
    pub fn quantile(&self, q: f64) -> Option<i64> {
        self.histogram.quantile(q)
    }
}

/// The redundant feed's settings (environment variables, read at startup).
#[derive(Clone, Debug, PartialEq)]
pub struct FeedSettings {
    /// Parallel connections carrying every subscription (FEED_CONNECTIONS, 1-4).
    pub connections: usize,
    /// A connection whose rolling median lag stays above this (ms) is replaced,
    /// while the clock check passes (FEED_LAG_LIMIT_MS).
    pub lag_limit_ms: i64,
    /// ... or stays this much (ms) above the best other connection's median, with
    /// no clock needed (FEED_LAG_EXCESS_MS).
    pub lag_excess_ms: i64,
    /// For this long (FEED_LAG_SUSTAIN_MS).
    pub lag_sustain: Duration,
    /// The rolling window of the median and p90 (FEED_LAG_WINDOW_MS).
    pub lag_window: Duration,
    /// The lag rules need at least this many messages in the window.
    pub min_lag_samples: u64,
    /// The absolute limit applies only with the local clock proven within this many
    /// ms of the venue's (FEED_MAX_CLOCK_OFFSET_MS).
    pub max_clock_offset_ms: i64,
    /// A slot whose connection failed after the one before it also failed waits
    /// this long before it opens another, doubled with each further failure in a
    /// row up to `reconnect_max`. Not from the
    /// environment.
    pub reconnect_base: Duration,
    pub reconnect_max: Duration,
    /// The market channel (tests point it at a local server).
    pub url: String,
    /// The venue clock to check against; None skips the check (the absolute limit
    /// then stays off unless a test sets the clock).
    pub clock_url: Option<String>,
}

impl Default for FeedSettings {
    fn default() -> Self {
        Self {
            connections: 3,
            lag_limit_ms: 50,
            lag_excess_ms: 30,
            lag_sustain: Duration::from_secs(3),
            lag_window: Duration::from_secs(2),
            min_lag_samples: 20,
            max_clock_offset_ms: 50,
            reconnect_base: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(30),
            url: WSS_URL.to_string(),
            clock_url: Some(CLOCK_URL.to_string()),
        }
    }
}

fn env_int(
    lookup: &dyn Fn(&str) -> Option<String>,
    name: &str,
    range: std::ops::RangeInclusive<i64>,
) -> Result<Option<i64>> {
    let Some(raw) = lookup(name) else {
        return Ok(None);
    };
    let value: i64 = raw
        .trim()
        .parse()
        .ok()
        .filter(|v| range.contains(v))
        .with_context(|| format!("Invalid {name}={raw}; use {} to {}", range.start(), range.end()))?;
    Ok(Some(value))
}

impl FeedSettings {
    /// The defaults with FEED_* overrides; an invalid value is an error, never a fallback.
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(&|name| std::env::var(name).ok())
    }

    pub fn from_lookup(lookup: &dyn Fn(&str) -> Option<String>) -> Result<Self> {
        let mut s = Self::default();
        if let Some(n) = env_int(lookup, "FEED_CONNECTIONS", 1..=4)? {
            s.connections = n as usize;
        }
        if let Some(ms) = env_int(lookup, "FEED_LAG_LIMIT_MS", 10..=2_000)? {
            s.lag_limit_ms = ms;
        }
        if let Some(ms) = env_int(lookup, "FEED_LAG_EXCESS_MS", 5..=2_000)? {
            s.lag_excess_ms = ms;
        }
        if let Some(ms) = env_int(lookup, "FEED_LAG_SUSTAIN_MS", 500..=60_000)? {
            s.lag_sustain = Duration::from_millis(ms as u64);
        }
        if let Some(ms) = env_int(lookup, "FEED_LAG_WINDOW_MS", 500..=10_000)? {
            s.lag_window = Duration::from_millis(ms as u64);
        }
        if let Some(ms) = env_int(lookup, "FEED_MAX_CLOCK_OFFSET_MS", 1..=1_000)? {
            s.max_clock_offset_ms = ms;
        }
        Ok(s)
    }

    pub fn describe(&self) -> String {
        format!(
            "{} market-data connection(s); one is replaced when its median lag over {} ms stays above {} ms \
             (with the clock checked within {} ms of the venue's) or {} ms above the best other connection's for {} ms",
            self.connections,
            self.lag_window.as_millis(),
            self.lag_limit_ms,
            self.max_clock_offset_ms,
            self.lag_excess_ms,
            self.lag_sustain.as_millis(),
        )
    }
}

/// What the venue's clock says about ours: server minus local time, in ms, lies in
/// [lo, hi].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OffsetBounds {
    pub lo: i64,
    pub hi: i64,
}

impl OffsetBounds {
    pub fn unbounded() -> Self {
        Self {
            lo: i64::MIN / 4,
            hi: i64::MAX / 4,
        }
    }
    /// One answer: the venue read its clock as second `server_s` while the request
    /// was between local wall ms `sent` and `received`.
    pub fn narrow(&mut self, server_s: i64, sent: i64, received: i64) {
        let server_ms = server_s.saturating_mul(1_000);
        self.lo = self.lo.max(server_ms.saturating_sub(received));
        self.hi = self.hi.min(server_ms.saturating_add(999).saturating_sub(sent));
    }
    pub fn consistent(&self) -> bool {
        self.lo <= self.hi
    }
    /// The largest offset the bounds allow, either way.
    pub fn worst(&self) -> i64 {
        self.lo.abs().max(self.hi.abs())
    }
    fn mid(&self) -> i64 {
        self.lo / 2 + self.hi / 2
    }
}

/// Bounds the local clock's offset from the venue's with `rounds` requests timed at
/// the venue's second boundaries. `get` answers the venue's whole second;
/// `wall_ms` is the local wall clock and `sleep_until` waits for a local wall time.
pub async fn measure_offset<G, GF, W, S, SF>(get: G, wall_ms: W, sleep_until: S, rounds: usize) -> Result<OffsetBounds>
where
    G: Fn() -> GF,
    GF: Future<Output = Result<i64>>,
    W: Fn() -> i64,
    S: Fn(i64) -> SF,
    SF: Future<Output = ()>,
{
    let mut bounds = OffsetBounds::unbounded();
    let mut rtt = i64::MAX;
    for round in 0..=rounds {
        if round > 0 {
            // Aim the request at the venue's next second boundary at least 200 ms
            // ahead, as the current midpoint estimates it: its answer tells on
            // which side of the boundary the venue read its clock.
            let mid = bounds.mid();
            let boundary = (wall_ms() + mid + 200).div_euclid(1_000) * 1_000 + 1_000;
            sleep_until(boundary - mid - rtt / 2).await;
        }
        let sent = wall_ms();
        let server_s = get().await?;
        let received = wall_ms();
        rtt = rtt.min(received - sent);
        bounds.narrow(server_s, sent, received);
        ensure!(
            bounds.consistent(),
            "the venue's clock answers are inconsistent (the local clock moved during the check?)"
        );
    }
    Ok(bounds)
}

/// The venue's second from a `GET /time` body (a bare integer).
pub fn parse_server_seconds(body: &str) -> Result<i64> {
    let text = body.trim().trim_matches('"');
    match text.parse::<i64>() {
        // 2001 to 5138: anything else is not the venue's clock.
        Ok(s) if (1_000_000_000..=100_000_000_000).contains(&s) => Ok(s),
        _ => bail!(
            "unexpected /time answer {:?}",
            body.chars().take(40).collect::<String>()
        ),
    }
}

/// Measures the offset against `url` over HTTP, a request per round.
#[cfg(feature = "net")]
pub async fn check_venue_clock(url: &str, rounds: usize) -> Result<OffsetBounds> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    // A first request opens the connection, so the timed ones carry no handshake.
    let _ = http.get(url).send().await;
    let get = || {
        let http = http.clone();
        async move {
            let response = http.get(url).send().await?.error_for_status()?;
            parse_server_seconds(&response.text().await?)
        }
    };
    let wall = || crate::market_data::now_ms() as i64;
    let sleep_until = |at: i64| async move {
        let wait = at - crate::market_data::now_ms() as i64;
        if wait > 0 {
            tokio::time::sleep(Duration::from_millis(wait as u64)).await;
        }
    };
    measure_offset(get, wall, sleep_until, rounds).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::sync::Arc;

    #[test]
    fn rolling_quantiles_forget_old_lags() {
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let mut w = LagWindow::new(Duration::from_millis(1_000));
        for (i, lag) in [10, 12, 9, 80, 11].iter().enumerate() {
            w.record(at(i as u64 * 100), *lag);
        }
        assert_eq!((w.count(), w.quantile(0.5), w.quantile(0.9)), (5, Some(11), Some(80)));
        // Exact values, signed and uncapped (a clock behind the venue's, a very late
        // connection).
        w.record(at(500), -3);
        w.record(at(500), 9_999);
        assert_eq!((w.quantile(0.0), w.quantile(1.0)), (Some(-3), Some(9_999)));
        let mut low = LagWindow::new(Duration::from_secs(1));
        for lag in [-90, -91, -89] {
            low.record(at(600), lag);
        }
        assert_eq!(low.quantile(0.5), Some(-90));
        low.record(at(600), -5_000);
        assert_eq!(low.quantile(0.0), Some(-5_000));
        // At 1.35 s the four received more than 1 s before are gone.
        w.expire(at(1_350));
        assert_eq!((w.count(), w.quantile(0.5)), (3, Some(11)));
        w.expire(at(10_000));
        assert_eq!((w.count(), w.quantile(0.5)), (0, None));
    }

    #[test]
    fn settings_come_from_the_environment_strictly() {
        let defaults = FeedSettings::default();
        assert_eq!(
            (defaults.connections, defaults.lag_limit_ms, defaults.lag_excess_ms),
            (3, 50, 30)
        );
        assert_eq!(defaults.lag_sustain, Duration::from_secs(3));
        assert_eq!(FeedSettings::from_lookup(&|_| None).unwrap(), defaults);
        let set = |pairs: &'static [(&'static str, &'static str)]| {
            move |name: &str| pairs.iter().find(|(n, _)| *n == name).map(|(_, v)| v.to_string())
        };
        let s = FeedSettings::from_lookup(&set(&[
            ("FEED_CONNECTIONS", "3"),
            ("FEED_LAG_LIMIT_MS", " 60 "),
            ("FEED_LAG_SUSTAIN_MS", "5000"),
        ]))
        .unwrap();
        assert_eq!(
            (s.connections, s.lag_limit_ms, s.lag_sustain),
            (3, 60, Duration::from_secs(5))
        );
        for bad in [
            &[("FEED_CONNECTIONS", "0")][..],
            &[("FEED_CONNECTIONS", "two")],
            &[("FEED_LAG_LIMIT_MS", "5")],
            &[("FEED_MAX_CLOCK_OFFSET_MS", "-1")],
        ] {
            let error = FeedSettings::from_lookup(&set(bad)).unwrap_err().to_string();
            assert!(error.contains(bad[0].0), "{error}");
        }
    }

    /// A venue whose clock is `offset` ms ahead of ours, answering whole seconds
    /// after `up` ms on the way there and `down` ms back.
    fn simulated(offset: i64, up: i64, down: i64) -> (OffsetBounds, Vec<i64>) {
        let now = Arc::new(Mutex::new(1_791_700_000_123i64));
        let asks = Arc::new(Mutex::new(Vec::new()));
        let get = {
            let (now, asks) = (now.clone(), asks.clone());
            move || {
                let (now, asks) = (now.clone(), asks.clone());
                async move {
                    let mut t = now.lock();
                    *t += up;
                    let server = *t + offset;
                    asks.lock().push(server);
                    *t += down;
                    Ok(server.div_euclid(1_000))
                }
            }
        };
        let wall = {
            let now = now.clone();
            move || *now.lock()
        };
        let sleep_until = {
            let now = now.clone();
            move |at: i64| {
                let now = now.clone();
                async move {
                    let mut t = now.lock();
                    *t = (*t).max(at);
                }
            }
        };
        let bounds = futures_util::FutureExt::now_or_never(measure_offset(get, wall, sleep_until, 12))
            .unwrap()
            .unwrap();
        let asks = asks.lock().clone();
        (bounds, asks)
    }

    #[test]
    fn the_clock_check_bounds_the_offset_to_about_one_round_trip() {
        for (offset, up, down) in [(0, 12, 12), (7, 12, 12), (-250, 30, 10), (1_500, 5, 40), (-3, 1, 1)] {
            let (b, _) = simulated(offset, up, down);
            assert!(b.lo <= offset && offset <= b.hi, "{offset}: {b:?}");
            assert!(b.hi - b.lo <= up + down + 2, "{offset} {up} {down}: {b:?}");
        }
        // A synced clock 25 ms from the venue passes a 30 ms limit.
        let (b, _) = simulated(0, 12, 13);
        assert!(b.worst() <= 30, "{b:?}");
        assert_eq!(parse_server_seconds("1791762377\n").unwrap(), 1_791_762_377);
        assert!(parse_server_seconds("<html>").is_err());
        // A long answer that is not ASCII is cut by characters, not bytes.
        assert!(parse_server_seconds(&"é".repeat(40)).is_err());
        // Out of range: refused, and the bounds never overflow.
        assert!(parse_server_seconds("9223372036854775807").is_err());
        let mut b = OffsetBounds::unbounded();
        b.narrow(i64::MAX, 0, 1);
        b.narrow(i64::MIN, 0, 1);
        assert!(!b.consistent());
    }
}
