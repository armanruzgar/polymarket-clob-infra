//! The venue's private stream (`/ws/user`): the fills and order updates of one
//! API key, as [`Fill`]s on a channel. The stream's health says only that the
//! transport is alive; what a fill is worth is settled by its receipt
//! ([`crate::receipts`]), never by this stream.
//!
//! The task logs in with the API key, secret and passphrase, sends `PING`
//! every 10 s, and reconnects 2 s after any end. A login the venue refuses
//! (an error frame about authentication before any `PONG` or fill) is noted
//! as such ([`StreamDrop::refused`]), so a caller can stop instead of
//! retrying bad credentials forever. Nothing of the login is ever logged.
use crate::execution::{Fill, MakerOrder};
use crate::polymarket::rest::ApiCreds;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinHandle, time::timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// The venue's private channel.
pub const USER_WSS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";

type Lifetime = Arc<parking_lot::Mutex<bool>>;

/// The private stream's task.
pub struct FillStream {
    task: Option<JoinHandle<()>>,
    active: Option<Lifetime>,
    healthy: Arc<AtomicBool>,
    dropped: Dropped,
}

impl Default for FillStream {
    fn default() -> Self {
        Self::new()
    }
}

/// Why the stream last dropped or failed to start, since it was last healthy.
pub type Dropped = Arc<parking_lot::Mutex<Option<StreamDrop>>>;

#[derive(Debug, Clone, PartialEq)]
pub struct StreamDrop {
    pub why: String,
    /// The venue refused the login: an error frame about authentication
    /// (`unauthorized`, `authentication_failed`, or a text naming auth or the
    /// API key) before the connection answered a PONG or a fill. Not a
    /// timeout, a lost connection, a close, silence, any handshake refusal
    /// (the credentials travel after it: a 401/403 there is the edge's, a
    /// WAF or a geo rule) or another error frame (the venue's trouble).
    pub refused: bool,
}

/// A login refused later than this after it was sent is no refused login
/// once a PONG came (StreamDrop::refused).
const LOGIN_ANSWER: Duration = Duration::from_secs(2);
/// At most this much of what the venue answered is kept.
const DROPPED_TEXT: usize = 160;

fn note_drop(dropped: &Dropped, why: String, refused: bool) {
    *dropped.lock() = Some(StreamDrop {
        why: why.chars().take(DROPPED_TEXT).collect(),
        refused,
    });
}

fn set_health(active: &Lifetime, healthy: &AtomicBool, value: bool) {
    // Stop and reconnect updates serialize so an old task cannot resurrect
    // health after stop, or invalidate a subsequently started stream.
    let active = active.lock();
    if *active {
        healthy.store(value, Ordering::Release);
    }
}

struct HealthGuard {
    active: Lifetime,
    healthy: Arc<AtomicBool>,
}

impl Drop for HealthGuard {
    fn drop(&mut self) {
        set_health(&self.active, &self.healthy, false);
    }
}

impl FillStream {
    pub fn new() -> Self {
        Self {
            task: None,
            active: None,
            healthy: Arc::new(AtomicBool::new(false)),
            dropped: Dropped::default(),
        }
    }
    /// True while the connection answers (a PONG or a fill came, and nothing
    /// ended it since).
    pub fn health(&self) -> Arc<AtomicBool> {
        self.healthy.clone()
    }
    /// Why the stream last dropped, until it is healthy again.
    pub fn dropped(&self) -> Dropped {
        self.dropped.clone()
    }
    /// Starts (or restarts) the stream on `url` for `creds`; fills go to
    /// `fill_tx`. The task ends when the receiver is dropped.
    pub fn start(&mut self, url: &str, creds: &ApiCreds, fill_tx: mpsc::UnboundedSender<Fill>) {
        self.stop();
        let active = Arc::new(parking_lot::Mutex::new(true));
        self.active = Some(active.clone());
        let healthy = self.healthy.clone();
        let dropped = self.dropped.clone();
        let url = url.to_string();
        let auth = json!({"auth": {"apiKey": creds.key(), "secret": creds.secret(), "passphrase": creds.passphrase()}, "type": "user", "markets": []}).to_string();
        self.task = Some(tokio::spawn(async move {
            let _guard = HealthGuard {
                active: active.clone(),
                healthy: healthy.clone(),
            };
            loop {
                set_health(&active, &healthy, false);
                if fill_tx.is_closed() {
                    return;
                }
                let mut why: String;
                let mut refused = false;
                match timeout(Duration::from_secs(10), connect_async(&url)).await {
                    Err(_) => why = "the connection timed out".to_string(),
                    Ok(Err(tokio_tungstenite::tungstenite::Error::Http(response))) => {
                        why = format!("the handshake was refused (HTTP {})", response.status().as_u16());
                    }
                    Ok(Err(error)) => why = format!("the connection failed ({error})"),
                    Ok(Ok((stream, _))) => {
                        let (mut write, mut read) = stream.split();
                        if matches!(
                            timeout(Duration::from_secs(5), write.send(Message::Text(auth.clone().into()))).await,
                            Ok(Ok(()))
                        ) {
                            why = "it went silent".to_string();
                            // A fill, or a PONG LOGIN_ANSWER after the login, came on this
                            // connection: the login was taken (the first PING goes right
                            // behind the login, and its PONG may overtake a refusal).
                            let mut answered = false;
                            let logged_in = tokio::time::Instant::now();
                            let mut ping = tokio::time::interval(Duration::from_secs(10));
                            let silence = tokio::time::sleep(Duration::from_secs(25));
                            tokio::pin!(silence);
                            'connection: loop {
                                tokio::select! {
                                    _ = fill_tx.closed() => return,
                                    _ = &mut silence => break,
                                    _ = ping.tick() => {
                                        if !matches!(timeout(Duration::from_secs(5), write.send(Message::Text("PING".into()))).await, Ok(Ok(()))) {
                                            why = "a ping could not be sent".to_string();
                                            break;
                                        }
                                    }
                                    message = read.next() => match message {
                                        Some(Ok(Message::Text(text))) => {
                                            silence.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(25));
                                            if text == "PONG" {
                                                set_health(&active, &healthy, true);
                                                dropped.lock().take();
                                                answered |= logged_in.elapsed() >= LOGIN_ANSWER;
                                                continue;
                                            }
                                            let Ok(value) = serde_json::from_str::<Value>(&text) else {
                                                why = "it sent a frame that is not JSON".to_string();
                                                break;
                                            };
                                            let rows = if let Some(array) = value.as_array() { array.clone() } else { vec![value] };
                                            for row in rows {
                                                let kind = row["event_type"].as_str().or_else(|| row["type"].as_str()).unwrap_or("");
                                                if row.get("error").is_some() || matches!(kind, "error" | "unauthorized" | "authentication_failed") {
                                                    // What it says, not the frame: nothing of ours is echoed into a log.
                                                    let said = row.get("error").or_else(|| row.get("message")).map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string)).unwrap_or_default();
                                                    why = format!("the venue refused it ({}: {})", if kind.is_empty() { "error" } else { kind }, said);
                                                    let about_auth = matches!(kind, "unauthorized" | "authentication_failed")
                                                        || ["unauthorized", "unauthenticated", "authentication", "api key", "apikey", "api_key", "invalid key", "credentials"]
                                                            .iter()
                                                            .any(|w| said.to_lowercase().contains(w));
                                                    refused = !answered && about_auth;
                                                    break 'connection;
                                                }
                                                if let Some(fill) = parse_fill(&row) {
                                                    if fill_tx.send(fill).is_err() {
                                                        return;
                                                    }
                                                    set_health(&active, &healthy, true);
                                                    dropped.lock().take();
                                                    answered = true;
                                                } else if kind.eq_ignore_ascii_case("trade") {
                                                    why = "it sent a trade it could not decode".to_string();
                                                    break 'connection;
                                                }
                                            }
                                        }
                                        Some(Ok(Message::Ping(bytes))) => {
                                            if !matches!(timeout(Duration::from_secs(5), write.send(Message::Pong(bytes))).await, Ok(Ok(()))) {
                                                why = "a pong could not be sent".to_string();
                                                break;
                                            }
                                        }
                                        Some(Ok(Message::Close(frame))) => {
                                            why = frame.map_or("the venue closed it".to_string(), |f| format!("the venue closed it ({} {})", u16::from(f.code), f.reason));
                                            break;
                                        }
                                        Some(Err(_)) | None => {
                                            why = "the connection was lost".to_string();
                                            break;
                                        }
                                        _ => {}
                                    },
                                }
                            }
                        } else {
                            why = "the login could not be sent".to_string();
                        }
                    }
                }
                set_health(&active, &healthy, false);
                note_drop(&dropped, why, refused);
                tokio::select! { _ = tokio::time::sleep(Duration::from_secs(2)) => {}, _ = fill_tx.closed() => return }
            }
        }));
    }
    pub fn stop(&mut self) {
        if let Some(active) = self.active.take() {
            let mut active = active.lock();
            *active = false;
            self.healthy.store(false, Ordering::Release);
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
        self.healthy.store(false, Ordering::Release);
    }
}

impl Drop for FillStream {
    fn drop(&mut self) {
        self.stop();
    }
}

fn number(value: Option<&Value>) -> Option<f64> {
    value.and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
}

/// A private-stream row as a [`Fill`]: a trade (MATCHED, MINED, CONFIRMED,
/// RETRYING, FAILED) or an order update; None for anything else.
pub fn parse_fill(data: &Value) -> Option<Fill> {
    let event_type = data
        .get("event_type")
        .or_else(|| data.get("type"))
        .and_then(|v| v.as_str())?;
    if event_type == "subscribed" || event_type == "authenticated" {
        return None;
    }
    let status = data
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim_start_matches("TRADE_STATUS_")
        .trim_start_matches("ORDER_STATUS_");
    let is_trade_event = event_type.eq_ignore_ascii_case("trade");
    let is_order_event = event_type.eq_ignore_ascii_case("order");
    // REST can report a trade match before broadcasting. It stays provisional;
    // neither this phase nor MATCHED can pass the receipt confirmation gate.
    let status = if is_trade_event && status == "MATCHED_NOT_BROADCASTED" {
        "MATCHED"
    } else {
        status
    };
    // Both trade and order updates: a fill may only land as an order event.
    if !is_trade_event && !is_order_event {
        return None;
    }
    if is_trade_event && !["MATCHED", "MINED", "CONFIRMED", "RETRYING", "FAILED"].contains(&status) {
        return None;
    }
    let maker_orders: Vec<MakerOrder> = data
        .get("maker_orders")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|mo| {
                    Some(MakerOrder {
                        order_id: mo.get("order_id")?.as_str()?.to_string(),
                        matched_amount: number(mo.get("matched_amount").or_else(|| mo.get("size_matched")))
                            .unwrap_or(0.0),
                        price: number(mo.get("price")).unwrap_or(0.0),
                        asset_id: mo.get("asset_id").and_then(|v| v.as_str()).map(str::to_string),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let size = number(if is_order_event {
        data.get("size_matched")
    } else {
        data.get("size")
    })
    .unwrap_or(0.0);
    let price = number(data.get("price").or_else(|| data.get("avg_price"))).unwrap_or(0.0);
    // FAILED may be a status-only notification. Never drop a settlement
    // failure just because its economic fields were omitted.
    if status != "FAILED" && (!size.is_finite() || !price.is_finite() || size <= 0.0 || price <= 0.0 || price >= 1.0) {
        return None;
    }
    let timestamp = data
        .get("match_time")
        .or_else(|| data.get("timestamp"))
        .and_then(|v| v.as_u64().or_else(|| v.as_str()?.parse().ok()))
        .unwrap_or(0);
    Some(Fill {
        transaction_hash: data
            .get("transaction_hash")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_lowercase),
        matched_at_ms: if timestamp < 100_000_000_000 {
            timestamp.saturating_mul(1000)
        } else {
            timestamp
        },
        cumulative: is_order_event,
        id: data
            .get("id")
            .or_else(|| data.get("trade_id"))
            .or_else(|| data.get("order_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        taker_order_id: data
            .get("taker_order_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        maker_orders,
        asset_id: data
            .get("asset_id")
            .or_else(|| data.get("token_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        side: data.get("side").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        size,
        price,
        status: status.to_string(),
        market: data
            .get("market")
            .and_then(Value::as_str)
            .filter(|m| m.len() == 66 && m.starts_with("0x") && hex::decode(&m[2..]).is_ok())
            .map(str::to_lowercase),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_order_update_uses_cumulative_matched_not_original_size() {
        let fill = parse_fill(&json!({"event_type":"order","type":"UPDATE","status":"LIVE","id":"order","asset_id":"123","size":"10","size_matched":"2","price":"0.4"})).unwrap();
        assert!(fill.cumulative);
        assert_eq!(fill.size, 2.0);
    }

    #[test]
    fn retrying_event_is_retained_for_durable_reconciliation() {
        let tx = format!("0x{}", "ab".repeat(32));
        let fill = parse_fill(
            &json!({"event_type":"trade","status":"RETRYING","id":"retry", "taker_order_id":"owned",
            "asset_id":"123","side":"BUY","size":"2","price":"0.4","match_time":"1700000000","transaction_hash":tx}),
        )
        .unwrap();
        assert_eq!(fill.status, "RETRYING");
        assert_eq!(fill.transaction_hash, Some(tx));
        assert_eq!(fill.matched_at_ms, 1700000000000);
    }

    #[test]
    fn trade_remains_delta_and_preserves_all_maker_portions() {
        let fill = parse_fill(&json!({"event_type":"trade","status":"CONFIRMED","id":"trade","asset_id":"123","size":"4","price":"0.4","maker_orders":[{"order_id":"a","matched_amount":"2","price":"0.4"},{"order_id":"b","matched_amount":"2","price":"0.4"}]})).unwrap();
        assert!(!fill.cumulative);
        assert_eq!(fill.maker_orders.len(), 2);
    }

    #[test]
    fn failure_without_amounts_is_not_lost_and_transaction_identity_is_preserved() {
        let failed = parse_fill(&json!({"event_type":"trade","id":"failed","status":"FAILED"})).unwrap();
        assert_eq!(failed.status, "FAILED");
        let tx = format!("0x{}", "ab".repeat(32));
        let fill = parse_fill(&json!({"event_type":"trade","id":"ok","status":"CONFIRMED","size":"2","price":"0.4", "match_time":"1700000000","transaction_hash":tx})).unwrap();
        assert_eq!(fill.transaction_hash.as_ref(), Some(&tx));
        assert_eq!(fill.matched_at_ms, 1700000000000);
    }

    fn creds() -> ApiCreds {
        ApiCreds::new("test-only", "AA==", "test-only")
    }

    #[tokio::test]
    async fn array_delivery_and_stop_or_drop_own_the_entire_connection() {
        for drop_stream in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("ws://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                assert!(matches!(ws.next().await, Some(Ok(Message::Text(_)))));
                ws.send(Message::Text("PONG".into())).await.unwrap();
                ws.send(Message::Text(json!([
                    {"event_type":"trade","id":"a","status":"CONFIRMED","size":"2","price":"0.4","match_time":"1700000000"},
                    {"event_type":"trade","id":"b","status":"FAILED"}]).to_string().into())).await.unwrap();
                loop {
                    match ws.next().await {
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                        _ => {}
                    }
                }
            });
            let (tx, mut rx) = mpsc::unbounded_channel();
            let mut stream = FillStream::new();
            let health = stream.health();
            stream.start(&url, &creds(), tx);
            assert_eq!(
                timeout(Duration::from_secs(1), rx.recv()).await.unwrap().unwrap().id,
                "a"
            );
            assert_eq!(
                timeout(Duration::from_secs(1), rx.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                "FAILED"
            );
            assert!(health.load(Ordering::Acquire));
            if drop_stream {
                drop(stream);
            } else {
                stream.stop();
            }
            assert!(!health.load(Ordering::Acquire));
            timeout(Duration::from_secs(1), server).await.unwrap().unwrap();
            assert!(!health.load(Ordering::Acquire));
        }
    }

    /// No handshake refusal is a refused login (the credentials travel
    /// after it; a 401 or 403 there is the edge's): only an error frame about
    /// authentication answering the login is, before any PONG.
    #[tokio::test]
    async fn only_an_auth_error_answering_the_login_is_a_refused_login() {
        let cases: [(Option<u16>, &str, bool); 7] = [
            (Some(401), "", false),
            (Some(403), "", false),
            (None, r#"{"type":"authentication_failed","error":"x"}"#, true),
            (None, r#"{"type":"error","error":"Invalid API key"}"#, true),
            (None, r#"{"type":"error","error":"internal error"}"#, false),
            // A PONG that overtakes the refusal does not hide it; one later
            // than LOGIN_ANSWER means the login was taken.
            (None, "PONG\n", true),
            (None, "PONG\nlater", false),
        ];
        for (status, frame, refused) in cases {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("ws://{}", listener.local_addr().unwrap());
            let frame = frame.to_string();
            // Every connection alike, so a reconnect notes the same drop.
            let server = tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let frame = frame.clone();
                    tokio::spawn(async move {
                        #[allow(clippy::result_large_err)]
                        let refuse = move |_: &tokio_tungstenite::tungstenite::handshake::server::Request,
                                           response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                            match status {
                                Some(status) => Err(tokio_tungstenite::tungstenite::http::Response::builder().status(status).body(None).unwrap()),
                                None => Ok(response),
                            }
                        };
                        let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(stream, refuse).await else {
                            return;
                        };
                        if let Some(Ok(Message::Text(_))) = ws.next().await {
                            // A PONG first (late or not), then an auth error.
                            if frame.ends_with("later") {
                                tokio::time::sleep(LOGIN_ANSWER + Duration::from_millis(500)).await;
                            }
                            for line in frame
                                .split('\n')
                                .filter(|line| *line == "PONG" || line.starts_with('{'))
                            {
                                let _ = ws.send(Message::Text(line.to_string().into())).await;
                            }
                            if frame.starts_with("PONG") {
                                let _ = ws
                                    .send(Message::Text(r#"{"type":"unauthorized","error":"x"}"#.into()))
                                    .await;
                            }
                        }
                        while ws.next().await.is_some() {}
                    });
                }
            });
            let (tx, _rx) = mpsc::unbounded_channel();
            let mut stream = FillStream::new();
            let dropped = stream.dropped();
            stream.start(&url, &creds(), tx);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            let drop = loop {
                if let Some(drop) = dropped.lock().clone() {
                    break drop;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "no drop noted ({status:?} {refused:?})"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            };
            stream.stop();
            server.abort();
            assert_eq!(drop.refused, refused, "{status:?}: {drop:?}");
            if let Some(status) = status {
                assert!(drop.why.contains(&format!("HTTP {status}")), "{drop:?}");
            }
        }
    }

    #[tokio::test]
    async fn auth_error_cannot_be_overridden_by_a_following_pong() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            ws.next().await;
            ws.send(Message::Text(
                json!([{"type":"authentication_failed","error":"fixture"},{"event_type":"authenticated"}])
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            let _ = ws.send(Message::Text("PONG".into())).await;
            loop {
                match ws.next().await {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {}
                }
            }
        });
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut stream = FillStream::new();
        let health = stream.health();
        stream.start(&url, &creds(), tx);
        timeout(Duration::from_secs(1), server).await.unwrap().unwrap();
        assert!(!health.load(Ordering::Acquire));
        stream.stop();
    }
}
