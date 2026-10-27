//! One websocket connection to the venue's market channel: connect, subscribe,
//! hand every text frame on with the instant it arrived, answer the venue's
//! pings and send `PING` every [`PING_INTERVAL`]. It decides nothing about
//! the frames: the merge ([`super::feed_merge`]) and the lag rules
//! ([`super::feed_lag`]) do, and [`super::feed`] runs several of these.
//!
//! The subscription is the documented one: `{"assets_ids": [...], "type":
//! "market"}` on connect; `{"assets_ids": [...], "operation": "subscribe" |
//! "unsubscribe"}` later. The venue answers a `book` snapshot per token
//! subscribed, then events; `PONG` answers each `PING`.
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

/// The venue's market channel.
pub const WSS_URL: &str = super::feed_lag::WSS_URL;
/// How often `PING` is sent: the venue closes a connection silent for long.
pub const PING_INTERVAL: Duration = Duration::from_secs(2);
/// The handshake must complete within this.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// No frame at all for this long (not even a `PONG`) closes the connection.
pub const SILENCE: Duration = Duration::from_secs(15);
/// Frames not yet read by the caller: beyond this the reader waits (back
/// pressure, never a dropped frame).
const QUEUE: usize = 65_536;

/// A text frame as the connection received it.
#[derive(Clone, Debug)]
pub struct Frame {
    /// When the frame was read from the socket.
    pub received: Instant,
    /// The local wall clock then, in ms since the epoch (lag arithmetic).
    pub wall_ms: u64,
    /// The frame's text: a JSON message or array of messages, or `PONG`.
    pub text: String,
}

/// Why a connection ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Closed {
    /// The venue closed it (its code and reason, when given).
    ByVenue(Option<(u16, String)>),
    /// Nothing arrived for [`SILENCE`].
    Silent,
    /// The socket failed.
    Lost(String),
    /// A `PING` or a subscription could not be sent.
    SendFailed,
    /// [`Connection::close`] was called, or the connection was dropped.
    ByUs,
}

impl std::fmt::Display for Closed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ByVenue(Some((code, reason))) if !reason.is_empty() => {
                write!(f, "the venue closed it ({code} {reason})")
            }
            Self::ByVenue(Some((code, _))) => write!(f, "the venue closed it ({code})"),
            Self::ByVenue(None) => write!(f, "the venue closed it"),
            Self::Silent => write!(f, "it went silent"),
            Self::Lost(why) => write!(f, "the connection was lost ({why})"),
            Self::SendFailed => write!(f, "a frame could not be sent"),
            Self::ByUs => write!(f, "closed"),
        }
    }
}

/// A live connection. Dropping it closes the socket.
pub struct Connection {
    frames: mpsc::Receiver<Frame>,
    outbox: Option<mpsc::UnboundedSender<String>>,
    task: tokio::task::JoinHandle<Closed>,
    /// When the handshake completed.
    pub opened: Instant,
}

/// Opens a connection to `url` subscribed to `tokens`. An error is the
/// handshake's: a timeout, a refusal (HTTP status), or a transport failure.
pub async fn connect(url: &str, tokens: &[String]) -> Result<Connection> {
    let stream = match timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(url)).await {
        Err(_) => anyhow::bail!("the connection timed out after {} s", CONNECT_TIMEOUT.as_secs()),
        Ok(Err(tokio_tungstenite::tungstenite::Error::Http(response))) => {
            anyhow::bail!("the handshake was refused (HTTP {})", response.status().as_u16())
        }
        Ok(Err(error)) => return Err(error).context("the connection failed"),
        Ok(Ok((stream, _))) => stream,
    };
    let opened = Instant::now();
    let (mut write, mut read) = stream.split();
    let subscribe = json!({"assets_ids": tokens, "type": "market"}).to_string();
    timeout(Duration::from_secs(5), write.send(Message::Text(subscribe.into())))
        .await
        .context("the subscription timed out")?
        .context("the subscription could not be sent")?;
    let (frame_tx, frames) = mpsc::channel(QUEUE);
    let (outbox, mut outbox_rx) = mpsc::unbounded_channel::<String>();
    let task = tokio::spawn(async move {
        let mut ping = tokio::time::interval(PING_INTERVAL);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let silence = tokio::time::sleep(SILENCE);
        tokio::pin!(silence);
        loop {
            tokio::select! {
                _ = &mut silence => return Closed::Silent,
                _ = ping.tick() => {
                    if !send(&mut write, "PING".to_string()).await {
                        return Closed::SendFailed;
                    }
                }
                text = outbox_rx.recv() => match text {
                    Some(text) => {
                        if !send(&mut write, text).await {
                            return Closed::SendFailed;
                        }
                    }
                    None => {
                        let _ = timeout(Duration::from_secs(1), write.send(Message::Close(None))).await;
                        return Closed::ByUs;
                    }
                },
                message = read.next() => {
                    let received = Instant::now();
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            silence.as_mut().reset(tokio::time::Instant::now() + SILENCE);
                            let frame = Frame { received, wall_ms: crate::market_data::now_ms(), text: text.to_string() };
                            if frame_tx.send(frame).await.is_err() {
                                return Closed::ByUs;
                            }
                        }
                        Some(Ok(Message::Ping(bytes))) => {
                            silence.as_mut().reset(tokio::time::Instant::now() + SILENCE);
                            if !matches!(timeout(Duration::from_secs(5), write.send(Message::Pong(bytes))).await, Ok(Ok(()))) {
                                return Closed::SendFailed;
                            }
                        }
                        Some(Ok(Message::Close(frame))) => {
                            return Closed::ByVenue(frame.map(|f| (u16::from(f.code), f.reason.to_string())));
                        }
                        Some(Ok(_)) => {}
                        Some(Err(error)) => return Closed::Lost(short(&error)),
                        None => return Closed::Lost("end of stream".into()),
                    }
                }
            }
        }
    });
    Ok(Connection {
        frames,
        outbox: Some(outbox),
        task,
        opened,
    })
}

async fn send<S>(write: &mut S, text: String) -> bool
where
    S: SinkExt<Message> + Unpin,
{
    matches!(
        timeout(Duration::from_secs(5), write.send(Message::Text(text.into()))).await,
        Ok(Ok(()))
    )
}

fn short(error: &tokio_tungstenite::tungstenite::Error) -> String {
    match error {
        tokio_tungstenite::tungstenite::Error::Io(e) => format!("io {:?}", e.kind()),
        other => other.to_string().chars().take(80).collect(),
    }
}

impl Connection {
    /// The next frame; None once the connection ended ([`Connection::closed`]
    /// then says why).
    pub async fn next(&mut self) -> Option<Frame> {
        self.frames.recv().await
    }
    /// Adds tokens to the subscription.
    pub fn subscribe(&self, tokens: &[String]) -> Result<()> {
        self.send_text(json!({"assets_ids": tokens, "operation": "subscribe"}).to_string())
    }
    pub fn unsubscribe(&self, tokens: &[String]) -> Result<()> {
        self.send_text(json!({"assets_ids": tokens, "operation": "unsubscribe"}).to_string())
    }
    fn send_text(&self, text: String) -> Result<()> {
        self.outbox
            .as_ref()
            .and_then(|outbox| outbox.send(text).ok())
            .ok_or_else(|| anyhow::anyhow!("the connection is closed"))
    }
    /// True once the socket task ended.
    pub fn is_closed(&self) -> bool {
        self.task.is_finished()
    }
    /// Closes the socket and says why the connection ended.
    pub async fn close(mut self) -> Closed {
        self.outbox.take();
        (&mut self.task).await.unwrap_or(Closed::ByUs)
    }
    /// Why the connection ended, once it did (None while it runs).
    pub async fn closed(&mut self) -> Option<Closed> {
        if !self.task.is_finished() {
            return None;
        }
        Some((&mut self.task).await.unwrap_or(Closed::ByUs))
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::Message;

    /// A local server: takes the subscription, sends a frame, a ping and a
    /// close; the connection reports each in order.
    #[tokio::test]
    async fn frames_pings_and_the_close_arrive_in_order() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let subscription = match ws.next().await {
                Some(Ok(Message::Text(text))) => text.to_string(),
                other => panic!("no subscription: {other:?}"),
            };
            ws.send(Message::Text(r#"[{"event_type":"book","asset_id":"1"}]"#.into()))
                .await
                .unwrap();
            ws.send(Message::Ping(vec![1, 2].into())).await.unwrap();
            let pong = loop {
                match ws.next().await {
                    Some(Ok(Message::Pong(bytes))) => break bytes.to_vec(),
                    Some(Ok(Message::Text(text))) if text == "PING" => {
                        ws.send(Message::Text("PONG".into())).await.unwrap();
                    }
                    other => panic!("unexpected {other:?}"),
                }
            };
            ws.close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
                reason: "bye".into(),
            }))
            .await
            .unwrap();
            (subscription, pong)
        });
        let mut conn = connect(&url, &["1".to_string(), "2".to_string()]).await.unwrap();
        let first = conn.next().await.unwrap();
        assert!(first.text.starts_with("[{\"event_type\":\"book\""));
        assert!(first.received >= conn.opened);
        let mut texts = Vec::new();
        while let Some(frame) = conn.next().await {
            texts.push(frame.text);
        }
        assert!(texts.iter().all(|t| t == "PONG"), "{texts:?}");
        assert_eq!(conn.closed().await, Some(Closed::ByVenue(Some((1000, "bye".into())))));
        let (subscription, pong) = server.await.unwrap();
        assert_eq!(subscription, r#"{"assets_ids":["1","2"],"type":"market"}"#);
        assert_eq!(pong, vec![1, 2]);
    }

    #[tokio::test]
    async fn a_refused_handshake_and_a_dead_port_are_errors() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            #[allow(clippy::result_large_err)]
            let refuse = |_: &tokio_tungstenite::tungstenite::handshake::server::Request,
                          _: tokio_tungstenite::tungstenite::handshake::server::Response| {
                Err(tokio_tungstenite::tungstenite::http::Response::builder()
                    .status(403)
                    .body(None)
                    .unwrap())
            };
            let _ = tokio_tungstenite::accept_hdr_async(stream, refuse).await;
        });
        let error = connect(&url, &["1".to_string()]).await.err().unwrap();
        assert!(error.to_string().contains("HTTP 403"), "{error:#}");
        let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", dead.local_addr().unwrap());
        drop(dead);
        assert!(connect(&url, &["1".to_string()]).await.is_err());
    }
}
