//! The venue's REST API: the authenticated order requests (POST /order,
//! DELETE /order, DELETE /cancel-all, DELETE /cancel-market-orders), the order
//! reads (GET /data/order/{id}, GET /data/orders) and the public book and
//! clock. Every authenticated request carries the L2 headers: the API key and
//! passphrase, the signer's address, a timestamp and an HMAC over the method,
//! path and body ([`signing::hmac_signature`]). The key, secret and passphrase
//! come from [`auth::derive_api_creds`](super::auth::derive_api_creds) or the
//! environment ([`ApiCreds::from_env`]).
//!
//! What this client does not do: retry, pace, or decide anything. A refused
//! request is an [`HttpStatus`] error with the venue's answer; what to do with
//! it is the caller's. Nothing secret is ever formatted into an error.
use super::signing::{self, SignedOrder, Wallet};
use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use zeroize::Zeroizing;

/// The venue's CLOB.
pub const CLOB_URL: &str = "https://clob.polymarket.com";
/// The venue's market metadata (public).
pub const GAMMA_URL: &str = "https://gamma-api.polymarket.com";
/// A request's default timeout.
pub const TIMEOUT: Duration = Duration::from_secs(5);

/// L2 API credentials. The secret and passphrase are wiped when dropped and
/// never shown by Debug.
#[derive(Clone)]
pub struct ApiCreds {
    key: String,
    secret: Zeroizing<String>,
    passphrase: Zeroizing<String>,
}

impl ApiCreds {
    pub fn new(key: impl Into<String>, secret: impl Into<String>, passphrase: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            secret: Zeroizing::new(secret.into()),
            passphrase: Zeroizing::new(passphrase.into()),
        }
    }
    /// POLYMARKET_API_KEY, POLYMARKET_API_SECRET and POLYMARKET_API_PASSPHRASE,
    /// all three or none (Ok(None)); a partial set is an error.
    pub fn from_env() -> Result<Option<Self>> {
        let read = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        match (
            read("POLYMARKET_API_KEY"),
            read("POLYMARKET_API_SECRET"),
            read("POLYMARKET_API_PASSPHRASE"),
        ) {
            (Some(k), Some(s), Some(p)) => Ok(Some(Self::new(k, s, p))),
            (None, None, None) => Ok(None),
            _ => bail!("set POLYMARKET_API_KEY, POLYMARKET_API_SECRET and POLYMARKET_API_PASSPHRASE together, or none"),
        }
    }
    /// The API key (the `owner` of every order).
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn secret(&self) -> &str {
        &self.secret
    }
    pub fn passphrase(&self) -> &str {
        &self.passphrase
    }
}

impl std::fmt::Debug for ApiCreds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiCreds")
            .field("key", &self.key)
            .field("secret", &"<redacted>")
            .field("passphrase", &"<redacted>")
            .finish()
    }
}

/// A request the venue refused: its status and the start of its answer (the
/// venue echoes nothing secret; a request's own body is never included).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpStatus {
    pub method: String,
    pub path: String,
    pub status: u16,
    pub body: String,
    /// The venue's Retry-After, in seconds, when it sent one (a rate limit).
    pub retry_after: Option<u64>,
}

impl std::fmt::Display for HttpStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} refused: HTTP {} {}",
            self.method, self.path, self.status, self.body
        )
    }
}
impl std::error::Error for HttpStatus {}

impl HttpStatus {
    /// The venue's `errorMsg` or `error`, when its answer was JSON.
    pub fn message(&self) -> Option<String> {
        let value: Value = serde_json::from_str(&self.body).ok()?;
        value["errorMsg"]
            .as_str()
            .or_else(|| value["error"].as_str())
            .map(str::to_string)
    }
}

/// The venue's order types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderType {
    /// Good till cancelled.
    Gtc,
    /// Good till `expiration` (unix seconds).
    Gtd,
    /// Fill and kill: what does not match at once is cancelled.
    Fak,
    /// Fill or kill.
    Fok,
}

impl OrderType {
    fn name(self) -> &'static str {
        match self {
            Self::Gtc => "GTC",
            Self::Gtd => "GTD",
            Self::Fak => "FAK",
            Self::Fok => "FOK",
        }
    }
}

/// An order to sign and send. Amounts are in the token's and collateral's
/// integer units (millionths): a BUY of 5 shares at 40c is
/// `maker_amount: 2_000_000, taker_amount: 5_000_000`; a SELL of the same is
/// `maker_amount: 5_000_000, taker_amount: 2_000_000`.
#[derive(Clone, Debug, PartialEq)]
pub struct NewOrder {
    pub token_id: String,
    pub maker_amount: u64,
    pub taker_amount: u64,
    /// [`signing::BUY`] or [`signing::SELL`].
    pub side: u8,
    /// The funder (the proxy wallet, or the signer itself with signature type 0).
    pub funder: String,
    /// 0: EOA, 1: Polymarket proxy, 2: Gnosis safe.
    pub signature_type: u8,
    /// The market's exchange: the neg-risk one for multi-outcome markets.
    pub neg_risk: bool,
    pub order_type: OrderType,
    pub post_only: bool,
    /// GTD only: unix seconds; 0 otherwise.
    pub expiration: u64,
}

impl NewOrder {
    /// A good-till-cancelled order on the proxy wallet (signature type 1), not
    /// post-only.
    pub fn gtc(token_id: &str, side: u8, maker_amount: u64, taker_amount: u64, funder: &str) -> Self {
        Self {
            token_id: token_id.to_string(),
            maker_amount,
            taker_amount,
            side,
            funder: funder.to_string(),
            signature_type: 1,
            neg_risk: false,
            order_type: OrderType::Gtc,
            post_only: false,
            expiration: 0,
        }
    }
    pub fn post_only(mut self) -> Self {
        self.post_only = true;
        self
    }
    pub fn neg_risk(mut self, neg_risk: bool) -> Self {
        self.neg_risk = neg_risk;
        self
    }
    pub fn signature_type(mut self, signature_type: u8) -> Self {
        self.signature_type = signature_type;
        self
    }
    pub fn order_type(mut self, order_type: OrderType) -> Self {
        self.order_type = order_type;
        self
    }
}

/// The body of POST /order for a signed order: the order as the venue's SDKs
/// send it, owned by the API key.
pub fn order_body(order: &NewOrder, signed: &SignedOrder, signer: &str, api_key: &str) -> Value {
    json!({
        "order": {
            "salt": signed.salt,
            "maker": order.funder,
            "signer": signer,
            "tokenId": order.token_id,
            "makerAmount": order.maker_amount.to_string(),
            "takerAmount": order.taker_amount.to_string(),
            "expiration": order.expiration.to_string(),
            "side": if order.side == signing::BUY { "BUY" } else { "SELL" },
            "signatureType": order.signature_type,
            "timestamp": signed.timestamp.to_string(),
            "metadata": format!("0x{}", "00".repeat(32)),
            "builder": format!("0x{}", "00".repeat(32)),
            "signature": signed.signature,
        },
        "owner": api_key,
        "orderType": order.order_type.name(),
        "postOnly": order.post_only,
    })
}

/// What a cancel request did.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Cancelled {
    pub canceled: Vec<String>,
    /// Order ID -> the venue's reason.
    pub not_canceled: Vec<(String, String)>,
}

impl Cancelled {
    fn from_answer(answer: &Value) -> Self {
        Self {
            canceled: answer["canceled"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect(),
            not_canceled: answer["not_canceled"]
                .as_object()
                .into_iter()
                .flatten()
                .map(|(k, v)| (k.clone(), v.as_str().map_or_else(|| v.to_string(), str::to_string)))
                .collect(),
        }
    }
    pub fn cancelled(&self, order_id: &str) -> bool {
        self.canceled.iter().any(|id| id == order_id)
    }
}

/// An order the venue accepted (or refused with HTTP 200 and `success: false`).
#[derive(Clone, Debug, PartialEq)]
pub struct Placed {
    /// The order's ID: the EIP-712 hash, known before the POST; the venue's
    /// `orderID` when it names one.
    pub order_id: String,
    pub signed: SignedOrder,
    /// `success` was not false.
    pub accepted: bool,
    /// The venue's `status` (live, matched, delayed) or `errorMsg`.
    pub status: String,
    pub answer: Value,
}

/// An authenticated client for one API key on one signer.
pub struct Client {
    http: reqwest::Client,
    clob: String,
    creds: ApiCreds,
    /// The signer's checksum address (POLY_ADDRESS).
    address: String,
}

impl Client {
    /// A client on `clob_url` for the wallet's API credentials. The HTTP client
    /// follows no redirect (a redirect would carry the signed headers elsewhere).
    pub fn new(clob_url: &str, wallet: &Wallet, creds: ApiCreds) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .tcp_nodelay(true)
            .pool_idle_timeout(Duration::from_secs(90))
            .build()?;
        Ok(Self::with_http(http, clob_url, wallet, creds))
    }
    pub fn with_http(http: reqwest::Client, clob_url: &str, wallet: &Wallet, creds: ApiCreds) -> Self {
        Self {
            http,
            clob: clob_url.trim_end_matches('/').to_string(),
            creds,
            address: wallet.address().to_checksum(None),
        }
    }
    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }
    pub fn creds(&self) -> &ApiCreds {
        &self.creds
    }
    /// The signer's checksum address.
    pub fn address(&self) -> &str {
        &self.address
    }
    pub fn clob_url(&self) -> &str {
        &self.clob
    }

    /// One authenticated request. The HMAC covers the path without its query
    /// and the body as sent (empty for GET). Any status outside 2xx is an
    /// [`HttpStatus`] error; a 2xx answer that is not JSON is an error too.
    pub async fn request(&self, method: reqwest::Method, path: &str, body: Option<&Value>) -> Result<Value> {
        ensure!(path.starts_with('/'), "path must start with /");
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs().to_string();
        let body_text = body.map(serde_json::to_string).transpose()?.unwrap_or_default();
        let sign_path = path.split('?').next().unwrap_or(path);
        let signature =
            signing::hmac_signature(self.creds.secret(), &timestamp, method.as_str(), sign_path, &body_text)?;
        let mut request = self
            .http
            .request(method.clone(), format!("{}{}", self.clob, path))
            .header("POLY_ADDRESS", &self.address)
            .header("POLY_SIGNATURE", signature)
            .header("POLY_TIMESTAMP", timestamp)
            .header("POLY_API_KEY", self.creds.key())
            .header("POLY_PASSPHRASE", self.creds.passphrase());
        if body.is_some() {
            request = request.header("Content-Type", "application/json").body(body_text);
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("{method} {sign_path} not sent"))?;
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse().ok());
        let text = response
            .text()
            .await
            .with_context(|| format!("{method} {sign_path}: answer not read"))?;
        if !(200..300).contains(&status) {
            return Err(HttpStatus {
                method: method.to_string(),
                path: sign_path.to_string(),
                status,
                body: text.chars().take(300).collect(),
                retry_after,
            }
            .into());
        }
        serde_json::from_str(&text).with_context(|| format!("{method} {sign_path}: answer is not JSON"))
    }

    /// Signs `order` with `wallet` and sends it. The order ID is known before
    /// the POST (journal it first: a lost answer leaves an order that may rest).
    pub async fn place(&self, wallet: &Wallet, order: &NewOrder) -> Result<Placed> {
        let signed = signing::sign_order(
            wallet,
            &order.token_id,
            order.maker_amount,
            order.taker_amount,
            order.side,
            &order.funder,
            order.neg_risk,
            order.signature_type,
        )
        .await?;
        let body = order_body(order, &signed, &self.address, self.creds.key());
        let answer = self.post_order(&body).await?;
        Ok(Self::placed(signed, answer))
    }

    /// Sends an order body already built ([`order_body`]): the signature was
    /// taken earlier, or on another thread.
    pub async fn post_order(&self, body: &Value) -> Result<Value> {
        self.request(reqwest::Method::POST, "/order", Some(body)).await
    }

    fn placed(signed: SignedOrder, answer: Value) -> Placed {
        let accepted = answer["success"].as_bool() != Some(false);
        let order_id = answer["orderID"]
            .as_str()
            .filter(|s| !s.is_empty())
            .unwrap_or(&signed.order_id)
            .to_string();
        let status = if accepted {
            answer["status"].as_str().unwrap_or("ok").to_string()
        } else {
            answer["errorMsg"].as_str().unwrap_or("refused").to_string()
        };
        Placed {
            order_id,
            signed,
            accepted,
            status,
            answer,
        }
    }

    /// DELETE /order: one order.
    pub async fn cancel(&self, order_id: &str) -> Result<Cancelled> {
        let answer = self
            .request(reqwest::Method::DELETE, "/order", Some(&json!({"orderID": order_id})))
            .await?;
        Ok(Cancelled::from_answer(&answer))
    }

    /// DELETE /cancel-all: every open order of these API credentials.
    pub async fn cancel_all(&self) -> Result<Cancelled> {
        let answer = self.request(reqwest::Method::DELETE, "/cancel-all", None).await?;
        Ok(Cancelled::from_answer(&answer))
    }

    /// DELETE /cancel-market-orders: every open order on one token.
    pub async fn cancel_market(&self, asset_id: &str) -> Result<Cancelled> {
        let answer = self
            .request(
                reqwest::Method::DELETE,
                "/cancel-market-orders",
                Some(&json!({"asset_id": asset_id})),
            )
            .await?;
        Ok(Cancelled::from_answer(&answer))
    }

    /// GET /data/order/{id}: the order as the venue holds it (`status` LIVE,
    /// MATCHED, CANCELED, ...).
    pub async fn order(&self, order_id: &str) -> Result<Value> {
        ensure!(
            !order_id.is_empty() && order_id.chars().all(|c| c.is_ascii_alphanumeric()),
            "order id is not hex"
        );
        self.request(reqwest::Method::GET, &format!("/data/order/{order_id}"), None)
            .await
    }

    /// True when the venue reports the order as not resting: cancelled,
    /// matched, or unknown to it (never accepted).
    pub async fn order_closed(&self, order_id: &str) -> Result<bool> {
        let order = self.order(order_id).await?;
        let status = order["status"].as_str().unwrap_or("").to_ascii_uppercase();
        Ok(!status.is_empty() && status != "LIVE")
    }

    /// GET /data/orders: the open orders of these credentials, on one token or
    /// all, every page.
    pub async fn open_orders(&self, asset_id: Option<&str>) -> Result<Vec<Value>> {
        let mut orders = Vec::new();
        let mut cursor = String::new();
        for _ in 0..1_000 {
            let mut query = Vec::new();
            if let Some(asset) = asset_id {
                ensure!(asset.chars().all(|c| c.is_ascii_digit()), "asset id is not decimal");
                query.push(format!("asset_id={asset}"));
            }
            if !cursor.is_empty() {
                query.push(format!("next_cursor={cursor}"));
            }
            let path = if query.is_empty() {
                "/data/orders".to_string()
            } else {
                format!("/data/orders?{}", query.join("&"))
            };
            let answer = self.request(reqwest::Method::GET, &path, None).await?;
            let (page, next) = match &answer {
                Value::Array(list) => (list.clone(), None),
                Value::Object(map) => (
                    map.get("data").and_then(Value::as_array).cloned().unwrap_or_default(),
                    map.get("next_cursor").and_then(Value::as_str).map(str::to_string),
                ),
                _ => bail!("GET /data/orders: unexpected answer"),
            };
            orders.extend(page);
            match next {
                Some(next) if !next.is_empty() && next != "LTE=" && next != cursor => cursor = next,
                _ => return Ok(orders),
            }
        }
        bail!("GET /data/orders: the pages never ended")
    }

    /// GET /book (public): the token's book, with `tick_size`, `min_order_size`
    /// and `neg_risk`.
    pub async fn book(&self, token_id: &str) -> Result<Value> {
        book(&self.http, &self.clob, token_id).await
    }

    /// GET /time (public): the venue's clock, whole seconds.
    pub async fn server_time(&self) -> Result<i64> {
        let text = self
            .http
            .get(format!("{}/time", self.clob))
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        crate::ws::feed_lag::parse_server_seconds(&text)
    }
}

/// GET /book (public) with any HTTP client.
pub async fn book(http: &reqwest::Client, clob_url: &str, token_id: &str) -> Result<Value> {
    ensure!(token_id.chars().all(|c| c.is_ascii_digit()), "token id is not decimal");
    let book: Value = http
        .get(format!("{}/book", clob_url.trim_end_matches('/')))
        .query(&[("token_id", token_id)])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .context("GET /book: answer is not JSON")?;
    Ok(book)
}

/// The best bid of a public book answer, 0 without bids.
pub fn best_bid(book: &Value) -> f64 {
    book["bids"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|l| l["price"].as_str()?.parse::<f64>().ok())
        .fold(0.0, f64::max)
}

/// The best ask of a public book answer, 1 without asks.
pub fn best_ask(book: &Value) -> f64 {
    book["asks"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|l| l["price"].as_str()?.parse::<f64>().ok())
        .fold(1.0, f64::min)
}

/// The two outcome tokens of a market on Gamma, by slug (public).
pub async fn market_tokens(http: &reqwest::Client, gamma_url: &str, slug: &str) -> Result<Vec<String>> {
    let markets: Value = http
        .get(format!("{}/markets", gamma_url.trim_end_matches('/')))
        .query(&[("slug", slug)])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let ids = markets[0]["clobTokenIds"]
        .as_str()
        .with_context(|| format!("market {slug} not found"))?;
    let tokens: Vec<String> = serde_json::from_str(ids).context("clobTokenIds is not a JSON list")?;
    ensure!(
        tokens.len() == 2
            && tokens
                .iter()
                .all(|t| !t.is_empty() && t.chars().all(|c| c.is_ascii_digit())),
        "market {slug}: unexpected clobTokenIds"
    );
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creds_hide_their_secrets_and_come_together_or_not_at_all() {
        let creds = ApiCreds::new("key", "hush-secret", "hush-phrase");
        let shown = format!("{creds:?}");
        assert!(shown.contains("key") && !shown.contains("hush"), "{shown}");
        assert_eq!(creds.secret(), "hush-secret");
    }

    #[test]
    fn order_body_is_the_sdk_shape() {
        let order = NewOrder::gtc("123", signing::BUY, 2_000_000, 5_000_000, "0xfunder").post_only();
        let signed = SignedOrder {
            signature: "0xsig".into(),
            salt: 7,
            timestamp: 1_700_000_000_123,
            order_id: "0xid".into(),
        };
        let body = order_body(&order, &signed, "0xsigner", "api-key");
        assert_eq!(body["order"]["makerAmount"], "2000000");
        assert_eq!(body["order"]["takerAmount"], "5000000");
        assert_eq!(body["order"]["side"], "BUY");
        assert_eq!(body["order"]["signatureType"], 1);
        assert_eq!(body["order"]["expiration"], "0");
        assert_eq!(body["order"]["timestamp"], "1700000000123");
        assert_eq!(body["owner"], "api-key");
        assert_eq!(body["orderType"], "GTC");
        assert_eq!(body["postOnly"], true);
    }

    #[test]
    fn cancel_answers_are_read_by_id() {
        let answer = json!({"canceled": ["a", "b"], "not_canceled": {"c": "order not found"}});
        let cancelled = Cancelled::from_answer(&answer);
        assert!(cancelled.cancelled("a") && !cancelled.cancelled("c"));
        assert_eq!(
            cancelled.not_canceled,
            vec![("c".to_string(), "order not found".to_string())]
        );
    }

    #[test]
    fn a_refusal_carries_the_venue_message() {
        let refused = HttpStatus {
            method: "POST".into(),
            path: "/order".into(),
            status: 400,
            body: r#"{"errorMsg":"Already canceled"}"#.into(),
            retry_after: None,
        };
        assert_eq!(refused.message().as_deref(), Some("Already canceled"));
        assert!(refused.to_string().starts_with("POST /order refused: HTTP 400"));
    }

    #[test]
    fn best_prices_read_the_public_book() {
        let book = json!({"bids": [{"price": "0.40", "size": "5"}, {"price": "0.41", "size": "1"}],
            "asks": [{"price": "0.45", "size": "5"}, {"price": "0.43", "size": "1"}]});
        assert_eq!(best_bid(&book), 0.41);
        assert_eq!(best_ask(&book), 0.43);
        assert_eq!(best_bid(&json!({})), 0.0);
        assert_eq!(best_ask(&json!({})), 1.0);
    }

    #[tokio::test]
    async fn a_placed_order_keeps_its_id_and_reads_the_refusal() {
        let signed = || SignedOrder {
            signature: "0x".into(),
            salt: 1,
            timestamp: 2,
            order_id: "0xmine".into(),
        };
        let ok = Client::placed(
            signed(),
            json!({"success": true, "orderID": "0xvenue", "status": "live"}),
        );
        assert!(ok.accepted);
        assert_eq!((ok.order_id.as_str(), ok.status.as_str()), ("0xvenue", "live"));
        let refused = Client::placed(signed(), json!({"success": false, "errorMsg": "not enough balance"}));
        assert!(!refused.accepted);
        assert_eq!(
            (refused.order_id.as_str(), refused.status.as_str()),
            ("0xmine", "not enough balance")
        );
    }
}
