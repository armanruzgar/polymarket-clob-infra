//! The execution journal's types: an order intent as journaled before it is
//! sent, a fill as the private stream reports it, and the archive ages.
use serde::{Deserialize, Serialize};

/// Settled history is archived once its last event is this old, so today's
/// UTC realizations always stay in the live journal.
pub const ARCHIVE_MIN_AGE_MS: u64 = 86_400_000;
/// The shortest archive age a caller may ask for.
pub const ARCHIVE_MIN_AGE_FLOOR_MS: u64 = 900_000;

/// One order of ours, journaled before it is sent.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Intent {
    pub order_id: String,
    pub token_id: String,
    pub action: String,
    pub price: f64,
    pub size: f64,
    pub state: String,
    #[serde(default)]
    pub filled: f64,
    #[serde(default)]
    pub post_only: bool,
    #[serde(default)]
    pub reserve_unit_cost: Option<f64>,
}

/// One maker order's part of a trade, as the venue reports it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MakerOrder {
    pub order_id: String,
    pub matched_amount: f64,
    pub price: f64,
    /// The maker order's token, when the venue names it: a leg on the other
    /// outcome's token matched the taker order at 1 - its price.
    #[serde(default)]
    pub asset_id: Option<String>,
}

/// A fill as the private stream reports it (ws::fill_stream): a trade event (a
/// delta of its own) or an order update (cumulative quantities). It is the
/// venue's claim; the ledger confirms it against the on-chain receipt.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    #[serde(default)]
    pub transaction_hash: Option<String>,
    #[serde(default)]
    pub matched_at_ms: u64,
    /// Order updates carry cumulative quantities; trades carry unique deltas.
    #[serde(default)]
    pub cumulative: bool,
    pub id: String,
    pub taker_order_id: String,
    pub maker_orders: Vec<MakerOrder>,
    pub asset_id: String,
    /// "BUY" or "SELL".
    pub side: String,
    pub size: f64,
    pub price: f64,
    /// MATCHED, MINED, CONFIRMED, RETRYING or FAILED for a trade; the order
    /// status for an order update.
    pub status: String,
    /// The condition ID the venue names (the trade's `market`): a claim the
    /// receipt check confirms before use.
    #[serde(default)]
    pub market: Option<String>,
}
