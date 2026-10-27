#![cfg_attr(feature = "net", doc = include_str!("../README.md"))]
#![cfg_attr(
    not(feature = "net"),
    doc = "Order book, redundant-feed merge, lag detection, EIP-712 signing and the execution ledger for Polymarket's CLOB, built without the `net` feature: no transport, no REST client, no fill stream. The README's quickstart is in the crate docs of the default build."
)]
pub mod abi;
pub mod accounting;
pub mod execution;
pub mod fees;
pub mod market_data;
pub mod polymarket;
pub mod receipts;
pub mod ws;
