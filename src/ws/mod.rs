//! The venue's market channel: the merge of redundant connections
//! ([`feed_merge`]), the lag rules ([`feed_lag`]) and, with the `net` feature,
//! the connections themselves ([`connect`]), the feed that runs them
//! ([`feed`]) and the private fill stream ([`fill_stream`]).
#[cfg(feature = "net")]
pub mod connect;
#[cfg(feature = "net")]
pub mod feed;
pub mod feed_lag;
pub mod feed_merge;
#[cfg(feature = "net")]
pub mod fill_stream;
