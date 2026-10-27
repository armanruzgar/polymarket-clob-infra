//! Polymarket specifics: order signing ([`signing`]), API authentication
//! ([`auth`]) and, with the `net` feature, the REST client ([`rest`]).
#[cfg(feature = "net")]
pub mod auth;
#[cfg(feature = "net")]
pub mod rest;
pub mod signing;
