//! The venue's V2 taker fee schedule and a conservative collateral reservation
//! for a BUY: an estimate that governs admission only. The fee a fill paid is
//! what its receipt says (receipts), never this estimate.
use anyhow::{ensure, Context, Result};
use serde_json::Value;
use std::time::{Duration, Instant};

pub const MAX_FEE_AGE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
pub struct FeeSchedule {
    pub rate: f64,
    pub exponent: f64,
    pub received: Instant,
}

impl FeeSchedule {
    pub fn from_market(market: &Value, token: &str) -> Result<Self> {
        let tokens = market["t"].as_array().context("Missing fee market tokens")?;
        ensure!(
            tokens.iter().any(|t| t["t"].as_str() == Some(token)),
            "Fee market does not contain requested token"
        );
        let number = |value: &Value| value.as_f64().or_else(|| value.as_str()?.parse().ok());
        let rate = number(&market["fd"]["r"]).context("Missing V2 fee rate")?;
        let exponent = number(&market["fd"]["e"]).context("Missing V2 fee exponent")?;
        ensure!(
            rate.is_finite() && (0.0..=1.0).contains(&rate) && exponent.is_finite() && (0.0..=10.0).contains(&exponent),
            "Unsupported V2 fee schedule"
        );
        Ok(Self {
            rate,
            exponent,
            received: Instant::now(),
        })
    }

    pub fn fresh(&self) -> bool {
        self.received.elapsed() <= MAX_FEE_AGE
    }

    /// Still fresh after `window` more time: refreshing before a strategy
    /// decision with this margin keeps the order path free of fee reads.
    pub fn fresh_for(&self, window: Duration) -> bool {
        self.received.elapsed() + window <= MAX_FEE_AGE
    }

    pub fn buy_unit_reserve(&self, limit: f64) -> Result<f64> {
        ensure!(self.fresh(), "Fee metadata is stale");
        ensure!(limit.is_finite() && limit > 0.0 && limit < 1.0, "Invalid buy limit");
        if self.rate == 0.0 {
            return Ok(limit);
        }
        // An execution below the buy limit can have a larger fee when the limit
        // is above 50c. Reserve the maximum curve value over every allowed price.
        let p = limit.min(0.5);
        let fee = self.rate * (p * (1.0 - p)).powf(self.exponent);
        // No minimum partial-fill size is assumed. For nearest-unit rounding,
        // a positive rounded fee is at most twice its unrounded value; smaller
        // fees round to zero. Summing this bound also covers arbitrary partial
        // fills without incorrectly granting a single rounding allowance/order.
        Ok(limit + 2.0 * fee)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn fee_curve_reserves_price_improvement_and_split_fill_rounding() {
        let fee = FeeSchedule::from_market(&json!({"t":[{"t":"123"}], "fd":{"r":0.07,"e":1}}), "123").unwrap();
        assert!((fee.buy_unit_reserve(0.4).unwrap() - 0.4336).abs() < 1e-12);
        assert!((fee.buy_unit_reserve(0.9).unwrap() - 0.935).abs() < 1e-12);
        for limit in [0.1, 0.4, 0.5, 0.9] {
            for i in 1..=1000 {
                let p = limit * i as f64 / 1000.;
                assert!(p + 0.07 * p * (1. - p) <= fee.buy_unit_reserve(limit).unwrap());
                for quantity in [0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1, 6.0] {
                    let rounded_fee = (quantity * 0.07 * p * (1. - p) * 100_000.).round() / 100_000.;
                    assert!(quantity * p + rounded_fee <= quantity * fee.buy_unit_reserve(limit).unwrap() + 1e-12);
                }
            }
        }
    }
    #[test]
    fn missing_malformed_wrong_market_and_stale_fees_fail_closed() {
        for market in [
            json!({}),
            json!({"t":[{"t":"456"}],"fd":{"r":0,"e":1}}),
            json!({"t":[{"t":"123"}]}),
            json!({"t":[{"t":"123"}],"fd":{"r":"NaN","e":1}}),
            json!({"t":[{"t":"123"}],"fd":{"r":0.07,"e":-1}}),
        ] {
            assert!(FeeSchedule::from_market(&market, "123").is_err());
        }
        let mut fee = FeeSchedule::from_market(&json!({"t":[{"t":"123"}],"fd":{"r":0,"e":1}}), "123").unwrap();
        assert_eq!(fee.buy_unit_reserve(0.4).unwrap(), 0.4);
        assert!(fee.fresh_for(Duration::from_secs(3)));
        fee.received -= Duration::from_secs(3);
        assert!(fee.fresh() && !fee.fresh_for(Duration::from_secs(3)));
        fee.received -= MAX_FEE_AGE;
        assert!(fee.buy_unit_reserve(0.4).is_err());
    }
}
