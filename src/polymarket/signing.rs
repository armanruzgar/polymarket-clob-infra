//! EIP-712 order signing for Polymarket CLOB.
//!
//! Polymarket uses the CTF Exchange contract on Polygon (chain_id=137).
//! Orders must be signed with EIP-712 typed data.

use crate::abi::{encode, keccak256, Address, Token, U256};
use anyhow::{ensure, Context, Result};

/// A secp256k1 signing key and its address.
pub struct Wallet {
    key: k256::ecdsa::SigningKey,
    address: Address,
}

impl std::str::FromStr for Wallet {
    type Err = anyhow::Error;
    /// A 32-byte private key as hex, with or without `0x`.
    fn from_str(value: &str) -> Result<Self> {
        // The key's bytes are wiped once the signing key holds them (it wipes its own
        // copy when dropped: ecdsa's SigningKey is ZeroizeOnDrop).
        let bytes = zeroize::Zeroizing::new(
            hex::decode(value.trim().trim_start_matches("0x"))
                .map_err(|_| anyhow::anyhow!("Private key is not hex"))?,
        );
        ensure!(bytes.len() == 32, "Private key must be 32 bytes");
        let key = k256::ecdsa::SigningKey::from_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("Private key is not a valid secp256k1 scalar"))?;
        let point = key.verifying_key().to_encoded_point(false);
        let address = Address::from_slice(&keccak256(&point.as_bytes()[1..])[12..]);
        Ok(Self { key, address })
    }
}

impl std::fmt::Debug for Wallet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Wallet({})", crate::abi::address_lower(&self.address))
    }
}

impl Wallet {
    pub fn address(&self) -> Address {
        self.address
    }
    /// RFC 6979 deterministic, low-s signature over a 32-byte digest as
    /// r || s || v with v = 27 or 28.
    pub fn sign_hash(&self, digest: &[u8; 32]) -> Result<[u8; 65]> {
        let (signature, recovery) = self.key.sign_prehash_recoverable(digest).context("Signing failed")?;
        let mut out = [0u8; 65];
        out[..64].copy_from_slice(&signature.to_bytes());
        out[64] = recovery.to_byte() + 27;
        Ok(out)
    }
}

/// Polymarket CTF Exchange contract on Polygon.
const CTF_EXCHANGE: &str = "0xE111180000d2663C0091e4f400237545B87B996B";
/// Polymarket Neg Risk CTF Exchange on Polygon.
const NEG_RISK_CTF_EXCHANGE: &str = "0xe2222d279d744050d28e00520010520000310F59";

/// Side constants matching the Polymarket contract.
pub const BUY: u8 = 0;
pub const SELL: u8 = 1;

/// V2 signature and deterministic exchange order ID, available before publishing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedOrder {
    pub signature: String,
    pub salt: u64,
    pub timestamp: u64,
    pub order_id: String,
}

/// An order before its signature: its salt and timestamp, and its EIP-712
/// hash, which is its exchange order ID. sign() gives the signature
/// SignedOrder carries (split so the order ID is known before signing, e.g. to
/// journal the intent while the signature is computed).
pub struct UnsignedOrder {
    pub salt: u64,
    pub timestamp: u64,
    pub order_id: String,
    digest: [u8; 32],
}

#[cfg(test)]
thread_local! {
    /// Tests: the next signature taken on this thread fails.
    pub(crate) static FAIL_NEXT_SIGNATURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl UnsignedOrder {
    /// The V2 signature over the order's hash, as 0x-prefixed hex.
    pub fn sign(&self, wallet: &Wallet) -> Result<String> {
        #[cfg(test)]
        if FAIL_NEXT_SIGNATURE.with(|fail| fail.replace(false)) {
            anyhow::bail!("injected signing failure");
        }
        Ok(format!("0x{}", hex::encode(wallet.sign_hash(&self.digest)?)))
    }
}

/// sign_order without the signature: the order's fields and its hash, for a
/// signature taken later (UnsignedOrder::sign).
#[allow(clippy::too_many_arguments)] // Each argument is a distinct field of the signed order or intent.
pub fn unsigned_order(
    wallet: &Wallet,
    token_id: &str,
    maker_amount: u64,
    taker_amount: u64,
    side: u8,
    funder: &str,
    neg_risk: bool,
    signature_type: u8,
) -> Result<UnsignedOrder> {
    use rand::Rng;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;
    let salt = rand::rng().random::<u64>() >> 11;
    unsigned_order_at(
        wallet,
        token_id,
        maker_amount,
        taker_amount,
        side,
        funder,
        neg_risk,
        signature_type,
        salt,
        timestamp,
    )
}

#[allow(clippy::too_many_arguments)] // Each argument is a distinct field of the signed order or intent.
pub async fn sign_order(
    wallet: &Wallet,
    token_id: &str,
    maker_amount: u64,
    taker_amount: u64,
    side: u8,
    funder: &str,
    neg_risk: bool,
    signature_type: u8,
) -> Result<SignedOrder> {
    use rand::Rng;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;
    // Its own statement: the thread-local generator must not live across the
    // await, or the order path could not run on a spawned (Send) task.
    let salt = rand::rng().random::<u64>() >> 11;
    sign_order_at(
        wallet,
        token_id,
        maker_amount,
        taker_amount,
        side,
        funder,
        neg_risk,
        signature_type,
        salt,
        timestamp,
    )
    .await
}

#[allow(clippy::too_many_arguments)] // Each argument is a distinct field of the signed order or intent.
async fn sign_order_at(
    wallet: &Wallet,
    token_id: &str,
    maker_amount: u64,
    taker_amount: u64,
    side: u8,
    funder: &str,
    neg_risk: bool,
    signature_type: u8,
    salt: u64,
    timestamp: u64,
) -> Result<SignedOrder> {
    let order = unsigned_order_at(
        wallet,
        token_id,
        maker_amount,
        taker_amount,
        side,
        funder,
        neg_risk,
        signature_type,
        salt,
        timestamp,
    )?;
    Ok(SignedOrder {
        signature: order.sign(wallet)?,
        salt: order.salt,
        timestamp: order.timestamp,
        order_id: order.order_id,
    })
}

#[allow(clippy::too_many_arguments)] // Each argument is a distinct field of the signed order or intent.
fn unsigned_order_at(
    wallet: &Wallet,
    token_id: &str,
    maker_amount: u64,
    taker_amount: u64,
    side: u8,
    funder: &str,
    neg_risk: bool,
    signature_type: u8,
    salt: u64,
    timestamp: u64,
) -> Result<UnsignedOrder> {
    anyhow::ensure!(
        maker_amount > 0 && taker_amount > 0 && side <= 1,
        "Invalid order amounts or side"
    );
    anyhow::ensure!(signature_type <= 2, "Only EOA, proxy and Safe signatures are supported");
    let maker: Address = funder.parse()?;
    anyhow::ensure!(
        signature_type != 0 || maker == wallet.address(),
        "EOA funder must equal signer"
    );
    let order = PolymarketOrder {
        salt,
        maker,
        signer: wallet.address(),
        token_id: crate::abi::u256_dec(token_id).map_err(|_| anyhow::anyhow!("Invalid token ID"))?,
        maker_amount: U256::from(maker_amount),
        taker_amount: U256::from(taker_amount),
        side,
        signature_type,
        timestamp,
        exchange: if neg_risk { NEG_RISK_CTF_EXCHANGE } else { CTF_EXCHANGE }.parse()?,
    };
    let digest = order.signing_hash();
    Ok(UnsignedOrder {
        salt,
        timestamp,
        order_id: format!("0x{}", hex::encode(digest)),
        digest,
    })
}

#[derive(Debug, Clone)]
struct PolymarketOrder {
    salt: u64,
    maker: Address,
    signer: Address,
    token_id: U256,
    maker_amount: U256,
    taker_amount: U256,
    side: u8,
    signature_type: u8,
    timestamp: u64,
    exchange: Address,
}

impl PolymarketOrder {
    /// EIP-712 domain: name "Polymarket CTF Exchange", version "2", Polygon.
    fn domain_separator(&self) -> [u8; 32] {
        keccak256(encode(&[
            Token::FixedBytes(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")
                    .to_vec(),
            ),
            Token::FixedBytes(keccak256("Polymarket CTF Exchange").to_vec()),
            Token::FixedBytes(keccak256("2").to_vec()),
            Token::Uint(U256::from(137u64)),
            Token::Address(self.exchange),
        ]))
    }
    fn struct_hash(&self) -> [u8; 32] {
        keccak256(encode(&[
            Token::FixedBytes(keccak256("Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)").to_vec()),
            Token::Uint(U256::from(self.salt)),
            Token::Address(self.maker),
            Token::Address(self.signer),
            Token::Uint(self.token_id),
            Token::Uint(self.maker_amount),
            Token::Uint(self.taker_amount),
            Token::Uint(U256::from(self.side)),
            Token::Uint(U256::from(self.signature_type)),
            Token::Uint(U256::from(self.timestamp)),
            Token::FixedBytes(vec![0; 32]),
            Token::FixedBytes(vec![0; 32]),
        ]))
    }
    /// keccak256(0x1901 || domainSeparator || structHash): the exchange order ID.
    fn signing_hash(&self) -> [u8; 32] {
        let mut preimage = vec![0x19, 0x01];
        preimage.extend(self.domain_separator());
        preimage.extend(self.struct_hash());
        keccak256(preimage)
    }
}

/// Generate HMAC signature for API authentication.
pub fn hmac_signature(secret: &str, timestamp: &str, method: &str, path: &str, body: &str) -> Result<String> {
    use base64::engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD};
    use base64::Engine;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    // Polymarket API secrets are URL-safe base64 encoded
    let secret_bytes = zeroize::Zeroizing::new(
        URL_SAFE
            .decode(secret)
            .or_else(|_| URL_SAFE_NO_PAD.decode(secret))
            .or_else(|_| STANDARD.decode(secret))?,
    );

    // Match Python: timestamp + method + requestPath, then body only if non-empty
    let mut message = format!("{}{}{}", timestamp, method, path);
    if !body.is_empty() {
        // Python replaces single quotes with double quotes for cross-language compat
        message.push_str(&body.replace('\'', "\""));
    }

    let mut mac = Hmac::<Sha256>::new_from_slice(&secret_bytes)?;
    mac.update(message.as_bytes());
    let result = mac.finalize();

    Ok(URL_SAFE.encode(result.into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    /// An order's signature (EIP-712 hash and the ECDSA signature), as the
    /// order path signs each order. Ignored: by hand, release:
    /// cargo test --release speed_bench_sign -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn speed_bench_sign() {
        let wallet: Wallet = format!("{:064x}", 1).parse().unwrap();
        let mut us = Vec::new();
        for _ in 0..20_000 {
            let started = std::time::Instant::now();
            let signed = sign_order(
                &wallet,
                "12345678901234567890",
                2500000,
                5000000,
                BUY,
                "0x1111111111111111111111111111111111111111",
                false,
                1,
            )
            .await
            .unwrap();
            us.push(started.elapsed().as_secs_f64() * 1e6);
            std::hint::black_box(signed);
        }
        // This file is also the latency probe's (src/bin): no client module here.
        us.sort_by(f64::total_cmp);
        let at = |q: f64| us[((us.len() - 1) as f64 * q).round() as usize];
        println!(
            "SPEED sign_order: n {} min {:.2} p50 {:.2} p90 {:.2} p99 {:.2} max {:.2} us",
            us.len(),
            us[0],
            at(0.5),
            at(0.9),
            at(0.99),
            us[us.len() - 1]
        );
    }
    #[tokio::test]
    async fn matches_official_python_v2_sdk_vectors() {
        // Public deterministic test key (scalar 1), never used for trading.
        let wallet: Wallet = format!("{:064x}", 1).parse().unwrap();
        let vectors: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/clob_v2_orders.json")).unwrap();
        for v in vectors.as_array().unwrap() {
            let signed = sign_order_at(
                &wallet,
                "12345678901234567890",
                2500000,
                5000000,
                v["side"].as_u64().unwrap() as u8,
                "0x1111111111111111111111111111111111111111",
                v["neg_risk"].as_bool().unwrap(),
                1,
                12345,
                1700000000123,
            )
            .await
            .unwrap();
            assert_eq!(signed.order_id, v["order_id"].as_str().unwrap());
            assert_eq!(signed.signature, v["signature"].as_str().unwrap());
        }
        assert!(sign_order(
            &wallet,
            "invalid",
            1,
            1,
            BUY,
            &crate::abi::address_lower(&wallet.address()),
            false,
            0
        )
        .await
        .is_err());
        assert!(sign_order(
            &wallet,
            "123",
            0,
            1,
            BUY,
            &crate::abi::address_lower(&wallet.address()),
            false,
            0
        )
        .await
        .is_err());
    }
}
