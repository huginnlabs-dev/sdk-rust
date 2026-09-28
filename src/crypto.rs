//! Client-side payload protection, wire-compatible with the other SDKs:
//! AES-256-GCM over the JSON payload, key derived from the user secret via
//! PBKDF2-SHA256 (10 000 iterations, 16-byte process salt). The secret
//! never leaves the host.

use crate::json;
use crate::Value;
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use std::collections::BTreeMap;
use std::sync::OnceLock;

const KEY_LEN: usize = 32;
const IV_LEN: usize = 12;
const SALT_LEN: usize = 16;
const ITERATIONS: u32 = 10_000;

struct Envelope {
    key: Key<Aes256Gcm>,
    salt_hex: String,
}

static ENVELOPE: OnceLock<Option<Envelope>> = OnceLock::new();

fn envelope() -> &'static Option<Envelope> {
    ENVELOPE.get_or_init(|| {
        let secret = &crate::settings().encryption_key;
        if secret.is_empty() {
            return None;
        }
        let mut salt = [0u8; SALT_LEN];
        crate::fill_random(&mut salt);
        let mut key = [0u8; KEY_LEN];
        pbkdf2::pbkdf2_hmac::<sha2::Sha256>(secret.as_bytes(), &salt, ITERATIONS, &mut key);
        let salt_hex: String = salt.iter().map(|b| format!("{:02x}", b)).collect();
        Some(Envelope { key: *Key::<Aes256Gcm>::from_slice(&key), salt_hex })
    })
}

/// Serializes the payload snapshot to JSON and seals it when a key is
/// configured; plaintext otherwise (visibility beats silence).
pub fn seal(payload: &BTreeMap<String, Value>) -> String {
    let plain = json::payload_object(payload);
    match envelope() {
        None => {
            // Plaintext payload document.
            plain
        }
        Some(env) => {
            let cipher = Aes256Gcm::new(&env.key);
            let mut iv = [0u8; IV_LEN];
            crate::fill_random(&mut iv);
            match cipher.encrypt(
                Nonce::from_slice(&iv),
                Payload { msg: plain.as_bytes(), aad: &[] },
            ) {
                Ok(ct) => {
                    let b64 = base64_encode(&ct);
                    let iv_b64 = base64_encode(&iv);
                    format!(
                        "{{\"encrypted\":true,\"data_b64\":\"{}\",\"iv_b64\":\"{}\",\"key_salt\":\"{}\"}}",
                        b64, iv_b64, env.salt_hex
                    )
                }
                Err(_) => plain,
            }
        }
    }
}

/// Standard base64 (no external crates).
pub fn base64_encode(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(TABLE[(n >> 18) as usize & 63] as char);
        out.push(TABLE[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { TABLE[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { TABLE[n as usize & 63] as char } else { '=' });
    }
    out
}
