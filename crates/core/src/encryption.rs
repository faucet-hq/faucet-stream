//! Encryption at rest for faucet-managed local files (#207).
//!
//! Seals [`FileStateStore`](crate::state::FileStateStore) bookmark files and
//! (via the jsonl sink's `encryption` feature) per-line JSONL / DLQ output
//! with authenticated encryption, so resume positions and dead-lettered
//! records — which can embed sensitive values — are not stored in plaintext.
//!
//! ## On-disk format (v1)
//!
//! ```text
//! +---------+--------+-----------+----------------------+
//! | "FCT1"  | format | nonce     | ciphertext ‖ GCM tag |
//! | 4 bytes | 1 byte | 12 bytes  | len(plaintext) + 16  |
//! +---------+--------+-----------+----------------------+
//! ```
//!
//! - Magic `FCT1` ("faucet ciphertext v1") marks a sealed payload; plaintext
//!   JSON can never start with these bytes, so [`is_encrypted`] is unambiguous.
//! - `format` byte `0x01` = AES-256-GCM. Unknown values are a typed error
//!   (never a panic), leaving room for future algorithms.
//! - The nonce is random per encryption (never reused across writes).
//! - The GCM tag authenticates the payload: any tampering — a flipped bit,
//!   truncation, a swapped nonce — fails decryption loudly.
//! - `format` byte `0x02` (v2) is AES-256-GCM with the payload's identity —
//!   for a state file, its key — bound in as associated data
//!   ([`CompiledEncryption::encrypt_bound`]), so a sealed file copied over
//!   another key's file fails to decrypt. v1 payloads stay readable.
//!
//! ## Keys
//!
//! The 32-byte AES key is **derived as `SHA-256(key string)`** — a
//! derivation, not a stretching KDF: it accepts arbitrary passphrases and
//! hex/base64 strings uniformly, but offers no brute-force hardening, so the
//! key string should be high-entropy material from a secrets manager
//! (`${vault:…}` / `${aws-sm:…}` / `${env:…}`), not a human password.
//!
//! Rotation: writes always seal with `key`; decryption tries `key` first and
//! then each entry of `previous_keys` in order, so a store can be rotated by
//! moving the old key into `previous_keys` — old files stay readable and are
//! re-sealed with the new key on their next write.

use crate::error::FaucetError;
use aes_gcm::aead::{Aead, AeadCore, Generate, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};

/// The AES-256-GCM nonce array type (12 bytes).
type GcmNonce = Nonce<<Aes256Gcm as AeadCore>::NonceSize>;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Magic prefix marking a faucet-sealed payload.
pub const MAGIC: &[u8; 4] = b"FCT1";
/// Format byte for AES-256-GCM.
const FORMAT_AES256GCM: u8 = 0x01;
/// AES-256-GCM with the payload's identity bound in as associated data (v2).
const FORMAT_AES256GCM_BOUND: u8 = 0x02;
/// Prefix of a v2 payload's associated data, ahead of its context.
const BOUND_AAD_PREFIX: &[u8] = b"faucet:FCT1:v2\0";
/// AES-GCM nonce length in bytes.
const NONCE_LEN: usize = 12;
/// Header length: magic + format byte + nonce.
const HEADER_LEN: usize = 4 + 1 + NONCE_LEN;

/// Supported AEAD algorithms. A single variant today; the format byte in the
/// on-disk header leaves room for more without breaking old files.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum EncryptionAlgorithm {
    /// AES-256-GCM authenticated encryption (the default and only option).
    #[default]
    #[serde(rename = "aes-256-gcm")]
    Aes256Gcm,
}

/// The user-facing `encryption:` config block (state store / jsonl sink).
///
/// ```yaml
/// encryption:
///   key: ${vault:secret/faucet#state-key}
///   # previous_keys: ["${env:OLD_STATE_KEY}"]   # rotation: read-only
///   # algorithm: aes-256-gcm                     # default
/// ```
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EncryptionSpec {
    /// Key material used to seal new writes (and tried first on reads).
    /// Should come from a secrets manager; derived to a 32-byte AES key via
    /// SHA-256.
    pub key: String,
    /// Older keys tried (in order) when decrypting, for rotation. Never used
    /// to seal new writes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub previous_keys: Vec<String>,
    /// AEAD algorithm. Only `aes-256-gcm` today.
    #[serde(default)]
    pub algorithm: EncryptionAlgorithm,
}

impl std::fmt::Debug for EncryptionSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print key material.
        f.debug_struct("EncryptionSpec")
            .field("key", &"***")
            .field(
                "previous_keys",
                &format!("[{} keys]", self.previous_keys.len()),
            )
            .field("algorithm", &self.algorithm)
            .finish()
    }
}

/// A compiled, validated encryption policy: the derived write key plus every
/// derived read key (write key first).
pub struct CompiledEncryption {
    /// Cipher used for new writes.
    write: Aes256Gcm,
    /// Ciphers tried on read: the write key first, then previous keys.
    read: Vec<Aes256Gcm>,
}

impl std::fmt::Debug for CompiledEncryption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledEncryption")
            .field("read_keys", &self.read.len())
            .finish()
    }
}

/// `true` when `data` carries the sealed-payload magic. Free function so
/// callers can detect ciphertext without holding a key.
pub fn is_encrypted(data: &[u8]) -> bool {
    data.len() >= MAGIC.len() && &data[..MAGIC.len()] == MAGIC
}

fn derive_key(key: &str) -> Aes256Gcm {
    let digest = Sha256::digest(key.as_bytes());
    // SHA-256 output is exactly the AES-256 key size; `new_from_slice` on a
    // 32-byte slice cannot fail.
    Aes256Gcm::new_from_slice(&digest).expect("SHA-256 digest is a valid AES-256 key")
}

impl CompiledEncryption {
    /// Validate and compile an [`EncryptionSpec`]. Fail-fast: empty key
    /// material is a typed config error at load time, never mid-run.
    pub fn compile(spec: &EncryptionSpec) -> Result<Self, FaucetError> {
        if spec.key.trim().is_empty() {
            return Err(FaucetError::Config(
                "encryption.key must not be empty".into(),
            ));
        }
        if let Some(idx) = spec.previous_keys.iter().position(|k| k.trim().is_empty()) {
            return Err(FaucetError::Config(format!(
                "encryption.previous_keys[{idx}] must not be empty"
            )));
        }
        let write = derive_key(&spec.key);
        let mut read = vec![derive_key(&spec.key)];
        read.extend(spec.previous_keys.iter().map(|k| derive_key(k)));
        Ok(Self { write, read })
    }

    /// Seal `plaintext` with the current key under a fresh random nonce.
    pub fn encrypt(&self, plaintext: &[u8]) -> Vec<u8> {
        self.seal(plaintext, FORMAT_AES256GCM, &[])
    }

    /// [`encrypt`](Self::encrypt), binding `context` (the identity of what is
    /// sealed — a state key, say) as associated data: the payload opens only
    /// through [`decrypt_bound`](Self::decrypt_bound) with the same context,
    /// so a sealed file copied over another key's file no longer decrypts.
    pub fn encrypt_bound(&self, plaintext: &[u8], context: &[u8]) -> Vec<u8> {
        self.seal(plaintext, FORMAT_AES256GCM_BOUND, &bound_aad(context))
    }

    fn seal(&self, plaintext: &[u8], format: u8, aad: &[u8]) -> Vec<u8> {
        let nonce = GcmNonce::generate();
        let ciphertext = self
            .write
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            // AES-GCM encryption only fails on plaintexts beyond 2^36 bytes;
            // faucet payloads (bookmarks, JSON lines) are nowhere near it.
            .expect("AES-GCM encryption of an in-memory payload cannot fail");
        let mut out = Vec::with_capacity(HEADER_LEN + ciphertext.len());
        out.extend_from_slice(MAGIC);
        out.push(format);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ciphertext);
        out
    }

    /// Open a sealed payload, trying the current key and then each previous
    /// key. Every failure is a typed error — a wrong key or tampered file
    /// must never be silently treated as "no data". A context-bound payload
    /// needs [`decrypt_bound`](Self::decrypt_bound).
    pub fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, FaucetError> {
        self.open(data, None)
    }

    /// Open a payload sealed by [`encrypt_bound`](Self::encrypt_bound) under
    /// `context`, or a legacy unbound one.
    pub fn decrypt_bound(&self, data: &[u8], context: &[u8]) -> Result<Vec<u8>, FaucetError> {
        self.open(data, Some(context))
    }

    fn open(&self, data: &[u8], context: Option<&[u8]>) -> Result<Vec<u8>, FaucetError> {
        if !is_encrypted(data) {
            return Err(FaucetError::State(
                "payload is not faucet-encrypted (missing FCT1 header)".into(),
            ));
        }
        if data.len() < HEADER_LEN {
            return Err(FaucetError::State(
                "encrypted payload is truncated (shorter than its header)".into(),
            ));
        }
        let format = data[MAGIC.len()];
        let aad = match (format, context) {
            (FORMAT_AES256GCM, _) => Vec::new(),
            (FORMAT_AES256GCM_BOUND, Some(context)) => bound_aad(context),
            (FORMAT_AES256GCM_BOUND, None) => {
                return Err(FaucetError::State(
                    "encrypted payload is bound to its identity and must be opened with it".into(),
                ));
            }
            _ => {
                return Err(FaucetError::State(format!(
                    "unknown encrypted-payload format byte 0x{format:02x} — written by a newer \
                     faucet version?"
                )));
            }
        };
        let nonce_bytes: [u8; NONCE_LEN] = data[MAGIC.len() + 1..HEADER_LEN]
            .try_into()
            .expect("slice length checked above");
        let nonce = GcmNonce::from(nonce_bytes);
        let ciphertext = &data[HEADER_LEN..];
        for cipher in &self.read {
            if let Ok(plaintext) = cipher.decrypt(
                &nonce,
                Payload {
                    msg: ciphertext,
                    aad: &aad,
                },
            ) {
                return Ok(plaintext);
            }
        }
        Err(FaucetError::State(
            "decryption failed — wrong or rotated key, or a payload sealed for another \
             identity? (tried the configured key and every previous_keys entry)"
                .into(),
        ))
    }
}

fn bound_aad(context: &[u8]) -> Vec<u8> {
    let mut aad = BOUND_AAD_PREFIX.to_vec();
    aad.extend_from_slice(context);
    aad
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(key: &str) -> EncryptionSpec {
        EncryptionSpec {
            key: key.into(),
            previous_keys: vec![],
            algorithm: EncryptionAlgorithm::default(),
        }
    }

    #[test]
    fn bound_payloads_open_only_under_their_context() {
        let enc = CompiledEncryption::compile(&spec("k1")).unwrap();
        let sealed = enc.encrypt_bound(b"bookmark", b"pipeline::a");
        assert_eq!(sealed[MAGIC.len()], FORMAT_AES256GCM_BOUND);
        assert_eq!(
            enc.decrypt_bound(&sealed, b"pipeline::a").unwrap(),
            b"bookmark"
        );
        let err = enc
            .decrypt_bound(&sealed, b"pipeline::b")
            .unwrap_err()
            .to_string();
        assert!(err.contains("another"), "{err}");
        let err = enc.decrypt(&sealed).unwrap_err().to_string();
        assert!(err.contains("bound to its identity"), "{err}");
        // A v1 payload still opens through the bound reader.
        let legacy = enc.encrypt(b"old");
        assert_eq!(enc.decrypt_bound(&legacy, b"pipeline::a").unwrap(), b"old");
        let mut unknown = legacy.clone();
        unknown[MAGIC.len()] = 0x7f;
        assert!(enc.decrypt_bound(&unknown, b"x").is_err());
    }

    #[test]
    fn a_serialized_spec_round_trips() {
        let spec: EncryptionSpec =
            serde_json::from_value(serde_json::json!({"key": "k1", "previous_keys": ["k0"]}))
                .unwrap();
        let back: EncryptionSpec =
            serde_json::from_value(serde_json::to_value(&spec).unwrap()).unwrap();
        assert_eq!(back.key, "k1");
        assert_eq!(back.previous_keys, vec!["k0".to_string()]);
        assert!(!format!("{spec:?}").contains("k1"));
    }

    #[test]
    fn round_trips() {
        let enc = CompiledEncryption::compile(&spec("k1")).unwrap();
        let sealed = enc.encrypt(b"hello bookmark");
        assert!(is_encrypted(&sealed));
        assert_eq!(enc.decrypt(&sealed).unwrap(), b"hello bookmark");
        // Empty plaintext round-trips too.
        let sealed = enc.encrypt(b"");
        assert_eq!(enc.decrypt(&sealed).unwrap(), b"");
    }

    #[test]
    fn nonces_are_unique_per_encryption() {
        let enc = CompiledEncryption::compile(&spec("k1")).unwrap();
        let a = enc.encrypt(b"same plaintext");
        let b = enc.encrypt(b"same plaintext");
        assert_ne!(a, b, "two seals of the same plaintext must differ");
    }

    #[test]
    fn tampering_is_detected() {
        let enc = CompiledEncryption::compile(&spec("k1")).unwrap();
        let sealed = enc.encrypt(b"payload");

        // Flip a ciphertext byte.
        let mut tampered = sealed.clone();
        let mid = HEADER_LEN + 1;
        tampered[mid] ^= 0x01;
        assert!(enc.decrypt(&tampered).is_err());

        // Flip a tag byte (the tag is the trailing 16 bytes).
        let mut tampered = sealed.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 0x01;
        assert!(enc.decrypt(&tampered).is_err());

        // Truncate.
        let truncated = &sealed[..sealed.len() - 4];
        assert!(enc.decrypt(truncated).is_err());
        // Truncated inside the header.
        assert!(enc.decrypt(&sealed[..6]).is_err());
    }

    #[test]
    fn wrong_key_is_a_typed_error() {
        let enc = CompiledEncryption::compile(&spec("k1")).unwrap();
        let other = CompiledEncryption::compile(&spec("k2")).unwrap();
        let sealed = enc.encrypt(b"payload");
        let err = other.decrypt(&sealed).unwrap_err();
        assert!(matches!(err, FaucetError::State(_)));
        assert!(err.to_string().contains("wrong or rotated key"));
    }

    #[test]
    fn rotation_reads_old_writes_new() {
        let old = CompiledEncryption::compile(&spec("old-key")).unwrap();
        let sealed_old = old.encrypt(b"v1");

        let rotated = CompiledEncryption::compile(&EncryptionSpec {
            key: "new-key".into(),
            previous_keys: vec!["old-key".into()],
            algorithm: EncryptionAlgorithm::default(),
        })
        .unwrap();
        // Reads sealed-with-old.
        assert_eq!(rotated.decrypt(&sealed_old).unwrap(), b"v1");
        // Writes seal with the new key only.
        let sealed_new = rotated.encrypt(b"v2");
        let new_only = CompiledEncryption::compile(&spec("new-key")).unwrap();
        assert_eq!(new_only.decrypt(&sealed_new).unwrap(), b"v2");
        assert!(old.decrypt(&sealed_new).is_err());
    }

    #[test]
    fn is_encrypted_detection() {
        assert!(!is_encrypted(b""));
        assert!(!is_encrypted(b"FC"));
        assert!(!is_encrypted(b"{\"json\": true}"));
        assert!(is_encrypted(b"FCT1 anything"));
    }

    #[test]
    fn unknown_format_byte_is_a_typed_error() {
        let enc = CompiledEncryption::compile(&spec("k1")).unwrap();
        let mut sealed = enc.encrypt(b"x");
        sealed[4] = 0x7f; // unknown format
        let err = enc.decrypt(&sealed).unwrap_err();
        assert!(err.to_string().contains("format byte 0x7f"));
    }

    #[test]
    fn non_encrypted_input_is_a_typed_error() {
        let enc = CompiledEncryption::compile(&spec("k1")).unwrap();
        let err = enc.decrypt(b"plain text").unwrap_err();
        assert!(err.to_string().contains("not faucet-encrypted"));
    }

    #[test]
    fn empty_keys_are_rejected_at_compile_time() {
        assert!(matches!(
            CompiledEncryption::compile(&spec("  ")),
            Err(FaucetError::Config(_))
        ));
        let bad_prev = EncryptionSpec {
            key: "k".into(),
            previous_keys: vec!["ok".into(), "".into()],
            algorithm: EncryptionAlgorithm::default(),
        };
        let err = CompiledEncryption::compile(&bad_prev).unwrap_err();
        assert!(err.to_string().contains("previous_keys[1]"));
    }

    #[test]
    fn spec_serde_shape_and_debug_masking() {
        let s: EncryptionSpec = serde_json::from_value(json!({
            "key": "SECRET-MATERIAL",
            "previous_keys": ["OLD-SECRET"],
        }))
        .unwrap();
        assert_eq!(s.algorithm, EncryptionAlgorithm::Aes256Gcm);
        let rendered = format!("{s:?}");
        assert!(!rendered.contains("SECRET-MATERIAL"));
        assert!(!rendered.contains("OLD-SECRET"));
        assert!(rendered.contains("[1 keys]"));
        // Unknown fields are rejected.
        assert!(serde_json::from_value::<EncryptionSpec>(json!({"key": "k", "nope": 1})).is_err());
        // Algorithm round-trips by its serde name.
        let s: EncryptionSpec =
            serde_json::from_value(json!({"key": "k", "algorithm": "aes-256-gcm"})).unwrap();
        assert_eq!(s.algorithm, EncryptionAlgorithm::Aes256Gcm);
    }

    #[test]
    fn compiled_debug_never_prints_key_material() {
        let enc = CompiledEncryption::compile(&spec("TOPSECRET")).unwrap();
        let rendered = format!("{enc:?}");
        assert!(!rendered.contains("TOPSECRET"));
    }
}
