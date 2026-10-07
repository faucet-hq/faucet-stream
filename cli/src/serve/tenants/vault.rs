//! The connection vault key (#709): seals tenant credentials with AES-256-GCM
//! before they reach the run-history store, and opens them on the instance
//! that runs the pipeline.

use base64::Engine as _;
use faucet_core::encryption::{CompiledEncryption, EncryptionSpec};
use serde_json::Value;

/// Seals and opens connection credentials.
pub struct Vault {
    enc: CompiledEncryption,
}

impl std::fmt::Debug for Vault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vault").finish_non_exhaustive()
    }
}

impl Vault {
    /// Build from the current key and any previous keys (rotation: previous
    /// keys only open, never seal).
    pub fn new(key: &str, previous: &[String]) -> Result<Self, String> {
        // The key is hashed once into the AES key, with no slow KDF, so its
        // strength is the key material's own (#789 SERVE-44). A previous key
        // only opens existing rows, so rotating away from a short one works.
        if key.len() < MIN_KEY_BYTES {
            return Err(format!(
                "vault key: must be at least {MIN_KEY_BYTES} bytes of random key material \
                 (got {}) — generate one with `openssl rand -hex 32`",
                key.len()
            ));
        }
        let spec = EncryptionSpec {
            key: key.to_string(),
            previous_keys: previous.to_vec(),
            algorithm: Default::default(),
        };
        CompiledEncryption::compile(&spec)
            .map(|enc| Self { enc })
            .map_err(|e| format!("vault key: {e}"))
    }

    /// Seal a JSON value as base64 text.
    pub fn seal(&self, value: &Value) -> String {
        let plain = serde_json::to_vec(value).expect("a JSON value always serializes");
        base64::engine::general_purpose::STANDARD.encode(self.enc.encrypt(&plain))
    }

    /// Seal a string.
    pub fn seal_str(&self, s: &str) -> String {
        self.seal(&Value::String(s.to_string()))
    }

    /// Open a sealed value.
    pub fn open(&self, sealed: &str) -> Result<Value, String> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(sealed)
            .map_err(|e| format!("sealed value is not base64: {e}"))?;
        let plain = self
            .enc
            .decrypt(&bytes)
            .map_err(|e| format!("cannot open sealed value (wrong vault key?): {e}"))?;
        serde_json::from_slice(&plain).map_err(|e| format!("sealed value is not JSON: {e}"))
    }

    /// Open a sealed string.
    pub fn open_str(&self, sealed: &str) -> Result<String, String> {
        match self.open(sealed)? {
            Value::String(s) => Ok(s),
            _ => Err("sealed value is not a string".into()),
        }
    }

    /// Seal `value` bound to `context` (e.g. `connection:<tenant>/<name>`):
    /// the context is the ciphertext's associated data, so a sealed row
    /// copied onto another tenant's or connection's record does not open
    /// there (#789 SERVE-44).
    pub fn seal_for(&self, value: &Value, context: &str) -> String {
        let plain = serde_json::to_vec(value).expect("a JSON value always serializes");
        base64::engine::general_purpose::STANDARD
            .encode(self.enc.encrypt_bound(&plain, context.as_bytes()))
    }

    /// Open a value sealed by [`Vault::seal_for`] under `context`. A value
    /// sealed before binding existed (by [`Vault::seal`]) still opens.
    pub fn open_for(&self, sealed: &str, context: &str) -> Result<Value, String> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(sealed)
            .map_err(|e| format!("sealed value is not base64: {e}"))?;
        let plain = self
            .enc
            .decrypt_bound(&bytes, context.as_bytes())
            .map_err(|e| {
                format!(
                    "cannot open sealed value (wrong vault key, or it belongs to another owner \
                     than `{context}`): {e}"
                )
            })?;
        serde_json::from_slice(&plain).map_err(|e| format!("sealed value is not JSON: {e}"))
    }

    /// [`Vault::seal_for`] for a string.
    pub fn seal_str_for(&self, s: &str, context: &str) -> String {
        self.seal_for(&Value::String(s.to_string()), context)
    }

    /// [`Vault::open_for`] for a string.
    pub fn open_str_for(&self, sealed: &str, context: &str) -> Result<String, String> {
        match self.open_for(sealed, context)? {
            Value::String(s) => Ok(s),
            _ => Err("sealed value is not a string".into()),
        }
    }

    /// The binding context of a tenant connection's credentials.
    pub fn connection_context(tenant: &str, name: &str) -> String {
        format!("connection:{tenant}/{name}")
    }
}

/// Minimum vault key length, in bytes.
pub const MIN_KEY_BYTES: usize = 32;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const OLD: &str = "0000000000000000000000000000000000000000000000000000000000000001";
    const NEW: &str = "0000000000000000000000000000000000000000000000000000000000000002";

    #[test]
    fn bound_values_open_only_under_their_context() {
        let v = Vault::new(NEW, &[]).unwrap();
        let sealed = v.seal_for(&json!({"token": "t"}), "connection:acme/api");
        assert_eq!(
            v.open_for(&sealed, "connection:acme/api").unwrap()["token"],
            "t"
        );
        let err = v.open_for(&sealed, "connection:globex/api").unwrap_err();
        assert!(err.contains("another owner"), "{err}");
        assert!(v.open(&sealed).is_err(), "a bound value needs its context");
        assert!(v.open_for("%%%", "c").unwrap_err().contains("base64"));
        let not_json =
            base64::engine::general_purpose::STANDARD.encode(v.enc.encrypt_bound(b"{", b"c"));
        assert!(v.open_for(&not_json, "c").unwrap_err().contains("not JSON"));
        let rotated = Vault::new(OLD, &[NEW.into()]).unwrap();
        assert_eq!(
            rotated.open_for(&sealed, "connection:acme/api").unwrap()["token"],
            "t"
        );
        // A value sealed before binding still opens.
        let legacy = v.seal(&json!({"token": "old"}));
        assert_eq!(v.open_for(&legacy, "anything").unwrap()["token"], "old");
        let s = v.seal_str_for("pkce", "session:x");
        assert_eq!(v.open_str_for(&s, "session:x").unwrap(), "pkce");
        assert!(v.open_str_for(&v.seal_for(&json!(1), "c"), "c").is_err());
        assert_eq!(
            Vault::connection_context("acme", "api"),
            "connection:acme/api"
        );
    }

    #[test]
    fn round_trips_and_rotates() {
        let old = Vault::new(OLD, &[]).unwrap();
        let sealed = old.seal(&json!({"token": "s3cret"}));
        assert!(!sealed.contains("s3cret"));
        assert_eq!(old.open(&sealed).unwrap()["token"], "s3cret");

        let rotated = Vault::new(NEW, &[OLD.into()]).unwrap();
        assert_eq!(rotated.open(&sealed).unwrap()["token"], "s3cret");
        let resealed = rotated.seal_str("v");
        assert_eq!(rotated.open_str(&resealed).unwrap(), "v");
        assert!(old.open(&resealed).unwrap_err().contains("wrong vault key"));
    }

    #[test]
    fn rejects_bad_input() {
        assert!(Vault::new("", &[]).unwrap_err().contains("vault key"));
        assert!(
            Vault::new("short", &[])
                .unwrap_err()
                .contains("at least 32")
        );
        let v = Vault::new(NEW, &[]).unwrap();
        assert!(v.open("%%%").unwrap_err().contains("base64"));
        assert!(
            v.open_str(&v.seal(&json!(1)))
                .unwrap_err()
                .contains("not a string")
        );
        let not_json = base64::engine::general_purpose::STANDARD.encode(v.enc.encrypt(b"{"));
        assert!(v.open(&not_json).unwrap_err().contains("not JSON"));
        assert_eq!(format!("{v:?}"), "Vault { .. }");
    }
}
