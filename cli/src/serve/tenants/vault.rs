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
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trips_and_rotates() {
        let old = Vault::new("old-key", &[]).unwrap();
        let sealed = old.seal(&json!({"token": "s3cret"}));
        assert!(!sealed.contains("s3cret"));
        assert_eq!(old.open(&sealed).unwrap()["token"], "s3cret");

        let rotated = Vault::new("new-key", &["old-key".into()]).unwrap();
        assert_eq!(rotated.open(&sealed).unwrap()["token"], "s3cret");
        let resealed = rotated.seal_str("v");
        assert_eq!(rotated.open_str(&resealed).unwrap(), "v");
        assert!(old.open(&resealed).unwrap_err().contains("wrong vault key"));
    }

    #[test]
    fn rejects_bad_input() {
        assert!(Vault::new("", &[]).unwrap_err().contains("vault key"));
        let v = Vault::new("k", &[]).unwrap();
        assert!(v.open("%%%").unwrap_err().contains("base64"));
        assert!(v.open_str(&v.seal(&json!(1))).unwrap_err().contains("not a string"));
        let not_json = base64::engine::general_purpose::STANDARD.encode(v.enc.encrypt(b"{"));
        assert!(v.open(&not_json).unwrap_err().contains("not JSON"));
        assert_eq!(format!("{v:?}"), "Vault { .. }");
    }
}
