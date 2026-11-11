//! A signed record, carried in a dead-letter envelope's `masking` object, of
//! the masking rules its payload went through: `{"rules": "<fingerprint>",
//! "sig": "<hex HMAC-SHA256>"}`. A replay that finds a valid record for the
//! rules in force today can write the payload as it is; any other payload is
//! masked again.

use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

const DOMAIN: &[u8] = b"faucet-dlq-masking/v1";

/// Signs and checks the `masking` object of DLQ envelopes for one tenant and
/// one set of masking rules.
#[derive(Clone)]
pub struct MaskingProof {
    key: Vec<u8>,
    tenant: String,
    rules: String,
}

impl std::fmt::Debug for MaskingProof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaskingProof")
            .field("tenant", &self.tenant)
            .field("rules", &self.rules)
            .finish_non_exhaustive()
    }
}

impl MaskingProof {
    /// Sign with `key` for `tenant` (`None`: no tenant) and the rules
    /// fingerprint `rules` ([`rules_fingerprint`]).
    pub fn new(key: impl Into<Vec<u8>>, tenant: Option<&str>, rules: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            tenant: tenant.unwrap_or_default().to_string(),
            rules: rules.into(),
        }
    }

    /// The rules fingerprint this proof signs.
    pub fn rules(&self) -> &str {
        &self.rules
    }

    /// Add the `masking` object to `envelope` (a [`crate::build_envelope`]
    /// object), signed over its pipeline, row and payload.
    pub fn sign(&self, envelope: &mut Value) {
        let Some(obj) = envelope.as_object() else {
            return;
        };
        let sig = self.mac(
            str_of(obj.get("pipeline")),
            str_of(obj.get("row")),
            obj.get("payload").unwrap_or(&Value::Null),
        );
        if let Some(obj) = envelope.as_object_mut() {
            obj.insert(
                "masking".into(),
                json!({ "rules": self.rules, "sig": hex(&sig) }),
            );
        }
    }

    /// Whether `masking` (an envelope's `masking` object) records these rules
    /// and carries a valid signature over `pipeline`, `row` and `payload`. A
    /// malformed object is not valid.
    pub fn verify(&self, pipeline: &str, row: &str, payload: &Value, masking: &Value) -> bool {
        let (Some(rules), Some(sig)) = (
            masking.get("rules").and_then(Value::as_str),
            masking.get("sig").and_then(Value::as_str),
        ) else {
            return false;
        };
        let Some(sig) = unhex(sig) else {
            return false;
        };
        if rules != self.rules {
            return false;
        }
        self.hmac(pipeline, row, payload).verify_slice(&sig).is_ok()
    }

    fn mac(&self, pipeline: &str, row: &str, payload: &Value) -> Vec<u8> {
        self.hmac(pipeline, row, payload)
            .finalize()
            .into_bytes()
            .to_vec()
    }

    fn hmac(&self, pipeline: &str, row: &str, payload: &Value) -> HmacSha256 {
        let mut mac =
            HmacSha256::new_from_slice(&self.key).expect("HMAC takes a key of any length");
        mac.update(DOMAIN);
        let payload = Sha256::digest(crate::util::canonical_json(payload).as_bytes());
        for part in [
            self.tenant.as_bytes(),
            pipeline.as_bytes(),
            row.as_bytes(),
            self.rules.as_bytes(),
            payload.as_slice(),
        ] {
            mac.update(&(part.len() as u64).to_be_bytes());
            mac.update(part);
        }
        mac
    }
}

/// A signing key for `label` derived from the secret `secret` (a server key):
/// HMAC-SHA256 of the label under the secret.
pub fn derive_key(secret: &[u8], label: &str) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC takes a key of any length");
    mac.update(b"faucet-derive/");
    mac.update(label.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// The fingerprint of the masking that applies to a destination: `rules` (the
/// rules that name it, as JSON) and the masking `key` they hash with, the key
/// entering only through an HMAC under `proof_key`, so the fingerprint says
/// nothing about it.
pub fn rules_fingerprint(rules: &Value, masking_key: Option<&str>, proof_key: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(proof_key).expect("HMAC takes a key of any length");
    mac.update(b"faucet-masking-key/");
    mac.update(masking_key.unwrap_or_default().as_bytes());
    let key_mac = mac.finalize().into_bytes();
    let mut h = Sha256::new();
    h.update(crate::util::canonical_json(rules).as_bytes());
    h.update([u8::from(masking_key.is_some())]);
    h.update(key_mac);
    hex(&h.finalize())
}

fn str_of(v: Option<&Value>) -> &str {
    v.and_then(Value::as_str).unwrap_or_default()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) || !s.is_ascii() {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(payload: Value) -> Value {
        json!({ "pipeline": "p", "row": "r", "payload": payload })
    }

    fn check(p: &MaskingProof, e: &Value) -> bool {
        p.verify(
            e["pipeline"].as_str().unwrap_or(""),
            e["row"].as_str().unwrap_or(""),
            &e["payload"],
            &e["masking"],
        )
    }

    #[test]
    fn a_signed_envelope_verifies_only_as_signed() {
        let key = derive_key(b"server-secret", "dlq-masking");
        let rules = rules_fingerprint(&json!([{"name": "email"}]), Some("k"), &key);
        let p = MaskingProof::new(key.clone(), Some("acme"), rules.clone());
        assert_eq!(p.rules(), rules);
        assert!(format!("{p:?}").contains("acme"));
        let mut e = envelope(json!({"email": "***", "id": 1}));
        p.sign(&mut e);
        assert!(check(&p, &e));

        let mut tampered = e.clone();
        tampered["payload"]["email"] = json!("a@b.example");
        assert!(!check(&p, &tampered), "payload changed");
        let mut moved = e.clone();
        moved["row"] = json!("other");
        assert!(!check(&p, &moved), "row changed");
        let other_tenant = MaskingProof::new(key.clone(), Some("globex"), rules.clone());
        assert!(!check(&other_tenant, &e));
        let other_rules = MaskingProof::new(
            key.clone(),
            Some("acme"),
            rules_fingerprint(&json!([{"name": "email"}]), Some("k2"), &key),
        );
        assert!(
            !check(&other_rules, &e),
            "a changed masking key changes the rules"
        );
        let other_key = MaskingProof::new(derive_key(b"x", "dlq-masking"), Some("acme"), rules);
        assert!(!check(&other_key, &e));

        for bad in [
            json!(null),
            json!({"rules": p.rules()}),
            json!({"rules": p.rules(), "sig": "zz"}),
            json!({"rules": p.rules(), "sig": "abc"}),
            json!({"rules": p.rules(), "sig": "é1"}),
            json!({"rules": "x", "sig": e["masking"]["sig"]}),
        ] {
            let mut forged = e.clone();
            forged["masking"] = bad;
            assert!(!check(&p, &forged));
        }
        let mut not_object = json!("x");
        p.sign(&mut not_object);
        assert_eq!(not_object, json!("x"));
    }

    #[test]
    fn fingerprints_follow_rules_and_key() {
        let k = derive_key(b"s", "dlq-masking");
        let a = rules_fingerprint(&json!([{"b": 1, "a": 2}]), None, &k);
        assert_eq!(a, rules_fingerprint(&json!([{"a": 2, "b": 1}]), None, &k));
        assert_ne!(a, rules_fingerprint(&json!([{"a": 2}]), None, &k));
        assert_ne!(
            a,
            rules_fingerprint(&json!([{"b": 1, "a": 2}]), Some(""), &k)
        );
        assert_ne!(derive_key(b"s", "a"), derive_key(b"s", "b"));
    }
}
