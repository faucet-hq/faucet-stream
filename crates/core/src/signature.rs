//! HMAC request-signature verification and constant-time token comparison,
//! shared by every component that authenticates an inbound HTTP request.
//!
//! [`SignatureSpec`] is the serde form; [`SignatureCheck`] is the borrowed
//! view the verifier runs on, so a caller with its own config type can verify
//! without copying the secret.

use hmac::{Mac, digest::KeyInit};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

/// Default accepted clock difference for a signed timestamp, in seconds.
pub const DEFAULT_TOLERANCE_SECS: u64 = 300;

/// HMAC digest of a request signature.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum HmacAlgorithm {
    /// HMAC-SHA256.
    #[default]
    Sha256,
    /// HMAC-SHA512.
    Sha512,
}

/// Text encoding of a signature digest.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DigestEncoding {
    /// Hexadecimal (case-insensitive).
    #[default]
    Hex,
    /// Standard base64.
    Base64,
}

/// HMAC signature verification over the raw request body.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct SignatureSpec {
    /// Request header carrying the signature (e.g. `X-Signature-256`).
    pub header: String,
    /// The shared signing secret.
    pub secret: String,
    /// HMAC digest (default `sha256`).
    #[serde(default)]
    pub algorithm: HmacAlgorithm,
    /// How the digest is written in the header (default `hex`).
    #[serde(default)]
    pub encoding: DigestEncoding,
    /// Text before the digest in the header value (e.g. `sha256=`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Header carrying the Unix-seconds timestamp the sender signed. When set,
    /// the signed content is `<timestamp>.<body>` and a timestamp more than
    /// `tolerance_secs` from now is rejected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_header: Option<String>,
    /// Accepted clock difference for `timestamp_header`, in seconds (default 300).
    #[serde(default = "default_tolerance_secs")]
    pub tolerance_secs: u64,
}

fn default_tolerance_secs() -> u64 {
    DEFAULT_TOLERANCE_SECS
}

impl SignatureSpec {
    /// A SHA-256, hex-encoded signature in `header`, keyed by `secret`.
    pub fn new(header: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            header: header.into(),
            secret: secret.into(),
            algorithm: HmacAlgorithm::default(),
            encoding: DigestEncoding::default(),
            prefix: None,
            timestamp_header: None,
            tolerance_secs: DEFAULT_TOLERANCE_SECS,
        }
    }

    /// Set the HMAC digest.
    pub fn algorithm(mut self, algorithm: HmacAlgorithm) -> Self {
        self.algorithm = algorithm;
        self
    }

    /// Set the digest encoding.
    pub fn encoding(mut self, encoding: DigestEncoding) -> Self {
        self.encoding = encoding;
        self
    }

    /// Require `prefix` before the digest in the header value.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Sign `<timestamp>.<body>` with the timestamp read from `header`.
    pub fn timestamp_header(mut self, header: impl Into<String>) -> Self {
        self.timestamp_header = Some(header.into());
        self
    }

    /// Set the accepted clock difference for the signed timestamp.
    pub fn tolerance_secs(mut self, secs: u64) -> Self {
        self.tolerance_secs = secs;
        self
    }

    /// Whether the header name and the secret are both set.
    pub fn is_complete(&self) -> bool {
        !self.header.is_empty() && !self.secret.is_empty()
    }

    /// The borrowed view the verifier runs on.
    pub fn check(&self) -> SignatureCheck<'_> {
        SignatureCheck {
            header: &self.header,
            secret: self.secret.as_bytes(),
            algorithm: self.algorithm,
            encoding: self.encoding,
            prefix: self.prefix.as_deref(),
            timestamp_header: self.timestamp_header.as_deref(),
            tolerance_secs: self.tolerance_secs,
        }
    }
}

/// A signed-timestamp request's identity inside its tolerance window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedRequest {
    /// The signature header value (lower-cased for hex).
    pub signature: String,
    /// The Unix-seconds timestamp the sender signed.
    pub timestamp: u64,
    /// The oldest timestamp still inside the tolerance window.
    pub oldest_accepted: u64,
}

/// Borrowed signature settings; build one from [`SignatureSpec::check`] or
/// from another config type's fields.
#[derive(Debug, Clone, Copy)]
pub struct SignatureCheck<'a> {
    /// Request header carrying the signature.
    pub header: &'a str,
    /// The shared signing secret.
    pub secret: &'a [u8],
    /// HMAC digest.
    pub algorithm: HmacAlgorithm,
    /// Digest encoding in the header.
    pub encoding: DigestEncoding,
    /// Text required before the digest.
    pub prefix: Option<&'a str>,
    /// Header carrying the signed Unix-seconds timestamp.
    pub timestamp_header: Option<&'a str>,
    /// Accepted clock difference for the timestamp, in seconds.
    pub tolerance_secs: u64,
}

impl SignatureCheck<'_> {
    /// Verify the signature over `body` (and the signed timestamp, when
    /// configured) in constant time. `header` looks a request header up by
    /// name; `now_unix` is the verifier's clock in Unix seconds.
    pub fn verify<'h>(
        &self,
        header: impl Fn(&str) -> Option<&'h str>,
        body: &[u8],
        now_unix: u64,
    ) -> bool {
        let Some(provided) = header(self.header) else {
            return false;
        };
        let provided = provided.trim();
        let provided = match self.prefix {
            Some(p) => match provided.strip_prefix(p) {
                Some(rest) => rest,
                None => return false,
            },
            None => provided,
        };
        let mut signed: Vec<u8> = Vec::with_capacity(body.len() + 21);
        if let Some(ts_name) = self.timestamp_header {
            let Some(ts) = header(ts_name).and_then(|t| t.trim().parse::<u64>().ok()) else {
                return false;
            };
            if ts.abs_diff(now_unix) > self.tolerance_secs {
                return false;
            }
            signed.extend_from_slice(ts.to_string().as_bytes());
            signed.push(b'.');
        }
        signed.extend_from_slice(body);
        let Some(expected) = self.encoded_digest(&signed) else {
            return false;
        };
        let provided = match self.encoding {
            DigestEncoding::Hex => provided.to_ascii_lowercase(),
            DigestEncoding::Base64 => provided.to_string(),
        };
        bool::from(provided.as_bytes().ct_eq(expected.as_bytes()))
    }

    /// For a request signed with a timestamp: its signature header value, its
    /// timestamp and the oldest timestamp still accepted. `None` when no
    /// timestamp header is configured or either header is missing or unreadable.
    pub fn signed_request<'h>(
        &self,
        header: impl Fn(&str) -> Option<&'h str>,
        now_unix: u64,
    ) -> Option<SignedRequest> {
        let timestamp = header(self.timestamp_header?)?.trim().parse::<u64>().ok()?;
        let provided = header(self.header)?.trim();
        let signature = match self.encoding {
            DigestEncoding::Hex => provided.to_ascii_lowercase(),
            DigestEncoding::Base64 => provided.to_string(),
        };
        Some(SignedRequest {
            signature,
            timestamp,
            oldest_accepted: now_unix.saturating_sub(self.tolerance_secs),
        })
    }

    /// The signature header value a sender sets for `body`: the prefix, then
    /// the digest over `<timestamp>.<body>` when `timestamp_unix` is given
    /// (send the same number in the timestamp header) or over `body` alone.
    /// `None` when the secret cannot key the digest.
    pub fn sign(&self, body: &[u8], timestamp_unix: Option<u64>) -> Option<String> {
        let mut signed: Vec<u8> = Vec::with_capacity(body.len() + 21);
        if let Some(ts) = timestamp_unix {
            signed.extend_from_slice(ts.to_string().as_bytes());
            signed.push(b'.');
        }
        signed.extend_from_slice(body);
        let digest = self.encoded_digest(&signed)?;
        Some(format!("{}{digest}", self.prefix.unwrap_or_default()))
    }

    fn encoded_digest(&self, signed: &[u8]) -> Option<String> {
        let digest = self.digest(signed)?;
        Some(match self.encoding {
            DigestEncoding::Hex => digest.iter().map(|b| format!("{b:02x}")).collect(),
            DigestEncoding::Base64 => {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD.encode(&digest)
            }
        })
    }

    fn digest(&self, signed: &[u8]) -> Option<Vec<u8>> {
        match self.algorithm {
            HmacAlgorithm::Sha256 => {
                let mut mac =
                    <hmac::Hmac<sha2::Sha256> as KeyInit>::new_from_slice(self.secret).ok()?;
                mac.update(signed);
                Some(mac.finalize().into_bytes().to_vec())
            }
            HmacAlgorithm::Sha512 => {
                let mut mac =
                    <hmac::Hmac<sha2::Sha512> as KeyInit>::new_from_slice(self.secret).ok()?;
                mac.update(signed);
                Some(mac.finalize().into_bytes().to_vec())
            }
        }
    }
}

/// Constant-time equality of two secrets. A length difference returns early;
/// a secret's length is not itself sensitive.
pub fn secrets_equal(a: &[u8], b: &[u8]) -> bool {
    bool::from(a.ct_eq(b))
}

/// Constant-time check of an `Authorization` header value against a shared
/// secret, accepting the raw token or `Bearer <token>`. Both forms are always
/// compared, so neither which one matched nor where the first byte differs is
/// observable through timing.
pub fn token_matches(provided: Option<&str>, expected: &str) -> bool {
    let Some(p) = provided else {
        return false;
    };
    let exp = expected.as_bytes();
    let raw = secrets_equal(p.as_bytes(), exp);
    let stripped = p
        .strip_prefix("Bearer ")
        .map(|s| secrets_equal(s.as_bytes(), exp))
        .unwrap_or(false);
    raw | stripped
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use std::collections::HashMap;

    const BODY: &[u8] = br#"{"event":"created","id":7}"#;

    #[test]
    fn a_signed_header_value_verifies_under_the_same_settings() {
        let now = 1_700_000_000;
        for spec in [
            SignatureSpec::new("X-Sig", "k1"),
            SignatureSpec::new("X-Sig", "k1").prefix("sha256="),
            SignatureSpec::new("X-Sig", "k1")
                .algorithm(HmacAlgorithm::Sha512)
                .encoding(DigestEncoding::Base64),
            SignatureSpec::new("X-Sig", "k1").timestamp_header("X-Ts"),
        ] {
            let check = spec.check();
            let ts = check.timestamp_header.map(|_| now);
            let value = check.sign(BODY, ts).unwrap();
            let stamp = now.to_string();
            let headers = |h: &str| match h {
                "X-Sig" => Some(value.as_str()),
                "X-Ts" => Some(stamp.as_str()),
                _ => None,
            };
            assert!(check.verify(headers, BODY, now), "{spec:?}");
            assert!(!check.verify(headers, b"another body", now), "{spec:?}");
        }
    }

    fn mac(algorithm: HmacAlgorithm, secret: &[u8], signed: &[u8]) -> Vec<u8> {
        match algorithm {
            HmacAlgorithm::Sha256 => {
                let mut m = <hmac::Hmac<sha2::Sha256> as KeyInit>::new_from_slice(secret).unwrap();
                m.update(signed);
                m.finalize().into_bytes().to_vec()
            }
            HmacAlgorithm::Sha512 => {
                let mut m = <hmac::Hmac<sha2::Sha512> as KeyInit>::new_from_slice(secret).unwrap();
                m.update(signed);
                m.finalize().into_bytes().to_vec()
            }
        }
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    fn headers(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn verify(spec: &SignatureSpec, h: &HashMap<String, String>, body: &[u8], now: u64) -> bool {
        spec.check()
            .verify(|name| h.get(name).map(String::as_str), body, now)
    }

    #[test]
    fn every_algorithm_and_encoding_verifies_and_rejects_tampering() {
        for algorithm in [HmacAlgorithm::Sha256, HmacAlgorithm::Sha512] {
            for encoding in [DigestEncoding::Hex, DigestEncoding::Base64] {
                let spec = SignatureSpec::new("x-signature", "topsecret")
                    .algorithm(algorithm)
                    .encoding(encoding);
                let digest = mac(algorithm, b"topsecret", BODY);
                let value = match encoding {
                    DigestEncoding::Hex => hex(&digest),
                    DigestEncoding::Base64 => b64(&digest),
                };
                let h = headers(&[("x-signature", &value)]);
                assert!(verify(&spec, &h, BODY, 0), "{algorithm:?}/{encoding:?}");
                assert!(!verify(&spec, &h, b"tampered", 0));
                assert!(!verify(&spec, &HashMap::new(), BODY, 0));
            }
        }
    }

    #[test]
    fn hex_is_case_insensitive_and_base64_is_not() {
        let digest = mac(HmacAlgorithm::Sha256, b"k", BODY);
        let spec = SignatureSpec::new("x-signature", "k");
        let upper = hex(&digest).to_ascii_uppercase();
        assert!(verify(
            &spec,
            &headers(&[("x-signature", &format!("  {upper} "))]),
            BODY,
            0
        ));
        let spec = spec.encoding(DigestEncoding::Base64);
        let swapped: String = b64(&digest)
            .chars()
            .map(|c| {
                if c.is_ascii_lowercase() {
                    c.to_ascii_uppercase()
                } else {
                    c.to_ascii_lowercase()
                }
            })
            .collect();
        assert!(!verify(
            &spec,
            &headers(&[("x-signature", &swapped)]),
            BODY,
            0
        ));
    }

    #[test]
    fn a_wrong_secret_is_rejected() {
        let value = hex(&mac(HmacAlgorithm::Sha256, b"other", BODY));
        let spec = SignatureSpec::new("x-signature", "topsecret");
        assert!(!verify(
            &spec,
            &headers(&[("x-signature", &value)]),
            BODY,
            0
        ));
    }

    #[test]
    fn the_prefix_is_required_when_configured() {
        let value = hex(&mac(HmacAlgorithm::Sha256, b"k", BODY));
        let spec = SignatureSpec::new("x-signature-256", "k").prefix("sha256=");
        let with = headers(&[("x-signature-256", &format!("sha256={value}"))]);
        assert!(verify(&spec, &with, BODY, 0));
        let without = headers(&[("x-signature-256", &value)]);
        assert!(!verify(&spec, &without, BODY, 0));
        let other = headers(&[("x-signature-256", &format!("v1={value}"))]);
        assert!(!verify(&spec, &other, BODY, 0));
    }

    #[test]
    fn a_signed_timestamp_is_accepted_inside_the_tolerance_only() {
        let now = 1_800_000_000u64;
        let spec = SignatureSpec::new("x-signature", "k")
            .timestamp_header("x-timestamp")
            .tolerance_secs(60);
        let signed_at = |ts: u64| {
            let mut content = ts.to_string().into_bytes();
            content.push(b'.');
            content.extend_from_slice(BODY);
            headers(&[
                (
                    "x-signature",
                    &hex(&mac(HmacAlgorithm::Sha256, b"k", &content)),
                ),
                ("x-timestamp", &ts.to_string()),
            ])
        };
        assert!(verify(&spec, &signed_at(now), BODY, now));
        assert!(verify(&spec, &signed_at(now - 60), BODY, now));
        assert!(verify(&spec, &signed_at(now + 60), BODY, now));
        assert!(!verify(&spec, &signed_at(now - 61), BODY, now));
        assert!(!verify(&spec, &signed_at(now + 61), BODY, now));
    }

    #[test]
    fn a_timestamp_that_was_not_signed_or_cannot_be_read_is_rejected() {
        let now = 1_800_000_000u64;
        let spec = SignatureSpec::new("x-signature", "k").timestamp_header("x-timestamp");
        let body_only = hex(&mac(HmacAlgorithm::Sha256, b"k", BODY));
        let unsigned_ts = headers(&[
            ("x-signature", &body_only),
            ("x-timestamp", &now.to_string()),
        ]);
        assert!(!verify(&spec, &unsigned_ts, BODY, now));
        let unreadable = headers(&[("x-signature", &body_only), ("x-timestamp", "soon")]);
        assert!(!verify(&spec, &unreadable, BODY, now));
        let missing = headers(&[("x-signature", &body_only)]);
        assert!(!verify(&spec, &missing, BODY, now));
    }

    #[test]
    fn signed_request_names_the_request_inside_its_window() {
        let spec = SignatureSpec::new("x-signature", "k")
            .timestamp_header("x-timestamp")
            .tolerance_secs(300);
        let h = headers(&[("x-signature", " ABCDEF "), ("x-timestamp", " 1000 ")]);
        let got = spec
            .check()
            .signed_request(|n| h.get(n).map(String::as_str), 1200);
        assert_eq!(
            got,
            Some(SignedRequest {
                signature: "abcdef".into(),
                timestamp: 1000,
                oldest_accepted: 900,
            })
        );
        let base64 = spec.clone().encoding(DigestEncoding::Base64);
        let got = base64
            .check()
            .signed_request(|n| h.get(n).map(String::as_str), 100);
        assert_eq!(
            got,
            Some(SignedRequest {
                signature: "ABCDEF".into(),
                timestamp: 1000,
                oldest_accepted: 0,
            })
        );
        let plain = SignatureSpec::new("x-signature", "k");
        assert_eq!(
            plain
                .check()
                .signed_request(|n| h.get(n).map(String::as_str), 1200),
            None
        );
        let no_sig = headers(&[("x-timestamp", "1000")]);
        assert_eq!(
            spec.check()
                .signed_request(|n| no_sig.get(n).map(String::as_str), 1200),
            None
        );
    }

    #[test]
    fn token_matches_compares_raw_and_bearer_forms() {
        assert!(token_matches(Some("sekret"), "sekret"));
        assert!(token_matches(Some("Bearer sekret"), "sekret"));
        assert!(!token_matches(Some("sekreT"), "sekret"));
        assert!(!token_matches(Some("Bearer sekreT"), "sekret"));
        assert!(!token_matches(Some("sekre"), "sekret"));
        assert!(!token_matches(Some("sekret-and-more"), "sekret"));
        assert!(!token_matches(Some("bearer sekret"), "sekret"));
        assert!(!token_matches(Some(""), "sekret"));
        assert!(!token_matches(None, "sekret"));
    }

    #[test]
    fn secrets_equal_handles_equal_unequal_and_different_lengths() {
        assert!(secrets_equal(b"abc", b"abc"));
        assert!(secrets_equal(b"", b""));
        assert!(!secrets_equal(b"abc", b"abd"));
        assert!(!secrets_equal(b"abc", b"ab"));
        assert!(!secrets_equal(b"", b"a"));
    }

    #[test]
    fn the_spec_reads_its_defaults_and_refuses_unknown_fields() {
        let spec: SignatureSpec =
            serde_json::from_value(serde_json::json!({"header": "x-signature", "secret": "k"}))
                .unwrap();
        assert_eq!(spec, SignatureSpec::new("x-signature", "k"));
        assert!(spec.is_complete());
        assert!(!SignatureSpec::new("", "k").is_complete());
        assert!(!SignatureSpec::new("x-signature", "").is_complete());
        let text = serde_json::to_value(&spec).unwrap();
        assert_eq!(
            text,
            serde_json::json!({
                "header": "x-signature",
                "secret": "k",
                "algorithm": "sha256",
                "encoding": "hex",
                "tolerance_secs": 300,
            })
        );
        assert!(
            serde_json::from_value::<SignatureSpec>(
                serde_json::json!({"header": "h", "secret": "k", "extra": 1})
            )
            .is_err()
        );
    }
}
