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
mod tests;
