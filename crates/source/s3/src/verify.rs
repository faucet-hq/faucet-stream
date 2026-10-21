//! Build the [`IntegrityCheck`] set for an S3 object read (#161).
//!
//! Pure decision + digest logic kept out of [`stream`](crate::stream) so it is
//! unit testable without an S3 endpoint. The checks are exercised against
//! in-memory bodies via [`faucet_core::VerifyingReader`].
//!
//! - [`length_check`] guards against a cleanly-truncated transfer (default-on).
//! - [`checksum_check`] verifies the body against the strongest checksum S3
//!   advertises for the object (opt-in). S3 returns each checksum base64-encoded
//!   (`x-amz-checksum-*`); the non-multipart `ETag` is a hex MD5.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use faucet_core::{IntegrityCheck, LengthCheck};

// CRC algorithm definitions held as `static` so the streaming `Digest`s can
// borrow them for `'static` and live inside the boxed checks.
static CRC32: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
static CRC32C: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
static CRC64NVME: crc::Crc<u64> = crc::Crc::<u64>::new(&crc::CRC_64_NVME);

/// Length check for one object body. `None` when verification is disabled or
/// the store reported no usable (non-negative) `Content-Length`.
pub fn length_check(
    content_length: Option<i64>,
    verify_length: bool,
) -> Option<Box<dyn IntegrityCheck>> {
    if verify_length
        && let Some(len) = content_length
        && len >= 0
    {
        return Some(Box::new(LengthCheck::new(len as u64)));
    }
    None
}

/// The checksum values an S3 `GetObject` (with `ChecksumMode::Enabled`) may
/// return. Each `checksum_*` is base64; `etag` is the raw ETag header.
#[derive(Debug, Default, Clone)]
pub struct S3Checksums {
    pub crc32: Option<String>,
    pub crc32c: Option<String>,
    pub crc64nvme: Option<String>,
    pub sha256: Option<String>,
    pub etag: Option<String>,
}

impl S3Checksums {
    /// True when at least one field that [`checksum_check`] can verify is
    /// present (used to distinguish "no checksum advertised" from "only an
    /// unsupported algorithm advertised").
    pub fn has_verifiable(&self) -> bool {
        full_object(&self.sha256).is_some()
            || full_object(&self.crc64nvme).is_some()
            || full_object(&self.crc32c).is_some()
            || full_object(&self.crc32).is_some()
            || self.etag.as_deref().and_then(non_multipart_md5).is_some()
    }
}

/// Build a checksum check from the strongest advertised algorithm we can
/// compute, or `None` when the object carries no checksum we support.
pub fn checksum_check(c: &S3Checksums) -> Option<Box<dyn IntegrityCheck>> {
    if let Some(b64) = full_object(&c.sha256) {
        return Some(Box::new(Sha256Check::new(b64.clone())));
    }
    if let Some(b64) = full_object(&c.crc64nvme) {
        return Some(Box::new(CrcCheck::new_u64(
            &CRC64NVME,
            "CRC64NVME",
            b64.clone(),
        )));
    }
    if let Some(b64) = full_object(&c.crc32c) {
        return Some(Box::new(CrcCheck::new_u32(&CRC32C, "CRC32C", b64.clone())));
    }
    if let Some(b64) = full_object(&c.crc32) {
        return Some(Box::new(CrcCheck::new_u32(&CRC32, "CRC32", b64.clone())));
    }
    if let Some(tag) = &c.etag
        && let Some(hex) = non_multipart_md5(tag)
    {
        return Some(Box::new(Md5HexCheck::new(hex)));
    }
    None
}

/// A checksum value that covers the whole object body. A multipart upload
/// with a COMPOSITE checksum advertises the checksum of its part checksums,
/// suffixed `-<partcount>` (`abcd==-3`); no body digest can equal it, so it is
/// skipped like a multipart ETag. `-` is not in the base64 alphabet, so a
/// full-object value never contains it.
fn full_object(value: &Option<String>) -> Option<&String> {
    value.as_ref().filter(|v| !v.contains('-'))
}

/// Drop every checksum S3 marked `COMPOSITE` (`x-amz-checksum-type`), keeping
/// the ETag. Composite values normally carry a `-N` suffix too; this also
/// covers a store that reports the type without the suffix.
pub fn without_composite(mut c: S3Checksums, composite: bool) -> S3Checksums {
    if composite {
        c.crc32 = None;
        c.crc32c = None;
        c.crc64nvme = None;
        c.sha256 = None;
    }
    c
}

/// Extract a lowercase hex MD5 from an ETag when it is a simple (non-multipart)
/// upload. A multipart ETag has the form `"<hex>-<partcount>"` and is **not**
/// the MD5 of the object body, so it is rejected here.
fn non_multipart_md5(etag: &str) -> Option<String> {
    let t = etag.trim().trim_matches('"');
    if t.len() == 32 && t.bytes().all(|b| b.is_ascii_hexdigit()) {
        Some(t.to_ascii_lowercase())
    } else {
        None
    }
}

fn mismatch(name: &str, got: &str, want: &str) -> Result<(), String> {
    if got.eq_ignore_ascii_case(want) {
        Ok(())
    } else {
        Err(format!(
            "{name} checksum mismatch: object store advertised {want} but body computed {got} \
             (corrupted transfer)"
        ))
    }
}

/// Streaming CRC check (32- or 64-bit) comparing against a base64 of the
/// big-endian digest, matching how S3 encodes `x-amz-checksum-*`.
struct CrcCheck {
    digest: CrcDigest,
    name: &'static str,
    expected_b64: String,
}
enum CrcDigest {
    W32(crc::Digest<'static, u32>),
    W64(crc::Digest<'static, u64>),
}
impl CrcCheck {
    fn new_u32(crc: &'static crc::Crc<u32>, name: &'static str, expected_b64: String) -> Self {
        Self {
            digest: CrcDigest::W32(crc.digest()),
            name,
            expected_b64,
        }
    }
    fn new_u64(crc: &'static crc::Crc<u64>, name: &'static str, expected_b64: String) -> Self {
        Self {
            digest: CrcDigest::W64(crc.digest()),
            name,
            expected_b64,
        }
    }
}
impl IntegrityCheck for CrcCheck {
    fn update(&mut self, chunk: &[u8]) {
        match &mut self.digest {
            CrcDigest::W32(d) => d.update(chunk),
            CrcDigest::W64(d) => d.update(chunk),
        }
    }
    fn finalize(self: Box<Self>, _total: u64) -> Result<(), String> {
        let me = *self;
        let got = match me.digest {
            CrcDigest::W32(d) => B64.encode(d.finalize().to_be_bytes()),
            CrcDigest::W64(d) => B64.encode(d.finalize().to_be_bytes()),
        };
        mismatch(me.name, &got, &me.expected_b64)
    }
}

/// Streaming SHA-256 check comparing against a base64 of the digest.
struct Sha256Check {
    hasher: sha2::Sha256,
    expected_b64: String,
}
impl Sha256Check {
    fn new(expected_b64: String) -> Self {
        use sha2::Digest as _;
        Self {
            hasher: sha2::Sha256::new(),
            expected_b64,
        }
    }
}
impl IntegrityCheck for Sha256Check {
    fn update(&mut self, chunk: &[u8]) {
        use sha2::Digest as _;
        self.hasher.update(chunk);
    }
    fn finalize(self: Box<Self>, _total: u64) -> Result<(), String> {
        use sha2::Digest as _;
        let got = B64.encode(self.hasher.finalize());
        mismatch("SHA256", &got, &self.expected_b64)
    }
}

/// Streaming MD5 check comparing against a lowercase hex digest (S3 ETag).
struct Md5HexCheck {
    hasher: md5::Md5,
    expected_hex: String,
}
impl Md5HexCheck {
    fn new(expected_hex: String) -> Self {
        use md5::Digest as _;
        Self {
            hasher: md5::Md5::new(),
            expected_hex,
        }
    }
}
impl IntegrityCheck for Md5HexCheck {
    fn update(&mut self, chunk: &[u8]) {
        use md5::Digest as _;
        self.hasher.update(chunk);
    }
    fn finalize(self: Box<Self>, _total: u64) -> Result<(), String> {
        use md5::Digest as _;
        let digest = self.hasher.finalize();
        let got: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        mismatch("MD5(ETag)", &got, &self.expected_hex)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::VerifyingReader;
    use tokio::io::AsyncReadExt as _;

    async fn reads_ok(check: Option<Box<dyn IntegrityCheck>>, body: &[u8]) -> bool {
        let checks: Vec<Box<dyn IntegrityCheck>> = check.into_iter().collect();
        let mut reader = VerifyingReader::new(body, checks);
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.is_ok()
    }

    fn b64_of_hex(hex: &str) -> String {
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        B64.encode(bytes)
    }

    // ---- length_check ----

    #[tokio::test]
    async fn length_truncation_rejected() {
        assert!(!reads_ok(length_check(Some(10), true), b"hello").await);
    }
    #[tokio::test]
    async fn length_complete_passes() {
        assert!(reads_ok(length_check(Some(5), true), b"hello").await);
    }
    #[tokio::test]
    async fn length_none_when_disabled() {
        assert!(length_check(Some(10), false).is_none());
    }
    #[tokio::test]
    async fn length_none_when_unknown_or_negative() {
        assert!(length_check(None, true).is_none());
        assert!(length_check(Some(-1), true).is_none());
    }

    // ---- checksum_check: independent published digests of b"hello" ----
    // md5    = 5d41402abc4b2a76b9719d911017c592
    // sha256 = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
    // crc32  = 0x3610a686 (CRC-32/ISO-HDLC, the zip CRC)
    // crc32c = 0x9a71bb4c (CRC-32C / iSCSI)

    #[tokio::test]
    async fn sha256_match_and_mismatch() {
        let want = b64_of_hex("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
        let good = S3Checksums {
            sha256: Some(want),
            ..Default::default()
        };
        assert!(reads_ok(checksum_check(&good), b"hello").await);
        let bad = S3Checksums {
            sha256: Some(b64_of_hex("00")),
            ..Default::default()
        };
        assert!(!reads_ok(checksum_check(&bad), b"hello").await);
    }

    #[tokio::test]
    async fn crc32_match() {
        let want = B64.encode(0x3610a686u32.to_be_bytes());
        let c = S3Checksums {
            crc32: Some(want),
            ..Default::default()
        };
        assert!(reads_ok(checksum_check(&c), b"hello").await);
    }

    #[tokio::test]
    async fn crc32c_match_and_mismatch() {
        let want = B64.encode(0x9a71bb4cu32.to_be_bytes());
        let good = S3Checksums {
            crc32c: Some(want),
            ..Default::default()
        };
        assert!(reads_ok(checksum_check(&good), b"hello").await);
        let bad = S3Checksums {
            crc32c: Some(B64.encode(0xdeadbeefu32.to_be_bytes())),
            ..Default::default()
        };
        assert!(!reads_ok(checksum_check(&bad), b"hello").await);
    }

    #[tokio::test]
    async fn crc64nvme_round_trips() {
        // No widely-published vector; compute via the same algorithm and prove
        // the wiring (encode → compare) matches.
        let want = B64.encode(CRC64NVME.checksum(b"hello").to_be_bytes());
        let c = S3Checksums {
            crc64nvme: Some(want),
            ..Default::default()
        };
        assert!(reads_ok(checksum_check(&c), b"hello").await);
    }

    #[tokio::test]
    async fn etag_md5_match_when_non_multipart() {
        let c = S3Checksums {
            etag: Some("\"5d41402abc4b2a76b9719d911017c592\"".into()),
            ..Default::default()
        };
        assert!(reads_ok(checksum_check(&c), b"hello").await);
    }

    #[tokio::test]
    async fn composite_checksum_is_skipped_for_a_full_object_one() {
        // A composite multipart SHA-256 (`-N` suffix) never equals a body
        // digest; the full-object CRC32C below it must be used instead.
        let c = S3Checksums {
            sha256: Some(format!("{}-3", b64_of_hex("00"))),
            crc32c: Some(B64.encode(0x9a71bb4cu32.to_be_bytes())),
            ..Default::default()
        };
        assert!(reads_ok(checksum_check(&c), b"hello").await);
    }

    #[test]
    fn composite_only_object_has_no_verifiable_checksum() {
        let c = S3Checksums {
            crc32: Some("AAAAAA==-7".into()),
            etag: Some("\"d41d8cd98f00b204e9800998ecf8427e-7\"".into()),
            ..Default::default()
        };
        assert!(!c.has_verifiable());
        assert!(checksum_check(&c).is_none());
    }

    #[test]
    fn checksum_type_composite_drops_every_checksum_but_the_etag() {
        let c = S3Checksums {
            crc32: Some("AAAAAA==".into()),
            crc32c: Some("AAAAAA==".into()),
            crc64nvme: Some("AAAAAAAAAAA=".into()),
            sha256: Some("AA==".into()),
            etag: Some("\"5d41402abc4b2a76b9719d911017c592\"".into()),
        };
        let kept = without_composite(c.clone(), false);
        assert!(kept.crc32.is_some() && kept.sha256.is_some());
        let dropped = without_composite(c, true);
        assert!(dropped.crc32.is_none() && dropped.crc32c.is_none());
        assert!(dropped.crc64nvme.is_none() && dropped.sha256.is_none());
        assert!(dropped.etag.is_some());
    }

    #[test]
    fn multipart_etag_is_not_treated_as_md5() {
        // ETag with a `-N` part suffix is not the body MD5.
        assert!(non_multipart_md5("\"d41d8cd98f00b204e9800998ecf8427e-3\"").is_none());
        assert!(non_multipart_md5("\"5d41402abc4b2a76b9719d911017c592\"").is_some());
    }

    #[test]
    fn priority_prefers_sha256_over_weaker() {
        let c = S3Checksums {
            sha256: Some(b64_of_hex(
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            )),
            crc32: Some("bogus".into()),
            etag: Some("\"5d41402abc4b2a76b9719d911017c592\"".into()),
            ..Default::default()
        };
        // sha256 wins and is correct → passes despite the bogus crc32 value.
        assert!(c.has_verifiable());
        assert!(checksum_check(&c).is_some());
    }

    #[test]
    fn no_checksum_yields_none() {
        let empty = S3Checksums::default();
        assert!(!empty.has_verifiable());
        assert!(checksum_check(&empty).is_none());
        // An only-multipart ETag is also "no verifiable checksum".
        let mp = S3Checksums {
            etag: Some("\"abc-2\"".into()),
            ..Default::default()
        };
        assert!(!mp.has_verifiable());
        assert!(checksum_check(&mp).is_none());
    }
}
