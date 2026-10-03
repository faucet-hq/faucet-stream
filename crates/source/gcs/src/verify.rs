//! Build the [`IntegrityCheck`] set for a GCS object read (#161).
//!
//! Pure decision + digest logic kept out of [`stream`](crate::stream) so it is
//! unit testable without a GCS endpoint. Checks run against in-memory bodies
//! via [`faucet_core::VerifyingReader`].
//!
//! GCS reports `crc32c` as a `u32` and `md5_hash` as raw bytes (16 bytes), so
//! the checks compare numerically / byte-for-byte (no base64, unlike S3).
//!
//! GCS-specific subtlety: an object stored with a non-empty `Content-Encoding`
//! (e.g. `gzip`) may be **decompressively transcoded** on read, so received
//! bytes match neither the stored `size` nor the stored checksum. Both the
//! length and checksum checks are therefore skipped when `content_encoding` is
//! non-empty.

use faucet_core::{IntegrityCheck, LengthCheck};

static CRC32C: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

/// Length check for one object body. `None` when disabled, the object is
/// transcoded (non-empty `content_encoding`), or `size` is negative.
pub fn length_check(
    size: i64,
    content_encoding: &str,
    verify_length: bool,
) -> Option<Box<dyn IntegrityCheck>> {
    if verify_length && content_encoding.is_empty() && size >= 0 {
        Some(Box::new(LengthCheck::new(size as u64)))
    } else {
        None
    }
}

/// Checksum check from the GCS-reported `crc32c` (preferred) or `md5_hash`.
/// `None` when the object is transcoded or carries no usable checksum.
pub fn checksum_check(
    crc32c: Option<u32>,
    md5_hash: &[u8],
    content_encoding: &str,
) -> Option<Box<dyn IntegrityCheck>> {
    if !content_encoding.is_empty() {
        return None;
    }
    if let Some(expected) = crc32c {
        return Some(Box::new(Crc32cNumCheck::new(expected)));
    }
    if md5_hash.len() == 16 {
        return Some(Box::new(Md5RawCheck::new(md5_hash.to_vec())));
    }
    None
}

/// Streaming CRC-32C check comparing against the numeric value GCS reports.
struct Crc32cNumCheck {
    digest: crc::Digest<'static, u32>,
    expected: u32,
}
impl Crc32cNumCheck {
    fn new(expected: u32) -> Self {
        Self {
            digest: CRC32C.digest(),
            expected,
        }
    }
}
impl IntegrityCheck for Crc32cNumCheck {
    fn update(&mut self, chunk: &[u8]) {
        self.digest.update(chunk);
    }
    fn finalize(self: Box<Self>, _total: u64) -> Result<(), String> {
        let me = *self;
        let got = me.digest.finalize();
        if got == me.expected {
            Ok(())
        } else {
            Err(format!(
                "CRC32C checksum mismatch: GCS reported {:#010x} but body computed {:#010x} \
                 (corrupted transfer)",
                me.expected, got
            ))
        }
    }
}

/// Streaming MD5 check comparing against the raw 16-byte digest GCS reports.
struct Md5RawCheck {
    hasher: md5::Md5,
    expected: Vec<u8>,
}
impl Md5RawCheck {
    fn new(expected: Vec<u8>) -> Self {
        use md5::Digest as _;
        Self {
            hasher: md5::Md5::new(),
            expected,
        }
    }
}
impl IntegrityCheck for Md5RawCheck {
    fn update(&mut self, chunk: &[u8]) {
        use md5::Digest as _;
        self.hasher.update(chunk);
    }
    fn finalize(self: Box<Self>, _total: u64) -> Result<(), String> {
        use md5::Digest as _;
        let got = self.hasher.finalize();
        if got[..] == self.expected[..] {
            Ok(())
        } else {
            Err(
                "MD5 checksum mismatch: body does not match the digest GCS reported \
                 (corrupted transfer)"
                    .to_string(),
            )
        }
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

    fn hex_bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    // ---- length_check ----

    #[tokio::test]
    async fn length_truncation_rejected() {
        assert!(!reads_ok(length_check(10, "", true), b"hello").await);
    }
    #[tokio::test]
    async fn length_complete_passes() {
        assert!(reads_ok(length_check(5, "", true), b"hello").await);
    }
    #[test]
    fn length_skipped_when_transcoded_disabled_or_negative() {
        assert!(length_check(10, "gzip", true).is_none());
        assert!(length_check(10, "", false).is_none());
        assert!(length_check(-1, "", true).is_none());
    }

    // ---- checksum_check: crc32c("hello") = 0x9a71bb4c, md5 = 5d41402a... ----

    #[tokio::test]
    async fn crc32c_match_and_mismatch() {
        assert!(reads_ok(checksum_check(Some(0x9a71bb4c), &[], ""), b"hello").await);
        assert!(!reads_ok(checksum_check(Some(0xdeadbeef), &[], ""), b"hello").await);
    }

    #[tokio::test]
    async fn md5_match_when_no_crc32c() {
        let md5 = hex_bytes("5d41402abc4b2a76b9719d911017c592");
        assert!(reads_ok(checksum_check(None, &md5, ""), b"hello").await);
        let wrong = hex_bytes("00000000000000000000000000000000");
        assert!(!reads_ok(checksum_check(None, &wrong, ""), b"hello").await);
    }

    #[test]
    fn checksum_prefers_crc32c_over_md5() {
        let md5 = hex_bytes("5d41402abc4b2a76b9719d911017c592");
        // Both present → crc32c is chosen (we can't easily assert which from
        // the trait object, but the call must return Some and not panic).
        assert!(checksum_check(Some(0x9a71bb4c), &md5, "").is_some());
    }

    #[test]
    fn checksum_skipped_when_transcoded_or_absent() {
        // transcoded object → skip even though a checksum is present
        assert!(checksum_check(Some(0x9a71bb4c), &[], "gzip").is_none());
        // no crc32c and no (16-byte) md5 → none
        assert!(checksum_check(None, &[], "").is_none());
        assert!(checksum_check(None, &[1, 2, 3], "").is_none());
    }
}
