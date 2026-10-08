//! Decryption of JSON Lines sealed per record.

use faucet_core::{CompiledEncryption, FaucetError};

/// Decrypt every record line back into newline-terminated plaintext,
/// verifying the file whole (header, trailer, count and digest) when it was
/// written with whole-file integrity.
pub(crate) fn lines(raw: &[u8], enc: &CompiledEncryption) -> Result<Vec<u8>, FaucetError> {
    faucet_common_file::sealed_lines::plaintext(raw, enc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;

    #[test]
    fn blank_lines_between_sealed_records_are_skipped() {
        let spec: faucet_core::EncryptionSpec =
            serde_json::from_value(serde_json::json!({"key": "k"})).unwrap();
        let enc = CompiledEncryption::compile(&spec).unwrap();
        let line = base64::engine::general_purpose::STANDARD.encode(enc.encrypt(b"{\"a\":1}"));
        let raw = format!("{line}\n\n  \n{line}\n");
        assert_eq!(
            lines(raw.as_bytes(), &enc).unwrap(),
            b"{\"a\":1}\n{\"a\":1}\n"
        );
    }
}
