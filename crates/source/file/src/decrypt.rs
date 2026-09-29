//! Line-by-line decryption of JSON Lines sealed per record.

use faucet_core::{CompiledEncryption, FaucetError};

/// Decrypt every non-empty line (base64 of a sealed payload) back into
/// newline-terminated plaintext.
pub(crate) fn lines(raw: &[u8], enc: &CompiledEncryption) -> Result<Vec<u8>, FaucetError> {
    use base64::Engine as _;
    let text = std::str::from_utf8(raw)
        .map_err(|e| FaucetError::Source(format!("encrypted lines are not UTF-8: {e}")))?;
    let mut out = Vec::with_capacity(raw.len());
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let sealed = base64::engine::general_purpose::STANDARD
            .decode(line)
            .ok()
            .filter(|b| faucet_core::encryption::is_encrypted(b))
            .ok_or_else(|| {
                FaucetError::Source(format!(
                    "line {} is not encrypted, but `encryption` is set — refusing to read \
                     plaintext as if it were authenticated",
                    i + 1
                ))
            })?;
        out.extend_from_slice(&enc.decrypt(&sealed)?);
        out.push(b'\n');
    }
    Ok(out)
}
