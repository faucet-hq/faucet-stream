//! Private (0600) temp files for Singer `--config` / `--catalog` / `--state`.

use faucet_core::Value;

/// Write `value` as JSON to a private (0600) temp file, deleted when the
/// returned handle drops. The error is a human-readable reason; callers wrap
/// it in the `FaucetError` variant for their side.
pub fn write_private_json(kind: &str, value: &Value) -> Result<tempfile::NamedTempFile, String> {
    use std::io::Write;
    let mut file = tempfile::Builder::new()
        .prefix(&format!("faucet-singer-{kind}-"))
        .suffix(".json")
        .tempfile()
        .map_err(|e| format!("failed to create {kind} temp file: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|e| format!("failed to chmod {kind} temp file: {e}"))?;
    }
    let bytes =
        serde_json::to_vec(value).map_err(|e| format!("failed to serialize {kind}: {e}"))?;
    file.write_all(&bytes)
        .and_then(|_| file.flush())
        .map_err(|e| format!("failed to write {kind} temp file: {e}"))?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn write_private_json_is_private_and_valid_json() {
        let f = write_private_json("config", &json!({"a": 1})).unwrap();
        let contents = std::fs::read_to_string(f.path()).unwrap();
        assert_eq!(contents, r#"{"a":1}"#);
        assert!(
            f.path()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("faucet-singer-config-")
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(f.path()).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }
}
