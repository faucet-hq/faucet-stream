//! API key header authentication.

use faucet_core::FaucetError;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

pub fn apply(headers: &mut HeaderMap, header: &str, value: &str) -> Result<(), FaucetError> {
    let name = HeaderName::from_bytes(header.as_bytes())
        .map_err(|e| FaucetError::Auth(format!("invalid header name '{header}': {e}")))?;
    let mut val = HeaderValue::from_str(value)
        .map_err(|e| FaucetError::Auth(format!("invalid header value: {e}")))?;
    val.set_sensitive(true);
    headers.insert(name, val);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_key_is_marked_sensitive() {
        let mut headers = HeaderMap::new();
        apply(&mut headers, "X-Api-Key", "dummy-key").unwrap();
        let v = headers.get("x-api-key").unwrap();
        assert_eq!(v, "dummy-key");
        assert!(v.is_sensitive());
        assert!(apply(&mut headers, "bad name", "v").is_err());
        assert!(apply(&mut headers, "x-ok", "bad\nvalue").is_err());
    }
}
