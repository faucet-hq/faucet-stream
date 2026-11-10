//! Custom header-based authentication.

use faucet_core::FaucetError;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::collections::HashMap;

pub fn apply(headers: &mut HeaderMap, custom: &HashMap<String, String>) -> Result<(), FaucetError> {
    for (name, value) in custom {
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| FaucetError::Auth(format!("invalid custom header name {name:?}: {e}")))?;
        let mut header_value = HeaderValue::from_str(value).map_err(|e| {
            FaucetError::Auth(format!("invalid custom header value for {name:?}: {e}"))
        })?;
        header_value.set_sensitive(true);
        headers.insert(header_name, header_value);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    #[test]
    fn apply_merges_custom_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("existing", HeaderValue::from_static("keep"));

        let custom: HashMap<String, String> = [
            ("x-custom".to_string(), "val1".to_string()),
            ("x-another".to_string(), "val2".to_string()),
        ]
        .into_iter()
        .collect();

        apply(&mut headers, &custom).unwrap();

        assert_eq!(headers.get("existing").unwrap(), "keep");
        assert_eq!(headers.get("x-custom").unwrap(), "val1");
        assert_eq!(headers.get("x-another").unwrap(), "val2");
        assert!(headers.get("x-custom").unwrap().is_sensitive());
        assert!(!headers.get("existing").unwrap().is_sensitive());
    }

    #[test]
    fn apply_overwrites_conflicting_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-key", HeaderValue::from_static("old"));

        let custom: HashMap<String, String> = [("x-key".to_string(), "new".to_string())]
            .into_iter()
            .collect();

        apply(&mut headers, &custom).unwrap();
        assert_eq!(headers.get("x-key").unwrap(), "new");
    }

    #[test]
    fn apply_empty_custom_is_noop() {
        let mut headers = HeaderMap::new();
        headers.insert("existing", HeaderValue::from_static("value"));

        apply(&mut headers, &HashMap::new()).unwrap();
        assert_eq!(headers.len(), 1);
        assert_eq!(headers.get("existing").unwrap(), "value");
    }

    #[test]
    fn apply_rejects_invalid_header_name() {
        let mut headers = HeaderMap::new();
        let custom: HashMap<String, String> = [("bad name".to_string(), "v".to_string())]
            .into_iter()
            .collect();
        assert!(apply(&mut headers, &custom).is_err());
    }

    #[test]
    fn apply_rejects_invalid_header_value() {
        let mut headers = HeaderMap::new();
        let custom: HashMap<String, String> = [("x-good".to_string(), "bad\nvalue".to_string())]
            .into_iter()
            .collect();
        assert!(apply(&mut headers, &custom).is_err());
    }
}
