//! Offset/limit pagination.

use faucet_core::FaucetError;
use jsonpath_rust::JsonPath;
use serde_json::Value;
use std::collections::HashMap;

pub fn apply_params(
    params: &mut HashMap<String, String>,
    offset_param: &str,
    limit_param: &str,
    offset: usize,
    limit: usize,
) {
    params.insert(offset_param.to_string(), offset.to_string());
    params.insert(limit_param.to_string(), limit.to_string());
}

/// Advance by `record_count` (the rows the server returned). Paging stops on
/// an empty page or once the offset reaches the total at `total_path`. Without
/// a readable total a short page does **not** stop paging: the server may cap
/// the page below `limit`, and stopping there would end the run with rows
/// unread (API-15). The identical-page guard in the caller ends a walk over a
/// server that ignores the offset.
pub fn advance(
    body: &Value,
    offset: &mut usize,
    record_count: usize,
    _limit: usize,
    total_path: Option<&str>,
) -> Result<bool, FaucetError> {
    // A zero-record page is always the end: the offset would not advance, so
    // honouring `total` here would re-issue the identical request forever
    // (#78/#6). Stop regardless of what `total_path` reports.
    if record_count == 0 {
        return Ok(false);
    }
    *offset += record_count;
    if let Some(tp) = total_path {
        let results = body
            .query(tp)
            .map_err(|e| FaucetError::JsonPath(format!("{e}")))?;
        if let Some(total_val) = results.first() {
            match total_val.as_u64() {
                Some(n) => return Ok(*offset < n as usize),
                None => tracing::warn!(
                    "total_path '{tp}' resolved to non-numeric value {total_val}; \
                     paging until an empty page"
                ),
            }
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn zero_record_page_stops_even_when_offset_below_total() {
        // Regression for #78/#6: a mid-stream empty page with `total_path` set
        // and offset < total used to leave the offset unchanged and re-issue
        // the identical request forever. It must stop instead.
        let body = json!({ "total": 100 });
        let mut offset = 50usize;
        let has_next = advance(&body, &mut offset, 0, 20, Some("$.total")).unwrap();
        assert!(!has_next, "a zero-record page must stop pagination");
    }

    #[test]
    fn nonempty_page_below_total_continues() {
        let body = json!({ "total": 100 });
        let mut offset = 20usize;
        let has_next = advance(&body, &mut offset, 20, 20, Some("$.total")).unwrap();
        assert!(has_next, "more records remain below total");
        assert_eq!(offset, 40);
    }

    #[test]
    fn reaching_total_stops() {
        let body = json!({ "total": 40 });
        let mut offset = 20usize;
        let has_next = advance(&body, &mut offset, 20, 20, Some("$.total")).unwrap();
        assert!(!has_next, "offset reached total");
    }

    #[test]
    fn a_short_page_without_total_continues() {
        // API-15: a short page used to end paging, losing rows when the server
        // caps the page below `limit`.
        let mut offset = 0usize;
        assert!(advance(&json!({}), &mut offset, 10, 100, None).unwrap());
        assert_eq!(offset, 10);
        let mut offset = 0usize;
        assert!(advance(&json!({"total": "n/a"}), &mut offset, 10, 100, Some("$.total")).unwrap());
        assert!(advance(&json!({}), &mut offset, 10, 100, Some("$.total")).unwrap());
    }
}
