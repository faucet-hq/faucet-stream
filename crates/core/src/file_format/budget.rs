//! A ceiling on what one object or record batch may decode to.
//!
//! `max_object_bytes` bounds the bytes a source reads; a format can still
//! decode a small input into far more memory (an Avro array of zero-width
//! items, a workbook declaring more shared strings than it holds). A
//! [`DecodeBudget`] is charged as a decoder produces output and refuses once
//! the output passes `max_decoded_bytes`. It applies to one materialised
//! object or one record batch at a time, never across a whole streamed file.

use crate::error::FaucetError;
use serde_json::Value;

/// How many decoded bytes a source allows per byte of `max_object_bytes`
/// when `max_decoded_bytes` is not set.
pub const DECODED_BYTES_PER_OBJECT_BYTE: u64 = 64;

/// The most a default `max_decoded_bytes` reaches (16 GiB).
pub const MAX_DEFAULT_DECODED_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// The `max_decoded_bytes` a source uses when the setting is not given:
/// 64 × `max_object_bytes`, at most 16 GiB.
pub fn default_max_decoded_bytes(max_object_bytes: u64) -> u64 {
    max_object_bytes
        .saturating_mul(DECODED_BYTES_PER_OBJECT_BYTE)
        .min(MAX_DEFAULT_DECODED_BYTES)
}

/// The `max_decoded_bytes` a source passes on: the configured value, else
/// [`default_max_decoded_bytes`].
pub fn effective_max_decoded_bytes(configured: Option<u64>, max_object_bytes: u64) -> u64 {
    configured.unwrap_or_else(|| default_max_decoded_bytes(max_object_bytes))
}

/// Decoded bytes charged against `max_decoded_bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeBudget {
    limit: u64,
    used: u64,
}

impl Default for DecodeBudget {
    fn default() -> Self {
        Self::new(MAX_DEFAULT_DECODED_BYTES)
    }
}

impl DecodeBudget {
    /// A budget of `limit` decoded bytes.
    pub fn new(limit: u64) -> Self {
        Self { limit, used: 0 }
    }

    /// The ceiling, in bytes.
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Bytes charged since the budget was made or last reset.
    pub fn used(&self) -> u64 {
        self.used
    }

    /// Charge `bytes` of output from `what`, failing once the total passes
    /// the limit.
    pub fn charge(&mut self, bytes: u64, what: &str) -> Result<(), FaucetError> {
        self.used = self.used.saturating_add(bytes);
        if self.used > self.limit {
            return Err(exceeded(what, self.limit));
        }
        Ok(())
    }

    /// Charge the in-memory size of a decoded record (see [`decoded_size`]).
    pub fn charge_value(&mut self, v: &Value, what: &str) -> Result<(), FaucetError> {
        self.charge(decoded_size(v), what)
    }

    /// Start a new object or batch.
    pub fn reset(&mut self) {
        self.used = 0;
    }
}

/// The error for output past `max_decoded_bytes`.
pub fn exceeded(what: &str, limit: u64) -> FaucetError {
    FaucetError::Source(format!(
        "{what} decodes to more than `max_decoded_bytes` ({limit} bytes); raise \
         `max_decoded_bytes` to read it"
    ))
}

/// Whether `e` is the error [`exceeded`] builds.
pub(crate) fn is_exceeded(e: &FaucetError) -> bool {
    matches!(e, FaucetError::Source(m) if m.contains("decodes to more than `max_decoded_bytes`"))
}

/// Approximate memory a decoded record holds: one [`Value`] per node plus
/// its string and key bytes.
pub fn decoded_size(v: &Value) -> u64 {
    let node = std::mem::size_of::<Value>() as u64;
    match v {
        Value::String(s) => node + s.len() as u64,
        Value::Array(items) => items
            .iter()
            .fold(node, |acc, i| acc.saturating_add(decoded_size(i))),
        Value::Object(m) => m.iter().fold(node, |acc, (k, i)| {
            acc.saturating_add(k.len() as u64)
                .saturating_add(decoded_size(i))
        }),
        _ => node,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_default_is_a_multiple_of_the_object_limit_with_a_ceiling() {
        assert_eq!(default_max_decoded_bytes(1024), 64 * 1024);
        assert_eq!(
            default_max_decoded_bytes(super::super::DEFAULT_MAX_OBJECT_BYTES),
            MAX_DEFAULT_DECODED_BYTES
        );
        assert_eq!(
            default_max_decoded_bytes(u64::MAX),
            MAX_DEFAULT_DECODED_BYTES
        );
        assert_eq!(effective_max_decoded_bytes(Some(7), 1024), 7);
        assert_eq!(effective_max_decoded_bytes(None, 1024), 64 * 1024);
    }

    #[test]
    fn charging_past_the_limit_names_the_setting() {
        let mut b = DecodeBudget::new(10);
        b.charge(10, "x").unwrap();
        assert_eq!(b.used(), 10);
        let err = b.charge(1, "'f.avro'").unwrap_err();
        assert!(matches!(err, FaucetError::Source(_)));
        assert!(err.to_string().contains("max_decoded_bytes"), "{err}");
        assert!(err.to_string().contains("'f.avro'"), "{err}");
        b.reset();
        assert_eq!(b.used(), 0);
        assert_eq!(b.limit(), 10);
        assert!(is_exceeded(&b.charge(u64::MAX, "x").unwrap_err()));
        assert!(!is_exceeded(&FaucetError::Source("other".into())));
    }

    #[test]
    fn the_default_budget_is_the_ceiling() {
        assert_eq!(DecodeBudget::default().limit(), MAX_DEFAULT_DECODED_BYTES);
    }

    #[test]
    fn decoded_size_counts_nodes_strings_and_keys() {
        let node = std::mem::size_of::<Value>() as u64;
        assert_eq!(decoded_size(&json!(null)), node);
        assert_eq!(decoded_size(&json!("abc")), node + 3);
        assert_eq!(decoded_size(&json!([1, 2])), 3 * node);
        assert_eq!(decoded_size(&json!({"ab": true})), 2 * node + 2);
        let mut b = DecodeBudget::new(node);
        b.charge_value(&json!(1), "x").unwrap();
        b.charge_value(&json!(1), "x").unwrap_err();
    }
}
