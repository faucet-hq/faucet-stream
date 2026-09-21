//! Stable content fingerprint for idempotency replay-vs-conflict detection. A
//! key replayed with the *same* merged config returns the existing run; reused
//! with a *different* config is a 409. The hash is order-independent for object
//! keys (canonical JSON) and stable across process restarts (sha256), so the
//! Phase 5 SQL backends can store and compare it unchanged.

use serde_json::Value;
use sha2::{Digest, Sha256};

/// Fingerprint of a merged, resolved config plus its pipeline `name`.
pub fn fingerprint(merged: &Value, name: Option<&str>) -> String {
    let mut canon = String::new();
    write_canonical(merged, &mut canon);
    let mut hasher = Sha256::new();
    hasher.update(name.unwrap_or("").as_bytes());
    hasher.update([0u8]); // separator so name|config can't collide across the boundary
    hasher.update(canon.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Fold the run-affecting request fields into the config fingerprint.
///
/// The idempotency key identifies a *request*, not just a config: a key
/// replayed with the same merged config but a different `clock`,
/// `timeout_secs`, or `labels` is a genuinely different run and must be
/// detected as a **conflict** (409), not silently replayed as the original
/// (#146 M7). `clock` is the most important — it sets the `${now.*}` backfill
/// window the run reads, so a retry that changes only the clock would otherwise
/// return the original backfill's result.
///
/// `labels` is hashed in `BTreeMap` (sorted-key) order, so the result is stable
/// regardless of insertion order.
pub fn request_fingerprint(
    config_fingerprint: &str,
    clock: Option<&str>,
    timeout_secs: Option<u64>,
    concurrency: Option<usize>,
    labels: &std::collections::BTreeMap<String, String>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(config_fingerprint.as_bytes());
    hasher.update([0u8]);
    hasher.update(clock.unwrap_or("").as_bytes());
    hasher.update([0u8]);
    hasher.update(
        timeout_secs
            .map(|t| t.to_string())
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.update([0u8]);
    // A key replayed with a different concurrency override is a different run
    // (it would hit the upstream with a different pool size), so it must be a
    // 409 rather than a silent replay of the original (#610).
    hasher.update(
        concurrency
            .map(|c| c.to_string())
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.update([0u8]);
    for (k, v) in labels {
        hasher.update(k.as_bytes());
        hasher.update([0u8]);
        hasher.update(v.as_bytes());
        hasher.update([0u8]);
    }
    format!("{:x}", hasher.finalize())
}

/// Append a canonical (object keys sorted) JSON rendering of `v` to `out`.
fn write_canonical(v: &Value, out: &mut String) {
    match v {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(k).expect("string key serializes"));
                out.push(':');
                write_canonical(&map[*k], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&serde_json::to_string(other).expect("scalar serializes")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn stable_and_order_independent() {
        let a = json!({ "b": 1, "a": [1, 2], "c": { "y": true, "x": null } });
        let b = json!({ "c": { "x": null, "y": true }, "a": [1, 2], "b": 1 });
        assert_eq!(fingerprint(&a, Some("p")), fingerprint(&b, Some("p")));
    }

    #[test]
    fn differs_on_value_change() {
        let a = json!({ "a": 1 });
        let b = json!({ "a": 2 });
        assert_ne!(fingerprint(&a, Some("p")), fingerprint(&b, Some("p")));
    }

    #[test]
    fn differs_on_name() {
        let v = json!({ "a": 1 });
        assert_ne!(fingerprint(&v, Some("p1")), fingerprint(&v, Some("p2")));
    }

    #[test]
    fn array_order_is_significant() {
        assert_ne!(
            fingerprint(&json!([1, 2]), None),
            fingerprint(&json!([2, 1]), None)
        );
    }

    #[test]
    fn request_fingerprint_includes_run_affecting_fields() {
        use std::collections::BTreeMap;
        let cfg_fp = fingerprint(&json!({ "a": 1 }), Some("p"));
        let empty = BTreeMap::new();
        let base = request_fingerprint(&cfg_fp, None, None, None, &empty);

        // Same inputs → same fingerprint.
        assert_eq!(base, request_fingerprint(&cfg_fp, None, None, None, &empty));
        // A different clock (backfill window) must change it (#146 M7).
        assert_ne!(
            base,
            request_fingerprint(&cfg_fp, Some("2026-01-01T00:00:00Z"), None, None, &empty)
        );
        // timeout_secs is run-affecting.
        assert_ne!(
            base,
            request_fingerprint(&cfg_fp, None, Some(30), None, &empty)
        );
        // labels are part of the request identity.
        let mut labels = BTreeMap::new();
        labels.insert("env".to_string(), "prod".to_string());
        assert_ne!(
            base,
            request_fingerprint(&cfg_fp, None, None, None, &labels)
        );
        // A different concurrency override is a different run — it hits the
        // upstream with a different pool size (#610).
        assert_ne!(
            base,
            request_fingerprint(&cfg_fp, None, None, Some(8), &empty)
        );
        assert_ne!(
            request_fingerprint(&cfg_fp, None, None, Some(8), &empty),
            request_fingerprint(&cfg_fp, None, None, Some(4), &empty)
        );
        // A different config fingerprint still changes the result.
        let other_cfg = fingerprint(&json!({ "a": 2 }), Some("p"));
        assert_ne!(
            base,
            request_fingerprint(&other_cfg, None, None, None, &empty)
        );
    }

    #[test]
    fn request_fingerprint_label_value_is_significant() {
        use std::collections::BTreeMap;
        let cfg_fp = fingerprint(&json!({}), None);
        let mut a = BTreeMap::new();
        a.insert("k".to_string(), "v1".to_string());
        let mut b = BTreeMap::new();
        b.insert("k".to_string(), "v2".to_string());
        assert_ne!(
            request_fingerprint(&cfg_fp, None, None, None, &a),
            request_fingerprint(&cfg_fp, None, None, None, &b)
        );
    }
}
