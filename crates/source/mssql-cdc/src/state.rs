//! Durable bookmark for SQL Server CDC progress.
//!
//! Because one source may poll several capture instances, the bookmark is a
//! **map** of capture-instance name → last-committed LSN (hex). Storing the
//! whole map in every emitted page's bookmark keeps the pipeline's single
//! state-key model intact: each page persists the complete, up-to-date map.
//!
//! JSON shape:
//! ```json
//! { "dbo_Orders": "0000002a000000550003", "dbo_Items": "0000002a000000560001" }
//! ```

use std::collections::BTreeMap;

use faucet_core::FaucetError;
use serde_json::Value;

use crate::lsn::Lsn;

/// Per-capture-instance LSN bookmark map. Ordered (`BTreeMap`) so serialization
/// is deterministic.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Bookmarks(BTreeMap<String, Lsn>);

impl Bookmarks {
    /// An empty bookmark map (fresh run, nothing committed yet).
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// The last committed LSN for `capture_instance`, if any.
    pub fn get(&self, capture_instance: &str) -> Option<Lsn> {
        self.0.get(capture_instance).copied()
    }

    /// Record `lsn` as the last committed LSN for `capture_instance`.
    pub fn set(&mut self, capture_instance: impl Into<String>, lsn: Lsn) {
        self.0.insert(capture_instance.into(), lsn);
    }

    /// Parse a bookmark map previously produced by [`to_value`](Self::to_value).
    ///
    /// The `null`/absent case yields an empty map (a fresh run). Every value must
    /// be a valid LSN hex string, else a typed [`FaucetError::State`].
    pub fn from_value(v: Value) -> Result<Self, FaucetError> {
        match v {
            Value::Null => Ok(Self::new()),
            Value::Object(map) => {
                let mut out = BTreeMap::new();
                for (ci, lsn_val) in map {
                    let hex = lsn_val.as_str().ok_or_else(|| {
                        FaucetError::State(format!(
                            "mssql-cdc bookmark: value for {ci:?} must be an LSN hex string"
                        ))
                    })?;
                    let lsn = Lsn::from_hex(hex)
                        .map_err(|e| FaucetError::State(format!("mssql-cdc bookmark: {e}")))?;
                    out.insert(ci, lsn);
                }
                Ok(Self(out))
            }
            other => Err(FaucetError::State(format!(
                "mssql-cdc bookmark must be a JSON object of capture_instance -> LSN hex, got {other}"
            ))),
        }
    }

    /// Serialize the map for the state store.
    pub fn to_value(&self) -> Result<Value, FaucetError> {
        let mut map = serde_json::Map::with_capacity(self.0.len());
        for (ci, lsn) in &self.0 {
            map.insert(ci.clone(), Value::String(lsn.to_hex()));
        }
        Ok(Value::Object(map))
    }
}

/// Per-capture-instance order: `a` is at or before `b` when every capture
/// instance both maps track is consumed at least as far in `b`. An instance
/// only one side tracks does not constrain the order — each instance is its
/// own cursor, and a multi-table mirror compares a table's position only on
/// the instances it tracks. `None` when either side is not a bookmark map.
pub fn bookmarks_le(a: &Value, b: &Value) -> Option<bool> {
    let (a, b) = (
        Bookmarks::from_value(a.clone()).ok()?,
        Bookmarks::from_value(b.clone()).ok()?,
    );
    Some(
        a.0.iter()
            .all(|(ci, lsn)| b.get(ci).is_none_or(|have| *lsn <= have)),
    )
}

/// The per-instance minimum of several bookmark maps: every instance any map
/// tracks, at the lowest LSN a map holds for it.
pub fn bookmarks_min(positions: &[Value]) -> Option<Value> {
    if positions.is_empty() {
        return None;
    }
    let mut out = Bookmarks::new();
    for v in positions {
        for (ci, lsn) in Bookmarks::from_value(v.clone()).ok()?.0 {
            let keep = out.get(&ci).map_or(lsn, |have| have.min(lsn));
            out.set(ci, keep);
        }
    }
    out.to_value().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_instance_order_and_minimum() {
        let lo = "0000002a000000550003";
        let hi = "0000002a000000560001";
        let a = serde_json::json!({"dbo_Orders": lo, "dbo_Items": lo});
        let b = serde_json::json!({"dbo_Orders": hi, "dbo_Items": lo});
        let c = serde_json::json!({"dbo_Orders": lo});
        let d = serde_json::json!({"dbo_New": hi});
        assert_eq!(bookmarks_le(&a, &b), Some(true));
        assert_eq!(bookmarks_le(&b, &a), Some(false));
        assert_eq!(bookmarks_le(&c, &a), Some(true));
        assert_eq!(bookmarks_le(&a, &c), Some(true), "Items is untracked by c");
        assert_eq!(bookmarks_le(&b, &c), Some(false));
        assert_eq!(bookmarks_le(&d, &a), Some(true), "no shared instance");
        assert_eq!(bookmarks_le(&a, &serde_json::json!([])), None);
        assert_eq!(
            bookmarks_min(&[b.clone(), a.clone()]),
            Some(serde_json::json!({"dbo_Orders": lo, "dbo_Items": lo}))
        );
        assert_eq!(
            bookmarks_min(&[b, d]),
            Some(serde_json::json!({"dbo_Orders": hi, "dbo_Items": lo, "dbo_New": hi}))
        );
        assert_eq!(bookmarks_min(&[]), None);
        assert_eq!(bookmarks_min(&[serde_json::json!(1)]), None);
    }
    use serde_json::json;

    fn lsn(hex: &str) -> Lsn {
        Lsn::from_hex(hex).unwrap()
    }

    #[test]
    fn set_get_round_trip() {
        let mut b = Bookmarks::new();
        assert!(b.get("dbo_Orders").is_none());
        b.set("dbo_Orders", lsn("00000000000000000005"));
        assert_eq!(b.get("dbo_Orders"), Some(lsn("00000000000000000005")));
    }

    #[test]
    fn value_round_trip() {
        let mut b = Bookmarks::new();
        b.set("dbo_Orders", lsn("0000002a000000550003"));
        b.set("dbo_Items", lsn("0000002a000000560001"));
        let v = b.to_value().unwrap();
        assert_eq!(Bookmarks::from_value(v).unwrap(), b);
    }

    #[test]
    fn value_shape_is_ci_to_hex() {
        let mut b = Bookmarks::new();
        b.set("dbo_Orders", lsn("00000000000000000009"));
        assert_eq!(
            b.to_value().unwrap(),
            json!({ "dbo_Orders": "00000000000000000009" })
        );
    }

    #[test]
    fn null_is_empty_map() {
        assert_eq!(
            Bookmarks::from_value(Value::Null).unwrap(),
            Bookmarks::new()
        );
    }

    #[test]
    fn rejects_non_object() {
        assert!(Bookmarks::from_value(json!("nope")).is_err());
        assert!(Bookmarks::from_value(json!([1, 2, 3])).is_err());
    }

    #[test]
    fn rejects_bad_lsn_value() {
        assert!(Bookmarks::from_value(json!({ "dbo_Orders": 42 })).is_err());
        assert!(Bookmarks::from_value(json!({ "dbo_Orders": "xx" })).is_err());
    }
}
