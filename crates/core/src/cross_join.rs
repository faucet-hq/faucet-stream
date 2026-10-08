//! Inbuilt `cross_join` transform (#534): expand one record into the
//! **cartesian product of two or more of its sibling array fields**, emitting
//! one flat record per combination.
//!
//! This is the last per-record reshape that otherwise falls to the DuckDB SQL
//! transform. It is a different shape from `explode` (one array → N rows) and
//! `unpivot` (wide → long): here several sibling arrays within a single record
//! are crossed (e.g. a HCM record's `jobs[] × compensation[] × employment[]`).
//!
//! The whole module is gated by `#[cfg(feature = "transform-cross-join")]` at
//! the `mod` site in `lib.rs`. It routes through
//! [`TransformStage::PageFn`](crate::stage::TransformStage) (page-level, fallible)
//! so an over-limit cartesian product fails loudly rather than risking OOM.

use crate::FaucetError;
use crate::stage::TransformStage;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::Arc;

/// Default per-record cartesian-product ceiling.
pub const DEFAULT_MAX_PRODUCT: usize = 10_000;

fn default_true() -> bool {
    true
}
fn default_max_product() -> usize {
    DEFAULT_MAX_PRODUCT
}

/// What to do when one of the crossed arrays is empty for a given record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnEmpty {
    /// SQL `CROSS JOIN` semantics: an empty crossed array ⇒ the record produces
    /// **zero** rows.
    #[default]
    Skip,
    /// `LEFT JOIN … ON true` semantics: an empty crossed array contributes a
    /// single `null` element, so the record still produces a (null-filled) row.
    OneRow,
}

/// User-facing `cross_join` config.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CrossJoinSpec {
    /// Sibling array fields to cross (≥2). Each element is expected to be an
    /// object; a scalar element is wrapped under the array's name.
    pub arrays: Vec<String>,
    /// Prefix each produced column with its array name (`jobs` → `jobs_title`).
    /// Avoids collisions between arrays that share field names. Default `false`.
    #[serde(default)]
    pub prefix: bool,
    /// Keep the record's non-array scalar fields on every output row.
    /// Default `true`.
    #[serde(default = "default_true")]
    pub keep_parent: bool,
    /// Behavior when a crossed array is empty. Default `skip`.
    #[serde(default)]
    pub on_empty: OnEmpty,
    /// Remove the source array fields from the output rows after expansion.
    /// Default `true`.
    #[serde(default = "default_true")]
    pub drop_arrays: bool,
    /// Fail (rather than OOM) if a single record's cartesian product would
    /// exceed this many rows. Default [`DEFAULT_MAX_PRODUCT`].
    #[serde(default = "default_max_product")]
    pub max_product: usize,
}

impl CrossJoinSpec {
    /// Validate the spec, returning a reusable [`CompiledCrossJoin`].
    pub fn compile(&self) -> Result<CompiledCrossJoin, FaucetError> {
        CompiledCrossJoin::compile(self)
    }

    /// Compile and wrap as a [`TransformStage::PageFn`] (1→0..N per record,
    /// fallible on product overflow).
    pub fn into_stage(&self) -> Result<TransformStage, FaucetError> {
        let compiled = self.compile()?;
        Ok(TransformStage::PageFn(Arc::new(move |page: Vec<Value>| {
            let mut out = Vec::with_capacity(page.len());
            for rec in page {
                out.extend(compiled.apply(rec)?);
            }
            Ok(out)
        })))
    }
}

/// Validated [`CrossJoinSpec`] — apply per record with [`CompiledCrossJoin::apply`].
#[derive(Debug, Clone)]
pub struct CompiledCrossJoin {
    spec: CrossJoinSpec,
}

impl CompiledCrossJoin {
    fn compile(spec: &CrossJoinSpec) -> Result<Self, FaucetError> {
        if spec.arrays.len() < 2 {
            return Err(FaucetError::Config(
                "cross_join: `arrays` needs at least 2 fields (a single array is `explode`)".into(),
            ));
        }
        if spec.arrays.iter().any(|a| a.trim().is_empty()) {
            return Err(FaucetError::Config(
                "cross_join: `arrays` entries must be non-empty field names".into(),
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for a in &spec.arrays {
            if !seen.insert(a) {
                return Err(FaucetError::Config(format!(
                    "cross_join: duplicate array field `{a}`"
                )));
            }
        }
        if spec.max_product == 0 {
            return Err(FaucetError::Config(
                "cross_join: `max_product` must be greater than 0".into(),
            ));
        }
        Ok(Self { spec: spec.clone() })
    }

    /// Expand one record into the cartesian product of its named sibling arrays.
    /// Non-object records pass through unchanged. Missing or `null` named fields
    /// are treated as empty sets (subject to `on_empty`); a field that is present
    /// but not an array is an error, so an upstream type change cannot silently
    /// drop whole records (CORE-59). A produced column that would overwrite an
    /// existing one (a parent field, or a field of an earlier array) is an error
    /// too — set `prefix: true` to keep both (CORE-33).
    pub fn apply(&self, rec: Value) -> Result<Vec<Value>, FaucetError> {
        let Value::Object(obj) = rec else {
            return Ok(vec![rec]);
        };

        // Resolve each named array into its element set (missing / non-array →
        // empty). Preserve declared order so column-collision precedence and the
        // `OneRow` null-fill are deterministic.
        let mut sets: Vec<(&str, Vec<Value>)> = Vec::with_capacity(self.spec.arrays.len());
        for name in &self.spec.arrays {
            let elems = match obj.get(name) {
                Some(Value::Array(a)) => a.clone(),
                None | Some(Value::Null) => Vec::new(),
                Some(other) => {
                    return Err(FaucetError::Transform(format!(
                        "cross_join: field `{name}` is {} rather than an array",
                        match other {
                            Value::Object(_) => "an object",
                            Value::String(_) => "a string",
                            Value::Number(_) => "a number",
                            _ => "a boolean",
                        }
                    )));
                }
            };
            sets.push((name.as_str(), elems));
        }

        // Empty-array handling.
        match self.spec.on_empty {
            OnEmpty::Skip => {
                if sets.iter().any(|(_, e)| e.is_empty()) {
                    return Ok(Vec::new());
                }
            }
            OnEmpty::OneRow => {
                for (_, e) in sets.iter_mut() {
                    if e.is_empty() {
                        e.push(Value::Null);
                    }
                }
            }
        }

        // Product-size guard (fail loud, never OOM). After `on_empty` every set
        // has len >= 1, so the product is always >= 1 (no zero-size case).
        sets.iter()
            .try_fold(1usize, |acc, (_, e)| acc.checked_mul(e.len()))
            .filter(|n| *n <= self.spec.max_product)
            .ok_or_else(|| {
                FaucetError::Transform(format!(
                    "cross_join: record's cartesian product over {:?} exceeds max_product={} \
                     — narrow the arrays or raise max_product",
                    self.spec.arrays, self.spec.max_product
                ))
            })?;

        // Parent scalars carried onto every row.
        let mut parent = Map::new();
        if self.spec.keep_parent {
            for (k, v) in &obj {
                if self.spec.drop_arrays && self.spec.arrays.iter().any(|a| a == k) {
                    continue;
                }
                parent.insert(k.clone(), v.clone());
            }
        } else if !self.spec.drop_arrays {
            // keep_parent=false still carries the raw arrays only if not dropping.
            for name in &self.spec.arrays {
                if let Some(v) = obj.get(name) {
                    parent.insert(name.clone(), v.clone());
                }
            }
        }

        // Iterative cartesian product.
        let mut rows: Vec<Map<String, Value>> = vec![parent];
        for (name, elems) in &sets {
            let mut next = Vec::with_capacity(rows.len() * elems.len());
            for base in &rows {
                for elem in elems {
                    let mut row = base.clone();
                    merge_element(&mut row, name, elem, self.spec.prefix)?;
                    next.push(row);
                }
            }
            rows = next;
        }

        Ok(rows.into_iter().map(Value::Object).collect())
    }
}

/// Merge one crossed array element into a product row. Object elements spread
/// their fields (name-prefixed when `prefix`); scalar/null elements land under
/// the array's own name, replacing the raw array a kept parent carries. Any
/// other overwrite of an existing column is refused.
fn merge_element(
    row: &mut Map<String, Value>,
    array_name: &str,
    elem: &Value,
    prefix: bool,
) -> Result<(), FaucetError> {
    match elem {
        Value::Object(fields) => {
            for (k, v) in fields {
                let key = if prefix {
                    format!("{array_name}_{k}")
                } else {
                    k.clone()
                };
                if row.contains_key(&key) {
                    return Err(FaucetError::Transform(format!(
                        "cross_join: field `{key}` of `{array_name}` would overwrite an \
                         existing column — set `prefix: true` or rename it first"
                    )));
                }
                row.insert(key, v.clone());
            }
        }
        other => {
            row.insert(array_name.to_string(), other.clone());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(arrays: &[&str]) -> CrossJoinSpec {
        CrossJoinSpec {
            arrays: arrays.iter().map(|s| s.to_string()).collect(),
            prefix: false,
            keep_parent: true,
            on_empty: OnEmpty::Skip,
            drop_arrays: true,
            max_product: DEFAULT_MAX_PRODUCT,
        }
    }

    #[test]
    fn crosses_two_sibling_arrays() {
        let c = spec(&["jobs", "comp"]).compile().unwrap();
        let rec = json!({
            "emp_id": 1,
            "jobs": [{"title": "eng"}, {"title": "mgr"}],
            "comp": [{"amount": 100}, {"amount": 200}]
        });
        let out = c.apply(rec).unwrap();
        assert_eq!(out.len(), 4); // 2 × 2
        // parent scalar carried; array fields dropped; element fields merged.
        assert_eq!(out[0]["emp_id"], json!(1));
        assert_eq!(out[0]["title"], json!("eng"));
        assert_eq!(out[0]["amount"], json!(100));
        assert!(out[0].get("jobs").is_none());
    }

    #[test]
    fn skip_vs_one_row_on_empty() {
        let rec = json!({"id": 1, "jobs": [{"t": "a"}], "comp": []});
        assert!(
            spec(&["jobs", "comp"])
                .compile()
                .unwrap()
                .apply(rec.clone())
                .unwrap()
                .is_empty()
        );
        let mut s = spec(&["jobs", "comp"]);
        s.on_empty = OnEmpty::OneRow;
        let out = s.compile().unwrap().apply(rec).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["t"], json!("a"));
        // OneRow fills the empty `comp` array with a null element → lands under
        // the array name as null.
        assert_eq!(out[0]["comp"], json!(null));
    }

    #[test]
    fn prefix_avoids_collisions() {
        let mut s = spec(&["a", "b"]);
        s.prefix = true;
        let rec = json!({"a": [{"x": 1}], "b": [{"x": 2}]});
        let out = s.compile().unwrap().apply(rec).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["a_x"], json!(1));
        assert_eq!(out[0]["b_x"], json!(2));
    }

    #[test]
    fn scalar_elements_wrap_under_array_name() {
        let rec = json!({"id": 1, "tags": ["x", "y"], "vals": [10]});
        let out = spec(&["tags", "vals"])
            .compile()
            .unwrap()
            .apply(rec)
            .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["tags"], json!("x"));
        assert_eq!(out[0]["vals"], json!(10));
    }

    #[test]
    fn max_product_overflow_errors() {
        let mut s = spec(&["a", "b"]);
        s.max_product = 3;
        let rec = json!({"a": [1, 2], "b": [1, 2]}); // 2×2 = 4 > 3
        assert!(s.compile().unwrap().apply(rec).is_err());
    }

    #[test]
    fn non_object_and_missing_array_passthrough() {
        let c = spec(&["a", "b"]).compile().unwrap();
        // non-object record → passthrough
        assert_eq!(c.apply(json!(5)).unwrap(), vec![json!(5)]);
        // missing array under Skip → 0 rows
        assert!(c.apply(json!({"a": [{"x": 1}]})).unwrap().is_empty());
    }

    #[test]
    fn overwriting_a_column_is_refused_without_prefix() {
        let c = spec(&["a", "b"]).compile().unwrap();
        let err = c
            .apply(json!({"id": 1, "a": [{"id": 9}], "b": [{"y": 2}]}))
            .unwrap_err();
        assert!(err.to_string().contains("field `id` of `a`"), "{err}");
        let err = c
            .apply(json!({"a": [{"x": 1}], "b": [{"x": 2}]}))
            .unwrap_err();
        assert!(err.to_string().contains("field `x` of `b`"), "{err}");
        let mut p = spec(&["a", "b"]);
        p.prefix = true;
        let out = p
            .compile()
            .unwrap()
            .apply(json!({"id": 1, "a": [{"id": 9}], "b": [{"id": 2}]}))
            .unwrap();
        assert_eq!(out[0]["id"], json!(1));
        assert_eq!(out[0]["a_id"], json!(9));
    }

    #[test]
    fn a_present_non_array_is_a_type_error() {
        let c = spec(&["a", "b"]).compile().unwrap();
        for bad in [json!({"x": 1}), json!("s"), json!(3), json!(true)] {
            let err = c.apply(json!({"a": [{"x": 1}], "b": bad})).unwrap_err();
            assert!(err.to_string().contains("rather than an array"), "{err}");
        }
        assert!(
            c.apply(json!({"a": [{"x": 1}], "b": null}))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn compile_rejects_bad_specs() {
        assert!(spec(&["only"]).compile().is_err()); // <2 arrays
        assert!(spec(&["a", ""]).compile().is_err()); // empty name
        assert!(spec(&["a", "a"]).compile().is_err()); // duplicate
        let mut s = spec(&["a", "b"]);
        s.max_product = 0;
        assert!(s.compile().is_err());
    }

    #[test]
    fn into_stage_is_pagefn_and_flat_maps() {
        let stage = spec(&["a", "b"]).into_stage().unwrap();
        assert!(matches!(stage, TransformStage::PageFn(_)));
    }

    #[test]
    fn into_stage_pagefn_runs_over_a_page() {
        // Exercise the PageFn closure body (flat-map over a real page).
        let TransformStage::PageFn(f) = spec(&["jobs", "comp"]).into_stage().unwrap() else {
            panic!("expected PageFn");
        };
        let page = vec![
            json!({"id": 1, "jobs": [{"t": "a"}], "comp": [{"c": 1}, {"c": 2}]}),
            json!({"id": 2, "jobs": [{"t": "b"}, {"t": "c"}], "comp": [{"c": 9}]}),
        ];
        let out = f(page).unwrap();
        assert_eq!(out.len(), 2 + 2); // (1×2) + (2×1)
        assert_eq!(out[0]["id"], json!(1));
    }

    #[test]
    fn into_stage_pagefn_propagates_overflow_error() {
        let mut s = spec(&["a", "b"]);
        s.max_product = 1;
        let TransformStage::PageFn(f) = s.into_stage().unwrap() else {
            panic!("expected PageFn");
        };
        assert!(f(vec![json!({"a": [1, 2], "b": [1, 2]})]).is_err());
    }

    #[test]
    fn keep_parent_false_drops_scalars() {
        let mut s = spec(&["a", "b"]);
        s.keep_parent = false;
        let out = s
            .compile()
            .unwrap()
            .apply(json!({"id": 7, "a": [{"x": 1}], "b": [{"y": 2}]}))
            .unwrap();
        assert_eq!(out.len(), 1);
        assert!(out[0].get("id").is_none()); // parent scalar dropped
        assert_eq!(out[0]["x"], json!(1));
    }

    #[test]
    fn keep_parent_false_keep_arrays_when_not_dropping() {
        let mut s = spec(&["a", "b"]);
        s.keep_parent = false;
        s.drop_arrays = false;
        let out = s
            .compile()
            .unwrap()
            .apply(json!({"id": 7, "a": [{"x": 1}], "b": [{"y": 2}]}))
            .unwrap();
        assert_eq!(out.len(), 1);
        // raw arrays carried (not dropped), parent scalar still dropped
        assert_eq!(out[0]["a"], json!([{"x": 1}]));
        assert!(out[0].get("id").is_none());
    }

    #[test]
    fn keep_parent_true_no_drop_keeps_arrays_and_scalars() {
        let mut s = spec(&["a", "b"]);
        s.drop_arrays = false;
        let out = s
            .compile()
            .unwrap()
            .apply(json!({"id": 7, "a": [{"x": 1}], "b": [{"y": 2}]}))
            .unwrap();
        assert_eq!(out[0]["id"], json!(7));
        assert_eq!(out[0]["a"], json!([{"x": 1}])); // array retained
        assert_eq!(out[0]["x"], json!(1)); // and exploded
    }
}
