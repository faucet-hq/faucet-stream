//! Per-run column profiling (#708): the accumulator that turns written pages
//! into one [`RunProfile`] — a [`ColumnProfile`] per top-level column with
//! null rate, type mix, distinct estimate, numeric / string summaries and
//! the most frequent values. Nested objects and arrays count as one column
//! (their canonical JSON is the value), matching schema-drift semantics.

use super::sketch::{HyperLogLog, Reservoir, TopK, Welford, hash64};
use super::spec::ProfilingSpec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

/// Reservoir size for approximate quantiles.
const RESERVOIR_CAPACITY: usize = 1024;
/// Longest value text kept in the top-values sketch (bounds memory per
/// counter; a longer value is truncated with a marker).
const MAX_VALUE_TEXT: usize = 256;

/// How many values of each JSON type the column held.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct TypeCounts {
    pub null: u64,
    pub boolean: u64,
    pub integer: u64,
    pub number: u64,
    pub string: u64,
    pub object: u64,
    pub array: u64,
}

impl TypeCounts {
    /// `(type name, count)` for every non-zero type, fixed order.
    pub fn present(&self) -> Vec<(&'static str, u64)> {
        [
            ("null", self.null),
            ("boolean", self.boolean),
            ("integer", self.integer),
            ("number", self.number),
            ("string", self.string),
            ("object", self.object),
            ("array", self.array),
        ]
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .collect()
    }

    /// The most common non-null type, if any value was non-null.
    pub fn dominant(&self) -> Option<&'static str> {
        self.present()
            .into_iter()
            .filter(|(t, _)| *t != "null")
            .max_by_key(|(_, n)| *n)
            .map(|(t, _)| t)
    }
}

/// Summary of the numeric values a column held.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct NumericSummary {
    pub count: u64,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub stddev: f64,
    /// Approximate median (reservoir sample).
    pub p50: f64,
    /// Approximate 95th percentile (reservoir sample).
    pub p95: f64,
}

/// Summary of the string values a column held (character lengths).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StringSummary {
    pub count: u64,
    pub len_min: u64,
    pub len_max: u64,
    pub len_mean: f64,
}

/// One of a column's most frequent values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TopValue {
    pub value: String,
    /// Occurrences (an upper bound; `error` is the possible over-count).
    pub count: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub error: u64,
    /// `count / non-null values` in this run.
    pub share: f64,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

/// The learned profile of one column over one run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ColumnProfile {
    /// Records the run wrote (the denominator of `null_rate`).
    pub rows: u64,
    /// Records in which the column was present (including explicit nulls).
    pub present: u64,
    /// Records where the column was absent or null.
    pub nulls: u64,
    /// `nulls / rows` (0 when no rows).
    pub null_rate: f64,
    pub types: TypeCounts,
    /// Estimated distinct non-null values (HyperLogLog, ~1.6 % error).
    pub distinct: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub numeric: Option<NumericSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub string: Option<StringSummary>,
    /// Most frequent values, count descending. Absent for high-cardinality
    /// columns and when `top_values: 0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_values: Option<Vec<TopValue>>,
    /// The estimated distinct count exceeded `categorical_max_distinct`, so
    /// no values are published for this column.
    #[serde(default)]
    pub high_cardinality: bool,
}

impl ColumnProfile {
    /// Non-null values observed.
    pub fn non_null(&self) -> u64 {
        self.rows.saturating_sub(self.nulls)
    }
}

/// The profile of everything one run wrote.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RunProfile {
    /// Records the run wrote.
    pub rows: u64,
    /// Profiled columns, name-ordered.
    pub columns: BTreeMap<String, ColumnProfile>,
    /// Columns seen beyond `max_columns` and therefore not profiled.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub skipped_columns: u64,
}

impl RunProfile {
    /// Whether the column cap was hit.
    pub fn truncated(&self) -> bool {
        self.skipped_columns > 0
    }
}

/// Per-column accumulator.
#[derive(Debug)]
struct ColumnAcc {
    present: u64,
    nulls: u64,
    types: TypeCounts,
    distinct: HyperLogLog,
    top: Option<TopK>,
    numeric: Welford,
    reservoir: Reservoir,
    string_len: Welford,
}

impl ColumnAcc {
    fn new(top_values: usize) -> Self {
        Self {
            present: 0,
            nulls: 0,
            types: TypeCounts::default(),
            distinct: HyperLogLog::new(),
            top: (top_values > 0).then(|| TopK::new(top_values)),
            numeric: Welford::default(),
            reservoir: Reservoir::new(RESERVOIR_CAPACITY),
            string_len: Welford::default(),
        }
    }

    fn observe(&mut self, v: &Value) {
        self.present += 1;
        match v {
            Value::Null => {
                self.nulls += 1;
                self.types.null += 1;
                return;
            }
            Value::Bool(_) => self.types.boolean += 1,
            Value::Number(n) => {
                if n.is_i64() || n.is_u64() {
                    self.types.integer += 1;
                } else {
                    self.types.number += 1;
                }
                if let Some(f) = n.as_f64()
                    && f.is_finite()
                {
                    self.numeric.insert(f);
                    self.reservoir.insert(f);
                }
            }
            Value::String(s) => {
                self.types.string += 1;
                self.string_len.insert(s.chars().count() as f64);
            }
            Value::Object(_) => self.types.object += 1,
            Value::Array(_) => self.types.array += 1,
        }
        let text = value_text(v);
        // Distinctness is typed — `1` and `"1"` are two values — while the
        // frequency sketch keys on the display text.
        self.distinct
            .insert_hash(hash64(&(type_tag(v), text.as_ref())));
        if let Some(top) = &mut self.top {
            top.insert(&truncate_text(&text));
        }
    }

    fn finish(&self, rows: u64, spec: &ProfilingSpec) -> ColumnProfile {
        let missing = rows.saturating_sub(self.present);
        let nulls = self.nulls + missing;
        let non_null = rows.saturating_sub(nulls);
        let distinct = self.distinct.estimate();
        let high_cardinality = distinct > spec.categorical_max_distinct;
        let numeric = (self.numeric.count > 0).then(|| NumericSummary {
            count: self.numeric.count,
            min: self.numeric.min,
            max: self.numeric.max,
            mean: self.numeric.mean().unwrap_or(0.0),
            stddev: self.numeric.stddev().unwrap_or(0.0),
            p50: self.reservoir.quantile(0.5).unwrap_or(0.0),
            p95: self.reservoir.quantile(0.95).unwrap_or(0.0),
        });
        let string = (self.string_len.count > 0).then(|| StringSummary {
            count: self.string_len.count,
            len_min: self.string_len.min as u64,
            len_max: self.string_len.max as u64,
            len_mean: self.string_len.mean().unwrap_or(0.0),
        });
        let top_values = match &self.top {
            Some(top) if !high_cardinality && non_null > 0 => Some(
                top.top(spec.top_values)
                    .into_iter()
                    .map(|(value, count, error)| TopValue {
                        value,
                        count,
                        error,
                        share: count as f64 / non_null as f64,
                    })
                    .collect(),
            ),
            _ => None,
        };
        ColumnProfile {
            rows,
            present: self.present,
            nulls,
            null_rate: if rows == 0 {
                0.0
            } else {
                nulls as f64 / rows as f64
            },
            types: self.types,
            distinct,
            numeric,
            string,
            top_values,
            high_cardinality,
        }
    }
}

/// The text a value is counted by: strings verbatim, everything else as
/// canonical JSON (so `1` and `"1"` are distinct values).
fn value_text(v: &Value) -> std::borrow::Cow<'_, str> {
    match v {
        Value::String(s) => std::borrow::Cow::Borrowed(s.as_str()),
        other => std::borrow::Cow::Owned(other.to_string()),
    }
}

fn type_tag(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

fn truncate_text(s: &str) -> String {
    if s.chars().count() <= MAX_VALUE_TEXT {
        return s.to_string();
    }
    let mut out: String = s.chars().take(MAX_VALUE_TEXT).collect();
    out.push('…');
    out
}

/// The run-level accumulator: hand it every page the sink wrote, then
/// [`finish`](Profiler::finish).
#[derive(Debug)]
pub struct Profiler {
    spec: ProfilingSpec,
    rows: u64,
    columns: HashMap<String, ColumnAcc>,
    /// Column names seen and rejected by the cap (so the count is exact).
    skipped: std::collections::HashSet<String>,
}

impl Profiler {
    pub fn new(spec: ProfilingSpec) -> Self {
        Self {
            spec,
            rows: 0,
            columns: HashMap::new(),
            skipped: std::collections::HashSet::new(),
        }
    }

    pub fn spec(&self) -> &ProfilingSpec {
        &self.spec
    }

    /// Records observed so far.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Fold one written page in. Non-object records count toward `rows` but
    /// contribute no columns.
    pub fn observe_page(&mut self, records: &[Value]) {
        for record in records {
            self.rows += 1;
            let Some(obj) = record.as_object() else {
                continue;
            };
            for (name, v) in obj {
                if !self.spec.selects(name) {
                    continue;
                }
                if let Some(acc) = self.columns.get_mut(name) {
                    acc.observe(v);
                    continue;
                }
                if self.columns.len() >= self.spec.max_columns {
                    self.skipped.insert(name.clone());
                    continue;
                }
                let mut acc = ColumnAcc::new(self.spec.top_values);
                acc.observe(v);
                self.columns.insert(name.clone(), acc);
            }
        }
    }

    /// The run's profile from everything observed so far.
    pub fn finish(&self) -> RunProfile {
        RunProfile {
            rows: self.rows,
            columns: self
                .columns
                .iter()
                .map(|(name, acc)| (name.clone(), acc.finish(self.rows, &self.spec)))
                .collect(),
            skipped_columns: self.skipped.len() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn profile(records: &[Value]) -> RunProfile {
        let mut p = Profiler::new(ProfilingSpec::default());
        p.observe_page(records);
        p.finish()
    }

    #[test]
    fn null_rate_counts_missing_and_explicit_nulls() {
        let rp = profile(&[
            json!({"a": 1}),
            json!({"a": null}),
            json!({}),
            json!({"a": 2}),
        ]);
        let a = &rp.columns["a"];
        assert_eq!(rp.rows, 4);
        assert_eq!(a.present, 3);
        assert_eq!(a.nulls, 2);
        assert_eq!(a.null_rate, 0.5);
        assert_eq!(a.non_null(), 2);
        assert_eq!(a.types.null, 1);
        assert_eq!(a.types.integer, 2);
        assert_eq!(a.types.dominant(), Some("integer"));
    }

    #[test]
    fn numeric_string_and_type_mix_summaries() {
        let rp = profile(&[
            json!({"n": 1, "s": "ab", "b": true, "o": {"x": 1}, "l": [1]}),
            json!({"n": 2.5, "s": "abcd", "b": false, "o": {"x": 2}, "l": [1, 2]}),
            json!({"n": 3, "s": "", "b": true, "o": {"x": 1}, "l": [1]}),
        ]);
        let n = rp.columns["n"].numeric.as_ref().unwrap();
        assert_eq!(n.count, 3);
        assert_eq!(n.min, 1.0);
        assert_eq!(n.max, 3.0);
        assert!((n.mean - 2.1666).abs() < 1e-3);
        assert_eq!(n.p50, 2.5);
        assert_eq!(rp.columns["n"].types.integer, 2);
        assert_eq!(rp.columns["n"].types.number, 1);
        let s = rp.columns["s"].string.as_ref().unwrap();
        assert_eq!((s.len_min, s.len_max), (0, 4));
        assert_eq!(s.len_mean, 2.0);
        assert!(rp.columns["s"].numeric.is_none());
        assert_eq!(rp.columns["b"].types.boolean, 3);
        assert_eq!(rp.columns["o"].types.object, 3);
        assert_eq!(rp.columns["o"].distinct, 2);
        assert_eq!(rp.columns["l"].types.array, 3);
        assert_eq!(
            rp.columns["b"].types.present(),
            vec![("boolean", 3)],
            "present lists only non-zero types"
        );
    }

    #[test]
    fn top_values_with_shares_and_high_cardinality_cutoff() {
        let mut rows = Vec::new();
        for i in 0..100 {
            let c = if i % 2 == 0 {
                "eu"
            } else if i % 3 == 0 {
                "us"
            } else {
                "apac"
            };
            rows.push(json!({"region": c, "id": format!("id-{i}"), "n": null}));
        }
        let spec = ProfilingSpec {
            categorical_max_distinct: 20,
            ..Default::default()
        };
        let mut p = Profiler::new(spec);
        p.observe_page(&rows);
        let rp = p.finish();
        let region = &rp.columns["region"];
        let top = region.top_values.as_ref().unwrap();
        assert_eq!(top[0].value, "eu");
        assert_eq!(top[0].count, 50);
        assert_eq!(top[0].share, 0.5);
        assert!(!region.high_cardinality);
        let id = &rp.columns["id"];
        assert!(id.high_cardinality);
        assert!(id.top_values.is_none());
        assert!(id.distinct >= 95 && id.distinct <= 105, "{}", id.distinct);
        // An all-null column has no values to rank.
        assert!(rp.columns["n"].top_values.is_none());
        assert_eq!(rp.columns["n"].null_rate, 1.0);
    }

    #[test]
    fn top_values_zero_disables_the_sketch() {
        let spec = ProfilingSpec {
            top_values: 0,
            ..Default::default()
        };
        let mut p = Profiler::new(spec);
        p.observe_page(&[json!({"a": "x"})]);
        assert!(p.finish().columns["a"].top_values.is_none());
    }

    #[test]
    fn selection_cap_and_non_object_records() {
        let mut spec = ProfilingSpec {
            max_columns: 2,
            ..Default::default()
        };
        spec.exclude.push("skip".into());
        let mut p = Profiler::new(spec);
        p.observe_page(&[
            json!({"a": 1, "b": 2, "c": 3, "d": 4, "skip": 5, "_faucet_run_id": "r"}),
            json!("scalar"),
            json!({"a": 1, "c": 3, "e": 9}),
        ]);
        let rp = p.finish();
        assert_eq!(rp.rows, 3);
        assert_eq!(rp.columns.len(), 2);
        assert!(rp.columns.contains_key("a") && rp.columns.contains_key("b"));
        assert_eq!(
            rp.skipped_columns, 3,
            "c, d, e were capped; skip/_faucet_ excluded"
        );
        assert!(rp.truncated());
        assert_eq!(p.rows(), 3);
        assert_eq!(p.spec().max_columns, 2);
    }

    #[test]
    fn strings_and_numbers_are_distinct_values_and_long_text_is_truncated() {
        let long = "x".repeat(600);
        let rp = profile(&[json!({"v": 1}), json!({"v": "1"}), json!({"v": long})]);
        let v = &rp.columns["v"];
        assert_eq!(v.distinct, 3);
        let top = v.top_values.as_ref().unwrap();
        let truncated = top.iter().find(|t| t.value.ends_with('…')).unwrap();
        assert_eq!(truncated.value.chars().count(), MAX_VALUE_TEXT + 1);
    }

    #[test]
    fn profile_round_trips_through_json() {
        let rp = profile(&[json!({"a": 1.5, "b": "x"}), json!({"a": null})]);
        let text = serde_json::to_string(&rp).unwrap();
        let back: RunProfile = serde_json::from_str(&text).unwrap();
        assert_eq!(back, rp);
        assert!(!text.contains("skipped_columns"), "zero is elided: {text}");
        assert!(!text.contains("\"error\""));
    }

    #[test]
    fn empty_run_profile() {
        let rp = profile(&[]);
        assert_eq!(rp.rows, 0);
        assert!(rp.columns.is_empty());
        assert!(!rp.truncated());
        assert_eq!(TypeCounts::default().dominant(), None);
    }
}
