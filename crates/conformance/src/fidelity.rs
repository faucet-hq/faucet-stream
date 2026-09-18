//! Shared round-trip fidelity corpus (#651 Category F).
//!
//! The promise faucet makes is not "the rows arrive" but "the rows arrive
//! *unchanged*". Those are different claims, and the second is the one that
//! breaks quietly: an `i64` past 2^53 that went through a JSON number, a
//! timestamp that lost its offset, a `""` that became `NULL`, a `0.0` that came
//! back `-0.0`. None of that fails a run — it lands, and the report is green.
//!
//! So every source↔sink pair wants the same test: seed a corpus that contains
//! exactly the values known to break, move it through a real pipeline, and
//! assert the landed rows are *equal* to what went in. Writing that per pair
//! invites each one to quietly pick an easier corpus. This module owns the
//! corpus and the comparison instead, so a pair's test is the pipeline wiring
//! and nothing else.
//!
//! ```no_run
//! # use faucet_conformance::fidelity;
//! # async fn example(records: Vec<faucet_core::Value>) {
//! // Landed rows come back from the destination however that sink reads back.
//! let landed: Vec<faucet_core::Value> = read_back_somehow().await;
//! fidelity::assert_round_trip(&fidelity::corpus(), &landed, fidelity::Tolerance::exact());
//! # }
//! # async fn read_back_somehow() -> Vec<faucet_core::Value> { vec![] }
//! ```

use std::collections::BTreeSet;

use faucet_core::{Value, json};

/// Key every corpus record carries, so a destination that reorders rows can
/// still be compared. Named with the reserved `__` prefix so it cannot collide
/// with a realistic column.
pub const ROW_KEY: &str = "__fidelity_id";

/// What a destination is *allowed* to change, because its type system genuinely
/// cannot represent the input.
///
/// Every allowance is a real, documented limitation of some destination — not a
/// convenience. A pair that needs one is saying "this column cannot survive
/// here", which is worth stating explicitly in that pair's test rather than
/// hiding inside a loose comparison.
#[derive(Debug, Clone, Default)]
pub struct Tolerance {
    /// Columns to skip entirely (the destination has no representation at all).
    skip: BTreeSet<String>,
    /// Compare floats within this relative epsilon instead of bit-exactly.
    /// `None` means bit-exact.
    float_epsilon: Option<f64>,
    /// Treat a landed `null` as equal to an input empty string — for the
    /// destinations (Oracle-family, some warehouse loaders) that cannot store a
    /// zero-length string distinctly from NULL.
    empty_string_is_null: bool,
    /// Accept a landed number where a string was sent, and vice versa, as long
    /// as the *rendered* values agree — for schemaless destinations that coerce
    /// on read.
    lenient_scalar_kind: bool,
}

impl Tolerance {
    /// No allowances: every value must land byte- and type-identical. Start
    /// here, and widen only with a comment naming the destination limitation.
    pub fn exact() -> Self {
        Self::default()
    }

    /// Skip a column this destination cannot represent.
    pub fn skipping(mut self, column: &str) -> Self {
        self.skip.insert(column.to_string());
        self
    }

    /// Compare floats within a relative epsilon.
    pub fn float_epsilon(mut self, eps: f64) -> Self {
        self.float_epsilon = Some(eps);
        self
    }

    /// Accept `null` where an empty string was sent.
    pub fn empty_string_becomes_null(mut self) -> Self {
        self.empty_string_is_null = true;
        self
    }

    /// Accept a scalar whose JSON kind changed but whose rendering did not.
    pub fn lenient_scalar_kind(mut self) -> Self {
        self.lenient_scalar_kind = true;
        self
    }
}

/// The corpus: one record per hazard class, each keyed by [`ROW_KEY`].
///
/// Every field here exists because it has broken a real pipeline somewhere.
/// Add to it when a fidelity bug is found — that is what stops the same class
/// of bug reappearing in the next connector.
pub fn corpus() -> Vec<Value> {
    vec![
        json!({
            ROW_KEY: "integers",
            // Past 2^53, so anything that round-trips through an f64 (a JS
            // number, an unguarded JSON parse) corrupts these silently.
            "i64_max": i64::MAX,
            "i64_min": i64::MIN,
            "beyond_f64_exact": 9_007_199_254_740_993i64,
            "zero": 0,
            "negative": -1,
        }),
        json!({
            ROW_KEY: "floats",
            "simple": 1.5,
            // Not representable in binary: a destination that re-renders via a
            // short decimal form changes it.
            "repeating": 0.1,
            "very_small": 1e-300,
            "very_large": 1e300,
            // Distinct from 0.0 by sign bit; many round-trips lose it.
            "negative_zero": -0.0,
        }),
        json!({
            ROW_KEY: "strings",
            "empty": "",
            "unicode": "héllo wörld — 日本語 🚰",
            // The three characters that break naive SQL literal escaping.
            "quote": "it's",
            "backslash": "back\\slash",
            "double_quote": "say \"hi\"",
            "newline": "line1\nline2",
            "tab": "a\tb",
            // Leading/trailing whitespace a CHAR column would pad or trim.
            "padded": "  spaced  ",
        }),
        json!({
            ROW_KEY: "temporal",
            "date": "2026-02-29",
            "timestamp_utc": "2026-09-18T12:34:56Z",
            "timestamp_offset": "2026-09-18T12:34:56+05:30",
            "timestamp_micros": "2026-09-18T12:34:56.123456Z",
            // Pre-epoch, which unsigned or unix-seconds encodings mangle.
            "before_epoch": "1969-07-20T20:17:40Z",
        }),
        json!({
            ROW_KEY: "booleans_and_null",
            "true_val": true,
            "false_val": false,
            "null_val": Value::Null,
        }),
        json!({
            ROW_KEY: "nested",
            "object": { "a": 1, "b": { "c": [1, 2, 3] } },
            "array": [1, "two", null, true],
            "empty_object": {},
            "empty_array": [],
        }),
    ]
}

/// The corpus as a flat single record, for destinations that take one wide row
/// rather than one row per hazard class. Field names are prefixed with the
/// class so they stay unique.
pub fn flat_corpus() -> Value {
    let mut out = serde_json::Map::new();
    out.insert(ROW_KEY.to_string(), json!("flat"));
    for rec in corpus() {
        let Some(obj) = rec.as_object() else { continue };
        let Some(class) = obj.get(ROW_KEY).and_then(|v| v.as_str()) else {
            continue;
        };
        for (k, v) in obj {
            if k == ROW_KEY {
                continue;
            }
            out.insert(format!("{class}_{k}"), v.clone());
        }
    }
    Value::Object(out)
}

/// One field that did not survive the trip.
#[derive(Debug, Clone, PartialEq)]
pub struct Mismatch {
    /// [`ROW_KEY`] of the record.
    pub row: String,
    /// Field name, or `"<row>"` when the whole record is missing or extra.
    pub field: String,
    /// What was sent.
    pub sent: Value,
    /// What landed (`Null` when absent).
    pub landed: Value,
}

impl std::fmt::Display for Mismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "row {:?} field {:?}: sent {} but landed {}",
            self.row, self.field, self.sent, self.landed
        )
    }
}

/// Compare landed records against what was sent, returning every mismatch.
///
/// Rows are matched by [`ROW_KEY`], not by position, so a destination free to
/// reorder is not penalised for it. Pure — the assertion wrapper is
/// [`assert_round_trip`].
pub fn diff_round_trip(sent: &[Value], landed: &[Value], tol: &Tolerance) -> Vec<Mismatch> {
    let key_of = |v: &Value| {
        v.get(ROW_KEY)
            .and_then(|k| k.as_str())
            .unwrap_or("<unkeyed>")
            .to_string()
    };
    let landed_by_key: std::collections::HashMap<String, &Value> =
        landed.iter().map(|v| (key_of(v), v)).collect();

    let mut out = Vec::new();
    for s in sent {
        let row = key_of(s);
        let Some(l) = landed_by_key.get(&row) else {
            out.push(Mismatch {
                row,
                field: "<row>".into(),
                sent: s.clone(),
                landed: Value::Null,
            });
            continue;
        };
        let Some(sobj) = s.as_object() else { continue };
        for (field, sent_v) in sobj {
            if field == ROW_KEY || tol.skip.contains(field) {
                continue;
            }
            let landed_v = l.get(field).cloned().unwrap_or(Value::Null);
            if !values_agree(sent_v, &landed_v, tol) {
                out.push(Mismatch {
                    row: row.clone(),
                    field: field.clone(),
                    sent: sent_v.clone(),
                    landed: landed_v,
                });
            }
        }
    }
    // A row the destination invented is a fidelity failure too — a duplicate
    // from a bad retry shows up here rather than being silently tolerated.
    let sent_keys: BTreeSet<String> = sent.iter().map(key_of).collect();
    for l in landed {
        let row = key_of(l);
        if !sent_keys.contains(&row) {
            out.push(Mismatch {
                row,
                field: "<row>".into(),
                sent: Value::Null,
                landed: l.clone(),
            });
        }
    }
    out
}

/// Whether two values agree under `tol`.
fn values_agree(sent: &Value, landed: &Value, tol: &Tolerance) -> bool {
    if sent == landed {
        // `==` on two JSON numbers does not separate 0.0 from -0.0, so the sign
        // bit is checked explicitly before accepting equality.
        if let (Some(a), Some(b)) = (sent.as_f64(), landed.as_f64())
            && a == 0.0
            && b == 0.0
        {
            return a.is_sign_negative() == b.is_sign_negative();
        }
        return true;
    }
    if tol.empty_string_is_null && sent.as_str() == Some("") && landed.is_null() {
        return true;
    }
    if let (Some(eps), Some(a), Some(b)) = (tol.float_epsilon, sent.as_f64(), landed.as_f64()) {
        let scale = a.abs().max(b.abs()).max(1.0);
        return (a - b).abs() <= eps * scale;
    }
    if tol.lenient_scalar_kind && !sent.is_object() && !sent.is_array() {
        let render = |v: &Value| match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        return render(sent) == render(landed);
    }
    false
}

/// Assert every value survived the round trip, panicking with every mismatch
/// listed rather than only the first — a type bug usually hits a whole class of
/// columns, and seeing all of them is what identifies the cause.
pub fn assert_round_trip(sent: &[Value], landed: &[Value], tol: Tolerance) {
    let mismatches = diff_round_trip(sent, landed, &tol);
    assert!(
        mismatches.is_empty(),
        "round-trip fidelity lost on {} field(s):\n  {}",
        mismatches.len(),
        mismatches
            .iter()
            .map(|m| m.to_string())
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corpus_rows_are_keyed_and_distinct() {
        let c = corpus();
        let keys: BTreeSet<&str> = c
            .iter()
            .map(|r| r[ROW_KEY].as_str().expect("every row is keyed"))
            .collect();
        assert_eq!(keys.len(), c.len(), "row keys must be unique");
        assert!(c.len() >= 6, "the corpus must cover every hazard class");
    }

    #[test]
    fn an_unchanged_round_trip_has_no_mismatches() {
        assert_round_trip(&corpus(), &corpus(), Tolerance::exact());
    }

    #[test]
    fn rows_are_matched_by_key_not_position() {
        let mut reordered = corpus();
        reordered.reverse();
        assert_round_trip(&corpus(), &reordered, Tolerance::exact());
    }

    #[test]
    fn a_missing_row_is_reported() {
        let landed: Vec<Value> = corpus().into_iter().skip(1).collect();
        let d = diff_round_trip(&corpus(), &landed, &Tolerance::exact());
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].field, "<row>");
        assert_eq!(d[0].row, "integers");
    }

    #[test]
    fn an_invented_row_is_reported() {
        let mut landed = corpus();
        landed.push(json!({ ROW_KEY: "ghost", "x": 1 }));
        let d = diff_round_trip(&corpus(), &landed, &Tolerance::exact());
        assert_eq!(d.len(), 1, "{d:?}");
        assert_eq!(d[0].row, "ghost");
        assert_eq!(d[0].sent, Value::Null, "nothing was sent for this row");
    }

    #[test]
    fn a_precision_loss_past_2_pow_53_is_caught() {
        // The bug this corpus exists for: the value survives as an f64 but is
        // no longer the integer that was sent.
        let mut landed = corpus();
        landed[0]["beyond_f64_exact"] = json!(9_007_199_254_740_992i64);
        let d = diff_round_trip(&corpus(), &landed, &Tolerance::exact());
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].field, "beyond_f64_exact");
    }

    #[test]
    fn negative_zero_losing_its_sign_is_caught() {
        // `json!(-0.0) == json!(0.0)` is true, so a naive comparison misses
        // this entirely.
        let mut landed = corpus();
        landed[1]["negative_zero"] = json!(0.0);
        let d = diff_round_trip(&corpus(), &landed, &Tolerance::exact());
        assert_eq!(d.len(), 1, "{d:?}");
        assert_eq!(d[0].field, "negative_zero");
    }

    #[test]
    fn an_empty_string_becoming_null_is_caught_but_tolerable() {
        let mut landed = corpus();
        landed[2]["empty"] = Value::Null;
        assert_eq!(
            diff_round_trip(&corpus(), &landed, &Tolerance::exact()).len(),
            1,
            "exact mode must flag it"
        );
        assert!(
            diff_round_trip(
                &corpus(),
                &landed,
                &Tolerance::exact().empty_string_becomes_null()
            )
            .is_empty(),
            "a destination that cannot store '' distinctly may opt out explicitly"
        );
    }

    #[test]
    fn a_dropped_field_reads_as_null_and_is_caught() {
        let mut landed = corpus();
        landed[2]
            .as_object_mut()
            .expect("object")
            .remove("unicode")
            .expect("field present");
        let d = diff_round_trip(&corpus(), &landed, &Tolerance::exact());
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].landed, Value::Null);
    }

    #[test]
    fn skipping_opts_a_column_out_entirely() {
        let mut landed = corpus();
        landed[3]["timestamp_offset"] = json!("2026-09-18T07:04:56Z");
        assert_eq!(
            diff_round_trip(&corpus(), &landed, &Tolerance::exact()).len(),
            1
        );
        assert!(
            diff_round_trip(
                &corpus(),
                &landed,
                &Tolerance::exact().skipping("timestamp_offset")
            )
            .is_empty()
        );
    }

    #[test]
    fn float_epsilon_accepts_a_reround_but_not_a_real_change() {
        let mut landed = corpus();
        landed[1]["repeating"] = json!(0.100000000000000_1);
        let tol = Tolerance::exact().float_epsilon(1e-12);
        assert!(diff_round_trip(&corpus(), &landed, &tol).is_empty());

        landed[1]["repeating"] = json!(0.2);
        assert_eq!(
            diff_round_trip(&corpus(), &landed, &tol).len(),
            1,
            "an epsilon must not excuse a genuinely different value"
        );
    }

    #[test]
    fn lenient_scalar_kind_accepts_a_stringified_number_only() {
        let mut landed = corpus();
        landed[0]["zero"] = json!("0");
        let tol = Tolerance::exact().lenient_scalar_kind();
        assert!(diff_round_trip(&corpus(), &landed, &tol).is_empty());

        landed[0]["zero"] = json!("1");
        assert_eq!(diff_round_trip(&corpus(), &landed, &tol).len(), 1);
    }

    #[test]
    fn flat_corpus_carries_every_field_with_class_prefixed_names() {
        let flat = flat_corpus();
        let obj = flat.as_object().expect("object");
        assert_eq!(obj[ROW_KEY], json!("flat"));
        assert_eq!(obj["integers_i64_max"], json!(i64::MAX));
        assert_eq!(obj["strings_empty"], json!(""));
        assert_eq!(obj["nested_empty_array"], json!([]));
        // Every non-key field of every row is present exactly once.
        let expected: usize = corpus()
            .iter()
            .map(|r| r.as_object().expect("object").len() - 1)
            .sum();
        assert_eq!(obj.len(), expected + 1, "plus the row key");
    }

    #[test]
    fn mismatch_display_names_both_sides() {
        let m = Mismatch {
            row: "integers".into(),
            field: "i64_max".into(),
            sent: json!(1),
            landed: json!(2),
        };
        let s = m.to_string();
        assert!(
            s.contains("integers") && s.contains("i64_max") && s.contains('1') && s.contains('2'),
            "{s}"
        );
    }
}
