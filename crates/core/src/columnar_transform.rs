//! Arrow-native (vectorized) forms of the built-in record transforms (#636).
//!
//! The `Value` transforms in [`crate::transform`] iterate records as
//! `serde_json::Value` — a boxed map + boxed string per field per row. On the
//! columnar fast path (#375) that means a `parquet → select → parquet` chain
//! disqualifies the fast path and re-materializes every record. This module
//! supplies the same transforms as **whole-column** operations on an Arrow
//! [`RecordBatch`], so the chain stays columnar end-to-end.
//!
//! Only the transforms that are genuinely a column operation live here —
//! `select`/`drop` (projection), `rename_field` (schema rename), `set`
//! (constant column), `redact` (constant overwrite). They touch the record's
//! *shape*, never parse or re-type a value, so each is byte-identical to
//! running the `Value` form on the same batch — which is exactly what the
//! parity tests assert. Value-inspecting transforms (`cast`, `value_case`,
//! `hash`, …) and the opaque ones (`flatten`, `explode`, …) return `None` and
//! keep the chain on the `Value` path until they gain their own kernels.

use crate::stage::PageFnBatchBox;
use crate::transform::RecordTransform;

// Everything below the always-present `batch_form` signature is used only when a
// vectorizable transform feature is on. Under bare `arrow` (how the rest and
// bigquery crates pull core) `batch_form` is just `_ => None`, so these imports
// and helpers would otherwise be unused. Each gate matches its real usage.
#[cfg(any(
    feature = "transform-select",
    feature = "transform-drop",
    feature = "transform-rename-field",
    feature = "transform-set",
    feature = "transform-redact"
))]
use crate::FaucetError;
#[cfg(any(feature = "transform-set", feature = "transform-redact"))]
use arrow::array::ArrayRef;
#[cfg(any(
    feature = "transform-select",
    feature = "transform-drop",
    feature = "transform-rename-field",
    feature = "transform-set",
    feature = "transform-redact"
))]
use arrow::array::RecordBatch;
#[cfg(any(feature = "transform-set", feature = "transform-redact"))]
use arrow::datatypes::DataType;
#[cfg(any(
    feature = "transform-rename-field",
    feature = "transform-set",
    feature = "transform-redact"
))]
use arrow::datatypes::Field;
#[cfg(any(
    feature = "transform-select",
    feature = "transform-drop",
    feature = "transform-rename-field",
    feature = "transform-set",
    feature = "transform-redact"
))]
use arrow::datatypes::Schema;
#[cfg(any(
    feature = "transform-select",
    feature = "transform-drop",
    feature = "transform-rename-field",
    feature = "transform-set",
    feature = "transform-redact"
))]
use std::sync::Arc;

/// The Arrow `RecordBatch → RecordBatch` form of `t`, or `None` when `t` has no
/// vectorized kernel yet (the chain then stays on the `Value` path).
pub fn batch_form(t: &RecordTransform) -> Option<PageFnBatchBox> {
    match t {
        #[cfg(feature = "transform-select")]
        RecordTransform::Select { fields } => {
            let fields = fields.clone();
            Some(Arc::new(move |b| select(b, &fields)))
        }
        #[cfg(feature = "transform-drop")]
        RecordTransform::Drop { fields } => {
            let fields = fields.clone();
            Some(Arc::new(move |b| drop(b, &fields)))
        }
        #[cfg(feature = "transform-rename-field")]
        RecordTransform::RenameField { fields } => {
            // Sort into a stable Vec, exactly as `compile_stage` does — a
            // HashMap's order is randomized, and interacting renames
            // (chains/swaps) must resolve identically to the Value path.
            let mut fields: Vec<(String, String)> =
                fields.iter().map(|(f, t)| (f.clone(), t.clone())).collect();
            fields.sort();
            Some(Arc::new(move |b| rename_field(b, &fields)))
        }
        // `set`/`redact` only vectorize when every constant is an
        // exactly-representable scalar. A float, a u64 above i64::MAX, or a
        // container has no simple constant Arrow column that round-trips
        // byte-identically to the `Value` path (a u64 would go lossy through
        // f64; an object would become JSON text instead of staying an object),
        // so those fall back to the `Value` path where they are handled
        // correctly. Common cases — a status string, a flag, an int, a null —
        // stay columnar.
        #[cfg(feature = "transform-set")]
        RecordTransform::Set { values } if values.values().all(is_columnar_safe_scalar) => {
            let values = values.clone();
            Some(Arc::new(move |b| set(b, &values)))
        }
        #[cfg(feature = "transform-redact")]
        RecordTransform::Redact { fields, mask } if is_columnar_safe_scalar(mask) => {
            let fields = fields.clone();
            let mask = mask.clone();
            Some(Arc::new(move |b| redact(b, &fields, &mask)))
        }
        _ => None,
    }
}

/// Whether `v` is a scalar that [`constant_column`] can represent as an Arrow
/// column that round-trips byte-identically to the `Value` path: a bool, an
/// integer that fits `i64`, a string, or null. A float, a `u64` above
/// `i64::MAX`, or any container is **not** safe (see the gate in
/// [`batch_form`]).
#[cfg(any(feature = "transform-set", feature = "transform-redact"))]
fn is_columnar_safe_scalar(v: &serde_json::Value) -> bool {
    use serde_json::Value;
    match v {
        Value::Bool(_) | Value::Null | Value::String(_) => true,
        Value::Number(n) => n.is_i64(),
        _ => false,
    }
}

/// Column index of `name` in `schema`, if present.
#[cfg(any(
    feature = "transform-select",
    feature = "transform-drop",
    feature = "transform-rename-field",
    feature = "transform-set",
    feature = "transform-redact"
))]
fn index_of(schema: &Schema, name: &str) -> Option<usize> {
    schema.fields().iter().position(|f| f.name() == name)
}

/// `select`: keep the named columns in `fields` order, skipping any not in the
/// batch — matching [`crate::transform`]'s `select_fields`.
#[cfg(feature = "transform-select")]
fn select(batch: RecordBatch, fields: &[String]) -> Result<RecordBatch, FaucetError> {
    let schema = batch.schema();
    let indices: Vec<usize> = fields.iter().filter_map(|f| index_of(&schema, f)).collect();
    batch
        .project(&indices)
        .map_err(|e| FaucetError::Transform(format!("columnar select: {e}")))
}

/// `drop`: remove the named columns, keeping the rest in their original order —
/// matching `drop_fields`.
#[cfg(feature = "transform-drop")]
fn drop(batch: RecordBatch, fields: &[String]) -> Result<RecordBatch, FaucetError> {
    let schema = batch.schema();
    let remove: std::collections::HashSet<&str> = fields.iter().map(String::as_str).collect();
    let keep: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !remove.contains(f.name().as_str()))
        .map(|(i, _)| i)
        .collect();
    batch
        .project(&keep)
        .map_err(|e| FaucetError::Transform(format!("columnar drop: {e}")))
}

/// `rename_field`: rename columns against a snapshot of the original schema, so
/// chains (`{a:b, b:c}`) and swaps (`{a:b, b:a}`) are order-independent and the
/// same collision rules as `rename_field` in [`crate::transform`] apply.
#[cfg(feature = "transform-rename-field")]
fn rename_field(
    batch: RecordBatch,
    fields: &[(String, String)],
) -> Result<RecordBatch, FaucetError> {
    let schema = batch.schema();
    // Only renames whose source column exists and that actually change the name.
    let renames: Vec<(&str, &str)> = fields
        .iter()
        .filter(|(from, to)| from != to && index_of(&schema, from).is_some())
        .map(|(from, to)| (from.as_str(), to.as_str()))
        .collect();
    let sources: std::collections::HashSet<&str> = renames.iter().map(|(f, _)| *f).collect();

    let mut seen_targets: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for (from, to) in &renames {
        if !seen_targets.insert(to) {
            return Err(FaucetError::Transform(format!(
                "rename_field: two fields rename to the same target key '{to}'"
            )));
        }
        if index_of(&schema, to).is_some() && !sources.contains(to) {
            return Err(FaucetError::Transform(format!(
                "rename_field: target key '{to}' already exists on the record \
                 (renaming from '{from}')"
            )));
        }
    }
    let map: std::collections::HashMap<&str, &str> = renames.iter().copied().collect();
    let new_fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|f| {
            let name = map.get(f.name().as_str()).copied().unwrap_or(f.name());
            Arc::new(Field::new(name, f.data_type().clone(), f.is_nullable()))
        })
        .collect();
    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, batch.columns().to_vec())
        .map_err(|e| FaucetError::Transform(format!("columnar rename_field: {e}")))
}

/// `set`: add or overwrite each `values` key as a constant column (the same
/// literal in every row) — matching `set_fields`. A key that already exists is
/// overwritten in place, preserving column position; a new key is appended.
#[cfg(feature = "transform-set")]
fn set(
    batch: RecordBatch,
    values: &serde_json::Map<String, serde_json::Value>,
) -> Result<RecordBatch, FaucetError> {
    let mut schema_fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let rows = batch.num_rows();
    for (k, v) in values {
        let (field, array) = constant_column(k, v, rows)?;
        match index_of(&batch.schema(), k) {
            Some(i) => {
                schema_fields[i] = Arc::new(field);
                columns[i] = array;
            }
            None => {
                schema_fields.push(Arc::new(field));
                columns.push(array);
            }
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(schema_fields)), columns)
        .map_err(|e| FaucetError::Transform(format!("columnar set: {e}")))
}

/// `redact`: overwrite each named column with the constant `mask` value, only
/// for columns present in the batch — matching `redact_fields`.
#[cfg(feature = "transform-redact")]
fn redact(
    batch: RecordBatch,
    fields: &[String],
    mask: &serde_json::Value,
) -> Result<RecordBatch, FaucetError> {
    let mut schema_fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let rows = batch.num_rows();
    for f in fields {
        if let Some(i) = index_of(&batch.schema(), f) {
            let (field, array) = constant_column(f, mask, rows)?;
            schema_fields[i] = Arc::new(field);
            columns[i] = array;
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(schema_fields)), columns)
        .map_err(|e| FaucetError::Transform(format!("columnar redact: {e}")))
}

#[cfg(any(feature = "transform-set", feature = "transform-redact"))]
/// Build a length-`rows` column holding the constant JSON scalar `v`, typed so
/// that `record_batch_to_values` reproduces `v` exactly. Non-scalar or null
/// values are rendered as their JSON text in a Utf8 column — the same shape the
/// `Value` path would round-trip through arrow-json, and never a silent type
/// change of a neighbouring column.
fn constant_column(
    name: &str,
    v: &serde_json::Value,
    rows: usize,
) -> Result<(Field, ArrayRef), FaucetError> {
    use arrow::array::{BooleanArray, Int64Array, StringArray};
    use serde_json::Value;
    // Only the exactly-representable scalar kinds reach here — `batch_form`
    // gates `set`/`redact` on `is_columnar_safe_scalar`, so a float, a wide
    // integer, or a container has already fallen back to the `Value` path.
    let (dt, arr): (DataType, ArrayRef) = match v {
        Value::Bool(b) => (
            DataType::Boolean,
            Arc::new(BooleanArray::from(vec![*b; rows])),
        ),
        Value::Number(n) if n.is_i64() => (
            DataType::Int64,
            Arc::new(Int64Array::from(vec![n.as_i64().unwrap(); rows])),
        ),
        Value::String(s) => (
            DataType::Utf8,
            Arc::new(StringArray::from(vec![s.clone(); rows])),
        ),
        // Null → an all-null Utf8 column, which round-trips to `key: null`.
        Value::Null => (
            DataType::Utf8,
            Arc::new(StringArray::from(vec![None::<String>; rows])),
        ),
        // Unreachable: gated out by `is_columnar_safe_scalar`. Fail loudly
        // rather than silently corrupt if that gate is ever weakened.
        other => {
            return Err(FaucetError::Transform(format!(
                "columnar set/redact: value {other} is not a columnar-safe scalar                  (should have fallen back to the Value path)"
            )));
        }
    };
    Ok((Field::new(name, dt, true), arr))
}

#[cfg(test)]
mod tests {
    use super::*;
    // Only the feature-gated kernel tests below use these; a bare `arrow` build
    // compiles none of them.
    #[cfg(any(
        feature = "transform-select",
        feature = "transform-drop",
        feature = "transform-rename-field",
        feature = "transform-set",
        feature = "transform-redact"
    ))]
    use crate::columnar::{record_batch_to_values, values_to_record_batch_inferred};
    #[cfg(any(
        feature = "transform-select",
        feature = "transform-drop",
        feature = "transform-rename-field",
        feature = "transform-set",
        feature = "transform-redact"
    ))]
    use crate::stage::{CompiledStage, TransformStage, apply_stages_to_page, compile_stage};
    use crate::transform::RecordTransform;
    #[cfg(any(
        feature = "transform-select",
        feature = "transform-drop",
        feature = "transform-rename-field",
        feature = "transform-set",
        feature = "transform-redact"
    ))]
    use serde_json::{Value, json};

    /// The whole point of this module: the columnar kernel must produce exactly
    /// what running the `Value` stage on the same batch produces. Both start
    /// from `records`, so any divergence is the kernel's.
    #[cfg(any(
        feature = "transform-select",
        feature = "transform-drop",
        feature = "transform-rename-field",
        feature = "transform-set",
        feature = "transform-redact"
    ))]
    fn assert_parity(records: Vec<Value>, t: RecordTransform) {
        let batch = values_to_record_batch_inferred(&records).expect("to batch");

        // Columnar path: kernel over the batch, then materialize.
        let kernel = batch_form(&t).expect("t has a batch form");
        let columnar = record_batch_to_values(&kernel(batch.clone()).expect("kernel"))
            .expect("kernel result to values");

        // Value path: materialize the SAME batch, then run the Value stage.
        let via_value_input = record_batch_to_values(&batch).expect("batch to values");
        let compiled: Vec<CompiledStage> =
            vec![compile_stage(&TransformStage::Map(t)).expect("compile")];
        let value = apply_stages_to_page(via_value_input, &compiled).expect("value stage");

        assert_eq!(
            columnar, value,
            "columnar kernel diverged from the Value stage"
        );
    }

    #[cfg(any(
        feature = "transform-select",
        feature = "transform-drop",
        feature = "transform-rename-field",
        feature = "transform-set",
        feature = "transform-redact"
    ))]
    fn corpus() -> Vec<Value> {
        vec![
            json!({ "id": 1, "name": "ada", "email": "a@x.io", "score": 1.5, "ok": true }),
            json!({ "id": 2, "name": "grace", "email": null, "score": 2.5, "ok": false }),
        ]
    }

    #[cfg(feature = "transform-select")]
    #[test]
    fn select_matches_the_value_path_and_keeps_field_order() {
        assert_parity(
            corpus(),
            RecordTransform::Select {
                // Reordered vs the batch, and one column that does not exist.
                fields: vec!["name".into(), "id".into(), "missing".into()],
            },
        );
        // The projection really is in `fields` order, not schema order.
        let batch = values_to_record_batch_inferred(&corpus()).unwrap();
        let kernel = batch_form(&RecordTransform::Select {
            fields: vec!["name".into(), "id".into()],
        })
        .unwrap();
        let out = kernel(batch).unwrap();
        let out_schema = out.schema();
        let names: Vec<&str> = out_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, vec!["name", "id"]);
    }

    #[cfg(feature = "transform-drop")]
    #[test]
    fn drop_matches_the_value_path() {
        assert_parity(
            corpus(),
            RecordTransform::Drop {
                fields: vec!["email".into(), "score".into(), "missing".into()],
            },
        );
    }

    #[cfg(feature = "transform-set")]
    #[test]
    fn set_matches_the_value_path_for_new_and_overwritten_keys() {
        let mut values = serde_json::Map::new();
        values.insert("stage".into(), json!("prod")); // new column
        values.insert("ok".into(), json!(false)); // overwrite existing
        values.insert("n".into(), json!(42)); // new int column
        assert_parity(corpus(), RecordTransform::Set { values });
    }

    #[cfg(feature = "transform-redact")]
    #[test]
    fn redact_matches_the_value_path() {
        assert_parity(
            corpus(),
            RecordTransform::Redact {
                fields: vec!["email".into(), "name".into(), "missing".into()],
                mask: json!("***"),
            },
        );
    }

    #[cfg(feature = "transform-rename-field")]
    #[test]
    fn rename_field_matches_the_value_path() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("name".to_string(), "full_name".to_string());
        fields.insert("email".to_string(), "contact".to_string());
        assert_parity(corpus(), RecordTransform::RenameField { fields });
    }

    /// A swap (`{a:b, b:a}`) must be order-independent and match the Value
    /// path's snapshot semantics — the case a naive sequential rename corrupts.
    #[cfg(feature = "transform-rename-field")]
    #[test]
    fn rename_field_swap_matches_the_value_path() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("id".to_string(), "name".to_string());
        fields.insert("name".to_string(), "id".to_string());
        assert_parity(corpus(), RecordTransform::RenameField { fields });
    }

    /// Two fields renaming to the *same* target must error, matching the Value
    /// path — otherwise one silently clobbers the other.
    #[cfg(feature = "transform-rename-field")]
    #[test]
    fn rename_field_two_to_one_target_errors_like_the_value_path() {
        let batch = values_to_record_batch_inferred(&corpus()).unwrap();
        let mut fields = std::collections::HashMap::new();
        fields.insert("id".to_string(), "merged".to_string());
        fields.insert("name".to_string(), "merged".to_string());
        let err = batch_form(&RecordTransform::RenameField { fields }).unwrap()(batch)
            .expect_err("two renames to one target must error");
        assert!(err.to_string().contains("same target key"), "{err}");
    }

    /// `set` vectorizes every exactly-representable scalar kind — int, string,
    /// bool, null — byte-identically to the Value path.
    #[cfg(feature = "transform-set")]
    #[test]
    fn set_covers_every_columnar_safe_scalar_kind() {
        let mut values = serde_json::Map::new();
        values.insert("n".into(), json!(7)); // Int64
        values.insert("label".into(), json!("prod")); // Utf8
        values.insert("flag".into(), json!(true)); // Boolean
        values.insert("cleared".into(), Value::Null); // null → nullable Utf8
        assert_parity(corpus(), RecordTransform::Set { values });
    }

    /// The correctness gate: a `set` whose value is NOT an exactly-representable
    /// scalar (a float, a u64 above i64::MAX, or a container) must NOT vectorize
    /// — it falls back to the Value path, which handles it losslessly. Without
    /// this gate the columnar kernel silently made u64::MAX a lossy float and a
    /// nested object a JSON string.
    #[cfg(feature = "transform-set")]
    #[test]
    fn set_with_an_unsafe_constant_falls_back_to_the_value_path() {
        for v in [
            json!(1.5),
            json!(u64::MAX),
            json!({ "a": [1, 2] }),
            json!([1, 2]),
        ] {
            let mut values = serde_json::Map::new();
            values.insert("x".into(), v.clone());
            assert!(
                batch_form(&RecordTransform::Set { values }).is_none(),
                "set of {v} must fall back to Value, not vectorize losslessly"
            );
        }
    }

    /// `redact` likewise only vectorizes a safe scalar mask.
    #[cfg(feature = "transform-redact")]
    #[test]
    fn redact_with_an_unsafe_mask_falls_back_to_the_value_path() {
        assert!(
            batch_form(&RecordTransform::Redact {
                fields: vec!["email".into()],
                mask: json!({ "hidden": true }),
            })
            .is_none()
        );
    }

    /// A rename onto an occupied, non-renamed key must error identically to the
    /// Value path rather than silently clobbering a column.
    #[cfg(feature = "transform-rename-field")]
    #[test]
    fn rename_field_collision_errors_like_the_value_path() {
        let batch = values_to_record_batch_inferred(&corpus()).unwrap();
        let mut fields = std::collections::HashMap::new();
        fields.insert("id".to_string(), "name".to_string()); // `name` already exists, not renamed away
        let err = batch_form(&RecordTransform::RenameField { fields }).unwrap()(batch)
            .expect_err("collision must error");
        assert!(err.to_string().contains("already exists"), "{err}");
    }

    /// Value-inspecting and opaque transforms have no kernel yet, so they must
    /// return `None` and hold the chain on the `Value` path — a wrong `Some`
    /// here would silently run an unimplemented columnar transform.
    #[test]
    fn non_vectorizable_transforms_have_no_batch_form() {
        assert!(
            batch_form(&RecordTransform::Flatten {
                separator: "_".into()
            })
            .is_none()
        );
        #[cfg(feature = "transform-cast")]
        assert!(
            batch_form(&RecordTransform::Cast {
                fields: std::collections::HashMap::new(),
                on_error: Default::default(),
            })
            .is_none(),
            "cast inspects values; it must not claim a kernel until it has one"
        );
    }
}
