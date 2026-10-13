//! Type-aware conversion from Databricks `JSON_ARRAY` result rows to JSON objects.
//!
//! In the Statement Execution API's `JSON_ARRAY` format every cell in
//! `result.data_array` is a JSON **string** (or `null`), regardless of the
//! column's real type, and the column types ship separately in
//! `manifest.schema.columns[].type_name`. This module pairs each string cell
//! with its column type and produces a typed `serde_json::Value::Object` per
//! row.
//!
//! | `type_name`                     | JSON output                                   |
//! |---------------------------------|-----------------------------------------------|
//! | `BOOLEAN`                       | `Bool`                                         |
//! | `BYTE`/`SHORT`/`INT`/`LONG`     | `Number` (i64; kept as `String` if it overflows i64 — full precision) |
//! | `FLOAT`/`DOUBLE`                | `Number` (f64; `NaN`/`Infinity` → counted `null`) |
//! | `DECIMAL`                       | `String` (exact decimal — no f64 precision loss) |
//! | `ARRAY`/`MAP`/`STRUCT`          | parsed nested JSON (falls back to `String`)    |
//! | `STRING`/`DATE`/`TIMESTAMP`/`BINARY`/`CHAR`/`INTERVAL`/… | `String` (raw cell) |
//!
//! A `null` cell maps to `Value::Null` regardless of declared type.

use serde_json::{Map, Value};

/// One entry in `manifest.schema.columns`.
pub use faucet_common_databricks::ResultColumn as ColumnInfo;

/// Build a JSON object from one Databricks row (array of string/null cells).
///
/// A row shorter than `columns` treats missing trailing cells as `null`;
/// extra cells are ignored.
pub fn row_to_json(row: &[Value], columns: &[ColumnInfo]) -> Value {
    let mut obj = Map::with_capacity(columns.len());
    for (i, col) in columns.iter().enumerate() {
        obj.insert(col.name.clone(), cell_to_json(row.get(i), &col.type_name));
    }
    Value::Object(obj)
}

/// Refuse a result with two columns of the same name — a join's `id`, say —
/// whose later cell would otherwise replace the earlier in every record
/// (#862 M-058).
pub fn check_unique_columns(columns: &[ColumnInfo]) -> Result<(), faucet_core::FaucetError> {
    let mut seen = std::collections::HashSet::new();
    for col in columns {
        if !seen.insert(col.name.as_str()) {
            return Err(faucet_core::FaucetError::Source(format!(
                "databricks: the query returns two columns named {:?}; alias them \
                 (`SELECT o.id AS order_id, c.id AS customer_id …`) so neither is lost",
                col.name
            )));
        }
    }
    Ok(())
}

/// Convert a single cell (a JSON string or null) to a typed value.
fn cell_to_json(cell: Option<&Value>, type_name: &str) -> Value {
    let Some(cell) = cell else {
        return Value::Null;
    };
    if cell.is_null() {
        return Value::Null;
    }
    // JSON_ARRAY ships every cell as a string; tolerate an already-typed value
    // defensively (pass through).
    let s = match cell {
        Value::String(s) => s.as_str(),
        other => return other.clone(),
    };

    match type_name.to_ascii_uppercase().as_str() {
        "BOOLEAN" => match s {
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            _ => Value::String(s.to_owned()),
        },
        "BYTE" | "SHORT" | "INT" | "LONG" => parse_int(s),
        "FLOAT" | "DOUBLE" => parse_float(s),
        // DECIMAL is preserved as an exact string (serde_json numbers are f64
        // here, which would lose precision on large/fractional decimals).
        "DECIMAL" => Value::String(s.to_owned()),
        "ARRAY" | "MAP" | "STRUCT" => faucet_core::fidelity::parse_json(s.as_bytes())
            .unwrap_or_else(|_| Value::String(s.to_owned())),
        // STRING / DATE / TIMESTAMP / BINARY / CHAR / INTERVAL / NULL / UDT / …
        _ => Value::String(s.to_owned()),
    }
}

/// Parse an integer cell, keeping full precision: an `i64` fits into a JSON
/// number; anything larger (e.g. a `LONG`/`DECIMAL(38,0)` beyond i64) is kept
/// as a lossless string rather than coerced through `f64`.
fn parse_int(s: &str) -> Value {
    match s.parse::<i64>() {
        Ok(n) => Value::Number(n.into()),
        Err(_) => Value::String(s.to_owned()),
    }
}

/// Parse a float cell. Non-finite values (`NaN`, `Infinity`, `-Infinity`),
/// which JSON cannot represent, become a counted `null` (#885).
fn parse_float(s: &str) -> Value {
    match s.parse::<f64>() {
        Ok(f) => faucet_core::fidelity::float64(f),
        Err(_) => Value::String(s.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The value-fidelity corpus (#885): the wide row, encoded as the
    /// Statement Execution API returns it (every cell a string, a complex cell
    /// its JSON text), decodes back unchanged.
    #[test]
    fn the_fidelity_corpus_decodes_unchanged() {
        use faucet_conformance::fidelity;
        let sent = fidelity::flat_corpus();
        let mut cols = Vec::new();
        let mut cells = Vec::new();
        for (name, v) in sent.as_object().unwrap() {
            let (ty, cell) = match v {
                Value::Null => ("STRING", Value::Null),
                Value::Bool(b) => ("BOOLEAN", json!(b.to_string())),
                Value::Number(n) if n.is_f64() => ("DOUBLE", json!(n.to_string())),
                Value::Number(n) => ("LONG", json!(n.to_string())),
                Value::String(s) if name.starts_with("canonical_decimal") => ("DECIMAL", json!(s)),
                Value::String(s) => ("STRING", json!(s)),
                Value::Array(_) => ("ARRAY", json!(v.to_string())),
                Value::Object(_) => ("STRUCT", json!(v.to_string())),
            };
            cols.push(ColumnInfo {
                name: name.clone(),
                type_name: ty.into(),
            });
            cells.push(cell);
        }
        let landed = row_to_json(&cells, &cols);
        fidelity::assert_round_trip(&[sent], &[landed], fidelity::Tolerance::exact());
    }

    #[test]
    fn duplicate_column_names_are_refused() {
        assert!(check_unique_columns(&cols()).is_ok());
        let mut dup = cols();
        dup.push(dup[0].clone());
        let err = check_unique_columns(&dup).unwrap_err().to_string();
        assert!(err.contains("two columns named \"id\""), "{err}");
    }

    fn cols() -> Vec<ColumnInfo> {
        [
            "id:LONG",
            "price:DECIMAL",
            "ok:BOOLEAN",
            "score:DOUBLE",
            "name:STRING",
            "dt:DATE",
        ]
        .iter()
        .map(|s| {
            let (n, t) = s.split_once(':').unwrap();
            ColumnInfo {
                name: n.into(),
                type_name: t.into(),
            }
        })
        .collect()
    }

    #[test]
    fn decodes_row_by_type() {
        let row = vec![
            json!("42"),
            json!("71433.16"),
            json!("true"),
            json!("1.5"),
            json!("alice"),
            json!("2026-01-01"),
        ];
        let obj = row_to_json(&row, &cols());
        assert_eq!(obj["id"], json!(42));
        assert_eq!(obj["price"], json!("71433.16")); // decimal kept as string
        assert_eq!(obj["ok"], json!(true));
        assert_eq!(obj["score"], json!(1.5));
        assert_eq!(obj["name"], json!("alice"));
        assert_eq!(obj["dt"], json!("2026-01-01"));
    }

    #[test]
    fn null_and_missing_cells_are_null() {
        let row = vec![Value::Null, json!("9.9")]; // shorter than cols
        let obj = row_to_json(&row, &cols());
        assert_eq!(obj["id"], Value::Null);
        assert_eq!(obj["price"], json!("9.9"));
        assert_eq!(obj["ok"], Value::Null); // missing trailing cell
        assert_eq!(obj["dt"], Value::Null);
    }

    #[test]
    fn huge_long_kept_as_string() {
        let c = [ColumnInfo {
            name: "n".into(),
            type_name: "LONG".into(),
        }];
        let obj = row_to_json(&[json!("123456789012345678901234")], &c);
        assert_eq!(obj["n"], json!("123456789012345678901234"));
    }

    #[test]
    fn non_finite_float_is_a_counted_null() {
        let c = [ColumnInfo {
            name: "f".into(),
            type_name: "DOUBLE".into(),
        }];
        assert_eq!(row_to_json(&[json!("NaN")], &c)["f"], Value::Null);
        assert_eq!(row_to_json(&[json!("Infinity")], &c)["f"], Value::Null);
        let c = [ColumnInfo {
            name: "a".into(),
            type_name: "ARRAY".into(),
        }];
        assert_eq!(
            row_to_json(&[json!("[12345678901234567890.5]")], &c)["a"],
            json!(["12345678901234567890.5"])
        );
    }

    #[test]
    fn nested_types_parsed_or_fallback() {
        let c = [ColumnInfo {
            name: "a".into(),
            type_name: "ARRAY".into(),
        }];
        assert_eq!(row_to_json(&[json!("[1,2,3]")], &c)["a"], json!([1, 2, 3]));
        // Non-JSON string falls back to the raw string.
        assert_eq!(row_to_json(&[json!("[oops")], &c)["a"], json!("[oops"));
    }

    #[test]
    fn boolean_and_int_edge_values() {
        let c = [ColumnInfo {
            name: "b".into(),
            type_name: "BOOLEAN".into(),
        }];
        assert_eq!(row_to_json(&[json!("maybe")], &c)["b"], json!("maybe"));
        let ci = [ColumnInfo {
            name: "i".into(),
            type_name: "INT".into(),
        }];
        assert_eq!(
            row_to_json(&[json!("not-a-num")], &ci)["i"],
            json!("not-a-num")
        );
    }
}
