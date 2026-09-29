//! Inbuilt `zip_columns` transform (#551): turn a **columnar payload** —
//! `{ columns: [{name}, …], rows: [[v0, v1, …], …] }` — into one object per row,
//! keyed by column name.
//!
//! Analytics / report APIs (e.g. a query-language `tableData` payload) return results
//! positionally: a list of column descriptors plus a list of value-arrays. This
//! transform zips each row against the column names so downstream stages and
//! sinks see ordinary `{col: value}` records. It is expressible today via the
//! DuckDB `sql` transform, but a small declarative transform is cleaner and
//! needs no embedded engine.
//!
//! The whole module is gated by `#[cfg(feature = "transform-zip-columns")]` at
//! the `mod` site in `lib.rs`. It routes through
//! [`TransformStage::PageFn`](crate::stage::TransformStage) (page-level, 1→0..N,
//! fallible) so a row whose width doesn't match the column count fails loudly
//! rather than silently dropping or misaligning fields.

use crate::FaucetError;
use crate::stage::TransformStage;
use crate::util::extract_records;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::Arc;

/// User-facing `zip_columns` config.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ZipColumnsSpec {
    /// JSONPath to the column **names**. Point it at the name of each column
    /// descriptor (`columns[*].name`) or at a plain array of strings
    /// (`columns`). Every matched value must be a string. Set exactly one of
    /// `columns_path` or `groups`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub columns_path: String,
    /// JSONPath to the **rows**. With `columns_path`, an array of positional
    /// value-arrays (`rows` or `rows[*]`), each exactly as wide as the column
    /// list. With `groups`, the row objects (`rows[*]`).
    pub rows_path: String,
    /// Column groups (#746): each row object holds several positional cell
    /// arrays, each named by its own header list (a `runReport`-style
    /// `dimensionValues` / `metricValues` pair). Every group is zipped on its own and
    /// the results are merged into one record; two groups naming the same
    /// column is an error.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<ColumnGroupSpec>,
}

/// One positional column group of a [`ZipColumnsSpec`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ColumnGroupSpec {
    /// Field (dot path) of each row holding this group's cell array
    /// (`dimensionValues`). A row without it fails the page.
    pub from: String,
    /// JSONPath, evaluated against the **record**, to this group's header list
    /// (`$.dimensionHeaders[*].name`, or `$.dimensionHeaders[*]` with
    /// `header_label`).
    pub header: String,
    /// Field of each header object to use as the column name, when `header`
    /// matches objects rather than strings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header_label: Option<String>,
    /// Field (dot path) of each cell holding the value (`value`). Omit when
    /// the cells are the values themselves. A cell without it yields `null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

impl ZipColumnsSpec {
    /// Validate the spec, returning a reusable [`CompiledZipColumns`].
    pub fn compile(&self) -> Result<CompiledZipColumns, FaucetError> {
        CompiledZipColumns::compile(self)
    }

    /// Compile and wrap as a [`TransformStage::PageFn`] (1→0..N per record,
    /// fallible on a row/column width mismatch).
    pub fn into_stage(&self) -> Result<TransformStage, FaucetError> {
        let compiled = self.compile()?;
        Ok(TransformStage::PageFn(Arc::new(move |page: Vec<Value>| {
            let mut out = Vec::with_capacity(page.len());
            for rec in page {
                out.extend(compiled.apply(&rec)?);
            }
            Ok(out)
        })))
    }
}

/// Validated [`ZipColumnsSpec`] — apply per record with [`CompiledZipColumns::apply`].
#[derive(Debug, Clone)]
pub struct CompiledZipColumns {
    columns_path: String,
    rows_path: String,
    groups: Vec<CompiledGroup>,
}

#[derive(Debug, Clone)]
struct CompiledGroup {
    from: String,
    header: String,
    header_label: Option<String>,
    value: Option<String>,
}

impl CompiledZipColumns {
    fn compile(spec: &ZipColumnsSpec) -> Result<Self, FaucetError> {
        let has_columns = !spec.columns_path.trim().is_empty();
        if has_columns != spec.groups.is_empty() {
            return Err(FaucetError::Config(
                "zip_columns: set exactly one of `columns_path` or `groups`".into(),
            ));
        }
        if spec.rows_path.trim().is_empty() {
            return Err(FaucetError::Config(
                "zip_columns: `rows_path` must not be empty".into(),
            ));
        }
        let blank = |s: &str| s.trim().is_empty();
        let mut groups = Vec::with_capacity(spec.groups.len());
        for (i, g) in spec.groups.iter().enumerate() {
            if blank(&g.from) || blank(&g.header) {
                return Err(FaucetError::Config(format!(
                    "zip_columns: group {i} needs a non-empty `from` and `header`"
                )));
            }
            if g.header_label.as_deref().is_some_and(blank) || g.value.as_deref().is_some_and(blank)
            {
                return Err(FaucetError::Config(format!(
                    "zip_columns: group '{}' has an empty `header_label` or `value`",
                    g.from
                )));
            }
            groups.push(CompiledGroup {
                from: g.from.trim().to_string(),
                header: normalize_path(&g.header),
                header_label: g.header_label.clone(),
                value: g.value.clone(),
            });
        }
        Ok(Self {
            columns_path: if has_columns {
                normalize_path(&spec.columns_path)
            } else {
                String::new()
            },
            rows_path: normalize_path(&spec.rows_path),
            groups,
        })
    }

    /// Zip one columnar record into one object per row. A record that carries no
    /// rows produces zero output records; a row whose width differs from the
    /// column count is a hard error (never silently misaligned).
    pub fn apply(&self, rec: &Value) -> Result<Vec<Value>, FaucetError> {
        if !self.groups.is_empty() {
            return self.apply_groups(rec);
        }
        let columns = self.column_names(rec)?;
        let rows = row_candidates(&extract_records(rec, Some(&self.rows_path))?);
        let mut out = Vec::with_capacity(rows.len());
        for (i, row) in rows.into_iter().enumerate() {
            let Value::Array(values) = row else {
                return Err(FaucetError::Transform(format!(
                    "zip_columns: row {i} at `{}` is not an array",
                    self.rows_path
                )));
            };
            if values.len() != columns.len() {
                return Err(FaucetError::Transform(format!(
                    "zip_columns: row {i} has {} value(s) but there are {} column(s)",
                    values.len(),
                    columns.len()
                )));
            }
            let obj: Map<String, Value> = columns.iter().cloned().zip(values).collect();
            out.push(Value::Object(obj));
        }
        Ok(out)
    }

    /// Resolve the column names, requiring every matched value to be a string.
    fn column_names(&self, rec: &Value) -> Result<Vec<String>, FaucetError> {
        let matched = extract_records(rec, Some(&self.columns_path))?;
        // A single match that is itself an array (`columns_path: columns` where
        // columns is already a string array) is unwrapped to its elements.
        let candidates = column_candidates(&matched);
        let mut names = Vec::with_capacity(candidates.len());
        for c in candidates {
            match c {
                Value::String(s) => names.push(s),
                other => {
                    return Err(FaucetError::Transform(format!(
                        "zip_columns: column name at `{}` is not a string: {other}",
                        self.columns_path
                    )));
                }
            }
        }
        if names.is_empty() {
            return Err(FaucetError::Transform(format!(
                "zip_columns: `columns_path` `{}` matched no column names",
                self.columns_path
            )));
        }
        Ok(names)
    }

    /// The `groups` form: every group's cells are zipped against its own
    /// header list and the groups are merged into one record per row.
    fn apply_groups(&self, rec: &Value) -> Result<Vec<Value>, FaucetError> {
        let mut headers: Vec<Vec<String>> = Vec::with_capacity(self.groups.len());
        let mut owner: Map<String, Value> = Map::new();
        for g in &self.groups {
            let names = g.header_names(rec)?;
            for n in &names {
                if let Some(Value::String(prev)) =
                    owner.insert(n.clone(), Value::String(g.from.clone()))
                {
                    let which = if prev == g.from {
                        format!("group '{prev}' names it twice")
                    } else {
                        format!("groups '{prev}' and '{}' both name it", g.from)
                    };
                    return Err(FaucetError::Transform(format!(
                        "zip_columns: duplicate column '{n}': {which}"
                    )));
                }
            }
            headers.push(names);
        }
        let matched = extract_records(rec, Some(&self.rows_path))?;
        let rows = match matched.as_slice() {
            [Value::Array(inner)] => inner.clone(),
            _ => matched,
        };
        let mut out = Vec::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            if !row.is_object() {
                return Err(FaucetError::Transform(format!(
                    "zip_columns: row {i} at `{}` is not an object",
                    self.rows_path
                )));
            }
            let mut obj = Map::new();
            for (g, names) in self.groups.iter().zip(&headers) {
                let Some(Value::Array(cells)) = path_get(row, &g.from) else {
                    return Err(FaucetError::Transform(format!(
                        "zip_columns: row {i} has no `{}` array (group '{}')",
                        g.from, g.from
                    )));
                };
                if cells.len() != names.len() {
                    return Err(FaucetError::Transform(format!(
                        "zip_columns: row {i}, group '{}': {} cell(s) but {} header(s)",
                        g.from,
                        cells.len(),
                        names.len()
                    )));
                }
                for (name, cell) in names.iter().zip(cells) {
                    let v = match &g.value {
                        Some(field) => path_get(cell, field).cloned().unwrap_or(Value::Null),
                        None => cell.clone(),
                    };
                    obj.insert(name.clone(), v);
                }
            }
            out.push(Value::Object(obj));
        }
        Ok(out)
    }
}

impl CompiledGroup {
    fn header_names(&self, rec: &Value) -> Result<Vec<String>, FaucetError> {
        let matched = extract_records(rec, Some(&self.header))?;
        let mut names = Vec::new();
        for h in column_candidates(&matched) {
            let name = match (&self.header_label, &h) {
                (Some(label), Value::Object(_)) => path_get(&h, label).cloned(),
                (None, _) => Some(h.clone()),
                (Some(_), _) => None,
            };
            match name {
                Some(Value::String(s)) => names.push(s),
                _ => {
                    return Err(FaucetError::Transform(format!(
                        "zip_columns: group '{}': header at `{}` is not a string: {h}",
                        self.from, self.header
                    )));
                }
            }
        }
        Ok(names)
    }
}

/// Resolve a dot path (`a.b`) inside a value.
fn path_get<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(root, |cur, seg| cur.get(seg))
}

/// Accept a bare path (`rows`, `columns[*].name`) by rooting it at `$`, while
/// leaving an already-`$`-rooted expression untouched.
fn normalize_path(path: &str) -> String {
    let p = path.trim();
    if p.starts_with('$') {
        p.to_string()
    } else {
        format!("$.{p}")
    }
}

/// Column candidates: a single array match (`columns_path: columns` pointing at
/// a string array) is unwrapped to its elements; a `columns[*].name`-style match
/// already yields the names directly.
fn column_candidates(matched: &[Value]) -> Vec<Value> {
    match matched {
        [Value::Array(inner)] => inner.clone(),
        other => other.to_vec(),
    }
}

/// Row candidates: `rows` matches the rows array (one match, an array *of
/// arrays*) → unwrap to the rows; `rows[*]` yields each row directly. Unwrapping
/// only when every element is itself an array disambiguates a single-row
/// `rows[*]` (one array of scalars) from the whole rows container.
fn row_candidates(matched: &[Value]) -> Vec<Value> {
    if let [Value::Array(inner)] = matched
        && inner.iter().all(|v| matches!(v, Value::Array(_)))
    {
        return inner.clone();
    }
    matched.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> CompiledZipColumns {
        ZipColumnsSpec {
            columns_path: "columns[*].name".into(),
            rows_path: "rows".into(),
            groups: vec![],
        }
        .compile()
        .unwrap()
    }

    #[test]
    fn zips_columns_into_row_objects() {
        let rec = json!({
            "columns": [{"name": "day"}, {"name": "sessions"}],
            "rows": [["2026-01-01", 12], ["2026-01-02", 7]],
        });
        let out = spec().apply(&rec).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], json!({"day": "2026-01-01", "sessions": 12}));
        assert_eq!(out[1], json!({"day": "2026-01-02", "sessions": 7}));
    }

    #[test]
    fn direct_string_array_columns_and_rows_star() {
        let compiled = ZipColumnsSpec {
            columns_path: "columns".into(),
            rows_path: "rows[*]".into(),
            groups: vec![],
        }
        .compile()
        .unwrap();
        let rec = json!({"columns": ["a", "b"], "rows": [[1, 2]]});
        let out = compiled.apply(&rec).unwrap();
        assert_eq!(out, vec![json!({"a": 1, "b": 2})]);
    }

    #[test]
    fn no_rows_yields_no_records() {
        let rec = json!({"columns": [{"name": "a"}], "rows": []});
        assert!(spec().apply(&rec).unwrap().is_empty());
    }

    #[test]
    fn width_mismatch_errors_clearly() {
        let rec = json!({"columns": [{"name": "a"}, {"name": "b"}], "rows": [[1]]});
        let err = spec().apply(&rec).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("1 value") && msg.contains("2 column"), "{msg}");
    }

    #[test]
    fn non_string_column_name_errors() {
        let rec = json!({"columns": [{"name": 7}], "rows": [[1]]});
        assert!(spec().apply(&rec).is_err());
    }

    #[test]
    fn empty_paths_rejected_at_compile() {
        assert!(
            ZipColumnsSpec {
                columns_path: "".into(),
                rows_path: "rows".into(),
                groups: vec![],
            }
            .compile()
            .is_err()
        );
        assert!(
            ZipColumnsSpec {
                columns_path: "columns".into(),
                rows_path: " ".into(),
                groups: vec![],
            }
            .compile()
            .is_err()
        );
    }

    #[test]
    fn into_stage_is_pagefn_and_flat_maps() {
        let stage = spec_spec().into_stage().unwrap();
        match stage {
            TransformStage::PageFn(f) => {
                let page = vec![json!({
                    "columns": [{"name": "a"}],
                    "rows": [[1], [2]],
                })];
                let out = f(page).unwrap();
                assert_eq!(out, vec![json!({"a": 1}), json!({"a": 2})]);
            }
            other => panic!("expected PageFn, got {other:?}"),
        }
    }

    fn spec_spec() -> ZipColumnsSpec {
        ZipColumnsSpec {
            columns_path: "columns[*].name".into(),
            rows_path: "rows".into(),
            groups: vec![],
        }
    }

    fn grouped_report() -> ZipColumnsSpec {
        serde_json::from_value(json!({
            "rows_path": "$.rows[*]",
            "groups": [
                {"from": "dimensionValues", "header": "$.dimensionHeaders[*].name", "value": "value"},
                {"from": "metricValues", "header": "$.metricHeaders[*]", "header_label": "name", "value": "value"}
            ]
        }))
        .unwrap()
    }

    fn report() -> Value {
        json!({
            "dimensionHeaders": [{"name": "date"}, {"name": "country"}],
            "metricHeaders": [{"name": "sessions", "type": "TYPE_INTEGER"}, {"name": "bounceRate", "type": "TYPE_FLOAT"}],
            "rows": [
                {"dimensionValues": [{"value": "20260901"}, {"value": "DE"}],
                 "metricValues": [{"value": "1204"}, {"value": "0.41"}]},
                {"dimensionValues": [{"value": "20260902"}, {}],
                 "metricValues": [{"value": "9"}, null]}
            ]
        })
    }

    #[test]
    fn groups_zip_ga4_rows() {
        let out = grouped_report().compile().unwrap().apply(&report()).unwrap();
        assert_eq!(
            out,
            vec![
                json!({"date": "20260901", "country": "DE", "sessions": "1204", "bounceRate": "0.41"}),
                json!({"date": "20260902", "country": null, "sessions": "9", "bounceRate": null}),
            ]
        );
    }

    #[test]
    fn groups_with_scalar_cells_and_a_rows_container() {
        let spec: ZipColumnsSpec = serde_json::from_value(json!({
            "rows_path": "rows",
            "groups": [{"from": "a.cells", "header": "names"}]
        }))
        .unwrap();
        let rec = json!({"names": ["x", "y"], "rows": [{"a": {"cells": [1, 2]}}]});
        let out = spec.compile().unwrap().apply(&rec).unwrap();
        assert_eq!(out, vec![json!({"x": 1, "y": 2})]);
    }

    #[test]
    fn groups_edge_cases() {
        let c = grouped_report().compile().unwrap();
        // An empty report (the API omits `rows`) yields nothing.
        assert!(
            c.apply(&json!({"dimensionHeaders": [], "metricHeaders": []}))
                .unwrap()
                .is_empty()
        );
        // Empty headers with empty cells yield an empty record.
        let empty = json!({"dimensionHeaders": [], "metricHeaders": [],
            "rows": [{"dimensionValues": [], "metricValues": []}]});
        assert_eq!(c.apply(&empty).unwrap(), vec![json!({})]);

        let err = |rec: Value| c.apply(&rec).unwrap_err().to_string();
        let mut r = report();
        r["rows"][1]["metricValues"] = json!([{"value": "1"}]);
        let e = err(r);
        assert!(
            e.contains("row 1, group 'metricValues'") && e.contains("1 cell(s) but 2 header(s)"),
            "{e}"
        );

        let mut r = report();
        r["rows"][0].as_object_mut().unwrap().remove("metricValues");
        let e = err(r);
        assert!(e.contains("row 0 has no `metricValues` array"), "{e}");

        let mut r = report();
        r["metricHeaders"][0]["name"] = json!("date");
        let e = err(r);
        assert!(
            e.contains("duplicate column 'date'")
                && e.contains("'dimensionValues' and 'metricValues'"),
            "{e}"
        );

        let mut r = report();
        r["dimensionHeaders"][1]["name"] = json!("date");
        assert!(err(r).contains("group 'dimensionValues' names it twice"));

        let mut r = report();
        r["dimensionHeaders"][0]["name"] = json!(7);
        assert!(err(r).contains("group 'dimensionValues': header"));

        let mut r = report();
        r["metricHeaders"][0] = json!("sessions");
        assert!(err(r).contains("group 'metricValues': header"));

        let mut r = report();
        r["rows"][0] = json!([1]);
        assert!(err(r).contains("row 0 at `$.rows[*]` is not an object"));
    }

    #[test]
    fn groups_compile_validation() {
        let bad = |v: Value| {
            serde_json::from_value::<ZipColumnsSpec>(v)
                .unwrap()
                .compile()
                .unwrap_err()
                .to_string()
        };
        let g = json!({"from": "a", "header": "h"});
        assert!(bad(json!({"rows_path": "r"})).contains("exactly one"));
        assert!(
            bad(json!({"rows_path": "r", "columns_path": "c", "groups": [g]}))
                .contains("exactly one")
        );
        assert!(
            bad(json!({"rows_path": "r", "groups": [{"from": " ", "header": "h"}]}))
                .contains("group 0")
        );
        assert!(
            bad(json!({"rows_path": "r", "groups": [{"from": "a", "header": "h", "value": ""}]}))
                .contains("group 'a'")
        );
        assert!(
            bad(json!({"rows_path": "r", "groups": [{"from": "a", "header": "h", "header_label": " "}]}))
                .contains("group 'a'")
        );
        assert!(bad(json!({"rows_path": " ", "groups": [g]})).contains("rows_path"));
        let s = grouped_report();
        assert_eq!(
            serde_json::from_value::<ZipColumnsSpec>(serde_json::to_value(&s).unwrap()).unwrap(),
            s
        );
    }
}
