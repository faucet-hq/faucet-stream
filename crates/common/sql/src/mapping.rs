//! Column mapping: which record field lands in which table column.
//!
//! Every SQL sink maps a JSON record onto the live columns of its target. The
//! rules differ only in how a field's spelling is compared with a column name
//! ([`ColumnMatching`]); everything else — a record matching no column, a
//! field present in only some records, rows carrying different column sets —
//! is decided here once.

use serde_json::{Map, Value};

/// How a record field is compared with a table column name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ColumnMatching {
    /// A field names a column only when spelled exactly the same
    /// (PostgreSQL's quoted identifiers).
    Exact,
    /// The exactly-named field first, else the first field equal to the
    /// column ignoring ASCII case (MySQL, SQLite).
    CaseInsensitive,
    /// A field with no exactly-named column folds to the one column equal to
    /// it ignoring ASCII case, unless the record already carries that column
    /// exactly or several columns match it (SQL Server's default collation).
    FoldUnique,
    /// The exactly-named field first, else the field whose upper-case form
    /// names an all-upper-case column (Oracle stores unquoted identifiers
    /// upper-cased).
    UpperFold,
}

/// The column `field` addresses among `columns` under `matching`, looked up
/// from the field's side.
pub fn column_for_field<'a>(
    field: &str,
    columns: &'a [String],
    matching: ColumnMatching,
) -> Option<&'a str> {
    if let Some(c) = columns.iter().find(|c| c.as_str() == field) {
        return Some(c);
    }
    match matching {
        ColumnMatching::Exact => None,
        ColumnMatching::CaseInsensitive => columns
            .iter()
            .find(|c| c.eq_ignore_ascii_case(field))
            .map(String::as_str),
        ColumnMatching::FoldUnique => {
            let mut folded = columns.iter().filter(|c| c.eq_ignore_ascii_case(field));
            match (folded.next(), folded.next()) {
                (Some(c), None) => Some(c.as_str()),
                _ => None,
            }
        }
        ColumnMatching::UpperFold => {
            let upper = field.to_uppercase();
            columns
                .iter()
                .find(|c| c.as_str() == upper && c.as_str() == c.to_uppercase())
                .map(String::as_str)
        }
    }
}

/// One live column of a target table.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TableColumn {
    /// The column name as the database reports it.
    pub name: String,
    /// The dialect's type name for the column (PostgreSQL `udt_name`, MySQL
    /// `DATA_TYPE`, …), or empty when the backend binds without one.
    pub sql_type: String,
}

impl TableColumn {
    /// A column with a type.
    pub fn new(name: impl Into<String>, sql_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            sql_type: sql_type.into(),
        }
    }

    /// A column whose type the backend does not need.
    pub fn untyped(name: impl Into<String>) -> Self {
        Self::new(name, String::new())
    }

    /// The type to bind with, or `None` when the column carries none.
    pub fn sql_type(&self) -> Option<&str> {
        (!self.sql_type.is_empty()).then_some(self.sql_type.as_str())
    }
}

/// The value a record carries for `column`, under `matching`.
pub fn field_for<'a>(
    column: &str,
    columns: &[TableColumn],
    obj: &'a Map<String, Value>,
    matching: ColumnMatching,
) -> Option<&'a Value> {
    if let Some(v) = obj.get(column) {
        return Some(v);
    }
    match matching {
        ColumnMatching::Exact => None,
        ColumnMatching::CaseInsensitive => obj
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(column))
            .map(|(_, v)| v),
        ColumnMatching::FoldUnique => obj
            .iter()
            .rfind(|(k, _)| {
                k.eq_ignore_ascii_case(column)
                    && !columns.iter().any(|c| c.name == **k)
                    && columns
                        .iter()
                        .filter(|c| c.name.eq_ignore_ascii_case(k))
                        .count()
                        == 1
            })
            .map(|(_, v)| v),
        ColumnMatching::UpperFold => {
            if column != column.to_uppercase() {
                return None;
            }
            obj.iter()
                .find(|(k, _)| k.to_uppercase() == column && !columns.iter().any(|c| c.name == **k))
                .map(|(_, v)| v)
        }
    }
}

/// The `(column index, value)` cells a record carries, in table order.
pub fn match_record<'a>(
    columns: &[TableColumn],
    obj: &'a Map<String, Value>,
    matching: ColumnMatching,
) -> Vec<(usize, &'a Value)> {
    columns
        .iter()
        .enumerate()
        .filter_map(|(i, c)| field_for(&c.name, columns, obj, matching).map(|v| (i, v)))
        .collect()
}

/// Record fields that land in no column.
pub fn unmatched_fields<'a>(
    obj: &'a Map<String, Value>,
    columns: &[TableColumn],
    matching: ColumnMatching,
) -> Vec<&'a str> {
    obj.keys()
        .filter(|k| match matching {
            ColumnMatching::Exact => !columns.iter().any(|c| c.name == **k),
            ColumnMatching::CaseInsensitive => {
                !columns.iter().any(|c| c.name.eq_ignore_ascii_case(k))
            }
            ColumnMatching::FoldUnique => {
                !columns.iter().any(|c| c.name == **k)
                    && columns
                        .iter()
                        .filter(|c| c.name.eq_ignore_ascii_case(k))
                        .count()
                        != 1
            }
            ColumnMatching::UpperFold => {
                let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
                column_for_field(k, &names, ColumnMatching::UpperFold).is_none()
            }
        })
        .map(String::as_str)
        .collect()
}

/// `obj` with every field renamed to the column it maps to under
/// [`ColumnMatching::FoldUnique`]; fields matching no column keep their name.
pub fn fold_record(columns: &[String], obj: &Map<String, Value>) -> Map<String, Value> {
    let mut out = Map::with_capacity(obj.len());
    for (key, value) in obj {
        let column = if columns.iter().any(|c| c == key) {
            key.clone()
        } else {
            let mut folded = columns.iter().filter(|c| c.eq_ignore_ascii_case(key));
            match (folded.next(), folded.next()) {
                (Some(c), None) if !obj.contains_key(c.as_str()) => c.clone(),
                _ => key.clone(),
            }
        };
        out.insert(column, value.clone());
    }
    out
}

/// One record matched to the target's columns.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct MatchedRow<'a> {
    /// The record's position in the page.
    pub index: usize,
    /// `(column index, value)` in table order.
    pub cells: Vec<(usize, &'a Value)>,
}

impl<'a> MatchedRow<'a> {
    /// The value for the column at `column`, if the record carries it.
    pub fn value(&self, column: usize) -> Option<&'a Value> {
        self.cells
            .iter()
            .find(|(c, _)| *c == column)
            .map(|(_, v)| *v)
    }

    /// Whether the record carries the column at `column`.
    pub fn carries(&self, column: usize) -> bool {
        self.cells.iter().any(|(c, _)| *c == column)
    }
}

/// Why a page could not be mapped.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MapError {
    /// The record at `index` is not a JSON object.
    NotObject {
        /// The record's position in the page.
        index: usize,
    },
    /// The record at `index` carries no field naming a column.
    NoMatch {
        /// The record's position in the page.
        index: usize,
    },
}

/// Match every record of a page, failing on the first record that is not an
/// object or matches no column.
pub fn map_records<'a>(
    columns: &[TableColumn],
    records: &'a [Value],
    matching: ColumnMatching,
) -> Result<Vec<MatchedRow<'a>>, MapError> {
    records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            let obj = record.as_object().ok_or(MapError::NotObject { index })?;
            let cells = match_record(columns, obj, matching);
            if cells.is_empty() {
                return Err(MapError::NoMatch { index });
            }
            Ok(MatchedRow { index, cells })
        })
        .collect()
}

/// Column indices carried by at least one row, in table order: the column set
/// of an append, where a row missing one binds NULL.
pub fn union_columns(rows: &[MatchedRow<'_>], width: usize) -> Vec<usize> {
    (0..width)
        .filter(|i| rows.iter().any(|r| r.carries(*i)))
        .collect()
}

/// Rows grouped by the exact column set each carries, in first-seen order:
/// an upsert of one group never names a column its rows omit, so an absent
/// column keeps its stored value instead of becoming NULL.
pub fn group_by_present<'r, 'a>(
    rows: &'r [MatchedRow<'a>],
    width: usize,
) -> Vec<(Vec<usize>, Vec<&'r MatchedRow<'a>>)> {
    let mut groups: Vec<(Vec<usize>, Vec<&'r MatchedRow<'a>>)> = Vec::new();
    for row in rows {
        let present: Vec<usize> = (0..width).filter(|i| row.carries(*i)).collect();
        match groups.iter_mut().find(|(p, _)| *p == present) {
            Some((_, members)) => members.push(row),
            None => groups.push((present, vec![row])),
        }
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cols(names: &[&str]) -> Vec<TableColumn> {
        names.iter().map(|n| TableColumn::untyped(*n)).collect()
    }

    fn obj(v: Value) -> Map<String, Value> {
        v.as_object().cloned().unwrap()
    }

    #[test]
    fn exact_matching_ignores_case_variants() {
        let c = cols(&["id", "Name"]);
        let o = obj(json!({"id": 1, "name": "x"}));
        let m = match_record(&c, &o, ColumnMatching::Exact);
        assert_eq!(m, vec![(0, &json!(1))]);
        assert_eq!(
            unmatched_fields(&o, &c, ColumnMatching::Exact),
            vec!["name"]
        );
    }

    #[test]
    fn case_insensitive_prefers_the_exact_field() {
        let c = cols(&["Name"]);
        let o = obj(json!({"name": "lower", "Name": "exact"}));
        assert_eq!(
            match_record(&c, &o, ColumnMatching::CaseInsensitive),
            vec![(0, &json!("exact"))]
        );
        let o = obj(json!({"NAME": "upper"}));
        assert_eq!(
            match_record(&c, &o, ColumnMatching::CaseInsensitive),
            vec![(0, &json!("upper"))]
        );
        assert!(unmatched_fields(&o, &c, ColumnMatching::CaseInsensitive).is_empty());
    }

    #[test]
    fn fold_unique_refuses_ambiguous_and_shadowed_fields() {
        let c = cols(&["UserID", "a", "A"]);
        let o = obj(json!({"userid": 1, "a": 2}));
        assert_eq!(
            match_record(&c, &o, ColumnMatching::FoldUnique),
            vec![(0, &json!(1)), (1, &json!(2))]
        );
        let o = obj(json!({"userid": 1, "UserID": 2}));
        assert_eq!(
            match_record(&c, &o, ColumnMatching::FoldUnique),
            vec![(0, &json!(2))]
        );
        let o = obj(json!({"zz": 1}));
        assert_eq!(
            unmatched_fields(&o, &c, ColumnMatching::FoldUnique),
            vec!["zz"]
        );
        let names: Vec<String> = c.iter().map(|c| c.name.clone()).collect();
        assert_eq!(
            Value::Object(fold_record(&names, &obj(json!({"userid": 1, "kind": 2})))),
            json!({"UserID": 1, "kind": 2})
        );
    }

    #[test]
    fn upper_fold_reaches_only_all_upper_columns() {
        let c = cols(&["ID", "Mixed"]);
        let o = obj(json!({"id": 1, "mixed": 2}));
        assert_eq!(
            match_record(&c, &o, ColumnMatching::UpperFold),
            vec![(0, &json!(1))]
        );
        assert_eq!(
            unmatched_fields(&o, &c, ColumnMatching::UpperFold),
            vec!["mixed"]
        );
        let names: Vec<String> = c.iter().map(|c| c.name.clone()).collect();
        assert_eq!(
            column_for_field("id", &names, ColumnMatching::UpperFold),
            Some("ID")
        );
        assert_eq!(
            column_for_field("mixed", &names, ColumnMatching::UpperFold),
            None
        );
        assert_eq!(
            column_for_field("ID", &names, ColumnMatching::Exact),
            Some("ID")
        );
        assert_eq!(column_for_field("id", &names, ColumnMatching::Exact), None);
        assert_eq!(
            column_for_field("mixed", &names, ColumnMatching::CaseInsensitive),
            Some("Mixed")
        );
        let dup = vec!["a".to_string(), "A".to_string()];
        assert_eq!(
            column_for_field("x", &dup, ColumnMatching::FoldUnique),
            None
        );
        assert_eq!(
            column_for_field("aa", &dup, ColumnMatching::FoldUnique),
            None
        );
        assert_eq!(
            column_for_field("mixed", &names, ColumnMatching::FoldUnique),
            Some("Mixed")
        );
    }

    #[test]
    fn mapping_refuses_non_objects_and_unmatched_records() {
        let c = cols(&["id"]);
        let ok = vec![json!({"id": 1})];
        assert_eq!(
            map_records(&c, &ok, ColumnMatching::Exact).unwrap().len(),
            1
        );
        let bad = vec![json!({"id": 1}), json!(3)];
        assert_eq!(
            map_records(&c, &bad, ColumnMatching::Exact),
            Err(MapError::NotObject { index: 1 })
        );
        let none = vec![json!({"zz": 1})];
        assert_eq!(
            map_records(&c, &none, ColumnMatching::Exact),
            Err(MapError::NoMatch { index: 0 })
        );
    }

    #[test]
    fn union_and_groups_follow_carried_columns() {
        let c = cols(&["id", "a", "b"]);
        let records = vec![
            json!({"id": 1, "a": 1}),
            json!({"id": 2, "b": null}),
            json!({"id": 3, "a": 3}),
        ];
        let rows = map_records(&c, &records, ColumnMatching::Exact).unwrap();
        assert_eq!(union_columns(&rows, 3), vec![0, 1, 2]);
        let groups = group_by_present(&rows, 3);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0, vec![0, 1]);
        assert_eq!(groups[0].1.len(), 2);
        assert_eq!(groups[1].0, vec![0, 2]);
        assert_eq!(rows[1].value(2), Some(&Value::Null));
        assert!(rows[1].value(1).is_none());
        assert_eq!(TableColumn::new("x", "int4").sql_type(), Some("int4"));
        assert_eq!(TableColumn::untyped("x").sql_type(), None);
    }
}
