//! How a record field addresses a destination column.
//!
//! A sink maps each record field onto one of its columns by a rule that
//! depends on the backend (PostgreSQL compares quoted names exactly, MySQL
//! ignores case, Oracle stores unquoted names upper-cased). Schema-drift
//! handling must resolve fields the same way the write path does, or it
//! reports a case variant of an existing column as a new one. A sink names
//! its rule through [`crate::Sink::column_matching`]; both sides look fields up
//! with [`column_for_field`] / [`field_for_column`].

use serde_json::{Map, Value};

/// How a record field is compared with a destination column name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum ColumnMatching {
    /// A field names a column only when spelled exactly the same
    /// (PostgreSQL's quoted identifiers).
    #[default]
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

/// The field (and its value) whose value lands in `column` when a record
/// carries several fields that resolve to it: the exactly-named field first,
/// else the one the write path picks under `matching`.
pub fn field_for_column<'a>(
    column: &str,
    columns: &[String],
    obj: &'a Map<String, Value>,
    matching: ColumnMatching,
) -> Option<(&'a str, &'a Value)> {
    if let Some((k, v)) = obj.get_key_value(column) {
        return Some((k.as_str(), v));
    }
    let found = match matching {
        ColumnMatching::Exact => None,
        ColumnMatching::CaseInsensitive => obj.iter().find(|(k, _)| k.eq_ignore_ascii_case(column)),
        ColumnMatching::FoldUnique => obj.iter().rfind(|(k, _)| {
            k.eq_ignore_ascii_case(column)
                && !columns.iter().any(|c| c == *k)
                && columns.iter().filter(|c| c.eq_ignore_ascii_case(k)).count() == 1
        }),
        ColumnMatching::UpperFold => {
            if column != column.to_uppercase() {
                return None;
            }
            obj.iter()
                .find(|(k, _)| k.to_uppercase() == column && !columns.iter().any(|c| c == *k))
        }
    };
    found.map(|(k, v)| (k.as_str(), v))
}

/// How one record field resolves against the destination's columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Resolution<'c> {
    /// The field's value lands in this column.
    Column(&'c str),
    /// The field addresses this column, but another field of the record is
    /// the one written there (an exact twin), so this one is not written.
    Shadowed(&'c str),
    /// The field addresses no column.
    Unmatched,
}

/// Resolve every field of `obj` against `columns` the way the write path does.
pub(crate) fn resolve_fields<'r, 'c>(
    obj: &'r Map<String, Value>,
    columns: &'c [String],
    matching: ColumnMatching,
) -> impl Iterator<Item = (&'r str, &'r Value, Resolution<'c>)> {
    obj.iter().map(move |(field, value)| {
        let resolution = match column_for_field(field, columns, matching) {
            None => Resolution::Unmatched,
            Some(column) if column == field => Resolution::Column(column),
            Some(column) => match field_for_column(column, columns, obj, matching) {
                Some((winner, _)) if winner == field => Resolution::Column(column),
                _ => Resolution::Shadowed(column),
            },
        };
        (field.as_str(), value, resolution)
    })
}

/// `obj` keyed the way the destination sees it: each written field under its
/// column's spelling, an unmatched field under its own name, and a field
/// shadowed by an exactly-named twin dropped.
pub fn canonical_record(
    obj: &Map<String, Value>,
    columns: &[String],
    matching: ColumnMatching,
) -> Map<String, Value> {
    let mut out = Map::with_capacity(obj.len());
    for (field, value, resolution) in resolve_fields(obj, columns, matching) {
        match resolution {
            Resolution::Column(c) => {
                out.insert(c.to_owned(), value.clone());
            }
            Resolution::Unmatched => {
                out.entry(field.to_owned()).or_insert_with(|| value.clone());
            }
            Resolution::Shadowed(_) => {}
        }
    }
    out
}

#[cfg(test)]
mod tests;
