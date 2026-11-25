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
use std::borrow::Cow;
use std::collections::HashMap;

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

/// Two record fields that both address `column` ignoring ASCII case while
/// neither spells it exactly (see [`ColumnResolver::ambiguous_fold`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AmbiguousFold<'r, 'c> {
    /// The column both fields address.
    pub column: &'c str,
    /// The first such field, in record order.
    pub first: &'r str,
    /// The second such field, in record order.
    pub second: &'r str,
}

/// One destination's columns, indexed once so each record field resolves
/// without scanning the column list.
///
/// Every lookup answers exactly as the free functions of this module do for
/// the same columns and rule ([`column_for_field`], [`field_for_column`],
/// [`canonical_record`]).
#[derive(Debug, Clone)]
pub struct ColumnResolver {
    columns: Vec<String>,
    matching: ColumnMatching,
    exact: HashMap<String, usize>,
    folded: HashMap<u64, Vec<usize>>,
    upper: HashMap<String, usize>,
}

fn fold_hash(name: &str) -> u64 {
    name.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b.to_ascii_lowercase())).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

impl ColumnResolver {
    /// Index `columns` (in destination order) for lookups under `matching`.
    pub fn new(columns: Vec<String>, matching: ColumnMatching) -> Self {
        let mut exact = HashMap::with_capacity(columns.len());
        let mut folded: HashMap<u64, Vec<usize>> = HashMap::with_capacity(columns.len());
        let mut upper = HashMap::new();
        for (i, column) in columns.iter().enumerate() {
            exact.entry(column.clone()).or_insert(i);
            folded.entry(fold_hash(column)).or_default().push(i);
            if matching == ColumnMatching::UpperFold && *column == column.to_uppercase() {
                upper.entry(column.clone()).or_insert(i);
            }
        }
        Self {
            columns,
            matching,
            exact,
            folded,
            upper,
        }
    }

    /// The columns, in destination order.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// The rule fields are matched by.
    pub fn matching(&self) -> ColumnMatching {
        self.matching
    }

    fn fold_matches<'s>(&'s self, name: &'s str) -> impl Iterator<Item = usize> + 's {
        self.folded
            .get(&fold_hash(name))
            .into_iter()
            .flatten()
            .copied()
            .filter(move |i| self.columns[*i].eq_ignore_ascii_case(name))
    }

    /// The position of the column `field` addresses.
    pub fn index_of(&self, field: &str) -> Option<usize> {
        if let Some(i) = self.exact.get(field) {
            return Some(*i);
        }
        match self.matching {
            ColumnMatching::Exact => None,
            ColumnMatching::CaseInsensitive => self.fold_matches(field).next(),
            ColumnMatching::FoldUnique => {
                let mut folded = self.fold_matches(field);
                match (folded.next(), folded.next()) {
                    (Some(i), None) => Some(i),
                    _ => None,
                }
            }
            ColumnMatching::UpperFold => self.upper.get(field.to_uppercase().as_str()).copied(),
        }
    }

    /// The column `field` addresses ([`column_for_field`]).
    pub fn column_for_field(&self, field: &str) -> Option<&str> {
        self.index_of(field).map(|i| self.columns[i].as_str())
    }

    /// The field (and its value) of `obj` whose value lands in `column`
    /// ([`field_for_column`]).
    pub fn field_for_column<'a>(
        &self,
        column: &str,
        obj: &'a Map<String, Value>,
    ) -> Option<(&'a str, &'a Value)> {
        if let Some((k, v)) = obj.get_key_value(column) {
            return Some((k.as_str(), v));
        }
        let found = match self.matching {
            ColumnMatching::Exact => None,
            ColumnMatching::CaseInsensitive => {
                obj.iter().find(|(k, _)| k.eq_ignore_ascii_case(column))
            }
            ColumnMatching::FoldUnique => obj.iter().rfind(|(k, _)| {
                k.eq_ignore_ascii_case(column)
                    && !self.exact.contains_key(k.as_str())
                    && self.fold_matches(k).count() == 1
            }),
            ColumnMatching::UpperFold => {
                if column != column.to_uppercase() {
                    return None;
                }
                obj.iter().find(|(k, _)| {
                    k.to_uppercase() == column && !self.exact.contains_key(k.as_str())
                })
            }
        };
        found.map(|(k, v)| (k.as_str(), v))
    }

    fn resolution(&self, field: &str, obj: &Map<String, Value>) -> Resolution<'_> {
        match self.column_for_field(field) {
            None => Resolution::Unmatched,
            Some(column) if column == field => Resolution::Column(column),
            Some(column) => match self.field_for_column(column, obj) {
                Some((winner, _)) if winner == field => Resolution::Column(column),
                _ => Resolution::Shadowed(column),
            },
        }
    }

    /// `obj` keyed the way the destination sees it ([`canonical_record`]),
    /// borrowed when every field already is.
    pub fn canonical_record<'a>(&self, obj: &'a Map<String, Value>) -> Cow<'a, Map<String, Value>> {
        let settled = obj.keys().all(|field| match self.index_of(field) {
            None => true,
            Some(i) => self.columns[i] == *field,
        });
        if settled {
            return Cow::Borrowed(obj);
        }
        let mut out = Map::with_capacity(obj.len());
        for (field, value) in obj {
            match self.resolution(field, obj) {
                Resolution::Column(c) => {
                    out.insert(c.to_owned(), value.clone());
                }
                Resolution::Unmatched => {
                    out.entry(field.to_owned()).or_insert_with(|| value.clone());
                }
                Resolution::Shadowed(_) => {}
            }
        }
        Cow::Owned(out)
    }

    /// The first column (in destination order) that `obj` does not carry
    /// under its exact name while two or more of its fields equal it ignoring
    /// ASCII case: writing either one would drop the other.
    pub fn ambiguous_fold<'r>(&self, obj: &'r Map<String, Value>) -> Option<AmbiguousFold<'r, '_>> {
        let mut folds: Vec<(usize, &'r str, Option<&'r str>)> = Vec::new();
        for field in obj.keys() {
            for i in self.fold_matches(field) {
                if obj.contains_key(self.columns[i].as_str()) {
                    continue;
                }
                match folds.iter_mut().find(|(column, _, _)| *column == i) {
                    Some((_, _, second)) => {
                        second.get_or_insert(field.as_str());
                    }
                    None => folds.push((i, field.as_str(), None)),
                }
            }
        }
        folds
            .into_iter()
            .filter_map(|(i, first, second)| Some((i, first, second?)))
            .min_by_key(|(i, _, _)| *i)
            .map(|(i, first, second)| AmbiguousFold {
                column: self.columns[i].as_str(),
                first,
                second,
            })
    }
}

#[cfg(test)]
mod tests;
