//! A planned statement: SQL text plus the values to bind, in placeholder order.

use serde_json::Value;
use std::borrow::Cow;

/// What a planned statement does. A backend's executor uses it to bind
/// parameters the way that statement needs and to word its errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StatementKind {
    /// A multi-row `INSERT` of mapped columns (append).
    Insert,
    /// A multi-row `INSERT` of whole records into one JSON/text column.
    InsertJson,
    /// A keyed upsert (`INSERT … ON CONFLICT`, `ON DUPLICATE KEY`, `MERGE`).
    Upsert,
    /// A keyed delete.
    Delete,
    /// Drop the scoped-cleanup key table.
    CleanupDrop,
    /// Create the scoped-cleanup key table.
    CleanupCreate,
    /// Load written keys into the scoped-cleanup key table.
    CleanupLoad,
    /// The scoped-cleanup `DELETE`.
    CleanupDelete,
    /// Create the commit-token watermark table.
    TokenTable,
    /// Upsert one scope's commit token.
    TokenUpsert,
    /// Read the keys the scoped-cleanup `DELETE` is about to remove, so a
    /// journaling (rollback-enabled) write records their before-images.
    CleanupJournal,
}

/// One bound value.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Param<'a> {
    /// The column the value is bound for.
    pub column: &'a str,
    /// The column's type, when the backend reported one.
    pub sql_type: Option<&'a str>,
    /// The value; SQL `NULL` is [`Value::Null`].
    pub value: Cow<'a, Value>,
}

impl<'a> Param<'a> {
    /// A value borrowed from the page.
    pub fn borrowed(column: &'a str, sql_type: Option<&'a str>, value: &'a Value) -> Self {
        Self {
            column,
            sql_type,
            value: Cow::Borrowed(value),
        }
    }

    /// A value the planner built (a serialized record, a scope, a token).
    pub fn owned(column: &'a str, sql_type: Option<&'a str>, value: Value) -> Self {
        Self {
            column,
            sql_type,
            value: Cow::Owned(value),
        }
    }

    /// The text to bind when the value is a non-integer JSON number: its
    /// shortest decimal form, so the server parses exactly the value the
    /// record holds instead of converting a binary `f64` (#885). `None` for
    /// any other value.
    pub fn non_integer_text(&self) -> Option<String> {
        match self.value.as_ref() {
            Value::Number(n) => faucet_core::fidelity::non_integer_text(n),
            _ => None,
        }
    }

    /// SQL `NULL` for a column the row does not carry.
    pub fn null(column: &'a str, sql_type: Option<&'a str>) -> Self {
        Self::borrowed(column, sql_type, &NULL)
    }
}

static NULL: Value = Value::Null;

/// SQL text and its parameters, numbered in order from 1.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Statement<'a> {
    /// What the statement does.
    pub kind: StatementKind,
    /// The SQL text.
    pub sql: String,
    /// The values to bind, in placeholder order.
    pub params: Vec<Param<'a>>,
}

impl<'a> Statement<'a> {
    /// A statement of `kind`.
    pub fn new(kind: StatementKind, sql: impl Into<String>, params: Vec<Param<'a>>) -> Self {
        Self {
            kind,
            sql: sql.into(),
            params,
        }
    }

    /// A statement without parameters.
    pub fn bare(kind: StatementKind, sql: impl Into<String>) -> Self {
        Self::new(kind, sql, Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn params_borrow_own_and_null() {
        let v = json!(7);
        let p = Param::borrowed("a", Some("int4"), &v);
        assert!(matches!(p.value, Cow::Borrowed(_)));
        assert_eq!(
            Param::owned("a", None, json!("s")).value.as_ref(),
            &json!("s")
        );
        assert_eq!(Param::null("a", None).value.as_ref(), &Value::Null);
        assert_eq!(
            Param::owned("a", None, json!(0.1)).non_integer_text(),
            Some("0.1".to_string())
        );
        assert_eq!(Param::owned("a", None, json!(1)).non_integer_text(), None);
        assert_eq!(Param::null("a", None).non_integer_text(), None);
        let s = Statement::bare(StatementKind::CleanupDrop, "DROP");
        assert!(s.params.is_empty());
        assert_eq!(s.kind, StatementKind::CleanupDrop);
    }
}
