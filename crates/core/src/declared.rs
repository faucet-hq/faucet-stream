//! Declared column types (#955).
//!
//! A source that knows the exact type of each column it emits (a SQL query
//! source, from the describe of its statement) declares them through
//! [`Source::declared_columns`](crate::Source::declared_columns). The pipeline
//! hands the declaration to the sink through
//! [`Sink::set_declared_columns`](crate::Sink::set_declared_columns), and a sink
//! that creates its destination (`create_table: true`) types the new columns
//! from it instead of inferring them from the first page — so an exact decimal,
//! a date or a timestamp, all rendered as JSON strings by
//! [`fidelity`](crate::fidelity), land in a typed column rather than a string
//! one.
//!
//! The rules every sink follows (see [`plan_columns_declared`](crate::plan_columns_declared)):
//!
//! - The declaration only shapes a table being **created**. An existing table
//!   is never retyped; a declared-vs-existing difference is the schema-drift
//!   policy's business.
//! - A created table carries the columns the first page carries. A declared
//!   type is used for a column only when every value of the first page fits it
//!   ([`LogicalType::accepts`]); otherwise the inferred type stands. This keeps
//!   a transform or a masking rule that changed a column's values from
//!   creating a column its values cannot be written to.
//! - A run that writes no records creates the declared columns, typed, when
//!   the declaration is [complete](DeclaredColumns::is_complete) — nothing
//!   between the source and the sink reshaped the records.

use crate::drift::SqlBaseType;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A portable column type, rendered by a source as [`fidelity`](crate::fidelity)
/// specifies and mapped by each sink's dialect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum LogicalType {
    /// A signed 64-bit integer (a JSON number).
    Int64,
    /// A double (a JSON number; non-finite values arrive as `null`).
    Float64,
    /// A boolean.
    Bool,
    /// Text.
    String,
    /// An exact decimal, rendered as a string. `precision: None` is unbounded.
    Decimal {
        /// Total significant digits, when the source declares them.
        precision: Option<u32>,
        /// Digits after the decimal point, when the source declares them.
        scale: Option<u32>,
    },
    /// A calendar date, `YYYY-MM-DD`.
    Date,
    /// A time of day, `HH:MM:SS[.f]`.
    Time,
    /// An instant (`tz: true`, RFC 3339 with an offset) or a wall-clock
    /// date-time with no zone (`tz: false`).
    Timestamp {
        /// Whether the value carries a time zone.
        tz: bool,
    },
    /// A UUID, rendered as its 36-character hyphenated text.
    Uuid,
    /// Binary, rendered as standard base64.
    Bytes,
    /// Any JSON value.
    Json,
}

impl LogicalType {
    /// The dialect-neutral base type a sink falls back to when its dialect has
    /// no keyword for this type.
    pub fn base_type(&self) -> SqlBaseType {
        match self {
            LogicalType::Int64 => SqlBaseType::Integer,
            LogicalType::Float64 => SqlBaseType::Double,
            LogicalType::Bool => SqlBaseType::Boolean,
            LogicalType::Json => SqlBaseType::Json,
            _ => SqlBaseType::Text,
        }
    }

    /// Whether `value` is a rendering of this type (`null` always is).
    pub fn accepts(&self, value: &Value) -> bool {
        use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
        if value.is_null() {
            return true;
        }
        match self {
            LogicalType::Int64 => value.as_i64().is_some(),
            LogicalType::Float64 => value.is_number(),
            LogicalType::Bool => value.is_boolean(),
            LogicalType::String => value.is_string(),
            LogicalType::Json => true,
            LogicalType::Decimal { .. } => match value {
                Value::Number(n) => n.is_i64() || n.is_u64(),
                Value::String(s) => is_decimal_text(s),
                _ => false,
            },
            LogicalType::Date => value
                .as_str()
                .is_some_and(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok()),
            LogicalType::Time => value
                .as_str()
                .is_some_and(|s| NaiveTime::parse_from_str(s, "%H:%M:%S%.f").is_ok()),
            LogicalType::Timestamp { tz: true } => value
                .as_str()
                .is_some_and(|s| DateTime::parse_from_rfc3339(s).is_ok()),
            LogicalType::Timestamp { tz: false } => value.as_str().is_some_and(|s| {
                NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f").is_ok()
                    || NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").is_ok()
            }),
            LogicalType::Uuid => value.as_str().is_some_and(is_uuid_text),
            LogicalType::Bytes => value.as_str().is_some_and(|s| {
                use base64::Engine as _;
                base64::engine::general_purpose::STANDARD.decode(s).is_ok()
            }),
        }
    }

    /// The logical type of a SQL column type name, as SQL backends spell it
    /// (`numeric(12,2)`, `TIMESTAMPTZ`, `datetime2`, `uniqueidentifier`, …).
    /// `precision` / `scale` override any the name carries. `None` for a type
    /// with no portable counterpart; the caller decides (most sources render
    /// such a column as text, so declare it [`LogicalType::String`]).
    ///
    /// This is the shared SQL-family mapping; a source whose rendering of a
    /// type differs (a `TIMESTAMP` it renders with an offset) adjusts the
    /// result.
    pub fn from_sql_type(
        type_name: &str,
        precision: Option<u32>,
        scale: Option<u32>,
    ) -> Option<LogicalType> {
        let lower = type_name.trim().to_ascii_lowercase();
        let (base, args) = match lower.find('(') {
            Some(i) => (lower[..i].trim(), lower[i + 1..].split(')').next()),
            None => (lower.as_str(), None),
        };
        let mut nums = args
            .into_iter()
            .flat_map(|a| a.split(','))
            .map(|n| n.trim().parse::<u32>().ok());
        let named_precision = nums.next().flatten();
        let named_scale = nums.next().flatten();
        let unsigned = base.contains("unsigned");
        let base = base.trim_end_matches(" unsigned").trim();
        let t = match base {
            "bool" | "boolean" | "bit" => LogicalType::Bool,
            "int2" | "int4" | "smallint" | "integer" | "int" | "mediumint" | "tinyint"
            | "serial" | "smallserial" | "int16" | "int32" | "uint8" | "uint16" | "uint32"
            | "utinyint" | "usmallint" | "uinteger" | "int8" | "int64" | "long" | "short" => {
                LogicalType::Int64
            }
            "bigint" | "bigserial" if !unsigned => LogicalType::Int64,
            "bigint" | "ubigint" | "uint64" => LogicalType::Decimal {
                precision: Some(20),
                scale: Some(0),
            },
            "hugeint" | "int128" | "uhugeint" | "uint128" => LogicalType::Decimal {
                precision: Some(39),
                scale: Some(0),
            },
            "float4" | "float8" | "real" | "float" | "double" | "double precision" | "float32"
            | "float64" | "binary_float" | "binary_double" => LogicalType::Float64,
            "numeric" | "decimal" | "number" | "dec" | "bignumeric" | "bigdecimal" => {
                LogicalType::Decimal {
                    precision: precision.or(named_precision),
                    scale: scale.or(named_scale),
                }
            }
            "money" | "smallmoney" => LogicalType::Decimal {
                precision: None,
                scale: None,
            },
            "date" | "date32" => LogicalType::Date,
            "time" | "time without time zone" => LogicalType::Time,
            "timestamp"
            | "timestamp without time zone"
            | "datetime"
            | "datetime2"
            | "smalldatetime"
            | "timestamp_ntz"
            | "datetime64" => LogicalType::Timestamp { tz: false },
            "timestamptz"
            | "timestamp with time zone"
            | "datetimeoffset"
            | "timestamp_tz"
            | "timestamp_ltz"
            | "timestamp with local time zone" => LogicalType::Timestamp { tz: true },
            "uuid" | "uniqueidentifier" => LogicalType::Uuid,
            "bytea" | "blob" | "binary" | "varbinary" | "image" | "tinyblob" | "mediumblob"
            | "longblob" | "bytes" | "raw" | "long raw" | "varbyte" => LogicalType::Bytes,
            "json" | "jsonb" => LogicalType::Json,
            "text" | "varchar" | "char" | "character" | "character varying" | "bpchar" | "name"
            | "nvarchar" | "nchar" | "ntext" | "string" | "clob" | "nclob" | "varchar2"
            | "nvarchar2" | "tinytext" | "mediumtext" | "longtext" | "citext" | "\"char\""
            | "enum" => LogicalType::String,
            _ => return None,
        };
        Some(t)
    }
}

fn is_decimal_text(s: &str) -> bool {
    let body = s.strip_prefix(['-', '+']).unwrap_or(s);
    let (int, frac) = match body.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (body, None),
    };
    let digits = |p: &str| p.bytes().all(|b| b.is_ascii_digit());
    match frac {
        Some(f) => digits(int) && digits(f) && !(int.is_empty() && f.is_empty()),
        None => !int.is_empty() && digits(int),
    }
}

fn is_uuid_text(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 36
        && b.iter().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => *c == b'-',
            _ => c.is_ascii_hexdigit(),
        })
}

/// One declared column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DeclaredColumn {
    /// The column's name, exactly as records carry it.
    pub name: String,
    /// Its portable type.
    pub logical_type: LogicalType,
    /// Whether it may hold `null`.
    pub nullable: bool,
}

impl DeclaredColumn {
    /// A declared column.
    pub fn new(name: impl Into<String>, logical_type: LogicalType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            logical_type,
            nullable,
        }
    }
}

/// The columns a stream declares, in the source's order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct DeclaredColumns {
    columns: Vec<DeclaredColumn>,
    complete: bool,
}

impl DeclaredColumns {
    /// A complete declaration of `columns`. A repeated name keeps its first
    /// occurrence.
    pub fn new(columns: impl IntoIterator<Item = DeclaredColumn>) -> Self {
        let mut out: Vec<DeclaredColumn> = Vec::new();
        for c in columns {
            if !out.iter().any(|o| o.name == c.name) {
                out.push(c);
            }
        }
        Self {
            columns: out,
            complete: true,
        }
    }

    /// The declared columns, in order.
    pub fn columns(&self) -> &[DeclaredColumn] {
        &self.columns
    }

    /// The column named `name`.
    pub fn get(&self, name: &str) -> Option<&DeclaredColumn> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Whether nothing is declared.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Whether the declaration describes the records the sink receives exactly:
    /// nothing between the source and the sink renamed, added, removed or
    /// rewrote a column. Only a complete declaration creates a table for a run
    /// that wrote no records.
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    /// The same columns, marked as no longer describing the records exactly
    /// (a transform or a masking rule sits in between).
    pub fn reshaped(mut self) -> Self {
        self.complete = false;
        self
    }

    /// The same declaration with every name passed through `rename` (a sink
    /// that folds record keys folds the declared names alike). A name that
    /// folds onto an earlier one keeps the earlier column.
    pub fn renamed(self, rename: impl Fn(&str) -> String) -> Self {
        let complete = self.complete;
        let mut out = Self::new(self.columns.into_iter().map(|mut c| {
            c.name = rename(&c.name);
            c
        }));
        out.complete = complete;
        out
    }

    /// Add `column`, replacing a declared column of the same name (a sink
    /// decorator that adds its own columns declares them this way).
    pub fn with_column(mut self, column: DeclaredColumn) -> Self {
        match self.columns.iter_mut().find(|c| c.name == column.name) {
            Some(c) => *c = column,
            None => self.columns.push(column),
        }
        self
    }
}

/// Where a sink keeps the declaration it was handed
/// ([`Sink::set_declared_columns`](crate::Sink::set_declared_columns)) until it
/// creates its destination.
#[derive(Debug, Default)]
pub struct DeclaredSlot(std::sync::Mutex<Option<DeclaredColumns>>);

impl DeclaredSlot {
    /// An empty slot.
    pub fn new() -> Self {
        Self::default()
    }

    /// Keep `columns`, replacing any earlier declaration.
    pub fn set(&self, columns: DeclaredColumns) {
        if let Ok(mut slot) = self.0.lock() {
            *slot = Some(columns);
        }
    }

    /// The kept declaration.
    pub fn get(&self) -> Option<DeclaredColumns> {
        self.0.lock().ok().and_then(|slot| slot.clone())
    }

    /// The kept declaration when it can create a table for a run that wrote
    /// nothing: complete and non-empty.
    pub fn for_empty_run(&self) -> Option<DeclaredColumns> {
        self.get().filter(|d| d.is_complete() && !d.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn maps_sql_type_names() {
        use LogicalType as L;
        let cases = [
            ("integer", L::Int64),
            ("INT8", L::Int64),
            ("bigint", L::Int64),
            (
                "BIGINT UNSIGNED",
                L::Decimal {
                    precision: Some(20),
                    scale: Some(0),
                },
            ),
            ("double precision", L::Float64),
            ("FLOAT4", L::Float64),
            ("boolean", L::Bool),
            ("bit", L::Bool),
            (
                "numeric(12,2)",
                L::Decimal {
                    precision: Some(12),
                    scale: Some(2),
                },
            ),
            (
                "NUMERIC",
                L::Decimal {
                    precision: None,
                    scale: None,
                },
            ),
            (
                "money",
                L::Decimal {
                    precision: None,
                    scale: None,
                },
            ),
            (
                "hugeint",
                L::Decimal {
                    precision: Some(39),
                    scale: Some(0),
                },
            ),
            ("date", L::Date),
            ("TIME", L::Time),
            ("timestamp", L::Timestamp { tz: false }),
            ("datetime2", L::Timestamp { tz: false }),
            ("TIMESTAMPTZ", L::Timestamp { tz: true }),
            ("datetimeoffset", L::Timestamp { tz: true }),
            ("uuid", L::Uuid),
            ("uniqueidentifier", L::Uuid),
            ("bytea", L::Bytes),
            ("VARBINARY(16)", L::Bytes),
            ("jsonb", L::Json),
            ("character varying(20)", L::String),
            ("TEXT", L::String),
        ];
        for (name, want) in cases {
            assert_eq!(
                LogicalType::from_sql_type(name, None, None),
                Some(want),
                "{name}"
            );
        }
        assert_eq!(LogicalType::from_sql_type("interval", None, None), None);
        assert_eq!(
            LogicalType::from_sql_type("numeric(12,2)", Some(20), Some(4)),
            Some(L::Decimal {
                precision: Some(20),
                scale: Some(4)
            })
        );
    }

    #[test]
    fn accepts_each_fidelity_rendering() {
        use LogicalType as L;
        let ok = [
            (L::Int64, json!(7)),
            (L::Float64, json!(1.5)),
            (L::Float64, json!(2)),
            (L::Bool, json!(true)),
            (L::String, json!("x")),
            (
                L::Decimal {
                    precision: None,
                    scale: None,
                },
                json!("-12.50"),
            ),
            (
                L::Decimal {
                    precision: None,
                    scale: None,
                },
                json!(".5"),
            ),
            (
                L::Decimal {
                    precision: None,
                    scale: None,
                },
                json!(12),
            ),
            (L::Date, json!("2024-10-27")),
            (L::Time, json!("10:11:12.123456")),
            (
                L::Timestamp { tz: true },
                json!("2024-02-22T02:00:00+00:00"),
            ),
            (L::Timestamp { tz: true }, json!("2024-02-22T02:00:00Z")),
            (L::Timestamp { tz: false }, json!("2024-02-22T02:00:00.5")),
            (L::Timestamp { tz: false }, json!("2024-02-22 02:00:00")),
            (L::Uuid, json!("123e4567-e89b-12d3-a456-426614174000")),
            (L::Bytes, json!("AAEC")),
            (L::Json, json!({"a": 1})),
            (L::Int64, Value::Null),
        ];
        for (t, v) in ok {
            assert!(t.accepts(&v), "{t:?} should accept {v}");
        }
        let bad = [
            (L::Int64, json!("7")),
            (L::Int64, json!(1.5)),
            (L::Float64, json!("1.5")),
            (L::Bool, json!(1)),
            (L::String, json!(1)),
            (
                L::Decimal {
                    precision: None,
                    scale: None,
                },
                json!("NaN"),
            ),
            (
                L::Decimal {
                    precision: None,
                    scale: None,
                },
                json!("1e5"),
            ),
            (
                L::Decimal {
                    precision: None,
                    scale: None,
                },
                json!("."),
            ),
            (
                L::Decimal {
                    precision: None,
                    scale: None,
                },
                json!(1.5),
            ),
            (L::Date, json!("2024-13-01")),
            (L::Time, json!("25:00:00")),
            (L::Timestamp { tz: true }, json!("2024-02-22T02:00:00")),
            (L::Timestamp { tz: false }, json!("yesterday")),
            (L::Uuid, json!("not-a-uuid")),
            (L::Bytes, json!("***")),
        ];
        for (t, v) in bad {
            assert!(!t.accepts(&v), "{t:?} should refuse {v}");
        }
    }

    #[test]
    fn base_types_fall_back_to_text() {
        assert_eq!(LogicalType::Int64.base_type(), SqlBaseType::Integer);
        assert_eq!(LogicalType::Float64.base_type(), SqlBaseType::Double);
        assert_eq!(LogicalType::Bool.base_type(), SqlBaseType::Boolean);
        assert_eq!(LogicalType::Json.base_type(), SqlBaseType::Json);
        assert_eq!(LogicalType::Date.base_type(), SqlBaseType::Text);
    }

    #[test]
    fn declarations_dedupe_reshape_and_extend() {
        let d = DeclaredColumns::new([
            DeclaredColumn::new("a", LogicalType::Int64, false),
            DeclaredColumn::new("a", LogicalType::String, true),
            DeclaredColumn::new("b", LogicalType::Date, true),
        ]);
        assert_eq!(d.columns().len(), 2);
        assert_eq!(d.get("a").unwrap().logical_type, LogicalType::Int64);
        assert!(d.is_complete() && !d.is_empty());
        let d = d
            .with_column(DeclaredColumn::new("b", LogicalType::String, true))
            .with_column(DeclaredColumn::new("c", LogicalType::Bool, true));
        assert_eq!(d.get("b").unwrap().logical_type, LogicalType::String);
        assert_eq!(d.columns().last().unwrap().name, "c");
        let upper = d.clone().renamed(|n| n.to_uppercase());
        assert_eq!(upper.columns()[0].name, "A");
        assert!(upper.is_complete());
        let folded = d.clone().reshaped().renamed(|_| "x".into());
        assert_eq!(folded.columns().len(), 1);
        assert!(!folded.is_complete());
        assert!(!d.reshaped().is_complete());
        assert!(DeclaredColumns::new([]).is_empty());
    }

    #[test]
    fn logical_types_serialize_tagged() {
        let v = serde_json::to_value(LogicalType::Decimal {
            precision: Some(10),
            scale: Some(2),
        })
        .unwrap();
        assert_eq!(v, json!({"type": "decimal", "precision": 10, "scale": 2}));
        let back: LogicalType =
            serde_json::from_value(json!({"type": "timestamp", "tz": true})).unwrap();
        assert_eq!(back, LogicalType::Timestamp { tz: true });
    }
    #[test]
    fn a_slot_keeps_the_last_declaration() {
        let slot = DeclaredSlot::new();
        assert!(slot.get().is_none() && slot.for_empty_run().is_none());
        slot.set(DeclaredColumns::new([]));
        assert!(slot.get().is_some() && slot.for_empty_run().is_none());
        let d = DeclaredColumns::new([DeclaredColumn::new("a", LogicalType::Bool, true)]);
        slot.set(d.clone().reshaped());
        assert!(slot.for_empty_run().is_none());
        slot.set(d.clone());
        assert_eq!(slot.for_empty_run(), Some(d));
    }
}
