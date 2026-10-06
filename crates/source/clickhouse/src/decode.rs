//! Typed decoding of `JSONCompactEachRowWithNamesAndTypes` responses (#789
//! SQL-05, SQL-15).
//!
//! The query asks ClickHouse to quote every 64-bit-and-wider integer and every
//! decimal, so no value reaches `serde_json` as a lossy float. The header's
//! column types then decide the JSON shape: a quoted `Int8`…`UInt64` becomes an
//! exact JSON number again (so an integer cursor orders numerically), while
//! `Int128`…`UInt256` and `Decimal` stay exact strings.

use faucet_core::FaucetError;
use serde_json::{Map, Value};

/// The query settings the decoder relies on.
pub(crate) const SETTINGS: &[(&str, &str)] = &[
    ("default_format", "JSONCompactEachRowWithNamesAndTypes"),
    ("output_format_json_quote_64bit_integers", "1"),
    ("output_format_json_quote_decimals", "1"),
];

/// How a column's quoted cells are turned back into JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// `Int8`…`Int64` / `UInt8`…`UInt64`: an exact JSON number.
    Integer,
    /// Everything else: the cell as ClickHouse rendered it.
    AsIs,
}

fn kind_of(type_name: &str) -> Kind {
    let mut t = type_name.trim();
    loop {
        let inner = ["Nullable(", "LowCardinality("]
            .iter()
            .find_map(|w| t.strip_prefix(w).and_then(|r| r.strip_suffix(')')));
        match inner {
            Some(i) => t = i.trim(),
            None => break,
        }
    }
    match t {
        "Int8" | "Int16" | "Int32" | "Int64" | "UInt8" | "UInt16" | "UInt32" | "UInt64" => {
            Kind::Integer
        }
        _ => Kind::AsIs,
    }
}

fn cell(kind: Kind, v: Value) -> Value {
    match (kind, v) {
        (Kind::Integer, Value::String(s)) => s
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| s.parse::<u64>().map(Value::from))
            .unwrap_or(Value::String(s)),
        (_, v) => v,
    }
}

#[derive(Debug)]
enum State {
    Names,
    Types(Vec<String>),
    Rows(Vec<String>, Vec<Kind>),
}

/// Line-at-a-time decoder: the first line names the columns, the second gives
/// their types, every later line is one row.
#[derive(Debug)]
pub(crate) struct CompactDecoder {
    state: State,
}

impl Default for CompactDecoder {
    fn default() -> Self {
        Self {
            state: State::Names,
        }
    }
}

fn strings(line: Value, what: &str) -> Result<Vec<String>, FaucetError> {
    match line {
        Value::Array(a) => a
            .into_iter()
            .map(|v| match v {
                Value::String(s) => Ok(s),
                other => Err(FaucetError::Source(format!(
                    "ClickHouse: expected a column {what}, got {other}"
                ))),
            })
            .collect(),
        other => Err(FaucetError::Source(format!(
            "ClickHouse: expected the column {what} header, got {other}"
        ))),
    }
}

impl CompactDecoder {
    /// Feed one response line; returns a record for a data row.
    pub(crate) fn push_line(&mut self, line: &[u8]) -> Result<Option<Value>, FaucetError> {
        let text = std::str::from_utf8(line)
            .map_err(|e| FaucetError::Source(format!("ClickHouse: non-UTF-8 response line: {e}")))?
            .trim();
        if text.is_empty() {
            return Ok(None);
        }
        let value: Value = serde_json::from_str(text).map_err(|e| {
            FaucetError::Source(format!("ClickHouse: failed to parse response line: {e}"))
        })?;
        match std::mem::replace(&mut self.state, State::Names) {
            State::Names => {
                self.state = State::Types(strings(value, "names")?);
                Ok(None)
            }
            State::Types(names) => {
                let types = strings(value, "types")?;
                if types.len() != names.len() {
                    return Err(FaucetError::Source(format!(
                        "ClickHouse: {} column names but {} types",
                        names.len(),
                        types.len()
                    )));
                }
                let kinds = types.iter().map(|t| kind_of(t)).collect();
                self.state = State::Rows(names, kinds);
                Ok(None)
            }
            State::Rows(names, kinds) => {
                let row = match value {
                    Value::Array(cells) if cells.len() == names.len() => {
                        let mut map = Map::with_capacity(names.len());
                        for ((name, kind), v) in names.iter().zip(&kinds).zip(cells) {
                            map.insert(name.clone(), cell(*kind, v));
                        }
                        Value::Object(map)
                    }
                    other => {
                        return Err(FaucetError::Source(format!(
                            "ClickHouse: expected a row of {} cells, got {other}",
                            names.len()
                        )));
                    }
                };
                self.state = State::Rows(names, kinds);
                Ok(Some(row))
            }
        }
    }

    /// Decode a whole buffered response body.
    pub(crate) fn decode_all(body: &str) -> Result<Vec<Value>, FaucetError> {
        let mut decoder = Self::default();
        let mut rows = Vec::new();
        for line in body.lines() {
            if let Some(row) = decoder.push_line(line.as_bytes())? {
                rows.push(row);
            }
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn decode(lines: &[&str]) -> Result<Vec<Value>, FaucetError> {
        CompactDecoder::decode_all(&lines.join("\n"))
    }

    #[test]
    fn integers_become_exact_numbers_and_wide_values_stay_exact_strings() {
        let rows = decode(&[
            r#"["id","n","big","price","name","f"]"#,
            r#"["UInt64","Nullable(Int32)","Int128","Decimal(38, 10)","LowCardinality(String)","Float64"]"#,
            r#"["18446744073709551615",-7,"170141183460469231731687303715884105727","12345678901234567890.0123456789","a",1.5]"#,
            r#"["99",null,"1","0.1000000000","b",2]"#,
        ])
        .unwrap();
        assert_eq!(rows[0]["id"], json!(18_446_744_073_709_551_615u64));
        assert_eq!(rows[0]["n"], json!(-7));
        assert_eq!(
            rows[0]["big"],
            json!("170141183460469231731687303715884105727")
        );
        assert_eq!(rows[0]["price"], json!("12345678901234567890.0123456789"));
        assert_eq!(rows[0]["name"], json!("a"));
        assert_eq!(rows[0]["f"], json!(1.5));
        assert_eq!(rows[1]["id"], json!(99));
        assert_eq!(rows[1]["n"], json!(null));
    }

    #[test]
    fn an_unparseable_integer_cell_is_kept_as_text() {
        assert_eq!(cell(Kind::Integer, json!("x")), json!("x"));
        assert_eq!(cell(Kind::Integer, json!("-5")), json!(-5));
        assert_eq!(kind_of("Nullable(LowCardinality(UInt8))"), Kind::Integer);
        assert_eq!(kind_of("Array(Int64)"), Kind::AsIs);
    }

    #[test]
    fn malformed_responses_are_typed_errors() {
        assert!(decode(&["not json"]).is_err());
        assert!(decode(&[r#"{"a":1}"#]).is_err());
        assert!(decode(&[r#"["a"]"#, r#"[1]"#]).is_err());
        assert!(decode(&[r#"["a","b"]"#, r#"["Int8"]"#]).is_err());
        assert!(decode(&[r#"["a"]"#, r#"["Int8"]"#, r#"[1,2]"#]).is_err());
        let mut d = CompactDecoder::default();
        assert!(d.push_line(&[0xff, 0xfe]).is_err());
        assert_eq!(d.push_line(b"   ").unwrap(), None);
        assert!(decode(&[r#"["a"]"#, r#"["Int8"]"#]).unwrap().is_empty());
    }
}
