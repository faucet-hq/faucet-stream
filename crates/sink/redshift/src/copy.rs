//! Pure SQL / payload builders for the Redshift sink.
//!
//! Everything here is I/O-free and unit-tested: the `COPY` statement, the
//! multi-row `INSERT` statement, and the CSV / JSONL serializers. Keeping them
//! pure means the exact SQL a live cluster receives is asserted in tests without
//! a database.

use crate::config::RedshiftCopyFormat;
use faucet_core::FaucetError;
use faucet_core::util::quote_ident;
use serde_json::Value;

/// Build a schema-qualified, quoted table reference (`"schema"."table"` or
/// `"table"`).
pub(crate) fn qualified_table_ref(schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(s) => format!("{}.{}", quote_ident(s), quote_ident(table)),
        None => quote_ident(table),
    }
}

/// Map a [`faucet_core::SqlBaseType`] to the Redshift column type used when
/// auto-creating a table (#580).
///
/// Redshift has no JSON column type, so nested values land in `VARCHAR(MAX)`
/// — the same text the writer already serialises them to. Integers widen to
/// `BIGINT` and floats to `DOUBLE PRECISION` so a later, wider value never
/// overflows a narrower column.
pub(crate) fn redshift_type(t: faucet_core::SqlBaseType) -> &'static str {
    use faucet_core::SqlBaseType::*;
    match t {
        Integer => "BIGINT",
        Double => "DOUBLE PRECISION",
        Boolean => "BOOLEAN",
        Text | Json => "VARCHAR(MAX)",
    }
}

/// `CREATE TABLE IF NOT EXISTS` for an auto-created target (#580).
///
/// No DISTKEY or SORTKEY: faucet has no basis to choose either, and the wrong
/// choice is baked into the table and has to be migrated out. An operator who
/// cares defines the table and sets `create_table: false`.
pub(crate) fn build_create_table_sql(
    table_ref: &str,
    columns: &[faucet_core::PlannedColumn],
) -> String {
    let cols = faucet_core::render_columns(columns, quote_ident, redshift_type);
    format!("CREATE TABLE IF NOT EXISTS {table_ref} ({cols})")
}

/// Render a value as a single-quoted SQL string literal, doubling interior
/// single quotes so it is injection-safe.
pub(crate) fn sql_string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The `s3://bucket/key` URI for a staged object.
pub(crate) fn s3_uri(bucket: &str, key: &str) -> String {
    format!("s3://{bucket}/{key}")
}

/// Build the Redshift `COPY` statement.
///
/// For JSONL the column list is omitted (`FORMAT AS JSON 'auto'` maps by key
/// name); for CSV the destination column order is passed explicitly. `s3_path`,
/// `iam_role`, and `region` are emitted as escaped string literals.
pub(crate) fn copy_statement(
    table_ref: &str,
    columns: Option<&[String]>,
    s3_path: &str,
    iam_role: &str,
    region: Option<&str>,
    format: RedshiftCopyFormat,
) -> String {
    let col_clause = match (format, columns) {
        // CSV needs an explicit, ordered column list.
        (RedshiftCopyFormat::Csv, Some(cols)) if !cols.is_empty() => {
            let list = cols
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ");
            format!(" ({list})")
        }
        _ => String::new(),
    };

    let format_clause = match format {
        RedshiftCopyFormat::Jsonl => "FORMAT AS JSON 'auto'".to_string(),
        RedshiftCopyFormat::Csv => "FORMAT AS CSV".to_string(),
    };

    let mut sql = format!(
        "COPY {table_ref}{col_clause} FROM {} IAM_ROLE {} {format_clause}",
        sql_string_literal(s3_path),
        sql_string_literal(iam_role),
    );
    if let Some(r) = region.filter(|r| !r.trim().is_empty()) {
        sql.push_str(&format!(" REGION {}", sql_string_literal(r)));
    }
    sql
}

/// Build a multi-row `INSERT INTO table (cols) VALUES ($1,$2),($3,$4)…`.
/// Placeholders are numbered `$1..=$(num_rows*columns.len())`.
pub(crate) fn insert_statement(table_ref: &str, columns: &[String], num_rows: usize) -> String {
    let col_list = columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let num_cols = columns.len();
    let mut tuples = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        let start = row * num_cols + 1;
        let placeholders = (0..num_cols)
            .map(|c| format!("${}", start + c))
            .collect::<Vec<_>>()
            .join(", ");
        tuples.push(format!("({placeholders})"));
    }
    format!(
        "INSERT INTO {table_ref} ({col_list}) VALUES {}",
        tuples.join(", ")
    )
}

/// The set of destination columns present in at least one record, preserving
/// the destination's declared order.
pub(crate) fn columns_present<'a>(
    records: &[Value],
    table_columns: &'a [String],
) -> Vec<&'a String> {
    table_columns
        .iter()
        .filter(|col| {
            records
                .iter()
                .any(|r| r.as_object().is_some_and(|o| o.contains_key(col.as_str())))
        })
        .collect()
}

/// Whether `record` (a JSON object) has at least one key matching a column in
/// `present`. A record that shares no column with the table carries nothing for
/// this destination; binding it would emit an all-NULL row, so the INSERT path
/// skips it (#466 L1). A non-object never shares a column.
pub(crate) fn shares_a_column(record: &Value, present: &[String]) -> bool {
    record
        .as_object()
        .is_some_and(|o| present.iter().any(|c| o.contains_key(c.as_str())))
}

/// Serialize records as newline-delimited JSON (JSONL) bytes for `FORMAT AS
/// JSON 'auto'`.
pub(crate) fn serialize_jsonl(records: &[Value]) -> Result<Vec<u8>, FaucetError> {
    let mut buf = Vec::new();
    for record in records {
        let line = serde_json::to_vec(record)
            .map_err(|e| FaucetError::Sink(format!("redshift: JSON serialization failed: {e}")))?;
        buf.extend_from_slice(&line);
        buf.push(b'\n');
    }
    Ok(buf)
}

/// Render one CSV cell from a JSON value (RFC-4180 quoting).
fn csv_cell(v: Option<&Value>) -> String {
    match v {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => quote_csv(s),
        Some(Value::Bool(b)) => b.to_string(),
        Some(Value::Number(n)) => n.to_string(),
        // Arrays/objects have no scalar form — emit their JSON text, quoted.
        Some(other) => quote_csv(&other.to_string()),
    }
}

/// RFC-4180 field quoting: wrap in double quotes and double interior quotes when
/// the field contains a comma, quote, CR, or LF.
fn quote_csv(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Serialize records as CSV, one row per record, columns in the given order.
pub(crate) fn serialize_csv(records: &[Value], columns: &[String]) -> Result<Vec<u8>, FaucetError> {
    let mut buf = String::new();
    for record in records {
        let obj = record.as_object().ok_or_else(|| {
            FaucetError::Sink("redshift: CSV requires JSON object records".into())
        })?;
        let cells: Vec<String> = columns.iter().map(|c| csv_cell(obj.get(c))).collect();
        buf.push_str(&cells.join(","));
        buf.push('\n');
    }
    Ok(buf.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn table_ref_qualified_and_bare() {
        assert_eq!(qualified_table_ref(None, "events"), "\"events\"");
        assert_eq!(
            qualified_table_ref(Some("analytics"), "events"),
            "\"analytics\".\"events\""
        );
    }

    #[test]
    fn table_ref_escapes_quotes() {
        assert_eq!(
            qualified_table_ref(Some("we\"ird"), "ta\"ble"),
            "\"we\"\"ird\".\"ta\"\"ble\""
        );
    }

    #[test]
    fn string_literal_doubles_single_quotes() {
        assert_eq!(sql_string_literal("a'b"), "'a''b'");
        assert_eq!(sql_string_literal("plain"), "'plain'");
    }

    #[test]
    fn copy_jsonl_omits_columns_and_uses_json_auto() {
        let sql = copy_statement(
            "\"public\".\"events\"",
            Some(&["id".into(), "name".into()]),
            "s3://bucket/staging/abc.jsonl",
            "arn:aws:iam::123:role/redshift",
            None,
            RedshiftCopyFormat::Jsonl,
        );
        assert_eq!(
            sql,
            "COPY \"public\".\"events\" FROM 's3://bucket/staging/abc.jsonl' \
             IAM_ROLE 'arn:aws:iam::123:role/redshift' FORMAT AS JSON 'auto'"
        );
    }

    #[test]
    fn copy_csv_includes_ordered_columns() {
        let sql = copy_statement(
            "\"events\"",
            Some(&["id".into(), "name".into()]),
            "s3://b/k.csv",
            "arn:role",
            Some("us-east-1"),
            RedshiftCopyFormat::Csv,
        );
        assert_eq!(
            sql,
            "COPY \"events\" (\"id\", \"name\") FROM 's3://b/k.csv' IAM_ROLE 'arn:role' \
             FORMAT AS CSV REGION 'us-east-1'"
        );
    }

    #[test]
    fn copy_escapes_injection_in_role_and_path() {
        let sql = copy_statement(
            "\"t\"",
            None,
            "s3://b/k'; DROP TABLE t;--.jsonl",
            "arn'evil",
            None,
            RedshiftCopyFormat::Jsonl,
        );
        assert!(sql.contains("'s3://b/k''; DROP TABLE t;--.jsonl'"));
        assert!(sql.contains("IAM_ROLE 'arn''evil'"));
    }

    #[test]
    fn s3_uri_joins_bucket_and_key() {
        assert_eq!(
            s3_uri("my-bucket", "prefix/obj.jsonl"),
            "s3://my-bucket/prefix/obj.jsonl"
        );
    }

    #[test]
    fn insert_statement_numbers_placeholders_per_row() {
        let sql = insert_statement("\"t\"", &["a".into(), "b".into()], 2);
        assert_eq!(
            sql,
            "INSERT INTO \"t\" (\"a\", \"b\") VALUES ($1, $2), ($3, $4)"
        );
    }

    #[test]
    fn insert_statement_single_row() {
        let sql = insert_statement("\"t\"", &["a".into()], 1);
        assert_eq!(sql, "INSERT INTO \"t\" (\"a\") VALUES ($1)");
    }

    #[test]
    fn columns_present_is_union_in_table_order() {
        let table = vec!["id".to_string(), "name".to_string(), "email".to_string()];
        let records = vec![json!({"id": 1, "email": "a@b.c"}), json!({"name": "Bob"})];
        let present = columns_present(&records, &table);
        assert_eq!(
            present.into_iter().cloned().collect::<Vec<_>>(),
            vec!["id".to_string(), "name".to_string(), "email".to_string()]
        );
    }

    #[test]
    fn shares_a_column_detects_overlap_and_its_absence() {
        let cols = vec!["id".to_string(), "name".to_string()];
        assert!(shares_a_column(&json!({"id": 1}), &cols));
        assert!(shares_a_column(&json!({"name": "x", "extra": 9}), &cols));
        // No overlap → skipped (would otherwise be an all-NULL row, #466 L1).
        assert!(!shares_a_column(&json!({"unrelated": 1}), &cols));
        assert!(!shares_a_column(&json!({}), &cols));
        // A non-object never shares a column.
        assert!(!shares_a_column(&json!(42), &cols));
        assert!(!shares_a_column(&json!(null), &cols));
    }

    #[test]
    fn columns_present_drops_absent_columns() {
        let table = vec!["id".to_string(), "unused".to_string()];
        let records = vec![json!({"id": 1})];
        let present = columns_present(&records, &table);
        assert_eq!(present, vec![&"id".to_string()]);
    }

    #[test]
    fn serialize_jsonl_is_newline_delimited() {
        let out = serialize_jsonl(&[json!({"id": 1}), json!({"id": 2})]).unwrap();
        let text = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(
            serde_json::from_str::<Value>(lines[0]).unwrap()["id"],
            json!(1)
        );
    }

    #[test]
    fn serialize_csv_orders_columns_and_quotes() {
        let cols = vec!["id".to_string(), "note".to_string()];
        let records = vec![
            json!({"id": 1, "note": "hello, world"}),
            json!({"id": 2, "note": "quote\"inside"}),
            json!({"id": 3}), // missing note → empty cell
        ];
        let out = serialize_csv(&records, &cols).unwrap();
        let text = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = text.trim_end().split('\n').collect();
        assert_eq!(lines[0], "1,\"hello, world\"");
        assert_eq!(lines[1], "2,\"quote\"\"inside\"");
        assert_eq!(lines[2], "3,");
    }

    #[test]
    fn serialize_csv_renders_scalar_types() {
        let cols = vec!["b".to_string(), "n".to_string(), "nul".to_string()];
        let out = serialize_csv(&[json!({"b": true, "n": 4.5, "nul": null})], &cols).unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), "true,4.5,\n");
    }

    #[test]
    fn serialize_csv_rejects_non_object() {
        assert!(serialize_csv(&[json!(5)], &["a".to_string()]).is_err());
    }

    #[test]
    fn create_table_sql_maps_each_inferred_type() {
        let cols = faucet_core::plan_columns(&[serde_json::json!({
            "id": 1, "name": "a", "amount": 1.5, "ok": true, "meta": {"k": 1}
        })])
        .expect("a plan");
        let sql = build_create_table_sql(r#""public"."events""#, &cols);
        assert!(sql.contains(r#""id" BIGINT"#), "{sql}");
        assert!(sql.contains(r#""name" VARCHAR(MAX)"#), "{sql}");
        assert!(sql.contains(r#""amount" DOUBLE PRECISION"#), "{sql}");
        assert!(sql.contains(r#""ok" BOOLEAN"#), "{sql}");
        // Redshift has no JSON type; nested values land as the text the
        // writer already serialises them to.
        assert!(sql.contains(r#""meta" VARCHAR(MAX)"#), "{sql}");
        assert!(sql.contains("IF NOT EXISTS"), "{sql}");
    }

    #[test]
    fn create_table_sql_picks_no_distkey_or_sortkey() {
        // Guessing either bakes a bad physical layout into the table that an
        // operator then has to migrate out of (#580).
        let cols = faucet_core::plan_columns(&[serde_json::json!({ "id": 1 })]).expect("plan");
        let sql = build_create_table_sql(r#""t""#, &cols);
        assert!(!sql.to_uppercase().contains("DISTKEY"), "{sql}");
        assert!(!sql.to_uppercase().contains("SORTKEY"), "{sql}");
    }
}
