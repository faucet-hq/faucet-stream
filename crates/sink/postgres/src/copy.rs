//! Pure encoding for the `COPY … FROM STDIN (FORMAT text)` bulk-load
//! fast-path (issue #308).
//!
//! COPY text format is line-oriented: one row per `\n`-terminated line,
//! fields separated by tabs, SQL NULL as the unquoted marker `\N`. The
//! server parses each field with the destination column's *input function* —
//! exactly the same semantics as the `INSERT` path's `$N::<udt>` casts — so
//! type behaviour is identical between the two write methods. What differs
//! is the wire encoding, and that is where silent corruption lives: a raw
//! tab, newline, or backslash inside a value would corrupt row/field framing
//! unless escaped. Everything in this module is pure and unit-tested.

use crate::sink::pg_bind_text;
use faucet_core::util::quote_ident;
use serde_json::Value;

/// Append one field's text to `buf`, escaping for COPY text format.
///
/// Escapes: `\` → `\\`, tab → `\t`, LF → `\n`, CR → `\r`, and the other
/// C-style control escapes PostgreSQL itself emits (`\b` backspace, `\f`
/// form feed, `\v` vertical tab). Everything else — including the literal
/// characters `N`, quotes, and all multibyte UTF-8 — passes through
/// unchanged. SQL NULL is *not* handled here: the caller writes the bare
/// `\N` marker instead of calling this (a literal string `"N"` therefore
/// stays `N`, and a literal `"\N"` becomes `\\N` — unambiguous).
pub(crate) fn encode_copy_text_field(buf: &mut String, text: &str) {
    for ch in text.chars() {
        match ch {
            '\\' => buf.push_str("\\\\"),
            '\t' => buf.push_str("\\t"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            '\u{0008}' => buf.push_str("\\b"),
            '\u{000C}' => buf.push_str("\\f"),
            '\u{000B}' => buf.push_str("\\v"),
            other => buf.push(other),
        }
    }
}

/// The COPY payload for one chunk: the (declared-order) column list actually
/// present in at least one record, the encoded row data, and how many rows
/// were encoded.
#[derive(Debug)]
pub(crate) struct CopyPayload {
    pub columns: Vec<String>,
    pub data: String,
    pub rows: usize,
}

/// Encode a chunk of records for AutoMap COPY.
///
/// Semantics mirror the `INSERT` path exactly:
/// - the column set is the **union** of table columns present in *any*
///   record (declared table order) — a column absent from every record is
///   left out entirely so its DEFAULT still applies;
/// - a record missing a unioned column ships SQL NULL (`\N`);
/// - a record with **no** matching column is an error, never skipped;
/// - non-object records are an error;
/// - each value is rendered with the same text semantics as the `INSERT`
///   path's [`pg_bind_text`] (JSON/JSONB columns get JSON text; scalars get
///   plain text; containers into non-JSON columns get JSON text so the
///   server's input function fails loudly), then COPY-escaped.
///
/// Returns `Ok(None)` for an empty page, and an error naming the first record
/// that matches no column.
pub(crate) fn build_auto_map_payload(
    records: &[Value],
    table_columns: &[(String, String)],
) -> Result<Option<CopyPayload>, String> {
    // Pass 1: validate + find which table columns any record uses.
    let mut used: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut matched_any: Vec<&serde_json::Map<String, Value>> = Vec::with_capacity(records.len());
    for (idx, record) in records.iter().enumerate() {
        let obj = record
            .as_object()
            .ok_or_else(|| "AutoMap requires JSON object records".to_string())?;
        let mut matched = false;
        for (col, _) in table_columns {
            if obj.contains_key(col) {
                used.insert(col.as_str());
                matched = true;
            }
        }
        if !matched {
            // Never a silent skip: the row would be lost with the bookmark
            // advancing past it (#789 SQL-50).
            return Err(crate::sink::no_matching_column_message(
                idx,
                obj,
                table_columns,
            ));
        }
        matched_any.push(obj);
    }
    if matched_any.is_empty() {
        return Ok(None);
    }

    let insert_columns: Vec<&(String, String)> = table_columns
        .iter()
        .filter(|(c, _)| used.contains(c.as_str()))
        .collect();

    // Pass 2: encode rows.
    let mut data = String::with_capacity(matched_any.len() * 64);
    for obj in &matched_any {
        for (idx, (col, udt)) in insert_columns.iter().enumerate() {
            if idx > 0 {
                data.push('\t');
            }
            match pg_bind_text(obj.get(col.as_str()), udt) {
                None => data.push_str("\\N"),
                Some(text) => encode_copy_text_field(&mut data, &text),
            }
        }
        data.push('\n');
    }

    Ok(Some(CopyPayload {
        columns: insert_columns.iter().map(|(c, _)| c.clone()).collect(),
        rows: matched_any.len(),
        data,
    }))
}

/// Encode a chunk of records for single-JSONB-column COPY: one escaped JSON
/// text per line. Nothing is skipped — every record has a JSON form.
pub(crate) fn build_jsonb_payload(records: &[Value]) -> CopyPayload {
    let mut data = String::with_capacity(records.len() * 64);
    for record in records {
        // `pg_bind_text` with a jsonb udt always yields the JSON text for
        // non-null values; a JSON `null` record ships SQL NULL.
        match pg_bind_text(Some(record), "jsonb") {
            None => data.push_str("\\N"),
            Some(text) => encode_copy_text_field(&mut data, &text),
        }
        data.push('\n');
    }
    CopyPayload {
        columns: vec![],
        rows: records.len(),
        data,
    }
}

/// `COPY <table_ref> ("c1", "c2") FROM STDIN (FORMAT text)`. `table_ref` is
/// already quoted/qualified; column names are quoted here.
pub(crate) fn copy_statement(table_ref: &str, columns: &[String]) -> String {
    let cols = columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    format!("COPY {table_ref} ({cols}) FROM STDIN (FORMAT text)")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn escaped(s: &str) -> String {
        let mut buf = String::new();
        encode_copy_text_field(&mut buf, s);
        buf
    }

    #[test]
    fn every_escape_character_is_escaped() {
        assert_eq!(escaped("back\\slash"), "back\\\\slash");
        assert_eq!(escaped("tab\there"), "tab\\there");
        assert_eq!(escaped("new\nline"), "new\\nline");
        assert_eq!(escaped("carriage\rreturn"), "carriage\\rreturn");
        assert_eq!(escaped("back\u{0008}space"), "back\\bspace");
        assert_eq!(escaped("form\u{000C}feed"), "form\\ffeed");
        assert_eq!(escaped("vertical\u{000B}tab"), "vertical\\vtab");
        // All of them at once, adjacent.
        assert_eq!(escaped("\\\t\n\r"), "\\\\\\t\\n\\r");
    }

    #[test]
    fn plain_text_unicode_and_quotes_pass_through() {
        assert_eq!(
            escaped("héllo wörld — 日本語 🚰"),
            "héllo wörld — 日本語 🚰"
        );
        assert_eq!(escaped(r#"quotes " and ' fine"#), r#"quotes " and ' fine"#);
        assert_eq!(escaped(""), "");
    }

    #[test]
    fn literal_n_strings_are_distinct_from_the_null_marker() {
        // The string "N" encodes as plain N — only bare `\N` (emitted by the
        // caller for SQL NULL) means NULL.
        assert_eq!(escaped("N"), "N");
        // The string "\N" escapes its backslash → `\\N`, which COPY decodes
        // back to the two characters `\N`, not NULL.
        assert_eq!(escaped("\\N"), "\\\\N");
    }

    fn cols(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(c, u)| (c.to_string(), u.to_string()))
            .collect()
    }

    #[test]
    fn auto_map_rows_are_tab_separated_in_declared_column_order() {
        let table = cols(&[("id", "int8"), ("name", "text"), ("ok", "bool")]);
        // Record key order differs from table order; encoding must follow
        // the table's declared order.
        let payload = build_auto_map_payload(&[json!({"ok": true, "id": 7, "name": "a"})], &table)
            .unwrap()
            .unwrap();
        assert_eq!(payload.columns, vec!["id", "name", "ok"]);
        assert_eq!(payload.data, "7\ta\ttrue\n");
        assert_eq!(payload.rows, 1);
    }

    #[test]
    fn auto_map_missing_field_ships_null_marker() {
        let table = cols(&[("id", "int8"), ("name", "text")]);
        let payload =
            build_auto_map_payload(&[json!({"id": 1, "name": "a"}), json!({"id": 2})], &table)
                .unwrap()
                .unwrap();
        assert_eq!(payload.data, "1\ta\n2\t\\N\n");
        // Explicit JSON null also ships NULL.
        let payload = build_auto_map_payload(&[json!({"id": 3, "name": null})], &table)
            .unwrap()
            .unwrap();
        assert_eq!(payload.data, "3\t\\N\n");
    }

    #[test]
    fn auto_map_unused_columns_are_left_out_so_defaults_apply() {
        // `created` appears in no record → omitted from the column list
        // (COPYing an explicit NULL would override a column DEFAULT).
        let table = cols(&[("id", "int8"), ("created", "timestamptz")]);
        let payload = build_auto_map_payload(&[json!({"id": 1})], &table)
            .unwrap()
            .unwrap();
        assert_eq!(payload.columns, vec!["id"]);
        assert_eq!(payload.data, "1\n");
    }

    #[test]
    fn auto_map_json_column_gets_json_text_and_scalars_stay_plain() {
        let table = cols(&[("meta", "jsonb"), ("note", "text")]);
        let payload =
            build_auto_map_payload(&[json!({"meta": {"a": "x\ty"}, "note": "plain"})], &table)
                .unwrap()
                .unwrap();
        // The tab inside the nested JSON string is escaped by serde_json to
        // \t (two chars: backslash + t), and COPY escaping doubles the
        // backslash so the server sees the JSON text {"a":"x\ty"} intact.
        assert_eq!(payload.data, "{\"a\":\"x\\\\ty\"}\tplain\n");
    }

    #[test]
    fn auto_map_object_into_non_json_column_emits_json_text() {
        // Same loud-failure semantics as the INSERT path: the int8 input
        // function will reject this text rather than silently coercing.
        let table = cols(&[("id", "int8")]);
        let payload = build_auto_map_payload(&[json!({"id": {"a": 1}})], &table)
            .unwrap()
            .unwrap();
        assert_eq!(payload.data, "{\"a\":1}\n");
    }

    #[test]
    fn auto_map_refuses_a_record_matching_no_column() {
        let table = cols(&[("id", "int8")]);
        let err =
            build_auto_map_payload(&[json!({"id": 1}), json!({"ID": true})], &table).unwrap_err();
        assert!(err.contains("record 1 has no field matching"), "{err}");
        assert!(build_auto_map_payload(&[], &table).unwrap().is_none());
    }

    #[test]
    fn auto_map_rejects_non_object_records() {
        let table = cols(&[("id", "int8")]);
        let err = build_auto_map_payload(&[json!([1, 2])], &table).unwrap_err();
        assert!(err.contains("JSON object"));
    }

    #[test]
    fn jsonb_rows_escape_embedded_specials_inside_json_text() {
        let payload = build_jsonb_payload(&[
            json!({"s": "tab\there and back\\slash and\nnewline"}),
            json!(null),
            json!("bare string"),
        ]);
        // serde_json escapes specials inside JSON strings (\t, \\, \n as
        // two-char sequences); COPY escaping then doubles those backslashes.
        assert_eq!(
            payload.data,
            "{\"s\":\"tab\\\\there and back\\\\\\\\slash and\\\\nnewline\"}\n\\N\n\"bare string\"\n"
        );
        assert_eq!(payload.rows, 3);
    }

    #[test]
    fn copy_statement_quotes_hostile_identifiers() {
        assert_eq!(
            copy_statement("\"t\"", &["id".into(), "we\"ird".into()]),
            "COPY \"t\" (\"id\", \"we\"\"ird\") FROM STDIN (FORMAT text)"
        );
        assert_eq!(
            copy_statement("\"s\".\"t\"", &["data".into()]),
            "COPY \"s\".\"t\" (\"data\") FROM STDIN (FORMAT text)"
        );
    }
}
