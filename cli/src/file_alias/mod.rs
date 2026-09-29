//! The deprecated `csv`, `jsonl` and `parquet` connector kinds (#779).
//!
//! Each is built as the `file` connector with its format pinned. The
//! registry decodes a config into the old connector's struct first, so an
//! unknown or invalid field fails exactly as before; the functions here then
//! translate the (already valid) raw config into a `file` config.

use serde_json::{Map, Value, json};

#[cfg(test)]
mod golden;

/// The kinds that are now aliases of `file`.
pub const DEPRECATED_FILE_KINDS: &[&str] = &["csv", "jsonl", "parquet"];

/// Which side of a pipeline a connector sits on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Source,
    Sink,
}

/// Whether `kind` is one of the deprecated file kinds.
pub fn is_deprecated_file_kind(kind: &str) -> bool {
    DEPRECATED_FILE_KINDS.contains(&kind)
}

/// What to write instead of a deprecated kind.
pub fn replacement(kind: &str) -> String {
    let format = match kind {
        "jsonl" => "json_lines",
        other => other,
    };
    format!("use `type: file` with `format: {format}`")
}

/// The one-line notice printed when a config uses a deprecated kind.
pub fn deprecation_notice(kind: &str) -> String {
    format!(
        "connector kind `{kind}` is deprecated: {} (the old kind still works)",
        replacement(kind)
    )
}

/// Whether a deprecated kind is still built by its own crate rather than the
/// `file` connector.
///
/// The `file` connector is local-only, so a Parquet location on S3 stays on
/// the old crate, as does a CSV delimiter or quote byte above ASCII (the
/// `file` connector takes them as one-byte strings). Everything else is
/// built as `file`. This is the single switch point for moving the S3 case
/// once the shared writer layer has object-store backends.
pub fn uses_legacy_crate(side: Side, kind: &str, config: &Value) -> bool {
    match (side, kind) {
        (Side::Source, "parquet") => location_type(config, "source") == Some("s3"),
        (Side::Sink, "parquet") => location_type(config, "destination") == Some("s3"),
        (Side::Source, "csv") => {
            non_ascii_byte(config, "delimiter") || non_ascii_byte(config, "quote")
        }
        (Side::Sink, "csv") => non_ascii_byte(config, "delimiter"),
        _ => false,
    }
}

fn location_type<'a>(config: &'a Value, key: &str) -> Option<&'a str> {
    config.get(key)?.get("type")?.as_str()
}

fn non_ascii_byte(config: &Value, key: &str) -> bool {
    config
        .get(key)
        .and_then(Value::as_u64)
        .is_some_and(|b| b > 127)
}

fn byte_string(v: &Value) -> Value {
    let b = v.as_u64().unwrap_or(u64::from(b','));
    Value::String(char::from(u8::try_from(b).unwrap_or(b',')).to_string())
}

fn copy(from: &Value, to: &mut Map<String, Value>, keys: &[&str]) {
    for k in keys {
        if let Some(v) = from.get(*k) {
            to.insert((*k).to_string(), v.clone());
        }
    }
}

fn truthy(v: &Value, key: &str) -> bool {
    v.get(key).and_then(Value::as_bool).unwrap_or(false)
}

/// `csv` source config → `file` source config.
pub fn csv_source(old: &Value) -> Value {
    let mut out = Map::new();
    copy(old, &mut out, &["path", "batch_size", "compression"]);
    out.insert("format".into(), json!("csv"));
    let mut csv = Map::new();
    copy(old, &mut csv, &["has_headers", "null_values"]);
    for k in ["delimiter", "quote"] {
        if let Some(b) = old.get(k) {
            csv.insert(k.into(), byte_string(b));
        }
    }
    csv.insert("flexible".into(), json!(truthy(old, "flexible")));
    out.insert("csv".into(), Value::Object(csv));
    Value::Object(out)
}

/// `parquet` source config (local path or glob) → `file` source config.
pub fn parquet_source(old: &Value) -> Value {
    let mut out = Map::new();
    let loc = old.get("source").cloned().unwrap_or(Value::Null);
    let path = loc.get("path").or_else(|| loc.get("pattern")).cloned();
    if let Some(p) = path {
        out.insert("path".into(), p);
    }
    out.insert("format".into(), json!("parquet"));
    copy(old, &mut out, &["batch_size", "concurrency"]);
    if let Some(cols) = old.get("columns").filter(|c| !c.is_null()) {
        out.insert("parquet".into(), json!({ "columns": cols }));
    }
    Value::Object(out)
}

/// `csv` sink config → `file` sink config.
///
/// The old sink fixed its header from the first page and dropped a later
/// field with a warning, so `csv.on_unknown_field` defaults to `warn` here
/// rather than the `file` sink's `widen`. It also never wrote a header in
/// append mode, so `append: true` turns `csv.has_headers` off.
pub fn csv_sink(old: &Value) -> Value {
    let mut out = Map::new();
    copy(old, &mut out, &["path", "batch_size", "compression"]);
    out.insert("format".into(), json!("csv"));
    if truthy(old, "append") {
        out.insert("mode".into(), json!("append"));
    }
    let mut csv = Map::new();
    if let Some(b) = old.get("delimiter") {
        csv.insert("delimiter".into(), byte_string(b));
    }
    if truthy(old, "append") {
        csv.insert("has_headers".into(), json!(false));
    } else if let Some(h) = old.get("write_headers") {
        csv.insert("has_headers".into(), h.clone());
    }
    let unknown = old
        .get("on_unknown_field")
        .cloned()
        .unwrap_or_else(|| json!("warn"));
    csv.insert("on_unknown_field".into(), unknown);
    out.insert("csv".into(), Value::Object(csv));
    Value::Object(out)
}

/// `jsonl` sink config → `file` sink config.
pub fn jsonl_sink(old: &Value) -> Value {
    let mut out = Map::new();
    copy(
        old,
        &mut out,
        &["path", "batch_size", "compression", "encryption"],
    );
    out.insert("format".into(), json!("json_lines"));
    if truthy(old, "append") {
        out.insert("mode".into(), json!("append"));
    }
    if let Some(p) = old.get("pretty") {
        out.insert("json_lines".into(), json!({ "pretty": p }));
    }
    Value::Object(out)
}

/// `parquet` sink config (local destination) → `file` sink config.
///
/// A fixed `*.parquet` path without rollover is one file, replaced each run.
/// A directory, or a rollover threshold, wrote new uniquely named files on
/// every run; that becomes numbered parts (`part-00001.parquet`, or
/// `name-00001.parquet` next to a fixed path) with `mode: append`, so each run
/// still adds files rather than replacing them. An explicit schema was never
/// supported by the old sink and is still refused.
pub fn parquet_sink(old: &Value) -> Result<Value, String> {
    if old.pointer("/schema/type").and_then(Value::as_str) == Some("explicit") {
        return Err("explicit parquet schemas are not supported yet; use inferred".into());
    }
    let mut out = Map::new();
    let path = old
        .pointer("/destination/path")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let rolls = ["max_rows_per_file", "max_bytes_per_file"]
        .iter()
        .any(|k| old.get(*k).is_some_and(|v| !v.is_null()));
    let fixed = std::path::Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        == Some("parquet");
    let path = if fixed || path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    };
    out.insert("path".into(), json!(path));
    out.insert("format".into(), json!("parquet"));
    if !fixed || rolls {
        out.insert("mode".into(), json!("append"));
    }
    if let Some(n) = old.get("max_rows_per_file").filter(|v| !v.is_null()) {
        out.insert("max_records_per_file".into(), n.clone());
    }
    copy(old, &mut out, &["batch_size"]);
    if let Some(n) = old.get("max_bytes_per_file").filter(|v| !v.is_null()) {
        out.insert("max_bytes_per_file".into(), n.clone());
    }
    let mut pq = Map::new();
    copy(old, &mut pq, &["compression", "row_group_size"]);
    if !pq.is_empty() {
        out.insert("parquet".into(), Value::Object(pq));
    }
    Ok(Value::Object(out))
}

/// Translate a deprecated kind's config into a `file` config.
pub fn to_file_config(side: Side, kind: &str, old: &Value) -> Result<Value, String> {
    match (side, kind) {
        (Side::Source, "csv") => Ok(csv_source(old)),
        (Side::Source, "parquet") => Ok(parquet_source(old)),
        (Side::Sink, "csv") => Ok(csv_sink(old)),
        (Side::Sink, "jsonl") => Ok(jsonl_sink(old)),
        (Side::Sink, "parquet") => parquet_sink(old),
        _ => Err(format!(
            "`{kind}` is not a deprecated file kind on this side"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_source_maps_the_dialect_under_csv() {
        let old = json!({
            "path": "a.tsv", "has_headers": false, "delimiter": 9, "quote": 39,
            "flexible": true, "null_values": [""], "batch_size": 5, "compression": "gzip"
        });
        assert_eq!(
            csv_source(&old),
            json!({
                "path": "a.tsv", "format": "csv", "batch_size": 5, "compression": "gzip",
                "csv": { "has_headers": false, "delimiter": "\t", "quote": "'",
                         "flexible": true, "null_values": [""] }
            })
        );
        assert_eq!(
            csv_source(&json!({"path": "a.csv"})),
            json!({"path": "a.csv", "format": "csv", "csv": {"flexible": false}})
        );
    }

    #[test]
    fn parquet_source_takes_the_path_or_pattern() {
        let old = json!({
            "source": {"type": "glob", "pattern": "d/*.parquet"},
            "columns": ["a"], "batch_size": 10, "concurrency": 2
        });
        assert_eq!(
            parquet_source(&old),
            json!({"path": "d/*.parquet", "format": "parquet", "batch_size": 10,
                   "concurrency": 2, "parquet": {"columns": ["a"]}})
        );
        let local = json!({"source": {"type": "local_path", "path": "a.parquet"}, "columns": null});
        assert_eq!(
            parquet_source(&local),
            json!({"path": "a.parquet", "format": "parquet"})
        );
    }

    #[test]
    fn csv_sink_keeps_the_old_unknown_field_default() {
        let old = json!({"path": "o.csv", "delimiter": 59, "write_headers": false, "append": true, "batch_size": 3});
        assert_eq!(
            csv_sink(&old),
            json!({"path": "o.csv", "format": "csv", "batch_size": 3, "mode": "append",
                   "csv": {"delimiter": ";", "has_headers": false, "on_unknown_field": "warn"}})
        );
        assert_eq!(
            csv_sink(&json!({"path": "o.csv", "append": true, "write_headers": true}))["csv"]["has_headers"],
            false
        );
        assert_eq!(
            csv_sink(&json!({"path": "o.csv", "write_headers": true}))["csv"]["has_headers"],
            true
        );
        let err = json!({"path": "o.csv", "on_unknown_field": "error", "append": false});
        assert_eq!(
            csv_sink(&err),
            json!({"path": "o.csv", "format": "csv", "csv": {"on_unknown_field": "error"}})
        );
    }

    #[test]
    fn jsonl_sink_pins_json_lines() {
        let old = json!({"path": "o.json", "append": true, "pretty": true, "compression": "zstd",
                         "encryption": {"key": "k"}});
        assert_eq!(
            jsonl_sink(&old),
            json!({"path": "o.json", "format": "json_lines", "compression": "zstd",
                   "encryption": {"key": "k"}, "mode": "append", "json_lines": {"pretty": true}})
        );
        assert_eq!(
            jsonl_sink(&json!({"path": "o.jsonl", "append": false})),
            json!({"path": "o.jsonl", "format": "json_lines"})
        );
    }

    #[test]
    fn parquet_sink_paths_and_modes() {
        let single = json!({"destination": {"type": "local_path", "path": "o/a.parquet"},
                            "compression": "zstd", "row_group_size": 7,
                            "schema": {"type": "inferred", "sample_size": 5}});
        assert_eq!(
            parquet_sink(&single).unwrap(),
            json!({"path": "o/a.parquet", "format": "parquet",
                   "parquet": {"compression": "zstd", "row_group_size": 7}})
        );
        let rolled = json!({"destination": {"type": "local_path", "path": "o/a.parquet"},
                            "max_rows_per_file": 10, "max_bytes_per_file": 99, "batch_size": 4});
        assert_eq!(
            parquet_sink(&rolled).unwrap(),
            json!({"path": "o/a.parquet", "format": "parquet", "mode": "append",
                   "max_records_per_file": 10, "max_bytes_per_file": 99, "batch_size": 4})
        );
        let dir = json!({"destination": {"type": "local_path", "path": "o"}});
        assert_eq!(
            parquet_sink(&dir).unwrap(),
            json!({"path": "o/", "format": "parquet", "mode": "append"})
        );
        let slash = json!({"destination": {"type": "local_path", "path": "o/"}});
        assert_eq!(parquet_sink(&slash).unwrap()["path"], "o/");
        let explicit = json!({"destination": {"type": "local_path", "path": "o/"}, "schema": {"type": "explicit"}});
        assert!(parquet_sink(&explicit).unwrap_err().contains("explicit"));
    }

    #[test]
    fn dispatch_and_legacy_switch() {
        assert!(to_file_config(Side::Source, "jsonl", &json!({})).is_err());
        assert!(to_file_config(Side::Sink, "file", &json!({})).is_err());
        assert_eq!(
            to_file_config(Side::Source, "csv", &json!({"path": "a"})).unwrap()["format"],
            "csv"
        );
        assert_eq!(
            to_file_config(Side::Source, "parquet", &json!({})).unwrap()["format"],
            "parquet"
        );
        assert_eq!(
            to_file_config(Side::Sink, "csv", &json!({})).unwrap()["format"],
            "csv"
        );
        assert_eq!(
            to_file_config(Side::Sink, "jsonl", &json!({})).unwrap()["format"],
            "json_lines"
        );
        assert_eq!(
            to_file_config(Side::Sink, "parquet", &json!({})).unwrap()["format"],
            "parquet"
        );

        let s3 = json!({"source": {"type": "s3", "bucket": "b"}});
        assert!(uses_legacy_crate(Side::Source, "parquet", &s3));
        assert!(!uses_legacy_crate(Side::Sink, "parquet", &s3));
        let s3_dest = json!({"destination": {"type": "s3", "bucket": "b"}});
        assert!(uses_legacy_crate(Side::Sink, "parquet", &s3_dest));
        let local = json!({"source": {"type": "local_path", "path": "a"}});
        assert!(!uses_legacy_crate(Side::Source, "parquet", &local));
        assert!(uses_legacy_crate(
            Side::Source,
            "csv",
            &json!({"quote": 200})
        ));
        assert!(uses_legacy_crate(
            Side::Sink,
            "csv",
            &json!({"delimiter": 167})
        ));
        assert!(!uses_legacy_crate(
            Side::Sink,
            "csv",
            &json!({"delimiter": 59})
        ));
        assert!(!uses_legacy_crate(Side::Sink, "jsonl", &json!({})));

        assert!(is_deprecated_file_kind("jsonl"));
        assert!(!is_deprecated_file_kind("file"));
        assert_eq!(
            replacement("jsonl"),
            "use `type: file` with `format: json_lines`"
        );
        assert!(deprecation_notice("csv").starts_with("connector kind `csv` is deprecated"));
    }
}
