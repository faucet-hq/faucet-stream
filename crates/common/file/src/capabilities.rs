//! The capabilities of a [`WriterSink`](crate::write::WriterSink)-based sink,
//! answered from its raw config alone (RFC 0013): what the sink's
//! `ConnectorDescriptor` returns.

use crate::FileFormatChoice;
use crate::write::PART_TOKEN;
use faucet_core::capabilities::config::str_field;
use faucet_core::{Capabilities, CsvUnknownField, FileFormat, SchemaReport, WriteMode};
use serde_json::Value;

/// How a file-writing sink's config names its output and format.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkShape {
    /// The config key holding the file name template (`"path"`).
    pub path_field: &'static str,
    /// The `format` used when the config does not set one.
    pub default_format: FileFormatChoice,
    /// The `file_extension` used when the config sets no path and no
    /// extension; `None` for a sink without the field.
    pub default_extension: Option<&'static str>,
    /// Whether the sink crate's `arrow` feature is on (`cfg!(feature = "arrow")`).
    pub arrow: bool,
}

/// The format a sink with `shape` writes under `config`, resolved the way
/// the writer resolves it; `None` when the config names no writable format.
pub fn resolved_format(config: &Value, shape: SinkShape) -> Option<FileFormat> {
    let choice = config
        .get("format")
        .and_then(|f| serde_json::from_value::<FileFormatChoice>(f.clone()).ok())
        .unwrap_or(shape.default_format);
    if let Some(f) = choice.explicit() {
        return Some(f);
    }
    let name = match config.get(shape.path_field) {
        Some(Value::String(p)) => p.replace(PART_TOKEN, "00001"),
        Some(Value::Null) | None => match shape.default_extension {
            Some(default) => str_field(config, "file_extension")
                .unwrap_or(default)
                .to_string(),
            None => return None,
        },
        Some(_) => return None,
    };
    FileFormatChoice::Auto.resolve_writable(&name).ok()
}

/// Whether a writer in `format` with `on_unknown` drops record fields that
/// are not in its header: CSV under `csv.on_unknown_field: warn` freezes the
/// header from the first page.
pub fn drops_unknown_fields(format: FileFormat, on_unknown: CsvUnknownField) -> bool {
    format == FileFormat::Csv && on_unknown == CsvUnknownField::Warn
}

/// What a [`WriterSink`](crate::write::WriterSink)-based sink with `shape`
/// declares under `config`: append and overwrite; columnar for Parquet and
/// Avro when `arrow` is on; drops unknown fields for CSV under
/// `csv.on_unknown_field: warn`. A null `config` is the kind-level view:
/// columnar whenever `arrow` is on, and the default config's schema report.
pub fn writer_sink_capabilities(config: &Value, shape: SinkShape) -> Capabilities {
    if config.is_null() {
        return Capabilities::builder()
            .write_modes([WriteMode::Append, WriteMode::Overwrite])
            .columnar(shape.arrow && cfg!(feature = "arrow"))
            .build();
    }
    let format = resolved_format(config, shape);
    let on_unknown = config
        .get("csv")
        .and_then(|c| c.get("on_unknown_field"))
        .and_then(|v| serde_json::from_value::<CsvUnknownField>(v.clone()).ok())
        .unwrap_or_default();
    let mut b = Capabilities::builder()
        .write_modes([WriteMode::Append, WriteMode::Overwrite])
        .columnar(
            shape.arrow
                && cfg!(feature = "arrow")
                && matches!(format, Some(FileFormat::Parquet | FileFormat::Avro)),
        );
    if format.is_some_and(|f| drops_unknown_fields(f, on_unknown)) {
        b = b.schema_report(SchemaReport::DropsUnknownFields);
    }
    b.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const LOCAL: SinkShape = SinkShape {
        path_field: "path",
        default_format: FileFormatChoice::Auto,
        default_extension: None,
        arrow: true,
    };
    const REMOTE: SinkShape = SinkShape {
        path_field: "path",
        default_format: FileFormatChoice::JsonLines,
        default_extension: Some(".jsonl"),
        arrow: true,
    };

    #[test]
    fn format_resolves_like_the_writer() {
        assert_eq!(
            resolved_format(&json!({"path": "a-{part}.csv.gz"}), LOCAL),
            Some(FileFormat::Csv)
        );
        assert_eq!(
            resolved_format(&json!({"path": "out/", "format": "parquet"}), LOCAL),
            Some(FileFormat::Parquet)
        );
        assert_eq!(resolved_format(&json!({"path": "a.bin"}), LOCAL), None);
        assert_eq!(resolved_format(&json!({"path": "a.orc"}), LOCAL), None);
        assert_eq!(resolved_format(&json!({}), LOCAL), None);
        assert_eq!(resolved_format(&json!({"path": 3}), LOCAL), None);
        assert_eq!(
            resolved_format(&json!({"path": "a.csv", "format": "${f}"}), LOCAL),
            Some(FileFormat::Csv)
        );
        assert_eq!(
            resolved_format(&json!({"path": "a.csv"}), REMOTE),
            Some(FileFormat::JsonLines)
        );
        assert_eq!(
            resolved_format(&json!({"format": "auto"}), REMOTE),
            Some(FileFormat::JsonLines)
        );
        assert_eq!(
            resolved_format(
                &json!({"format": "auto", "file_extension": ".parquet", "path": null}),
                REMOTE
            ),
            Some(FileFormat::Parquet)
        );
        assert_eq!(
            resolved_format(&json!({"format": "auto", "path": "x.avro"}), REMOTE),
            Some(FileFormat::Avro)
        );
    }

    #[test]
    fn capabilities_follow_format_and_csv_policy() {
        let plain = writer_sink_capabilities(&json!({"path": "a.jsonl"}), LOCAL);
        assert_eq!(
            plain.write_modes(),
            &[WriteMode::Append, WriteMode::Overwrite]
        );
        assert_eq!(plain.schema_report(), SchemaReport::Undeclared);
        assert!(!plain.columnar());
        assert!(!plain.idempotent_writes() && !plain.keyed_dedup());

        let warn = writer_sink_capabilities(
            &json!({"path": "a.csv", "csv": {"on_unknown_field": "warn"}}),
            LOCAL,
        );
        assert_eq!(warn.schema_report(), SchemaReport::DropsUnknownFields);
        for policy in ["widen", "error", "${p}"] {
            let c = writer_sink_capabilities(
                &json!({"path": "a.csv", "csv": {"on_unknown_field": policy}}),
                LOCAL,
            );
            assert_eq!(c.schema_report(), SchemaReport::Undeclared, "{policy}");
        }
        let json_warn = writer_sink_capabilities(
            &json!({"path": "a.jsonl", "csv": {"on_unknown_field": "warn"}}),
            LOCAL,
        );
        assert_eq!(json_warn.schema_report(), SchemaReport::Undeclared);

        for path in ["a.parquet", "a.avro"] {
            let c = writer_sink_capabilities(&json!({ "path": path }), LOCAL);
            assert_eq!(c.columnar(), cfg!(feature = "arrow"), "{path}");
            let off = writer_sink_capabilities(
                &json!({ "path": path }),
                SinkShape {
                    arrow: false,
                    ..LOCAL
                },
            );
            assert!(!off.columnar());
        }
        assert!(!writer_sink_capabilities(&json!({"path": "a.bin"}), LOCAL).columnar());
    }

    #[test]
    fn null_config_is_the_kind_level_view() {
        let c = writer_sink_capabilities(&Value::Null, REMOTE);
        assert_eq!(c.write_modes(), &[WriteMode::Append, WriteMode::Overwrite]);
        assert_eq!(c.columnar(), cfg!(feature = "arrow"));
        assert_eq!(c.schema_report(), SchemaReport::Undeclared);
        assert!(!c.keyed_dedup());
        let off = writer_sink_capabilities(
            &Value::Null,
            SinkShape {
                arrow: false,
                ..LOCAL
            },
        );
        assert!(!off.columnar());
    }

    #[test]
    fn drops_unknown_fields_only_for_csv_warn() {
        assert!(drops_unknown_fields(FileFormat::Csv, CsvUnknownField::Warn));
        assert!(!drops_unknown_fields(
            FileFormat::Csv,
            CsvUnknownField::Widen
        ));
        assert!(!drops_unknown_fields(
            FileFormat::Csv,
            CsvUnknownField::Error
        ));
        assert!(!drops_unknown_fields(
            FileFormat::JsonLines,
            CsvUnknownField::Warn
        ));
    }
}
