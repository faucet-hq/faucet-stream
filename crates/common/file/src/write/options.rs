//! Write options shared by every file-writing sink: what happens to an
//! existing file, the pipeline write mode, the rollover template token, and
//! the per-format blocks the shared encoders read (Parquet, JSON Lines).
//!
//! **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
//! minor release; any change is called out in the changelog.

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_true() -> bool {
    true
}

/// The placeholder a path template uses for the rollover part number.
pub const PART_TOKEN: &str = "{part}";

/// What happens when a file this run writes already exists (`if_exists`).
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IfExists {
    /// Replace that one file: the new file is built beside it and swapped in,
    /// so the old contents stay readable until the new ones are complete.
    /// `overwrite` is accepted as another name.
    #[default]
    #[serde(alias = "overwrite")]
    Replace,
    /// Add to it. JSON Lines, CSV and raw text only — a JSON array, XML,
    /// Excel, Avro or Parquet file cannot be appended to without rewriting.
    /// With rollover, numbering continues after the highest existing part.
    Append,
    /// Fail the run instead of touching an existing file.
    /// `error_if_exists` is accepted as another name.
    #[serde(alias = "error_if_exists")]
    Error,
}

/// Pipeline-level write mode (`write_mode`).
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileWriteMode {
    /// Write this run's files; leave everything else in place.
    #[default]
    Append,
    /// Replace the destination's whole output set. The run writes into a
    /// hidden swap area beside the destination; only when it succeeds are the
    /// files moved into place, and then files of an earlier run that match
    /// the path template and were not rewritten are removed. A failed or
    /// cancelled run leaves the previous output untouched. The move is not
    /// atomic across files: a reader listing the destination while it runs
    /// can see some new files beside some old ones. A move that was
    /// interrupted is finished by the next run before it writes anything.
    Overwrite,
}

/// Parquet column-chunk compression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParquetCodec {
    /// Uncompressed pages.
    None,
    /// Snappy: fast and universally readable.
    Snappy,
    /// Gzip.
    Gzip,
    /// Zstandard.
    Zstd,
    /// LZ4 (the `LZ4_RAW` codec).
    Lz4,
}

/// Rows per Parquet row group unless `parquet.row_group_size` says otherwise.
pub const DEFAULT_ROW_GROUP_SIZE: usize = 1024 * 1024;

fn default_row_group_size() -> usize {
    DEFAULT_ROW_GROUP_SIZE
}

/// Parquet write options.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParquetOptions {
    /// Column-chunk compression. Unset, it is `zstd` on the object-store and
    /// SFTP sinks and `snappy` on the local file sink. A file-level
    /// `compression` codec, when one applies, compresses the finished file
    /// on top of this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<ParquetCodec>,
    /// Maximum rows per row group (default 1,048,576). Smaller groups let
    /// readers skip more data and bound the writer's memory.
    #[serde(default = "default_row_group_size")]
    pub row_group_size: usize,
    /// An explicit schema, in column order. Without it the schema is
    /// inferred from the records and widened when a later page adds a field.
    /// With it the file has exactly these columns: a record field the schema
    /// does not name fails the write, and a value that does not fit its
    /// column's type fails naming the column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Vec<ParquetField>>,
}

impl Default for ParquetOptions {
    fn default() -> Self {
        Self {
            compression: None,
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            schema: None,
        }
    }
}

impl ParquetOptions {
    /// The codec to write with: the configured one, else `default`.
    pub fn codec_or(&self, default: ParquetCodec) -> ParquetCodec {
        self.compression.unwrap_or(default)
    }
}

/// One column of an explicit Parquet schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParquetField {
    /// Column name — the record field it is read from.
    pub name: String,
    /// Column type.
    #[serde(rename = "type")]
    pub data_type: ParquetType,
    /// Whether the column may be null or missing (default `true`).
    #[serde(default = "default_true")]
    pub nullable: bool,
}

/// A column type of an explicit Parquet schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParquetType {
    /// `true` / `false`.
    Boolean,
    /// 32-bit signed integer.
    Int32,
    /// 64-bit signed integer.
    Int64,
    /// 64-bit unsigned integer.
    Uint64,
    /// 32-bit float.
    Float32,
    /// 64-bit float.
    Float64,
    /// UTF-8 text.
    String,
    /// A calendar date, from `YYYY-MM-DD` text.
    Date,
    /// A UTC timestamp in milliseconds, from RFC 3339 text or epoch numbers.
    TimestampMs,
    /// A UTC timestamp in microseconds.
    TimestampUs,
    /// A UTC timestamp in nanoseconds.
    TimestampNs,
    /// A fixed-point decimal with the given precision and scale, from
    /// numbers or numeric text.
    Decimal {
        /// Total digits (1–38).
        precision: u8,
        /// Digits after the point.
        scale: i8,
    },
}

/// JSON Lines write options.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JsonLinesOptions {
    /// Pretty-print each record across several lines. The output is then a
    /// stream of JSON documents rather than one record per line, which the
    /// file source cannot read back.
    #[serde(default)]
    pub pretty: bool,
}

/// Check the Parquet options for values that would fail mid-run.
pub fn validate_parquet(p: &ParquetOptions) -> Result<(), FaucetError> {
    if p.row_group_size == 0 {
        return Err(FaucetError::Config(
            "`parquet.row_group_size` must be at least 1".into(),
        ));
    }
    let Some(fields) = &p.schema else {
        return Ok(());
    };
    if fields.is_empty() {
        return Err(FaucetError::Config(
            "`parquet.schema` must name at least one column".into(),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for f in fields {
        if f.name.is_empty() || !seen.insert(f.name.as_str()) {
            return Err(FaucetError::Config(format!(
                "`parquet.schema` column names must be unique and non-empty, got {:?}",
                f.name
            )));
        }
        if let ParquetType::Decimal { precision, scale } = f.data_type
            && (!(1..=38).contains(&precision)
                || i16::from(scale).unsigned_abs() > u16::from(precision))
        {
            return Err(FaucetError::Config(format!(
                "`parquet.schema` column '{}': decimal precision must be 1–38 and \
                 the scale no larger than the precision",
                f.name
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn if_exists_reads_the_value_names_that_shipped_first() {
        for (v, want) in [
            ("replace", IfExists::Replace),
            ("overwrite", IfExists::Replace),
            ("append", IfExists::Append),
            ("error", IfExists::Error),
            ("error_if_exists", IfExists::Error),
        ] {
            let got: IfExists = serde_json::from_value(json!(v)).unwrap();
            assert_eq!(got, want, "{v}");
        }
        assert_eq!(
            serde_json::to_value(IfExists::Replace).unwrap(),
            json!("replace")
        );
    }

    #[test]
    fn parquet_codec_has_one_name_per_value_and_a_per_sink_default() {
        assert!(serde_json::from_value::<ParquetCodec>(json!("uncompressed")).is_err());
        let p = ParquetOptions::default();
        assert_eq!(p.codec_or(ParquetCodec::Zstd), ParquetCodec::Zstd);
        let p: ParquetOptions = serde_json::from_value(json!({"compression": "lz4"})).unwrap();
        assert_eq!(p.codec_or(ParquetCodec::Snappy), ParquetCodec::Lz4);
        assert_eq!(p.row_group_size, DEFAULT_ROW_GROUP_SIZE);
    }

    #[test]
    fn parquet_validation_refuses_what_would_fail_mid_run() {
        let mut p = ParquetOptions {
            row_group_size: 0,
            ..ParquetOptions::default()
        };
        assert!(validate_parquet(&p).is_err());
        p.row_group_size = 1;
        p.schema = Some(Vec::new());
        assert!(validate_parquet(&p).is_err());
        let col = |name: &str, t| ParquetField {
            name: name.into(),
            data_type: t,
            nullable: true,
        };
        p.schema = Some(vec![
            col("a", ParquetType::Int64),
            col("a", ParquetType::Int64),
        ]);
        assert!(validate_parquet(&p).is_err());
        p.schema = Some(vec![col(
            "d",
            ParquetType::Decimal {
                precision: 3,
                scale: 4,
            },
        )]);
        assert!(validate_parquet(&p).is_err());
        p.schema = Some(vec![col("a", ParquetType::Int64)]);
        validate_parquet(&p).unwrap();
    }
}
