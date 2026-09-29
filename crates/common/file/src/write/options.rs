//! Write options shared by every file-writing sink: output modes, the
//! rollover template token, and the per-format blocks the shared encoders
//! read (Parquet, JSON Lines).

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_true() -> bool {
    true
}

/// The placeholder a path template uses for the rollover part number.
pub const PART_TOKEN: &str = "{part}";

/// What happens when a file this run writes already exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileMode {
    /// Replace it: the new file is written beside it and renamed over it, so
    /// the old contents stay readable until the new ones are complete.
    #[default]
    Overwrite,
    /// Add to it. JSON Lines, CSV and raw text only — a JSON array, XML,
    /// Excel, Avro or Parquet file cannot be appended to without rewriting.
    /// With rollover, numbering continues after the highest existing part.
    Append,
    /// Fail the run instead of touching an existing file.
    ErrorIfExists,
}

/// Pipeline-level write mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileWriteMode {
    /// Write this run's files; leave everything else in place.
    #[default]
    Append,
    /// Replace the destination's whole output set: files are staged in a
    /// hidden directory beside the destination and moved into place only when
    /// the run succeeds, then files of an earlier run that match the path
    /// template and were not rewritten are removed. A failed or cancelled run
    /// leaves the previous output untouched.
    Overwrite,
}

/// Parquet column-chunk compression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParquetCodec {
    /// Uncompressed pages. `uncompressed` is accepted as another name.
    #[serde(alias = "uncompressed")]
    None,
    /// Snappy (the default): fast and universally readable.
    #[default]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParquetOptions {
    /// Column-chunk compression (default `snappy`). File-level `compression`
    /// does not apply to Parquet.
    #[serde(default)]
    pub compression: ParquetCodec,
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
            compression: ParquetCodec::default(),
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            schema: None,
        }
    }
}

fn zstd() -> ParquetCodec {
    ParquetCodec::Zstd
}

/// Parquet write options for the object-store and SFTP sinks: the same
/// fields as [`ParquetOptions`], but `compression` defaults to `zstd`, which
/// is what those sinks wrote before the shared writer — smaller objects to
/// upload and store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RemoteParquetOptions {
    /// Column-chunk compression (default `zstd`). File-level `compression`
    /// does not apply to Parquet.
    #[serde(default = "zstd")]
    pub compression: ParquetCodec,
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

impl Default for RemoteParquetOptions {
    fn default() -> Self {
        Self {
            compression: ParquetCodec::Zstd,
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            schema: None,
        }
    }
}

impl From<RemoteParquetOptions> for ParquetOptions {
    fn from(o: RemoteParquetOptions) -> Self {
        Self {
            compression: o.compression,
            row_group_size: o.row_group_size,
            schema: o.schema,
        }
    }
}

impl From<ParquetOptions> for RemoteParquetOptions {
    fn from(o: ParquetOptions) -> Self {
        Self {
            compression: o.compression,
            row_group_size: o.row_group_size,
            schema: o.schema,
        }
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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JsonLinesOptions {
    /// Pretty-print each record across several lines. The output is then a
    /// stream of JSON documents rather than one record per line.
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
