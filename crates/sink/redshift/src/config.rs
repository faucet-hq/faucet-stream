//! Amazon Redshift sink configuration.

use faucet_common_redshift::RedshiftConnection;
use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How the sink loads rows into Redshift.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RedshiftWriteStrategy {
    /// Stage each page to S3 and bulk-load it with `COPY … FROM 's3://…'` — the
    /// default and by far the fastest path for Redshift (the recommended way to
    /// load data). Requires `staging_bucket` and `iam_role`.
    #[default]
    Copy,
    /// Multi-row `INSERT INTO … VALUES (…), (…)`. Portable and needs no S3, but
    /// much slower than `COPY` for anything beyond small batches. Redshift does
    /// not recommend row-by-row inserts for bulk data.
    Insert,
}

impl RedshiftWriteStrategy {
    /// Lower-case wire name, for error messages.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Copy => "copy",
            Self::Insert => "insert",
        }
    }
}

/// Format of the staged file that `COPY` reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RedshiftCopyFormat {
    /// Newline-delimited JSON objects, loaded with `FORMAT AS JSON 'auto ignorecase'` (keys match columns ignoring case).
    /// Maps by column **name** (order-independent) and handles NULLs and typed
    /// columns cleanly — the default.
    #[default]
    Jsonl,
    /// RFC-4180 CSV, loaded with `FORMAT AS CSV`. Column order is taken from the
    /// destination table's schema and passed explicitly in the `COPY` column
    /// list.
    Csv,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_max_connections() -> u32 {
    5
}

/// `COPY`-path settings for the Redshift sink (#654 M20).
///
/// Six keys that only mean anything under `write_strategy: copy` used to sit
/// flat beside `table_name` and `batch_size`, with nothing saying they were
/// inert for `insert`. Grouping them is what makes that legible — the same
/// shape the BigQuery sink already uses for `bulk_load:`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RedshiftCopySpec {
    /// Staged-file format. Defaults to [`RedshiftCopyFormat::Jsonl`].
    #[serde(default)]
    pub format: RedshiftCopyFormat,
    /// S3 bucket used to stage `COPY` files. **Required** for `copy`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staging_bucket: Option<String>,
    /// Key prefix for staged objects (e.g. `redshift-staging/`). Empty by
    /// default.
    #[serde(default)]
    pub staging_prefix: String,
    /// IAM role ARN Redshift assumes to read the staged file
    /// (`COPY … IAM_ROLE '<arn>'`). **Required** for `copy`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iam_role: Option<String>,
    /// AWS region of the staging bucket, used for both the S3 client and the
    /// `COPY … REGION '<region>'` clause. `None` uses the SDK default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Custom endpoint URL for S3-compatible services (e.g. MinIO) — a testing
    /// aid; production loads use real S3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-faucet-resource" = "url"))]
    pub endpoint_url: Option<String>,
}

/// Configuration for the Amazon Redshift sink.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct RedshiftSinkConfig {
    /// Connection block (host / port / database / user / credentials / tls),
    /// flattened to the config top level.
    #[serde(flatten)]
    pub connection: RedshiftConnection,
    /// Target table name.
    pub table_name: String,
    /// Create the target table (and its schema, when `schema:` is set) if it
    /// does not exist, inferring the columns from the first written page
    /// (#580). Enabled by default: a first-ever sync cannot assume the
    /// destination already exists.
    ///
    /// Every inferred column is created nullable, and without a DISTKEY or
    /// SORTKEY — faucet has no basis to choose either, and the wrong choice is
    /// baked into the table. Define the table yourself and set
    /// `create_table: false` when distribution or sort matters, which it does
    /// for any table you intend to query at scale.
    #[serde(default = "default_create_table")]
    pub create_table: bool,
    /// Commit-group size for the cross-page accumulator (#617).
    ///
    /// Warehouse loads are dominated by per-operation overhead, and
    /// `batch_size` can only ever *split* a page — it can never merge
    /// undersized ones, so a small source page meant one expensive warehouse
    /// operation per small page. Records now accumulate across `write_batch`
    /// calls and commit once per threshold, plus once at `flush`.
    ///
    /// When neither `commit_rows` nor `commit_bytes` is set, a group commits
    /// at 100,000 rows or ~256 MiB, whichever comes first, so a large load
    /// never buffers the whole run in memory. `0` means no row limit. A
    /// failed group commit keeps its uncommitted rows for the retry.
    ///
    /// Only the append path accumulates: `delivery: exactly_once` and the DLQ
    /// path commit per page, because a watermark must land with its own page
    /// and a DLQ must report which rows of *this* page failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_rows: Option<usize>,
    /// Estimated-bytes counterpart of [`commit_rows`](Self::commit_rows)
    /// (#617). Rows are a poor proxy for how much work a warehouse commit is;
    /// this bounds the buffered size. `0` means no byte limit; see
    /// [`commit_rows`](Self::commit_rows) for the default when both are unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_bytes: Option<usize>,
    /// Optional schema (namespace) qualifying [`table_name`](Self::table_name).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// How rows are loaded. Defaults to [`RedshiftWriteStrategy::Copy`].
    #[serde(default)]
    pub write_strategy: RedshiftWriteStrategy,
    /// `COPY`-path settings, grouped (#654 M20). Applies only to
    /// `write_strategy: copy`; when present it supersedes the six deprecated
    /// flat keys below. This mirrors the BigQuery sink's `bulk_load:` block,
    /// which is the house shape for a staged-load configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copy: Option<RedshiftCopySpec>,
    /// **Deprecated** — use `copy.format`.
    ///
    /// Staged-file format for the `COPY` path. Defaults to
    /// [`RedshiftCopyFormat::Jsonl`]. Ignored by the `insert` strategy.
    #[serde(default)]
    pub copy_format: RedshiftCopyFormat,
    /// **Deprecated** — use `copy.staging_bucket`.
    ///
    /// S3 bucket used to stage `COPY` files. **Required** when
    /// `write_strategy: copy`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staging_bucket: Option<String>,
    /// **Deprecated** — use `copy.staging_prefix`.
    ///
    /// Key prefix for staged objects (e.g. `redshift-staging/`). Defaults to
    /// empty.
    #[serde(default)]
    pub staging_prefix: String,
    /// **Deprecated** — use `copy.iam_role`.
    ///
    /// IAM role ARN Redshift assumes to read the staged file
    /// (`COPY … IAM_ROLE '<arn>'`). **Required** when `write_strategy: copy`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iam_role: Option<String>,
    /// **Deprecated** — use `copy.region`.
    ///
    /// AWS region of the staging bucket (used for both the S3 client and the
    /// `COPY … REGION '<region>'` clause). `None` uses the SDK default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// **Deprecated** — use `copy.endpoint_url`.
    ///
    /// Custom endpoint URL for S3-compatible services (e.g. MinIO) — testing
    /// aid; production loads use real S3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-faucet-resource" = "url"))]
    pub endpoint_url: Option<String>,
    /// Rows per load unit. For `insert`, the per-statement multi-row chunk size;
    /// for `copy`, the number of rows per staged S3 object (a commit group's
    /// objects load in one `COPY` through a manifest). Defaults to
    /// [`DEFAULT_BATCH_SIZE`]. `0` = one unit for the whole page.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum number of connections in the pool. Defaults to 5.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

fn default_create_table() -> bool {
    true
}

impl RedshiftSinkConfig {
    /// The effective `COPY` settings: the `copy:` block when present,
    /// otherwise the six deprecated flat keys (#654 M20).
    ///
    /// The block wins wholesale — several flat keys have defaults, so "was it
    /// set?" is not observable and a per-field merge would silently mix two
    /// spellings of one setting.
    pub fn copy_spec(&self) -> RedshiftCopySpec {
        self.copy.clone().unwrap_or_else(|| RedshiftCopySpec {
            format: self.copy_format,
            staging_bucket: self.staging_bucket.clone(),
            staging_prefix: self.staging_prefix.clone(),
            iam_role: self.iam_role.clone(),
            region: self.region.clone(),
            endpoint_url: self.endpoint_url.clone(),
        })
    }

    /// Validate the config. Enforces that the `copy` strategy has a staging
    /// bucket and IAM role.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.table_name.trim().is_empty() {
            return Err(FaucetError::Config(
                "redshift sink: `table_name` must not be empty".into(),
            ));
        }
        if self.write_strategy == RedshiftWriteStrategy::Copy {
            // Validate what the load will use: the `copy:` block wins wholesale.
            let spec = self.copy_spec();
            let bucket_ok = spec
                .staging_bucket
                .as_ref()
                .is_some_and(|b| !b.trim().is_empty());
            if !bucket_ok {
                return Err(FaucetError::Config(
                    "redshift sink: write_strategy: copy requires a non-empty `staging_bucket`"
                        .into(),
                ));
            }
            let role_ok = spec.iam_role.as_ref().is_some_and(|r| !r.trim().is_empty());
            if !role_ok {
                return Err(FaucetError::Config(
                    "redshift sink: write_strategy: copy requires a non-empty `iam_role`".into(),
                ));
            }
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        Ok(())
    }
}

impl RedshiftSinkConfig {
    /// What a failed batch write leaves behind (#737): a `copy` commit group
    /// is one `COPY` (its chunks staged as objects behind one manifest);
    /// `insert` splits by bind parameters.
    pub fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        if self.write_strategy == RedshiftWriteStrategy::Copy {
            faucet_core::BatchAtomicity::Atomic
        } else {
            faucet_core::BatchAtomicity::BestEffort
        }
    }
}

#[cfg(test)]
mod tests;
