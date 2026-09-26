//! Configuration types for the Apache Iceberg source.

use faucet_core::FaucetError;
use iceberg::{NamespaceIdent, TableIdent};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use faucet_common_iceberg::{CatalogConfig, CatalogInner};

/// How much of the table each run reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReadMode {
    /// Read the whole table at the selected snapshot on every run.
    #[default]
    Full,
    /// Read only the data files added by `append` snapshots since the
    /// bookmarked snapshot. The first run (no bookmark) reads the current
    /// snapshot in full; the bookmark is the last processed snapshot id.
    Incremental,
}

/// What an incremental run does when a snapshot since the bookmark rewrote
/// existing data (`overwrite` / `delete`), or the bookmark is no longer an
/// ancestor of the current snapshot (a rollback).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnRewrite {
    /// Fail the run with a message naming the snapshot.
    #[default]
    Fail,
    /// Re-read the current snapshot in full and move the bookmark to it.
    /// Rows already delivered are delivered again — pair with an upsert or
    /// overwrite sink.
    FullRefresh,
}

/// What an incremental run does when the bookmarked snapshot (or one between
/// it and the current snapshot) has been expired from the table metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnExpired {
    /// Fail the run with guidance.
    #[default]
    Fail,
    /// Re-read the current snapshot in full and move the bookmark to it.
    FullRefresh,
}

/// Configuration for the Apache Iceberg source.
///
/// Reads an Iceberg table through a REST / Glue / SQL / HMS catalog (the same
/// catalog block as the Iceberg sink) with column projection, a pushed-down
/// row filter, time travel, and incremental reads between snapshots.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IcebergSourceConfig {
    /// Catalog type and connection settings.
    pub catalog: CatalogConfig,

    /// Table to read, as `namespace.table` (multi-level namespaces are
    /// dot-separated: `lake.analytics.events`).
    pub table: String,

    /// Columns to read (projection). Empty reads every column.
    #[serde(default)]
    pub columns: Vec<String>,

    /// Row filter pushed down to the scan, e.g.
    /// `status = 'active' and (amount >= 10 or vip is not null)`.
    ///
    /// Operators: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `in (…)`,
    /// `not in (…)`, `is null`, `is not null`, `starts_with`,
    /// `not starts_with`, combined with `and` / `or` / `not` and parentheses.
    /// Literals are numbers, `'strings'`, `true` / `false`; dates and
    /// timestamps are written as strings and typed against the column.
    #[serde(default)]
    pub filter: Option<String>,

    /// Time travel: read this snapshot id instead of the current one.
    /// `mode: full` only; mutually exclusive with `as_of_timestamp`.
    #[serde(default)]
    pub snapshot_id: Option<i64>,

    /// Time travel: read the snapshot that was current at this RFC 3339
    /// instant (e.g. `2026-01-31T00:00:00Z`). `mode: full` only.
    #[serde(default)]
    pub as_of_timestamp: Option<String>,

    /// Arrow batch size (rows per page). `0` emits one page per snapshot read.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Data files read concurrently.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    /// `full` (default) re-reads the table each run; `incremental` reads only
    /// data appended since the bookmarked snapshot.
    #[serde(default)]
    pub mode: ReadMode,

    /// Incremental only: reaction to an `overwrite` / `delete` snapshot since
    /// the bookmark.
    #[serde(default)]
    pub on_rewrite: OnRewrite,

    /// Incremental only: reaction to a bookmarked snapshot that has expired.
    #[serde(default)]
    pub on_expired: OnExpired,
}

fn default_batch_size() -> usize {
    faucet_core::DEFAULT_BATCH_SIZE
}

fn default_concurrency() -> usize {
    4
}

/// Split `a.b.table` into its namespace and table name.
pub fn parse_table(table: &str) -> Result<TableIdent, FaucetError> {
    let parts: Vec<&str> = table.split('.').map(str::trim).collect();
    if parts.len() < 2 || parts.iter().any(|p| p.is_empty()) {
        return Err(FaucetError::Config(format!(
            "iceberg: `table` must be `namespace.table` with no empty segment, got {table:?}"
        )));
    }
    let (name, ns) = parts.split_last().expect("at least two parts");
    let ns = NamespaceIdent::from_strs(ns.iter().copied()).map_err(|e| {
        FaucetError::Config(format!("iceberg: invalid namespace in {table:?}: {e}"))
    })?;
    Ok(TableIdent::new(ns, (*name).to_string()))
}

/// Parse an RFC 3339 instant into epoch milliseconds.
pub fn parse_timestamp_ms(ts: &str) -> Result<i64, FaucetError> {
    chrono::DateTime::parse_from_rfc3339(ts.trim())
        .map(|dt| dt.timestamp_millis())
        .map_err(|e| {
            FaucetError::Config(format!(
                "iceberg: `as_of_timestamp` {ts:?} is not an RFC 3339 instant: {e}"
            ))
        })
}

impl IcebergSourceConfig {
    /// Validate the configuration at load time.
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.catalog.validate_connection()?;
        parse_table(&self.table)?;
        faucet_core::validate_batch_size(self.batch_size)?;
        if self.concurrency == 0 {
            return Err(FaucetError::Config(
                "iceberg: `concurrency` must be > 0".to_string(),
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for c in &self.columns {
            if c.trim().is_empty() {
                return Err(FaucetError::Config(
                    "iceberg: `columns` entries must not be empty".to_string(),
                ));
            }
            if !seen.insert(c.as_str()) {
                return Err(FaucetError::Config(format!(
                    "iceberg: column {c:?} is listed twice in `columns`"
                )));
            }
        }
        if let Some(f) = &self.filter {
            crate::filter::parse(f)?;
        }
        if self.snapshot_id.is_some() && self.as_of_timestamp.is_some() {
            return Err(FaucetError::Config(
                "iceberg: set at most one of `snapshot_id` and `as_of_timestamp`".to_string(),
            ));
        }
        if let Some(ts) = &self.as_of_timestamp {
            parse_timestamp_ms(ts)?;
        }
        if self.mode == ReadMode::Incremental
            && (self.snapshot_id.is_some() || self.as_of_timestamp.is_some())
        {
            return Err(FaucetError::Config(
                "iceberg: time travel (`snapshot_id` / `as_of_timestamp`) is only supported \
                 with `mode: full`; incremental reads always advance to the current snapshot"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// The parsed table identifier.
    pub fn table_ident(&self) -> Result<TableIdent, FaucetError> {
        parse_table(&self.table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(v: serde_json::Value) -> IcebergSourceConfig {
        serde_json::from_value(v).expect("config parses")
    }

    fn base() -> serde_json::Value {
        json!({
            "catalog": { "type": "sql", "uri": "sqlite::memory:", "warehouse": "/tmp/wh" },
            "table": "db.events"
        })
    }

    fn with(key: &str, value: serde_json::Value) -> IcebergSourceConfig {
        let mut v = base();
        v[key] = value;
        parse(v)
    }

    #[test]
    fn defaults_apply() {
        let c = parse(base());
        assert_eq!(c.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
        assert_eq!(c.concurrency, 4);
        assert_eq!(c.mode, ReadMode::Full);
        assert_eq!(c.on_rewrite, OnRewrite::Fail);
        assert_eq!(c.on_expired, OnExpired::Fail);
        assert!(c.columns.is_empty() && c.filter.is_none());
        c.validate().expect("valid");
    }

    #[test]
    fn enums_parse_snake_case() {
        let mut v = base();
        v["mode"] = json!("incremental");
        v["on_rewrite"] = json!("full_refresh");
        v["on_expired"] = json!("full_refresh");
        let c = parse(v);
        assert_eq!(c.mode, ReadMode::Incremental);
        assert_eq!(c.on_rewrite, OnRewrite::FullRefresh);
        assert_eq!(c.on_expired, OnExpired::FullRefresh);
    }

    #[test]
    fn unknown_field_rejected() {
        let mut v = base();
        v["bogus"] = json!(1);
        assert!(serde_json::from_value::<IcebergSourceConfig>(v).is_err());
    }

    #[test]
    fn parse_table_splits_namespace() {
        let t = parse_table("lake.analytics.events").unwrap();
        assert_eq!(t.name(), "events");
        assert_eq!(
            t.namespace().as_ref(),
            &vec!["lake".to_string(), "analytics".to_string()]
        );
        for bad in ["events", "", "db.", ".events", "a..b"] {
            assert!(parse_table(bad).is_err(), "{bad:?} should fail");
        }
    }

    #[test]
    fn timestamp_parsing() {
        assert_eq!(parse_timestamp_ms("1970-01-01T00:00:01Z").unwrap(), 1000);
        assert!(parse_timestamp_ms("yesterday").is_err());
    }

    #[test]
    fn validation_errors() {
        let cases: Vec<(IcebergSourceConfig, &str)> = vec![
            (with("table", json!("events")), "namespace.table"),
            (
                with("batch_size", json!(faucet_core::MAX_BATCH_SIZE + 1)),
                "batch_size",
            ),
            (with("concurrency", json!(0)), "concurrency"),
            (with("columns", json!(["a", " "])), "must not be empty"),
            (with("columns", json!(["a", "a"])), "listed twice"),
            (with("filter", json!("a = ")), "filter"),
            (with("as_of_timestamp", json!("nope")), "RFC 3339"),
            (
                with("catalog", json!({ "type": "rest" })),
                "requires a non-empty `uri`",
            ),
        ];
        for (cfg, needle) in cases {
            let err = cfg.validate().unwrap_err().to_string();
            assert!(err.contains(needle), "{err:?} should contain {needle:?}");
        }

        let mut v = base();
        v["snapshot_id"] = json!(1);
        v["as_of_timestamp"] = json!("2026-01-01T00:00:00Z");
        assert!(
            parse(v)
                .validate()
                .unwrap_err()
                .to_string()
                .contains("at most one")
        );

        for (k, val) in [
            ("snapshot_id", json!(1)),
            ("as_of_timestamp", json!("2026-01-01T00:00:00Z")),
        ] {
            let mut v = base();
            v["mode"] = json!("incremental");
            v[k] = val;
            assert!(
                parse(v)
                    .validate()
                    .unwrap_err()
                    .to_string()
                    .contains("mode: full")
            );
        }
    }

    #[test]
    fn valid_time_travel_and_filter() {
        let mut v = base();
        v["snapshot_id"] = json!(42);
        v["filter"] = json!("id > 3 and name = 'x'");
        v["columns"] = json!(["id", "name"]);
        let c = parse(v);
        c.validate().unwrap();
        assert_eq!(c.table_ident().unwrap().name(), "events");
    }
}
