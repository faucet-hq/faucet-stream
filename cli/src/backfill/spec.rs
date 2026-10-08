//! Serde config types for the optional top-level `backfill:` block.
//!
//! The block holds **defaults** for `faucet backfill` (window size,
//! concurrency, timezone); the actual range always comes from the command
//! line (`--from/--to` or `--from-bookmark/--to-bookmark`). Ignored by
//! `faucet run` (like `schedule:` / `replication:`).

use crate::error::{CliError, CliResult};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Top-level `backfill:` defaults block.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BackfillSpec {
    /// Default window chunk when `--window` is not passed — a duration like
    /// `1d`, `6h`, `30m`, `45s`, or `1w`. Omitted = the whole range runs as a
    /// single unit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<String>,
    /// Default max concurrently-running window units when `--concurrency` is
    /// not passed. Defaults to 1 (sequential).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<usize>,
    /// IANA timezone (e.g. `America/New_York`) in which date boundaries like
    /// `--from 2026-06-01` are interpreted and `${now.*}` tokens render.
    /// Defaults to UTC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
}

impl BackfillSpec {
    /// Fail-fast validation of the defaults block, run by `faucet validate`
    /// and again by `faucet backfill` before any execution. `source_configs`
    /// are the serialized source configs of the pipeline's root rows — a
    /// `backfill:` block on a pipeline whose source references no
    /// `${backfill.*}` / `${now.*}` scoping token would replay identical data
    /// into every window, so it is rejected here (bookmark-mode backfills
    /// don't need the block at all).
    pub fn validate(&self, source_configs: &[String]) -> CliResult<()> {
        if let Some(w) = &self.window {
            crate::backfill::plan::parse_window(w)?;
        }
        if self.concurrency == Some(0) {
            return Err(CliError::Config(
                "backfill.concurrency must be at least 1".into(),
            ));
        }
        if let Some(tz) = &self.timezone {
            parse_timezone(tz)?;
        }
        if !source_configs.is_empty() && !source_configs.iter().any(|c| has_scoping_tokens(c)) {
            return Err(CliError::Config(
                "the config has a `backfill:` block but no source config references a \
                 `${backfill.start}` / `${backfill.end}` / `${now.*}` token — every window \
                 would replay identical data. Scope the source to the window (e.g. \
                 `query: SELECT * FROM t WHERE updated_at >= '${backfill.start}' AND \
                 updated_at < '${backfill.end}'`), or drop the block and use \
                 `faucet backfill --from-bookmark` instead"
                    .into(),
            ));
        }
        Ok(())
    }
}

/// Whether a serialized source config references a window-scoping token.
pub fn has_scoping_tokens(serialized_config: &str) -> bool {
    crate::interpolate::contains_unescaped(serialized_config, "${backfill.")
        || crate::interpolate::contains_unescaped(serialized_config, "${now.")
}

/// Parse an IANA timezone name.
pub fn parse_timezone(name: &str) -> CliResult<chrono_tz::Tz> {
    name.parse::<chrono_tz::Tz>().map_err(|_| {
        CliError::Config(format!(
            "'{name}' is not a valid IANA timezone (e.g. UTC, America/New_York)"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_block() {
        let yaml = "window: 1d\nconcurrency: 4\ntimezone: America/New_York\n";
        let spec: BackfillSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.window.as_deref(), Some("1d"));
        assert_eq!(spec.concurrency, Some(4));
        spec.validate(&["${backfill.start}".into()]).unwrap();
    }

    #[test]
    fn rejects_unknown_field() {
        assert!(serde_yaml::from_str::<BackfillSpec>("bogus: 1\n").is_err());
    }

    #[test]
    fn rejects_bad_window_concurrency_timezone() {
        let spec = BackfillSpec {
            window: Some("soon".into()),
            ..Default::default()
        };
        assert!(spec.validate(&[]).is_err());
        let spec = BackfillSpec {
            concurrency: Some(0),
            ..Default::default()
        };
        assert!(spec.validate(&[]).is_err());
        let spec = BackfillSpec {
            timezone: Some("Mars/Olympus".into()),
            ..Default::default()
        };
        assert!(spec.validate(&[]).is_err());
    }

    #[test]
    fn rejects_unscoped_source_with_block() {
        let spec = BackfillSpec::default();
        let err = spec
            .validate(&[r#"{"query":"SELECT * FROM t"}"#.into()])
            .unwrap_err();
        assert!(err.to_string().contains("${backfill.start}"), "{err}");
        // A `${now.*}`-scoped source passes.
        spec.validate(&[r#"{"prefix":"dt=${now.date}/"}"#.into()])
            .unwrap();
        // No sources to check (empty) passes.
        spec.validate(&[]).unwrap();
    }

    #[test]
    fn timezone_parses() {
        parse_timezone("UTC").unwrap();
        parse_timezone("America/New_York").unwrap();
        assert!(parse_timezone("Nowhere").is_err());
    }
}
