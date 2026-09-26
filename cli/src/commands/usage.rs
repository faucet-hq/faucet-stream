//! `faucet usage` (#704): the cost & usage report over the invocations a
//! config's `catalog:` store has accumulated.

use crate::cli::UsageArgs;
use crate::error::{CliError, CliResult};
use crate::usage::{GroupBy, UsageFilter, aggregate, render_report};
use chrono::{DateTime, NaiveDate, Utc};

/// Parse `--since` / `--until`: RFC 3339, or a `YYYY-MM-DD` date at midnight UTC.
pub fn parse_when(s: &str) -> Result<DateTime<Utc>, String> {
    if let Ok(t) = DateTime::parse_from_rfc3339(s) {
        return Ok(t.with_timezone(&Utc));
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(d.and_hms_opt(0, 0, 0).expect("midnight").and_utc());
    }
    Err(format!(
        "`{s}` is not an RFC 3339 timestamp or a YYYY-MM-DD date"
    ))
}

/// The currency the report is rendered in: the one the records were priced
/// with (every record of one deployment shares it), else the default.
pub fn report_currency(records: &[crate::usage::UsageRecord]) -> String {
    records
        .first()
        .map(|r| r.cost.currency.clone())
        .unwrap_or_else(|| crate::usage::PricingSpec::default().currency)
}

pub async fn run(args: UsageArgs) -> CliResult<()> {
    let by = GroupBy::parse(&args.by).ok_or_else(|| {
        CliError::Config(format!(
            "--by `{}` is not one of pipeline, row, dataset, sink, day",
            args.by
        ))
    })?;
    let filter = UsageFilter {
        since: args
            .since
            .as_deref()
            .map(parse_when)
            .transpose()
            .map_err(|e| CliError::Config(format!("--since: {e}")))?,
        until: args
            .until
            .as_deref()
            .map(parse_when)
            .transpose()
            .map_err(|e| CliError::Config(format!("--until: {e}")))?,
        pipeline: args.pipeline.clone(),
        limit: args.limit,
    };
    let handle = super::catalog::connect(&args.common).await?;
    let records = handle
        .store
        .usage_list(&filter)
        .await
        .map_err(|e| CliError::Internal(format!("usage read: {e}")))?;
    let report = aggregate(&records, by, &report_currency(&records));
    if args.common.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report)
                .map_err(|e| CliError::Internal(format!("rendering usage JSON: {e}")))?
        );
    } else {
        print!("{}", render_report(&report));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn when_accepts_rfc3339_and_dates() {
        assert_eq!(
            parse_when("2026-09-01").unwrap().to_rfc3339(),
            "2026-09-01T00:00:00+00:00"
        );
        assert_eq!(
            parse_when("2026-09-01T10:00:00+02:00")
                .unwrap()
                .to_rfc3339(),
            "2026-09-01T08:00:00+00:00"
        );
        assert!(parse_when("yesterday").unwrap_err().contains("yesterday"));
        assert_eq!(report_currency(&[]), "USD");
    }
}
