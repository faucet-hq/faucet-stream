//! `faucet dlq` — inspect / replay / discard dead-letter-queue envelopes.
//!
//! Thin command layer over [`crate::dlq_replay`]: it loads config (for
//! `replay`), calls the orchestration functions, and renders the returned
//! summaries for the terminal (or as JSON with `--json`).

use crate::cli::{DlqArgs, DlqCommand, DlqDiscardArgs, DlqInspectArgs, DlqReplayArgs};
use crate::config::PipelineConfig;
use crate::dlq_replay::{self, ReplayInputs, reader::DlqDecryptor};
use crate::error::{CliError, CliResult};
use chrono::{DateTime, Utc};

/// Serialize a result struct to pretty JSON, mapping the (practically
/// impossible) serialization error to a typed CLI error.
fn to_json<T: serde::Serialize>(value: &T) -> CliResult<String> {
    serde_json::to_string_pretty(value)
        .map_err(|e| CliError::Internal(format!("serializing JSON output: {e}")))
}

/// The DLQ keys from `--encryption-key` / `FAUCET_DLQ_ENCRYPTION_KEY` and
/// `--encryption-key-file`, each registered for log redaction.
fn dlq_keys(keys: &[String], files: &[std::path::PathBuf]) -> CliResult<Vec<String>> {
    let mut out = keys.to_vec();
    for path in files {
        let text = std::fs::read_to_string(path).map_err(|e| {
            CliError::Config(format!("reading --encryption-key-file {}: {e}", path.display()))
        })?;
        out.push(text.trim_end().to_string());
    }
    for key in &out {
        crate::secrets::registry::register(key);
    }
    Ok(out)
}

fn decryptor(keys: &[String], files: &[std::path::PathBuf]) -> CliResult<DlqDecryptor> {
    Ok(DlqDecryptor::from_keys(&dlq_keys(keys, files)?)?)
}

/// Dispatch a `faucet dlq` subcommand.
pub async fn run(args: DlqArgs) -> CliResult<()> {
    match args.command {
        DlqCommand::Inspect(a) => inspect(a),
        DlqCommand::Replay(a) => replay(a).await,
        DlqCommand::Discard(a) => discard(a),
    }
}

fn inspect(args: DlqInspectArgs) -> CliResult<()> {
    let dec = decryptor(&args.encryption_key, &args.encryption_key_file)?;
    let summary = dlq_replay::inspect(&args.location, args.reason.as_deref(), args.limit, &dec)?;
    if args.json {
        println!("{}", to_json(&summary)?);
        return Ok(());
    }
    println!("DLQ inspect: {}", summary.location);
    println!(
        "  files read: {}   envelopes: {}   malformed: {}   non-envelope: {}",
        summary.files_read, summary.total_envelopes, summary.malformed, summary.non_envelope
    );
    if summary.undecryptable > 0 {
        println!(
            "  encrypted lines that could not be decrypted: {} (pass --encryption-key)",
            summary.undecryptable
        );
    }
    if !summary.by_reason.is_empty() {
        println!("  by reason:");
        for (reason, count) in &summary.by_reason {
            println!("    {reason:<14} {count}");
        }
    }
    if !summary.by_error_kind.is_empty() {
        println!("  by error kind:");
        for (kind, count) in &summary.by_error_kind {
            println!("    {kind:<20} {count}");
        }
    }
    if !summary.sample.is_empty() {
        println!(
            "  sample ({} of {}):",
            summary.sample.len(),
            summary.total_envelopes
        );
        for env in &summary.sample {
            let reason = env.reason.as_deref().unwrap_or("?");
            let kind = env.error_kind.as_deref().unwrap_or("?");
            let msg = env.error_message.as_deref().unwrap_or("");
            println!("    [{reason}/{kind}] {msg}");
            println!("      {}", env.payload);
        }
    }
    Ok(())
}

async fn replay(args: DlqReplayArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };

    let cfg = PipelineConfig::from_path_async(&path, args.profile.as_deref()).await?;
    crate::obs::install(&cfg)?;

    let pipeline_name = cfg.name.clone().unwrap_or_else(|| {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("pipeline")
            .to_owned()
    });
    let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;

    let outcome = dlq_replay::replay(
        &cfg,
        &args.from,
        ReplayInputs {
            reason: args.reason.as_deref(),
            failed_dlq: args.failed_dlq.as_deref(),
            row: args.row.as_deref(),
            dry_run: args.dry_run,
            pipeline_name,
            execution: cfg.execution.clone(),
            auth,
            clock: Utc::now().fixed_offset(),
            decryptor: decryptor(&args.encryption_key, &args.encryption_key_file)?,
        },
    )
    .await?;

    faucet_core::shutdown_otel();

    if args.json {
        println!("{}", to_json(&outcome)?);
        return Ok(());
    }
    if outcome.dry_run {
        println!(
            "DLQ replay (dry-run): {} candidate record(s) from {} would be re-fed; \
             {} would reach the sink. Failures would go to {}.",
            outcome.candidates, args.from, outcome.records_written, outcome.failed_dlq
        );
    } else {
        println!(
            "DLQ replay: {} candidate record(s) from {} re-fed; {} written to the sink. \
             Rows that failed again went to {}.",
            outcome.candidates, args.from, outcome.records_written, outcome.failed_dlq
        );
    }
    Ok(())
}

fn discard(args: DlqDiscardArgs) -> CliResult<()> {
    let before_ms = match args.before.as_deref() {
        Some(s) => Some(parse_before(s, Utc::now())?),
        None => None,
    };
    let dec = decryptor(&args.encryption_key, &args.encryption_key_file)?;
    let outcome = dlq_replay::discard(
        &args.location,
        args.reason.as_deref(),
        before_ms,
        args.delete,
        &dec,
    )?;
    if args.json {
        println!("{}", to_json(&outcome)?);
        return Ok(());
    }
    if args.delete {
        println!(
            "DLQ discard: deleted {} envelope(s) across {} file(s).",
            outcome.discarded, outcome.files_rewritten
        );
    } else {
        println!(
            "DLQ discard: archived {} envelope(s) across {} file(s){}.",
            outcome.discarded,
            outcome.files_rewritten,
            if outcome.archived_to.is_empty() {
                String::new()
            } else {
                format!(" → {}", outcome.archived_to.join(", "))
            }
        );
    }
    Ok(())
}

/// Parse a `--before` value into an epoch-millis cutoff: an RFC3339 timestamp
/// (`2026-06-01T00:00:00Z`), or a relative age (`7d` / `24h` / `30m` / `45s`)
/// subtracted from `now`.
fn parse_before(s: &str, now: DateTime<Utc>) -> CliResult<i64> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp_millis());
    }
    if let Some(cutoff) = parse_relative_age(s, now) {
        return Ok(cutoff);
    }
    Err(CliError::Config(format!(
        "--before '{s}' is not an RFC3339 timestamp or a relative age like 7d / 24h / 30m / 45s"
    )))
}

/// Parse a relative age like `7d` into an epoch-millis cutoff (`now - age`).
/// Returns `None` if `s` is not `<positive-int><d|h|m|s>`.
fn parse_relative_age(s: &str, now: DateTime<Utc>) -> Option<i64> {
    let (num, unit) = s.split_at(s.len().checked_sub(1)?);
    let n: i64 = num.parse().ok()?;
    if n <= 0 {
        return None;
    }
    let secs = match unit {
        "d" => n.checked_mul(86_400)?,
        "h" => n.checked_mul(3_600)?,
        "m" => n.checked_mul(60)?,
        "s" => n,
        _ => return None,
    };
    Some((now - chrono::Duration::seconds(secs)).timestamp_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-07-06T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn parse_before_rfc3339() {
        let ms = parse_before("2026-06-01T00:00:00Z", now()).unwrap();
        assert_eq!(
            ms,
            DateTime::parse_from_rfc3339("2026-06-01T00:00:00Z")
                .unwrap()
                .timestamp_millis()
        );
    }

    #[test]
    fn parse_before_relative_ages() {
        let now = now();
        assert_eq!(
            parse_before("1d", now).unwrap(),
            (now - chrono::Duration::seconds(86_400)).timestamp_millis()
        );
        assert_eq!(
            parse_before("2h", now).unwrap(),
            (now - chrono::Duration::seconds(7_200)).timestamp_millis()
        );
        assert_eq!(
            parse_before("30m", now).unwrap(),
            (now - chrono::Duration::seconds(1_800)).timestamp_millis()
        );
    }

    #[test]
    fn parse_before_rejects_garbage() {
        assert!(parse_before("soon", now()).is_err());
        assert!(parse_before("0d", now()).is_err());
        assert!(parse_before("-5d", now()).is_err());
        assert!(parse_before("7y", now()).is_err());
    }
}
