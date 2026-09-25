//! `faucet dlq` — inspect, replay, and discard dead-letter-queue envelopes.
//!
//! The DLQ subsystem writes a fixed-shape envelope (`faucet_core::dlq`) for
//! every quarantined row. This module closes the loop: read those envelopes
//! back, group them by why they failed, re-feed the original payloads through
//! the referenced pipeline (transforms → quality → contract → sink), and
//! archive/delete what's been handled.
//!
//! Orchestration only — it produces serializable result structs and does IO,
//! but never prints. The CLI command layer ([`crate::commands::dlq`]) renders
//! them for the terminal; `faucet serve` renders them as JSON.

pub mod plan;
pub mod reader;

use crate::auth_catalog::AuthCatalog;
use crate::config::{ExecutionSpec, PipelineConfig};
use crate::error::{CliError, CliResult};
use crate::executor::{ExecuteOptions, run_expanded};
use chrono::{DateTime, FixedOffset};
use faucet_core::UnwrappedEnvelope;
use plan::{build_replay_node, default_failed_dlq_path, validate_reason};
use reader::{DlqDecryptor, expand_location, reason_matches, scan_files};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// A compact, serializable view of one envelope for the `inspect` sample.
#[derive(Debug, Clone, Serialize)]
pub struct EnvelopeSummary {
    pub reason: Option<String>,
    pub error_kind: Option<String>,
    pub error_message: Option<String>,
    pub pipeline: Option<String>,
    pub row: Option<String>,
    pub record_index: Option<u64>,
    pub payload: Value,
}

impl From<&UnwrappedEnvelope> for EnvelopeSummary {
    fn from(e: &UnwrappedEnvelope) -> Self {
        Self {
            reason: e.reason.clone(),
            error_kind: e.error_kind.clone(),
            error_message: e.error_message.clone(),
            pipeline: e.pipeline.clone(),
            row: e.row.clone(),
            record_index: e.record_index,
            payload: e.payload.clone(),
        }
    }
}

/// Grouped summary of a DLQ location, produced by [`inspect`].
#[derive(Debug, Clone, Serialize)]
pub struct InspectSummary {
    pub location: String,
    pub files_read: usize,
    /// Envelopes matching the reason filter (all envelopes when no filter).
    pub total_envelopes: usize,
    /// Non-blank lines that were not valid JSON.
    pub malformed: usize,
    /// Valid-JSON lines that were not DLQ envelopes.
    pub non_envelope: usize,
    /// Sealed (encrypted) lines that could not be decrypted with the
    /// available keys — pass `--encryption-key` to read them.
    pub undecryptable: usize,
    pub by_reason: BTreeMap<String, usize>,
    pub by_error_kind: BTreeMap<String, usize>,
    pub sample: Vec<EnvelopeSummary>,
}

/// Outcome of a [`replay`] run.
#[derive(Debug, Clone, Serialize)]
pub struct ReplayOutcome {
    /// Envelopes that matched the reason filter and would be fed to the pipeline.
    pub candidates: usize,
    /// Records the sink accepted (survivors of transforms/quality/contract).
    /// `0` extra sink writes under `--dry-run` (the sink is a no-op counter).
    pub records_written: usize,
    pub dry_run: bool,
    /// Where replayed rows that fail *again* are quarantined.
    pub failed_dlq: String,
}

/// Outcome of a [`discard`] run.
#[derive(Debug, Clone, Serialize)]
pub struct DiscardOutcome {
    pub discarded: usize,
    pub files_rewritten: usize,
    /// Archive files written (empty when `--delete` was passed).
    pub archived_to: Vec<String>,
}

/// Read a DLQ location back and group its envelopes by reason and error kind,
/// with a bounded sample. `reason` restricts the included envelopes; malformed
/// and non-envelope line counts always reflect the whole scan.
pub fn inspect(
    location: &str,
    reason: Option<&str>,
    sample_limit: usize,
    dec: &DlqDecryptor,
) -> CliResult<InspectSummary> {
    let reason = validate_reason(reason)?;
    let files = expand_location(location)?;
    let scan = scan_files(&files, dec)?;
    let envs: Vec<UnwrappedEnvelope> = scan
        .envelopes
        .into_iter()
        .filter(|e| reason_matches(e, reason.as_deref()))
        .collect();

    let mut by_reason: BTreeMap<String, usize> = BTreeMap::new();
    let mut by_error_kind: BTreeMap<String, usize> = BTreeMap::new();
    for e in &envs {
        *by_reason
            .entry(e.reason.clone().unwrap_or_else(|| "unknown".into()))
            .or_default() += 1;
        *by_error_kind
            .entry(e.error_kind.clone().unwrap_or_else(|| "unknown".into()))
            .or_default() += 1;
    }
    let sample = envs
        .iter()
        .take(sample_limit)
        .map(EnvelopeSummary::from)
        .collect();

    Ok(InspectSummary {
        location: location.to_string(),
        files_read: scan.files_read,
        total_envelopes: envs.len(),
        malformed: scan.malformed,
        non_envelope: scan.non_envelope,
        undecryptable: scan.undecryptable,
        by_reason,
        by_error_kind,
        sample,
    })
}

/// Inputs for a [`replay`] run beyond the config + location.
pub struct ReplayInputs<'a> {
    pub reason: Option<&'a str>,
    /// Explicit fresh-DLQ location for failures; defaults to a sibling of the
    /// source when `None`.
    pub failed_dlq: Option<&'a str>,
    /// Which root to replay (`None` = the first root).
    pub row: Option<&'a str>,
    pub dry_run: bool,
    pub pipeline_name: String,
    pub execution: Option<ExecutionSpec>,
    pub auth: AuthCatalog,
    pub clock: DateTime<FixedOffset>,
    /// Decryption for sealed DLQ lines. When inert, the referenced config's
    /// own dlq jsonl `encryption` block is picked up automatically.
    pub decryptor: DlqDecryptor,
}

/// Reconstruct a pipeline whose source is the DLQ location (envelopes →
/// unwrapped payloads) and whose sink/transforms/quality/contract come from
/// `cfg`, then run it through the normal executor path. Replayed rows that
/// fail again land in a *fresh* DLQ so replay can never re-feed itself.
pub async fn replay(
    cfg: &PipelineConfig,
    location: &str,
    inputs: ReplayInputs<'_>,
) -> CliResult<ReplayOutcome> {
    let reason = validate_reason(inputs.reason)?;
    let files = expand_location(location)?;
    let failed = inputs
        .failed_dlq
        .map(PathBuf::from)
        .unwrap_or_else(|| default_failed_dlq_path(&files));

    // Resolve the effective decryptor once: explicit keys win; otherwise the
    // `encryption` block of the config's own dlq jsonl sink applies (the file
    // being replayed was almost always written by exactly that sink).
    let decryptor = if inputs.decryptor.is_active() {
        inputs.decryptor.clone()
    } else {
        let nodes = crate::expand::expand(cfg)?;
        let original_dlq = nodes
            .iter()
            .find(|n| matches!(n.role, crate::expand::NodeRole::Root))
            .and_then(|n| n.dlq.as_ref());
        DlqDecryptor::from_config_value(plan::dlq_encryption_value(original_dlq))?
    };

    let node = build_replay_node(
        cfg,
        files.clone(),
        reason.clone(),
        &failed,
        inputs.row,
        decryptor.clone(),
    )?;

    // Count candidates up front with the same decryptor the reader will use.
    let scan = scan_files(&files, &decryptor)?;
    let candidates = scan
        .envelopes
        .iter()
        .filter(|e| reason_matches(e, reason.as_deref()))
        .count();

    let summary = run_expanded(
        vec![node],
        ExecuteOptions {
            pipeline_name: inputs.pipeline_name,
            run_id: None,
            execution: inputs.execution,
            concurrency: None,
            dry_run: inputs.dry_run,
            limit: None,
            state_path_override: None,
            shard: None,
            auth: inputs.auth,
            clock: inputs.clock,
            cancel: None,
            resilience: None,
            sla: None,
            reconcile: None,
            verify: None,
            rollback: None,
            #[cfg(feature = "lineage")]
            lineage: None,
            #[cfg(feature = "lineage")]
            lineage_cfg: None,
            #[cfg(feature = "notify")]
            notifier: None,
            // A replay is a repair action over quarantined rows, not an
            // observation of the original source — recording it would
            // attribute the DLQ reader's rows to the pipeline's source
            // dataset. Deliberately not catalogued.
            #[cfg(feature = "catalog")]
            catalog: None,
        },
    )
    .await?;

    if summary.had_failures() {
        let detail = summary
            .invocations
            .iter()
            .find_map(|i| i.error.clone())
            .unwrap_or_else(|| "unknown error".to_string());
        return Err(CliError::Internal(format!("dlq replay failed: {detail}")));
    }
    let records_written = summary.invocations.iter().map(|i| i.records_written).sum();

    Ok(ReplayOutcome {
        candidates,
        records_written,
        dry_run: inputs.dry_run,
        failed_dlq: failed.to_string_lossy().into_owned(),
    })
}

/// Discard (archive or delete) DLQ envelopes matching a reason / age filter.
///
/// Only DLQ envelopes matching the filter are removed; blank, malformed, and
/// non-envelope lines are preserved verbatim. By default discarded envelopes
/// are appended to a `<file>.archived.jsonl` sibling before being removed from
/// the source; `delete = true` removes them without archiving. A file is
/// rewritten only when it actually lost lines.
pub fn discard(
    location: &str,
    reason: Option<&str>,
    before_ms: Option<i64>,
    delete: bool,
    dec: &DlqDecryptor,
) -> CliResult<DiscardOutcome> {
    let reason = validate_reason(reason)?;
    let files = expand_location(location)?;

    let mut discarded = 0usize;
    let mut files_rewritten = 0usize;
    let mut archived_to = Vec::new();

    for file in &files {
        let text = std::fs::read_to_string(file).map_err(|e| {
            CliError::Internal(format!("reading DLQ file '{}': {e}", file.display()))
        })?;
        let mut kept = String::new();
        let mut removed = String::new();
        let mut file_discarded = 0usize;
        for line in text.lines() {
            if plan::discard_keep_line(line, dec, reason.as_deref(), before_ms) {
                kept.push_str(line);
                kept.push('\n');
            } else {
                file_discarded += 1;
                if !delete {
                    removed.push_str(line);
                    removed.push('\n');
                }
            }
        }
        if file_discarded == 0 {
            continue;
        }
        discarded += file_discarded;
        if !delete && !removed.is_empty() {
            let archive = archive_path(file);
            append_to(&archive, &removed)?;
            archived_to.push(archive.to_string_lossy().into_owned());
        }
        atomic_rewrite(file, kept.as_bytes())?;
        files_rewritten += 1;
    }

    Ok(DiscardOutcome {
        discarded,
        files_rewritten,
        archived_to,
    })
}

/// Rewrite `file` atomically: write the surviving lines to a temp file in the
/// same directory, then `rename` it over the original. A process kill mid-write
/// leaves the original intact (the incomplete write lands only in the temp
/// file), so the un-discarded kept envelopes can never be lost to a truncated
/// prefix — unlike a direct `fs::write` truncate-then-write (audit #321 L3).
fn atomic_rewrite(file: &std::path::Path, contents: &[u8]) -> CliResult<()> {
    let parent = file.parent().unwrap_or_else(|| std::path::Path::new("."));
    let name = file
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("dlq.jsonl");
    // Same-directory temp so the rename is atomic (same filesystem). The pid
    // keeps concurrent discards on the same file from colliding on the temp name.
    let tmp = parent.join(format!(".{name}.{}.tmp", std::process::id()));
    std::fs::write(&tmp, contents).map_err(|e| {
        CliError::Internal(format!("writing temp DLQ file '{}': {e}", tmp.display()))
    })?;
    std::fs::rename(&tmp, file).map_err(|e| {
        // Best-effort cleanup of the temp file if the rename failed.
        let _ = std::fs::remove_file(&tmp);
        CliError::Internal(format!("rewriting DLQ file '{}': {e}", file.display()))
    })?;
    Ok(())
}

/// The archive sibling for a DLQ file: `dlq.jsonl` → `dlq.archived.jsonl`.
fn archive_path(file: &std::path::Path) -> PathBuf {
    let stem = file.file_stem().and_then(|s| s.to_str()).unwrap_or("dlq");
    let parent = file.parent().unwrap_or_else(|| std::path::Path::new("."));
    parent.join(format!("{stem}.archived.jsonl"))
}

/// Append `body` to `path`, creating it if absent.
fn append_to(path: &std::path::Path, body: &str) -> CliResult<()> {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| CliError::Internal(format!("opening archive '{}': {e}", path.display())))?;
    f.write_all(body.as_bytes())
        .map_err(|e| CliError::Internal(format!("writing archive '{}': {e}", path.display())))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Write;

    fn env_line(reason: &str, kind: &str, ts_ms: i64, payload: Value) -> String {
        json!({
            "error": { "kind": kind, "message": "boom" },
            "reason": reason,
            "payload": payload,
            "ts_ms": ts_ms,
            "sink": "pg", "pipeline": "etl", "row": "", "record_index": 0,
        })
        .to_string()
    }

    fn write(dir: &std::path::Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        f.flush().unwrap();
        path
    }

    #[test]
    fn inspect_groups_by_reason_and_kind() {
        let dir = tempfile::tempdir().unwrap();
        let body = format!(
            "{}\n{}\n{}\nnot json\n{{\"a\":1}}\n",
            env_line("quality", "QualityFailure", 1, json!({"id": 1})),
            env_line("quality", "QualityFailure", 2, json!({"id": 2})),
            env_line("contract", "ContractViolation", 3, json!({"id": 3})),
        );
        let path = write(dir.path(), "dlq.jsonl", &body);
        let s = inspect(path.to_str().unwrap(), None, 10, &DlqDecryptor::default()).unwrap();
        assert_eq!(s.total_envelopes, 3);
        assert_eq!(s.malformed, 1);
        assert_eq!(s.non_envelope, 1);
        assert_eq!(s.by_reason.get("quality"), Some(&2));
        assert_eq!(s.by_reason.get("contract"), Some(&1));
        assert_eq!(s.by_error_kind.get("QualityFailure"), Some(&2));
        assert_eq!(s.sample.len(), 3);
    }

    #[test]
    fn inspect_reason_filter_and_sample_limit() {
        let dir = tempfile::tempdir().unwrap();
        let body = format!(
            "{}\n{}\n{}\n",
            env_line("quality", "QualityFailure", 1, json!({"id": 1})),
            env_line("quality", "QualityFailure", 2, json!({"id": 2})),
            env_line("contract", "ContractViolation", 3, json!({"id": 3})),
        );
        let path = write(dir.path(), "dlq.jsonl", &body);
        let s = inspect(
            path.to_str().unwrap(),
            Some("quality"),
            1,
            &DlqDecryptor::default(),
        )
        .unwrap();
        assert_eq!(s.total_envelopes, 2);
        assert_eq!(s.by_reason.len(), 1);
        assert_eq!(s.sample.len(), 1);
    }

    #[test]
    fn discard_archives_matching_and_keeps_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let body = format!(
            "{}\n{}\n{{\"other\":1}}\n",
            env_line("quality", "QualityFailure", 1, json!({"id": 1})),
            env_line("contract", "ContractViolation", 2, json!({"id": 2})),
        );
        let path = write(dir.path(), "dlq.jsonl", &body);
        let out = discard(
            path.to_str().unwrap(),
            Some("quality"),
            None,
            false,
            &DlqDecryptor::default(),
        )
        .unwrap();
        assert_eq!(out.discarded, 1);
        assert_eq!(out.files_rewritten, 1);
        assert_eq!(out.archived_to.len(), 1);
        // The quality envelope is gone; the contract one and the non-envelope
        // line remain.
        let remaining = std::fs::read_to_string(&path).unwrap();
        assert!(!remaining.contains("\"id\":1") && !remaining.contains("\"id\": 1"));
        assert!(remaining.contains("ContractViolation"));
        assert!(remaining.contains("other"));
        // The archive holds the discarded envelope.
        let archived = std::fs::read_to_string(dir.path().join("dlq.archived.jsonl")).unwrap();
        assert!(archived.contains("QualityFailure"));
        // #321 L3: the atomic rewrite leaves no lingering temp file behind.
        let leftover: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(
            leftover.is_empty(),
            "no .tmp file should remain: {leftover:?}"
        );
    }

    #[test]
    fn discard_delete_does_not_archive() {
        let dir = tempfile::tempdir().unwrap();
        let body = format!(
            "{}\n",
            env_line("quality", "QualityFailure", 1, json!({"id": 1}))
        );
        let path = write(dir.path(), "dlq.jsonl", &body);
        let out = discard(
            path.to_str().unwrap(),
            None,
            None,
            true,
            &DlqDecryptor::default(),
        )
        .unwrap();
        assert_eq!(out.discarded, 1);
        assert!(out.archived_to.is_empty());
        assert!(!dir.path().join("dlq.archived.jsonl").exists());
    }

    #[test]
    fn discard_before_filter_only_removes_older() {
        let dir = tempfile::tempdir().unwrap();
        let body = format!(
            "{}\n{}\n",
            env_line("quality", "QualityFailure", 100, json!({"id": 1})),
            env_line("quality", "QualityFailure", 5000, json!({"id": 2})),
        );
        let path = write(dir.path(), "dlq.jsonl", &body);
        let out = discard(
            path.to_str().unwrap(),
            None,
            Some(1000),
            true,
            &DlqDecryptor::default(),
        )
        .unwrap();
        assert_eq!(out.discarded, 1);
        let remaining = std::fs::read_to_string(&path).unwrap();
        assert!(remaining.contains("\"id\":2") || remaining.contains("\"id\": 2"));
    }

    #[test]
    fn discard_no_match_leaves_file_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let body = format!(
            "{}\n",
            env_line("quality", "QualityFailure", 1, json!({"id": 1}))
        );
        let path = write(dir.path(), "dlq.jsonl", &body);
        let before = std::fs::read_to_string(&path).unwrap();
        let out = discard(
            path.to_str().unwrap(),
            Some("contract"),
            None,
            false,
            &DlqDecryptor::default(),
        )
        .unwrap();
        assert_eq!(out.discarded, 0);
        assert_eq!(out.files_rewritten, 0);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
    }
}
