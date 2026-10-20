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
use crate::executor::ExecuteOptions;
use chrono::{DateTime, FixedOffset};
use faucet_core::UnwrappedEnvelope;
use plan::{build_replay_node, default_failed_dlq_path, validate_reason};
use reader::{DlqDecryptor, expand_location, reason_matches, replay_selects, scan_files};
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
    /// Envelopes matching the reason filter that another pipeline or row
    /// wrote, and so were not replayed (`0` with `--all-rows`).
    pub skipped_other_rows: usize,
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
    /// Replay every envelope, whichever pipeline and row wrote it. By
    /// default only the target row's own envelopes are replayed.
    pub all_rows: bool,
    pub dry_run: bool,
    pub pipeline_name: String,
    pub execution: Option<ExecutionSpec>,
    pub auth: AuthCatalog,
    pub clock: DateTime<FixedOffset>,
    /// Decryption for sealed DLQ lines. When inert, the referenced config's
    /// own dlq jsonl `encryption` block is picked up automatically.
    pub decryptor: DlqDecryptor,
    /// The caller's budget (a tenant's limits, a request's ceiling), merged
    /// with the config's own `budget:` — the stricter ceiling wins (#871).
    pub budget: Option<faucet_core::BudgetSpec>,
    /// Cooperative cancellation: the replay stops at its next page boundary
    /// and flushes (#871).
    pub cancel: Option<tokio_util::sync::CancellationToken>,
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
    let resolved = crate::partition::resolve_runtime(cfg).await?;
    let cfg = &resolved;
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
        let node = plan::select_replay_node(crate::expand::expand(cfg)?, inputs.row)?;
        DlqDecryptor::from_config_value(plan::dlq_encryption_value(node.dlq.as_ref()))?
    };

    let node = build_replay_node(
        cfg,
        files.clone(),
        reason.clone(),
        &failed,
        inputs.row,
        decryptor.clone(),
        inputs.all_rows,
    )?;

    // Count candidates up front with the same decryptor and filters the
    // reader will use.
    let target = plan::replay_target(cfg, &node, inputs.all_rows);
    let scan = scan_files(&files, &decryptor)?;
    let matching: Vec<_> = scan
        .envelopes
        .iter()
        .filter(|e| reason_matches(e, reason.as_deref()))
        .collect();
    let candidates = matching
        .iter()
        .filter(|e| replay_selects(e, None, target.as_ref()))
        .count();
    let skipped_other_rows = matching.len() - candidates;
    if skipped_other_rows > 0 {
        tracing::warn!(
            skipped = skipped_other_rows,
            row = %node.id,
            "dlq replay: skipping envelopes another pipeline or row wrote; pass --all-rows to \
             replay them through this row"
        );
    }

    // A replay runs under the config's `budget:` (merged with the caller's)
    // and `resilience:`, and stops on the caller's token (#871).
    let budget = crate::budget::effective_budget_with_kinds(
        cfg.budget.as_ref(),
        inputs.budget,
        &crate::budget::sink_template_kinds(cfg),
    )
    .map_err(CliError::Config)?;
    let resilience = match &cfg.resilience {
        Some(spec) => Some(spec.to_policy()?),
        None => None,
    };
    let summary = crate::runtime::run_local(
        vec![node],
        ExecuteOptions {
            legacy_state_writes: false,
            force_lease: false,
            pipeline_name: inputs.pipeline_name,
            run_id: None,
            execution: inputs.execution,
            concurrency: None,
            dry_run: inputs.dry_run,
            limit: None,
            state_path_override: None,
            state_scope: Default::default(),
            shard: None,
            auth: inputs.auth,
            clock: inputs.clock,
            cancel: inputs.cancel,
            resilience,
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
            usage: Default::default(),
            budget,
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
        skipped_other_rows,
        records_written,
        dry_run: inputs.dry_run,
        failed_dlq: failed.to_string_lossy().into_owned(),
    })
}

/// Discard (archive or delete) DLQ envelopes matching a reason / age filter.
///
/// Only DLQ envelopes matching the filter are discarded; blank, malformed, and
/// non-envelope lines are never touched. The DLQ file itself is never
/// rewritten — a running pipeline may be appending to it, and a rewrite would
/// lose whatever it appends meanwhile — so each discarded envelope's
/// [`reader::line_id`] is appended to a `<file>.discarded` sidecar that every
/// reader skips. By default discarded envelopes are also appended to a
/// `<file>.archived` sibling; `delete = true` only records them.
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
        let mut already = reader::discarded_lines(file)?;
        let mut ids = String::new();
        let mut removed = String::new();
        let mut file_discarded = 0usize;
        for line in text.lines() {
            let id = reader::line_id(line);
            if already.contains(&id)
                || plan::discard_keep_line(line, dec, reason.as_deref(), before_ms)
            {
                continue;
            }
            file_discarded += 1;
            ids.push_str(&id);
            ids.push('\n');
            already.insert(id);
            if !delete {
                removed.push_str(line);
                removed.push('\n');
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
        append_to(&reader::discard_sidecar(file), &ids)?;
        files_rewritten += 1;
    }

    Ok(DiscardOutcome {
        discarded,
        files_rewritten,
        archived_to,
    })
}

/// What [`purge`] did.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct PurgeOutcome {
    /// Envelope lines physically removed.
    pub purged: usize,
    /// DLQ files rewritten.
    pub files_rewritten: usize,
}

/// Physically remove every envelope a discard recorded (#862 M-024): each
/// file at `location` is rewritten without the lines in its `.discarded`
/// sidecar, atomically (temp file + rename), and the sidecar is removed. A
/// discard alone only hides lines from readers; their payloads stay on disk.
///
/// Refuses a file that changed while it was being rewritten: a writer still
/// appending to the DLQ would lose its new lines to the rename. Run it while
/// nothing writes the DLQ.
pub fn purge(location: &str) -> CliResult<PurgeOutcome> {
    purge_with(location, &DlqDecryptor::default())
}

/// [`purge`] with the keys an encrypted DLQ was sealed with: a file sealed
/// with whole-file integrity is rewritten and re-sealed under a fresh header
/// and trailer, so it still verifies for the file source and for the DLQ
/// sink's next append. Without a key such a file is refused.
pub fn purge_with(location: &str, dec: &DlqDecryptor) -> CliResult<PurgeOutcome> {
    let files = expand_location(location)?;
    let mut out = PurgeOutcome::default();
    for file in &files {
        let ids = reader::discarded_lines(file)?;
        if ids.is_empty() {
            continue;
        }
        let stamp = |f: &std::path::Path| -> CliResult<(u64, Option<std::time::SystemTime>)> {
            let m = std::fs::metadata(f).map_err(|e| {
                CliError::Internal(format!("reading DLQ file '{}': {e}", f.display()))
            })?;
            Ok((m.len(), m.modified().ok()))
        };
        let before = stamp(file)?;
        let text = std::fs::read_to_string(file).map_err(|e| {
            CliError::Internal(format!("reading DLQ file '{}': {e}", file.display()))
        })?;
        let (kept, purged) = match dec.reseal_without(file, &text, &ids)? {
            Some(rebuilt) => rebuilt,
            None => {
                let mut kept = String::with_capacity(text.len());
                let mut purged = 0usize;
                for line in text.lines() {
                    if ids.contains(&reader::line_id(line)) {
                        purged += 1;
                    } else {
                        kept.push_str(line);
                        kept.push('\n');
                    }
                }
                (kept, purged)
            }
        };
        let tmp = file.with_file_name(format!(
            "{}.faucet-purge",
            file.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("dlq.jsonl")
        ));
        std::fs::write(&tmp, &kept)
            .map_err(|e| CliError::Internal(format!("writing '{}': {e}", tmp.display())))?;
        if stamp(file)? != before {
            let _ = std::fs::remove_file(&tmp);
            return Err(CliError::Config(format!(
                "DLQ file '{}' changed while it was being purged — a pipeline is still \
                 writing to it; purge it once nothing writes the DLQ",
                file.display()
            )));
        }
        std::fs::rename(&tmp, file)
            .map_err(|e| CliError::Internal(format!("replacing '{}': {e}", file.display())))?;
        let sidecar = reader::discard_sidecar(file);
        if let Err(e) = std::fs::remove_file(&sidecar)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            return Err(CliError::Internal(format!(
                "removing '{}': {e}",
                sidecar.display()
            )));
        }
        out.purged += purged;
        out.files_rewritten += 1;
    }
    Ok(out)
}

/// The archive sibling for a DLQ file: `dlq.jsonl` → `dlq.jsonl.archived`.
/// Not a `.jsonl` name, and skipped by directory / glob locations
/// ([`reader::is_archive`]), so a later replay never re-reads what was
/// discarded.
fn archive_path(file: &std::path::Path) -> PathBuf {
    let name = file
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("dlq.jsonl");
    let parent = file.parent().unwrap_or_else(|| std::path::Path::new("."));
    parent.join(format!("{name}{}", reader::ARCHIVE_SUFFIX))
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

    /// Review of #868 (cli F8): purging an encrypted DLQ file sealed with
    /// whole-file integrity re-seals it, so it still verifies and can still be
    /// appended to; without the key, or when the file is damaged, it is refused.
    #[cfg(feature = "encryption")]
    #[test]
    fn purge_reseals_an_encrypted_dlq_file() {
        use faucet_common_file::sealed_lines::{LineSeal, Opened, open, open_for_append};
        let enc = faucet_core::CompiledEncryption::compile(&faucet_core::EncryptionSpec {
            key: "k".into(),
            previous_keys: vec![],
            algorithm: Default::default(),
        })
        .unwrap();
        let mut seal = LineSeal::new();
        let mut body = String::from_utf8(seal.header(&enc)).unwrap();
        let mut lines = Vec::new();
        for text in ["keep-1", "secret", "keep-2"] {
            let line = String::from_utf8(seal.seal(&enc, text.as_bytes())).unwrap();
            lines.push(line.trim_end().to_string());
            body.push_str(&line);
        }
        body.push_str(&String::from_utf8(seal.trailer(&enc)).unwrap());
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("dlq.jsonl");
        std::fs::write(&file, &body).unwrap();
        std::fs::write(
            reader::discard_sidecar(&file),
            format!("{}\n", reader::line_id(&lines[1])),
        )
        .unwrap();
        let loc = file.to_str().unwrap();

        let err = purge(loc).unwrap_err().to_string();
        assert!(
            err.contains("encrypted") && err.contains("--encryption-key"),
            "{err}"
        );
        assert_eq!(std::fs::read_to_string(&file).unwrap(), body, "untouched");

        let dec = DlqDecryptor::from_keys(&["k".to_string()]).unwrap();
        let out = purge_with(loc, &dec).unwrap();
        assert_eq!((out.purged, out.files_rewritten), (1, 1));
        let raw = std::fs::read(&file).unwrap();
        match open(&raw, &enc).unwrap() {
            Opened::Sealed { plain, .. } => {
                assert_eq!(String::from_utf8(plain).unwrap(), "keep-1\nkeep-2\n")
            }
            other => panic!("expected a verified sealed file, got {other:?}"),
        }
        assert!(matches!(
            open_for_append(&raw, &enc).unwrap(),
            Opened::Sealed { .. }
        ));
        assert!(!String::from_utf8_lossy(&raw).contains(&lines[1]));

        let mut damaged: Vec<&str> = body.lines().collect();
        damaged.remove(2);
        let damaged = format!("{}\n", damaged.join("\n"));
        std::fs::write(&file, &damaged).unwrap();
        std::fs::write(
            reader::discard_sidecar(&file),
            format!("{}\n", reader::line_id(&lines[0])),
        )
        .unwrap();
        let err = purge_with(loc, &dec).unwrap_err().to_string();
        assert!(err.contains("can not be purged"), "{err}");

        use base64::Engine as _;
        let unbound: Vec<String> = ["old-1", "old-2"]
            .iter()
            .map(|t| base64::engine::general_purpose::STANDARD.encode(enc.encrypt(t.as_bytes())))
            .collect();
        std::fs::write(&file, format!("{}\n{}\n", unbound[0], unbound[1])).unwrap();
        std::fs::write(
            reader::discard_sidecar(&file),
            format!("{}\n", reader::line_id(&unbound[0])),
        )
        .unwrap();
        let out = purge_with(loc, &dec).unwrap();
        assert_eq!(
            out.purged, 1,
            "a file without a trailer is filtered line by line"
        );
        assert_eq!(
            std::fs::read_to_string(&file).unwrap(),
            format!("{}\n", unbound[1])
        );
    }

    #[test]
    fn purge_removes_discarded_lines_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("dlq.jsonl");
        std::fs::write(&file, "secret-1\nkeep\nsecret-2\n").unwrap();
        let sidecar = reader::discard_sidecar(&file);
        assert_eq!(
            purge(file.to_str().unwrap()).unwrap().purged,
            0,
            "nothing discarded yet"
        );
        std::fs::write(
            &sidecar,
            format!(
                "{}\n{}\n",
                reader::line_id("secret-1"),
                reader::line_id("secret-2")
            ),
        )
        .unwrap();
        let out = purge(file.to_str().unwrap()).unwrap();
        assert_eq!((out.purged, out.files_rewritten), (2, 1));
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "keep\n");
        assert!(!sidecar.exists());
    }
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
        // The live file is untouched (#789 CLI-48); readers see only the
        // contract envelope and the non-envelope line.
        assert_eq!(std::fs::read_to_string(&path).unwrap(), body);
        let scan =
            reader::scan_files(std::slice::from_ref(&path), &DlqDecryptor::default()).unwrap();
        assert_eq!(scan.envelopes.len(), 1);
        assert_eq!(
            scan.envelopes[0].error_kind.as_deref(),
            Some("ContractViolation")
        );
        assert_eq!(scan.non_envelope, 1);
        // The archive holds the discarded envelope.
        let archived = std::fs::read_to_string(dir.path().join("dlq.jsonl.archived")).unwrap();
        assert!(archived.contains("QualityFailure"));
    }

    /// An envelope a running pipeline appends while (or after) a discard runs
    /// is kept, and a second discard never double-archives (#789 CLI-48).
    #[test]
    fn discard_never_loses_envelopes_appended_by_a_live_writer() {
        let dir = tempfile::tempdir().unwrap();
        let first = env_line("quality", "QualityFailure", 1, json!({"id": 1}));
        let path = write(dir.path(), "dlq.jsonl", &format!("{first}\n"));
        let mut live = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        discard(
            path.to_str().unwrap(),
            None,
            None,
            false,
            &DlqDecryptor::default(),
        )
        .unwrap();
        use std::io::Write;
        let late = env_line("quality", "QualityFailure", 2, json!({"id": 2}));
        writeln!(live, "{late}").unwrap();
        drop(live);
        let scan =
            reader::scan_files(std::slice::from_ref(&path), &DlqDecryptor::default()).unwrap();
        assert_eq!(scan.envelopes.len(), 1, "the late envelope survives");
        let again = discard(
            path.to_str().unwrap(),
            None,
            None,
            false,
            &DlqDecryptor::default(),
        )
        .unwrap();
        assert_eq!(again.discarded, 1, "only the late one is new");
        let archived = std::fs::read_to_string(dir.path().join("dlq.jsonl.archived")).unwrap();
        assert_eq!(archived.lines().count(), 2);
        assert!(reader::is_archive(&reader::discard_sidecar(&path)));
        assert!(
            reader::scan_files(&[path], &DlqDecryptor::default())
                .unwrap()
                .envelopes
                .is_empty()
        );
    }

    #[test]
    fn a_replay_of_the_directory_never_rereads_discarded_envelopes() {
        let dir = tempfile::tempdir().unwrap();
        let body = format!(
            "{}\n{}\n",
            env_line("quality", "QualityFailure", 1, json!({"id": 1})),
            env_line("contract", "ContractViolation", 2, json!({"id": 2})),
        );
        write(dir.path(), "dlq.jsonl", &body);
        let location = dir.path().to_str().unwrap();
        let out = discard(
            location,
            Some("quality"),
            None,
            false,
            &DlqDecryptor::default(),
        )
        .unwrap();
        assert_eq!(out.discarded, 1);
        let files = reader::expand_location(location).unwrap();
        let scan = reader::scan_files(&files, &DlqDecryptor::default()).unwrap();
        assert_eq!(scan.envelopes.len(), 1, "only the kept envelope is left");
        assert_eq!(
            scan.envelopes[0].error_kind.as_deref(),
            Some("ContractViolation")
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
        assert!(!dir.path().join("dlq.jsonl.archived").exists());
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
        let scan = reader::scan_files(&[path], &DlqDecryptor::default()).unwrap();
        assert_eq!(scan.envelopes.len(), 1);
        assert_eq!(scan.envelopes[0].ts_ms, Some(5000));
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
