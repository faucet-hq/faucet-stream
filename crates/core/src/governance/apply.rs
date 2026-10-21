//! The per-page governance passes (masking → quality → contract → schema
//! drift) and the governed DLQ writer. They live beside [`GovernedPage`] so
//! that the only code able to mint one is the code that ran the passes
//! (RFC 0014).

use super::page::{GovernedEnvelopes, GovernedPage};
use crate::dlq::DlqStats;
use crate::error::FaucetError;
use crate::traits::Sink;
use serde_json::Value;
#[cfg(any(feature = "masking", feature = "quality", feature = "contract"))]
use std::sync::Arc;

/// Compiled governance specs for one run, borrowed per page.
///
/// Bundled into a struct rather than passed as loose arguments because the set
/// is feature-gated: a `#[cfg]` on a function parameter is not expressible, and
/// threading five `Option`s through two call sites invites them being passed in
/// the wrong order.
pub(crate) struct GovernanceSpecs<'a> {
    #[cfg(feature = "masking")]
    pub masking: Option<&'a Arc<crate::masking::CompiledMasking>>,
    #[cfg(feature = "quality")]
    pub quality: Option<&'a Arc<crate::quality::CompiledQuality>>,
    #[cfg(feature = "contract")]
    pub contract: Option<&'a Arc<crate::contract::CompiledContract>>,
    pub schema_drift: Option<&'a crate::drift::SchemaDriftPolicy>,
    pub pipeline: &'a str,
    pub row: &'a str,
    /// Only read by the masking/quality/contract label builders, so it is
    /// dead under a build with none of those features.
    #[cfg_attr(
        not(any(feature = "masking", feature = "quality", feature = "contract")),
        allow(dead_code)
    )]
    pub run_id: &'a str,
    pub sink_name: &'a str,
    /// Whether a DLQ is configured. A drift/incompatible `fail` is **deferred**
    /// when it is, so this page's already-built quarantine envelopes still
    /// reach the DLQ before the run stops (#146 M4); the page's survivors and
    /// bookmark are never written (CORE-20).
    pub has_dlq: bool,
}

impl GovernanceSpecs<'_> {
    /// Proof that no record pass is configured, so a page is governed without
    /// being materialized: the columnar and native paths hand it through.
    pub(crate) fn clearance(&self) -> Option<super::page::Clearance> {
        #[allow(unused_mut)]
        let mut any = self.schema_drift.is_some();
        #[cfg(feature = "masking")]
        {
            any |= self.masking.is_some();
        }
        #[cfg(feature = "quality")]
        {
            any |= self.quality.is_some();
        }
        #[cfg(feature = "contract")]
        {
            any |= self.contract.is_some();
        }
        (!any).then_some(super::page::Clearance(()))
    }
}

/// State the governance passes carry **across** pages of one run: the lazily
/// fetched destination schema and the one-shot warning latches.
#[derive(Default)]
pub(crate) struct GovernanceState {
    pub dest_schema_cache: Option<Option<Value>>,
    pub warned_drift_inert: bool,
    #[cfg(feature = "contract")]
    pub warned_contract_breach: bool,
}

/// What one page's governance passes produced.
pub(crate) struct GovernanceOutcome {
    /// Records that survived every pass and should be written.
    pub records: GovernedPage<'static>,
    /// DLQ envelopes from quality + contract + drift quarantine, merged in that
    /// order so the caller writes them as one batch.
    pub envelopes: GovernedEnvelopes,
    /// A drift `fail` / incompatible-`fail` abort to raise once the envelopes
    /// are durable, without writing `records` or the page's bookmark. `None`
    /// when there is nothing to defer; with no DLQ configured the error is
    /// returned directly instead.
    pub deferred_abort: Option<FaucetError>,
}

/// Run the per-page governance passes in their load-bearing order —
/// **masking → quality → contract → schema drift** — and return the survivors
/// plus any DLQ envelopes.
///
/// Extracted from `run_stream` so the columnar loop runs the *same* code rather
/// than a second copy (#636). The ordering and the page-index bookkeeping are
/// the subtle parts and must not be duplicated: masking runs first so PII never
/// reaches a sink, the DLQ, or a lineage sample; quality and contract each
/// remove rows, and `page_indices` is carried in lockstep so a later drift
/// quarantine annotates an envelope with the record's **true** page position
/// rather than a survivor-relative one (#321 L6, #466 M1).
pub(crate) async fn apply_governance<Si: Sink + ?Sized>(
    records: Vec<Value>,
    specs: &GovernanceSpecs<'_>,
    state: &mut GovernanceState,
    sink: &Si,
) -> Result<GovernanceOutcome, FaucetError> {
    // Envelope construction is entirely feature-gated (quality/contract), so
    // this import is too — otherwise a bare `arrow` build warns.
    #[cfg(any(feature = "quality", feature = "contract"))]
    use crate::dlq::{DlqReason, build_envelope};

    let pipeline_name = specs.pipeline;
    let row = specs.row;
    let sink_name = specs.sink_name;

    // ── Masking pass (FIRST — before quality/contract/drift and every sink
    // write) ────────────────────────────────────────────────────────────────
    // Runs ahead of everything so PII never reaches a sink, the DLQ (quarantine
    // envelopes are built downstream from these already-masked records), or the
    // sink-side lineage sample.
    #[cfg(feature = "masking")]
    let records = if let Some(m) = specs.masking {
        let labels = crate::observability::Labels::new(pipeline_name, row, specs.run_id);
        crate::observability::instrumented_apply_masking(records, m, &labels).records
    } else {
        records
    };

    // True page positions of the records currently flowing, kept in lockstep as
    // quality/contract remove rows.
    let page_len = records.len();

    // ── Quality pass ────────────────────────────────────────────────────────
    #[cfg(feature = "quality")]
    let (records, quality_envelopes, page_indices): (Vec<Value>, Vec<Value>, Vec<usize>) =
        if let Some(q) = specs.quality {
            let labels = crate::observability::Labels::new(pipeline_name, row, specs.run_id);
            let outcome = crate::observability::instrumented_apply_quality(records, q, &labels)?;
            let quarantined_idx: std::collections::HashSet<usize> =
                outcome.quarantined.iter().map(|qr| qr.page_index).collect();
            let envelopes: Vec<Value> = outcome
                .quarantined
                .iter()
                .map(|qr| {
                    let err = FaucetError::QualityFailure {
                        check: qr.check.to_string(),
                        message: qr.message.clone(),
                    };
                    // `record_index` is the position within the PAGE (the frozen
                    // envelope contract), not the index in the quarantine list.
                    build_envelope(
                        &qr.record,
                        &err,
                        DlqReason::Quality,
                        sink_name,
                        pipeline_name,
                        row,
                        qr.page_index,
                    )
                })
                .collect();
            let survivor_idx: Vec<usize> = (0..page_len)
                .filter(|i| !quarantined_idx.contains(i))
                .collect();
            (outcome.survivors, envelopes, survivor_idx)
        } else {
            (records, Vec::new(), (0..page_len).collect())
        };
    #[cfg(not(feature = "quality"))]
    let (records, quality_envelopes, page_indices): (Vec<Value>, Vec<Value>, Vec<usize>) =
        (records, Vec::new(), (0..page_len).collect());

    // ── Contract pass ───────────────────────────────────────────────────────
    // `fail` mirrors a quality `abort`: the breach error propagates immediately
    // and nothing from this page is written.
    #[cfg(feature = "contract")]
    let (records, contract_envelopes, page_indices): (Vec<Value>, Vec<Value>, Vec<usize>) =
        if let Some(c) = specs.contract {
            let labels = crate::observability::Labels::new(pipeline_name, row, specs.run_id);
            let outcome = crate::observability::instrumented_apply_contract(records, c, &labels)?;
            if !outcome.warned.is_empty() && !state.warned_contract_breach {
                tracing::warn!(
                    version = %c.version,
                    breaches = outcome.warned.len(),
                    first = %outcome.warned[0].describe(),
                    "contract: breaching records written unchanged (on_breach=warn); \
                     this warning fires once per run"
                );
                state.warned_contract_breach = true;
            }
            let envelopes: Vec<Value> = outcome
                .quarantined
                .iter()
                .map(|vr| {
                    let err = FaucetError::ContractViolation {
                        version: c.version.clone(),
                        message: vr.violation.describe(),
                    };
                    // `vr.violation.page_index` is the position within *this
                    // pass's input* — the quality survivors — which is aligned
                    // with `page_indices`, so translate through it (#466 M1).
                    let page_index = page_indices
                        .get(vr.violation.page_index)
                        .copied()
                        .unwrap_or(vr.violation.page_index);
                    build_envelope(
                        &vr.record,
                        &err,
                        DlqReason::Contract,
                        sink_name,
                        pipeline_name,
                        row,
                        page_index,
                    )
                })
                .collect();
            let contract_quarantined: std::collections::HashSet<usize> = outcome
                .quarantined
                .iter()
                .map(|vr| vr.violation.page_index)
                .collect();
            let survivor_idx: Vec<usize> = page_indices
                .iter()
                .enumerate()
                .filter(|(pos, _)| !contract_quarantined.contains(pos))
                .map(|(_, orig)| *orig)
                .collect();
            (outcome.survivors, envelopes, survivor_idx)
        } else {
            (records, Vec::new(), page_indices)
        };

    // ── Schema-drift pass ───────────────────────────────────────────────────
    let mut drift_envelopes: Vec<Value> = Vec::new();
    let (records, drift_abort): (Vec<Value>, Option<FaucetError>) =
        if let Some(policy) = specs.schema_drift.filter(|_| !records.is_empty()) {
            // Re-ask while the sink reports none: a target the sink creates on
            // the first page has a schema from the second page on, and a
            // cached `None` would leave drift inert for the rest of a first
            // run (#862 M-054).
            if !matches!(state.dest_schema_cache, Some(Some(_))) {
                state.dest_schema_cache = Some(sink.current_schema().await?);
            }
            let dest = state.dest_schema_cache.as_ref().and_then(|o| o.as_ref());
            match dest {
                None => {
                    if !state.warned_drift_inert {
                        tracing::info!(
                            connector = sink_name,
                            "schema-drift: sink reports no destination schema yet; \
                             drift handling is inert until it does"
                        );
                        state.warned_drift_inert = true;
                    }
                    (records, None)
                }
                Some(dest) => {
                    let inferred = crate::drift::infer_page_schema(dest, &records);
                    let diff = crate::drift::diff_schema(dest, &inferred, policy.allow_widening);
                    if diff.is_empty() {
                        (records, None)
                    } else {
                        // The cache may be replaced inside the evolve arm; `dest`
                        // borrows it, so re-clone before the call to drop the borrow.
                        let dest_owned = dest.clone();
                        apply_drift_policy(
                            policy,
                            &diff,
                            &dest_owned,
                            records,
                            &page_indices,
                            sink,
                            sink_name,
                            pipeline_name,
                            row,
                            &mut state.dest_schema_cache,
                            &mut drift_envelopes,
                        )
                        .await?
                    }
                }
            }
        } else {
            (records, None)
        };

    // Merge quality + contract + drift envelopes so the caller writes one batch.
    let mut envelopes = quality_envelopes;
    #[cfg(feature = "contract")]
    envelopes.extend(contract_envelopes);
    envelopes.append(&mut drift_envelopes);

    // With no DLQ there are no envelopes to strand (a no-DLQ quarantine config
    // is rejected at run start), so a drift abort is raised immediately.
    let mut drift_abort = drift_abort;
    if !specs.has_dlq
        && let Some(e) = drift_abort.take()
    {
        return Err(e);
    }

    Ok(GovernanceOutcome {
        records: GovernedPage::sealed(records),
        envelopes: GovernedEnvelopes::sealed(envelopes),
        deferred_abort: drift_abort,
    })
}

/// Make a page's quarantine envelopes durable ahead of a drift `fail` abort, so
/// the run stops without writing the page's survivors or its bookmark while the
/// rows already routed to the DLQ are not lost.
pub(crate) async fn abort_with_envelopes(
    dlq_cfg: &crate::dlq::DlqConfig,
    envelopes: &GovernedEnvelopes,
    dlq_stats: &mut DlqStats,
) -> Result<(), FaucetError> {
    if envelopes.is_empty() {
        return Ok(());
    }
    super::write::dlq(dlq_cfg.sink.as_ref(), envelopes)
        .await
        .map_err(|e| dlq_write_error("DLQ sink write failed", e))?;
    dlq_cfg
        .sink
        .flush()
        .await
        .map_err(|e| FaucetError::Sink(format!("DLQ sink flush failed: {e}")))?;
    dlq_stats.records_dlq += envelopes.len();
    dlq_stats.pages_with_failures += 1;
    Ok(())
}

/// A failed DLQ write. A refusal by the DLQ's own governance — its policy, the
/// run budget, or row accounting (#870, #879) — keeps its type so the run fails
/// with the real reason; anything else is a DLQ sink failure.
pub(crate) fn dlq_write_error(context: &str, e: FaucetError) -> FaucetError {
    match e {
        e @ (FaucetError::PolicyViolation { .. }
        | FaucetError::BudgetExceeded { .. }
        | FaucetError::RowsUnaccounted { .. }) => e,
        e => FaucetError::Sink(format!("{context}: {e}")),
    }
}

/// Apply the schema-drift policy to a page (#194). Returns the (possibly
/// trimmed) records and an optional deferred abort error. The page is never
/// written when it is set: with a DLQ the caller first makes the page's
/// quality/drift quarantine envelopes durable, then raises it; with no DLQ —
/// where no envelopes can exist — it is raised immediately.
/// Appends drift quarantine envelopes to `drift_envelopes`.
#[allow(clippy::too_many_arguments)]
async fn apply_drift_policy<Si: Sink + ?Sized>(
    policy: &crate::drift::SchemaDriftPolicy,
    diff: &crate::drift::SchemaDiff,
    dest: &Value,
    records: Vec<Value>,
    page_indices: &[usize],
    sink: &Si,
    sink_name: &str,
    pipeline_name: &str,
    row: &str,
    dest_schema_cache: &mut Option<Option<Value>>,
    drift_envelopes: &mut Vec<Value>,
) -> Result<(Vec<Value>, Option<FaucetError>), FaucetError> {
    use crate::drift::{OnDrift, OnIncompatible};
    use crate::observability::schema_drift as emit_drift;

    let mode = match policy.on_drift {
        OnDrift::Warn => "warn",
        OnDrift::Ignore => "ignore",
        OnDrift::Quarantine => "quarantine",
        OnDrift::Fail => "fail",
        OnDrift::Evolve => "evolve",
    };
    emit_drift(
        pipeline_name,
        row,
        sink_name,
        mode,
        "added",
        diff.additions.len() as u64,
    );
    emit_drift(
        pipeline_name,
        row,
        sink_name,
        mode,
        "widened",
        diff.widenings.len() as u64,
    );
    emit_drift(
        pipeline_name,
        row,
        sink_name,
        mode,
        "narrowed",
        diff.incompatible.len() as u64,
    );
    emit_drift(
        pipeline_name,
        row,
        sink_name,
        mode,
        "dropped",
        diff.droppable_required.len() as u64,
    );

    match policy.on_drift {
        OnDrift::Warn => {
            tracing::warn!(
                connector = sink_name,
                columns = ?diff.changed_columns(),
                "schema-drift detected (on_drift=warn); writing page unchanged"
            );
            Ok((records, None))
        }
        OnDrift::Fail => Ok((
            records,
            Some(FaucetError::SchemaDrift {
                columns: diff.changed_columns(),
                message: "schema drift detected (on_drift=fail)".to_string(),
            }),
        )),
        OnDrift::Ignore => {
            // Drop fields not present in the destination schema.
            let allowed: std::collections::HashSet<String> = dest
                .get("properties")
                .and_then(|p| p.as_object())
                .map(|m| m.keys().cloned().collect())
                .unwrap_or_default();
            let trimmed = records
                .into_iter()
                .map(|r| match r {
                    Value::Object(map) => Value::Object(
                        map.into_iter()
                            .filter(|(k, _)| allowed.contains(k))
                            .collect(),
                    ),
                    other => other,
                })
                .collect();
            Ok((trimmed, None))
        }
        OnDrift::Quarantine => {
            let (kept, env) =
                quarantine_drift_rows(diff, records, page_indices, sink_name, pipeline_name, row);
            drift_envelopes.extend(env);
            Ok((kept, None))
        }
        OnDrift::Evolve => {
            let evolution = crate::drift::SchemaEvolution {
                additions: diff.additions.clone(),
                widenings: diff
                    .widenings
                    .iter()
                    .filter(|c| {
                        c.from
                            .as_ref()
                            .map(|f| crate::drift::base_widened(f, &c.to))
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect(),
                relax_nullability: diff
                    .droppable_required
                    .iter()
                    // A column merely *absent* from this page only relaxes its
                    // NOT NULL constraint when explicitly opted in — otherwise a
                    // transient/partial page would silently and irreversibly
                    // weaken the destination schema (F28).
                    .filter(|_| policy.relax_nullability_on_missing)
                    .cloned()
                    .chain(
                        diff.widenings
                            .iter()
                            .filter(|c| {
                                c.from
                                    .as_ref()
                                    .map(|f| crate::drift::adds_null(f, &c.to))
                                    .unwrap_or(false)
                            })
                            .map(|c| c.name.clone()),
                    )
                    .collect(),
            };
            if !evolution.is_empty() {
                sink.evolve_schema(&evolution).await?;
                // Refresh the cached destination schema so later pages diff
                // against the evolved shape (re-introspect authoritatively).
                *dest_schema_cache = Some(sink.current_schema().await?);
            }
            // Handle the incompatible residue.
            if diff.incompatible.is_empty() {
                Ok((records, None))
            } else {
                match policy.on_incompatible {
                    OnIncompatible::Fail => Ok((
                        records,
                        Some(FaucetError::SchemaDrift {
                            columns: diff.incompatible.iter().map(|c| c.name.clone()).collect(),
                            message: "incompatible type change cannot be auto-evolved \
                                      (on_incompatible=fail)"
                                .into(),
                        }),
                    )),
                    OnIncompatible::Quarantine => {
                        // Build a diff carrying only the incompatible columns.
                        let incompat_only = crate::drift::SchemaDiff {
                            incompatible: diff.incompatible.clone(),
                            ..Default::default()
                        };
                        let (kept, env) = quarantine_drift_rows(
                            &incompat_only,
                            records,
                            page_indices,
                            sink_name,
                            pipeline_name,
                            row,
                        );
                        drift_envelopes.extend(env);
                        Ok((kept, None))
                    }
                }
            }
        }
    }
}

/// Partition records: those exhibiting any drift column go to the DLQ; the rest
/// are kept. Returns `(kept, envelopes)`.
///
/// A row "exhibits drift" if it either **contains** a column the destination
/// lacks (an addition), **holds a value** that does not fit its destination
/// column's type (the rows behind a widening or incompatible change), or **omits** a `droppable_required` column (a destination NOT NULL
/// column absent from the page). All four buckets must be covered: a widening or
/// droppable-required column written to a *non-evolved* destination is exactly
/// the silent corruption `quarantine` exists to prevent.
pub(crate) fn quarantine_drift_rows(
    diff: &crate::drift::SchemaDiff,
    records: Vec<Value>,
    page_indices: &[usize],
    sink_name: &str,
    pipeline_name: &str,
    row: &str,
) -> (Vec<Value>, Vec<Value>) {
    use crate::dlq::{DlqReason, build_envelope};
    // Columns that taint a row by their PRESENCE in the record.
    let present_cols: std::collections::HashSet<&str> =
        diff.additions.iter().map(|c| c.name.as_str()).collect();
    // Columns the page diverges on: a row is tainted only when its OWN value
    // does not fit the destination column (one 1.5 in an INTEGER column must
    // not divert every row that carries an integer there).
    let typed_cols: Vec<(&str, &Value)> = diff
        .widenings
        .iter()
        .chain(&diff.incompatible)
        .filter_map(|c| c.from.as_ref().map(|dest| (c.name.as_str(), dest)))
        .collect();
    // Required destination columns that taint a row by their ABSENCE.
    let required_cols: std::collections::HashSet<&str> =
        diff.droppable_required.iter().map(|s| s.as_str()).collect();
    let mut kept = Vec::new();
    let mut envelopes = Vec::new();
    for (idx, rec) in records.into_iter().enumerate() {
        let exhibits = rec
            .as_object()
            .map(|m| {
                m.keys().any(|k| present_cols.contains(k.as_str()))
                    || typed_cols.iter().any(|(name, dest)| {
                        m.get(*name)
                            .is_some_and(|v| !crate::drift::value_fits(dest, name, v))
                    })
                    || required_cols.iter().any(|c| !m.contains_key(*c))
            })
            .unwrap_or(false);
        if exhibits {
            let err = FaucetError::SchemaDrift {
                columns: diff.changed_columns(),
                message: "row exhibits schema drift (on_drift=quarantine)".into(),
            };
            // Map the survivor-relative position back to the record's true page
            // index so the envelope annotation matches quality/contract (#321 L6).
            let page_index = page_indices.get(idx).copied().unwrap_or(idx);
            envelopes.push(build_envelope(
                &rec,
                &err,
                DlqReason::SchemaDrift,
                sink_name,
                pipeline_name,
                row,
                page_index,
            ));
        } else {
            kept.push(rec);
        }
    }
    (kept, envelopes)
}
