//! `faucet state show|set|reset|export|import` (#735) as reusable operations —
//! the CLI command and the `/v1/state` endpoints are thin adapters over these.
//!
//! Every mutation plans first (`dry_run` returns the plan without writing) and
//! refuses to touch a row whose run lease is live unless forced. Exactly-once
//! rows keep their envelope: the committed sequence is never lowered, and it is
//! raised to the sink's watermark when that is ahead, so a moved or reset
//! bookmark is honoured instead of being re-anchored to the sink's position.

use super::keys::{ClassifiedKey, KeyKind, classify};
use super::lease::{self, RunLease};
use super::target::{PipelineTarget, RowRole, RowTarget};
use crate::auth_catalog::AuthCatalog;
use crate::config::StateStoreSpec;
use crate::error::{CliError, CliResult};
use chrono::{DateTime, Utc};
use faucet_core::idempotency::{parse_token_parts, unwrap_state, wrap_state};
use faucet_core::state::{StateExport, namespace_prefix, validate_state_key};
use faucet_core::{StateStore, Value};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;

/// The state stores a pipeline's rows use, each built once.
pub struct Stores {
    specs: Vec<StateStoreSpec>,
    stores: Vec<Arc<dyn StateStore>>,
    /// Row id → index into `stores`.
    rows: BTreeMap<String, usize>,
    /// The pipeline-level store (pipeline markers, keys of unknown rows).
    pipeline: Option<usize>,
}

impl Stores {
    /// Build every distinct store the target's rows use. With `single`, every
    /// row (and the pipeline markers) use that one store instead.
    pub async fn build(
        target: &PipelineTarget,
        single: Option<Arc<dyn StateStore>>,
    ) -> CliResult<Self> {
        let mut out = Self {
            specs: Vec::new(),
            stores: Vec::new(),
            rows: BTreeMap::new(),
            pipeline: None,
        };
        if let Some(store) = single {
            out.specs.push(StateStoreSpec {
                kind: "override".into(),
                config: Value::Null,
            });
            out.stores.push(store);
            out.pipeline = Some(0);
            for r in &target.rows {
                out.rows.insert(r.id.clone(), 0);
            }
            return Ok(out);
        }
        if let Some(spec) = &target.state {
            out.pipeline = Some(out.index_for(spec).await?);
        }
        for r in &target.rows {
            if let Some(spec) = &r.state {
                let i = out.index_for(spec).await?;
                out.rows.insert(r.id.clone(), i);
            }
        }
        if out.pipeline.is_none() && !out.stores.is_empty() {
            out.pipeline = Some(0);
        }
        Ok(out)
    }

    async fn index_for(&mut self, spec: &StateStoreSpec) -> CliResult<usize> {
        if let Some(i) = self
            .specs
            .iter()
            .position(|s| s.kind == spec.kind && s.config == spec.config)
        {
            return Ok(i);
        }
        let store = crate::state::build_state_store(spec).await?;
        self.specs.push(spec.clone());
        self.stores.push(store);
        Ok(self.stores.len() - 1)
    }

    /// The store a row's keys live in (`None`: the row has no `state:`).
    pub fn for_row(&self, row: &str) -> Option<&Arc<dyn StateStore>> {
        self.rows.get(row).map(|&i| &self.stores[i])
    }

    /// The store a classified key lives in.
    pub fn for_key(&self, key: &ClassifiedKey) -> Option<&Arc<dyn StateStore>> {
        key.row
            .as_deref()
            .and_then(|r| self.for_row(r))
            .or_else(|| self.pipeline.map(|i| &self.stores[i]))
    }

    /// Every distinct store, with its kind.
    fn all(&self) -> impl Iterator<Item = (&str, &Arc<dyn StateStore>)> {
        self.specs
            .iter()
            .map(|s| s.kind.as_str())
            .zip(self.stores.iter())
    }

    /// Whether any store is configured.
    pub fn is_empty(&self) -> bool {
        self.stores.is_empty()
    }

    /// The kind of the store a row uses.
    pub fn kind_for_row(&self, row: &str) -> Option<&str> {
        self.rows.get(row).map(|&i| self.specs[i].kind.as_str())
    }
}

/// One stored key with its decoded meaning.
#[derive(Debug, Clone, Serialize)]
pub struct KeyEntry {
    #[serde(flatten)]
    pub key: ClassifiedKey,
    pub value: Value,
}

/// Every key under the namespace, classified — listed where the stores can
/// enumerate, derived from the config otherwise.
pub async fn collect_keys(target: &PipelineTarget, stores: &Stores) -> CliResult<Vec<KeyEntry>> {
    let prefix = namespace_prefix(&target.pipeline);
    let mut keys: BTreeMap<String, ClassifiedKey> = BTreeMap::new();
    for (_, store) in stores.all() {
        let listed = if store.supports_list() {
            store.list(&prefix).await?
        } else {
            derived_keys(target, store.as_ref()).await?
        };
        for k in listed {
            if let Some(c) = classify(&target.pipeline, &k) {
                keys.insert(k, c);
            }
        }
    }
    let mut out = Vec::new();
    for (k, c) in keys {
        let Some(store) = stores.for_key(&c) else {
            continue;
        };
        if let Some(value) = store.get(&k).await? {
            out.push(KeyEntry { key: c, value });
        }
    }
    Ok(out)
}

/// The keys a config implies, for a store that cannot list: each row's
/// bookmark and markers, plus the rollback runs its index names.
async fn derived_keys(target: &PipelineTarget, store: &dyn StateStore) -> CliResult<Vec<String>> {
    let mut out = Vec::new();
    for r in &target.rows {
        let base = target.base_key(&r.id);
        let index = crate::rollback::state::index_key(&base);
        if let Some(v) = store.get(&index).await? {
            for run in crate::rollback::state::RunIndex::decode(Some(&v)).runs {
                out.push(crate::rollback::state::marker_key(&base, &run));
            }
        }
        out.push(index);
        out.push(crate::sla::sla_state_key(&base));
        out.push(crate::profiling::profiling_state_key(&base));
        out.push(super::keys::status_key(&base));
        out.push(super::keys::lease_key(&base));
        out.push(base);
    }
    out.push(crate::replication::state::marker_key(&target.pipeline));
    Ok(out)
}

// ── show ────────────────────────────────────────────────────────────────────

/// The exactly-once envelope's sequence.
#[derive(Debug, Clone, Serialize)]
pub struct EnvelopeInfo {
    pub seq: u64,
}

/// One row's state.
#[derive(Debug, Clone, Serialize)]
pub struct RowState {
    pub row: String,
    pub role: RowRole,
    pub state_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store: Option<String>,
    /// The resume position (the envelope's inner bookmark on an exactly-once row).
    pub bookmark: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exactly_once: Option<EnvelopeInfo>,
    /// The bookmark's owner / shape version against what the source reads (#736).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_format: Option<StateFormat>,
    /// Child / product / shard bookmarks under the row.
    pub sub_bookmarks: Vec<KeyEntry>,
    pub markers: Vec<KeyEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub running: Option<RunLease>,
}

/// `faucet state show`.
#[derive(Debug, Clone, Serialize)]
pub struct ShowReport {
    pub pipeline: String,
    pub rows: Vec<RowState>,
    /// Replication / backfill markers and backfill-unit bookmarks.
    pub pipeline_keys: Vec<KeyEntry>,
    /// Keys under the namespace for rows the config no longer has.
    pub orphans: Vec<KeyEntry>,
}

/// Decode a stored bookmark value into `(bookmark, envelope)`, looking through
/// the versioned state envelope (#736).
pub fn decode_bookmark(value: &Value) -> (Option<Value>, Option<EnvelopeInfo>) {
    let payload = faucet_core::state_version::peel_versioned(value);
    if faucet_core::idempotency::is_eo_envelope(&payload) {
        let (bm, seq) = unwrap_state(&payload);
        (bm, Some(EnvelopeInfo { seq }))
    } else {
        (Some(payload), None)
    }
}

/// Whether `value` holds an exactly-once state envelope (inside the versioned
/// envelope or bare).
pub fn is_envelope(value: &Value) -> bool {
    faucet_core::idempotency::is_eo_envelope(&faucet_core::state_version::peel_versioned(value))
}

/// How a row's stored bookmark relates to what its source reads (#736).
#[derive(Debug, Clone, Serialize)]
pub struct StateFormat {
    /// Envelope version (`0` = stored before versioning).
    pub format: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    pub schema: u32,
    /// The owner and shape version this release's source reads.
    pub expected_owner: String,
    pub expected_schema: u32,
    /// `current`, `legacy` (rewritten in the envelope by the next run),
    /// `migrate` (migrated by the next run or `faucet migrate --state`), or
    /// `incompatible` (the next run refuses it).
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Compare a stored bookmark with what `row`'s source reads.
pub fn state_format(row: &RowTarget, stored: &Value) -> StateFormat {
    use faucet_core::state_version::{StateCompat, StoredState, check_compat};
    let codec = row_codec(row);
    let st = StoredState::parse(stored);
    let (status, detail) = match check_compat(&st, &codec.owner, codec.schema) {
        StateCompat::Current if st.is_legacy() => (
            "legacy",
            Some("stored before versioning — the next run rewrites it in the envelope".to_string()),
        ),
        StateCompat::Current => ("current", None),
        StateCompat::Migrate { from, to } => (
            "migrate",
            Some(format!(
                "bookmark schema {from} → {to}: migrated by the next run, or now with \
                 `faucet migrate --state`"
            )),
        ),
        StateCompat::Incompatible { found, expected } => (
            "incompatible",
            Some(format!(
                "found {found}, expected {expected} — the next run refuses it"
            )),
        ),
    };
    StateFormat {
        format: st.format,
        owner: st.owner,
        schema: st.schema,
        expected_owner: codec.owner,
        expected_schema: codec.schema,
        status,
        detail,
    }
}

/// The owner / shape version a row's bookmark is written under.
pub fn row_codec(row: &RowTarget) -> faucet_core::state_version::StateCodec {
    crate::registry::state_codec_for(row.source.as_ref().map(|(k, c)| (k.as_str(), c)))
}

/// [`row_codec`], writing the legacy shape when `legacy` is set — the same
/// decision the executor takes from `legacy_state_writes`.
pub fn row_codec_with(row: &RowTarget, legacy: bool) -> faucet_core::state_version::StateCodec {
    faucet_core::state_version::StateCodec {
        legacy,
        ..row_codec(row)
    }
}

/// Read every row's state (optionally one row).
pub async fn show(
    target: &PipelineTarget,
    stores: &Stores,
    row: Option<&str>,
    now: DateTime<Utc>,
) -> CliResult<ShowReport> {
    let selected: Vec<&RowTarget> = target.select(row)?;
    let entries = collect_keys(target, stores).await?;
    let mut rows = Vec::new();
    for r in &selected {
        let base = target.base_key(&r.id);
        let mut state = RowState {
            row: r.id.clone(),
            role: r.role,
            state_key: base.clone(),
            store: stores.kind_for_row(&r.id).map(str::to_owned),
            bookmark: None,
            exactly_once: None,
            state_format: None,
            sub_bookmarks: Vec::new(),
            markers: Vec::new(),
            running: None,
        };
        for e in entries
            .iter()
            .filter(|e| e.key.row.as_deref() == Some(&r.id))
        {
            match (&e.key.kind, &e.key.sub) {
                (KeyKind::Bookmark, None) => {
                    let (bm, eo) = decode_bookmark(&e.value);
                    state.bookmark = bm;
                    state.exactly_once = eo;
                    state.state_format = Some(state_format(r, &e.value));
                }
                (KeyKind::Bookmark, Some(_)) => state.sub_bookmarks.push(e.clone()),
                (KeyKind::Lease, None) => {
                    state.running =
                        RunLease::from_value(e.value.clone()).filter(|l| l.is_live(now));
                    state.markers.push(e.clone());
                }
                _ => state.markers.push(e.clone()),
            }
        }
        rows.push(state);
    }
    let known = |row: &str| target.rows.iter().any(|r| r.id == row);
    let (pipeline_keys, orphans) = if row.is_some() {
        (Vec::new(), Vec::new())
    } else {
        (
            entries
                .iter()
                .filter(|e| e.key.row.is_none())
                .cloned()
                .collect(),
            entries
                .iter()
                .filter(|e| e.key.row.as_deref().is_some_and(|r| !known(r)))
                .cloned()
                .collect(),
        )
    };
    Ok(ShowReport {
        pipeline: target.pipeline.clone(),
        rows,
        pipeline_keys,
        orphans,
    })
}

// ── guards ──────────────────────────────────────────────────────────────────

/// Refuse when a live lease covers any of `bases` (unless `force`).
async fn refuse_if_running(
    store: &dyn StateStore,
    bases: &[String],
    force: bool,
    now: DateTime<Utc>,
) -> CliResult<Vec<String>> {
    let mut warnings = Vec::new();
    for base in bases {
        if let Some(l) = lease::read(store, base).await?
            && l.is_live(now)
        {
            let msg = format!(
                "'{base}' is held by run {} (pid {}{}) since {}",
                l.run_id,
                l.pid,
                l.host
                    .as_deref()
                    .map(|h| format!(" on {h}"))
                    .unwrap_or_default(),
                l.acquired_at.format("%Y-%m-%dT%H:%M:%SZ")
            );
            if !force {
                return Err(CliError::StateBusy(format!(
                    "{msg} — wait for it to finish (or pass --force if that run is gone)"
                )));
            }
            warnings.push(format!("{msg}; proceeding because --force was given"));
        }
    }
    Ok(warnings)
}

fn store_for<'a>(stores: &'a Stores, row: &RowTarget) -> CliResult<&'a Arc<dyn StateStore>> {
    stores.for_row(&row.id).ok_or_else(|| {
        CliError::Config(format!(
            "row '{}' has no `state:` block — there is no durable state to change",
            row.id
        ))
    })
}

fn row_key(
    target: &PipelineTarget,
    row: &RowTarget,
    parent_key: Option<&str>,
) -> CliResult<String> {
    let key = crate::executor::build_state_key(&target.pipeline, &row.id, parent_key);
    validate_state_key(&key).map_err(|e| CliError::Config(format!("state key '{key}': {e}")))?;
    Ok(key)
}

// ── exactly-once watermark ──────────────────────────────────────────────────

/// What the sink's committed watermark says for a scope.
#[derive(Debug, Clone, Default, Serialize)]
pub struct WatermarkProbe {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bookmark: Option<Value>,
}

/// Read the sink's committed watermark for `scope` (read-only).
pub async fn probe_watermark(
    row: &RowTarget,
    scope: &str,
    auth: &AuthCatalog,
) -> CliResult<WatermarkProbe> {
    let sink = build_row_sink(row, auth).await?;
    let token = sink.last_committed_token(scope).await?;
    let (seq, bookmark) = match token.as_deref().and_then(parse_token_parts) {
        Some((s, b)) => (Some(s), b),
        None => (None, None),
    };
    Ok(WatermarkProbe {
        token,
        seq,
        bookmark,
    })
}

/// Ask the row's source how far behind its head it is (#733), after pointing
/// it at `bookmark` — the position the next run resumes from. Read-only.
pub async fn probe_lag(
    row: &RowTarget,
    bookmark: Option<&Value>,
    auth: &AuthCatalog,
) -> CliResult<Option<faucet_core::SourceLag>> {
    let Some((kind, config)) = &row.source else {
        return Ok(None);
    };
    let mut cfg = config.clone();
    crate::executor::resolve_now_inplace(&mut cfg, Utc::now().fixed_offset())?;
    let source = crate::registry::build_source(kind, cfg, auth, None).await?;
    if let Some(bm) = bookmark {
        source.apply_start_bookmark(bm.clone()).await?;
    }
    Ok(source.lag().await?)
}

pub(crate) async fn build_row_sink(
    row: &RowTarget,
    auth: &AuthCatalog,
) -> CliResult<Box<dyn faucet_core::Sink>> {
    let mut cfg = row.sink_config.clone();
    crate::executor::resolve_now_inplace(&mut cfg, Utc::now().fixed_offset())?;
    crate::registry::build_sink(&row.sink_kind, cfg, auth).await
}

/// How an exactly-once row's envelope was adjusted by a mutation.
#[derive(Debug, Clone, Serialize)]
pub struct EnvelopeAdjust {
    /// The envelope's sequence before.
    pub state_seq: u64,
    /// The sink's committed sequence (`None`: no token, or not probed).
    pub sink_seq: Option<u64>,
    /// The sequence written.
    pub written_seq: u64,
    pub probed: bool,
    pub note: String,
}

/// Work out the sequence to write so the sink watermark does not re-anchor
/// the row: never below the envelope's, raised to the sink's when ahead.
async fn envelope_seq(
    row: &RowTarget,
    key: &str,
    before: Option<&Value>,
    auth: &AuthCatalog,
    skip_watermark_check: bool,
) -> CliResult<EnvelopeAdjust> {
    let state_seq = before.map(|v| unwrap_state(v).1).unwrap_or(0);
    if skip_watermark_check {
        return Ok(EnvelopeAdjust {
            state_seq,
            sink_seq: None,
            written_seq: state_seq,
            probed: false,
            note: "sink watermark not checked (--skip-watermark-check): if the sink has \
                   committed past sequence {state_seq}, the next run re-anchors to the sink's \
                   position instead of this one"
                .replace("{state_seq}", &state_seq.to_string()),
        });
    }
    let probe = probe_watermark(row, key, auth).await.map_err(|e| {
        CliError::Config(format!(
            "row '{}' commits an exactly-once watermark with its data, and reading it failed: \
             {e}. Without it the change could be silently overridden by the sink's position — \
             fix the sink connection, or pass --skip-watermark-check to write anyway",
            row.id
        ))
    })?;
    let written_seq = state_seq.max(probe.seq.unwrap_or(0));
    let note = match probe.seq {
        Some(s) if s > state_seq => format!(
            "the sink's watermark (sequence {s}) is ahead of the state store's ({state_seq}); \
             writing sequence {s} so the next run honours this bookmark instead of re-anchoring \
             to the sink's position"
        ),
        Some(s) => {
            format!("the sink's watermark (sequence {s}) agrees; keeping sequence {written_seq}")
        }
        None => format!("the sink holds no watermark for this row; keeping sequence {written_seq}"),
    };
    Ok(EnvelopeAdjust {
        state_seq,
        sink_seq: probe.seq,
        written_seq,
        probed: true,
        note,
    })
}

// ── set ─────────────────────────────────────────────────────────────────────

/// `faucet state set`.
#[derive(Debug, Clone)]
pub struct SetRequest {
    pub row: String,
    pub parent_key: Option<String>,
    pub bookmark: Value,
    pub force: bool,
    pub dry_run: bool,
    pub skip_watermark_check: bool,
    /// Write the pre-versioning bookmark shape, for a cluster that still has
    /// a member older than the state envelope (#789 CLI-165).
    pub legacy_format: bool,
}

/// What `set` did (or would do).
#[derive(Debug, Clone, Serialize)]
pub struct SetOutcome {
    pub row: String,
    pub key: String,
    pub before: Option<Value>,
    pub after: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exactly_once: Option<EnvelopeAdjust>,
    pub applied: bool,
    pub warnings: Vec<String>,
}

/// Move a row's bookmark.
pub async fn set(
    target: &PipelineTarget,
    stores: &Stores,
    auth: &AuthCatalog,
    req: &SetRequest,
    now: DateTime<Utc>,
) -> CliResult<SetOutcome> {
    let row = target.row(&req.row)?;
    if row.role == RowRole::Child && req.parent_key.is_none() {
        return Err(CliError::Config(format!(
            "row '{}' is a child row — its bookmarks are per parent record; pass --parent-key",
            row.id
        )));
    }
    if req.bookmark.is_null() {
        return Err(CliError::Config(
            "a null bookmark means \"start over\" — use `faucet state reset` for that".into(),
        ));
    }
    let store = store_for(stores, row)?;
    let key = row_key(target, row, req.parent_key.as_deref())?;
    let mut warnings = refuse_if_running(
        store.as_ref(),
        &[target.base_key(&row.id), key.clone()],
        req.force,
        now,
    )
    .await?;
    let before = store.get(&key).await?;
    let eo = row.atomic_watermark || before.as_ref().is_some_and(is_envelope);
    let (after, exactly_once) = if eo {
        let adj = envelope_seq(row, &key, before.as_ref(), auth, req.skip_watermark_check).await?;
        warnings.push(adj.note.clone());
        (wrap_state(Some(&req.bookmark), adj.written_seq), Some(adj))
    } else {
        (req.bookmark.clone(), None)
    };
    let after = row_codec_with(row, req.legacy_format).encode(&after);
    if !req.dry_run {
        store.put(&key, &after).await?;
    }
    Ok(SetOutcome {
        row: row.id.clone(),
        key,
        before,
        after,
        exactly_once,
        applied: !req.dry_run,
        warnings,
    })
}

// ── reset ───────────────────────────────────────────────────────────────────

/// `faucet state reset`.
#[derive(Debug, Clone, Default)]
pub struct ResetRequest {
    pub row: String,
    pub parent_key: Option<String>,
    pub include_markers: bool,
    pub force: bool,
    pub dry_run: bool,
    pub skip_watermark_check: bool,
    /// Exactly-once rows: also delete the sink's commit token, instead of
    /// keeping the committed sequence in the state store.
    pub rewind_token: bool,
    /// Write the pre-versioning bookmark shape (see [`SetRequest`]).
    pub legacy_format: bool,
}

/// One key a reset changes.
#[derive(Debug, Clone, Serialize)]
pub struct KeyChange {
    pub key: String,
    pub before: Value,
    /// `None`: deleted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<Value>,
}

/// What `reset` did (or would do).
#[derive(Debug, Clone, Serialize)]
pub struct ResetOutcome {
    pub row: String,
    pub changes: Vec<KeyChange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exactly_once: Option<EnvelopeAdjust>,
    pub token_rewound: bool,
    pub applied: bool,
    pub warnings: Vec<String>,
}

/// Forget a row's position so its next run re-syncs from the start.
pub async fn reset(
    target: &PipelineTarget,
    stores: &Stores,
    auth: &AuthCatalog,
    req: &ResetRequest,
    now: DateTime<Utc>,
) -> CliResult<ResetOutcome> {
    let row = target.row(&req.row)?;
    let store = store_for(stores, row)?;
    let base = target.base_key(&row.id);
    let entries: Vec<KeyEntry> = collect_keys(target, stores)
        .await?
        .into_iter()
        .filter(|e| e.key.row.as_deref() == Some(&row.id))
        .collect();
    if row.role == RowRole::Child && req.parent_key.is_none() && !store.supports_list() {
        return Err(CliError::Config(format!(
            "row '{}' is a child row and this state store cannot enumerate its per-parent \
             bookmarks — pass --parent-key",
            row.id
        )));
    }
    let wanted = |e: &KeyEntry| -> bool {
        if let Some(pk) = &req.parent_key
            && e.key.sub.as_deref() != Some(pk.as_str())
        {
            return false;
        }
        match &e.key.kind {
            KeyKind::Bookmark => true,
            KeyKind::Lease => false,
            _ => req.include_markers,
        }
    };
    let targets: Vec<&KeyEntry> = entries.iter().filter(|e| wanted(e)).collect();
    let mut bases: Vec<String> = vec![base.clone()];
    bases.extend(targets.iter().filter_map(|e| e.key.base(&target.pipeline)));
    bases.dedup();
    let mut warnings = refuse_if_running(store.as_ref(), &bases, req.force, now).await?;

    let mut changes = Vec::new();
    let mut exactly_once = None;
    let mut token_rewound = false;
    for e in targets {
        let eo_bookmark =
            e.key.kind == KeyKind::Bookmark && (row.atomic_watermark || is_envelope(&e.value));
        if eo_bookmark && !req.rewind_token {
            let adj = envelope_seq(
                row,
                &e.key.key,
                Some(&e.value),
                auth,
                req.skip_watermark_check,
            )
            .await?;
            warnings.push(format!(
                "exactly-once row: keeping the envelope with a null bookmark at sequence {} so \
                 the sink's watermark cannot re-anchor the re-sync ({})",
                adj.written_seq, adj.note
            ));
            changes.push(KeyChange {
                key: e.key.key.clone(),
                before: e.value.clone(),
                after: Some(
                    row_codec_with(row, req.legacy_format)
                        .encode(&wrap_state(None, adj.written_seq)),
                ),
            });
            exactly_once = Some(adj);
        } else {
            if eo_bookmark && !req.dry_run {
                let sink = build_row_sink(row, auth).await?;
                sink.rewind_commit_token(&e.key.key, None)
                    .await
                    .map_err(|err| {
                        CliError::Config(format!(
                            "--rewind-token: {err} — reset without --rewind-token to keep the \
                         sequence in the state store instead"
                        ))
                    })?;
                token_rewound = true;
            }
            changes.push(KeyChange {
                key: e.key.key.clone(),
                before: e.value.clone(),
                after: None,
            });
        }
    }
    if !req.dry_run {
        for c in &changes {
            match &c.after {
                Some(v) => store.put(&c.key, v).await?,
                None => store.delete(&c.key).await?,
            }
        }
    }
    Ok(ResetOutcome {
        row: row.id.clone(),
        changes,
        exactly_once,
        token_rewound,
        applied: !req.dry_run,
        warnings,
    })
}

// ── export / import ─────────────────────────────────────────────────────────

/// Snapshot every key of the pipeline (run leases excluded — they describe a
/// process, not a position).
pub async fn export(
    target: &PipelineTarget,
    stores: &Stores,
    now: DateTime<Utc>,
) -> CliResult<StateExport> {
    let mut out = StateExport::new(&target.pipeline);
    out.exported_at = Some(now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
    for e in collect_keys(target, stores).await? {
        if e.key.kind != KeyKind::Lease {
            out.keys.insert(e.key.key, e.value);
        }
    }
    Ok(out)
}

/// `faucet state import`.
#[derive(Debug, Clone)]
pub struct ImportRequest {
    pub export: StateExport,
    /// Replace a non-empty namespace (keys absent from the export are deleted).
    pub overwrite: bool,
    pub force: bool,
    pub dry_run: bool,
}

/// What `import` did (or would do).
#[derive(Debug, Clone, Serialize)]
pub struct ImportOutcome {
    pub pipeline: String,
    /// Keys in the export.
    pub keys: usize,
    /// Keys already in the target namespace before the import.
    pub existing: Vec<String>,
    pub written: Vec<String>,
    pub deleted: Vec<String>,
    /// Every store written went through one all-or-nothing batch.
    pub atomic: bool,
    pub applied: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub warnings: Vec<String>,
}

/// The keys bound for one store during an import.
type StoreGroup = (Arc<dyn StateStore>, Vec<(String, Value)>);

/// Restore an export into the pipeline's stores (or the single store `stores`
/// was built with).
pub async fn import(
    target: &PipelineTarget,
    stores: &Stores,
    req: &ImportRequest,
    now: DateTime<Utc>,
) -> CliResult<ImportOutcome> {
    req.export.validate()?;
    if req.export.pipeline != target.pipeline {
        return Err(CliError::Config(format!(
            "this export holds pipeline '{}', but the config's pipeline is '{}' — import it with \
             the config it was exported from",
            req.export.pipeline, target.pipeline
        )));
    }
    if stores.is_empty() {
        return Err(CliError::Config(
            "the config has no `state:` block — pass --to-state to choose where to import".into(),
        ));
    }
    let existing: Vec<KeyEntry> = collect_keys(target, stores)
        .await?
        .into_iter()
        .filter(|e| e.key.kind != KeyKind::Lease)
        .collect();
    let existing_keys: Vec<String> = existing.iter().map(|e| e.key.key.clone()).collect();
    if !existing.is_empty() && !req.overwrite {
        return Err(CliError::Config(format!(
            "pipeline '{}' already has {} state key(s) in the target store — pass --overwrite to \
             replace them with the export",
            target.pipeline,
            existing.len()
        )));
    }
    let mut bases: Vec<(String, Arc<dyn StateStore>)> = Vec::new();
    for r in &target.rows {
        if let Some(s) = stores.for_row(&r.id) {
            bases.push((target.base_key(&r.id), Arc::clone(s)));
        }
    }
    let mut warnings = Vec::new();
    for (base, store) in &bases {
        warnings.extend(
            refuse_if_running(store.as_ref(), std::slice::from_ref(base), req.force, now).await?,
        );
    }

    // Route each key to the store its row uses.
    let mut groups: Vec<StoreGroup> = Vec::new();
    for (key, value) in &req.export.keys {
        let Some(c) = classify(&target.pipeline, key) else {
            continue;
        };
        if c.kind == KeyKind::Lease {
            continue;
        }
        let Some(store) = stores.for_key(&c) else {
            return Err(CliError::Config(format!(
                "key '{key}' belongs to row '{}', which has no `state:` block",
                c.row.unwrap_or_default()
            )));
        };
        match groups.iter_mut().find(|(s, _)| Arc::ptr_eq(s, store)) {
            Some((_, entries)) => entries.push((key.clone(), value.clone())),
            None => groups.push((Arc::clone(store), vec![(key.clone(), value.clone())])),
        }
    }
    let stale: Vec<&KeyEntry> = existing
        .iter()
        .filter(|e| !req.export.keys.contains_key(&e.key.key))
        .collect();
    let mut out = ImportOutcome {
        pipeline: target.pipeline.clone(),
        keys: req.export.keys.len(),
        existing: existing_keys,
        written: Vec::new(),
        deleted: Vec::new(),
        atomic: groups.iter().all(|(s, _)| s.supports_atomic_batch()),
        applied: !req.dry_run,
        error: None,
        warnings,
    };
    if req.dry_run {
        out.written = groups
            .iter()
            .flat_map(|(_, e)| e.iter().map(|(k, _)| k.clone()))
            .collect();
        out.deleted = stale.iter().map(|e| e.key.key.clone()).collect();
        return Ok(out);
    }
    for (store, entries) in &groups {
        if store.supports_atomic_batch() {
            if let Err(e) = store.put_batch(entries).await {
                out.error = Some(e.to_string());
                return Ok(out);
            }
            out.written.extend(entries.iter().map(|(k, _)| k.clone()));
        } else {
            for (k, v) in entries {
                if let Err(e) = store.put(k, v).await {
                    out.error = Some(format!("writing '{k}': {e}"));
                    return Ok(out);
                }
                out.written.push(k.clone());
            }
        }
    }
    for e in stale {
        let Some(store) = stores.for_key(&e.key) else {
            continue;
        };
        if let Err(err) = store.delete(&e.key.key).await {
            out.error = Some(format!("deleting stale '{}': {err}", e.key.key));
            return Ok(out);
        }
        out.deleted.push(e.key.key.clone());
    }
    Ok(out)
}

// ── --to-state ──────────────────────────────────────────────────────────────

/// Parse `--to-state`: a `{type, config}` document, a `postgres://` /
/// `redis://` URL, `memory`, or a directory (`file:PATH` or a bare path).
pub fn parse_state_target(raw: &str) -> CliResult<StateStoreSpec> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(CliError::Config("--to-state must not be empty".into()));
    }
    if raw.starts_with('{') {
        return serde_yaml::from_str::<StateStoreSpec>(raw)
            .map_err(|e| CliError::Config(format!("--to-state: {e}")));
    }
    let lower = raw.to_ascii_lowercase();
    let spec = |kind: &str, config: Value| StateStoreSpec {
        kind: kind.to_string(),
        config,
    };
    Ok(
        if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
            spec(
                "postgres",
                serde_json::json!({ "url": raw, "ensure_table": true }),
            )
        } else if lower.starts_with("redis://") || lower.starts_with("rediss://") {
            spec("redis", serde_json::json!({ "url": raw }))
        } else if lower == "memory" {
            spec("memory", serde_json::json!({}))
        } else {
            let path = raw.strip_prefix("file:").unwrap_or(raw);
            spec("file", serde_json::json!({ "path": path }))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PipelineConfig;
    use faucet_core::MemoryStateStore;
    use serde_json::json;
    use std::path::Path;

    fn target(extra_rows: &str) -> PipelineTarget {
        let text = format!(
            r#"version: 1
name: orders
pipeline:
  source: {{ type: csv, config: {{ path: in.csv }} }}
  sink: {{ type: stdout, config: {{}} }}
  state: {{ type: memory, config: {{}} }}
matrix:
  - id: a
  - id: kid
    parent: a
    parent_key: id
{extra_rows}"#
        );
        let cfg = PipelineConfig::from_text(&text, Path::new("t.yaml")).unwrap();
        PipelineTarget::resolve(&cfg, "orders").unwrap()
    }

    async fn stores(t: &PipelineTarget) -> (Stores, Arc<dyn StateStore>) {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        (
            Stores::build(t, Some(Arc::clone(&store))).await.unwrap(),
            store,
        )
    }

    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    #[tokio::test]
    async fn show_groups_bookmarks_markers_children_and_orphans() {
        let t = target("");
        let (s, store) = stores(&t).await;
        store.put("orders::a", &json!({"id": 5})).await.unwrap();
        store
            .put("orders::a::__sla__", &json!({"last_success_unix": 1}))
            .await
            .unwrap();
        store.put("orders::kid::1", &json!({"c": 1})).await.unwrap();
        store.put("orders::gone", &json!(1)).await.unwrap();
        store
            .put("orders::__replication__", &json!({"phase": "cdc"}))
            .await
            .unwrap();
        let r = show(&t, &s, None, now()).await.unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].bookmark, Some(json!({"id": 5})));
        assert_eq!(r.rows[0].markers.len(), 1);
        assert_eq!(r.rows[1].sub_bookmarks.len(), 1);
        assert_eq!(r.pipeline_keys.len(), 1);
        assert_eq!(r.orphans.len(), 1);
        let one = show(&t, &s, Some("a"), now()).await.unwrap();
        assert_eq!(one.rows.len(), 1);
        assert!(one.orphans.is_empty() && one.pipeline_keys.is_empty());
    }

    #[tokio::test]
    async fn show_decodes_envelopes_and_live_leases() {
        let t = target("");
        let (s, store) = stores(&t).await;
        store
            .put("orders::a", &wrap_state(Some(&json!({"lsn": 9})), 4))
            .await
            .unwrap();
        let g = lease::acquire(Arc::clone(&store), "orders::a", "run-x")
            .await
            .unwrap();
        let r = show(&t, &s, Some("a"), now()).await.unwrap();
        assert_eq!(r.rows[0].bookmark, Some(json!({"lsn": 9})));
        assert_eq!(r.rows[0].exactly_once.as_ref().unwrap().seq, 4);
        assert_eq!(r.rows[0].running.as_ref().unwrap().run_id, "run-x");
        g.release().await;
    }

    #[tokio::test]
    async fn set_writes_bare_bookmarks_and_refuses_bad_input() {
        let t = target("");
        let (s, store) = stores(&t).await;
        let auth = AuthCatalog::new();
        let req = |row: &str, bm: Value| SetRequest {
            row: row.into(),
            parent_key: None,
            bookmark: bm,
            force: false,
            dry_run: false,
            skip_watermark_check: false,
            legacy_format: false,
        };
        let dry = SetRequest {
            dry_run: true,
            ..req("a", json!({"id": 1}))
        };
        let o = set(&t, &s, &auth, &dry, now()).await.unwrap();
        assert!(!o.applied);
        assert!(store.get("orders::a").await.unwrap().is_none());
        let o = set(&t, &s, &auth, &req("a", json!({"id": 1})), now())
            .await
            .unwrap();
        assert!(o.applied && o.before.is_none());
        assert_eq!(
            store
                .get("orders::a")
                .await
                .unwrap()
                .map(|v| faucet_core::state_version::peel_versioned(&v)),
            Some(json!({"id": 1}))
        );

        assert!(
            set(&t, &s, &auth, &req("kid", json!(1)), now())
                .await
                .is_err()
        );
        let with_pk = SetRequest {
            parent_key: Some("7".into()),
            ..req("kid", json!({"c": 2}))
        };
        set(&t, &s, &auth, &with_pk, now()).await.unwrap();
        assert_eq!(
            store
                .get("orders::kid::7")
                .await
                .unwrap()
                .map(|v| faucet_core::state_version::peel_versioned(&v)),
            Some(json!({"c": 2}))
        );
        let err = set(&t, &s, &auth, &req("a", Value::Null), now())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("reset"), "{err}");
        let bad = SetRequest {
            parent_key: Some("a b".into()),
            ..req("kid", json!(1))
        };
        assert!(set(&t, &s, &auth, &bad, now()).await.is_err());
    }

    #[tokio::test]
    async fn set_and_reset_write_the_legacy_shape_when_asked() {
        let t = target("");
        let (s, store) = stores(&t).await;
        let req = SetRequest {
            row: "a".into(),
            parent_key: None,
            bookmark: json!({"id": 5}),
            force: false,
            dry_run: false,
            skip_watermark_check: false,
            legacy_format: true,
        };
        set(&t, &s, &AuthCatalog::new(), &req, now()).await.unwrap();
        assert_eq!(
            store.get("orders::a").await.unwrap(),
            Some(json!({"id": 5}))
        );
        set(
            &t,
            &s,
            &AuthCatalog::new(),
            &SetRequest {
                legacy_format: false,
                ..req
            },
            now(),
        )
        .await
        .unwrap();
        let stored = store.get("orders::a").await.unwrap().unwrap();
        assert_ne!(stored, json!({"id": 5}), "default writes the envelope");
    }

    #[tokio::test]
    async fn mutations_refuse_a_live_lease_unless_forced() {
        let t = target("");
        let (s, store) = stores(&t).await;
        let auth = AuthCatalog::new();
        let g = lease::acquire(Arc::clone(&store), "orders::a", "run-1")
            .await
            .unwrap();
        let mut req = SetRequest {
            row: "a".into(),
            parent_key: None,
            bookmark: json!(1),
            force: false,
            dry_run: false,
            skip_watermark_check: false,
            legacy_format: false,
        };
        let err = set(&t, &s, &auth, &req, now())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("held by run run-1"), "{err}");
        req.force = true;
        let o = set(&t, &s, &auth, &req, now()).await.unwrap();
        assert!(o.warnings.iter().any(|w| w.contains("--force")));
        let err = reset(
            &t,
            &s,
            &auth,
            &ResetRequest {
                row: "a".into(),
                ..Default::default()
            },
            now(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("held by run"));
        g.release().await;
    }

    #[tokio::test]
    async fn set_on_an_envelope_keeps_the_sequence_when_unchecked() {
        let t = target("");
        let (s, store) = stores(&t).await;
        store
            .put("orders::a", &wrap_state(Some(&json!(1)), 9))
            .await
            .unwrap();
        let o = set(
            &t,
            &s,
            &AuthCatalog::new(),
            &SetRequest {
                row: "a".into(),
                parent_key: None,
                bookmark: json!({"id": 3}),
                force: false,
                dry_run: false,
                skip_watermark_check: true,
                legacy_format: false,
            },
            now(),
        )
        .await
        .unwrap();
        let adj = o.exactly_once.unwrap();
        assert_eq!((adj.state_seq, adj.written_seq, adj.probed), (9, 9, false));
        let (bm, seq) = unwrap_state(&store.get("orders::a").await.unwrap().unwrap());
        assert_eq!((bm, seq), (Some(json!({"id": 3})), 9));
    }

    #[tokio::test]
    async fn set_on_an_envelope_refuses_when_the_watermark_cannot_be_read() {
        let t = target("");
        let (s, store) = stores(&t).await;
        store
            .put("orders::a", &wrap_state(Some(&json!(1)), 9))
            .await
            .unwrap();
        let mut req = SetRequest {
            row: "a".into(),
            parent_key: None,
            bookmark: json!(2),
            force: false,
            dry_run: false,
            skip_watermark_check: false,
            legacy_format: false,
        };
        // The stdout sink has no watermark, so probing succeeds with no token.
        let o = set(&t, &s, &AuthCatalog::new(), &req, now()).await.unwrap();
        assert!(o.exactly_once.unwrap().note.contains("holds no watermark"));
        // An unbuildable sink makes the probe fail — refuse.
        let mut broken = t.clone();
        broken.rows[0].sink_kind = "no-such-sink".into();
        req.bookmark = json!(3);
        let err = set(&broken, &s, &AuthCatalog::new(), &req, now())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("--skip-watermark-check"), "{err}");
    }

    #[tokio::test]
    async fn reset_deletes_positions_and_optionally_markers() {
        let t = target("");
        let (s, store) = stores(&t).await;
        let auth = AuthCatalog::new();
        store.put("orders::a", &json!(1)).await.unwrap();
        store.put("orders::a::__sla__", &json!({})).await.unwrap();
        store
            .put("orders::a::__status__", &json!({}))
            .await
            .unwrap();
        store.put("orders::kid::1", &json!(1)).await.unwrap();
        store.put("orders::kid::2", &json!(2)).await.unwrap();

        let dry = ResetRequest {
            row: "a".into(),
            dry_run: true,
            ..Default::default()
        };
        let o = reset(&t, &s, &auth, &dry, now()).await.unwrap();
        assert_eq!(o.changes.len(), 1);
        assert!(store.get("orders::a").await.unwrap().is_some());

        let o = reset(
            &t,
            &s,
            &auth,
            &ResetRequest {
                row: "a".into(),
                include_markers: true,
                ..Default::default()
            },
            now(),
        )
        .await
        .unwrap();
        assert_eq!(o.changes.len(), 3);
        assert!(store.list("orders::a").await.unwrap().is_empty());

        let o = reset(
            &t,
            &s,
            &auth,
            &ResetRequest {
                row: "kid".into(),
                parent_key: Some("1".into()),
                ..Default::default()
            },
            now(),
        )
        .await
        .unwrap();
        assert_eq!(o.changes.len(), 1);
        assert!(store.get("orders::kid::2").await.unwrap().is_some());
        reset(
            &t,
            &s,
            &auth,
            &ResetRequest {
                row: "kid".into(),
                ..Default::default()
            },
            now(),
        )
        .await
        .unwrap();
        assert!(store.get("orders::kid::2").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn reset_keeps_an_envelope_sequence_or_rewinds_the_token() {
        let t = target("");
        let (s, store) = stores(&t).await;
        let auth = AuthCatalog::new();
        store
            .put("orders::a", &wrap_state(Some(&json!(1)), 5))
            .await
            .unwrap();
        let o = reset(
            &t,
            &s,
            &auth,
            &ResetRequest {
                row: "a".into(),
                ..Default::default()
            },
            now(),
        )
        .await
        .unwrap();
        assert_eq!(o.exactly_once.unwrap().written_seq, 5);
        assert_eq!(
            unwrap_state(&store.get("orders::a").await.unwrap().unwrap()),
            (None, 5)
        );
        // stdout cannot rewind a token: the reset refuses and changes nothing.
        let err = reset(
            &t,
            &s,
            &auth,
            &ResetRequest {
                row: "a".into(),
                rewind_token: true,
                ..Default::default()
            },
            now(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("--rewind-token"), "{err}");
        assert!(store.get("orders::a").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn rows_without_state_are_refused() {
        let text = "version: 1\nname: o\npipeline:\n  source: { type: csv, config: { path: a } }\n  sink: { type: jsonl, config: { path: b } }\n";
        let cfg = PipelineConfig::from_text(text, Path::new("t.yaml")).unwrap();
        let t = PipelineTarget::resolve(&cfg, "o").unwrap();
        let s = Stores::build(&t, None).await.unwrap();
        assert!(s.is_empty());
        let err = reset(
            &t,
            &s,
            &AuthCatalog::new(),
            &ResetRequest {
                row: "row-0".into(),
                ..Default::default()
            },
            now(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("no `state:` block"), "{err}");
        let r = show(&t, &s, None, now()).await.unwrap();
        assert!(r.rows[0].store.is_none());
        let err = import(
            &t,
            &s,
            &ImportRequest {
                export: StateExport::new("o"),
                overwrite: false,
                force: false,
                dry_run: false,
            },
            now(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("--to-state"));
    }

    #[tokio::test]
    async fn export_import_round_trip_with_overwrite_rules() {
        let t = target("");
        let (s, store) = stores(&t).await;
        store.put("orders::a", &json!({"id": 1})).await.unwrap();
        store
            .put("orders::a::__status__", &json!({}))
            .await
            .unwrap();
        let g = lease::acquire(Arc::clone(&store), "orders::kid", "r")
            .await
            .unwrap();
        let exp = export(&t, &s, now()).await.unwrap();
        assert_eq!(exp.keys.len(), 2, "lease excluded: {:?}", exp.keys);
        assert!(exp.exported_at.is_some());
        g.release().await;

        let (s2, store2) = stores(&t).await;
        let req = |overwrite, dry_run| ImportRequest {
            export: exp.clone(),
            overwrite,
            force: false,
            dry_run,
        };
        let o = import(&t, &s2, &req(false, true), now()).await.unwrap();
        assert_eq!(o.written.len(), 2);
        assert!(!o.applied && store2.get("orders::a").await.unwrap().is_none());
        let o = import(&t, &s2, &req(false, false), now()).await.unwrap();
        assert!(o.atomic && o.error.is_none());
        assert_eq!(
            store2
                .get("orders::a")
                .await
                .unwrap()
                .map(|v| faucet_core::state_version::peel_versioned(&v)),
            Some(json!({"id": 1}))
        );

        let err = import(&t, &s2, &req(false, false), now())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("--overwrite"));
        store2.put("orders::kid::9", &json!(1)).await.unwrap();
        let o = import(&t, &s2, &req(true, false), now()).await.unwrap();
        assert_eq!(o.deleted, vec!["orders::kid::9"]);
        assert_eq!(o.existing.len(), 3);

        let mut other = exp.clone();
        other.pipeline = "else".into();
        other.keys.clear();
        let err = import(
            &t,
            &s2,
            &ImportRequest {
                export: other,
                overwrite: true,
                force: false,
                dry_run: false,
            },
            now(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("holds pipeline 'else'"));
    }

    #[tokio::test]
    async fn import_on_a_sequential_store_reports_partial_progress() {
        struct Bare {
            inner: MemoryStateStore,
        }
        #[faucet_core::async_trait]
        impl StateStore for Bare {
            async fn get(&self, k: &str) -> Result<Option<Value>, faucet_core::FaucetError> {
                self.inner.get(k).await
            }
            async fn put(&self, k: &str, v: &Value) -> Result<(), faucet_core::FaucetError> {
                if k.ends_with("::b") {
                    return Err(faucet_core::FaucetError::State("full".into()));
                }
                self.inner.put(k, v).await
            }
            async fn delete(&self, k: &str) -> Result<(), faucet_core::FaucetError> {
                self.inner.delete(k).await
            }
        }
        let t = target("  - id: b\n");
        let store: Arc<dyn StateStore> = Arc::new(Bare {
            inner: MemoryStateStore::new(),
        });
        store.delete("orders::none").await.unwrap();
        let s = Stores::build(&t, Some(store)).await.unwrap();
        let mut exp = StateExport::new("orders");
        exp.keys.insert("orders::a".into(), json!(1));
        exp.keys.insert("orders::b".into(), json!(2));
        let o = import(
            &t,
            &s,
            &ImportRequest {
                export: exp,
                overwrite: false,
                force: false,
                dry_run: false,
            },
            now(),
        )
        .await
        .unwrap();
        assert!(!o.atomic);
        assert_eq!(o.written, vec!["orders::a"]);
        assert!(o.error.unwrap().contains("orders::b"));
    }

    #[tokio::test]
    async fn import_skips_leases_and_reports_batch_and_delete_failures() {
        struct Flaky {
            inner: MemoryStateStore,
            batch_fails: bool,
        }
        #[faucet_core::async_trait]
        impl StateStore for Flaky {
            async fn get(&self, k: &str) -> Result<Option<Value>, faucet_core::FaucetError> {
                self.inner.get(k).await
            }
            async fn put(&self, k: &str, v: &Value) -> Result<(), faucet_core::FaucetError> {
                self.inner.put(k, v).await
            }
            async fn delete(&self, _: &str) -> Result<(), faucet_core::FaucetError> {
                Err(faucet_core::FaucetError::State("read-only".into()))
            }
            async fn list(&self, p: &str) -> Result<Vec<String>, faucet_core::FaucetError> {
                self.inner.list(p).await
            }
            fn supports_list(&self) -> bool {
                true
            }
            fn supports_atomic_batch(&self) -> bool {
                true
            }
            async fn put_batch(
                &self,
                e: &[(String, Value)],
            ) -> Result<(), faucet_core::FaucetError> {
                if self.batch_fails {
                    return Err(faucet_core::FaucetError::State("tx aborted".into()));
                }
                for (k, v) in e {
                    self.inner.put(k, v).await?;
                }
                Ok(())
            }
        }
        let t = target("");
        let req = |keys: &[(&str, Value)], overwrite: bool| {
            let mut exp = StateExport::new("orders");
            for (k, v) in keys {
                exp.keys.insert((*k).to_string(), v.clone());
            }
            ImportRequest {
                export: exp,
                overwrite,
                force: false,
                dry_run: false,
            }
        };

        let failing: Arc<dyn StateStore> = Arc::new(Flaky {
            inner: MemoryStateStore::new(),
            batch_fails: true,
        });
        let s = Stores::build(&t, Some(failing)).await.unwrap();
        let o = import(&t, &s, &req(&[("orders::a", json!(1))], false), now())
            .await
            .unwrap();
        assert!(o.atomic);
        assert!(o.written.is_empty());
        assert_eq!(o.error.as_deref(), Some("State error: tx aborted"));

        let flaky = Flaky {
            inner: MemoryStateStore::new(),
            batch_fails: false,
        };
        flaky.inner.put("orders::a", &json!(0)).await.unwrap();
        flaky
            .inner
            .put("orders::a::__sla__", &json!({}))
            .await
            .unwrap();
        let store: Arc<dyn StateStore> = Arc::new(flaky);
        let s = Stores::build(&t, Some(Arc::clone(&store))).await.unwrap();
        let lease = json!({"run_id": "r", "pid": 1, "acquired_at": now(), "expires_at": now()});
        let o = import(
            &t,
            &s,
            &req(
                &[("orders::a", json!(9)), ("orders::a::__lease__", lease)],
                true,
            ),
            now(),
        )
        .await
        .unwrap();
        assert_eq!(o.written, vec!["orders::a"], "the lease is never imported");
        assert!(store.get("orders::a::__lease__").await.unwrap().is_none());
        assert_eq!(store.get("orders::a").await.unwrap(), Some(json!(9)));
        let err = o.error.expect("the stale delete fails");
        assert!(err.contains("deleting stale 'orders::a::__sla__'"), "{err}");
    }

    #[tokio::test]
    async fn stores_route_rows_to_their_own_backends() {
        let dir = tempfile::tempdir().unwrap();
        let t = target(&format!(
            "  - id: f\n    state: {{ type: file, config: {{ path: {} }} }}\n",
            dir.path().display()
        ));
        let s = Stores::build(&t, None).await.unwrap();
        assert_eq!(s.kind_for_row("a"), Some("memory"));
        assert_eq!(s.kind_for_row("f"), Some("file"));
        s.for_row("f")
            .unwrap()
            .put("orders::f", &json!(1))
            .await
            .unwrap();
        s.for_row("a")
            .unwrap()
            .put("orders::a", &json!(2))
            .await
            .unwrap();
        let keys: Vec<String> = collect_keys(&t, &s)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.key.key)
            .collect();
        assert_eq!(keys, vec!["orders::a", "orders::f"]);
        assert!(dir.path().join("orders%3A%3Af.json").exists());
    }

    #[tokio::test]
    async fn derived_keys_cover_a_store_that_cannot_list() {
        struct NoList(MemoryStateStore);
        #[faucet_core::async_trait]
        impl StateStore for NoList {
            async fn get(&self, k: &str) -> Result<Option<Value>, faucet_core::FaucetError> {
                self.0.get(k).await
            }
            async fn put(&self, k: &str, v: &Value) -> Result<(), faucet_core::FaucetError> {
                self.0.put(k, v).await
            }
            async fn delete(&self, k: &str) -> Result<(), faucet_core::FaucetError> {
                self.0.delete(k).await
            }
        }
        let t = target("");
        let store: Arc<dyn StateStore> = Arc::new(NoList(MemoryStateStore::new()));
        store.put("orders::a", &json!(1)).await.unwrap();
        store
            .put("orders::a::__rollback__", &json!({"runs": ["r1"]}))
            .await
            .unwrap();
        store
            .put("orders::a::__rollback__::r1", &json!({"x": 1}))
            .await
            .unwrap();
        store.put("orders::kid::1", &json!(1)).await.unwrap();
        store.put("orders::gone", &json!(1)).await.unwrap();
        store.delete("orders::gone").await.unwrap();
        let s = Stores::build(&t, Some(store)).await.unwrap();
        let keys: Vec<String> = collect_keys(&t, &s)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.key.key)
            .collect();
        assert_eq!(
            keys,
            vec![
                "orders::a",
                "orders::a::__rollback__",
                "orders::a::__rollback__::r1"
            ]
        );
        let err = reset(
            &t,
            &s,
            &AuthCatalog::new(),
            &ResetRequest {
                row: "kid".into(),
                ..Default::default()
            },
            now(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("--parent-key"), "{err}");
    }

    #[test]
    fn parses_state_targets() {
        let s = parse_state_target("postgres://u@h/db").unwrap();
        assert_eq!(
            (s.kind.as_str(), s.config["ensure_table"].as_bool()),
            ("postgres", Some(true))
        );
        assert_eq!(parse_state_target("REDIS://h:6379").unwrap().kind, "redis");
        assert_eq!(parse_state_target("memory").unwrap().kind, "memory");
        let f = parse_state_target("file:./st").unwrap();
        assert_eq!(
            (f.kind.as_str(), f.config["path"].as_str()),
            ("file", Some("./st"))
        );
        assert_eq!(
            parse_state_target("/var/x").unwrap().config["path"],
            "/var/x"
        );
        let doc =
            parse_state_target("{type: redis, config: {url: 'redis://h', namespace: n}}").unwrap();
        assert_eq!(doc.config["namespace"], "n");
        assert!(parse_state_target("  ").is_err());
        assert!(parse_state_target("{type: [").is_err());
    }

    #[test]
    fn decodes_bare_and_enveloped_bookmarks() {
        let (bm, eo) = decode_bookmark(&json!({"id": 1}));
        assert_eq!(bm, Some(json!({"id": 1})));
        assert!(eo.is_none());
        let (bm, eo) = decode_bookmark(&wrap_state(None, 3));
        assert!(bm.is_none());
        assert_eq!(eo.unwrap().seq, 3);
    }
}
