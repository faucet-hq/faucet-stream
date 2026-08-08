//! `DashMap`-backed run history (default backend). Lost on restart; that is the
//! documented memory-backend trade-off. Idempotency claims live in a second map
//! and are pruned both lazily (on re-claim) and by `purge_expired`.

use super::catalog::{
    self, CatalogDataset, CatalogDatasetDetail, CatalogDatasetPage, CatalogLineageEdge,
    CatalogListFilter, CatalogSchemaVersion, CatalogStatsPoint, CatalogUpdate,
};
use super::templates;
use super::{
    AuditEntry, AuditFilter, Claim, DeleteOutcome, HistoryError, ListFilter, ListPage, RunHistory,
    RunRecord,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

/// Cap on in-memory audit records (oldest dropped past this). The memory backend
/// is ephemeral anyway; this just bounds growth for a long-lived process.
const AUDIT_RING_CAP: usize = 10_000;

struct IdemEntry {
    run_id: String,
    fingerprint: String,
    claimed_at: DateTime<Utc>,
}

/// In-memory Data Movement Catalog state (#279). One `Mutex` guards the whole
/// catalog so a `catalog_record` (a read-modify-write across three maps) is
/// atomic without per-map lock ordering.
#[derive(Default)]
struct CatalogState {
    datasets: std::collections::HashMap<String, CatalogDataset>,
    /// dataset id → timeline, oldest first.
    schema_versions: std::collections::HashMap<String, Vec<CatalogSchemaVersion>>,
    /// dataset id → volume points, oldest first, capped at `STATS_RETAIN`.
    stats: std::collections::HashMap<String, Vec<CatalogStatsPoint>>,
    /// (src id, dst id) → edge.
    edges: std::collections::HashMap<(String, String), CatalogLineageEdge>,
    /// pipeline name → latest config snapshot (#374). Latest-wins.
    config_snapshots: std::collections::HashMap<String, super::catalog::ConfigSnapshot>,
}

pub struct MemoryHistory {
    runs: DashMap<String, RunRecord>,
    idem: DashMap<String, IdemEntry>,
    /// Bounded, newest-at-back ring of audit records (RBAC, #205).
    audit: Mutex<VecDeque<AuditEntry>>,
    /// Data Movement Catalog (#279). Ephemeral like everything else here.
    catalog: Mutex<CatalogState>,
    /// Pipeline-template registry (#444): id → version → record. One `Mutex`
    /// keeps version assignment atomic (read-max-then-insert).
    templates: Mutex<std::collections::HashMap<String, BTreeMap<u32, templates::TemplateRecord>>>,
    /// Named channel pointers (#444): template id → tag → version. Guarded by
    /// the same lock as `templates` would be if it mattered; a separate `Mutex` is
    /// fine because a tag is only ever written after its version exists.
    template_tags: Mutex<std::collections::HashMap<String, BTreeMap<String, u32>>>,
    /// Append-only launch log per template, newest first. Source of truth for
    /// `stable` / `previous` and for the derived template status.
    template_launches: Mutex<std::collections::HashMap<String, Vec<templates::LaunchRecord>>>,
    /// Deprecation markers — the only *stored* part of the lifecycle status.
    template_deprecations: Mutex<std::collections::HashMap<String, templates::DeprecationRecord>>,
    /// Retention window for idempotency claims (separate from run retention).
    idem_retention: Duration,
}

impl MemoryHistory {
    pub fn new(idem_retention: Duration) -> Self {
        Self {
            runs: DashMap::new(),
            idem: DashMap::new(),
            audit: Mutex::new(VecDeque::new()),
            catalog: Mutex::new(CatalogState::default()),
            templates: Mutex::new(std::collections::HashMap::new()),
            template_tags: Mutex::new(std::collections::HashMap::new()),
            template_launches: Mutex::new(std::collections::HashMap::new()),
            template_deprecations: Mutex::new(std::collections::HashMap::new()),
            idem_retention,
        }
    }
}

/// True when `claimed_at` is older than `window` relative to `now`. A claim
/// timestamped in the future (clock skew) is treated as *not* expired.
fn is_expired(claimed_at: DateTime<Utc>, now: DateTime<Utc>, window: Duration) -> bool {
    now.signed_duration_since(claimed_at)
        .to_std()
        .map(|age| age >= window)
        .unwrap_or(false)
}

#[async_trait]
impl RunHistory for MemoryHistory {
    async fn claim_idempotency(
        &self,
        key: &str,
        fingerprint: &str,
        run_id: &str,
        window: Duration,
    ) -> Result<Claim, HistoryError> {
        use dashmap::mapref::entry::Entry;
        let now = Utc::now();
        // Holding the entry locks the shard, so claim is atomic under contention.
        match self.idem.entry(key.to_string()) {
            Entry::Occupied(mut e) => {
                let expired = is_expired(e.get().claimed_at, now, window);
                if expired {
                    e.insert(IdemEntry {
                        run_id: run_id.to_string(),
                        fingerprint: fingerprint.to_string(),
                        claimed_at: now,
                    });
                    Ok(Claim::Fresh)
                } else if e.get().fingerprint == fingerprint {
                    Ok(Claim::Replay(e.get().run_id.clone()))
                } else {
                    Ok(Claim::Conflict)
                }
            }
            Entry::Vacant(v) => {
                v.insert(IdemEntry {
                    run_id: run_id.to_string(),
                    fingerprint: fingerprint.to_string(),
                    claimed_at: now,
                });
                Ok(Claim::Fresh)
            }
        }
    }

    async fn upsert(&self, rec: &RunRecord) -> Result<(), HistoryError> {
        self.runs.insert(rec.run_id.clone(), rec.clone());
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<RunRecord>, HistoryError> {
        Ok(self.runs.get(id).map(|r| r.clone()))
    }

    async fn list(&self, filter: &ListFilter) -> Result<ListPage, HistoryError> {
        let mut rows: Vec<RunRecord> = self
            .runs
            .iter()
            .map(|r| r.clone())
            .filter(|r| filter.status.is_none_or(|s| r.status == s))
            .filter(|r| {
                filter
                    .name
                    .as_deref()
                    .is_none_or(|n| r.name.as_deref() == Some(n))
            })
            .filter(|r| filter.since.is_none_or(|t| r.submitted_at >= t))
            .filter(|r| filter.until.is_none_or(|t| r.submitted_at <= t))
            .collect();
        // (submitted_at DESC, run_id DESC)
        rows.sort_by(|a, b| {
            b.submitted_at
                .cmp(&a.submitted_at)
                .then_with(|| b.run_id.cmp(&a.run_id))
        });
        // Cursor = last run_id seen on the previous page; skip past it.
        if let Some(cursor) = &filter.cursor
            && let Some(pos) = rows.iter().position(|r| &r.run_id == cursor)
        {
            rows.drain(..=pos);
        }
        let limit = filter.limit.max(1);
        let next_cursor = if rows.len() > limit {
            Some(rows[limit - 1].run_id.clone())
        } else {
            None
        };
        rows.truncate(limit);
        Ok(ListPage {
            runs: rows,
            next_cursor,
        })
    }

    async fn delete(&self, id: &str) -> Result<DeleteOutcome, HistoryError> {
        let Some(rec) = self.runs.get(id).map(|r| r.clone()) else {
            return Ok(DeleteOutcome::NotFound);
        };
        if !rec.status.is_terminal() {
            return Ok(DeleteOutcome::StillRunning);
        }
        self.runs.remove(id);
        // Also drop this run's idempotency claim so a replay of the key starts a
        // fresh run instead of 404-ing on the now-deleted record until the claim
        // self-expires (#146 M8). Only remove it if the claim still points at
        // THIS run — a newer run may have re-claimed the key after expiry.
        if let Some(key) = rec.idempotency_key.as_deref() {
            self.idem.remove_if(key, |_, e| e.run_id == id);
        }
        Ok(DeleteOutcome::Deleted)
    }

    async fn purge_expired(&self, retain_for: Duration) -> Result<usize, HistoryError> {
        let now = Utc::now();
        let before = self.runs.len();
        self.runs.retain(|_, r| {
            !r.status.is_terminal()
                || r.finished_at
                    .map(|f| !is_expired(f, now, retain_for))
                    .unwrap_or(true)
        });
        // Also drop stale idempotency claims so the map stays bounded.
        self.idem
            .retain(|_, e| !is_expired(e.claimed_at, now, self.idem_retention));
        // Trim audit records older than the run-retention window.
        if let Ok(mut ring) = self.audit.lock() {
            ring.retain(|e| !is_expired(e.timestamp, now, retain_for));
        }
        Ok(before.saturating_sub(self.runs.len()))
    }

    async fn record_audit(&self, entry: &AuditEntry) -> Result<(), HistoryError> {
        let mut ring = self
            .audit
            .lock()
            .map_err(|_| HistoryError::Backend("audit ring lock poisoned".into()))?;
        ring.push_back(entry.clone());
        while ring.len() > AUDIT_RING_CAP {
            ring.pop_front();
        }
        Ok(())
    }

    async fn list_audit(&self, filter: &AuditFilter) -> Result<Vec<AuditEntry>, HistoryError> {
        let ring = self
            .audit
            .lock()
            .map_err(|_| HistoryError::Backend("audit ring lock poisoned".into()))?;
        let mut rows: Vec<AuditEntry> = ring
            .iter()
            .filter(|e| filter.principal.as_deref().is_none_or(|p| e.principal == p))
            .filter(|e| filter.action.as_deref().is_none_or(|a| e.action == a))
            .filter(|e| filter.since.is_none_or(|t| e.timestamp >= t))
            .filter(|e| filter.until.is_none_or(|t| e.timestamp <= t))
            .cloned()
            .collect();
        // Newest first (timestamp DESC, id DESC).
        rows.sort_by(|a, b| b.timestamp.cmp(&a.timestamp).then_with(|| b.id.cmp(&a.id)));
        rows.truncate(filter.limit.max(1));
        Ok(rows)
    }

    async fn recover_orphans(&self) -> Result<usize, HistoryError> {
        Ok(0)
    }

    async fn cancel_pending(&self, run_id: &str) -> Result<bool, HistoryError> {
        use crate::serve::history::RunStatus;
        if let Some(mut r) = self.runs.get_mut(run_id)
            && r.status == RunStatus::Pending
        {
            r.status = RunStatus::Cancelled;
            r.finished_at = Some(Utc::now());
            return Ok(true);
        }
        Ok(false)
    }

    // ── Data Movement Catalog (#279) ─────────────────────────────────────────

    async fn catalog_record(&self, update: &CatalogUpdate) -> Result<(), HistoryError> {
        let lock_err = |_| HistoryError::Backend("catalog lock poisoned".into());
        let mut cat = self.catalog.lock().map_err(lock_err)?;
        for obs in update.sources.iter().chain(std::iter::once(&update.sink)) {
            let id = catalog::dataset_id(&obs.uri);
            let (ds, new_version) = catalog::apply_observation(
                cat.datasets.get(&id),
                obs,
                &update.run_id,
                &update.pipeline,
                &update.row,
                update.recorded_at,
            );
            if let Some(v) = new_version {
                cat.schema_versions.entry(id.clone()).or_default().push(v);
            }
            let points = cat.stats.entry(id.clone()).or_default();
            points.push(CatalogStatsPoint {
                recorded_at: update.recorded_at,
                run_id: update.run_id.clone(),
                records: obs.records,
            });
            if points.len() > catalog::STATS_RETAIN {
                let drop_n = points.len() - catalog::STATS_RETAIN;
                points.drain(..drop_n);
            }
            cat.datasets.insert(id, ds);
        }
        // One edge per input dataset — a merge/join sink has several (#459).
        for source in &update.sources {
            let key = (
                catalog::dataset_id(&source.uri),
                catalog::dataset_id(&update.sink.uri),
            );
            let edge = catalog::apply_edge(cat.edges.get(&key), update, source);
            cat.edges.insert(key, edge);
        }
        Ok(())
    }

    async fn catalog_list_datasets(
        &self,
        filter: &CatalogListFilter,
    ) -> Result<CatalogDatasetPage, HistoryError> {
        let cat = self
            .catalog
            .lock()
            .map_err(|_| HistoryError::Backend("catalog lock poisoned".into()))?;
        Ok(catalog::filter_datasets(
            cat.datasets.values().cloned().collect(),
            filter,
        ))
    }

    async fn catalog_get_dataset(
        &self,
        id: &str,
    ) -> Result<Option<CatalogDatasetDetail>, HistoryError> {
        let cat = self
            .catalog
            .lock()
            .map_err(|_| HistoryError::Backend("catalog lock poisoned".into()))?;
        let Some(dataset) = cat.datasets.get(id).cloned() else {
            return Ok(None);
        };
        let schema_timeline = cat.schema_versions.get(id).cloned().unwrap_or_default();
        let mut stats: Vec<CatalogStatsPoint> = cat.stats.get(id).cloned().unwrap_or_default();
        stats.reverse(); // newest first
        stats.truncate(catalog::STATS_DETAIL_LIMIT);
        let upstream = cat
            .edges
            .values()
            .filter(|e| e.dst_id == id)
            .cloned()
            .collect();
        let downstream = cat
            .edges
            .values()
            .filter(|e| e.src_id == id)
            .cloned()
            .collect();
        Ok(Some(CatalogDatasetDetail {
            dataset,
            schema_timeline,
            stats,
            upstream,
            downstream,
        }))
    }

    async fn catalog_lineage(
        &self,
        root: Option<&str>,
        depth: u32,
    ) -> Result<Vec<CatalogLineageEdge>, HistoryError> {
        let cat = self
            .catalog
            .lock()
            .map_err(|_| HistoryError::Backend("catalog lock poisoned".into()))?;
        let mut edges: Vec<CatalogLineageEdge> = cat.edges.values().cloned().collect();
        // Stable order for pagination-free consumers (newest activity first).
        edges.sort_by(|a, b| {
            b.last_seen
                .cmp(&a.last_seen)
                .then_with(|| (&a.src_id, &a.dst_id).cmp(&(&b.src_id, &b.dst_id)))
        });
        Ok(catalog::lineage_slice(edges, root, depth))
    }

    async fn catalog_record_config_snapshot(
        &self,
        snapshot: &catalog::ConfigSnapshot,
    ) -> Result<(), HistoryError> {
        let mut cat = self
            .catalog
            .lock()
            .map_err(|_| HistoryError::Backend("catalog lock poisoned".into()))?;
        cat.config_snapshots
            .insert(snapshot.pipeline.clone(), snapshot.clone());
        Ok(())
    }

    async fn catalog_last_config_snapshot(
        &self,
        pipeline: &str,
    ) -> Result<Option<catalog::ConfigSnapshot>, HistoryError> {
        let cat = self
            .catalog
            .lock()
            .map_err(|_| HistoryError::Backend("catalog lock poisoned".into()))?;
        Ok(cat.config_snapshots.get(pipeline).cloned())
    }

    // ── Pipeline-template registry (#444) ────────────────────────────────────

    async fn template_register(
        &self,
        draft: &templates::TemplateDraft,
    ) -> Result<templates::TemplateRecord, HistoryError> {
        let mut store = self
            .templates
            .lock()
            .map_err(|_| HistoryError::Backend("template lock poisoned".into()))?;
        let id = draft.id.to_string();
        let versions = store.entry(id.clone()).or_default();
        let next = versions.keys().copied().max().unwrap_or(0) + 1;
        let record = templates::TemplateRecord {
            id,
            version: next,
            name: draft.name.clone(),
            description: draft.description.clone(),
            body: draft.body.clone(),
            format: draft.format,
            params: draft.params.clone(),
            created_at: Utc::now(),
            created_by: draft.created_by.clone(),
        };
        versions.insert(next, record.clone());
        for stale in templates::versions_to_prune(versions.keys().copied().collect()) {
            versions.remove(&stale);
        }
        Ok(record)
    }

    async fn template_get(
        &self,
        id: &str,
        version: Option<u32>,
    ) -> Result<Option<templates::TemplateRecord>, HistoryError> {
        let store = self
            .templates
            .lock()
            .map_err(|_| HistoryError::Backend("template lock poisoned".into()))?;
        let Some(versions) = store.get(id) else {
            return Ok(None);
        };
        let picked = match version {
            Some(v) => versions.get(&v),
            None => versions
                .keys()
                .copied()
                .max()
                .and_then(|v| versions.get(&v)),
        };
        Ok(picked.cloned())
    }

    async fn template_list(&self) -> Result<Vec<templates::TemplateSummary>, HistoryError> {
        let store = self
            .templates
            .lock()
            .map_err(|_| HistoryError::Backend("template lock poisoned".into()))?;
        let all: Vec<templates::TemplateRecord> =
            store.values().flat_map(|v| v.values().cloned()).collect();
        Ok(templates::latest_per_id(all))
    }

    async fn template_versions(&self, id: &str) -> Result<Vec<u32>, HistoryError> {
        let store = self
            .templates
            .lock()
            .map_err(|_| HistoryError::Backend("template lock poisoned".into()))?;
        let mut versions: Vec<u32> = store
            .get(id)
            .map(|v| v.keys().copied().collect())
            .unwrap_or_default();
        versions.sort_unstable_by(|a, b| b.cmp(a));
        Ok(versions)
    }

    async fn template_delete(&self, id: &str, version: Option<u32>) -> Result<usize, HistoryError> {
        let mut store = self
            .templates
            .lock()
            .map_err(|_| HistoryError::Backend("template lock poisoned".into()))?;
        let mut tags = self
            .template_tags
            .lock()
            .map_err(|_| HistoryError::Backend("template tag lock poisoned".into()))?;
        let mut launches = self
            .template_launches
            .lock()
            .map_err(|_| HistoryError::Backend("template launch lock poisoned".into()))?;
        match version {
            None => {
                tags.remove(id);
                launches.remove(id);
                self.template_deprecations
                    .lock()
                    .map_err(|_| {
                        HistoryError::Backend("template deprecation lock poisoned".into())
                    })?
                    .remove(id);
                Ok(store.remove(id).map(|v| v.len()).unwrap_or(0))
            }
            Some(v) => {
                let Some(versions) = store.get_mut(id) else {
                    return Ok(0);
                };
                let removed = versions.remove(&v).is_some() as usize;
                if versions.is_empty() {
                    store.remove(id);
                    tags.remove(id);
                    launches.remove(id);
                } else {
                    if let Some(t) = tags.get_mut(id) {
                        // A channel must never dangle at a deleted version.
                        t.retain(|_, pointed| *pointed != v);
                        if t.is_empty() {
                            tags.remove(id);
                        }
                    }
                    // Likewise the launch log: a `stable` / `previous` pointer must
                    // never resolve to a version that no longer exists.
                    if let Some(log) = launches.get_mut(id) {
                        log.retain(|l| l.version != v);
                        if log.is_empty() {
                            launches.remove(id);
                        }
                    }
                }
                Ok(removed)
            }
        }
    }

    async fn template_set_tag(
        &self,
        id: &str,
        tag: &str,
        version: u32,
    ) -> Result<(), HistoryError> {
        let mut tags = self
            .template_tags
            .lock()
            .map_err(|_| HistoryError::Backend("template tag lock poisoned".into()))?;
        tags.entry(id.to_string())
            .or_default()
            .insert(tag.to_string(), version);
        Ok(())
    }

    async fn template_tags(&self, id: &str) -> Result<BTreeMap<String, u32>, HistoryError> {
        let tags = self
            .template_tags
            .lock()
            .map_err(|_| HistoryError::Backend("template tag lock poisoned".into()))?;
        Ok(tags.get(id).cloned().unwrap_or_default())
    }

    async fn template_delete_tag(&self, id: &str, tag: &str) -> Result<bool, HistoryError> {
        let mut tags = self
            .template_tags
            .lock()
            .map_err(|_| HistoryError::Backend("template tag lock poisoned".into()))?;
        let Some(t) = tags.get_mut(id) else {
            return Ok(false);
        };
        let existed = t.remove(tag).is_some();
        if t.is_empty() {
            tags.remove(id);
        }
        Ok(existed)
    }

    async fn template_launch(
        &self,
        id: &str,
        version: u32,
        launched_by: Option<&str>,
    ) -> Result<Option<u32>, HistoryError> {
        let mut launches = self
            .template_launches
            .lock()
            .map_err(|_| HistoryError::Backend("template launch lock poisoned".into()))?;
        let log = launches.entry(id.to_string()).or_default();
        // Re-launching what is already stable is a no-op: appending would make
        // `previous` a duplicate of `stable` and destroy the rollback target.
        if templates::stable_version(log) == Some(version) {
            return Ok(None);
        }
        let seq = log.first().map(|l| l.seq).unwrap_or(0) + 1;
        log.insert(
            0,
            templates::LaunchRecord {
                seq,
                version,
                launched_at: Utc::now(),
                launched_by: launched_by.map(str::to_string),
            },
        );
        Ok(Some(seq))
    }

    async fn template_launches(
        &self,
        id: &str,
    ) -> Result<Vec<templates::LaunchRecord>, HistoryError> {
        let launches = self
            .template_launches
            .lock()
            .map_err(|_| HistoryError::Backend("template launch lock poisoned".into()))?;
        Ok(launches.get(id).cloned().unwrap_or_default())
    }

    async fn template_set_deprecation(
        &self,
        id: &str,
        record: Option<&templates::DeprecationRecord>,
    ) -> Result<(), HistoryError> {
        let mut deprecations = self
            .template_deprecations
            .lock()
            .map_err(|_| HistoryError::Backend("template deprecation lock poisoned".into()))?;
        match record {
            Some(r) => {
                deprecations.insert(id.to_string(), r.clone());
            }
            None => {
                deprecations.remove(id);
            }
        }
        Ok(())
    }

    async fn template_deprecation(
        &self,
        id: &str,
    ) -> Result<Option<templates::DeprecationRecord>, HistoryError> {
        let deprecations = self
            .template_deprecations
            .lock()
            .map_err(|_| HistoryError::Backend("template deprecation lock poisoned".into()))?;
        Ok(deprecations.get(id).cloned())
    }

    fn degraded(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::RunStatus;
    use std::collections::BTreeMap;

    fn rec(id: &str, status: RunStatus, submitted: DateTime<Utc>) -> RunRecord {
        let mut r = RunRecord::queued(id.into(), None, BTreeMap::new(), None, submitted);
        r.status = status;
        if status.is_terminal() {
            r.finished_at = Some(submitted);
        }
        r
    }

    #[tokio::test]
    async fn upsert_then_get_roundtrips() {
        let h = MemoryHistory::new(Duration::from_secs(60));
        let r = rec("a", RunStatus::Queued, Utc::now());
        h.upsert(&r).await.unwrap();
        assert_eq!(h.get("a").await.unwrap().unwrap().run_id, "a");
        assert!(h.get("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn idempotency_fresh_replay_conflict() {
        let h = MemoryHistory::new(Duration::from_secs(60));
        let w = Duration::from_secs(60);
        assert_eq!(
            h.claim_idempotency("k", "fp1", "run1", w).await.unwrap(),
            Claim::Fresh
        );
        // Same key + same fingerprint → replay the first run id.
        assert_eq!(
            h.claim_idempotency("k", "fp1", "run2", w).await.unwrap(),
            Claim::Replay("run1".into())
        );
        // Same key + different fingerprint → conflict.
        assert_eq!(
            h.claim_idempotency("k", "fp2", "run3", w).await.unwrap(),
            Claim::Conflict
        );
    }

    #[tokio::test]
    async fn expired_claim_is_reclaimable() {
        let h = MemoryHistory::new(Duration::from_secs(60));
        // Zero window → any prior claim is immediately expired.
        let w = Duration::ZERO;
        assert_eq!(
            h.claim_idempotency("k", "fp1", "run1", w).await.unwrap(),
            Claim::Fresh
        );
        assert_eq!(
            h.claim_idempotency("k", "fp2", "run2", w).await.unwrap(),
            Claim::Fresh
        );
    }

    #[tokio::test]
    async fn delete_respects_terminal_state() {
        let h = MemoryHistory::new(Duration::from_secs(60));
        h.upsert(&rec("run", RunStatus::Running, Utc::now()))
            .await
            .unwrap();
        assert_eq!(h.delete("run").await.unwrap(), DeleteOutcome::StillRunning);
        assert_eq!(h.delete("nope").await.unwrap(), DeleteOutcome::NotFound);
        h.upsert(&rec("run", RunStatus::Completed, Utc::now()))
            .await
            .unwrap();
        assert_eq!(h.delete("run").await.unwrap(), DeleteOutcome::Deleted);
        assert!(h.get("run").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_also_removes_matching_idem_claim() {
        // M8 (#146): deleting a run must drop its idempotency claim, so a later
        // replay of the key starts a fresh run instead of 404-ing on the
        // now-missing record until the claim self-expires.
        let h = MemoryHistory::new(Duration::from_secs(3600));
        let w = Duration::from_secs(3600);
        assert_eq!(
            h.claim_idempotency("k", "fp", "r1", w).await.unwrap(),
            Claim::Fresh
        );
        let mut r = RunRecord::queued(
            "r1".into(),
            None,
            BTreeMap::new(),
            Some("k".into()),
            Utc::now(),
        );
        r.status = RunStatus::Completed;
        r.finished_at = Some(Utc::now());
        h.upsert(&r).await.unwrap();

        assert_eq!(h.delete("r1").await.unwrap(), DeleteOutcome::Deleted);
        // The key is free again → fresh run, not a replay of the deleted one.
        assert_eq!(
            h.claim_idempotency("k", "fp", "r2", w).await.unwrap(),
            Claim::Fresh
        );
    }

    #[tokio::test]
    async fn delete_keeps_claim_owned_by_a_newer_run() {
        // Guard: deleting an OLD run must not remove a claim a NEWER run owns.
        let h = MemoryHistory::new(Duration::from_secs(3600));
        h.claim_idempotency("k", "fp", "r1", Duration::from_secs(3600))
            .await
            .unwrap();
        // r2 re-claims the key (force the prior claim stale with a zero window).
        assert_eq!(
            h.claim_idempotency("k", "fp", "r2", Duration::ZERO)
                .await
                .unwrap(),
            Claim::Fresh
        );
        let mut r1 = RunRecord::queued(
            "r1".into(),
            None,
            BTreeMap::new(),
            Some("k".into()),
            Utc::now(),
        );
        r1.status = RunStatus::Completed;
        r1.finished_at = Some(Utc::now());
        h.upsert(&r1).await.unwrap();
        assert_eq!(h.delete("r1").await.unwrap(), DeleteOutcome::Deleted);
        // The claim still belongs to r2.
        assert_eq!(
            h.claim_idempotency("k", "fp", "r3", Duration::from_secs(3600))
                .await
                .unwrap(),
            Claim::Replay("r2".into())
        );
    }

    #[tokio::test]
    async fn list_orders_desc_and_paginates() {
        let h = MemoryHistory::new(Duration::from_secs(60));
        let t0 = Utc::now();
        for (i, id) in ["a", "b", "c"].iter().enumerate() {
            h.upsert(&rec(
                id,
                RunStatus::Completed,
                t0 + chrono::Duration::seconds(i as i64),
            ))
            .await
            .unwrap();
        }
        // Newest first → c, b, a. Page size 2.
        let page = h
            .list(&ListFilter {
                limit: 2,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            page.runs
                .iter()
                .map(|r| r.run_id.clone())
                .collect::<Vec<_>>(),
            vec!["c", "b"]
        );
        assert_eq!(page.next_cursor.as_deref(), Some("b"));
        // Next page from the cursor → a.
        let page2 = h
            .list(&ListFilter {
                limit: 2,
                cursor: Some("b".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            page2
                .runs
                .iter()
                .map(|r| r.run_id.clone())
                .collect::<Vec<_>>(),
            vec!["a"]
        );
        assert!(page2.next_cursor.is_none());
    }

    #[tokio::test]
    async fn list_filters_by_status_and_name() {
        let h = MemoryHistory::new(Duration::from_secs(60));
        let mut r = rec("x", RunStatus::Failed, Utc::now());
        r.name = Some("nightly".into());
        h.upsert(&r).await.unwrap();
        h.upsert(&rec("y", RunStatus::Completed, Utc::now()))
            .await
            .unwrap();
        let only_failed = h
            .list(&ListFilter {
                status: Some(RunStatus::Failed),
                limit: 50,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(only_failed.runs.len(), 1);
        assert_eq!(only_failed.runs[0].run_id, "x");
        // Name filter also works.
        let by_name = h
            .list(&ListFilter {
                name: Some("nightly".into()),
                limit: 50,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(by_name.runs.len(), 1);
        assert_eq!(by_name.runs[0].run_id, "x");
    }

    #[tokio::test]
    async fn audit_record_list_filter_and_purge() {
        use crate::serve::history::{AuditEntry, AuditFilter};
        let h = MemoryHistory::new(Duration::from_secs(60));
        let now = Utc::now();
        let entry =
            |id: &str, principal: &str, action: &str, result: &str, ts: DateTime<Utc>| AuditEntry {
                id: id.into(),
                timestamp: ts,
                principal: principal.into(),
                role: "admin".into(),
                action: action.into(),
                run_id: None,
                config_fingerprint: None,
                source_ip: None,
                result: result.into(),
            };
        h.record_audit(&entry(
            "1",
            "alice",
            "run.submit",
            "ok",
            now - chrono::Duration::seconds(2),
        ))
        .await
        .unwrap();
        h.record_audit(&entry(
            "2",
            "bob",
            "run.submit",
            "denied",
            now - chrono::Duration::seconds(1),
        ))
        .await
        .unwrap();
        h.record_audit(&entry("3", "alice", "run.cancel", "ok", now))
            .await
            .unwrap();

        // Newest first, no filter.
        let all = h
            .list_audit(&AuditFilter {
                limit: 50,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].id, "3", "newest first");

        // Filter by principal + action.
        let alice = h
            .list_audit(&AuditFilter {
                principal: Some("alice".into()),
                limit: 50,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(alice.len(), 2);
        assert!(alice.iter().all(|e| e.principal == "alice"));

        let denied = h
            .list_audit(&AuditFilter {
                action: Some("run.submit".into()),
                limit: 50,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(denied.len(), 2);

        // Limit is honoured.
        let one = h
            .list_audit(&AuditFilter {
                limit: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(one.len(), 1);

        // purge_expired(0) drops all audit records (every ts is "expired").
        h.purge_expired(Duration::ZERO).await.unwrap();
        let after = h
            .list_audit(&AuditFilter {
                limit: 50,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(after.is_empty(), "audit purge should clear expired entries");
    }

    fn catalog_update(src: &str, dst: &str, schema: Option<serde_json::Value>) -> CatalogUpdate {
        use crate::serve::history::catalog::{DatasetObservation, DatasetRole};
        CatalogUpdate {
            run_id: "r1".into(),
            pipeline: "p".into(),
            row: "default".into(),
            recorded_at: Utc::now(),
            sources: vec![DatasetObservation {
                uri: src.into(),
                kind: "csv".into(),
                role: DatasetRole::Source,
                schema: schema.clone(),
                records: 10,
            }],
            sink: DatasetObservation {
                uri: dst.into(),
                kind: "jsonl".into(),
                role: DatasetRole::Sink,
                schema,
                records: 10,
            },
            column_lineage: None,
        }
    }

    #[tokio::test]
    async fn config_snapshot_roundtrips_latest_wins() {
        use crate::serve::history::catalog::ConfigSnapshot;
        use std::collections::BTreeMap;
        let h = MemoryHistory::new(Duration::from_secs(60));
        assert!(
            h.catalog_last_config_snapshot("p").await.unwrap().is_none(),
            "no snapshot before any record"
        );
        let mk = |ver: &str| ConfigSnapshot {
            pipeline: "p".into(),
            recorded_at: Utc::now(),
            faucet_version: ver.into(),
            rows: BTreeMap::new(),
        };
        h.catalog_record_config_snapshot(&mk("1")).await.unwrap();
        h.catalog_record_config_snapshot(&mk("2")).await.unwrap();
        let got = h.catalog_last_config_snapshot("p").await.unwrap().unwrap();
        assert_eq!(got.faucet_version, "2", "latest-wins upsert");
        assert!(
            h.catalog_last_config_snapshot("other")
                .await
                .unwrap()
                .is_none(),
            "snapshots are keyed per pipeline"
        );
    }

    #[tokio::test]
    async fn catalog_record_accumulates_datasets_edges_and_timeline() {
        use serde_json::json;
        let h = MemoryHistory::new(Duration::from_secs(60));
        let schema_v1 = json!({"type": "object", "properties": {"id": {"type": "integer"}}});
        let schema_v2 = json!({"type": "object", "properties": {"id": {"type": "integer"}, "email": {"type": "string"}}});

        h.catalog_record(&catalog_update(
            "csv://./in.csv",
            "jsonl://./out.jsonl",
            Some(schema_v1.clone()),
        ))
        .await
        .unwrap();
        // Same schema again → no new version.
        h.catalog_record(&catalog_update(
            "csv://./in.csv",
            "jsonl://./out.jsonl",
            Some(schema_v1),
        ))
        .await
        .unwrap();
        // Changed schema → second version with a diff.
        h.catalog_record(&catalog_update(
            "csv://./in.csv",
            "jsonl://./out.jsonl",
            Some(schema_v2),
        ))
        .await
        .unwrap();

        let page = h
            .catalog_list_datasets(&CatalogListFilter {
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.datasets.len(), 2, "source + sink datasets");

        let src_id = catalog::dataset_id("csv://./in.csv");
        let detail = h.catalog_get_dataset(&src_id).await.unwrap().unwrap();
        assert_eq!(detail.dataset.runs, 3);
        assert_eq!(detail.dataset.total_records, 30);
        assert_eq!(
            detail.schema_timeline.len(),
            2,
            "identical schema deduped; change appended"
        );
        assert!(detail.schema_timeline[0].diff.is_none());
        assert!(detail.schema_timeline[1].diff.is_some());
        assert_eq!(detail.stats.len(), 3);
        assert_eq!(detail.downstream.len(), 1);
        assert!(detail.upstream.is_empty());
        assert_eq!(detail.downstream[0].runs, 3);

        // Lineage: one edge, whole graph == rooted graph.
        let all = h.catalog_lineage(None, 5).await.unwrap();
        assert_eq!(all.len(), 1);
        let rooted = h.catalog_lineage(Some(&src_id), 3).await.unwrap();
        assert_eq!(rooted.len(), 1);
        assert!(
            h.catalog_lineage(Some("missing"), 3)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(h.catalog_get_dataset("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn purge_drops_expired_terminal_runs() {
        let h = MemoryHistory::new(Duration::from_secs(60));
        h.upsert(&rec(
            "old",
            RunStatus::Completed,
            Utc::now() - chrono::Duration::seconds(10),
        ))
        .await
        .unwrap();
        h.upsert(&rec("live", RunStatus::Running, Utc::now()))
            .await
            .unwrap();
        // retain_for = 0 → every terminal record is expired; running is kept.
        let removed = h.purge_expired(Duration::ZERO).await.unwrap();
        assert_eq!(removed, 1);
        assert!(h.get("old").await.unwrap().is_none());
        assert!(h.get("live").await.unwrap().is_some());
    }
}
