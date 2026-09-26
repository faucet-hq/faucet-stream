//! Degradation wrapper around a persistent run-history backend (Phase 5 of
//! #127). While the primary (Postgres/SQLite) backend is healthy, every call
//! goes to it. The first time a call errors — or if the backend could not be
//! reached at startup — the wrapper flips to **degraded**: it logs once, sets
//! the `faucet_serve_history_degraded` gauge, surfaces `503` on `/readyz` via
//! [`RunHistory::degraded`], and serves all subsequent calls from an in-memory
//! backend so the server stays up (spec §11). Data already in the primary is not
//! migrated — degraded mode is a stay-alive fallback, not a replica.

use super::memory::MemoryHistory;
use super::{
    AuditEntry, AuditFilter, Claim, DeleteOutcome, HistoryError, InstanceHeartbeat, InstanceRecord,
    ListFilter, ListPage, ReclaimReport, RunHistory, RunRecord, RunStatus,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

pub struct FallbackHistory {
    /// Persistent backend; `None` when it was unreachable at startup.
    primary: Option<Box<dyn RunHistory>>,
    fallback: MemoryHistory,
    degraded: AtomicBool,
    /// `"postgres"` / `"sqlite"` — for log + metric context.
    label: &'static str,
}

impl FallbackHistory {
    /// Wrap a healthy primary backend.
    pub fn healthy(
        primary: Box<dyn RunHistory>,
        idem_retention: Duration,
        label: &'static str,
    ) -> Self {
        Self {
            primary: Some(primary),
            fallback: MemoryHistory::new(idem_retention),
            degraded: AtomicBool::new(false),
            label,
        }
    }

    /// Start already degraded: the primary backend was unreachable at startup.
    pub fn degraded_at_startup(idem_retention: Duration, label: &'static str) -> Self {
        crate::serve::metrics::set_history_degraded(true);
        Self {
            primary: None,
            fallback: MemoryHistory::new(idem_retention),
            degraded: AtomicBool::new(true),
            label,
        }
    }

    fn is_degraded(&self) -> bool {
        self.primary.is_none() || self.degraded.load(Ordering::Acquire)
    }

    /// Record a primary-backend failure and flip to degraded (log + metric once).
    fn trip(&self, err: &HistoryError) {
        if !self.degraded.swap(true, Ordering::AcqRel) {
            tracing::error!(
                backend = self.label,
                error = %err,
                "run-history backend failed; falling back to in-memory store (DEGRADED — \
                 persisted run records are no longer served; /readyz now reports 503)"
            );
            crate::serve::metrics::set_history_degraded(true);
        }
    }
}

/// Run `$call` against the primary backend; on the first error, trip into
/// degraded mode and re-run it against the in-memory fallback. Once degraded,
/// skip the primary entirely.
macro_rules! via {
    ($self:ident, $primary:ident => $pcall:expr, $fb:ident => $fcall:expr) => {{
        if !$self.is_degraded()
            && let Some($primary) = $self.primary.as_ref()
        {
            match $pcall.await {
                Ok(v) => return Ok(v),
                Err(e) => $self.trip(&e),
            }
        }
        let $fb = &$self.fallback;
        $fcall.await
    }};
}

#[async_trait]
impl RunHistory for FallbackHistory {
    async fn claim_idempotency(
        &self,
        key: &str,
        fingerprint: &str,
        run_id: &str,
        window: Duration,
    ) -> Result<Claim, HistoryError> {
        // Idempotency is correctness-critical, so it does NOT use the generic
        // `via!` fall-through. While the primary is healthy it is the
        // authoritative claim store; its first error trips degraded.
        if !self.is_degraded()
            && let Some(p) = self.primary.as_ref()
        {
            match p.claim_idempotency(key, fingerprint, run_id, window).await {
                Ok(v) => return Ok(v),
                Err(e) => self.trip(&e),
            }
        }
        // Tripped from a healthy primary: the in-memory fallback cannot see
        // claims persisted to the primary before the trip, so serving a `Fresh`
        // from memory could duplicate a run the primary already claimed.
        // Fail closed — reject idempotent submissions while degraded rather than
        // risk a silent duplicate (#146 M5). A submission with no idempotency
        // key never reaches here. When the wrapper *started* degraded (`primary`
        // is `None` — no primary ever held a claim), the in-memory store is the
        // sole authoritative store, so its claims are safe to serve.
        if self.primary.is_some() {
            return Err(HistoryError::Degraded(
                "idempotency unavailable: the run-history backend is degraded; retry once it \
                 recovers, or resubmit without an idempotency key"
                    .into(),
            ));
        }
        self.fallback
            .claim_idempotency(key, fingerprint, run_id, window)
            .await
    }

    async fn upsert(&self, rec: &RunRecord) -> Result<(), HistoryError> {
        via!(self, p => p.upsert(rec), f => f.upsert(rec))
    }

    async fn get(&self, id: &str) -> Result<Option<RunRecord>, HistoryError> {
        via!(self, p => p.get(id), f => f.get(id))
    }

    async fn list(&self, filter: &ListFilter) -> Result<ListPage, HistoryError> {
        via!(self, p => p.list(filter), f => f.list(filter))
    }

    async fn delete(&self, id: &str) -> Result<DeleteOutcome, HistoryError> {
        via!(self, p => p.delete(id), f => f.delete(id))
    }

    async fn purge_expired(&self, retain_for: Duration) -> Result<usize, HistoryError> {
        via!(self, p => p.purge_expired(retain_for), f => f.purge_expired(retain_for))
    }

    async fn release_idempotency(&self, run_id: &str) -> Result<(), HistoryError> {
        via!(self, p => p.release_idempotency(run_id), f => f.release_idempotency(run_id))
    }

    async fn recover_orphans(&self) -> Result<usize, HistoryError> {
        via!(self, p => p.recover_orphans(), f => f.recover_orphans())
    }

    async fn renew_leases(&self) -> Result<usize, HistoryError> {
        via!(self, p => p.renew_leases(), f => f.renew_leases())
    }

    async fn claim_pending(&self, limit: usize) -> Result<Vec<RunRecord>, HistoryError> {
        via!(self, p => p.claim_pending(limit), f => f.claim_pending(limit))
    }
    async fn reclaim_orphans(&self, max_attempts: u32) -> Result<ReclaimReport, HistoryError> {
        via!(self, p => p.reclaim_orphans(max_attempts), f => f.reclaim_orphans(max_attempts))
    }
    async fn finalize_owned(&self, rec: &RunRecord) -> Result<bool, HistoryError> {
        via!(self, p => p.finalize_owned(rec), f => f.finalize_owned(rec))
    }
    async fn finalize_sharded_parent(
        &self,
        run_id: &str,
        status: RunStatus,
        finished_at: DateTime<Utc>,
        error: Option<String>,
    ) -> Result<bool, HistoryError> {
        via!(
            self,
            p => p.finalize_sharded_parent(run_id, status, finished_at, error.clone()),
            f => f.finalize_sharded_parent(run_id, status, finished_at, error.clone())
        )
    }
    async fn cancel_pending(&self, run_id: &str) -> Result<bool, HistoryError> {
        via!(self, p => p.cancel_pending(run_id), f => f.cancel_pending(run_id))
    }
    async fn request_cancel(&self, run_id: &str) -> Result<(), HistoryError> {
        via!(self, p => p.request_cancel(run_id), f => f.request_cancel(run_id))
    }
    async fn pending_cancellations(&self) -> Result<Vec<String>, HistoryError> {
        via!(self, p => p.pending_cancellations(), f => f.pending_cancellations())
    }
    async fn heartbeat_instance(&self, beat: &InstanceHeartbeat) -> Result<(), HistoryError> {
        via!(self, p => p.heartbeat_instance(beat), f => f.heartbeat_instance(beat))
    }
    async fn live_instances(&self, ttl: Duration) -> Result<Vec<InstanceRecord>, HistoryError> {
        via!(self, p => p.live_instances(ttl), f => f.live_instances(ttl))
    }

    // ── Source shards (Mode B, #230) ─────────────────────────────────────────

    async fn insert_shards(
        &self,
        run_id: &str,
        shards: &[crate::serve::history::ShardInsert],
    ) -> Result<usize, HistoryError> {
        via!(self, p => p.insert_shards(run_id, shards), f => f.insert_shards(run_id, shards))
    }
    async fn claim_shards(
        &self,
        limit: usize,
    ) -> Result<Vec<crate::serve::history::ClaimedShard>, HistoryError> {
        via!(self, p => p.claim_shards(limit), f => f.claim_shards(limit))
    }
    async fn renew_shard_leases(&self) -> Result<usize, HistoryError> {
        via!(self, p => p.renew_shard_leases(), f => f.renew_shard_leases())
    }
    async fn reclaim_shards(&self, max_attempts: u32) -> Result<ReclaimReport, HistoryError> {
        via!(self, p => p.reclaim_shards(max_attempts), f => f.reclaim_shards(max_attempts))
    }
    async fn finalize_shard(
        &self,
        run_id: &str,
        shard_id: &str,
        success: bool,
    ) -> Result<bool, HistoryError> {
        via!(self, p => p.finalize_shard(run_id, shard_id, success), f => f.finalize_shard(run_id, shard_id, success))
    }
    async fn shard_progress(
        &self,
        run_id: &str,
    ) -> Result<crate::serve::history::ShardProgress, HistoryError> {
        via!(self, p => p.shard_progress(run_id), f => f.shard_progress(run_id))
    }
    async fn pending_shard_cancellations(&self) -> Result<Vec<String>, HistoryError> {
        via!(self, p => p.pending_shard_cancellations(), f => f.pending_shard_cancellations())
    }
    async fn finalize_completed_sharded_parents(&self) -> Result<usize, HistoryError> {
        via!(self, p => p.finalize_completed_sharded_parents(), f => f.finalize_completed_sharded_parents())
    }

    async fn record_audit(&self, entry: &AuditEntry) -> Result<(), HistoryError> {
        via!(self, p => p.record_audit(entry), f => f.record_audit(entry))
    }
    async fn list_audit(&self, filter: &AuditFilter) -> Result<Vec<AuditEntry>, HistoryError> {
        via!(self, p => p.list_audit(filter), f => f.list_audit(filter))
    }

    // ── Persistent run logs (#529) ────────────────────────────────────────────

    async fn record_run_logs(
        &self,
        run_id: &str,
        lines: &[crate::serve::history::RunLogLine],
    ) -> Result<(), HistoryError> {
        via!(self, p => p.record_run_logs(run_id, lines), f => f.record_run_logs(run_id, lines))
    }
    async fn list_run_logs(
        &self,
        run_id: &str,
        after_seq: Option<u64>,
        limit: usize,
    ) -> Result<crate::serve::history::RunLogPage, HistoryError> {
        via!(self, p => p.list_run_logs(run_id, after_seq, limit), f => f.list_run_logs(run_id, after_seq, limit))
    }
    async fn purge_run_logs(&self, older_than: std::time::Duration) -> Result<usize, HistoryError> {
        via!(self, p => p.purge_run_logs(older_than), f => f.purge_run_logs(older_than))
    }

    // ── Data Movement Catalog (#279) ─────────────────────────────────────────

    async fn catalog_record(
        &self,
        update: &crate::serve::history::catalog::CatalogUpdate,
    ) -> Result<(), HistoryError> {
        via!(self, p => p.catalog_record(update), f => f.catalog_record(update))
    }
    async fn catalog_list_datasets(
        &self,
        filter: &crate::serve::history::catalog::CatalogListFilter,
    ) -> Result<crate::serve::history::catalog::CatalogDatasetPage, HistoryError> {
        via!(self, p => p.catalog_list_datasets(filter), f => f.catalog_list_datasets(filter))
    }
    async fn catalog_get_dataset(
        &self,
        id: &str,
    ) -> Result<Option<crate::serve::history::catalog::CatalogDatasetDetail>, HistoryError> {
        via!(self, p => p.catalog_get_dataset(id), f => f.catalog_get_dataset(id))
    }
    async fn catalog_lineage(
        &self,
        root: Option<&str>,
        depth: u32,
    ) -> Result<Vec<crate::serve::history::catalog::CatalogLineageEdge>, HistoryError> {
        via!(self, p => p.catalog_lineage(root, depth), f => f.catalog_lineage(root, depth))
    }
    async fn tenant_upsert(
        &self,
        tenant: &super::tenants::TenantRecord,
    ) -> Result<(), HistoryError> {
        via!(self, p => p.tenant_upsert(tenant), f => f.tenant_upsert(tenant))
    }
    async fn tenant_get(
        &self,
        id: &str,
    ) -> Result<Option<super::tenants::TenantRecord>, HistoryError> {
        via!(self, p => p.tenant_get(id), f => f.tenant_get(id))
    }
    async fn tenant_list(&self) -> Result<Vec<super::tenants::TenantRecord>, HistoryError> {
        via!(self, p => p.tenant_list(), f => f.tenant_list())
    }
    async fn tenant_delete(&self, id: &str) -> Result<bool, HistoryError> {
        via!(self, p => p.tenant_delete(id), f => f.tenant_delete(id))
    }
    async fn connection_upsert(
        &self,
        connection: &super::tenants::ConnectionRecord,
    ) -> Result<(), HistoryError> {
        via!(self, p => p.connection_upsert(connection), f => f.connection_upsert(connection))
    }
    async fn connection_get(
        &self,
        tenant: &str,
        name: &str,
    ) -> Result<Option<super::tenants::ConnectionRecord>, HistoryError> {
        via!(self, p => p.connection_get(tenant, name), f => f.connection_get(tenant, name))
    }
    async fn connection_list(
        &self,
        tenant: &str,
    ) -> Result<Vec<super::tenants::ConnectionRecord>, HistoryError> {
        via!(self, p => p.connection_list(tenant), f => f.connection_list(tenant))
    }
    async fn connection_delete(&self, tenant: &str, name: &str) -> Result<bool, HistoryError> {
        via!(self, p => p.connection_delete(tenant, name), f => f.connection_delete(tenant, name))
    }
    async fn connect_session_put(
        &self,
        session: &super::tenants::ConnectSession,
    ) -> Result<(), HistoryError> {
        via!(self, p => p.connect_session_put(session), f => f.connect_session_put(session))
    }
    async fn connect_session_take(
        &self,
        state: &str,
    ) -> Result<Option<super::tenants::ConnectSession>, HistoryError> {
        via!(self, p => p.connect_session_take(state), f => f.connect_session_take(state))
    }
    async fn tenant_run_link(&self, run_id: &str, tenant: &str) -> Result<(), HistoryError> {
        via!(self, p => p.tenant_run_link(run_id, tenant), f => f.tenant_run_link(run_id, tenant))
    }
    async fn tenant_state_ref_add(
        &self,
        state_ref: &super::tenants::TenantStateRef,
    ) -> Result<(), HistoryError> {
        via!(self, p => p.tenant_state_ref_add(state_ref), f => f.tenant_state_ref_add(state_ref))
    }
    async fn tenant_state_refs(
        &self,
        tenant: &str,
    ) -> Result<Vec<super::tenants::TenantStateRef>, HistoryError> {
        via!(self, p => p.tenant_state_refs(tenant), f => f.tenant_state_refs(tenant))
    }
    async fn change_delete(&self, id: &str) -> Result<bool, HistoryError> {
        via!(self, p => p.change_delete(id), f => f.change_delete(id))
    }
    async fn usage_delete_runs(&self, run_ids: &[String]) -> Result<usize, HistoryError> {
        via!(self, p => p.usage_delete_runs(run_ids), f => f.usage_delete_runs(run_ids))
    }
    async fn change_upsert(
        &self,
        change: &crate::serve::changes::ChangeRequest,
    ) -> Result<(), HistoryError> {
        via!(self, p => p.change_upsert(change), f => f.change_upsert(change))
    }
    async fn change_get(
        &self,
        id: &str,
    ) -> Result<Option<crate::serve::changes::ChangeRequest>, HistoryError> {
        via!(self, p => p.change_get(id), f => f.change_get(id))
    }
    async fn change_list(
        &self,
        filter: &crate::serve::changes::ChangeListFilter,
    ) -> Result<Vec<crate::serve::changes::ChangeRequest>, HistoryError> {
        via!(self, p => p.change_list(filter), f => f.change_list(filter))
    }
    async fn usage_record(&self, record: &crate::usage::UsageRecord) -> Result<(), HistoryError> {
        via!(self, p => p.usage_record(record), f => f.usage_record(record))
    }
    async fn usage_list(
        &self,
        filter: &crate::usage::UsageFilter,
    ) -> Result<Vec<crate::usage::UsageRecord>, HistoryError> {
        via!(self, p => p.usage_list(filter), f => f.usage_list(filter))
    }
    async fn local_output_record(
        &self,
        obs: &crate::local_outputs::LocalOutputObservation,
    ) -> Result<(), HistoryError> {
        via!(self, p => p.local_output_record(obs), f => f.local_output_record(obs))
    }
    async fn local_output_list(
        &self,
        filter: &crate::local_outputs::LocalOutputFilter,
    ) -> Result<Vec<crate::local_outputs::LocalOutputRecord>, HistoryError> {
        via!(self, p => p.local_output_list(filter), f => f.local_output_list(filter))
    }
    async fn local_output_get(
        &self,
        id: &str,
    ) -> Result<Option<crate::local_outputs::LocalOutputRecord>, HistoryError> {
        via!(self, p => p.local_output_get(id), f => f.local_output_get(id))
    }
    async fn local_output_mark_deleted(
        &self,
        id: &str,
        at: DateTime<Utc>,
        bytes: u64,
    ) -> Result<bool, HistoryError> {
        via!(
            self,
            p => p.local_output_mark_deleted(id, at, bytes),
            f => f.local_output_mark_deleted(id, at, bytes)
        )
    }
    async fn catalog_annotate(
        &self,
        dataset_id: &str,
        annotation: &crate::serve::history::catalog::CatalogAnnotation,
    ) -> Result<bool, HistoryError> {
        via!(self, p => p.catalog_annotate(dataset_id, annotation), f => f.catalog_annotate(dataset_id, annotation))
    }
    async fn catalog_record_config_snapshot(
        &self,
        snapshot: &crate::serve::history::catalog::ConfigSnapshot,
    ) -> Result<(), HistoryError> {
        via!(self, p => p.catalog_record_config_snapshot(snapshot), f => f.catalog_record_config_snapshot(snapshot))
    }
    async fn catalog_record_profile(
        &self,
        dataset_id: &str,
        record: &crate::serve::history::catalog::CatalogProfileRecord,
    ) -> Result<(), HistoryError> {
        via!(self, p => p.catalog_record_profile(dataset_id, record), f => f.catalog_record_profile(dataset_id, record))
    }
    async fn catalog_profile_history(
        &self,
        dataset_id: &str,
        limit: usize,
    ) -> Result<Vec<crate::serve::history::catalog::CatalogProfileRecord>, HistoryError> {
        via!(self, p => p.catalog_profile_history(dataset_id, limit), f => f.catalog_profile_history(dataset_id, limit))
    }
    async fn catalog_last_config_snapshot(
        &self,
        pipeline: &str,
    ) -> Result<Option<crate::serve::history::catalog::ConfigSnapshot>, HistoryError> {
        via!(self, p => p.catalog_last_config_snapshot(pipeline), f => f.catalog_last_config_snapshot(pipeline))
    }

    // ── Pipeline-template registry (#444) ────────────────────────────────────
    //
    // Forwarded like every other method: while the SQL backend is reachable
    // templates persist; once degraded they land in the in-memory fallback, so
    // the control plane keeps serving (a registration made while degraded is
    // process-lifetime only — the same trade-off as a degraded run record).
    async fn template_register(
        &self,
        draft: &crate::serve::history::templates::TemplateDraft,
    ) -> Result<crate::serve::history::templates::TemplateRecord, HistoryError> {
        via!(self, p => p.template_register(draft), f => f.template_register(draft))
    }
    async fn template_get(
        &self,
        id: &str,
        version: Option<u32>,
    ) -> Result<Option<crate::serve::history::templates::TemplateRecord>, HistoryError> {
        via!(self, p => p.template_get(id, version), f => f.template_get(id, version))
    }
    async fn template_list(
        &self,
    ) -> Result<Vec<crate::serve::history::templates::TemplateSummary>, HistoryError> {
        via!(self, p => p.template_list(), f => f.template_list())
    }
    async fn template_versions(&self, id: &str) -> Result<Vec<u32>, HistoryError> {
        via!(self, p => p.template_versions(id), f => f.template_versions(id))
    }
    async fn template_delete(&self, id: &str, version: Option<u32>) -> Result<usize, HistoryError> {
        via!(self, p => p.template_delete(id, version), f => f.template_delete(id, version))
    }
    async fn template_set_tag(
        &self,
        id: &str,
        tag: &str,
        version: u32,
    ) -> Result<(), HistoryError> {
        via!(self, p => p.template_set_tag(id, tag, version), f => f.template_set_tag(id, tag, version))
    }
    async fn template_tags(
        &self,
        id: &str,
    ) -> Result<std::collections::BTreeMap<String, u32>, HistoryError> {
        via!(self, p => p.template_tags(id), f => f.template_tags(id))
    }
    async fn template_delete_tag(&self, id: &str, tag: &str) -> Result<bool, HistoryError> {
        via!(self, p => p.template_delete_tag(id, tag), f => f.template_delete_tag(id, tag))
    }
    async fn template_launch(
        &self,
        id: &str,
        version: u32,
        launched_by: Option<&str>,
    ) -> Result<Option<u32>, HistoryError> {
        via!(
            self,
            p => p.template_launch(id, version, launched_by),
            f => f.template_launch(id, version, launched_by)
        )
    }
    async fn template_launches(
        &self,
        id: &str,
    ) -> Result<Vec<crate::serve::history::templates::LaunchRecord>, HistoryError> {
        via!(self, p => p.template_launches(id), f => f.template_launches(id))
    }
    async fn template_set_deprecation(
        &self,
        id: &str,
        record: Option<&crate::serve::history::templates::DeprecationRecord>,
    ) -> Result<(), HistoryError> {
        via!(
            self,
            p => p.template_set_deprecation(id, record),
            f => f.template_set_deprecation(id, record)
        )
    }
    async fn template_set_version_deprecation(
        &self,
        id: &str,
        version: u32,
        record: Option<&crate::serve::history::templates::DeprecationRecord>,
    ) -> Result<(), HistoryError> {
        via!(
            self,
            p => p.template_set_version_deprecation(id, version, record),
            f => f.template_set_version_deprecation(id, version, record)
        )
    }

    async fn template_version_deprecations(
        &self,
        id: &str,
    ) -> Result<Vec<crate::serve::history::templates::VersionDeprecation>, HistoryError> {
        via!(
            self,
            p => p.template_version_deprecations(id),
            f => f.template_version_deprecations(id)
        )
    }

    async fn template_deprecation(
        &self,
        id: &str,
    ) -> Result<Option<crate::serve::history::templates::DeprecationRecord>, HistoryError> {
        via!(self, p => p.template_deprecation(id), f => f.template_deprecation(id))
    }

    fn degraded(&self) -> bool {
        self.is_degraded()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::RunStatus;

    /// A primary backend whose every call fails — drives the degrade path.
    struct AlwaysFail;

    #[async_trait]
    impl RunHistory for AlwaysFail {
        async fn claim_idempotency(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: Duration,
        ) -> Result<Claim, HistoryError> {
            Err(HistoryError::Backend("down".into()))
        }
        async fn upsert(&self, _: &RunRecord) -> Result<(), HistoryError> {
            Err(HistoryError::Backend("down".into()))
        }
        async fn get(&self, _: &str) -> Result<Option<RunRecord>, HistoryError> {
            Err(HistoryError::Backend("down".into()))
        }
        async fn list(&self, _: &ListFilter) -> Result<ListPage, HistoryError> {
            Err(HistoryError::Backend("down".into()))
        }
        async fn delete(&self, _: &str) -> Result<DeleteOutcome, HistoryError> {
            Err(HistoryError::Backend("down".into()))
        }
        async fn purge_expired(&self, _: Duration) -> Result<usize, HistoryError> {
            Err(HistoryError::Backend("down".into()))
        }
        async fn recover_orphans(&self) -> Result<usize, HistoryError> {
            Err(HistoryError::Backend("down".into()))
        }
        fn degraded(&self) -> bool {
            false
        }
    }

    fn change(id: &str) -> crate::serve::changes::ChangeRequest {
        use crate::serve::changes::*;
        let now = chrono::Utc::now();
        ChangeRequest {
            id: id.into(),
            kind: ChangeKind::Run,
            status: ChangeStatus::Pending,
            requester: "bob".into(),
            requester_role: crate::serve::rbac::Role::Operator,
            reason: None,
            payload: serde_json::json!({}),
            plan: None,
            budget: None,
            required_approvals: 1,
            approvals: Vec::new(),
            rejection: None,
            created_at: now,
            updated_at: now,
            expires_at: now,
            run_id: None,
            template: None,
            error: None,
        }
    }

    #[tokio::test]
    async fn defaults_are_inert_and_changes_and_usage_forward() {
        let bare = AlwaysFail;
        assert!(bare.change_upsert(&change("c0")).await.is_err());
        assert!(bare.change_get("c0").await.unwrap().is_none());
        assert!(
            bare.change_list(&Default::default())
                .await
                .unwrap()
                .is_empty()
        );
        let usage = crate::usage::UsageFilter::default();
        assert!(bare.usage_list(&usage).await.unwrap().is_empty());

        let fb = FallbackHistory::healthy(Box::new(AlwaysFail), Duration::from_secs(60), "test");
        assert!(fb.change_get("c1").await.unwrap().is_none());
        assert!(
            fb.change_list(&Default::default())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(fb.usage_list(&usage).await.unwrap().is_empty());
        // The primary refuses the write, so the store trips and memory serves.
        fb.change_upsert(&change("c1")).await.unwrap();
        assert!(fb.degraded());
        assert_eq!(fb.change_get("c1").await.unwrap().unwrap().id, "c1");
        assert_eq!(fb.change_list(&Default::default()).await.unwrap().len(), 1);
        assert!(fb.usage_list(&usage).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn trips_to_fallback_on_primary_error_and_serves_from_memory() {
        let fb = FallbackHistory::healthy(Box::new(AlwaysFail), Duration::from_secs(60), "test");
        assert!(!fb.degraded(), "starts healthy");

        // First write fails on the primary, trips degraded, lands in memory.
        let rec = RunRecord::queued(
            "r1".into(),
            None,
            Default::default(),
            None,
            chrono::Utc::now(),
        );
        fb.upsert(&rec).await.unwrap();
        assert!(fb.degraded(), "primary error must flip degraded");

        // Subsequent reads are served from the in-memory fallback.
        let got = fb.get("r1").await.unwrap();
        assert_eq!(got.unwrap().run_id, "r1");
    }

    #[tokio::test]
    async fn claim_fails_closed_once_degraded_from_a_healthy_primary() {
        // M5 (#146): the primary may hold claims the in-memory fallback can't
        // see, so once it trips, an idempotency claim must fail CLOSED rather
        // than return a `Fresh` from the empty memory store and duplicate a run.
        let fb = FallbackHistory::healthy(Box::new(AlwaysFail), Duration::from_secs(60), "test");
        let err = fb
            .claim_idempotency("k", "fp", "r1", Duration::from_secs(60))
            .await
            .unwrap_err();
        assert!(matches!(err, HistoryError::Degraded(_)), "got {err:?}");
        assert!(fb.degraded(), "the failed primary claim trips degraded");
        // A second attempt (already degraded) also fails closed — never Fresh.
        assert!(matches!(
            fb.claim_idempotency("k", "fp", "r2", Duration::from_secs(60))
                .await,
            Err(HistoryError::Degraded(_))
        ));
    }

    #[tokio::test]
    async fn claim_uses_memory_when_started_degraded() {
        // No primary ever existed → the in-memory store is authoritative, so
        // idempotency works normally (no split is possible).
        let fb = FallbackHistory::degraded_at_startup(Duration::from_secs(60), "test");
        assert_eq!(
            fb.claim_idempotency("k", "fp", "r1", Duration::from_secs(60))
                .await
                .unwrap(),
            Claim::Fresh
        );
        assert_eq!(
            fb.claim_idempotency("k", "fp", "r2", Duration::from_secs(60))
                .await
                .unwrap(),
            Claim::Replay("r1".into())
        );
    }

    #[tokio::test]
    async fn degraded_at_startup_uses_memory_only() {
        let fb = FallbackHistory::degraded_at_startup(Duration::from_secs(60), "test");
        assert!(fb.degraded());
        let mut rec = RunRecord::queued(
            "r2".into(),
            None,
            Default::default(),
            None,
            chrono::Utc::now(),
        );
        rec.status = RunStatus::Completed;
        rec.finished_at = Some(chrono::Utc::now());
        fb.upsert(&rec).await.unwrap();
        assert_eq!(
            fb.get("r2").await.unwrap().unwrap().status,
            RunStatus::Completed
        );
        assert_eq!(fb.delete("r2").await.unwrap(), DeleteOutcome::Deleted);
    }

    #[tokio::test]
    async fn shard_methods_delegate_to_a_healthy_primary() {
        use crate::serve::history::memory::MemoryHistory;
        use crate::serve::history::{ReclaimReport, ShardProgress};
        // A healthy (memory) primary → the shard methods delegate via `via!`;
        // memory's shard methods are inert, so we get the inert results back
        // (exercising the forwarding path).
        let fb = FallbackHistory::healthy(
            Box::new(MemoryHistory::new(Duration::from_secs(60))),
            Duration::from_secs(60),
            "test",
        );
        assert_eq!(fb.insert_shards("r", &[]).await.unwrap(), 0);
        assert!(fb.claim_shards(4).await.unwrap().is_empty());
        assert_eq!(fb.renew_shard_leases().await.unwrap(), 0);
        assert_eq!(
            fb.reclaim_shards(3).await.unwrap(),
            ReclaimReport::default()
        );
        assert!(!fb.finalize_shard("r", "0", true).await.unwrap());
        assert_eq!(
            fb.shard_progress("r").await.unwrap(),
            ShardProgress::default()
        );
        assert!(!fb.degraded());
    }
}
