//! A template id keeps one kind even when two registrations race: the store
//! refuses a version of another kind where it assigns the version number, not
//! only in the registry's earlier read.
#![cfg(feature = "templates")]

use std::sync::Arc;
use std::time::Duration;

use faucet_cli::error::CliError;
use faucet_cli::hub::TemplateKind;
use faucet_cli::serve::history::memory::MemoryHistory;
use faucet_cli::serve::history::templates::{
    LaunchRecord, TemplateDraft, TemplateId, TemplateRecord,
};
use faucet_cli::serve::history::{
    Claim, DeleteOutcome, HistoryError, ListFilter, ListPage, RunHistory, RunRecord,
};
use faucet_cli::serve::load::ConfigFormat;
use faucet_cli::templates::store::{RegisterRequest, TemplateStore, register};

/// A store whose "current version" read always comes back empty, as it does
/// for a registration that read before a concurrent one committed.
struct StaleRead(MemoryHistory);

#[async_trait::async_trait]
impl RunHistory for StaleRead {
    async fn claim_idempotency(
        &self,
        a: &str,
        b: &str,
        c: &str,
        d: Duration,
    ) -> Result<Claim, HistoryError> {
        self.0.claim_idempotency(a, b, c, d).await
    }
    async fn upsert(&self, rec: &RunRecord) -> Result<(), HistoryError> {
        self.0.upsert(rec).await
    }
    async fn get(&self, id: &str) -> Result<Option<RunRecord>, HistoryError> {
        self.0.get(id).await
    }
    async fn list(&self, f: &ListFilter) -> Result<ListPage, HistoryError> {
        self.0.list(f).await
    }
    async fn delete(&self, id: &str) -> Result<DeleteOutcome, HistoryError> {
        self.0.delete(id).await
    }
    async fn purge_expired(&self, r: Duration) -> Result<usize, HistoryError> {
        self.0.purge_expired(r).await
    }
    async fn recover_orphans(&self) -> Result<usize, HistoryError> {
        self.0.recover_orphans().await
    }
    fn degraded(&self) -> bool {
        false
    }
    async fn template_get(
        &self,
        id: &str,
        version: Option<u32>,
    ) -> Result<Option<TemplateRecord>, HistoryError> {
        match version {
            None => Ok(None),
            Some(_) => self.0.template_get(id, version).await,
        }
    }
    async fn template_register(&self, d: &TemplateDraft) -> Result<TemplateRecord, HistoryError> {
        self.0.template_register(d).await
    }
    async fn template_launches(&self, id: &str) -> Result<Vec<LaunchRecord>, HistoryError> {
        self.0.template_launches(id).await
    }
}

#[tokio::test]
async fn a_racing_registration_of_another_kind_is_refused() {
    let inner = MemoryHistory::new(Duration::from_secs(60));
    let source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../hub/source-templates/faucet-hq/example-csv.yaml"
    ))
    .unwrap();
    inner
        .template_register(&TemplateDraft {
            id: TemplateId::parse("faucet-hq/example-csv").unwrap(),
            kind: TemplateKind::Pipeline,
            name: None,
            description: None,
            body: "version: 1\n".into(),
            format: ConfigFormat::Yaml,
            params: Default::default(),
            created_by: None,
        })
        .await
        .unwrap();
    let store: TemplateStore = Arc::new(StaleRead(inner));
    let err = register(
        &store,
        RegisterRequest {
            id: None,
            body: source,
            format: ConfigFormat::Yaml,
            description: None,
            tags: Vec::new(),
            launch: false,
            created_by: None,
        },
    )
    .await
    .unwrap_err();
    match err {
        CliError::Config(m) => assert!(
            m.contains("is a pipeline") && m.contains("cannot be registered under the same id"),
            "{m}"
        ),
        other => panic!("expected a config refusal, got {other:?}"),
    }
    let versions = store
        .template_get("faucet-hq/example-csv", Some(2))
        .await
        .unwrap();
    assert!(versions.is_none(), "no second version was stored");
}
