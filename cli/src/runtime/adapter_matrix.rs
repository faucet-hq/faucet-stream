//! One matrix (#887): every operation a `faucet serve` adapter exposes runs
//! through the runtime, so each adapter — HTTP, MCP, a trigger, an approved
//! change request and the CLI — meets the same governance: the reference
//! policy for a caller's body, the data-flow policy, the budget, tenant
//! admission and the audit record.

use crate::serve::history::RunStatus;
use crate::serve::history::tenants::{TenantRecord, TenantSelector};
use crate::serve::rbac::{AuthContext, Role};
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Extension, Path, State};
use serde_json::{Value, json};
use std::path::Path as FsPath;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Adapter {
    Http,
    HttpTemplate,
    Mcp,
    McpTemplate,
    Trigger,
    TemplateTrigger,
    Change,
    Cli,
}

const SERVE: [Adapter; 7] = [
    Adapter::Http,
    Adapter::HttpTemplate,
    Adapter::Mcp,
    Adapter::McpTemplate,
    Adapter::Trigger,
    Adapter::TemplateTrigger,
    Adapter::Change,
];

impl Adapter {
    /// Whether the body comes from the caller (and meets the caller
    /// reference policy) rather than from the server's operator.
    fn caller_body(self) -> bool {
        matches!(self, Adapter::Http | Adapter::Mcp | Adapter::Change)
    }

    /// Who the adapter's audit records name.
    fn principal(self) -> &'static str {
        match self {
            Adapter::Trigger | Adapter::TemplateTrigger => "trigger:hook",
            _ => "alice",
        }
    }
}

const CONTRACT: &str = "  contract:\n    version: \"1\"\n    fields:\n      - { name: id, type: string }\n      - { name: email, type: string }\n";
const POLICY: &str = "policy:\n  version: 1\n  classifications:\n    - label: pii\n      fields: [email]\n  rules:\n    - name: pii-eu\n      when: { label: pii }\n      require: { residency: [eu] }\n";

fn config(dir: &FsPath, name: &str, top: &str, contract: bool) -> String {
    let input = dir.join("in.csv");
    std::fs::write(&input, "id,email\n1,a@x.io\n2,b@x.io\n3,c@x.io\n").unwrap();
    format!(
        "version: 1\nname: {name}\n{top}pipeline:\n  source: {{ type: csv, config: {{ path: \"{}\" }} }}\n{}  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
        input.display(),
        if contract { CONTRACT } else { "" },
        dir.join(format!("{name}.jsonl")).display(),
    )
}

fn alice(tenant: Option<&str>) -> AuthContext {
    AuthContext {
        principal: "alice".into(),
        role: Role::Admin,
        source_ip: None,
        tenant: tenant.map(str::to_string),
    }
}

async fn fresh() -> ServerState {
    let state = crate::serve::test_support::test_state();
    state.set_tenants(crate::serve::tenants::TenantsRuntime::new(
        Some(
            crate::serve::tenants::vault::Vault::new("0123456789abcdef0123456789abcdef", &[])
                .unwrap(),
        ),
        Default::default(),
    ));
    state
}

async fn tenant(state: &ServerState, suspended: bool, max_records: Option<u64>) {
    let now = chrono::Utc::now();
    let mut t = TenantRecord {
        id: "acme".into(),
        name: None,
        labels: Default::default(),
        limits: Default::default(),
        notifications: Vec::new(),
        notifications_sealed: None,
        suspended,
        created_at: now,
        updated_at: now,
        created_by: "t".into(),
    };
    t.limits.max_records_per_run = max_records;
    state.history().tenant_upsert(&t).await.unwrap();
}

/// What an adapter returned: the run it started, or why it refused.
type Outcome = Result<Option<String>, String>;

fn error_text(e: crate::serve::error::ServeError) -> String {
    let api = e.api_error();
    format!(
        "{} {}",
        api.error.message,
        api.error.details.map(|d| d.to_string()).unwrap_or_default()
    )
}

async fn response_run_id(resp: axum::response::Response) -> Outcome {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    Ok(body["run_id"].as_str().map(str::to_string))
}

/// Register `body` as draft template `tpl` (operator-written); a draft runs
/// its `newest` version.
async fn register(state: &ServerState, body: &str) {
    crate::templates::register(
        &state.history(),
        crate::templates::RegisterRequest {
            id: Some("tpl".into()),
            body: body.to_string(),
            format: crate::serve::load::ConfigFormat::Yaml,
            description: None,
            tags: vec![],
            launch: false,
            created_by: None,
        },
    )
    .await
    .expect("register");
}

/// Run `body` through `adapter` as alice (or for `tenant`), with `budget`
/// as the request's own ceiling where the adapter carries one.
async fn submit(
    adapter: Adapter,
    state: &ServerState,
    dir: &FsPath,
    body: &str,
    tenant: Option<&str>,
    budget: Option<Value>,
) -> Outcome {
    let mut req = json!({ "config": body });
    if let Some(b) = &budget {
        req["budget"] = b.clone();
    }
    match adapter {
        Adapter::Http => {
            let req = serde_json::from_value(req).unwrap();
            let resp = match tenant {
                None => {
                    crate::serve::handlers::runs::submit_run(
                        State(state.clone()),
                        Extension(alice(None)),
                        Json(req),
                    )
                    .await
                }
                Some(t) => {
                    crate::serve::handlers::tenants::submit_tenant_run(
                        State(state.clone()),
                        Extension(alice(None)),
                        Path(t.to_string()),
                        Json(req),
                    )
                    .await
                }
            };
            match resp {
                Ok(r) => response_run_id(r).await,
                Err(e) => Err(error_text(e)),
            }
        }
        Adapter::HttpTemplate => {
            register(state, body).await;
            let mut trigger = json!({ "version": "newest" });
            if let Some(b) = budget {
                trigger["budget"] = b;
            }
            let trigger = serde_json::from_value(trigger).unwrap();
            let resp = match tenant {
                None => {
                    crate::serve::handlers::templates::trigger_template(
                        State(state.clone()),
                        Extension(alice(None)),
                        Path("tpl".to_string()),
                        Json(trigger),
                    )
                    .await
                }
                Some(t) => {
                    crate::serve::handlers::tenants::trigger_tenant_template(
                        State(state.clone()),
                        Extension(alice(None)),
                        Path((t.to_string(), "tpl".to_string())),
                        Json(trigger),
                    )
                    .await
                }
            };
            match resp {
                Ok(r) => response_run_id(r).await,
                Err(e) => Err(error_text(e)),
            }
        }
        Adapter::Mcp | Adapter::McpTemplate => {
            let ctx = crate::mcp::McpContext {
                auth: Default::default(),
                allow_mutations: true,
                allow_config_execution: true,
                allow_template_admin: false,
                templates: Some(state.history()),
                changes: None,
                submitted: Some(state.caller_origin()),
                approval_required: Vec::new(),
                server: Some(crate::mcp::ChangeProposer {
                    state: state.clone(),
                    actor: alice(tenant),
                }),
            };
            let out = if adapter == Adapter::Mcp {
                crate::mcp::tools::call_tool(&ctx, "run_pipeline", &json!({ "config": body })).await
            } else {
                register(state, body).await;
                crate::mcp::tools::call_tool(
                    &ctx,
                    "run_template",
                    &json!({ "id": "tpl", "version": "newest" }),
                )
                .await
            };
            let text = out["content"][0]["text"].as_str().unwrap_or("").to_string();
            if out["isError"] == true {
                Err(text)
            } else {
                let v: Value = serde_json::from_str(&text).unwrap();
                Ok(v["run_id"].as_str().map(str::to_string))
            }
        }
        Adapter::Trigger | Adapter::TemplateTrigger => {
            use crate::serve::triggers::spec::{
                PipelineRef, RunTemplate, TemplateTrigger, TriggerKind, TriggerSpec,
            };
            let (config, template) = if adapter == Adapter::Trigger {
                let doc: Value = crate::yaml::from_str(body).unwrap();
                (Some(PipelineRef::Inline(doc)), None)
            } else {
                register(state, body).await;
                let t: TemplateTrigger =
                    serde_json::from_value(json!({ "id": "tpl", "version": "newest" })).unwrap();
                (None, Some(t))
            };
            let compiled = crate::serve::triggers::compiled::CompiledTrigger {
                spec: TriggerSpec {
                    name: "hook".into(),
                    enabled: true,
                    config,
                    template,
                    tenants: tenant.map(|t| TenantSelector::Named(vec![t.to_string()])),
                    run: RunTemplate {
                        name: None,
                        labels: Default::default(),
                        timeout_secs: None,
                        selection: None,
                        depends_on: None,
                        pool: None,
                    },
                    kind: TriggerKind::Webhook {
                        methods: vec!["POST".into()],
                        dedupe_header: None,
                        debounce_secs: 0,
                    },
                },
                webhook_path: Some("/v1/triggers/hook".into()),
            };
            let event = crate::serve::triggers::context::TriggerEvent::Schedule {
                tick: uuid::Uuid::now_v7().to_string(),
            };
            use crate::serve::triggers::enqueue::FireOutcome;
            match crate::serve::triggers::enqueue::fire(state, &compiled, event, "now").await {
                FireOutcome::Enqueued(id) => Ok(Some(id)),
                FireOutcome::Failed(e) | FireOutcome::Error(e) => Err(e),
                other => Err(format!("{other:?}")),
            }
        }
        Adapter::Change => {
            use crate::serve::changes::{ChangeKind, ChangeStatus, NewChange};
            let actor = alice(tenant);
            let change = crate::serve::changes::create(
                state,
                &actor,
                NewChange {
                    kind: ChangeKind::Run,
                    payload: req,
                    reason: None,
                    budget: None,
                    trusted_config: false,
                    caller_params: Default::default(),
                },
            )
            .await
            .map_err(error_text)?;
            let approved = crate::serve::changes::approve(state, &alice(None), &change.id, None)
                .await
                .map_err(error_text)?;
            match approved.status {
                ChangeStatus::Executed => Ok(approved.run_id),
                _ => Err(approved.error.unwrap_or_default()),
            }
        }
        Adapter::Cli => {
            let path = dir.join("cli.yaml");
            std::fs::write(&path, body).unwrap();
            let max_records = budget.and_then(|b| b["max_records"].as_u64());
            crate::commands::run::run(crate::cli::RunArgs {
                config: Some(path),
                no_env_file: true,
                quiet: true,
                max_records,
                ..Default::default()
            })
            .await
            .map(|()| None)
            .map_err(|e| e.to_string())
        }
    }
}

/// The run's terminal record.
#[allow(clippy::disallowed_methods)]
async fn finished(state: &ServerState, id: &str) -> crate::serve::history::RunRecord {
    for _ in 0..400 {
        if let Some(rec) = state.history().get(id).await.unwrap()
            && rec.status.is_terminal()
        {
            return rec;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("run {id} did not finish");
}

async fn audited(state: &ServerState, action: &str) -> Vec<crate::serve::history::AuditEntry> {
    state
        .history()
        .list_audit(&crate::serve::history::AuditFilter {
            principal: None,
            tenant: None,
            action: Some(action.into()),
            since: None,
            until: None,
            result: None,
            before: None,
            limit: 100,
        })
        .await
        .unwrap()
}

fn lines(dir: &FsPath, name: &str) -> usize {
    std::fs::read_to_string(dir.join(format!("{name}.jsonl")))
        .map(|s| s.lines().count())
        .unwrap_or(0)
}

#[tokio::test]
async fn every_adapter_refuses_a_policy_violation_and_the_server_audits_it() {
    for adapter in SERVE.into_iter().chain([Adapter::Cli]) {
        let dir = tempfile::tempdir().unwrap();
        let state = fresh().await;
        let body = config(dir.path(), "pol", POLICY, true);
        let err = submit(adapter, &state, dir.path(), &body, None, None)
            .await
            .expect_err(&format!("{adapter:?} ran a policy-violating config"));
        assert!(
            err.contains("pii-eu") || err.contains("policy"),
            "{adapter:?}: {err}"
        );
        assert_eq!(lines(dir.path(), "pol"), 0, "{adapter:?} wrote");
        if adapter != Adapter::Cli {
            let denied = audited(&state, "policy.denied").await;
            assert!(
                denied.iter().any(|a| a.principal == adapter.principal()),
                "{adapter:?}: {denied:?}"
            );
        }
    }
}

#[tokio::test]
async fn a_callers_body_meets_the_reference_policy_and_an_operators_does_not() {
    for adapter in SERVE.into_iter().chain([Adapter::Cli]) {
        let dir = tempfile::tempdir().unwrap();
        let state = fresh().await;
        let body = config(
            dir.path(),
            "refs",
            "vars:\n  home: \"${env:HOME}\"\n",
            false,
        );
        let out = submit(adapter, &state, dir.path(), &body, None, None).await;
        if adapter.caller_body() {
            let err = out.expect_err(&format!("{adapter:?} loaded a caller's ${{env:HOME}}"));
            assert!(err.contains("HOME"), "{adapter:?}: {err}");
        } else {
            out.unwrap_or_else(|e| panic!("{adapter:?} refused an operator's body: {e}"));
        }
    }
}

#[tokio::test]
async fn every_adapter_runs_under_the_budget_and_records_the_submission() {
    let ceiling = json!({ "max_records": 1 });
    for adapter in SERVE.into_iter().chain([Adapter::Cli]) {
        // Unbudgeted, the run writes all three rows.
        let dir = tempfile::tempdir().unwrap();
        let state = fresh().await;
        let open = config(dir.path(), "open", "", false);
        let id = submit(adapter, &state, dir.path(), &open, None, None)
            .await
            .unwrap_or_else(|e| panic!("{adapter:?}: {e}"));
        if let Some(id) = id {
            assert_eq!(finished(&state, &id).await.status, RunStatus::Completed);
            let submitted = audited(&state, "run.submit").await;
            assert!(
                submitted
                    .iter()
                    .any(|a| a.principal == adapter.principal() && a.run_id.as_deref() == Some(&id)),
                "{adapter:?}: {submitted:?}"
            );
        }
        assert_eq!(lines(dir.path(), "open"), 3, "{adapter:?}");

        // The request's ceiling where the adapter carries one, else the
        // config's: the page that would cross it is refused whole.
        let dir = tempfile::tempdir().unwrap();
        let state = fresh().await;
        let carries = matches!(
            adapter,
            Adapter::Http | Adapter::HttpTemplate | Adapter::Change | Adapter::Cli
        );
        let top = if carries {
            ""
        } else {
            "budget:\n  max_records: 1\n"
        };
        let body = config(dir.path(), "capped", top, false);
        let budget = carries.then(|| ceiling.clone());
        match submit(adapter, &state, dir.path(), &body, None, budget).await {
            Ok(Some(id)) => assert_eq!(
                finished(&state, &id).await.status,
                RunStatus::Failed,
                "{adapter:?}"
            ),
            Ok(None) => panic!("{adapter:?} ran past its budget"),
            Err(e) => assert!(adapter == Adapter::Cli, "{adapter:?}: {e}"),
        }
        assert_eq!(
            lines(dir.path(), "capped"),
            0,
            "{adapter:?} wrote past the budget"
        );
    }
}

#[tokio::test]
async fn every_adapter_admits_a_tenant_run_against_its_tenant() {
    for adapter in SERVE {
        // A suspended tenant runs nothing.
        let dir = tempfile::tempdir().unwrap();
        let state = fresh().await;
        tenant(&state, true, None).await;
        let body = config(dir.path(), "t", "", false);
        let err = submit(adapter, &state, dir.path(), &body, Some("acme"), None)
            .await
            .expect_err(&format!("{adapter:?} ran for a suspended tenant"));
        // A trigger commits a refused fire as a no-op so its watcher does not
        // retry the same event.
        let coalesced =
            matches!(adapter, Adapter::Trigger | Adapter::TemplateTrigger) && err == "Coalesced";
        assert!(err.contains("suspended") || coalesced, "{adapter:?}: {err}");
        let runs = state
            .history()
            .list(&crate::serve::history::ListFilter {
                limit: 100,
                ..Default::default()
            })
            .await
            .unwrap()
            .runs;
        assert!(runs.is_empty(), "{adapter:?} recorded {runs:?}");

        // An active tenant's run is its own, under its per-run ceiling.
        let dir = tempfile::tempdir().unwrap();
        let state = fresh().await;
        tenant(&state, false, Some(1)).await;
        let body = config(dir.path(), "t", "", false);
        let id = submit(adapter, &state, dir.path(), &body, Some("acme"), None)
            .await
            .unwrap_or_else(|e| panic!("{adapter:?}: {e}"))
            .expect("a run id");
        let rec = finished(&state, &id).await;
        assert_eq!(rec.tenant.as_deref(), Some("acme"), "{adapter:?}");
        assert_eq!(rec.status, RunStatus::Failed, "{adapter:?}");
        assert_eq!(
            lines(dir.path(), "t"),
            0,
            "{adapter:?} wrote past the tenant's ceiling"
        );
    }
}

/// The inline operations (verify repair, DLQ replay, rollback) meet the
/// same policy gate a run does, and plan / doctor never write.
#[tokio::test]
async fn the_inline_operations_meet_the_same_gates() {
    let dir = tempfile::tempdir().unwrap();
    let state = fresh().await;
    let body = config(dir.path(), "inline", POLICY, true);
    let principal = crate::runtime::Principal::caller(&state, alice(None));

    let verify: crate::serve::handlers::verify::VerifyRequest =
        serde_json::from_value(json!({ "config": body, "repair": true })).unwrap();
    let err = super::server::verify(&state, &principal, verify)
        .await
        .unwrap_err();
    assert!(error_text(err).contains("pii-eu"));

    let dlq = dir.path().join("dlq.jsonl");
    std::fs::write(&dlq, "").unwrap();
    let replay: crate::serve::handlers::dlq::DlqReplayRequest =
        serde_json::from_value(json!({ "config": body, "from": dlq.display().to_string() }))
            .unwrap();
    let err = super::server::replay_dlq(&state, &principal, replay)
        .await
        .unwrap_err();
    assert!(error_text(err).contains("pii-eu"));

    let mut rec = crate::serve::history::RunRecord::queued(
        "done".into(),
        None,
        Default::default(),
        None,
        chrono::Utc::now(),
    );
    rec.status = RunStatus::Completed;
    state.history().upsert(&rec).await.unwrap();
    let rollback: crate::serve::handlers::runs::RollbackRequest =
        serde_json::from_value(json!({ "config": body, "invocation_id": "inv" })).unwrap();
    let err = super::server::rollback(&state, &principal, "done", rollback)
        .await
        .unwrap_err();
    assert!(error_text(err).contains("pii-eu"));

    // Reads are not gated: the plan reports the violation, it does not refuse.
    let plan: crate::serve::handlers::plan::PlanRequest =
        serde_json::from_value(json!({ "config": body })).unwrap();
    let report = super::server::plan(&state, &principal, plan).await.unwrap();
    assert!(report.get("row").is_some(), "{report}");
    let caller_env = config(dir.path(), "env", "vars:\n  home: \"${env:HOME}\"\n", false);
    let err = super::server::doctor(
        &state,
        &principal,
        super::server::Submission::new(&caller_env, crate::serve::load::ConfigFormat::Yaml),
    )
    .await
    .unwrap_err();
    assert!(error_text(err).contains("HOME"));
    assert_eq!(lines(dir.path(), "inline"), 0);
}

#[tokio::test]
async fn cancel_is_scoped_to_what_the_principal_sees() {
    let state = fresh().await;
    let mut rec = crate::serve::history::RunRecord::queued(
        "theirs".into(),
        None,
        Default::default(),
        None,
        chrono::Utc::now(),
    );
    rec.tenant = Some("globex".into());
    state.history().upsert(&rec).await.unwrap();
    let scoped = crate::runtime::Principal::caller(&state, alice(Some("acme")));
    assert!(matches!(
        super::server::cancel(&state, &scoped, "theirs").await,
        Err(crate::serve::error::ServeError::NotFound)
    ));
    let admin = crate::runtime::Principal::caller(&state, alice(None));
    assert_eq!(
        super::server::cancel(&state, &admin, "theirs")
            .await
            .unwrap(),
        super::server::CancelOutcome::Finished
    );
    assert!(matches!(
        super::server::cancel(&state, &admin, "missing").await,
        Err(crate::serve::error::ServeError::NotFound)
    ));
}

/// An inline deployment overlay is the request's own document merged into an
/// operator-written body: every adapter that accepts one screens it like a
/// submitted config, and refuses it outright for a tenant run.
#[tokio::test]
async fn an_inline_overlay_is_screened_as_caller_input_on_every_adapter() {
    let dir = tempfile::tempdir().unwrap();
    let state = fresh().await;
    tenant(&state, false, None).await;
    register(&state, &config(dir.path(), "tpl", "", false)).await;
    let overlay = json!({ "dlq": { "sink": { "type": "singer", "config": {
        "target_command": ["/bin/sh", "-c", "true"] } } } });
    let trigger = || -> crate::serve::handlers::templates::TriggerBody {
        serde_json::from_value(json!({ "version": "newest", "overlay": overlay })).unwrap()
    };
    let http = crate::serve::handlers::templates::trigger_template(
        State(state.clone()),
        Extension(alice(None)),
        Path("tpl".to_string()),
        Json(trigger()),
    )
    .await
    .map(|_| ())
    .map_err(error_text)
    .unwrap_err();
    assert!(
        http.contains("inline `overlay`") && http.contains("singer"),
        "{http}"
    );

    let tenant_route = crate::serve::handlers::tenants::trigger_tenant_template(
        State(state.clone()),
        Extension(alice(None)),
        Path(("acme".to_string(), "tpl".to_string())),
        Json(
            serde_json::from_value(json!({ "version": "newest",
            "overlay": { "execution": { "max_concurrent": 1 } } }))
            .unwrap(),
        ),
    )
    .await
    .map(|_| ())
    .map_err(error_text)
    .unwrap_err();
    assert!(tenant_route.contains("tenant"), "{tenant_route}");

    let ctx = crate::mcp::McpContext {
        auth: Default::default(),
        allow_mutations: true,
        allow_config_execution: true,
        allow_template_admin: false,
        templates: Some(state.history()),
        changes: None,
        submitted: Some(state.caller_origin()),
        approval_required: Vec::new(),
        server: Some(crate::mcp::ChangeProposer {
            state: state.clone(),
            actor: alice(None),
        }),
    };
    let out = crate::mcp::tools::call_tool(
        &ctx,
        "run_template",
        &json!({ "id": "tpl", "version": "newest",
                 "overlay": { "notifications": [{ "name": "n", "on": ["run_success"],
                    "channel": { "type": "webhook", "config": {
                        "url": "https://h.example/?k=${env:FAUCET_TEST_OVERLAY_SECRET}" } } }] } }),
    )
    .await;
    assert_eq!(out["isError"], true, "{out}");
    let text = out["content"][0]["text"].as_str().unwrap_or("");
    assert!(text.contains("FAUCET_TEST_OVERLAY_SECRET"), "{text}");
    assert_eq!(audited(&state, "template.run").await.len(), 3);
    assert!(
        state
            .history()
            .list(&crate::serve::history::ListFilter {
                limit: 100,
                ..Default::default()
            })
            .await
            .unwrap()
            .runs
            .is_empty(),
        "no run was started"
    );
}

// ── Caller-supplied template params (#989) ───────────────────────────────────

/// Where a caller triggers a registered template with params.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParamAdapter {
    Http,
    TenantHttp,
    Fanout,
    Mcp,
    McpDryRun,
    Change,
    Status,
    Mirror,
    /// A template trigger in the server's triggers file: the operator's
    /// params, never checked.
    TriggersFile,
}

const PARAM_ADAPTERS: [ParamAdapter; 9] = [
    ParamAdapter::Http,
    ParamAdapter::TenantHttp,
    ParamAdapter::Fanout,
    ParamAdapter::Mcp,
    ParamAdapter::McpDryRun,
    ParamAdapter::Change,
    ParamAdapter::Status,
    ParamAdapter::Mirror,
    ParamAdapter::TriggersFile,
];

/// A server whose caller policy allows only paths under `root`.
async fn fresh_rooted(root: &FsPath) -> ServerState {
    let mut config = crate::serve::test_support::test_config();
    config.references =
        crate::serve::references::ServeReferencePolicy::from_args(&crate::cli::ReferenceArgs {
            allow_path_root: vec![root.to_path_buf()],
            ..Default::default()
        })
        .unwrap();
    let state = crate::serve::test_support::state_from(&config);
    state.set_tenants(crate::serve::tenants::TenantsRuntime::new(
        Some(
            crate::serve::tenants::vault::Vault::new("0123456789abcdef0123456789abcdef", &[])
                .unwrap(),
        ),
        Default::default(),
    ));
    tenant(&state, false, None).await;
    state
}

/// A template whose sink path is the caller's `out` param.
fn param_template(dir: &FsPath, trusted: bool) -> String {
    let input = dir.join("in.csv");
    std::fs::write(&input, "id,email\n1,a@x.io\n").unwrap();
    format!(
        "version: 1\nname: pt\nparams:\n  out: {{ required: true{} }}\npipeline:\n  source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"${{param.out}}\" }} }}\n",
        if trusted { ", trusted: true" } else { "" },
        input.display(),
    )
}

async fn trigger_params(adapter: ParamAdapter, state: &ServerState, out: &str) -> Outcome {
    let trigger = |extra: Value| -> crate::serve::handlers::templates::TriggerBody {
        let mut body = json!({ "version": "newest", "params": { "out": out } });
        if let (Some(b), Some(e)) = (body.as_object_mut(), extra.as_object()) {
            b.extend(e.clone());
        }
        serde_json::from_value(body).unwrap()
    };
    let mcp = || crate::mcp::McpContext {
        auth: Default::default(),
        allow_mutations: true,
        allow_config_execution: true,
        allow_template_admin: false,
        templates: Some(state.history()),
        changes: None,
        submitted: Some(state.caller_origin()),
        approval_required: Vec::new(),
        server: Some(crate::mcp::ChangeProposer {
            state: state.clone(),
            actor: alice(None),
        }),
    };
    let source = || crate::serve::handlers::state::ConfigSource {
        template: Some("tpl".into()),
        version: Some("newest".into()),
        params: [("out".to_string(), json!(out))].into_iter().collect(),
        ..Default::default()
    };
    match adapter {
        ParamAdapter::Http => {
            match crate::serve::handlers::templates::trigger_template(
                State(state.clone()),
                Extension(alice(None)),
                Path("tpl".to_string()),
                Json(trigger(json!({}))),
            )
            .await
            {
                Ok(r) => response_run_id(r).await,
                Err(e) => Err(error_text(e)),
            }
        }
        ParamAdapter::TenantHttp => match crate::serve::handlers::tenants::trigger_tenant_template(
            State(state.clone()),
            Extension(alice(None)),
            Path(("acme".to_string(), "tpl".to_string())),
            Json(trigger(json!({}))),
        )
        .await
        {
            Ok(r) => response_run_id(r).await,
            Err(e) => Err(error_text(e)),
        },
        ParamAdapter::Fanout => {
            let body: crate::serve::handlers::tenants::FanoutBody = serde_json::from_value(
                json!({ "tenants": ["acme"], "version": "newest", "params": { "out": out } }),
            )
            .unwrap();
            let results =
                crate::serve::handlers::tenants::fan_out(state, &alice(None), "tpl", body, "f")
                    .await
                    .map_err(error_text)?;
            let r = &results[0];
            match r.status.as_str() {
                "submitted" => Ok(r.run_id.clone()),
                _ => Err(r.reason.clone().unwrap_or_default()),
            }
        }
        ParamAdapter::Mcp | ParamAdapter::McpDryRun => {
            let mut args = json!({ "id": "tpl", "version": "newest", "params": { "out": out } });
            if adapter == ParamAdapter::McpDryRun {
                args["dry_run"] = json!(true);
            }
            let res = crate::mcp::tools::call_tool(&mcp(), "run_template", &args).await;
            let text = res["content"][0]["text"].as_str().unwrap_or("").to_string();
            if res["isError"] == true {
                Err(text)
            } else {
                let v: Value = serde_json::from_str(&text).unwrap();
                Ok(v["run_id"].as_str().map(str::to_string))
            }
        }
        ParamAdapter::Change => {
            use crate::runtime::server::TriggerOutcome;
            let outcome = crate::serve::handlers::templates::trigger_template_outcome(
                state.clone(),
                alice(None),
                "tpl".into(),
                trigger(json!({ "require_approval": true })),
            )
            .await
            .map_err(error_text)?;
            let TriggerOutcome::PendingApproval(change) = outcome else {
                return Err("expected a pending change".into());
            };
            let approved = crate::serve::changes::approve(state, &alice(None), &change.id, None)
                .await
                .map_err(error_text)?;
            match approved.status {
                crate::serve::changes::ChangeStatus::Executed => Ok(approved.run_id),
                _ => Err(approved.error.unwrap_or_default()),
            }
        }
        ParamAdapter::Status => crate::serve::handlers::state::post_status(
            State(state.clone()),
            Extension(alice(None)),
            Json(crate::serve::handlers::state::StatusRequest {
                source: source(),
                ..Default::default()
            }),
        )
        .await
        .map(|_| None)
        .map_err(error_text),
        ParamAdapter::Mirror => crate::serve::handlers::state::post_mirror(
            State(state.clone()),
            Extension(alice(None)),
            Path("pt".to_string()),
            Json(source()),
        )
        .await
        .map(|_| None)
        .map_err(error_text),
        ParamAdapter::TriggersFile => {
            use crate::serve::triggers::spec::{
                RunTemplate, TemplateTrigger, TriggerKind, TriggerSpec,
            };
            let t: TemplateTrigger = serde_json::from_value(
                json!({ "id": "tpl", "version": "newest", "params": { "out": out } }),
            )
            .unwrap();
            let compiled = crate::serve::triggers::compiled::CompiledTrigger {
                spec: TriggerSpec {
                    name: "hook".into(),
                    enabled: true,
                    config: None,
                    template: Some(t),
                    tenants: None,
                    run: RunTemplate {
                        name: None,
                        labels: Default::default(),
                        timeout_secs: None,
                        selection: None,
                        depends_on: None,
                        pool: None,
                    },
                    kind: TriggerKind::Webhook {
                        methods: vec!["POST".into()],
                        dedupe_header: None,
                        debounce_secs: 0,
                    },
                },
                webhook_path: Some("/v1/triggers/hook".into()),
            };
            let event = crate::serve::triggers::context::TriggerEvent::Schedule {
                tick: uuid::Uuid::now_v7().to_string(),
            };
            use crate::serve::triggers::enqueue::FireOutcome;
            match crate::serve::triggers::enqueue::fire(state, &compiled, event, "now").await {
                FireOutcome::Enqueued(id) => Ok(Some(id)),
                FireOutcome::Failed(e) | FireOutcome::Error(e) => Err(e),
                other => Err(format!("{other:?}")),
            }
        }
    }
}

/// #989: every surface that runs a registered template holds the values a
/// caller supplies for its params to the caller reference policy — refused
/// (naming the param and the field, audited `policy.denied`, nothing
/// written) outside the allowed path roots, accepted inside them — except
/// a trigger in the server's own triggers file, whose params the operator
/// wrote.
#[tokio::test]
async fn every_template_surface_holds_caller_params_to_the_caller_policy() {
    for adapter in PARAM_ADAPTERS {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let state = fresh_rooted(root.path()).await;
        register(&state, &param_template(root.path(), false)).await;
        let target = outside.path().join("out.jsonl");
        let out = trigger_params(adapter, &state, &target.display().to_string()).await;
        if adapter == ParamAdapter::TriggersFile {
            let id = out
                .unwrap_or_else(|e| panic!("{adapter:?} refused an operator's param: {e}"))
                .unwrap();
            assert_eq!(finished(&state, &id).await.status, RunStatus::Completed);
            assert!(target.exists(), "{adapter:?}");
            continue;
        }
        let err = out.expect_err(&format!("{adapter:?} let a caller's path through"));
        assert!(
            err.contains("param 'out'") && err.contains("pipeline.sink.config.path"),
            "{adapter:?}: {err}"
        );
        assert!(!target.exists(), "{adapter:?} wrote");
        let denied = audited(&state, "policy.denied").await;
        assert!(
            denied.iter().any(|a| a.principal == "alice"),
            "{adapter:?}: {denied:?}"
        );

        // Inside an allowed root the same surface goes ahead.
        let inside = root.path().join("ok.jsonl");
        let out = trigger_params(adapter, &state, &inside.display().to_string()).await;
        match adapter {
            // A mirror read of a non-mirror config fails later, for its own
            // reason — not this one.
            ParamAdapter::Mirror => {
                if let Err(e) = out {
                    assert!(!e.contains("param 'out'"), "{adapter:?}: {e}");
                }
            }
            _ => {
                let id = out.unwrap_or_else(|e| panic!("{adapter:?} refused an allowed path: {e}"));
                if let Some(id) = id {
                    assert_eq!(finished(&state, &id).await.status, RunStatus::Completed);
                    assert!(inside.exists(), "{adapter:?}");
                }
            }
        }
    }
}

/// The acceptance case (#989): an operator's `/etc/faucet-x` for a template's
/// sink path is a 403 naming `x`; `trusted: true` lets the value through.
#[tokio::test]
async fn a_trusted_param_passes_unchecked_and_others_get_a_403() {
    let root = tempfile::tempdir().unwrap();
    let state = fresh_rooted(root.path()).await;
    let input = root.path().join("in.csv");
    std::fs::write(&input, "id\n1\n").unwrap();
    let body = |trusted: bool| {
        format!(
            "version: 1\nname: pt\nparams:\n  x: {{ required: true{} }}\npipeline:\n  source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n  dlq: {{ sink: {{ type: jsonl, config: {{ path: \"${{param.x}}\" }} }} }}\n",
            if trusted { ", trusted: true" } else { "" },
            input.display(),
            root.path().join("out.jsonl").display(),
        )
    };
    let trigger = |x: &str| -> crate::serve::handlers::templates::TriggerBody {
        serde_json::from_value(json!({ "version": "newest", "params": { "x": x } })).unwrap()
    };
    register(&state, &body(false)).await;
    let err = crate::serve::handlers::templates::trigger_template(
        State(state.clone()),
        Extension(AuthContext {
            role: Role::Operator,
            ..alice(None)
        }),
        Path("tpl".to_string()),
        Json(trigger("/etc/faucet-x")),
    )
    .await
    .map(|_| ())
    .unwrap_err();
    assert!(
        matches!(&err, crate::serve::error::ServeError::Forbidden(m)
            if m.contains("param 'x'") && m.contains("pipeline.dlq.sink.config.path")),
        "{err:?}"
    );
    assert!(!FsPath::new("/etc/faucet-x").exists());

    // A template marked `trusted` takes the value as given.
    let elsewhere = tempfile::tempdir().unwrap();
    let dlq = elsewhere.path().join("dlq.jsonl");
    register(&state, &body(true)).await;
    let resp = crate::serve::handlers::templates::trigger_template(
        State(state.clone()),
        Extension(alice(None)),
        Path("tpl".to_string()),
        Json(trigger(&dlq.display().to_string())),
    )
    .await
    .unwrap();
    let id = response_run_id(resp).await.unwrap().unwrap();
    assert_eq!(finished(&state, &id).await.status, RunStatus::Completed);
}

/// #989: a caller's `env` override is caller text like a param: one that
/// lands in a path outside the caller roots is refused naming the variable
/// (and the 403 shows none of the operator's own text around it); inside a
/// root it runs; a `${env:}` the server's own environment fills is the
/// operator's and is not checked.
#[tokio::test]
#[serial_test::serial(env)]
async fn an_env_override_is_held_to_the_caller_policy() {
    let root = tempfile::tempdir().unwrap();
    let state = fresh_rooted(root.path()).await;
    let input = root.path().join("in.csv");
    std::fs::write(&input, "id\n1\n").unwrap();
    let elsewhere = tempfile::tempdir().unwrap();
    // SAFETY: serialised on `env` with every other test that sets variables.
    unsafe {
        std::env::set_var("FAUCET_989_DIR", elsewhere.path());
        std::env::set_var("FAUCET_989_KEY", "operator-only-key-989");
    }
    register(
        &state,
        &format!(
            "version: 1\nname: pe\npipeline:\n  source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"${{env:FAUCET_989_DIR}}/${{env:FAUCET_989_KEY}}.jsonl\" }} }}\n",
            input.display(),
        ),
    )
    .await;
    let trigger = |env: Value| -> crate::serve::handlers::templates::TriggerBody {
        serde_json::from_value(json!({ "version": "newest", "env": env })).unwrap()
    };
    let run = |env: Value| {
        crate::serve::handlers::templates::trigger_template(
            State(state.clone()),
            Extension(AuthContext {
                role: Role::Operator,
                ..alice(None)
            }),
            Path("tpl".to_string()),
            Json(trigger(env)),
        )
    };
    // An override is registered for redaction like any `${env:}` value, so
    // the refusal shows it masked; the operator's text around it is elided.
    let err = run(json!({ "FAUCET_989_DIR": "/etc/faucet-989-override" }))
        .await
        .map(|_| ())
        .unwrap_err();
    let msg = error_text(err);
    assert!(
        msg.contains("env override 'FAUCET_989_DIR'") && msg.contains("pipeline.sink.config.path"),
        "{msg}"
    );
    assert!(msg.contains("…"), "{msg}");
    assert!(!msg.contains("operator-only-key-989"), "{msg}");
    assert!(
        !msg.contains(&elsewhere.path().display().to_string()),
        "{msg}"
    );

    // Inside a root the override goes ahead.
    let resp = run(json!({ "FAUCET_989_DIR": root.path().display().to_string() }))
        .await
        .unwrap();
    let id = response_run_id(resp).await.unwrap().unwrap();
    assert_eq!(finished(&state, &id).await.status, RunStatus::Completed);
    assert!(root.path().join("operator-only-key-989.jsonl").exists());

    // The server's own environment is the operator's: not checked.
    let resp = run(json!({})).await.unwrap();
    let id = response_run_id(resp).await.unwrap().unwrap();
    assert_eq!(finished(&state, &id).await.status, RunStatus::Completed);
    assert!(
        elsewhere
            .path()
            .join("operator-only-key-989.jsonl")
            .exists()
    );
}

/// A clustered run re-checks its caller's param values on the instance
/// that claims it: the stored record carries where they are.
#[tokio::test]
async fn a_claimed_run_rechecks_its_callers_params() {
    let root = tempfile::tempdir().unwrap();
    let state = fresh_rooted(root.path()).await;
    let input = root.path().join("in.csv");
    std::fs::write(&input, "id\n1\n").unwrap();
    let body = json!({
        "version": 1,
        "pipeline": {
            "source": { "type": "csv", "config": { "path": input.display().to_string() } },
            "sink": { "type": "jsonl", "config": { "path": "/etc/faucet-x" } }
        }
    })
    .to_string();
    let mut rec = crate::serve::history::RunRecord::queued(
        "r1".into(),
        None,
        Default::default(),
        None,
        chrono::Utc::now(),
    );
    rec.config_format = Some(crate::serve::load::ConfigFormat::Json);
    // Without the record of where the caller's value is, the body is the
    // operator's and loads.
    super::server::reload(&state, &rec, &body).await.unwrap();
    rec.caller_params.insert("/pipeline/sink/config/path", "x");
    let err = super::server::reload(&state, &rec, &body)
        .await
        .unwrap_err();
    assert!(
        error_text(err).contains("param 'x'"),
        "a claimed run skipped its caller's params"
    );
}
