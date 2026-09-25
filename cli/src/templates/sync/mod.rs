//! Template hosting + sync (RFC 0006 / #589): pull pipeline templates from a
//! remote **origin** — a GitHub directory, or an S3 / GCS / Azure Blob prefix —
//! into the registry, and publish one back.
//!
//! The registry stays the single source of truth for *what runs*; an origin is
//! where templates are **authored** (reviewed in a repo, dropped in a bucket).
//! A pull is `list → pair → plan → apply`:
//!
//! - [`fetch`] lists the origin's directory and pairs each `<id>.yaml` /
//!   `.json` with its optional `<id>.faucet.yaml` sidecar;
//! - [`plan`] diffs that against the registry (pure) — a new or changed body
//!   appends a version, an unchanged one is a no-op, a vanished one is
//!   reported or deprecated per `prune`, and `stable` moves only under
//!   `launch: follow` / `always`;
//! - [`apply`] runs the plan through the ordinary register / launch /
//!   deprecate verbs, collecting per-template failures.
//!
//! Every origin owns an id namespace (its `prefix`), and overlapping prefixes
//! are a load-time error ([`spec::SyncFile::validate`]), so no template ever
//! has two owners. `publish` is the deliberate, manual reverse direction: it
//! writes one registered version to an origin and never runs on its own.

pub mod apply;
pub mod fetch;
pub mod plan;
pub mod spec;

use std::path::Path;
use std::sync::Arc;

use serde::Serialize;

use crate::error::{CliError, CliResult};
use crate::serve::history::templates::VersionSelector;
use crate::serve::load::ConfigFormat;
use crate::templates::TemplateStore;

pub use apply::ApplyOutcome;
pub use plan::{LocalTemplate, SyncAction, SyncPlan};
pub use spec::{LaunchPolicy, Origin, OriginSource, PrunePolicy, Sidecar, SyncFile};

/// Metric names.
pub const METRIC_SYNC_RUNS: &str = "faucet_serve_template_sync_runs_total";
pub const METRIC_SYNC_MUTATIONS: &str = "faucet_serve_template_sync_mutations_total";
pub const METRIC_SYNC_LAST: &str = "faucet_serve_template_sync_last_unix_seconds";

/// Register HELP text for the sync metric family (idempotent).
pub fn describe_metrics() {
    metrics::describe_counter!(
        METRIC_SYNC_RUNS,
        "Template sync pulls per origin, by outcome (ok|partial|error)."
    );
    metrics::describe_counter!(
        METRIC_SYNC_MUTATIONS,
        "Registry mutations (registers, launches, deprecations, revivals) made by template sync, per origin."
    );
    metrics::describe_gauge!(
        METRIC_SYNC_LAST,
        "Unix time of the last template sync attempt per origin."
    );
}

/// The result of syncing one origin — the plan, and what applying it did
/// (absent on a dry run).
#[derive(Debug, Clone, Serialize)]
pub struct SyncReport {
    pub origin: String,
    pub kind: &'static str,
    pub dry_run: bool,
    /// Files the origin listing could not use (see [`fetch::pair_files`]).
    pub warnings: Vec<String>,
    pub plan: Vec<SyncAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<ApplyOutcome>,
}

impl SyncReport {
    /// Whether any per-template action failed.
    pub fn failed(&self) -> usize {
        self.outcome.as_ref().map(|o| o.failed.len()).unwrap_or(0)
    }

    /// Registry mutations planned (dry run) or performed.
    pub fn mutations(&self) -> usize {
        match &self.outcome {
            Some(o) => o.mutations(),
            None => self.plan.iter().filter(|a| a.is_mutation()).count(),
        }
    }

    /// One-line-per-action human rendering.
    pub fn render_human(&self) -> String {
        let mut s = String::new();
        let verb = if self.dry_run { "would sync" } else { "synced" };
        s.push_str(&format!(
            "origin '{}' ({}): {verb} {} change(s)\n",
            self.origin,
            self.kind,
            self.mutations()
        ));
        for a in &self.plan {
            let line = match a {
                SyncAction::Register {
                    id,
                    launch,
                    replaces,
                    ..
                } => format!(
                    "  register   {id}{}{}",
                    replaces
                        .map(|v| format!(" (new version after v{v})"))
                        .unwrap_or_default(),
                    if *launch { " → launch" } else { "" }
                ),
                SyncAction::Launch { id, version } => format!("  launch     {id} v{version}"),
                SyncAction::Revive { id } => format!("  revive     {id}"),
                SyncAction::Unchanged { id, version } => format!("  unchanged  {id} (v{version})"),
                SyncAction::Orphaned { id } => format!("  orphaned   {id} (gone upstream; kept)"),
                SyncAction::Deprecate { id } => format!("  deprecate  {id} (gone upstream)"),
                SyncAction::DeprecateVersion {
                    id,
                    version,
                    reason,
                } => format!("  deprecate  {id} v{version} ({reason})"),
                SyncAction::Skipped { name, reason } => format!("  skipped    {name}: {reason}"),
            };
            s.push_str(&line);
            s.push('\n');
        }
        for w in &self.warnings {
            s.push_str(&format!("  warning    {w}\n"));
        }
        if let Some(o) = &self.outcome {
            for f in &o.failed {
                s.push_str(&format!("  FAILED     {}: {}\n", f.id, f.error));
            }
        }
        s
    }
}

/// The result of publishing one version to an origin.
#[derive(Debug, Clone, Serialize)]
pub struct PublishReport {
    pub id: String,
    pub version: u32,
    pub origin: String,
    /// File name written under the origin's directory.
    pub name: String,
    /// Where it landed, as the adapter describes it.
    pub location: String,
}

/// Read, interpolate (`${env:…}` / `${file:…}` / `${secret:…}`), parse, and
/// validate a sync file. GitHub tokens are registered for log redaction.
pub async fn load_sync_file(path: &Path) -> CliResult<SyncFile> {
    let text = tokio::fs::read_to_string(path).await.map_err(|e| {
        CliError::Config(format!(
            "reading templates-sync file {}: {e}",
            path.display()
        ))
    })?;
    let text = crate::interpolate::interpolate(&text)?;
    let file = parse_sync_file(&text, path)?;
    file.validate()?;
    for o in &file.origins {
        if let OriginSource::Github(g) = &o.source
            && let Some(t) = &g.token
        {
            crate::secrets::registry::register(t);
        }
        if o.source.needs_object_store() && !cfg!(feature = "templates-sync-object-store") {
            return Err(CliError::Config(format!(
                "templates-sync: origin '{}' is a `{}` source, which needs a build with the \
                 `templates-sync-object-store` feature",
                o.name,
                o.source.kind()
            )));
        }
    }
    Ok(file)
}

fn parse_sync_file(text: &str, path: &Path) -> CliResult<SyncFile> {
    let is_json = path
        .extension()
        .map(|e| e.eq_ignore_ascii_case("json"))
        .unwrap_or(false);
    if is_json {
        serde_json::from_str(text)
            .map_err(|e| CliError::Config(format!("parsing templates-sync JSON: {e}")))
    } else {
        serde_yaml::from_str(text)
            .map_err(|e| CliError::Config(format!("parsing templates-sync YAML: {e}")))
    }
}

/// Snapshot the registry's view of every template under `prefix`.
pub async fn local_snapshot(store: &TemplateStore, prefix: &str) -> CliResult<Vec<LocalTemplate>> {
    let summaries = crate::templates::list_with_state(store).await?;
    let mut out = Vec::new();
    for s in summaries {
        if !s.id.starts_with(prefix) {
            continue;
        }
        let state = match s.state {
            Some(st) => st,
            None => crate::templates::template_state(store, &s.id).await?,
        };
        // Sync reasons about the newest *registered* body, so a version this
        // registry retired still counts as the one the origin last delivered.
        let newest = state.versions.first().copied();
        let newest_hash = match newest {
            Some(v) => match store.template_get(&s.id, Some(v)).await {
                Ok(Some(rec)) => plan::body_hash(&rec.body).ok(),
                Ok(None) => None,
                Err(e) => {
                    return Err(CliError::Internal(format!("template registry read: {e}")));
                }
            },
            None => None,
        };
        out.push(LocalTemplate {
            newest_deprecated: newest.is_some_and(|v| state.version_deprecation(v).is_some()),
            id: s.id,
            status: state.status,
            newest,
            newest_hash,
            stable: state.stable,
        });
    }
    Ok(out)
}

/// The `created_by` recorded for a pull.
pub fn sync_actor(origin: &str) -> String {
    format!("sync:{origin}")
}

fn now_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Pull one origin. Fetch or registry-read errors propagate (nothing was
/// changed); per-template apply failures are collected in the report.
pub async fn sync_origin(
    store: &TemplateStore,
    origin: &Origin,
    dry_run: bool,
    actor: Option<&str>,
) -> CliResult<SyncReport> {
    let labels = [("origin", origin.name.clone())];
    metrics::gauge!(METRIC_SYNC_LAST, &labels).set(now_secs());
    let result = sync_origin_inner(store, origin, dry_run, actor).await;
    let outcome = match &result {
        Ok(r) if r.failed() > 0 => "partial",
        Ok(_) => "ok",
        Err(_) => "error",
    };
    metrics::counter!(METRIC_SYNC_RUNS, "origin" => origin.name.clone(), "outcome" => outcome)
        .increment(1);
    if let Ok(r) = &result
        && !r.dry_run
    {
        metrics::counter!(METRIC_SYNC_MUTATIONS, &labels).increment(r.mutations() as u64);
    }
    result
}

async fn sync_origin_inner(
    store: &TemplateStore,
    origin: &Origin,
    dry_run: bool,
    actor: Option<&str>,
) -> CliResult<SyncReport> {
    let fetcher = fetch::fetcher_for(&origin.source)?;
    let files = fetcher.list().await?;
    let mut paired = fetch::pair_files(files);
    match fetcher.catalog_index().await {
        Ok(Some(index)) => fetch::apply_catalog_index(&mut paired.templates, &index),
        Ok(None) => {}
        // Deprecation is advisory for the mirror; an unreadable index is
        // surfaced rather than failing the whole pull.
        Err(e) => paired.warnings.push(format!(
            "catalog index.json unreadable ({e}); version deprecations not applied"
        )),
    }
    let local = local_snapshot(store, &origin.prefix).await?;
    let plan = plan::plan(origin, &paired.templates, &local);
    tracing::info!(
        origin = %origin.name,
        remote = paired.templates.len(),
        local = local.len(),
        mutations = plan.mutations(),
        dry_run,
        "template sync planned"
    );
    let actions = plan.actions.clone();
    let outcome = if dry_run {
        None
    } else {
        let actor = actor
            .map(str::to_string)
            .unwrap_or_else(|| sync_actor(&origin.name));
        Some(apply::apply(store, plan, &actor).await)
    };
    Ok(SyncReport {
        origin: origin.name.clone(),
        kind: origin.source.kind(),
        dry_run,
        warnings: paired.warnings,
        plan: actions,
        outcome,
    })
}

/// Pull every origin (or just `only`). A failing origin does not stop the
/// others; its error is returned in place of a report.
pub async fn sync_all(
    store: &TemplateStore,
    file: &SyncFile,
    only: Option<&str>,
    dry_run: bool,
    actor: Option<&str>,
) -> CliResult<Vec<Result<SyncReport, (String, CliError)>>> {
    let origins: Vec<&Origin> = match only {
        Some(name) => vec![file.origin(name)?],
        None => file.origins.iter().collect(),
    };
    let mut out = Vec::with_capacity(origins.len());
    for o in origins {
        out.push(
            sync_origin(store, o, dry_run, actor)
                .await
                .map_err(|e| (o.name.clone(), e)),
        );
    }
    Ok(out)
}

/// The file stem a template publishes under at `origin` — its id with the
/// origin's prefix removed. Refuses ids outside the origin's namespace.
pub fn publish_stem<'a>(id: &'a str, origin: &Origin) -> CliResult<&'a str> {
    let stem = id.strip_prefix(&origin.prefix).ok_or_else(|| {
        CliError::Config(format!(
            "template '{id}' is outside origin '{}' (prefix '{}') — publishing it there would \
             re-register as '{}{id}' on the next pull",
            origin.name, origin.prefix, origin.prefix
        ))
    })?;
    if stem.is_empty() {
        return Err(CliError::Config(format!(
            "template '{id}' is exactly origin '{}''s prefix; nothing is left for a file name",
            origin.name
        )));
    }
    Ok(stem)
}

/// File name a registered version publishes as.
pub fn publish_name(id: &str, origin: &Origin, format: ConfigFormat) -> CliResult<String> {
    let stem = publish_stem(id, origin)?;
    let ext = match format {
        ConfigFormat::Yaml => "yaml",
        ConfigFormat::Json => "json",
    };
    Ok(format!("{stem}.{ext}"))
}

/// Write one registered version to an origin. Manual and explicit — the only
/// path by which anything flows registry → origin.
pub async fn publish(
    store: &TemplateStore,
    file: &SyncFile,
    id: &str,
    origin_name: &str,
    selector: VersionSelector,
) -> CliResult<PublishReport> {
    let origin = file.origin(origin_name)?;
    publish_stem(id, origin)?;
    let version = crate::templates::resolve_version(store, id, selector).await?;
    let record = store
        .template_get(id, Some(version))
        .await
        .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?
        .ok_or_else(|| CliError::UnknownPipelineTemplate {
            id: id.to_string(),
            version: Some(version),
        })?;
    let name = publish_name(id, origin, record.format)?;
    let publisher = fetch::publisher_for(&origin.source)?;
    let location = publisher.put(&name, &record.body).await?;
    tracing::info!(template = %id, version, origin = %origin.name, %location, "template published");
    Ok(PublishReport {
        id: id.to_string(),
        version,
        origin: origin.name.clone(),
        name,
        location,
    })
}

/// Spawn one periodic pull task per origin that declares `interval_secs`.
/// Each runs until `shutdown` fires; failures are logged and counted, never
/// fatal.
pub fn spawn_interval_syncs(
    store: TemplateStore,
    file: Arc<SyncFile>,
    shutdown: tokio_util::sync::CancellationToken,
) -> Vec<tokio::task::JoinHandle<()>> {
    file.origins
        .iter()
        .filter_map(|o| o.interval_secs.map(|s| (o.name.clone(), s)))
        .map(|(name, secs)| {
            let store = store.clone();
            let file = Arc::clone(&file);
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                let interval = std::time::Duration::from_secs(secs);
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => break,
                        _ = tokio::time::sleep(interval) => {}
                    }
                    let Ok(origin) = file.origin(&name) else { break };
                    match sync_origin(&store, origin, false, None).await {
                        Ok(r) => {
                            if r.failed() > 0 {
                                tracing::warn!(origin = %name, failed = r.failed(), "periodic template sync had failures");
                            }
                        }
                        Err(e) => tracing::warn!(origin = %name, error = %e, "periodic template sync failed"),
                    }
                }
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::memory::MemoryHistory;
    use crate::serve::history::templates::TemplateStatus;
    use spec::GithubSource;
    use std::time::Duration;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const BODY: &str = "version: 1\nname: t\npipeline:\n  source: {type: rest, config: {base_url: \"https://x\", path: /e}}\n  sink: {type: stdout, config: {}}\n";

    fn store() -> TemplateStore {
        Arc::new(MemoryHistory::new(Duration::from_secs(60)))
    }

    fn github_origin(
        server: &MockServer,
        prefix: &str,
        launch: LaunchPolicy,
        prune: PrunePolicy,
    ) -> Origin {
        Origin {
            name: "gh".into(),
            source: OriginSource::Github(GithubSource {
                repo: "acme/tpl".into(),
                r#ref: "main".into(),
                path: "templates".into(),
                paths: Vec::new(),
                token: Some("ghp_secret_token".into()),
                api_base: server.uri(),
            }),
            prefix: prefix.into(),
            launch,
            prune,
            interval_secs: None,
        }
    }

    /// Mount a directory listing + raw file reads on the mock GitHub API.
    async fn mount_repo(server: &MockServer, files: &[(&str, &str)]) {
        let entries: Vec<serde_json::Value> = files
            .iter()
            .map(|(n, _)| {
                serde_json::json!({
                    "name": n, "type": "file", "sha": "abc",
                    "url": format!("{}/repos/acme/tpl/contents/templates/{n}?ref=main", server.uri()),
                })
            })
            .chain(std::iter::once(serde_json::json!({
                "name": "sub", "type": "dir", "url": format!("{}/repos/acme/tpl/contents/templates/sub", server.uri())
            })))
            .collect();
        Mock::given(method("GET"))
            .and(path("/repos/acme/tpl/contents/templates"))
            .and(query_param("ref", "main"))
            .and(header("Authorization", "Bearer ghp_secret_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(entries))
            .mount(server)
            .await;
        // Subdirectories are read one level down (owner namespaces, #682);
        // this one is empty, so it contributes nothing.
        Mock::given(method("GET"))
            .and(path("/repos/acme/tpl/contents/templates/sub"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(server)
            .await;
        for (n, body) in files {
            Mock::given(method("GET"))
                .and(path(format!("/repos/acme/tpl/contents/templates/{n}")))
                .and(header("Accept", "application/vnd.github.raw+json"))
                .respond_with(ResponseTemplate::new(200).set_body_string(*body))
                .mount(server)
                .await;
        }
    }

    #[tokio::test]
    async fn github_pull_registers_then_is_idempotent_then_appends_on_change() {
        let server = MockServer::start().await;
        mount_repo(
            &server,
            &[
                ("sync.yaml", BODY),
                ("sync.faucet.yaml", "launch: true\ndescription: Nightly sync"),
                ("plain.json", r#"{"version":1,"pipeline":{"source":{"type":"rest","config":{"base_url":"https://y","path":"/p"}},"sink":{"type":"stdout","config":{}}}}"#),
                ("README.md", "# ignored"),
            ],
        )
        .await;
        let s = store();
        let o = github_origin(&server, "plat-", LaunchPolicy::Follow, PrunePolicy::Keep);

        // Dry run: plans, mutates nothing.
        let r = sync_origin(&s, &o, true, None).await.unwrap();
        assert!(r.dry_run && r.outcome.is_none());
        assert_eq!(r.mutations(), 2);
        assert!(
            crate::templates::list_with_state(&s)
                .await
                .unwrap()
                .is_empty()
        );
        let human = r.render_human();
        assert!(
            human.contains("would sync 2 change(s)") && human.contains("plat-sync → launch"),
            "{human}"
        );

        // Real pull.
        let r = sync_origin(&s, &o, false, None).await.unwrap();
        let o1 = r.outcome.as_ref().unwrap();
        assert_eq!(o1.registered.len(), 2, "{o1:?}");
        assert!(o1.failed.is_empty());
        let st = crate::templates::template_state(&s, "plat-sync")
            .await
            .unwrap();
        assert_eq!(
            st.status,
            TemplateStatus::Launched,
            "sidecar launch: true under follow"
        );
        let st = crate::templates::template_state(&s, "plat-plain")
            .await
            .unwrap();
        assert_eq!(
            st.status,
            TemplateStatus::Draft,
            "no sidecar → not launched"
        );
        let rec = s.template_get("plat-sync", Some(1)).await.unwrap().unwrap();
        assert_eq!(rec.description.as_deref(), Some("Nightly sync"));
        assert_eq!(rec.created_by.as_deref(), Some("sync:gh"));
        assert_eq!(rec.format, ConfigFormat::Yaml);
        assert_eq!(
            s.template_get("plat-plain", Some(1))
                .await
                .unwrap()
                .unwrap()
                .format,
            ConfigFormat::Json
        );

        // Idempotent: a second pull registers nothing.
        let r = sync_origin(&s, &o, false, None).await.unwrap();
        assert_eq!(r.mutations(), 0);
        assert_eq!(r.outcome.as_ref().unwrap().unchanged, 2);
        assert_eq!(s.template_versions("plat-sync").await.unwrap(), vec![1]);

        // Upstream change → a new version, and `follow` launches it.
        server.reset().await;
        let changed = BODY.replace("path: /e", "path: /e2");
        mount_repo(
            &server,
            &[("sync.yaml", &changed), ("sync.faucet.yaml", "launch: true"), ("plain.json", "{\"version\":1,\"pipeline\":{\"source\":{\"type\":\"rest\",\"config\":{\"base_url\":\"https://y\",\"path\":\"/p\"}},\"sink\":{\"type\":\"stdout\",\"config\":{}}}}")],
        )
        .await;
        let r = sync_origin(&s, &o, false, None).await.unwrap();
        let out = r.outcome.unwrap();
        assert_eq!(out.registered.len(), 1);
        assert_eq!(out.registered[0].version, 2);
        let st = crate::templates::template_state(&s, "plat-sync")
            .await
            .unwrap();
        assert_eq!(st.stable, Some(2));
        assert_eq!(
            st.previous,
            Some(1),
            "the launch log keeps the rollback target"
        );
    }

    #[tokio::test]
    async fn prune_deprecate_retires_vanished_templates_and_leaves_other_namespaces() {
        let server = MockServer::start().await;
        mount_repo(&server, &[("keep.yaml", BODY)]).await;
        let s = store();
        // A template in this origin's namespace that is not upstream, and one
        // outside the namespace.
        for id in ["p-gone", "unrelated"] {
            crate::templates::register(
                &s,
                crate::templates::RegisterRequest {
                    id: Some(id.into()),
                    body: BODY.into(),
                    format: ConfigFormat::Yaml,
                    description: None,
                    tags: vec![],
                    launch: true,
                    created_by: None,
                },
            )
            .await
            .unwrap();
        }
        let o = github_origin(&server, "p-", LaunchPolicy::Ignore, PrunePolicy::Deprecate);
        let r = sync_origin(&s, &o, false, None).await.unwrap();
        let out = r.outcome.unwrap();
        assert_eq!(out.deprecated, vec!["p-gone"]);
        assert_eq!(out.registered.len(), 1);
        assert_eq!(
            crate::templates::template_state(&s, "unrelated")
                .await
                .unwrap()
                .status,
            TemplateStatus::Launched,
            "another namespace is never touched"
        );
        // Second pull: the orphan is reported, not re-deprecated.
        let r = sync_origin(&s, &o, false, None).await.unwrap();
        assert!(
            r.plan
                .iter()
                .any(|a| matches!(a, SyncAction::Orphaned { id } if id == "p-gone"))
        );
        assert_eq!(r.mutations(), 0);
    }

    #[tokio::test]
    async fn fetch_errors_propagate_without_touching_the_registry() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/acme/tpl/contents/templates"))
            .respond_with(ResponseTemplate::new(404).set_body_string("Not Found"))
            .mount(&server)
            .await;
        let s = store();
        let o = github_origin(&server, "", LaunchPolicy::Ignore, PrunePolicy::Keep);
        let err = sync_origin(&s, &o, false, None)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("HTTP 404") && err.contains("private repo needs a token"),
            "{err}"
        );
        assert!(
            crate::templates::list_with_state(&s)
                .await
                .unwrap()
                .is_empty()
        );

        // A file where a directory was expected.
        server.reset().await;
        Mock::given(method("GET"))
            .and(path("/repos/acme/tpl/contents/templates"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"name": "templates", "type": "file"})),
            )
            .mount(&server)
            .await;
        let err = sync_origin(&s, &o, false, None)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("is a file, not a directory"), "{err}");
    }

    #[tokio::test]
    async fn sync_all_isolates_a_failing_origin_and_honours_only() {
        let server = MockServer::start().await;
        mount_repo(&server, &[("a.yaml", BODY)]).await;
        let good = github_origin(&server, "g-", LaunchPolicy::Ignore, PrunePolicy::Keep);
        let mut bad = github_origin(&server, "b-", LaunchPolicy::Ignore, PrunePolicy::Keep);
        bad.name = "bad".into();
        if let OriginSource::Github(g) = &mut bad.source {
            g.path = "missing".into();
        }
        let file = SyncFile {
            version: 1,
            origins: vec![good, bad],
        };
        let s = store();
        let results = sync_all(&s, &file, None, false, Some("alice"))
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok());
        let (name, _) = results[1].as_ref().unwrap_err();
        assert_eq!(name, "bad");
        let rec = s.template_get("g-a", Some(1)).await.unwrap().unwrap();
        assert_eq!(
            rec.created_by.as_deref(),
            Some("alice"),
            "an HTTP principal is recorded as the actor"
        );

        let results = sync_all(&s, &file, Some("gh"), true, None).await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(sync_all(&s, &file, Some("nope"), true, None).await.is_err());
    }

    #[tokio::test]
    async fn publish_writes_the_registered_body_via_the_contents_api() {
        let server = MockServer::start().await;
        let s = store();
        crate::templates::register(
            &s,
            crate::templates::RegisterRequest {
                id: Some("plat-out".into()),
                body: BODY.into(),
                format: ConfigFormat::Yaml,
                description: None,
                tags: vec![],
                launch: true,
                created_by: None,
            },
        )
        .await
        .unwrap();
        let o = github_origin(&server, "plat-", LaunchPolicy::Ignore, PrunePolicy::Keep);
        let file = SyncFile {
            version: 1,
            origins: vec![o],
        };
        // First publish: no existing file (404) → create without `sha`.
        Mock::given(method("GET"))
            .and(path("/repos/acme/tpl/contents/templates/out.yaml"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;
        let put = Mock::given(method("PUT"))
            .and(path("/repos/acme/tpl/contents/templates/out.yaml"))
            .and(header("Authorization", "Bearer ghp_secret_token"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "content": {"html_url": "https://github.com/acme/tpl/blob/main/templates/out.yaml"}
            })))
            .expect(1)
            .mount_as_scoped(&server)
            .await;
        let r = publish(&s, &file, "plat-out", "gh", VersionSelector::default())
            .await
            .unwrap();
        assert_eq!((r.version, r.name.as_str()), (1, "out.yaml"));
        assert!(r.location.ends_with("templates/out.yaml"));
        let reqs = server.received_requests().await.unwrap();
        let put_req = reqs.iter().find(|r| r.method == "PUT").expect("PUT sent");
        let payload: serde_json::Value = serde_json::from_slice(&put_req.body).unwrap();
        assert_eq!(payload["branch"], "main");
        assert!(payload.get("sha").is_none(), "create must not carry a sha");
        use base64::Engine as _;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(payload["content"].as_str().unwrap())
            .unwrap();
        assert_eq!(
            String::from_utf8(decoded).unwrap(),
            BODY,
            "body is written verbatim"
        );
        drop(put);

        // Second publish: the file exists → update carries its sha.
        server.reset().await;
        Mock::given(method("GET"))
            .and(path("/repos/acme/tpl/contents/templates/out.yaml"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "out.yaml", "type": "file", "sha": "deadbeef",
                "url": "u"
            })))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/repos/acme/tpl/contents/templates/out.yaml"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"content": {}})),
            )
            .mount(&server)
            .await;
        let r = publish(&s, &file, "plat-out", "gh", VersionSelector::Pinned(1))
            .await
            .unwrap();
        assert_eq!(
            r.location, "acme/tpl:main/out.yaml",
            "falls back to repo:ref/name without an html_url"
        );
        let reqs = server.received_requests().await.unwrap();
        let put_req = reqs.iter().find(|r| r.method == "PUT").unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&put_req.body).unwrap();
        assert_eq!(payload["sha"], "deadbeef");

        // Outside the origin's namespace / unknown origin / unknown version.
        let err = publish(&s, &file, "other", "gh", VersionSelector::default())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("outside origin 'gh'"), "{err}");
        assert!(
            publish(&s, &file, "plat-out", "nope", VersionSelector::default())
                .await
                .is_err()
        );
        assert!(matches!(
            publish(&s, &file, "plat-out", "gh", VersionSelector::Pinned(9)).await,
            Err(CliError::UnknownPipelineTemplate { .. })
        ));
    }

    #[test]
    fn publish_name_maps_prefix_and_format() {
        let o = Origin {
            name: "o".into(),
            source: OriginSource::Github(GithubSource {
                repo: "a/b".into(),
                r#ref: "main".into(),
                path: String::new(),
                paths: Vec::new(),
                token: None,
                api_base: String::new(),
            }),
            prefix: "p-".into(),
            launch: LaunchPolicy::Ignore,
            prune: PrunePolicy::Keep,
            interval_secs: None,
        };
        assert_eq!(
            publish_name("p-x", &o, ConfigFormat::Yaml).unwrap(),
            "x.yaml"
        );
        assert_eq!(
            publish_name("p-x", &o, ConfigFormat::Json).unwrap(),
            "x.json"
        );
        assert!(publish_name("q-x", &o, ConfigFormat::Yaml).is_err());
        assert!(publish_name("p-", &o, ConfigFormat::Yaml).is_err());
    }

    #[tokio::test]
    async fn load_sync_file_interpolates_validates_and_redacts() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("sync.yaml");
        // SAFETY (test): single-threaded access to this variable name.
        unsafe { std::env::set_var("FAUCET_TEST_SYNC_TOKEN", "tok-very-secret") };
        std::fs::write(
            &p,
            "version: 1\norigins:\n  - name: gh\n    source:\n      type: github\n      config: {repo: a/b, token: \"${env:FAUCET_TEST_SYNC_TOKEN}\"}\n    prefix: gh-\n",
        )
        .unwrap();
        let f = load_sync_file(&p).await.unwrap();
        match &f.origins[0].source {
            OriginSource::Github(g) => assert_eq!(g.token.as_deref(), Some("tok-very-secret")),
            other => panic!("{other:?}"),
        }
        assert_eq!(
            crate::secrets::registry::redact("token tok-very-secret here"),
            "token *** here"
        );

        // Invalid (overlapping prefixes) is a load error.
        std::fs::write(
            &p,
            "version: 1\norigins:\n  - {name: a, prefix: x-, source: {type: github, config: {repo: a/b}}}\n  - {name: b, prefix: x-, source: {type: github, config: {repo: a/c}}}\n",
        )
        .unwrap();
        assert!(
            load_sync_file(&p)
                .await
                .unwrap_err()
                .to_string()
                .contains("overlapping")
        );

        // JSON is accepted by extension; a missing file is a config error.
        let j = dir.path().join("sync.json");
        std::fs::write(&j, r#"{"version":1,"origins":[{"name":"a","source":{"type":"github","config":{"repo":"a/b"}}}]}"#).unwrap();
        assert_eq!(load_sync_file(&j).await.unwrap().origins.len(), 1);
        assert!(load_sync_file(&dir.path().join("nope.yaml")).await.is_err());
        // Unknown keys are rejected.
        std::fs::write(&p, "version: 1\norigins: []\nextra: 1\n").unwrap();
        assert!(
            load_sync_file(&p)
                .await
                .unwrap_err()
                .to_string()
                .contains("extra")
        );
    }

    #[tokio::test]
    async fn local_snapshot_filters_by_prefix_and_hashes_the_newest_body() {
        let s = store();
        for (id, body) in [("p-a", BODY), ("q-b", BODY)] {
            crate::templates::register(
                &s,
                crate::templates::RegisterRequest {
                    id: Some(id.into()),
                    body: body.into(),
                    format: ConfigFormat::Yaml,
                    description: None,
                    tags: vec![],
                    launch: false,
                    created_by: None,
                },
            )
            .await
            .unwrap();
        }
        let snap = local_snapshot(&s, "p-").await.unwrap();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].id, "p-a");
        assert_eq!(snap[0].newest, Some(1));
        assert_eq!(
            snap[0].newest_hash.as_deref(),
            Some(plan::body_hash(BODY).unwrap().as_str())
        );
        assert_eq!(snap[0].stable, None);
        assert_eq!(local_snapshot(&s, "").await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn interval_tasks_pull_until_shutdown() {
        let server = MockServer::start().await;
        mount_repo(&server, &[("t.yaml", BODY)]).await;
        let mut o = github_origin(&server, "i-", LaunchPolicy::Ignore, PrunePolicy::Keep);
        o.interval_secs = Some(1);
        let mut none = github_origin(&server, "n-", LaunchPolicy::Ignore, PrunePolicy::Keep);
        none.name = "none".into();
        let file = Arc::new(SyncFile {
            version: 1,
            origins: vec![o, none],
        });
        let s = store();
        let shutdown = tokio_util::sync::CancellationToken::new();
        // Real time, not `tokio::time::pause()`: the pull is a real HTTP
        // round-trip to the mock server, which paused time cannot drive
        // deterministically (it flaked on CI). One-second interval, bounded
        // real-time wait.
        let handles = spawn_interval_syncs(s.clone(), file, shutdown.clone());
        assert_eq!(
            handles.len(),
            1,
            "only origins with interval_secs get a task"
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while s.template_get("i-t", None).await.unwrap().is_none() {
            assert!(
                std::time::Instant::now() < deadline,
                "periodic pull did not register the template within 20s"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        shutdown.cancel();
        for h in handles {
            h.await.unwrap();
        }
    }

    #[test]
    fn report_rendering_covers_every_action_shape() {
        let r = SyncReport {
            origin: "o".into(),
            kind: "s3",
            dry_run: false,
            warnings: vec!["w1".into()],
            plan: vec![
                SyncAction::Register {
                    id: "a".into(),
                    body: String::new(),
                    format: ConfigFormat::Yaml,
                    description: None,
                    launch: true,
                    tags: vec![],
                    replaces: Some(2),
                },
                SyncAction::Launch {
                    id: "b".into(),
                    version: 3,
                },
                SyncAction::Revive { id: "c".into() },
                SyncAction::Unchanged {
                    id: "d".into(),
                    version: 1,
                },
                SyncAction::Orphaned { id: "e".into() },
                SyncAction::Deprecate { id: "f".into() },
                SyncAction::DeprecateVersion {
                    id: "h".into(),
                    version: 4,
                    reason: "catalog v4 is deprecated: broken".into(),
                },
                SyncAction::Skipped {
                    name: "g".into(),
                    reason: "why".into(),
                },
            ],
            outcome: Some(ApplyOutcome {
                failed: vec![apply::Failure {
                    id: "a".into(),
                    error: "boom".into(),
                }],
                ..Default::default()
            }),
        };
        let h = r.render_human();
        for needle in [
            "synced 0 change(s)",
            "register   a (new version after v2) → launch",
            "launch     b v3",
            "revive     c",
            "unchanged  d (v1)",
            "deprecate  h v4 (catalog v4 is deprecated: broken)",
            "orphaned   e",
            "deprecate  f",
            "skipped    g: why",
            "warning    w1",
            "FAILED     a: boom",
        ] {
            assert!(h.contains(needle), "missing {needle:?} in:\n{h}");
        }
        assert_eq!(r.failed(), 1);
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["plan"][0]["action"], "register");
        assert_eq!(v["outcome"]["failed"][0]["id"], "a");
        describe_metrics();
    }
}
