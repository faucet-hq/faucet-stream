//! `faucet template` — the CLI half of the pipeline template registry (#444).
//!
//! Register a parameterized config once into a store (`sqlite:` / `postgres://` /
//! `memory`), then list / inspect / delete versions, or materialize one with
//! `--param` values and run it locally. Pointing `faucet serve --history` at the
//! same URL makes the very same templates triggerable over HTTP — the CLI and
//! the control plane share one registry, not two.

use crate::cli::{
    TemplateArgs, TemplateCommand, TemplateDeleteArgs, TemplateDeprecateArgs, TemplateLaunchArgs,
    TemplateListArgs, TemplatePromoteArgs, TemplateRegisterArgs, TemplateRollbackArgs,
    TemplateRunArgs, TemplateShowArgs, TemplateStoreArgs,
};
use crate::error::{CliError, CliResult};
use crate::serve::history::templates::{
    TemplateRecord, TemplateSummary, VersionChannel, VersionSelector,
};
use crate::serve::load::ConfigFormat;
use crate::templates::{RegisterRequest, TemplateStore};

/// Execute the `template` subcommand.
pub async fn run(args: TemplateArgs) -> CliResult<()> {
    match args.command {
        TemplateCommand::Register(a) => register(a).await,
        TemplateCommand::List(a) => list(a).await,
        TemplateCommand::Show(a) => show(a).await,
        TemplateCommand::Launch(a) => launch(a).await,
        TemplateCommand::Rollback(a) => rollback(a).await,
        TemplateCommand::Deprecate(a) => deprecate(a).await,
        TemplateCommand::Promote(a) => promote(a).await,
        TemplateCommand::Delete(a) => delete(a).await,
        TemplateCommand::Run(a) => run_template(a).await,
        TemplateCommand::Test(a) => test_suite(a).await,
        #[cfg(feature = "templates-sync")]
        TemplateCommand::Sync(a) => sync(a).await,
        #[cfg(feature = "templates-sync")]
        TemplateCommand::Publish(a) => publish(a).await,
    }
}

/// `faucet template sync`: pull every origin (or `--origin`) and print one
/// report per origin. Exit non-zero when an origin could not be read or any
/// template failed to apply — a partial pull must not look green.
#[cfg(feature = "templates-sync")]
async fn sync(args: crate::cli::TemplateSyncArgs) -> CliResult<()> {
    use crate::templates::sync as tsync;
    let store = connect(&args.common).await?;
    let file = tsync::load_sync_file(&args.config).await?;
    let results =
        tsync::sync_all(&store, &file, args.origin.as_deref(), args.dry_run, None).await?;
    let mut origin_errors = Vec::new();
    let mut failed = 0usize;
    let mut reports = Vec::new();
    for r in results {
        match r {
            Ok(rep) => {
                failed += rep.failed();
                reports.push(rep);
            }
            Err((name, e)) => origin_errors.push((name, e.to_string())),
        }
    }
    if args.common.json {
        println!(
            "{}",
            to_pretty(&serde_json::json!({
                "dry_run": args.dry_run,
                "reports": reports,
                "origin_errors": origin_errors.iter().map(|(n, e)| serde_json::json!({"origin": n, "error": e})).collect::<Vec<_>>(),
            }))?
        );
    } else {
        for rep in &reports {
            print!("{}", rep.render_human());
        }
        for (name, e) in &origin_errors {
            println!("origin '{name}': ERROR {e}");
        }
    }
    if !origin_errors.is_empty() || failed > 0 {
        return Err(CliError::Config(format!(
            "template sync: {} origin(s) unreadable, {failed} template(s) failed to apply",
            origin_errors.len()
        )));
    }
    Ok(())
}

/// `faucet template publish <id> --origin X`: write one version to an origin.
#[cfg(feature = "templates-sync")]
async fn publish(args: crate::cli::TemplatePublishArgs) -> CliResult<()> {
    use crate::templates::sync as tsync;
    let store = connect(&args.common).await?;
    let file = tsync::load_sync_file(&args.config).await?;
    let selector = VersionSelector::parse(&args.version)?;
    let rep = tsync::publish(&store, &file, &args.id, &args.origin, selector).await?;
    if args.common.json {
        println!("{}", to_pretty(&rep)?);
    } else {
        println!(
            "published {} v{} → origin '{}' as {} ({})",
            rep.id, rep.version, rep.origin, rep.name, rep.location
        );
    }
    Ok(())
}

/// Load `.env` (so a `${env:…}` in a materialized template resolves) and connect
/// the registry store.
async fn connect(common: &TemplateStoreArgs) -> CliResult<TemplateStore> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(common.env_file.as_deref(), common.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    crate::templates::resolve_store_url(&common.store).await
}

fn to_pretty<T: serde::Serialize>(value: &T) -> CliResult<String> {
    serde_json::to_string_pretty(value)
        .map_err(|e| CliError::Internal(format!("rendering template JSON: {e}")))
}

/// Pick the wire format from a config path's extension.
fn format_of(path: &std::path::Path) -> CliResult<ConfigFormat> {
    match path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("yaml" | "yml") => Ok(ConfigFormat::Yaml),
        Some("json") => Ok(ConfigFormat::Json),
        _ => Err(CliError::UnknownExtension {
            path: path.to_path_buf(),
        }),
    }
}

async fn register(args: TemplateRegisterArgs) -> CliResult<()> {
    let store = connect(&args.common).await?;
    let format = format_of(&args.config)?;
    let body = std::fs::read_to_string(&args.config).map_err(|e| {
        CliError::Config(format!(
            "reading template config '{}': {e}",
            args.config.display()
        ))
    })?;
    let tags = args
        .tag
        .iter()
        .map(|t| VersionChannel::parse(t))
        .collect::<CliResult<Vec<_>>>()?;
    let record = crate::templates::register(
        &store,
        RegisterRequest {
            id: args.id.clone(),
            body,
            format,
            description: args.description.clone(),
            tags: tags.clone(),
            launch: args.launch,
            created_by: None,
        },
    )
    .await?;
    if crate::hub::detect_kind_in_file(&args.config).is_none() {
        eprintln!(
            "note: '{}' has no `kind:` — registering a complete pipeline config this way is deprecated. \
             The registry's model is `kind: source-template` + `kind: sink-template` composed at run time \
             (`faucet template run <source> --sink <sink>`); add `kind: pipeline` to keep registering a \
             complete config explicitly.",
            args.config.display()
        );
    }

    if args.common.json {
        println!("{}", to_pretty(&record.summary())?);
        return Ok(());
    }
    println!(
        "registered template '{}' version {}{}",
        record.id,
        record.version,
        if tags.is_empty() {
            String::new()
        } else {
            format!(
                "  (channels: {})",
                tags.iter()
                    .map(|c| c.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    );
    print_params(&record.summary());
    println!(
        "\n{}",
        trigger_hint(
            record.kind,
            &record.id,
            &args.common.store,
            &required_param_hint(&record.summary())
        )
    );
    Ok(())
}

/// `--param name=<…>` hints for every required param, for the register / show
/// "how do I run this" line.
fn required_param_hint(summary: &TemplateSummary) -> String {
    summary
        .params
        .iter()
        .filter(|(_, p)| p.required)
        .map(|(name, p)| format!(" --param {name}=<{}>", p.kind.as_str()))
        .collect()
}

fn print_params(summary: &TemplateSummary) {
    if summary.params.is_empty() {
        println!("params: (none — this template takes no overrides)");
        return;
    }
    println!("\nparams:");
    for (name, p) in &summary.params {
        let requirement = if p.required {
            "required".to_string()
        } else {
            match &p.default {
                Some(d) => format!("default {d}"),
                None => "optional".to_string(),
            }
        };
        println!(
            "  {:<20} {:<7} {}{}{}",
            name,
            p.kind.as_str(),
            requirement,
            if p.secret { "  [secret]" } else { "" },
            match &p.description {
                Some(d) => format!("  — {d}"),
                None => String::new(),
            }
        );
    }
}

async fn list(args: TemplateListArgs) -> CliResult<()> {
    let store = connect(&args.common).await?;
    let mut templates = crate::templates::list_with_state(&store).await?;
    if let Some(kind) = args.kind {
        let kind: crate::hub::TemplateKind = kind.into();
        templates.retain(|t| t.kind == kind);
    }
    if args.common.json {
        println!(
            "{}",
            to_pretty(&serde_json::json!({ "templates": templates }))?
        );
        return Ok(());
    }
    if templates.is_empty() {
        println!("no templates registered in this store — add one with `faucet template register`");
        return Ok(());
    }
    // LIVE is what an unpinned run gets; NEWEST is the build tip. Showing both
    // side by side is the whole point of the model — a nightly can sit at v7 while
    // production still rides v4.
    println!(
        "{:<26}  {:<15}  {:<11}  {:<6}  {:<7}  {:>6}  DESCRIPTION",
        "ID", "KIND", "STATUS", "LIVE", "NEWEST", "PARAMS"
    );
    for t in &templates {
        let (status, live, newest) = match &t.state {
            Some(st) => (
                st.status.to_string(),
                st.stable.map(|v| format!("v{v}")).unwrap_or("—".into()),
                st.newest.map(|v| format!("v{v}")).unwrap_or("—".into()),
            ),
            None => ("?".into(), "?".into(), format!("v{}", t.version)),
        };
        println!(
            "{:<26}  {:<15}  {:<11}  {:<6}  {:<7}  {:>6}  {}",
            t.id,
            t.kind,
            status,
            live,
            newest,
            t.params.len(),
            t.description.as_deref().unwrap_or("")
        );
    }
    Ok(())
}

/// Fetch one template, mapping "not found" to a typed error naming the id.
async fn fetch(store: &TemplateStore, id: &str, version: Option<u32>) -> CliResult<TemplateRecord> {
    store
        .template_get(id, version)
        .await
        .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?
        .ok_or_else(|| CliError::UnknownPipelineTemplate {
            id: id.to_string(),
            version,
        })
}

async fn show(args: TemplateShowArgs) -> CliResult<()> {
    let store = connect(&args.common).await?;
    let selector = VersionSelector::parse(&args.version)?;
    let want = crate::templates::resolve_version(&store, &args.id, selector).await?;
    let record = fetch(&store, &args.id, Some(want)).await?;

    // `--clean`: emit ONLY the pure template config — comments stripped, canonical
    // YAML — so it pipes to a file. Skips the metadata report (and the extra store
    // reads below). `--json` takes precedence.
    if args.clean && !args.common.json {
        print!("{}", crate::templates::clean_config_yaml(&record.body)?);
        return Ok(());
    }

    let state = crate::templates::template_state(&store, &args.id).await?;
    let launches = store
        .template_launches(&args.id)
        .await
        .map_err(|e| CliError::Internal(format!("template launch read: {e}")))?;

    if args.common.json {
        println!(
            "{}",
            to_pretty(&serde_json::json!({
                "template": record,
                "state": state,
                "is_stable": state.stable == Some(record.version),
                "launches": launches,
            }))?
        );
        return Ok(());
    }
    println!("template  {}   [{}]", record.id, state.status);
    println!("kind      {}", record.kind);
    if let Some(name) = &record.name {
        println!("name      {name}");
    }
    if let Some(d) = &record.description {
        println!("about     {d}");
    }
    println!(
        "created   {}{}",
        record.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
        match &record.created_by {
            Some(p) => format!(" by {p}"),
            None => String::new(),
        }
    );
    println!(
        "showing   v{}{}",
        record.version,
        if state.stable == Some(record.version) {
            "  (live)"
        } else {
            ""
        }
    );
    // One row per version with its channels — the version-first view, which is
    // how you actually think about "what is v3 tagged as?".
    println!("\nversions:");
    for v in &state.versions {
        let mut marks: Vec<String> = Vec::new();
        if state.stable == Some(*v) {
            marks.push("live".into());
        }
        if state.previous == Some(*v) {
            marks.push("previous".into());
        }
        if state.newest == Some(*v) {
            marks.push("newest".into());
        }
        if let Some(d) = state.version_deprecation(*v) {
            marks.push(match &d.record.reason {
                Some(r) => format!("DEPRECATED ({r})"),
                None => "DEPRECATED".into(),
            });
        }
        marks.extend(
            state
                .tags
                .iter()
                .filter(|(_, pointed)| *pointed == v)
                .map(|(t, _)| t.clone()),
        );
        println!(
            "  v{:<4} {}",
            v,
            if marks.is_empty() {
                String::from("—")
            } else {
                marks.join(", ")
            }
        );
    }
    if let Some(d) = &state.deprecation {
        println!(
            "\ndeprecated {}{}",
            d.deprecated_at.format("%Y-%m-%dT%H:%M:%SZ"),
            match &d.reason {
                Some(r) => format!("  — {r}"),
                None => String::new(),
            }
        );
    }
    if !launches.is_empty() {
        println!("\nlaunch history (newest first):");
        for l in launches.iter().take(10) {
            println!(
                "  v{:<4} {}{}",
                l.version,
                l.launched_at.format("%Y-%m-%dT%H:%M:%SZ"),
                match &l.launched_by {
                    Some(by) => format!("  by {by}"),
                    None => String::new(),
                }
            );
        }
    }
    print_params(&record.summary());
    println!("\nconfig ({:?}, stored verbatim):", record.format);
    for line in record.body.lines() {
        println!("  {line}");
    }
    Ok(())
}

async fn delete(args: TemplateDeleteArgs) -> CliResult<()> {
    let store = connect(&args.common).await?;
    // No `--version` deletes the whole template; a selector deletes one version.
    let pinned = match args.version.as_deref() {
        None => None,
        // A selector always resolves to a concrete version, so `--version stable`
        // removes just the launched one rather than the whole template.
        Some(raw) => Some(
            crate::templates::resolve_version(&store, &args.id, VersionSelector::parse(raw)?)
                .await?,
        ),
    };
    let removed = store
        .template_delete(&args.id, pinned)
        .await
        .map_err(|e| CliError::Internal(format!("template registry write: {e}")))?;
    if removed == 0 {
        return Err(CliError::UnknownPipelineTemplate {
            id: args.id.clone(),
            version: pinned,
        });
    }
    if args.common.json {
        println!(
            "{}",
            to_pretty(&serde_json::json!({ "id": args.id, "deleted_versions": removed }))?
        );
        return Ok(());
    }
    println!("deleted {removed} version(s) of template '{}'", args.id);
    Ok(())
}

/// Render a launch/rollback outcome.
fn report_launch(
    id: &str,
    outcome: &crate::templates::LaunchOutcome,
    json: bool,
    verb: &str,
) -> CliResult<()> {
    if json {
        println!(
            "{}",
            to_pretty(&serde_json::json!({
                "id": id,
                "version": outcome.version,
                "replaced": outcome.replaced,
                "already_launched": outcome.already_launched,
                "first_launch": outcome.first_launch,
            }))?
        );
        return Ok(());
    }
    if outcome.already_launched {
        println!(
            "template '{id}': v{} was already live — nothing changed",
            outcome.version
        );
        return Ok(());
    }
    println!(
        "template '{id}': {verb} v{}{}",
        outcome.version,
        match outcome.replaced {
            Some(prev) => format!(" (was v{prev}; previous → v{prev})"),
            None => String::from(" — first launch, template is now `launched`"),
        }
    );
    Ok(())
}

async fn launch(args: TemplateLaunchArgs) -> CliResult<()> {
    let store = connect(&args.common).await?;
    let target = VersionSelector::parse(&args.version)?;
    let outcome = crate::templates::launch(&store, &args.id, target, None).await?;
    report_launch(&args.id, &outcome, args.common.json, "launched")
}

async fn rollback(args: TemplateRollbackArgs) -> CliResult<()> {
    let store = connect(&args.common).await?;
    let outcome = crate::templates::rollback(&store, &args.id, None).await?;
    report_launch(&args.id, &outcome, args.common.json, "rolled back to")
}

async fn deprecate(args: TemplateDeprecateArgs) -> CliResult<()> {
    let store = connect(&args.common).await?;
    if let Some(raw) = &args.version {
        let version =
            crate::templates::resolve_version(&store, &args.id, VersionSelector::parse(raw)?)
                .await?;
        crate::templates::set_version_deprecated(
            &store,
            &args.id,
            version,
            args.reason.clone(),
            None,
            !args.undo,
        )
        .await?;
        if args.common.json {
            println!(
                "{}",
                to_pretty(&serde_json::json!({
                    "id": args.id, "version": version, "deprecated": !args.undo,
                }))?
            );
        } else if args.undo {
            println!("v{version} of '{}' is live again", args.id);
        } else {
            println!(
                "v{version} of '{}' is deprecated\n  pinned runs and channels pointing at it keep \
                 working but warn; `newest` skips it and `launch` refuses it",
                args.id
            );
        }
        return Ok(());
    }
    let status =
        crate::templates::set_deprecated(&store, &args.id, args.reason.clone(), None, !args.undo)
            .await?;
    if args.common.json {
        println!(
            "{}",
            to_pretty(&serde_json::json!({ "id": args.id, "status": status.as_str() }))?
        );
        return Ok(());
    }
    println!("template '{}' is now {status}", args.id);
    if !args.undo {
        println!(
            "  existing callers keep working (pinned runs and `stable` still resolve) but every \
             trigger warns — use `faucet template delete` for a hard stop"
        );
    }
    Ok(())
}

async fn promote(args: TemplatePromoteArgs) -> CliResult<()> {
    let store = connect(&args.common).await?;
    let tag = VersionChannel::parse(&args.tag)?;
    let target = VersionSelector::parse(&args.version)?;
    let version = crate::templates::promote(&store, &args.id, tag, target).await?;
    if args.common.json {
        println!(
            "{}",
            to_pretty(&serde_json::json!({
                "id": args.id, "tag": tag.as_str(), "version": version,
            }))?
        );
        return Ok(());
    }
    println!("template '{}': {tag} → v{version}", args.id);
    Ok(())
}

/// How to use a just-registered template: only a pipeline or a source
/// template is triggered directly; a sink or a deployment joins a source's run.
fn trigger_hint(kind: crate::hub::TemplateKind, id: &str, store: &str, params: &str) -> String {
    use crate::hub::TemplateKind::*;
    match kind {
        Pipeline => format!("trigger it with:\n  faucet template run {id} --store {store}{params}"),
        SourceTemplate => format!(
            "trigger it with a sink template:\n  faucet template run {id} --sink <sink-template> --store {store}{params}"
        ),
        SinkTemplate => format!(
            "compose it into a source template's run:\n  faucet template run <source-template> --sink {id} --store {store}{params}"
        ),
        Deployment => format!(
            "apply it to a composed run:\n  faucet template run <source-template> --sink <sink-template> --overlay {id} --store {store}{params}"
        ),
    }
}

/// `--overlay`: an existing file is applied inline; anything else is a
/// registered deployment id.
fn overlay_choice(
    raw: Option<&str>,
    version: &str,
) -> CliResult<Option<crate::templates::OverlayChoice>> {
    let Some(raw) = raw else { return Ok(None) };
    let path = std::path::Path::new(raw);
    if path.is_file() {
        let text = std::fs::read_to_string(path)
            .map_err(|e| CliError::Config(format!("reading overlay {raw}: {e}")))?;
        let value: serde_json::Value = serde_yaml::from_str(&text)
            .map_err(|e| CliError::Config(format!("overlay {raw}: invalid YAML: {e}")))?;
        return Ok(Some(crate::templates::OverlayChoice::Inline(value)));
    }
    Ok(Some(crate::templates::OverlayChoice::Registered {
        id: raw.to_string(),
        version: VersionSelector::parse(version)?,
    }))
}

async fn run_template(args: TemplateRunArgs) -> CliResult<()> {
    let store = connect(&args.common).await?;
    let supplied = crate::params::collect_cli_params(&args.param)?;
    let env = crate::params::collect_env_overrides(&args.param_env)?;
    let selector = VersionSelector::parse(&args.version)?;
    let want = crate::templates::resolve_version(&store, &args.id, selector).await?;
    let state = crate::templates::template_state(&store, &args.id).await?;
    if let Some(warning) = crate::templates::deprecation_warning(&state, want) {
        eprintln!("warning: '{}' — {warning}", args.id);
    }
    let sink = crate::templates::SinkChoice {
        id: args.sink.clone(),
        version: VersionSelector::parse(&args.sink_version)?,
        overlay: overlay_choice(args.overlay.as_deref(), &args.overlay_version)?,
    };
    let materialized = crate::templates::materialize_for_run(
        &store,
        &args.id,
        want,
        &sink,
        &supplied,
        &env,
        // `faucet template run` executes locally; nothing is persisted.
        crate::templates::Materialize::Local,
    )
    .await?;

    tracing::info!(
        template = %materialized.template_id,
        version = materialized.version,
        sink = materialized.sink_id.as_deref().unwrap_or("-"),
        "materialized pipeline template"
    );
    if !args.common.json {
        for p in &materialized.streams {
            eprintln!("  {:<32} write_mode: {}", p.stream, p.describe());
        }
        if let Some(o) = &materialized.overlay_id {
            eprintln!(
                "  overlay '{o}' sets: {}",
                materialized.overlay_contributes.join(", ")
            );
        }
        for w in &materialized.warnings {
            eprintln!("warning: {w}");
        }
    }

    // The materialized body is JSON with every `${param.*}` bound; `${env:…}`
    // for overridden variables is bound too. Remaining directives (secrets,
    // un-overridden env) resolve on the normal load path below.
    let doc: serde_json::Value = serde_json::from_str(&materialized.body)
        .map_err(|e| CliError::Internal(format!("re-parsing materialized template: {e}")))?;
    let mut cfg = crate::config::PipelineConfig::from_value(doc)?;
    crate::secrets::resolve_secrets(&mut cfg).await?;

    if args.dry_run && args.common.json {
        println!("{}", to_pretty(&cfg)?);
        return Ok(());
    }

    // Run through the identical path as `faucet run`, so observability,
    // lineage, notifications, the catalog, SLA evaluation, and row selection all
    // behave the same as they would for the same config on disk.
    let run_args = crate::cli::RunArgs {
        dry_run: args.dry_run,
        limit: args.limit,
        no_env_file: true,
        ..Default::default()
    };
    crate::commands::run::execute(cfg, run_args, None).await
}

/// `faucet template test` — run a suite across a template's parameter space
/// (#648).
///
/// Offline by default and by design: the validation tier materializes every
/// combination and checks it expands and compiles, which catches the whole
/// "param X breaks the config" class with no data, no network, and no credits.
async fn test_suite(args: crate::cli::TemplateTestArgs) -> CliResult<()> {
    use crate::templates::suite::{SuiteFile, Target, report::SuiteReport};

    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;

    let file = SuiteFile::from_path(&args.suite)?;
    let select = args.select.as_deref().or(file.select.as_deref());

    // A `template:` that names an existing file is tested straight from disk —
    // no registry needed, which is the point at which most of these failures
    // are cheapest to fix.
    let as_path = std::path::Path::new(&file.template);
    let (outcome, version) = if as_path.is_file() {
        let body = std::fs::read_to_string(as_path).map_err(|e| {
            CliError::Config(format!("template test: reading {}: {e}", as_path.display()))
        })?;
        // A file-based suite composes with a file-based sink template.
        let sink_body = match &file.sink {
            Some(p) => Some(std::fs::read_to_string(p).map_err(|e| {
                CliError::Config(format!("template test: reading sink template {p}: {e}"))
            })?),
            None => None,
        };
        let overlay = match &file.overlay {
            Some(p) => overlay_choice(Some(p), "stable")?.filter(|c| {
                matches!(c, crate::templates::OverlayChoice::Inline(_))
            }).ok_or_else(|| CliError::Config(format!(
                "template test: overlay '{p}' is not a readable file — a file-based suite applies an overlay file"
            )))
            .map(Some)?,
            None => None,
        };
        (
            crate::templates::suite::run(
                &file,
                Target::Document {
                    body,
                    sink_body,
                    overlay,
                },
                args.filter.as_deref(),
            )
            .await?,
            None,
        )
    } else {
        let store_url = args.store.as_deref().ok_or_else(|| {
            CliError::Config(format!(
                "template test: `{}` is neither a readable config path nor usable without a \
                 registry — pass --store (or FAUCET_TEMPLATE_STORE) to test a registered template",
                file.template
            ))
        })?;
        let store = crate::templates::resolve_store_url(store_url).await?;
        let version =
            crate::templates::suite::resolve_target_version(&store, &file.template, select).await?;
        let sink = match &file.sink {
            Some(sink_id) => Some((
                sink_id.as_str(),
                crate::templates::suite::resolve_target_version(
                    &store,
                    sink_id,
                    file.sink_select.as_deref(),
                )
                .await?,
            )),
            None => None,
        };
        let overlay = match &file.overlay {
            Some(oid) => Some(crate::templates::OverlayChoice::Registered {
                id: oid.clone(),
                version: VersionSelector::Pinned(
                    crate::templates::suite::resolve_target_version(
                        &store,
                        oid,
                        file.overlay_select.as_deref(),
                    )
                    .await?,
                ),
            }),
            None => None,
        };
        (
            crate::templates::suite::run(
                &file,
                Target::Registered {
                    store: &store,
                    id: &file.template,
                    version,
                    sink,
                    overlay,
                },
                args.filter.as_deref(),
            )
            .await?,
            Some(version),
        )
    };

    if outcome.cases.is_empty() {
        return Err(CliError::Config(match &args.filter {
            Some(f) => format!("no cases match --filter '{f}'"),
            None => "the suite produced no cases".into(),
        }));
    }

    let report = SuiteReport::new(&file.template, version, &outcome);
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report)
                .map_err(|e| CliError::Internal(format!("template test report: {e}")))?
        );
    } else {
        print!("{}", report.render_human());
    }

    // Exit code is the failed-case count, mirroring `faucet test`, so CI can
    // gate on it without parsing output.
    if report.failed > 0 {
        return Err(CliError::TestsFailed {
            failed: report.failed,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::TemplateStoreArgs;

    #[test]
    fn clean_config_strips_comments_and_preserves_params() {
        let body = "\
version: 1  # trailing comment
# a top-level comment
name: orders
pipeline:
  source: { type: csv, config: { path: \"${param.p}\" } }  # inline
  sink: { type: jsonl, config: { path: ./out.jsonl } }
";
        let out = crate::templates::clean_config_yaml(body).unwrap();
        // comments are gone
        assert!(!out.contains('#'), "comments must be stripped: {out}");
        // param placeholders survive (they're plain strings)
        assert!(
            out.contains("${param.p}"),
            "param token must survive: {out}"
        );
        // round-trips to the same parsed value
        let before: serde_yaml::Value = serde_yaml::from_str(body).unwrap();
        let after: serde_yaml::Value = serde_yaml::from_str(&out).unwrap();
        assert_eq!(before, after, "clean output must parse to the same config");
    }

    #[test]
    fn clean_config_normalizes_json_body_to_yaml() {
        // A JSON-format template body normalizes to YAML too (JSON ⊂ YAML).
        let body = r#"{"version":1,"name":"j","pipeline":{"source":{"type":"csv"}}}"#;
        let out = crate::templates::clean_config_yaml(body).unwrap();
        assert!(out.contains("version: 1"), "should be YAML now: {out}");
        assert!(
            !out.contains('{'),
            "no JSON braces in canonical YAML: {out}"
        );
    }

    fn common(store: &str, json: bool) -> TemplateStoreArgs {
        TemplateStoreArgs {
            store: store.to_string(),
            env_file: None,
            no_env_file: true,
            json,
        }
    }

    #[tokio::test]
    async fn source_and_sink_templates_register_list_and_run_from_the_cli() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("orders.csv"), "id,total\n1,10\n2,20\n").unwrap();
        let src_path = dir.path().join("acme-exports.yaml");
        std::fs::write(
            &src_path,
            format!(
                "kind: source-template\nname: acme-exports\ndescription: Acme exports\nparams:\n  data_dir: {{ type: string, default: {} }}\nsource:\n  type: csv\n  config:\n    path: \"${{param.data_dir}}/orders.csv\"\nstreams:\n  - {{ name: orders, primary_keys: [id], write: [overwrite, upsert] }}\n",
                dir.path().display()
            ),
        )
        .unwrap();
        let sink_path = dir.path().join("local-jsonl.yaml");
        std::fs::write(
            &sink_path,
            format!(
                "kind: sink-template\nname: local-jsonl\ndescription: Local files\nparams:\n  out_dir: {{ type: string, default: {} }}\nsink:\n  type: jsonl\n  config: {{ append: false }}\nper_stream:\n  path: \"${{param.out_dir}}/${{source}}/${{stream}}.jsonl\"\nwrite_mode_aliases: {{ overwrite: append }}\n",
                dir.path().display()
            ),
        )
        .unwrap();
        let store = format!("sqlite:{}", dir.path().join("registry.db").display());
        for path in [&src_path, &sink_path] {
            register(TemplateRegisterArgs {
                config: path.clone(),
                id: None,
                description: None,
                tag: vec![],
                launch: true,
                common: common(&store, false),
            })
            .await
            .expect("register hub template");
        }
        // `--id` disagreeing with `name` is refused for a hub template.
        let err = register(TemplateRegisterArgs {
            config: src_path.clone(),
            id: Some("other".into()),
            description: None,
            tag: vec![],
            launch: false,
            common: common(&store, false),
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("registered under its own hub id"), "{err}");

        for kind in [
            None,
            Some(crate::cli::TemplateKindArg::SinkTemplate),
            Some(crate::cli::TemplateKindArg::SourceTemplate),
        ] {
            list(TemplateListArgs {
                common: common(&store, kind.is_none()),
                kind,
            })
            .await
            .expect("list");
        }
        show(TemplateShowArgs {
            id: "acme-exports".into(),
            version: "stable".into(),
            clean: false,
            common: common(&store, true),
        })
        .await
        .expect("show");

        let run_args = |sink: Option<&str>, dry_run: bool| TemplateRunArgs {
            id: "acme-exports".into(),
            version: "stable".into(),
            sink: sink.map(str::to_string),
            sink_version: "stable".into(),
            overlay: None,
            overlay_version: "stable".into(),
            param: vec![],
            param_env: vec![],
            dry_run,
            limit: None,
            common: common(&store, false),
        };
        let err = run_template(run_args(None, true))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("--sink"), "{err}");
        run_template(run_args(Some("local-jsonl"), true))
            .await
            .expect("dry run composes");
        run_template(run_args(Some("local-jsonl"), false))
            .await
            .expect("run composes");
        let written =
            std::fs::read_to_string(dir.path().join("acme-exports/orders.jsonl")).unwrap();
        assert_eq!(
            written.lines().count(),
            2,
            "one file per stream under <out_dir>/<source>/"
        );

        // A sink template is never run on its own.
        let err = run_template(TemplateRunArgs {
            id: "local-jsonl".into(),
            ..run_args(None, true)
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("not runnable on its own"), "{err}");

        // #679: an overlay file applies to the composed run; an id that is not
        // registered is the registry's typed error.
        let overlay = dir.path().join("ops.yaml");
        std::fs::write(
            &overlay,
            "kind: deployment\nname: ops\nstate: { type: memory }\n",
        )
        .unwrap();
        run_template(TemplateRunArgs {
            overlay: Some(overlay.display().to_string()),
            ..run_args(Some("local-jsonl"), true)
        })
        .await
        .expect("dry run with an overlay file");
        let err = run_template(TemplateRunArgs {
            overlay: Some("not-registered".into()),
            ..run_args(Some("local-jsonl"), true)
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("not-registered"), "{err}");
    }

    #[test]
    fn the_register_hint_matches_the_kind() {
        use crate::hub::TemplateKind::*;
        assert!(
            trigger_hint(Pipeline, "p", "memory", "").contains("faucet template run p --store")
        );
        assert!(
            trigger_hint(SourceTemplate, "s", "memory", "")
                .contains("run s --sink <sink-template>")
        );
        assert!(trigger_hint(SinkTemplate, "k", "memory", "").contains("--sink k"));
        assert!(
            trigger_hint(Deployment, "d", "memory", " --param x=…")
                .ends_with("--overlay d --store memory --param x=…")
        );
    }

    #[test]
    fn an_overlay_flag_is_a_file_or_a_registered_id() {
        assert!(overlay_choice(None, "stable").unwrap().is_none());
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("o.yaml");
        std::fs::write(&f, "state: { type: memory }\n").unwrap();
        assert!(matches!(
            overlay_choice(Some(f.to_str().unwrap()), "stable").unwrap(),
            Some(crate::templates::OverlayChoice::Inline(v)) if v["state"]["type"] == "memory"
        ));
        assert!(matches!(
            overlay_choice(Some("ops"), "3").unwrap(),
            Some(crate::templates::OverlayChoice::Registered { id, version: VersionSelector::Pinned(3) }) if id == "ops"
        ));
        let bad = dir.path().join("bad.yaml");
        std::fs::write(&bad, "state: [unclosed\n").unwrap();
        assert!(overlay_choice(Some(bad.to_str().unwrap()), "stable").is_err());
        assert!(overlay_choice(Some("ops"), "latest").is_err());
    }

    #[tokio::test]
    async fn a_file_suite_applies_an_overlay_file() {
        let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap();
        let src = repo.join("hub/source-templates/faucet-hq/example-csv.yaml");
        let sink = repo.join("hub/sink-templates/faucet-hq/sqlite.yaml");
        let dir = tempfile::tempdir().unwrap();
        let overlay = dir.path().join("ops.yaml");
        std::fs::write(
            &overlay,
            "kind: deployment\nname: ops\nstate: { type: memory }\n",
        )
        .unwrap();
        let suite = dir.path().join("suite.yaml");
        let body = |ov: &str| {
            format!(
                "version: 1\ntemplate: {}\nsink: {}\noverlay: {ov}\nsuite:\n  cases:\n    - name: defaults\n      params: {{}}\n",
                src.display(),
                sink.display()
            )
        };
        std::fs::write(&suite, body(&overlay.display().to_string())).unwrap();
        test_suite(test_args(&suite))
            .await
            .expect("suite with an overlay passes");
        std::fs::write(&suite, body("./does-not-exist.yaml")).unwrap();
        let err = test_suite(test_args(&suite)).await.unwrap_err().to_string();
        assert!(err.contains("not a readable file"), "{err}");
    }

    const BODY: &str = "\
version: 1
name: cli-tpl
params:
  tag: { required: true, description: Output tag }
  page: { type: int, default: 5 }
pipeline:
  source:
    type: csv
    config:
      path: IN_PATH
  sink:
    type: jsonl
    config:
      path: OUT_PATH
";

    /// A registered template needs a *persistent* store to be visible to a
    /// second command, so the CLI round-trip test uses a temp SQLite file.
    /// Without the SQL backend feature the whole test is skipped.
    #[cfg(feature = "serve-history-sqlite")]
    #[tokio::test]
    async fn register_launch_promote_run_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        std::fs::write(&input, "id,name\n1,alice\n2,bob\n").unwrap();
        let output = dir.path().join("out.jsonl");
        let cfg_path = dir.path().join("tpl.yaml");
        std::fs::write(
            &cfg_path,
            BODY.replace("IN_PATH", &input.display().to_string())
                .replace("OUT_PATH", &output.display().to_string()),
        )
        .unwrap();
        let store = format!("sqlite:{}", dir.path().join("registry.db").display());
        let reg = |launch: bool, tag: Vec<String>| TemplateRegisterArgs {
            config: cfg_path.clone(),
            id: None,
            description: Some("round trip".into()),
            tag,
            launch,
            common: common(&store, false),
        };

        // A plain register is inert: the template is a draft.
        register(reg(false, vec!["dev".into()]))
            .await
            .expect("register v1");
        list(TemplateListArgs {
            common: common(&store, true),
            kind: None,
        })
        .await
        .expect("list");

        // An unpinned run refuses, naming the launch command.
        let err = run_template(TemplateRunArgs {
            id: "cli-tpl".into(),
            version: "stable".into(),
            sink: None,
            sink_version: "stable".into(),
            overlay: None,
            overlay_version: "stable".into(),
            param: vec!["tag=alpha".into()],
            param_env: vec![],
            dry_run: true,
            limit: None,
            common: common(&store, false),
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("no launched version"), "{err}");

        // Launching makes it live; then an unpinned run works.
        launch(TemplateLaunchArgs {
            id: "cli-tpl".into(),
            version: "newest".into(),
            common: common(&store, false),
        })
        .await
        .expect("launch");
        run_template(TemplateRunArgs {
            id: "cli-tpl".into(),
            version: "stable".into(),
            sink: None,
            sink_version: "stable".into(),
            overlay: None,
            overlay_version: "stable".into(),
            param: vec!["tag=alpha".into()],
            param_env: vec![],
            dry_run: false,
            limit: None,
            common: common(&store, false),
        })
        .await
        .expect("run");
        assert_eq!(
            std::fs::read_to_string(&output).unwrap().lines().count(),
            2,
            "the launched version's pipeline wrote both records"
        );

        // Register v2 (a build) — the live version must not move.
        register(reg(false, vec![])).await.expect("register v2");
        show(TemplateShowArgs {
            id: "cli-tpl".into(),
            version: "stable".into(),
            clean: false,
            common: common(&store, false),
        })
        .await
        .expect("show");
        promote(TemplatePromoteArgs {
            id: "cli-tpl".into(),
            tag: "pre-prod".into(),
            version: "newest".into(),
            common: common(&store, false),
        })
        .await
        .expect("promote");
        // Launch from the channel, then roll back.
        launch(TemplateLaunchArgs {
            id: "cli-tpl".into(),
            version: "pre-prod".into(),
            common: common(&store, true),
        })
        .await
        .expect("launch from channel");
        rollback(TemplateRollbackArgs {
            id: "cli-tpl".into(),
            common: common(&store, false),
        })
        .await
        .expect("rollback");

        // Derived channels and invented names are refused on promote.
        for tag in ["stable", "previous", "newest", "prd", "latest"] {
            assert!(
                promote(TemplatePromoteArgs {
                    id: "cli-tpl".into(),
                    tag: tag.into(),
                    version: "1".into(),
                    common: common(&store, false),
                })
                .await
                .is_err(),
                "`{tag}` must not be promotable"
            );
        }

        // Deprecate → revive.
        deprecate(TemplateDeprecateArgs {
            id: "cli-tpl".into(),
            reason: Some("superseded".into()),
            undo: false,
            version: None,
            common: common(&store, false),
        })
        .await
        .expect("deprecate");
        deprecate(TemplateDeprecateArgs {
            id: "cli-tpl".into(),
            reason: None,
            undo: true,
            version: None,
            common: common(&store, true),
        })
        .await
        .expect("undeprecate");

        // One version: retired, refused by launch, then revived (#697).
        deprecate(TemplateDeprecateArgs {
            id: "cli-tpl".into(),
            reason: Some("bad build".into()),
            undo: false,
            version: Some("1".into()),
            common: common(&store, false),
        })
        .await
        .expect("deprecate v1");
        let err = launch(TemplateLaunchArgs {
            id: "cli-tpl".into(),
            version: "1".into(),
            common: common(&store, false),
        })
        .await
        .expect_err("a deprecated version cannot be launched");
        assert!(err.to_string().contains("deprecated"), "{err}");
        deprecate(TemplateDeprecateArgs {
            id: "cli-tpl".into(),
            reason: None,
            undo: true,
            version: Some("1".into()),
            common: common(&store, true),
        })
        .await
        .expect("revive v1");
        deprecate(TemplateDeprecateArgs {
            id: "cli-tpl".into(),
            reason: None,
            undo: true,
            version: Some("1".into()),
            common: common(&store, false),
        })
        .await
        .expect("reviving a live version is a no-op");

        // Delete a single version, then the whole template.
        delete(TemplateDeleteArgs {
            id: "cli-tpl".into(),
            version: Some("newest".into()),
            common: common(&store, false),
        })
        .await
        .expect("delete newest");
        delete(TemplateDeleteArgs {
            id: "cli-tpl".into(),
            version: None,
            common: common(&store, true),
        })
        .await
        .expect("delete all");
        let err = delete(TemplateDeleteArgs {
            id: "cli-tpl".into(),
            version: None,
            common: common(&store, false),
        })
        .await
        .unwrap_err();
        assert!(
            matches!(err, CliError::UnknownPipelineTemplate { .. }),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn show_and_run_report_an_unknown_template() {
        let c = common("memory", false);
        let store = connect(&c).await.unwrap();
        let err = fetch(&store, "nope", None).await.unwrap_err();
        assert!(
            matches!(err, CliError::UnknownPipelineTemplate { ref id, .. } if id == "nope"),
            "{err:?}"
        );
        // Promoting a channel on a template that does not exist is the same
        // typed error, not a silently-created pointer.
        let err = promote(TemplatePromoteArgs {
            id: "nope".into(),
            tag: "prod".into(),
            version: "newest".into(),
            common: common("memory", false),
        })
        .await
        .unwrap_err();
        assert!(
            matches!(err, CliError::UnknownPipelineTemplate { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn format_is_taken_from_the_extension() {
        assert_eq!(
            format_of(std::path::Path::new("a.yaml")).unwrap(),
            ConfigFormat::Yaml
        );
        assert_eq!(
            format_of(std::path::Path::new("a.YML")).unwrap(),
            ConfigFormat::Yaml
        );
        assert_eq!(
            format_of(std::path::Path::new("a.json")).unwrap(),
            ConfigFormat::Json
        );
        assert!(format_of(std::path::Path::new("a.toml")).is_err());
        assert!(format_of(std::path::Path::new("a")).is_err());
    }

    #[test]
    fn required_param_hint_lists_only_required_params() {
        let mut params = crate::params::ParamsSpec::new();
        params.insert(
            "tag".into(),
            crate::params::ParamSpec {
                kind: crate::params::ParamType::String,
                required: true,
                default: None,
                secret: false,
                description: None,
                computed: None,
                values: Vec::new(),
            },
        );
        params.insert(
            "page".into(),
            crate::params::ParamSpec {
                kind: crate::params::ParamType::Int,
                required: false,
                default: Some(serde_json::json!(5)),
                secret: false,
                computed: None,
                values: Vec::new(),
                description: None,
            },
        );
        let summary = TemplateSummary {
            kind: crate::hub::TemplateKind::Pipeline,
            state: None,
            id: "t".into(),
            version: 1,
            name: None,
            description: None,
            params,
            created_at: chrono::Utc::now(),
            created_by: None,
        };
        let hint = required_param_hint(&summary);
        assert_eq!(hint, " --param tag=<string>");
        // `print_params` renders both without panicking.
        print_params(&summary);
    }

    #[tokio::test]
    async fn register_rejects_a_bad_extension_and_a_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let bad = dir.path().join("cfg.toml");
        std::fs::write(&bad, "x = 1").unwrap();
        let err = register(TemplateRegisterArgs {
            config: bad,
            id: None,
            description: None,
            tag: vec![],
            launch: false,
            common: common("memory", false),
        })
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::UnknownExtension { .. }), "{err:?}");

        let err = register(TemplateRegisterArgs {
            config: dir.path().join("nope.yaml"),
            id: None,
            description: None,
            tag: vec![],
            launch: false,
            common: common("memory", false),
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("reading template config"), "{err}");
    }

    #[tokio::test]
    async fn list_reports_an_empty_store() {
        list(TemplateListArgs {
            common: common("memory", false),
            kind: None,
        })
        .await
        .expect("empty list is not an error");
    }

    // ── `faucet template test` (#648) ─────────────────────────────────────

    fn test_args(suite: &std::path::Path) -> crate::cli::TemplateTestArgs {
        crate::cli::TemplateTestArgs {
            suite: suite.to_path_buf(),
            store: None,
            select: None,
            filter: None,
            json: false,
            env_file: None,
            no_env_file: true,
        }
    }

    /// Writes a template and a suite that points at it by **path**, which is
    /// the form that needs no registry.
    fn suite_fixture(suite_body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let tpl = dir.path().join("tpl.yaml");
        std::fs::write(
            &tpl,
            r#"version: 1
name: adapter-fixture
params:
  tenant: { type: string, required: true }
  region: { type: string, default: us, values: [us, eu] }
pipeline:
  source:
    type: rest
    config:
      base_url: "https://${param.region}.example.com"
      path: "/t/${param.tenant}"
  sink:
    type: jsonl
    config: { path: "./out/${param.tenant}.jsonl" }
"#,
        )
        .expect("write template");
        let suite = dir.path().join("suite.yaml");
        std::fs::write(
            &suite,
            suite_body.replace("TEMPLATE_PATH", tpl.to_str().expect("utf-8")),
        )
        .expect("write suite");
        (dir, suite)
    }

    #[tokio::test]
    async fn a_passing_suite_exits_ok_without_a_store() {
        let (_d, suite) = suite_fixture(
            r#"version: 1
template: TEMPLATE_PATH
suite:
  cases:
    - name: ok
      params: { tenant: acme, region: eu }
"#,
        );
        test_suite(test_args(&suite)).await.expect("suite passes");
    }

    /// The exit code is the failed-case count, so CI can gate on it without
    /// parsing output.
    #[tokio::test]
    async fn a_failing_suite_reports_the_failed_count() {
        let (_d, suite) = suite_fixture(
            r#"version: 1
template: TEMPLATE_PATH
suite:
  cases:
    - name: bad-region
      params: { tenant: acme, region: mars }
    - name: also-bad
      params: { tenant: acme, region: pluto }
"#,
        );
        match test_suite(test_args(&suite)).await {
            Err(CliError::TestsFailed { failed }) => assert_eq!(failed, 2),
            other => panic!("expected TestsFailed{{2}}, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn json_output_is_selectable() {
        let (_d, suite) = suite_fixture(
            r#"version: 1
template: TEMPLATE_PATH
suite:
  cases:
    - name: ok
      params: { tenant: acme }
"#,
        );
        let mut args = test_args(&suite);
        args.json = true;
        test_suite(args).await.expect("json run passes");
    }

    #[tokio::test]
    async fn a_filter_matching_nothing_is_an_error_not_a_silent_pass() {
        // A suite that runs zero cases and reports green is the failure mode
        // the whole feature exists to prevent.
        let (_d, suite) = suite_fixture(
            r#"version: 1
template: TEMPLATE_PATH
suite:
  cases:
    - name: ok
      params: { tenant: acme }
"#,
        );
        let mut args = test_args(&suite);
        args.filter = Some("nothing-matches-this".into());
        let err = test_suite(args).await.expect_err("no cases must error");
        assert!(err.to_string().contains("no cases match"), "{err}");
    }

    /// A `template:` that is neither a readable path nor accompanied by a
    /// store cannot be resolved, and the message has to say which of the two
    /// is missing.
    #[tokio::test]
    async fn a_registry_target_without_a_store_names_the_missing_flag() {
        let dir = tempfile::tempdir().expect("tempdir");
        let suite = dir.path().join("suite.yaml");
        std::fs::write(
            &suite,
            "version: 1
template: not-a-path-and-not-registered
suite:
  cases:
    - name: a
      params: {}
",
        )
        .expect("write");
        let err = test_suite(test_args(&suite)).await.expect_err("no store");
        assert!(err.to_string().contains("--store"), "{err}");
    }

    /// The registry branch: a `template:` that is not a path goes through the
    /// store, and an id that was never registered is reported as such rather
    /// than silently producing zero cases.
    #[tokio::test]
    async fn a_registry_target_reports_an_unregistered_id() {
        let dir = tempfile::tempdir().expect("tempdir");
        let suite = dir.path().join("suite.yaml");
        std::fs::write(
            &suite,
            "version: 1\ntemplate: never-registered\nsuite:\n  cases:\n    - name: a\n      params: {}\n",
        )
        .expect("write");
        let mut args = test_args(&suite);
        args.store = Some("memory".into());
        let err = test_suite(args).await.expect_err("unknown template");
        assert!(err.to_string().contains("never-registered"), "{err}");
    }

    /// The registry *success* path, end to end through the dispatcher.
    ///
    /// `memory` cannot serve this: `resolve_store_url("memory")` builds a
    /// fresh store per call, so a template registered in the test would be
    /// invisible to `test_suite`'s own connection. A sqlite file is the
    /// smallest store that actually persists between the two.
    #[tokio::test]
    async fn a_registered_template_is_resolved_and_its_suite_runs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("tpl.db");
        let store_url = format!("sqlite:{}", db.display());

        let store = crate::templates::resolve_store_url(&store_url)
            .await
            .expect("sqlite store");
        crate::templates::register(
            &store,
            crate::templates::RegisterRequest {
                id: Some("adapter-registered".into()),
                body: r#"version: 1
name: adapter-registered
params:
  tenant: { type: string, required: true }
pipeline:
  source:
    type: rest
    config: { base_url: "https://example.com", path: "/t/${param.tenant}" }
  sink:
    type: jsonl
    config: { path: "./out/${param.tenant}.jsonl" }
"#
                .into(),
                format: ConfigFormat::Yaml,
                description: None,
                tags: Vec::new(),
                launch: true,
                created_by: None,
            },
        )
        .await
        .expect("register");

        let suite = dir.path().join("suite.yaml");
        std::fs::write(
            &suite,
            "version: 1\ntemplate: adapter-registered\nsuite:\n  cases:\n    - name: ok\n      params: { tenant: acme }\n",
        )
        .expect("write suite");

        let mut args = test_args(&suite);
        args.store = Some(store_url);
        // Resolves `stable` through the registry and runs the case.
        test_suite(args).await.expect("registered suite passes");
    }

    #[tokio::test]
    async fn a_registered_suite_resolves_its_overlay_through_the_store() {
        let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap();
        let dir = tempfile::tempdir().expect("tempdir");
        let store_url = format!("sqlite:{}", dir.path().join("tpl.db").display());
        let store = crate::templates::resolve_store_url(&store_url)
            .await
            .expect("sqlite store");
        for body in [
            std::fs::read_to_string(repo.join("hub/source-templates/faucet-hq/example-csv.yaml"))
                .unwrap(),
            std::fs::read_to_string(repo.join("hub/sink-templates/faucet-hq/sqlite.yaml")).unwrap(),
            "kind: deployment\nname: ops\nstate: { type: memory }\n".to_string(),
        ] {
            crate::templates::register(
                &store,
                crate::templates::RegisterRequest {
                    id: None,
                    body,
                    format: ConfigFormat::Yaml,
                    description: None,
                    tags: Vec::new(),
                    launch: true,
                    created_by: None,
                },
            )
            .await
            .expect("register");
        }
        let suite = dir.path().join("suite.yaml");
        std::fs::write(
            &suite,
            "version: 1\ntemplate: faucet-hq/example-csv\nsink: faucet-hq/sqlite\noverlay: ops\noverlay_select: stable\nsuite:\n  cases:\n    - name: defaults\n      params: {}\n",
        )
        .expect("write suite");
        let mut args = test_args(&suite);
        args.store = Some(store_url);
        test_suite(args)
            .await
            .expect("registered suite with an overlay passes");
    }

    /// The dispatcher arm itself — `faucet template test` reaches
    /// `test_suite` rather than some other subcommand.
    #[tokio::test]
    async fn the_test_subcommand_dispatches_to_the_runner() {
        let (_d, suite) = suite_fixture(
            r#"version: 1
template: TEMPLATE_PATH
suite:
  cases:
    - name: ok
      params: { tenant: acme }
"#,
        );
        run(crate::cli::TemplateArgs {
            command: TemplateCommand::Test(test_args(&suite)),
        })
        .await
        .expect("dispatched and passed");
    }

    #[tokio::test]
    async fn a_missing_suite_file_is_a_clear_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = test_suite(test_args(&dir.path().join("nope.yaml")))
            .await
            .expect_err("missing suite");
        assert!(err.to_string().contains("template test suite"), "{err}");
    }
}
