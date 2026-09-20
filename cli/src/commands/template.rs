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
    }
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
        "\ntrigger it with:\n  faucet template run {} --store {}{}",
        record.id,
        args.common.store,
        required_param_hint(&record.summary())
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
    let templates = crate::templates::list_with_state(&store).await?;
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
        "{:<26}  {:<11}  {:<6}  {:<7}  {:>6}  DESCRIPTION",
        "ID", "STATUS", "LIVE", "NEWEST", "PARAMS"
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
            "{:<26}  {:<11}  {:<6}  {:<7}  {:>6}  {}",
            t.id,
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

async fn run_template(args: TemplateRunArgs) -> CliResult<()> {
    let store = connect(&args.common).await?;
    let supplied = crate::params::collect_cli_params(&args.param)?;
    let env = crate::params::collect_env_overrides(&args.param_env)?;
    let selector = VersionSelector::parse(&args.version)?;
    let want = crate::templates::resolve_version(&store, &args.id, selector).await?;
    let materialized = crate::templates::materialize(
        &store,
        &args.id,
        want,
        &supplied,
        &env,
        // `faucet template run` executes locally; nothing is persisted.
        crate::templates::Materialize::Local,
    )
    .await?;

    tracing::info!(
        template = %materialized.template_id,
        version = materialized.version,
        "materialized pipeline template"
    );

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
        })
        .await
        .expect("list");

        // An unpinned run refuses, naming the launch command.
        let err = run_template(TemplateRunArgs {
            id: "cli-tpl".into(),
            version: "stable".into(),
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
            common: common(&store, false),
        })
        .await
        .expect("deprecate");
        deprecate(TemplateDeprecateArgs {
            id: "cli-tpl".into(),
            reason: None,
            undo: true,
            common: common(&store, true),
        })
        .await
        .expect("undeprecate");

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
        })
        .await
        .expect("empty list is not an error");
    }
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
        (
            crate::templates::suite::run(&file, Target::Document { body }, args.filter.as_deref())
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
        (
            crate::templates::suite::run(
                &file,
                Target::Registered {
                    store: &store,
                    id: &file.template,
                    version,
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
