//! `faucet state show|set|reset|export|import` (#735) — thin command layer
//! over [`crate::pipeline_state::ops`]: load the config, plan, confirm, apply,
//! render (human or `--json`). Every rendered value passes the secrets
//! redaction boundary.

use crate::cli::{StateArgs, StateCommand, StateLoadArgs, StateMutateArgs};
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::pipeline_state::keys::KeyKind;
use crate::pipeline_state::ops::{self, KeyEntry, Stores};
use crate::pipeline_state::{PipelineTarget, cli_pipeline_name};
use chrono::Utc;
use std::io::{BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};

/// Execute the `state` subcommand.
pub async fn run(args: StateArgs) -> CliResult<()> {
    match args.command {
        StateCommand::Show(a) => {
            let (cfg, target, _) = load(a.config.as_deref(), &a.load).await?;
            let stores = Stores::build(&target, None).await?;
            let report = ops::show(&target, &stores, a.row.as_deref(), Utc::now()).await?;
            let _ = cfg;
            emit(a.load.json, &report, || render_show(&report))
        }
        StateCommand::Set(a) => {
            let bookmark: serde_json::Value = serde_json::from_str(&a.bookmark).map_err(|e| {
                CliError::Config(format!(
                    "--bookmark must be JSON (quote a string bookmark: '\"2026-09-19\"'): {e}"
                ))
            })?;
            let (cfg, target, _) = load(a.config.as_deref(), &a.load).await?;
            let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;
            let stores = Stores::build(&target, None).await?;
            let mut req = ops::SetRequest {
                row: a.row,
                parent_key: a.parent_key,
                bookmark,
                force: a.mutate.force,
                dry_run: true,
                skip_watermark_check: a.skip_watermark_check,
                legacy_format: a.legacy_format,
            };
            guard_history(&cfg, &target, a.mutate.force).await?;
            let plan = ops::set(&target, &stores, &auth, &req, Utc::now()).await?;
            if !proceed(&a.mutate, a.load.json, &plan, || render_set(&plan))? {
                return Ok(());
            }
            req.dry_run = false;
            let done = ops::set(&target, &stores, &auth, &req, Utc::now()).await?;
            emit(a.load.json, &done, || render_set(&done))
        }
        StateCommand::Reset(a) => {
            let (cfg, target, _) = load(a.config.as_deref(), &a.load).await?;
            let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;
            let stores = Stores::build(&target, None).await?;
            let mut req = ops::ResetRequest {
                row: a.row,
                parent_key: a.parent_key,
                include_markers: a.include_markers,
                force: a.mutate.force,
                dry_run: true,
                skip_watermark_check: a.skip_watermark_check,
                rewind_token: a.rewind_token,
                legacy_format: a.legacy_format,
            };
            guard_history(&cfg, &target, a.mutate.force).await?;
            let plan = ops::reset(&target, &stores, &auth, &req, Utc::now()).await?;
            if plan.changes.is_empty() {
                return emit(a.load.json, &plan, || {
                    format!("row {}: no state to reset\n", plan.row)
                });
            }
            if !proceed(&a.mutate, a.load.json, &plan, || render_reset(&plan))? {
                return Ok(());
            }
            req.dry_run = false;
            let done = ops::reset(&target, &stores, &auth, &req, Utc::now()).await?;
            emit(a.load.json, &done, || render_reset(&done))
        }
        StateCommand::Export(a) => {
            let (_, target, _) = load(a.config.as_deref(), &a.load).await?;
            let stores = Stores::build(&target, None).await?;
            let export = ops::export(&target, &stores, Utc::now()).await?;
            // Verbatim: value-based redaction would also mask env-sourced values
            // that bookmarks legitimately hold (a topic name), and an import would
            // then restore the altered state. A written export is owner-only.
            let text = to_pretty(&export)?;
            match &a.output {
                Some(path) => {
                    write_private(path, format!("{text}\n").as_bytes())?;
                    eprintln!(
                        "exported {} key(s) of pipeline {} to {}",
                        export.keys.len(),
                        export.pipeline,
                        path.display()
                    );
                }
                None => println!("{text}"),
            }
            Ok(())
        }
        StateCommand::Import(a) => {
            let text = std::fs::read_to_string(&a.file).map_err(|e| {
                CliError::Config(format!("reading export {}: {e}", a.file.display()))
            })?;
            let doc: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
                CliError::Config(format!("export {} is not JSON: {e}", a.file.display()))
            })?;
            let export = faucet_core::StateExport::from_value(doc)?;
            let (cfg, target, _) = load(Some(&a.config), &a.load).await?;
            let single = match &a.to_state {
                Some(raw) => {
                    Some(crate::state::build_state_store(&ops::parse_state_target(raw)?).await?)
                }
                None => None,
            };
            let stores = Stores::build(&target, single).await?;
            let mut req = ops::ImportRequest {
                export,
                overwrite: a.overwrite,
                force: a.mutate.force,
                dry_run: true,
            };
            guard_history(&cfg, &target, a.mutate.force).await?;
            let plan = ops::import(&target, &stores, &req, Utc::now()).await?;
            if !proceed(&a.mutate, a.load.json, &plan, || render_import(&plan))? {
                return Ok(());
            }
            req.dry_run = false;
            let done = ops::import(&target, &stores, &req, Utc::now()).await?;
            emit(a.load.json, &done, || render_import(&done))?;
            match &done.error {
                Some(e) => Err(CliError::Config(format!(
                    "import stopped: {e} — {} key(s) were written before the failure (listed \
                     above); re-run with --overwrite once the store is healthy",
                    done.written.len()
                ))),
                None => Ok(()),
            }
        }
    }
}

/// Write `bytes` to `path`, readable only by its owner on Unix.
fn write_private(path: &Path, bytes: &[u8]) -> CliResult<()> {
    use std::io::Write as _;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(0o600);
    }
    let mut file = opts.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    file.write_all(bytes)?;
    Ok(())
}

/// Load the config named by `config` (or discovered) into its target.
pub async fn load(
    config: Option<&Path>,
    load: &StateLoadArgs,
) -> CliResult<(PipelineConfig, PipelineTarget, PathBuf)> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(load.env_file.as_deref(), load.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    let path = match config {
        Some(p) => p.to_path_buf(),
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };
    let cfg = PipelineConfig::from_path_async(&path, load.profile.as_deref()).await?;
    let name = cli_pipeline_name(&cfg, &path);
    let target = PipelineTarget::resolve(&cfg, &name)?;
    Ok((cfg, target, path))
}

/// Refuse while a run-history store (a config's `catalog:` store) reports a
/// run of this pipeline in flight.
async fn guard_history(
    cfg: &PipelineConfig,
    target: &PipelineTarget,
    force: bool,
) -> CliResult<()> {
    #[cfg(feature = "catalog")]
    if let Some(spec) = &cfg.catalog {
        let handle = crate::catalog::connect_from_spec(spec).await?;
        let (_, active) = crate::status::history::read(handle.store.as_ref(), &target.pipeline)
            .await
            .map_err(|e| CliError::Internal(format!("reading run history: {e}")))?;
        if let Some(run) = active.first() {
            if !force {
                return Err(CliError::StateBusy(format!(
                    "run {run} of pipeline '{}' is in flight according to the catalog store — \
                     wait for it (or pass --force if it is gone)",
                    target.pipeline
                )));
            }
            eprintln!("warning: run {run} is in flight; proceeding because --force was given");
        }
    }
    let _ = (cfg, target, force);
    Ok(())
}

/// Show the plan, then decide whether to apply it.
fn proceed<T: serde::Serialize>(
    m: &StateMutateArgs,
    json: bool,
    plan: &T,
    human: impl FnOnce() -> String,
) -> CliResult<bool> {
    if m.dry_run {
        emit(json, plan, human)?;
        if !json {
            println!("dry run — nothing was written");
        }
        return Ok(false);
    }
    if m.yes {
        return Ok(true);
    }
    emit(json, plan, human)?;
    let stdin = std::io::stdin();
    confirm_with(
        &mut stdin.lock(),
        &mut std::io::stderr(),
        std::io::stdin().is_terminal(),
    )
}

/// Ask on a terminal; refuse without one.
pub fn confirm_with(
    input: &mut dyn BufRead,
    output: &mut dyn Write,
    interactive: bool,
) -> CliResult<bool> {
    if !interactive {
        return Err(CliError::Config(
            "refusing to change durable state without confirmation — pass --yes (or --dry-run \
             to only see the plan)"
                .into(),
        ));
    }
    write!(output, "Apply these changes? [y/N] ")?;
    output.flush()?;
    let mut answer = String::new();
    input.read_line(&mut answer)?;
    let yes = matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes");
    if !yes {
        writeln!(output, "aborted — nothing was written")?;
    }
    Ok(yes)
}

fn to_pretty<T: serde::Serialize>(v: &T) -> CliResult<String> {
    serde_json::to_string_pretty(v)
        .map_err(|e| CliError::Internal(format!("serializing JSON output: {e}")))
}

fn emit<T: serde::Serialize>(json: bool, v: &T, human: impl FnOnce() -> String) -> CliResult<()> {
    let text = if json { to_pretty(v)? + "\n" } else { human() };
    print!("{}", crate::secrets::registry::redact(&text));
    Ok(())
}

fn compact(v: &serde_json::Value) -> String {
    let s = v.to_string();
    if s.chars().count() > 200 {
        let mut t: String = s.chars().take(199).collect();
        t.push('…');
        t
    } else {
        s
    }
}

fn marker_summary(e: &KeyEntry) -> String {
    let v = &e.value;
    let detail = match &e.key.kind {
        KeyKind::Sla => v
            .get("last_success_unix")
            .and_then(|s| s.as_i64())
            .and_then(|s| chrono::DateTime::from_timestamp(s, 0))
            .map(|t| format!("last success {}", t.format("%Y-%m-%dT%H:%M:%SZ")))
            .unwrap_or_else(|| "no success yet".into()),
        KeyKind::Profiling => format!(
            "{} run(s) in the baseline",
            v.get("runs").and_then(|r| r.as_array()).map_or(0, Vec::len)
        ),
        KeyKind::RollbackIndex => format!(
            "{} undoable run(s)",
            v.get("runs").and_then(|r| r.as_array()).map_or(0, Vec::len)
        ),
        KeyKind::RollbackRun(id) => format!("run {id}"),
        KeyKind::Status => {
            let o = crate::pipeline_state::outcome::RunOutcomes::from_value(v.clone());
            let fmt = |e: &Option<crate::pipeline_state::outcome::OutcomeEvent>| {
                e.as_ref()
                    .map(|e| e.at.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                    .unwrap_or_else(|| "never".into())
            };
            format!(
                "last success {}, last failure {}",
                fmt(&o.last_success),
                fmt(&o.last_failure)
            )
        }
        KeyKind::Lease => match crate::pipeline_state::lease::RunLease::from_value(v.clone()) {
            Some(l) if l.is_live(Utc::now()) => format!("run {} in flight", l.run_id),
            Some(l) => format!("expired lease of run {}", l.run_id),
            None => "unreadable".into(),
        },
        _ => compact(v),
    };
    format!("{:<15} {detail}", e.key.kind.label())
}

fn render_show(r: &ops::ShowReport) -> String {
    let mut out = format!("pipeline {}\n", r.pipeline);
    for row in &r.rows {
        out.push_str(&format!(
            "\nrow {} ({:?}) — key {}{}\n",
            row.row,
            row.role,
            row.state_key,
            row.store
                .as_deref()
                .map(|s| format!(", {s} store"))
                .unwrap_or_else(|| ", no state store".into())
        ));
        match &row.bookmark {
            Some(b) => out.push_str(&format!("  bookmark        {}\n", compact(b))),
            None => {
                out.push_str("  bookmark        none — the next run starts from the beginning\n")
            }
        }
        if let Some(eo) = &row.exactly_once {
            out.push_str(&format!("  exactly-once    envelope sequence {}\n", eo.seq));
        }
        if let Some(f) = &row.state_format {
            out.push_str(&format!(
                "  state format    {} schema {} ({}{})\n",
                f.owner.as_deref().unwrap_or(&f.expected_owner),
                f.schema,
                f.status,
                f.detail
                    .as_deref()
                    .map(|d| format!(": {d}"))
                    .unwrap_or_default()
            ));
        }
        if let Some(l) = &row.running {
            out.push_str(&format!(
                "  running         run {} (pid {}) since {}\n",
                l.run_id,
                l.pid,
                l.acquired_at.format("%Y-%m-%dT%H:%M:%SZ")
            ));
        }
        for e in &row.sub_bookmarks {
            out.push_str(&format!(
                "  sub-bookmark    {} = {}\n",
                e.key.sub.as_deref().unwrap_or(""),
                compact(&e.value)
            ));
        }
        for e in &row.markers {
            out.push_str(&format!("  {}\n", marker_summary(e)));
        }
    }
    if !r.pipeline_keys.is_empty() {
        out.push_str("\npipeline markers\n");
        for e in &r.pipeline_keys {
            out.push_str(&format!(
                "  {:<15} {} = {}\n",
                e.key.kind.label(),
                e.key.key,
                compact(&e.value)
            ));
        }
    }
    if !r.orphans.is_empty() {
        out.push_str("\nkeys of rows no longer in the config\n");
        for e in &r.orphans {
            out.push_str(&format!("  {} = {}\n", e.key.key, compact(&e.value)));
        }
    }
    out
}

fn warnings(out: &mut String, w: &[String]) {
    for line in w {
        out.push_str(&format!("  note: {line}\n"));
    }
}

fn render_set(o: &ops::SetOutcome) -> String {
    let mut out = format!(
        "{} bookmark of row {} ({})\n  before: {}\n  after:  {}\n",
        if o.applied { "moved" } else { "would move" },
        o.row,
        o.key,
        o.before
            .as_ref()
            .map(compact)
            .unwrap_or_else(|| "none".into()),
        compact(&o.after)
    );
    warnings(&mut out, &o.warnings);
    out
}

fn render_reset(o: &ops::ResetOutcome) -> String {
    let mut out = format!(
        "{} row {}\n",
        if o.applied { "reset" } else { "would reset" },
        o.row
    );
    for c in &o.changes {
        match &c.after {
            None => out.push_str(&format!(
                "  delete  {} (was {})\n",
                c.key,
                compact(&c.before)
            )),
            Some(v) => out.push_str(&format!(
                "  rewrite {}: {} → {}\n",
                c.key,
                compact(&c.before),
                compact(v)
            )),
        }
    }
    if o.token_rewound {
        out.push_str("  the sink's commit token was deleted\n");
    }
    warnings(&mut out, &o.warnings);
    out
}

fn render_import(o: &ops::ImportOutcome) -> String {
    let mut out = format!(
        "{} {} key(s) of pipeline {}{}\n",
        if o.applied {
            "imported"
        } else {
            "would import"
        },
        o.written.len(),
        o.pipeline,
        if o.atomic { " (all-or-nothing)" } else { "" }
    );
    for k in &o.written {
        out.push_str(&format!("  write   {k}\n"));
    }
    for k in &o.deleted {
        out.push_str(&format!("  delete  {k}\n"));
    }
    if !o.existing.is_empty() {
        out.push_str(&format!(
            "  replacing {} existing key(s)\n",
            o.existing.len()
        ));
    }
    if let Some(e) = &o.error {
        out.push_str(&format!("  FAILED: {e}\n"));
    }
    warnings(&mut out, &o.warnings);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline_state::keys::classify;
    use serde_json::json;

    #[test]
    fn confirm_requires_a_terminal_or_yes() {
        let mut out = Vec::new();
        let err = confirm_with(&mut &b""[..], &mut out, false).unwrap_err();
        assert!(err.to_string().contains("--yes"));
        assert!(confirm_with(&mut &b"y\n"[..], &mut out, true).unwrap());
        assert!(confirm_with(&mut &b"YES\n"[..], &mut out, true).unwrap());
        assert!(!confirm_with(&mut &b"n\n"[..], &mut out, true).unwrap());
        assert!(String::from_utf8(out).unwrap().contains("aborted"));
    }

    fn entry(key: &str, value: serde_json::Value) -> KeyEntry {
        KeyEntry {
            key: classify("p", key).unwrap(),
            value,
        }
    }

    #[test]
    fn marker_summaries() {
        assert!(
            marker_summary(&entry("p::r::__sla__", json!({"last_success_unix": 1})))
                .contains("last success 1970")
        );
        assert!(marker_summary(&entry("p::r::__sla__", json!({}))).contains("no success"));
        assert!(
            marker_summary(&entry("p::r::__profiling__", json!({"runs": [1, 2]})))
                .contains("2 run")
        );
        assert!(
            marker_summary(&entry("p::r::__rollback__", json!({"runs": ["a"]})))
                .contains("1 undoable")
        );
        assert!(marker_summary(&entry("p::r::__rollback__::x", json!({}))).contains("run x"));
        assert!(marker_summary(&entry("p::r::__status__", json!({}))).contains("never"));
        let live = json!({"run_id": "r", "pid": 1, "acquired_at": Utc::now(), "expires_at": Utc::now() + chrono::Duration::seconds(60)});
        assert!(marker_summary(&entry("p::r::__lease__", live)).contains("in flight"));
        let dead = json!({"run_id": "r", "pid": 1, "acquired_at": Utc::now(), "expires_at": Utc::now() - chrono::Duration::seconds(60)});
        assert!(marker_summary(&entry("p::r::__lease__", dead)).contains("expired"));
        assert!(marker_summary(&entry("p::r::__lease__", json!(1))).contains("unreadable"));
        assert!(marker_summary(&entry("p::__replication__", json!({"a": 1}))).contains("\"a\""));
        assert_eq!(compact(&json!("x".repeat(300))).chars().count(), 200);
    }

    #[test]
    fn show_renders_every_section() {
        use crate::pipeline_state::lease::RunLease;
        use crate::pipeline_state::target::RowRole;
        let now = Utc::now();
        let report = ops::ShowReport {
            pipeline: "p".into(),
            rows: vec![ops::RowState {
                row: "r".into(),
                role: RowRole::Root,
                state_key: "p::r".into(),
                store: Some("file".into()),
                bookmark: Some(json!({"id": 3})),
                exactly_once: Some(ops::EnvelopeInfo { seq: 7 }),
                state_format: None,
                sub_bookmarks: vec![entry("p::r::parent-1", json!({"id": 1}))],
                markers: vec![entry("p::r::__sla__", json!({}))],
                running: Some(RunLease {
                    run_id: "run-9".into(),
                    pid: 42,
                    host: None,
                    pid_ns: None,
                    acquired_at: now,
                    expires_at: now,
                }),
            }],
            pipeline_keys: vec![entry("p::__replication__", json!({"phase": "cdc"}))],
            orphans: vec![entry("p::gone", json!({"id": 5}))],
        };
        let text = render_show(&report);
        assert!(
            text.contains("exactly-once    envelope sequence 7"),
            "{text}"
        );
        assert!(
            text.contains("running         run run-9 (pid 42)"),
            "{text}"
        );
        assert!(
            text.contains(r#"sub-bookmark    parent-1 = {"id":1}"#),
            "{text}"
        );
        assert!(text.contains("pipeline markers"), "{text}");
        assert!(
            text.contains(r#"p::__replication__ = {"phase":"cdc"}"#),
            "{text}"
        );
        assert!(
            text.contains("keys of rows no longer in the config"),
            "{text}"
        );
        assert!(text.contains(r#"p::gone = {"id":5}"#), "{text}");
    }

    #[test]
    fn import_renders_deletes_and_failures() {
        let o = ops::ImportOutcome {
            pipeline: "p".into(),
            keys: 1,
            existing: vec!["p::old".into()],
            written: vec!["p::r".into()],
            deleted: vec!["p::old".into()],
            atomic: false,
            applied: true,
            error: Some("store down".into()),
            warnings: vec![],
        };
        let text = render_import(&o);
        assert!(text.contains("delete  p::old"), "{text}");
        assert!(text.contains("replacing 1 existing key(s)"), "{text}");
        assert!(text.contains("FAILED: store down"), "{text}");
    }
}
