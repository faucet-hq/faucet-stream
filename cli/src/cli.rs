//! Argument parser shared by `main.rs` and the integration tests.

use crate::commands::completions;
use clap::{Args, Parser, Subcommand};
use clap_complete::engine::ArgValueCandidates;
use std::path::PathBuf;

/// Runtime matrix-row selection flags, shared by `run`/`validate`/`preview`/
/// `plan` via `#[command(flatten)]`. Implements the selection model of
/// #370 (identity), #371 (status), #376 (tags), and #377 (include_parents).
#[derive(Debug, Args, Default, Clone)]
pub struct SelectionArgs {
    /// Run only matrix rows whose id exactly matches. Repeatable and/or
    /// comma-joined (`--select people --select time_off` or `--select a,b`).
    /// Force-includes by name, bypassing the `status` gate. (#370)
    #[arg(long = "select", value_delimiter = ',', env = "FAUCET_SELECT",
          add = ArgValueCandidates::new(completions::matrix_id_candidates))]
    pub select: Vec<String>,

    /// Like `--select` but glob-matched against row ids (`--only 'timeoff_*'`).
    /// Also bypasses the `status` gate. Repeatable / comma-joined. (#370)
    #[arg(long = "only", value_delimiter = ',',
          add = ArgValueCandidates::new(completions::matrix_id_candidates))]
    pub only: Vec<String>,

    /// Remove matching rows (exact id or glob) from the run set, applied last.
    /// A `mandatory` row is removable only by an exact `--skip <id>`. (#370)
    #[arg(long = "skip", value_delimiter = ',', env = "FAUCET_SKIP",
          add = ArgValueCandidates::new(completions::matrix_id_candidates))]
    pub skip: Vec<String>,

    /// Additively include a readiness tier beyond the default
    /// `{mandatory, active}` set: `available` / `draft` / `archived`.
    /// Repeatable / comma-joined. (#371)
    #[arg(long = "status", value_delimiter = ',', env = "FAUCET_STATUS",
          add = ArgValueCandidates::new(completions::status_candidates))]
    pub status: Vec<String>,

    /// Narrow the eligible set to rows carrying any listed tag (union).
    /// Cannot resurrect a non-eligible row — raise `--status` for that.
    /// Repeatable / comma-joined. (#376)
    #[arg(long = "tag", value_delimiter = ',', env = "FAUCET_TAGS",
          add = ArgValueCandidates::new(completions::tag_candidates))]
    pub tags: Vec<String>,

    /// How a selected row's `parent:` / `depends_on:` ancestors are resolved
    /// when not independently selected: `off` (default, strict — error on a
    /// missing ancestor), `eligible`, or `all`. Overrides
    /// `selection.include_parents` in the config. (#377)
    #[arg(long = "include-parents", env = "FAUCET_INCLUDE_PARENTS")]
    pub include_parents: Option<String>,
}

/// `faucet` — config-driven runner for faucet-stream pipelines.
#[derive(Debug, Parser)]
#[command(name = "faucet", version, about, long_about = None)]
pub struct Cli {
    /// Override the global log level (also honors `FAUCET_LOG`).
    #[arg(long, global = true, env = "FAUCET_LOG", default_value = "info")]
    pub log_level: String,

    #[command(subcommand)]
    pub command: Command,
}

/// Top-level subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Execute a pipeline config end-to-end.
    Run(RunArgs),
    /// Replay a bounded historical window of a pipeline: chunk --from/--to
    /// into window units, run them with bounded parallelism, and record
    /// durable, resumable progress. Exits non-zero if any unit fails.
    Backfill(BackfillArgs),
    /// Bulk-snapshot a database table, then stream CDC from a position captured
    /// before the snapshot (a true mirror with `write_mode: upsert`).
    /// Long-running when `replication.continuous` is true (Ctrl-C / SIGTERM to stop).
    Replicate(ReplicateArgs),
    /// Connect to a config's source, enumerate the datasets behind it
    /// (tables / collections / indices / prefixes), and emit a ready-to-run
    /// config with one matrix row per dataset.
    Discover(DiscoverArgs),
    /// Parse + validate a pipeline config without running it.
    Validate(ValidateArgs),
    /// Print the JSON Schema for a specific connector.
    Schema(SchemaArgs),
    /// List every compiled-in source, sink, and transform with a one-line
    /// description (`--available` lists the whole connector registry instead).
    List(ListArgs),
    /// Search the connector registry index for connectors by name / keyword.
    Search(SearchArgs),
    /// Score each connector's conformance to the faucet SDK contract and print
    /// its maturity tier (Stable / Experimental / Beta / Draft) + capabilities.
    Conformance(ConformanceArgs),
    /// Show how to install or enable a connector from the registry index
    /// (prints the recipe; never executes anything).
    Install(InstallArgs),
    /// Run only the source side and print records to stdout (uses the stdout sink).
    Preview(PreviewArgs),
    /// Read-only preview of what a config would do: resolved pipeline, inferred
    /// output schema, sink schema delta, lineage, and target sinks — zero writes.
    Plan(PlanArgs),
    /// Watch a config and re-run a sample offline on every save, printing a
    /// live diff of the output. Requires the `cli-dev` build feature.
    #[cfg(feature = "cli-dev")]
    Dev(DevArgs),
    /// Scaffold a starter `pipeline.yaml` to disk.
    Init(InitArgs),
    /// Scaffold a new artifact — currently a third-party connector crate.
    New(NewArgs),
    /// Probe every connector in a config (auth / network / permissions) and
    /// print a green/red checklist. Exits non-zero if any probe fails.
    Doctor(DoctorArgs),
    /// Run fixture-based offline pipeline tests from one or more spec files.
    /// No real source or sink is touched. Exits non-zero if any case fails.
    Test(TestArgs),
    /// Inspect, replay, or discard dead-letter-queue envelopes written by a
    /// pipeline's `dlq:` sink.
    Dlq(DlqArgs),
    /// Validate a config's `contract:` block and print a summary, or export
    /// it in a machine-readable format (`--export`).
    #[cfg(feature = "contract")]
    Contract(ContractArgs),
    /// Validate a config's `masking:` block and print which rules apply to
    /// each destination sink.
    #[cfg(feature = "masking")]
    Masking(MaskingArgs),
    /// Run a pipeline on a cron schedule (long-running; Ctrl-C / SIGTERM to stop).
    #[cfg(feature = "schedule")]
    Schedule(ScheduleArgs),
    /// Run a long-running HTTP control plane (submit / poll / cancel pipeline runs).
    #[cfg(feature = "serve")]
    Serve(ServeArgs),
    /// Run an MCP (Model Context Protocol) server over stdio, exposing faucet's
    /// introspection surfaces as agent tool calls (for Claude Desktop / Code).
    #[cfg(feature = "mcp")]
    Mcp(McpArgs),
    /// Send a synthetic notification through a config's `notifications:` rules
    /// to validate channel setup end-to-end (no pipeline runs).
    #[cfg(feature = "notify")]
    Notify(NotifyArgs),
    /// Browse the Data Movement Catalog accumulated by a config's `catalog:`
    /// store — datasets, schema timelines, volume/freshness, lineage.
    #[cfg(feature = "catalog")]
    Catalog(CatalogArgs),
    /// Register a parameterized config once, then trigger runs by id + params.
    /// The registry is shared with `faucet serve` — point both at the same
    /// store URL and templates registered here are triggerable over HTTP.
    #[cfg(feature = "templates")]
    Template(TemplateArgs),
    /// Generate a shell tab-completion script (bash / zsh / fish / powershell /
    /// elvish). For registry- and config-aware *dynamic* completion, enable the
    /// `COMPLETE` hook instead, e.g. `source <(COMPLETE=zsh faucet)`.
    Completions(CompletionsArgs),
    /// Upgrade a config written against an older `faucet` grammar to the current
    /// shape (e.g. pre-`pipeline:` top-level source/sink, legacy inline auth).
    /// Idempotent; rewrites in place unless `--check` / `--stdout`.
    Migrate(MigrateArgs),
    /// Canonicalize a config: stable key order, normalized style. Idempotent;
    /// rewrites in place unless `--check` / `--stdout`. Comments are not
    /// preserved (the config is parsed and re-serialized).
    Fmt(FmtArgs),
    /// Explain, in plain English, what a pipeline config does — source →
    /// transforms → sink, matrix expansion, replication, delivery guarantee,
    /// and state store. Read-only and fully offline (no source is touched).
    Explain(ExplainArgs),
    /// Show recent run history recorded in a config's `catalog:` store —
    /// status, duration, throughput, and bookmark. Read-only; requires the
    /// `catalog` build feature.
    #[cfg(feature = "catalog")]
    History(HistoryArgs),
    /// Reclaim the local files a pipeline's sinks wrote (jsonl / csv / parquet)
    /// — the manual half of the retention GC `faucet serve` runs on a timer.
    /// Deletes only paths faucet recorded as its own outputs; run history,
    /// catalog entries, and lineage are untouched. Requires the `catalog` build
    /// feature.
    #[cfg(feature = "catalog")]
    Cleanup(CleanupArgs),
}

/// `faucet cleanup` arguments (#587).
#[cfg(feature = "catalog")]
#[derive(Debug, Parser)]
pub struct CleanupArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config whose `catalog:`
    /// block names the store holding the local-output ledger. If omitted,
    /// auto-discover `faucet.yaml` / `faucet.yml` / `faucet.json` in cwd.
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
    /// Emit the machine-readable sweep report instead of the human summary.
    #[arg(long)]
    pub json: bool,
    /// Ledger store URL (`sqlite:<path>`, `postgres://…`, or `memory`), instead
    /// of reading it from a config's `catalog:` block. Point this at the same URL
    /// `faucet serve --history` uses to clean a server's outputs.
    #[arg(long)]
    pub store: Option<String>,
    /// Delete outputs older than this many days, ignoring per-pipeline
    /// `local_outputs.retention_days` overrides.
    #[arg(long, conflicts_with_all = ["dataset", "output", "all", "run"])]
    pub older_than_days: Option<u32>,
    /// Delete every tracked output of one dataset (a catalog dataset id).
    #[arg(long, conflicts_with_all = ["output", "all", "run"])]
    pub dataset: Option<String>,
    /// Delete the outputs one run most recently wrote — "clean up after that
    /// run". The run's history record is untouched.
    #[arg(long, conflicts_with_all = ["output", "all"])]
    pub run: Option<String>,
    /// Delete one output, by the id `faucet cleanup --json` / the console shows.
    #[arg(long, conflicts_with = "all")]
    pub output: Option<String>,
    /// Delete **every** tracked output, including ones still inside their
    /// retention window. Requires `--yes` (or `--dry-run`).
    #[arg(long)]
    pub all: bool,
    /// Retention window in days for the default (expired-only) sweep, overriding
    /// the config's `local_outputs.retention_days`. `0` = keep forever. Distinct
    /// from `--older-than-days`, which is a *scope selector* that ignores every
    /// retention setting.
    ///
    /// Reads `FAUCET_LOCAL_SINK_OUTPUT_RETENTION_DAYS` when unset, so the runtime
    /// default is the same number here and under `faucet serve` rather than the
    /// env being silently serve-only.
    #[arg(long, env = "FAUCET_LOCAL_SINK_OUTPUT_RETENTION_DAYS")]
    pub retention_days: Option<u32>,
    /// Never delete an output touched within this many seconds — the guard
    /// against unlinking a file a run is still writing (including one in another
    /// process, e.g. a live `faucet serve` sharing this store). `0` disables it
    /// when you know nothing is running. Default: 60.
    #[arg(long, default_value_t = 60)]
    pub in_flight_grace_secs: u64,
    /// Report what would be deleted without touching anything.
    #[arg(long)]
    pub dry_run: bool,
    /// Confirm a sweep that ignores retention windows (`--all`, or
    /// `--older-than-days 0`).
    #[arg(long)]
    pub yes: bool,
}

/// `faucet migrate` arguments.
#[derive(Debug, Args)]
pub struct MigrateArgs {
    /// Config file to migrate. Auto-discovered (`faucet.yaml` → `.yml` →
    /// `.json`) when omitted.
    #[arg(value_hint = clap::ValueHint::FilePath)]
    pub config: Option<PathBuf>,
    /// Report whether a migration is needed without writing (exits non-zero if
    /// the config is not current). For CI / pre-upgrade checks.
    #[arg(long)]
    pub check: bool,
    /// Write the migrated config to stdout instead of rewriting the file.
    #[arg(long, conflicts_with = "check")]
    pub stdout: bool,
}

/// `faucet fmt` arguments.
#[derive(Debug, Args)]
pub struct FmtArgs {
    /// Config file(s) to format. Auto-discovered (`faucet.yaml` → `.yml` →
    /// `.json`) when none are given.
    #[arg(value_hint = clap::ValueHint::FilePath)]
    pub configs: Vec<PathBuf>,
    /// Report whether each file is already canonical without writing (exits
    /// non-zero and prints a unified diff for any file that is not). For CI.
    #[arg(long)]
    pub check: bool,
    /// Write the formatted result to stdout instead of rewriting the file(s).
    #[arg(long, conflicts_with = "check")]
    pub stdout: bool,
}

/// `faucet explain` arguments.
#[derive(Debug, Args)]
pub struct ExplainArgs {
    /// Path to a `.yaml`/`.yml`/`.json` config (auto-discovered if omitted).
    pub config: Option<PathBuf>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
    /// Emit the narration as structured JSON instead of prose.
    #[arg(long)]
    pub json: bool,
    /// Narrate every matrix row instead of summarizing a large matrix.
    #[arg(long)]
    pub rows: bool,
}

/// `faucet history` arguments.
#[cfg(feature = "catalog")]
#[derive(Debug, Args)]
pub struct HistoryArgs {
    /// Path to a config carrying a `catalog:` block (auto-discovered if omitted).
    pub config: Option<PathBuf>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
    /// Maximum number of runs to show, newest first.
    #[arg(long, default_value_t = 20)]
    pub limit: usize,
    /// Show only runs that contain an invocation for this matrix row id.
    #[arg(long)]
    pub row: Option<String>,
    /// Emit the history as JSON instead of a table.
    #[arg(long)]
    pub json: bool,
}

/// `faucet completions` arguments.
#[derive(Debug, Args)]
pub struct CompletionsArgs {
    /// Target shell.
    pub shell: clap_complete::aot::Shell,
}

/// `faucet catalog` arguments.
#[cfg(feature = "catalog")]
#[derive(Debug, Parser)]
pub struct CatalogArgs {
    #[command(subcommand)]
    pub command: CatalogCommand,
}

/// `faucet catalog` subcommands.
#[cfg(feature = "catalog")]
#[derive(Debug, Subcommand)]
pub enum CatalogCommand {
    /// List every catalogued dataset (newest activity first).
    Datasets(CatalogDatasetsArgs),
    /// Show one dataset's detail: schema timeline, volume points, edges.
    Show(CatalogShowArgs),
    /// Print the dataset lineage graph (optionally rooted at a dataset).
    Lineage(CatalogLineageArgs),
}

/// Shared config-loading flags for the `faucet catalog` subcommands.
#[cfg(feature = "catalog")]
#[derive(Debug, Parser)]
pub struct CatalogConfigArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config with a
    /// `catalog:` block naming the store. If omitted, auto-discover
    /// `faucet.yaml` / `faucet.yml` / `faucet.json` in cwd.
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block.
    /// Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
    /// Emit machine-readable JSON instead of the human summary.
    #[arg(long)]
    pub json: bool,
}

/// `faucet catalog datasets` arguments.
#[cfg(feature = "catalog")]
#[derive(Debug, Parser)]
pub struct CatalogDatasetsArgs {
    #[command(flatten)]
    pub common: CatalogConfigArgs,
    /// Only datasets of this connector kind (e.g. `postgres`, `csv`).
    #[arg(long)]
    pub kind: Option<String>,
    /// Case-insensitive substring match on the dataset URI.
    #[arg(long)]
    pub q: Option<String>,
    /// Max datasets to list.
    #[arg(long, default_value_t = 100)]
    pub limit: usize,
}

/// `faucet catalog show <id>` arguments.
#[cfg(feature = "catalog")]
#[derive(Debug, Parser)]
pub struct CatalogShowArgs {
    /// Dataset id (from `faucet catalog datasets`), or a unique prefix of one.
    pub id: String,
    #[command(flatten)]
    pub common: CatalogConfigArgs,
}

/// `faucet catalog lineage` arguments.
#[cfg(feature = "catalog")]
#[derive(Debug, Parser)]
pub struct CatalogLineageArgs {
    #[command(flatten)]
    pub common: CatalogConfigArgs,
    /// Dataset id to root the graph at (whole graph when omitted).
    #[arg(long)]
    pub root: Option<String>,
    /// BFS hop bound around --root.
    #[arg(long, default_value_t = 5)]
    pub depth: u32,
}

/// `faucet template` arguments (#444).
#[cfg(feature = "templates")]
#[derive(Debug, Parser)]
pub struct TemplateArgs {
    #[command(subcommand)]
    pub command: TemplateCommand,
}

/// `faucet template` subcommands.
#[cfg(feature = "templates")]
#[derive(Debug, Subcommand)]
pub enum TemplateCommand {
    /// Validate a config and register it as a new template version.
    Register(TemplateRegisterArgs),
    /// List registered templates (newest version of each, plus its release state).
    List(TemplateListArgs),
    /// Show one template: its params, config body, and versions.
    Show(TemplateShowArgs),
    /// Make a version live — what unpinned runs will use. The one action that
    /// moves existing callers; registering a build never does.
    Launch(TemplateLaunchArgs),
    /// Re-launch the previously launched version.
    Rollback(TemplateRollbackArgs),
    /// Retire a template (or revive one with `--undo`).
    Deprecate(TemplateDeprecateArgs),
    /// Point a named environment channel (`prod`, `staging`, …) at a version.
    Promote(TemplatePromoteArgs),
    /// Delete one version, or every version, of a template.
    Delete(TemplateDeleteArgs),
    /// Materialize a template with the given params and run it locally.
    Run(TemplateRunArgs),
}

/// Where the template registry lives — shared by every `faucet template`
/// subcommand.
#[cfg(feature = "templates")]
#[derive(Debug, Parser)]
pub struct TemplateStoreArgs {
    /// Registry store URL: `sqlite:<path>`, a `postgres://…` URL, or `memory`
    /// (process-lifetime only — useful for a smoke test). Point
    /// `faucet serve --history` at the same URL to trigger these templates over
    /// HTTP. SQL backends need the matching `serve-history-sqlite` /
    /// `serve-history-postgres` build feature.
    #[arg(long, env = "FAUCET_TEMPLATE_STORE")]
    pub store: String,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Emit machine-readable JSON instead of the human summary.
    #[arg(long)]
    pub json: bool,
}

/// `faucet template register <config>` arguments.
#[cfg(feature = "templates")]
#[derive(Debug, Parser)]
pub struct TemplateRegisterArgs {
    /// Path to the `.yaml`, `.yml`, or `.json` config to register. Stored
    /// verbatim, so `${env:…}` / `${vault:…}` stay unresolved and are resolved
    /// when a run is triggered.
    #[arg(value_hint = clap::ValueHint::FilePath)]
    pub config: PathBuf,
    /// Registry id. Derived from the config's `name:` when omitted.
    #[arg(long)]
    pub id: Option<String>,
    /// Free-text description shown by `list` / `show`.
    #[arg(long)]
    pub description: Option<String>,
    /// Point a named channel at the newly registered version, e.g.
    /// `--tag dev --tag test`. The version number itself always auto-increments;
    /// channels come from a fixed set (`dev`, `test`, `staging`, `pre-prod`,
    /// `canary`, `stable`, `prod`, `previous`). `latest` is derived and always
    /// names the newest version, so it cannot be assigned.
    #[arg(long = "tag", value_name = "CHANNEL")]
    pub tag: Vec<String>,
    /// Launch the new version immediately, making it the one unpinned runs use.
    /// Without this the version is registered but inert — a new build never moves
    /// existing callers until you launch it.
    #[arg(long)]
    pub launch: bool,
    #[command(flatten)]
    pub common: TemplateStoreArgs,
}

/// `faucet template promote <id>` arguments.
#[cfg(feature = "templates")]
#[derive(Debug, Parser)]
pub struct TemplatePromoteArgs {
    /// Template id.
    pub id: String,
    /// Channel to move: `dev`, `test`, `staging`, `pre-prod`, `canary`, or
    /// `prod`. The derived channels (`stable`, `previous`, `newest`) cannot be
    /// promoted — `stable` moves with `faucet template launch`.
    #[arg(long = "tag", value_name = "CHANNEL")]
    pub tag: String,
    /// What to point it at: a version number, or another channel whose current
    /// target should be copied (`--tag prod --version pre-prod`). Defaults to
    /// `stable`, the currently launched version.
    #[arg(long, default_value = "stable")]
    pub version: String,
    #[command(flatten)]
    pub common: TemplateStoreArgs,
}

/// `faucet template launch <id>` arguments.
#[cfg(feature = "templates")]
#[derive(Debug, Parser)]
pub struct TemplateLaunchArgs {
    /// Template id.
    pub id: String,
    /// Which version to make live: a number, or a channel whose current target to
    /// copy (`--version pre-prod` launches whatever passed pre-prod). Defaults to
    /// `newest` — launching what you just registered is the common case.
    #[arg(long, default_value = "newest")]
    pub version: String,
    #[command(flatten)]
    pub common: TemplateStoreArgs,
}

/// `faucet template rollback <id>` arguments.
#[cfg(feature = "templates")]
#[derive(Debug, Parser)]
pub struct TemplateRollbackArgs {
    /// Template id.
    pub id: String,
    #[command(flatten)]
    pub common: TemplateStoreArgs,
}

/// `faucet template deprecate <id>` arguments.
#[cfg(feature = "templates")]
#[derive(Debug, Parser)]
pub struct TemplateDeprecateArgs {
    /// Template id.
    pub id: String,
    /// Why it is being retired — shown to anyone who triggers it.
    #[arg(long)]
    pub reason: Option<String>,
    /// Revive a deprecated template instead of retiring it.
    #[arg(long)]
    pub undo: bool,
    #[command(flatten)]
    pub common: TemplateStoreArgs,
}

/// `faucet template list` arguments.
#[cfg(feature = "templates")]
#[derive(Debug, Parser)]
pub struct TemplateListArgs {
    #[command(flatten)]
    pub common: TemplateStoreArgs,
}

/// `faucet template show <id>` arguments.
#[cfg(feature = "templates")]
#[derive(Debug, Parser)]
pub struct TemplateShowArgs {
    /// Template id.
    pub id: String,
    /// Version to show: a number, or a named channel (`stable` — the default,
    /// i.e. the launched version — `newest`, `previous`, `prod`, `dev`, …).
    #[arg(long, default_value = "stable")]
    pub version: String,
    /// Print ONLY the pure template config — comments stripped, re-emitted as
    /// canonical YAML — so it pipes cleanly to a file. Suppresses the metadata
    /// report. Ignored with `--json`.
    #[arg(long)]
    pub clean: bool,
    #[command(flatten)]
    pub common: TemplateStoreArgs,
}

/// `faucet template delete <id>` arguments.
#[cfg(feature = "templates")]
#[derive(Debug, Parser)]
pub struct TemplateDeleteArgs {
    /// Template id.
    pub id: String,
    /// Delete only this version — a number, or a named channel (`latest`,
    /// `prod`, …) resolved to the version it points at. Omitted = delete every
    /// version of the template.
    #[arg(long)]
    pub version: Option<String>,
    #[command(flatten)]
    pub common: TemplateStoreArgs,
}

/// `faucet template run <id>` arguments.
#[cfg(feature = "templates")]
#[derive(Debug, Parser)]
pub struct TemplateRunArgs {
    /// Template id.
    pub id: String,
    /// Version to run: a number, or a named channel. Defaults to `stable` — the
    /// launched version — so an unpinned run never picks up a build that has not
    /// been launched. Use `newest` to run the most recent build regardless.
    #[arg(long, default_value = "stable")]
    pub version: String,
    /// Supply a declared param: `--param tenant_id=acme`. Repeatable.
    #[arg(long = "param", value_name = "NAME=VALUE")]
    pub param: Vec<String>,
    /// Override an environment variable for this materialization only:
    /// `--param-env REGION=eu`, or bare `--param-env TOKEN` to take it from the
    /// caller's environment. Repeatable.
    #[arg(long = "param-env", value_name = "NAME[=VALUE]")]
    pub param_env: Vec<String>,
    /// Materialize and validate without running (prints the resolved config).
    #[arg(long)]
    pub dry_run: bool,
    /// Stop after writing this many records to the sink.
    #[arg(long)]
    pub limit: Option<usize>,
    #[command(flatten)]
    pub common: TemplateStoreArgs,
}

/// `faucet notify test` arguments.
#[cfg(feature = "notify")]
#[derive(Debug, Parser)]
pub struct NotifyArgs {
    #[command(subcommand)]
    pub command: NotifyCommand,
}

/// `faucet notify` subcommands.
#[cfg(feature = "notify")]
#[derive(Debug, Subcommand)]
pub enum NotifyCommand {
    /// Fire one synthetic event at every matching rule in the config.
    Test(NotifyTestArgs),
}

/// `faucet notify test <config>` arguments.
#[cfg(feature = "notify")]
#[derive(Debug, Parser)]
pub struct NotifyTestArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config with a
    /// `notifications:` block. If omitted, auto-discover in cwd.
    pub config: Option<PathBuf>,
    /// Which event to synthesize (defaults to `run_failure`).
    #[arg(long, default_value = "run_failure")]
    pub event: String,
    /// Path to a `.env` file for `${env:VAR}` interpolation.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Disable `.env` auto-discovery.
    #[arg(long)]
    pub no_env_file: bool,
}

/// `faucet test` arguments.
#[derive(Debug, Parser)]
pub struct TestArgs {
    /// One or more test-spec files (`.yaml`, `.yml`, or `.json`), e.g.
    /// `faucet test tests/*.yaml`.
    #[arg(required = true)]
    pub specs: Vec<PathBuf>,
    /// Run only cases whose name contains this substring.
    #[arg(long)]
    pub filter: Option<String>,
    /// Emit a machine-readable JSON report instead of the human checklist.
    #[arg(long)]
    pub json: bool,
    /// Default `${now.*}` clock for cases without their own `clock:` field
    /// (RFC3339 like `2026-01-31T00:00:00Z`, or a date `2026-01-31`).
    /// Defaults to process start (UTC).
    #[arg(long)]
    pub clock: Option<String>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation in
    /// referenced pipeline configs. Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from each referenced config's `profiles:` block.
    /// Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
    /// Resolve `${vault:…}` / `${aws-sm:…}` / … secret directives in
    /// referenced configs (requires network + credentials). By default tests
    /// load configs offline and leave secret directives unresolved — safe
    /// because the real source/sink configs holding them are never used.
    #[arg(long)]
    pub resolve_secrets: bool,
}

/// `faucet dlq` arguments.
#[derive(Debug, Parser)]
pub struct DlqArgs {
    #[command(subcommand)]
    pub command: DlqCommand,
}

/// `faucet dlq` subcommands.
#[derive(Debug, Subcommand)]
pub enum DlqCommand {
    /// Read a DLQ location back and print a per-reason / per-error-kind
    /// breakdown plus a sample of quarantined records.
    Inspect(DlqInspectArgs),
    /// Re-feed quarantined records through a pipeline config (transforms →
    /// quality → contract → sink). Rows that fail again land in a *fresh* DLQ.
    Replay(DlqReplayArgs),
    /// Remove processed envelopes from a DLQ location (archive by default,
    /// or `--delete`), filtered by reason and/or age.
    Discard(DlqDiscardArgs),
}

/// `faucet dlq inspect <location>` arguments.
#[derive(Debug, Parser)]
pub struct DlqInspectArgs {
    /// DLQ location: a `.jsonl` file, a directory of `*.jsonl` files, or a glob.
    pub location: String,
    /// Only include envelopes with this DLQ reason (`partial` / `dlq_all` /
    /// `quality` / `schema_drift` / `contract`).
    #[arg(long)]
    pub reason: Option<String>,
    /// Number of sample records to show. Default: 5.
    #[arg(long, default_value_t = 5)]
    pub limit: usize,
    /// Key for a DLQ sealed at rest by the jsonl sink's `encryption` block.
    /// Repeat the flag to also try older (rotated) keys. Requires a build
    /// with the `encryption` feature.
    #[arg(long = "encryption-key")]
    pub encryption_key: Vec<String>,
    /// Emit a machine-readable JSON summary instead of the human report.
    #[arg(long)]
    pub json: bool,
}

/// `faucet dlq replay <config> --from <location>` arguments.
#[derive(Debug, Parser)]
pub struct DlqReplayArgs {
    /// Path to the pipeline config whose sink / transforms / quality / contract
    /// the replayed records flow through. If omitted, auto-discover in cwd.
    pub config: Option<PathBuf>,
    /// DLQ location to replay from: a `.jsonl` file, a directory, or a glob.
    #[arg(long)]
    pub from: String,
    /// Only replay envelopes with this DLQ reason.
    #[arg(long)]
    pub reason: Option<String>,
    /// Where replayed rows that fail *again* are quarantined. Defaults to a
    /// `replay-failed.jsonl` sibling of the source (never the source itself).
    #[arg(long)]
    pub failed_dlq: Option<String>,
    /// Which root row of the config to replay through. Defaults to the first root.
    #[arg(long)]
    pub row: Option<String>,
    /// Report what would be replayed without writing to the sink.
    #[arg(long)]
    pub dry_run: bool,
    /// Key for a DLQ sealed at rest by the jsonl sink's `encryption` block.
    /// Repeat the flag to also try older (rotated) keys. Requires a build
    /// with the `encryption` feature.
    #[arg(long = "encryption-key")]
    pub encryption_key: Vec<String>,
    /// (Replay picks up the config's own dlq `encryption` block automatically
    /// when no key is passed.)
    /// Emit a machine-readable JSON result instead of the human summary.
    #[arg(long)]
    pub json: bool,
    /// Path to a `.env` file for `${env:VAR}` interpolation in the config.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
}

/// `faucet dlq discard <location>` arguments.
#[derive(Debug, Parser)]
pub struct DlqDiscardArgs {
    /// DLQ location: a `.jsonl` file, a directory of `*.jsonl` files, or a glob.
    pub location: String,
    /// Only discard envelopes with this DLQ reason.
    #[arg(long)]
    pub reason: Option<String>,
    /// Only discard envelopes older than this: an RFC3339 timestamp
    /// (`2026-06-01T00:00:00Z`) or a relative age (`7d`, `24h`, `30m`).
    #[arg(long)]
    pub before: Option<String>,
    /// Permanently delete matching envelopes instead of archiving them to a
    /// `<file>.archived.jsonl` sibling.
    #[arg(long)]
    pub delete: bool,
    /// Key for a DLQ sealed at rest by the jsonl sink's `encryption` block.
    /// Repeat the flag to also try older (rotated) keys. Requires a build
    /// with the `encryption` feature.
    #[arg(long = "encryption-key")]
    pub encryption_key: Vec<String>,
    /// Emit a machine-readable JSON result instead of the human summary.
    #[arg(long)]
    pub json: bool,
}

/// `faucet doctor` arguments.
#[derive(Debug, Parser)]
pub struct DoctorArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config. If omitted,
    /// auto-discover `faucet.yaml` / `faucet.yml` / `faucet.json` in cwd.
    pub config: Option<PathBuf>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Per-probe timeout in seconds.
    #[arg(long, default_value_t = 10)]
    pub timeout_secs: u64,
    /// Emit machine-readable JSON instead of the human checklist.
    #[arg(long)]
    pub json: bool,
    /// Run only the offline static config lints (no network probes): dangling /
    /// unreferenced `auth:` providers, unused `vars:`, and no-op sink
    /// `batch_size: 0`. Fast and credential-free — ideal for CI. Exits non-zero
    /// on any lint *error* (warnings don't fail).
    #[arg(long)]
    pub offline: bool,
    /// Select a named overlay from the config's `profiles:` block and deep-merge
    /// it over the composed base. Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
}

/// `faucet contract` arguments.
#[cfg(feature = "contract")]
#[derive(Debug, Parser)]
pub struct ContractArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config with a
    /// `pipeline.contract:` block. If omitted, auto-discover
    /// `faucet.yaml` / `faucet.yml` / `faucet.json` in cwd.
    pub config: Option<PathBuf>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block and deep-merge
    /// it over the composed base. Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
    /// Export the contract in a machine-readable format instead of the
    /// human summary: the canonical contract JSON, a standalone JSON Schema,
    /// or an OpenLineage schema facet.
    #[arg(long, value_enum)]
    pub export: Option<ContractExportFormat>,
}

/// Arguments for `faucet masking`.
#[cfg(feature = "masking")]
#[derive(Debug, Parser)]
pub struct MaskingArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config with a
    /// `pipeline.masking:` block. If omitted, auto-discover
    /// `faucet.yaml` / `faucet.yml` / `faucet.json` in cwd.
    pub config: Option<PathBuf>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block and deep-merge
    /// it over the composed base. Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
}

/// Export format for `faucet contract --export`.
#[cfg(feature = "contract")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ContractExportFormat {
    /// The canonical contract document as JSON.
    Contract,
    /// A standalone JSON Schema (draft 2020-12) for the promised records.
    JsonSchema,
    /// An OpenLineage `SchemaDatasetFacet` JSON document.
    Openlineage,
}

/// `faucet schedule` arguments.
#[cfg(feature = "schedule")]
#[derive(Debug, Parser)]
pub struct ScheduleArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config with a `schedule:`
    /// block. If omitted, auto-discover `faucet.yaml` / `.yml` / `.json` in cwd.
    pub config: Option<PathBuf>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Run exactly one pipeline run immediately, then exit (ignores cron timing).
    /// Useful for platform-driven invocation (k8s CronJob / systemd OnCalendar).
    #[arg(long)]
    pub once: bool,
    /// Select a named overlay from the config's `profiles:` block and deep-merge
    /// it over the composed base. Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
}

/// `faucet serve` arguments.
#[cfg(feature = "serve")]
#[derive(Debug, Clone, Parser)]
pub struct ServeArgs {
    /// Bind address. Defaults to loopback; set 0.0.0.0:PORT to expose externally.
    #[arg(long, env = "FAUCET_SERVE_LISTEN", default_value = "127.0.0.1:8080")]
    pub listen: String,
    /// Bearer token required on /v1/* requests. Prefer the env var (avoids `ps` leakage).
    #[arg(long, env = "FAUCET_SERVE_AUTH_TOKEN", conflicts_with = "no_auth")]
    pub auth_token: Option<String>,
    /// Explicitly disable authentication. Required if no token is set, so an
    /// unauthenticated server is never accidental.
    #[arg(long)]
    pub no_auth: bool,
    /// Path to an RBAC auth config (YAML/JSON) defining principals — each a
    /// `{ name, token, role }` where role is `viewer` / `operator` / `admin`.
    /// Enables role-based access control + an audit log. Mutually exclusive with
    /// `--auth-token` / `--no-auth`.
    #[arg(long, conflicts_with_all = ["auth_token", "no_auth"])]
    pub auth_config: Option<std::path::PathBuf>,
    /// Max pipeline runs executing at once. Default: min(16, cpu count).
    #[arg(long)]
    pub max_concurrent_runs: Option<usize>,
    /// Max queued (not-yet-running) runs before POST /v1/runs returns 429.
    /// Default: 8 × max-concurrent-runs.
    #[arg(long)]
    pub max_queued_runs: Option<usize>,
    /// Workspace-default config merged under every submitted run.
    #[arg(long)]
    pub default_config: Option<std::path::PathBuf>,
    /// Run-history backend URL: omitted = in-memory; postgres://… ; sqlite:… .
    #[arg(long)]
    pub history: Option<String>,
    /// CORS allow-list origin (repeatable). Omitted = CORS disabled.
    #[arg(long)]
    pub cors_origin: Vec<String>,
    /// Max POST /v1/runs body size in bytes (413 on exceed).
    #[arg(long, default_value_t = 1_048_576)]
    pub body_limit_bytes: usize,
    /// SIGTERM/SIGINT drain window in seconds.
    #[arg(long, default_value_t = 60)]
    pub shutdown_grace_secs: u64,
    /// Retain terminal run records this long (seconds).
    #[arg(long, default_value_t = 604_800)]
    pub retain_terminal_runs_secs: u64,
    /// Idempotency-key replay window (seconds).
    #[arg(long, default_value_t = 86_400)]
    pub idempotency_retention_secs: u64,
    /// How long persisted run logs are kept (seconds, #529), independent of run
    /// records. Requires a persistent `--history` backend; `0` disables durable
    /// log persistence (ephemeral SSE only). Default: 7 days.
    #[arg(long, default_value_t = 604_800)]
    pub log_retention_secs: u64,
    /// Per-run cap on persisted log lines (#529). Past it a truncation marker is
    /// recorded and further lines are dropped.
    #[arg(long, default_value_t = 100_000)]
    pub log_max_lines_per_run: usize,
    /// How long the **local files** a run's sinks wrote (jsonl / csv / parquet)
    /// are kept before the retention GC reclaims them, in days (#587). `0`
    /// disables the automatic sweep — outputs are still tracked and can be
    /// cleaned on demand from the Datasets page or `faucet cleanup`. A pipeline's
    /// `local_outputs.retention_days` overrides this per pipeline. Default: 7
    /// days.
    ///
    /// The GC only ever deletes files faucet recorded as its own sink outputs —
    /// never a glob or a directory, and never a file it merely appended to.
    #[arg(
        long,
        env = "FAUCET_LOCAL_SINK_OUTPUT_RETENTION_DAYS",
        default_value_t = crate::local_outputs::DEFAULT_RETENTION_DAYS
    )]
    pub local_output_retention_days: u32,
    /// Never delete a local sink output touched within this many seconds (#587) —
    /// the guard against unlinking a file a run is still writing, including a run
    /// the ledger has not recorded yet. Raise it above the longest expected gap
    /// between a slow source's pages; `0` disables it. Default: 60.
    #[arg(
        long,
        env = "FAUCET_LOCAL_SINK_OUTPUT_IN_FLIGHT_GRACE_SECS",
        default_value_t = 60
    )]
    pub local_output_in_flight_grace_secs: u64,
    /// Serve **dataset previews** of the local files this server's sinks wrote
    /// — read the first N rows of a tracked jsonl / csv / parquet output back
    /// into the console's Datasets page (#586).
    ///
    /// Off by default, and deliberately so: it returns the *contents* of files
    /// on the server's disk over HTTP. It is a local-testing convenience, not
    /// something a normally-exposed `serve` should offer. Only paths the
    /// local-output ledger recorded as faucet's own sink outputs can ever be
    /// read — never a path from the request.
    #[arg(long, env = "FAUCET_SERVE_PREVIEW_LOCAL_OUTPUTS")]
    pub preview_local_outputs: bool,
    /// Rows a preview loads when the request omits `row_count_to_load` (#586) —
    /// the *soft* cap. `0` = the whole dataset by default. Bounded by
    /// `--preview-max-rows`.
    #[arg(
        long,
        env = "FAUCET_SERVE_PREVIEW_DEFAULT_ROWS",
        default_value_t = crate::serve::preview::DEFAULT_PREVIEW_ROWS
    )]
    pub preview_default_rows: usize,
    /// Ceiling on the rows one preview request may load (#586) — the *hard* cap.
    /// A larger `row_count_to_load` is clamped to it, never honoured.
    ///
    /// `0` lifts the ceiling, which is what makes `row_count_to_load=all`
    /// actually load an entire dataset. Do that only where reading a whole output
    /// file into one HTTP response is acceptable — the read is still bounded by a
    /// response-size budget and a deadline, and a dataset that exceeds either
    /// comes back as a partial answer that says so.
    #[arg(
        long,
        env = "FAUCET_SERVE_PREVIEW_MAX_ROWS",
        default_value_t = crate::serve::preview::MAX_PREVIEW_ROWS
    )]
    pub preview_max_rows: usize,
    /// Run-ownership lease TTL in seconds (multi-instance orphan fencing). A run
    /// is owned by the instance executing it and its lease is heartbeated at
    /// ~⅓ of this interval; only a run whose lease has expired (owner presumed
    /// dead) is recovered as failed. Make this comfortably larger than expected
    /// GC/IO stalls so a healthy-but-slow instance is never falsely reclaimed.
    /// Only relevant with a persistent (postgres/sqlite) history backend.
    #[arg(long, default_value_t = 30)]
    pub lease_ttl_secs: u64,
    /// Per-probe timeout for `doctor_first` preflight (seconds).
    #[arg(long, default_value_t = 10)]
    pub probe_timeout_secs: u64,
    /// Path to a `.env` file loaded for the server's own startup interpolation.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<std::path::PathBuf>,
    /// Skip auto-loading `.env` from cwd at startup.
    #[arg(long)]
    pub no_env_file: bool,
    /// Disable serving the embedded web console (only meaningful in a build that
    /// includes the `serve-ui` feature; the API is unaffected).
    #[arg(long)]
    pub no_ui: bool,
    /// Enable clustered execution: run a claim loop that pulls Pending runs from
    /// the shared history DB so N instances pull-balance and fail over. Requires
    /// a postgres/sqlite --history backend.
    #[arg(long)]
    pub cluster: bool,
    /// Claim-loop poll interval (seconds) in cluster mode. Also the
    /// cross-instance cancel-propagation lag. Must be > 0.
    #[arg(long, default_value_t = 2)]
    pub cluster_poll_secs: u64,
    /// Max failover re-runs of an orphaned run before it is marked Failed
    /// (poison). Must be > 0.
    #[arg(long, default_value_t = 3)]
    pub cluster_max_attempts: u32,
    /// Path to a triggers file (YAML/JSON) defining event-driven pipeline
    /// triggers (object-arrival / webhook / queue-depth). Requires a build with
    /// the `triggers` feature. See `faucet schema triggers`.
    #[arg(long)]
    pub triggers: Option<std::path::PathBuf>,
    /// Restrict per-run completion callbacks (`callback` on a submit) to these
    /// hosts. Repeatable. When unset, any host is permitted **except**
    /// link-local / cloud-metadata addresses, which are always refused unless
    /// named here. See the HTTP API reference for the egress posture.
    #[arg(long = "callback-allow-host")]
    pub callback_allow_host: Vec<String>,
    /// Mount the MCP (Model Context Protocol) endpoint at `/mcp`, exposing
    /// faucet as agent tool calls. Effective only in a build with the `mcp`
    /// feature; the endpoint inherits serve's bearer-auth + RBAC + audit.
    #[arg(long)]
    pub mcp: bool,
    /// Allow the MCP endpoint's *mutating* tools (`run_pipeline`). Off by
    /// default: only read-only tools are exposed. A caller still needs the
    /// `RunWrite` RBAC scope. Only meaningful together with `--mcp`.
    #[arg(long)]
    pub mcp_allow_mutations: bool,
}

/// `faucet mcp` arguments — run an MCP server over stdio (#420).
#[cfg(feature = "mcp")]
#[derive(Debug, Clone, Parser)]
pub struct McpArgs {
    /// Allow mutating tools (`run_pipeline`). Off by default — only read-only
    /// tools (list / schema / scaffold / validate / preview) are exposed.
    /// stdio is local-trust: there is no bearer/RBAC layer, so enable this only
    /// for a trusted local agent.
    #[arg(long)]
    pub allow_mutations: bool,
    /// Optional `.env` file to load before starting (for `${env:…}` in configs
    /// passed to `validate`/`preview`/`run_pipeline`).
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<std::path::PathBuf>,
    /// Skip auto-loading `.env` from cwd at startup.
    #[arg(long)]
    pub no_env_file: bool,
    /// Pipeline-template registry to expose (#444): `sqlite:<path>`, a
    /// `postgres://…` URL, or `memory`. Enables the `list_templates` /
    /// `get_template` tools (plus `register_template` / `run_template` with
    /// `--allow-mutations`). Omitted = no template tools are advertised.
    #[cfg(feature = "templates")]
    #[arg(long, env = "FAUCET_TEMPLATE_STORE")]
    pub template_store: Option<String>,
}

/// `faucet run` arguments.
///
/// `Default` is derived so callers that execute an already-loaded config through
/// `commands::run::execute` (notably `faucet template run`) can build a
/// plain-run argument set without restating every flag.
#[derive(Debug, Parser, Default)]
pub struct RunArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config.
    /// If omitted (and `--from-env` is not set), auto-discover
    /// `faucet.yaml` / `faucet.yml` / `faucet.json` in the current directory.
    /// Mutually exclusive with `--from-env`.
    #[arg(conflicts_with = "from_env")]
    pub config: Option<PathBuf>,
    /// Build the pipeline entirely from `FAUCET_*` environment variables —
    /// no YAML required. See `cli/README.md` for the variable schema.
    #[arg(long)]
    pub from_env: bool,
    /// Path to a `.env` file to load before reading variables. Works in both
    /// YAML mode (for `${env:VAR}` interpolation) and `--from-env` mode.
    /// When omitted, `.env` in the current directory is auto-loaded if present.
    /// Existing process-env values always win over file-supplied ones.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from the current directory.
    #[arg(long)]
    pub no_env_file: bool,
    /// Stop after fetching from the source — write nothing to the sink.
    #[arg(long)]
    pub dry_run: bool,
    /// Stop after writing this many records to the sink. Default: unlimited.
    #[arg(long)]
    pub limit: Option<usize>,
    /// Override the state-store directory (file backend only).
    #[arg(long)]
    pub state_path: Option<PathBuf>,
    /// Override the `${now.*}` interpolation clock (RFC3339 like
    /// `2026-01-31T00:00:00Z`, or a date `2026-01-31`). Default: process start (UTC).
    /// Use for backfills.
    #[arg(long)]
    pub clock: Option<String>,
    /// Select a named overlay from the config's `profiles:` block and deep-merge
    /// it over the composed base. Overrides the `FAUCET_PROFILE` env var.
    /// Not applicable in `--from-env` mode (no config file to compose).
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
    /// Show a live full-screen terminal UI (per-invocation throughput, errors,
    /// DLQ counts, bookmark age) while the pipeline runs. Requires a binary
    /// built with the `cli-tui` feature and a real terminal on stdout —
    /// on a non-TTY (CI, pipes) the run proceeds normally with a notice.
    /// Press `q` to cancel cooperatively (in-flight work flushes at the next
    /// page boundary).
    #[arg(long)]
    pub tui: bool,

    /// Suppress the inline live progress line (records in/out, rows/s, pages,
    /// elapsed) that `faucet run` shows on an interactive terminal. The
    /// progress line is already auto-disabled on a non-TTY stdout (CI, pipes)
    /// and when `--tui` is used; `--quiet` turns it off explicitly, keeping
    /// only the periodic log output.
    #[arg(long)]
    pub quiet: bool,

    /// Format for the end-of-run summary: `text` (default, human — written to
    /// **stderr** so stdout stays clean for the sink), `json` (a single
    /// machine-readable document on **stdout**), or `ndjson` (one JSON object
    /// per matrix row on **stdout**). With `json`/`ndjson`, stdout carries only
    /// the summary — logs stay on stderr — so `faucet run` is scriptable.
    #[arg(long, value_enum, default_value_t = RunOutput::Text)]
    pub output: RunOutput,

    /// Supply a value for a `params:` entry declared by the config (#444):
    /// `--param tenant_id=acme`. Repeatable. Values are coerced to the declared
    /// type, so `--param page=50` satisfies a `type: int` param. A param with a
    /// `default` needs no flag; a `required` one errors when unsupplied.
    #[arg(long = "param", value_name = "NAME=VALUE")]
    pub param: Vec<String>,

    /// Override an environment variable for this run's `${env:VAR}` resolution
    /// only (#444): `--param-env REGION=eu` sets it, bare `--param-env TOKEN`
    /// takes the value from the caller's environment. Repeatable. The process
    /// environment is not modified.
    #[arg(long = "param-env", value_name = "NAME[=VALUE]")]
    pub param_env: Vec<String>,

    /// Runtime matrix-row selection (`--select`/`--only`/`--skip`/`--status`/
    /// `--tag`/`--include-parents`).
    #[command(flatten)]
    pub selection: SelectionArgs,
}

/// Format for `faucet run`'s end-of-run summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum RunOutput {
    /// Human-readable one-line summary (default).
    #[default]
    Text,
    /// A single machine-readable JSON document with per-row + total stats.
    Json,
    /// One JSON object per matrix row (newline-delimited) for streaming consumers.
    Ndjson,
}

/// `faucet backfill` arguments.
#[derive(Debug, Parser)]
pub struct BackfillArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config. If omitted,
    /// auto-discover `faucet.yaml` / `faucet.yml` / `faucet.json` in cwd.
    pub config: Option<PathBuf>,
    /// Window start (inclusive): RFC3339 (`2026-06-01T00:00:00Z`) or a date
    /// (`2026-06-01`, midnight in --timezone). Requires --to.
    #[arg(long, requires = "to", conflicts_with = "from_bookmark")]
    pub from: Option<String>,
    /// Window end (exclusive): RFC3339 or a date.
    #[arg(long, requires = "from", conflicts_with = "from_bookmark")]
    pub to: Option<String>,
    /// Chunk the range into windows of this duration (`45s`, `30m`, `6h`,
    /// `1d`, `1w`) so each chunk is an independent, resumable unit. Defaults
    /// to the config's `backfill.window`; omitted = one unit for the whole
    /// range.
    #[arg(long)]
    pub window: Option<String>,
    /// Replay from this explicit bookmark value instead of a wall-clock
    /// range (seeded into the backfill's scoped state key; the source's own
    /// incremental logic reads forward from it). JSON or a bare string.
    #[arg(long)]
    pub from_bookmark: Option<String>,
    /// Upper bookmark bound: records whose --bookmark-field orders after
    /// this value are dropped before the sink.
    #[arg(long, requires_all = ["from_bookmark", "bookmark_field"])]
    pub to_bookmark: Option<String>,
    /// Record field the --to-bookmark bound applies to.
    #[arg(long)]
    pub bookmark_field: Option<String>,
    /// Max concurrently-running window units. Defaults to the config's
    /// `backfill.concurrency`, else 1 (sequential).
    #[arg(long)]
    pub concurrency: Option<usize>,
    /// IANA timezone for date boundaries and `${now.*}` rendering. Defaults
    /// to the config's `backfill.timezone`, else UTC.
    #[arg(long)]
    pub timezone: Option<String>,
    /// Root row of the config to backfill. Defaults to the only root.
    #[arg(long)]
    pub row: Option<String>,
    /// Redirect writes to this named sink template under `pipeline.sinks`
    /// (backfill into a staging table first).
    #[arg(long)]
    pub into: Option<String>,
    /// Print the planned units without running anything.
    #[arg(long)]
    pub dry_run: bool,
    /// Continue a previously-interrupted backfill of the same range: skip
    /// units already done, re-run failed and pending ones.
    #[arg(long, conflicts_with = "restart")]
    pub resume: bool,
    /// Discard a previous progress marker for this range and start over.
    #[arg(long)]
    pub restart: bool,
    /// Emit a machine-readable JSON report instead of the human summary.
    #[arg(long)]
    pub json: bool,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block.
    /// Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
}

/// `faucet replicate` arguments.
#[derive(Debug, Parser)]
pub struct ReplicateArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config with a
    /// `replication:` block. If omitted, auto-discover
    /// `faucet.yaml` / `.yml` / `.json` in cwd.
    pub config: Option<PathBuf>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block and deep-merge
    /// it over the composed base. Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
}

/// `faucet discover` arguments.
#[derive(Debug, Parser)]
pub struct DiscoverArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config whose source
    /// points at the system to introspect. If omitted, auto-discover
    /// `faucet.yaml` / `faucet.yml` / `faucet.json` in cwd.
    pub config: Option<PathBuf>,
    /// Which source template to introspect (an entry under `pipeline.sources`).
    /// Defaults to `default` (the legacy singular `pipeline.source`).
    #[arg(long)]
    pub source: Option<String>,
    /// Sink template each generated matrix row should target (an entry under
    /// `pipeline.sinks`). Needed when the config selects a named sink (e.g. the
    /// Salesforce template's `${param.sink}`); omit for a single default sink.
    #[arg(long)]
    pub sink: Option<String>,
    /// Only include datasets whose name matches this `*`-wildcard pattern
    /// (repeatable; no patterns = include everything).
    #[arg(long)]
    pub include: Vec<String>,
    /// Exclude datasets whose name matches this `*`-wildcard pattern
    /// (repeatable; applied after --include).
    #[arg(long)]
    pub exclude: Vec<String>,
    /// Write the generated config to this file instead of stdout.
    #[arg(long, short = 'o')]
    pub output: Option<PathBuf>,
    /// Overwrite the --output file if it already exists.
    #[arg(long)]
    pub force: bool,
    /// Emit the discovered datasets as machine-readable JSON instead of a
    /// generated config.
    #[arg(long)]
    pub json: bool,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block.
    /// Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
}

/// `faucet validate` arguments.
#[derive(Debug, Parser)]
pub struct ValidateArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config. If omitted,
    /// auto-discover `faucet.yaml` / `faucet.yml` / `faucet.json` in cwd.
    pub config: Option<PathBuf>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Validate grammar and structure only — skip fetching from secrets
    /// managers (no network / credentials needed).
    #[arg(long)]
    pub no_secrets: bool,
    /// Select a named overlay from the config's `profiles:` block and deep-merge
    /// it over the composed base. Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
    /// Print the fully-composed config (after extends/!include/profile, before
    /// `${...}` interpolation) and exit. For debugging composition precedence.
    /// `--no-secrets` is redundant here (no interpolation or secret fetch occurs).
    #[arg(long)]
    pub show_composed: bool,

    /// Supply a value for a declared `params:` entry (#444), e.g.
    /// `--param tenant_id=acme`. Repeatable. Without any `--param`, a `required`
    /// param is validated against a type-shaped placeholder — so a
    /// parameterized config validates in CI without inventing real values.
    /// Passing at least one `--param` switches to strict binding, checking that
    /// every required param is supplied and every value has the declared type.
    #[arg(long = "param", value_name = "NAME=VALUE")]
    pub param: Vec<String>,

    /// Override an environment variable for this validation only:
    /// `--param-env REGION=eu`, or bare `--param-env TOKEN` to take it from the
    /// caller's environment. Repeatable.
    #[arg(long = "param-env", value_name = "NAME[=VALUE]")]
    pub param_env: Vec<String>,

    /// Runtime matrix-row selection — `validate` reports each row's resolved
    /// status/tags and whether the selection would run or skip it.
    #[command(flatten)]
    pub selection: SelectionArgs,

    /// Emit a structured JSON validation summary instead of the prose report,
    /// so CI can assert on it programmatically. Suppresses the human lines.
    #[arg(long)]
    pub json: bool,
}

/// `faucet schema` arguments.
#[derive(Debug, Parser)]
pub struct SchemaArgs {
    #[command(subcommand)]
    pub target: Option<SchemaTarget>,
    /// List every valid schema target and exit, instead of printing a schema.
    #[arg(long)]
    pub list: bool,
}

/// Schema subcommand target — which connector or system component to describe.
#[derive(Debug, Subcommand)]
pub enum SchemaTarget {
    /// Composed JSON Schema for the **entire** `faucet.yaml` / `faucet.json`
    /// config document (top-level grammar + per-connector `type` discrimination).
    /// Point an editor at it with a `# yaml-language-server: $schema=…` header.
    Config,
    /// JSON Schema for a source connector config.
    Source {
        /// Connector name (e.g. `rest`, `graphql`, `postgres`).
        #[arg(add = ArgValueCandidates::new(completions::source_kind_candidates))]
        name: String,
    },
    /// JSON Schema for a sink connector config.
    Sink {
        /// Connector name (e.g. `jsonl`, `bigquery`, `postgres`).
        #[arg(add = ArgValueCandidates::new(completions::sink_kind_candidates))]
        name: String,
    },
    /// JSON Schema for a transform's inline config.
    Transform {
        /// Transform name (e.g. `flatten`, `keys_case`, `cast`).
        /// Run `faucet list` to see what is compiled in.
        #[arg(add = ArgValueCandidates::new(completions::transform_candidates))]
        name: String,
    },
    /// JSON Schema for the DLQ (Dead Letter Queue) specification.
    Dlq,
    /// JSON Schema for the `replication:` (snapshot→CDC) block.
    Replication,
    /// JSON Schema for the `backfill:` (window replay defaults) block.
    Backfill,
    /// JSON Schema for the `partition:` (range partitioning) block.
    Partition,
    /// JSON Schema for the top-level `execution:` block.
    Execution,
    /// JSON Schema for the top-level `resilience:` block.
    Resilience,
    /// JSON Schema for the top-level `sla:` (freshness/volume SLA) block.
    Sla,
    /// JSON Schema for the `quality:` block.
    #[cfg(feature = "quality")]
    Quality,
    /// JSON Schema for the `contract:` block.
    #[cfg(feature = "contract")]
    Contract,
    /// JSON Schema for the `masking:` (PII masking) block.
    #[cfg(feature = "masking")]
    Masking,
    /// JSON Schema for the `faucet test` spec file.
    Test,
    /// Grammar reference for secrets-manager interpolation directives.
    Secrets,
    /// JSON Schema for the `schedule:` block.
    #[cfg(feature = "schedule")]
    Schedule,
    /// JSON Schema for the `lineage:` (OpenLineage) block.
    #[cfg(feature = "lineage")]
    Lineage,
    /// JSON Schema for the `--triggers` file (event-driven pipeline triggers).
    #[cfg(feature = "triggers")]
    Triggers,
    /// JSON Schema for the `notifications:` (incident-routing) block.
    #[cfg(feature = "notify")]
    Notifications,
    /// JSON Schema for the `catalog:` (Data Movement Catalog store) block.
    #[cfg(feature = "catalog")]
    Catalog,
    /// JSON Schema for the `local_outputs:` (local sink output retention) block.
    #[cfg(feature = "catalog")]
    LocalOutputs,
    /// JSON Schema for one entry of the `params:` (typed run parameters) block.
    /// A config's `params:` maps names to entries of this shape; values are
    /// supplied per run via `--param` or a template trigger.
    Params,
}

/// `faucet preview` arguments.
#[derive(Debug, Parser)]
pub struct PreviewArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config. If omitted,
    /// auto-discover `faucet.yaml` / `faucet.yml` / `faucet.json` in cwd.
    pub config: Option<PathBuf>,
    /// Stop after this many records. Default: 10.
    #[arg(long, default_value_t = 10)]
    pub limit: usize,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block and deep-merge
    /// it over the composed base. Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,

    /// Runtime matrix-row selection — `preview` previews the first root row of
    /// the selected run set.
    #[command(flatten)]
    pub selection: SelectionArgs,
}

/// `faucet init` arguments.
#[derive(Debug, Parser)]
pub struct InitArgs {
    /// Name written into the generated file's `name:` field. Defaults to
    /// `my-pipeline` when omitted.
    pub name: Option<String>,
    /// Source connector kind to scaffold (e.g. `rest`, `postgres`, `s3`).
    /// Defaults to `rest`. Run `faucet list` to see what is compiled in.
    #[arg(long)]
    pub source: Option<String>,
    /// Sink connector kind to scaffold (e.g. `jsonl`, `bigquery`).
    /// Defaults to `jsonl`. Run `faucet list` to see what is compiled in.
    #[arg(long)]
    pub sink: Option<String>,
    /// Output file path. Defaults to `pipeline.yaml`.
    #[arg(long, short = 'o', default_value = "pipeline.yaml")]
    pub output: PathBuf,
    /// Overwrite the output file if it already exists.
    #[arg(long)]
    pub force: bool,
    /// Prompt for the source and sink kinds interactively instead of using
    /// `--source` / `--sink`. Requires the `cli-interactive` build feature
    /// and a TTY on stdin; falls back to the arg-driven path otherwise.
    #[arg(long)]
    pub interactive: bool,
    /// Name of the template under which to register the scaffolded source
    /// and sink. The generated config uses `pipeline.sources.<TEMPLATE>` and
    /// `pipeline.sinks.<TEMPLATE>`. Defaults to `default` so a matrix row
    /// without a `ref:` field still resolves through the new schema.
    #[arg(long, default_value = "default")]
    pub template: String,
    /// (singer only) Run `<executable> --discover` to fetch the tap's catalog,
    /// write it next to the output, and scaffold the config with the discovered
    /// streams listed. Requires `--source singer` and `--executable`.
    #[arg(long)]
    pub discover: bool,
    /// (singer only) The Singer tap executable to discover with (used by
    /// `--discover`), e.g. `tap-github` or `/opt/taps/tap-csv`.
    #[arg(long)]
    pub executable: Option<String>,
    /// (singer only) The target stream to emit. When given with `--discover`,
    /// the written catalog marks this stream — and any inferable parent
    /// streams (e.g. a parent-keyed tap's parent) — `selected`, and the
    /// scaffolded config's `stream:` is set to it. Most DB / SDK taps sync
    /// nothing unless a stream is selected in the catalog.
    #[arg(long)]
    pub stream: Option<String>,
}

/// `faucet plan` arguments.
#[derive(Debug, Parser)]
pub struct PlanArgs {
    /// Path to a `.yaml`/`.yml`/`.json` config (auto-discovered if omitted).
    pub config: Option<PathBuf>,
    /// Which row to plan (default: the first root row).
    #[arg(long)]
    pub row: Option<String>,
    /// Offline sample of input records (`.jsonl` or a `.json` array) to preview
    /// the output schema, volume, and sink delta through — no source is touched.
    #[arg(long)]
    pub sample: Option<PathBuf>,
    /// Pull a capped, read-only sample from the real source instead of a
    /// fixture (bounded by `--limit`; no bookmark is advanced).
    #[arg(long)]
    pub live: bool,
    /// Cap for `--live` sampling.
    #[arg(long, default_value_t = 10)]
    pub limit: usize,
    /// Emit the plan as JSON.
    #[arg(long)]
    pub json: bool,
    /// Show a `terraform plan`-style diff of the current config against the last
    /// recorded run, instead of the resolved-pipeline preview (#374). Requires a
    /// `catalog:` block. Resolves secrets so the diff matches what `run` records.
    #[arg(long)]
    pub diff: bool,
    /// Resolve secrets-manager directives (needs network/credentials). Off by
    /// default so `plan` works offline like `faucet test`. Implied by `--diff`.
    #[arg(long)]
    pub resolve_secrets: bool,
    /// Select a `profiles:` overlay.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
}

/// `faucet dev` arguments.
#[derive(Debug, Parser)]
pub struct DevArgs {
    /// Path to the `.yaml`/`.yml`/`.json` config to watch.
    pub config: PathBuf,
    /// Which row to run (default: the first root row).
    #[arg(long)]
    pub row: Option<String>,
    /// Offline sample of input records (`.jsonl` or `.json` array). Required
    /// for the offline loop.
    #[arg(long)]
    pub sample: Option<PathBuf>,
    /// (reserved) pull a capped read-only sample from the real source.
    #[arg(long)]
    pub live: bool,
    /// Cap for `--live` sampling.
    #[arg(long, default_value_t = 10)]
    pub limit: usize,
    /// Run once and exit instead of watching (also the non-TTY fallback).
    #[arg(long)]
    pub once: bool,
    /// Debounce window between re-runs, in milliseconds.
    #[arg(long, default_value_t = 300)]
    pub debounce_ms: u64,
    /// Select a `profiles:` overlay.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
}

/// `faucet list` arguments.
#[derive(Debug, Parser)]
pub struct ListArgs {
    /// List every connector in the registry index (not just the compiled-in
    /// ones), marking which are already in this binary.
    #[arg(long)]
    pub available: bool,
    /// Read a custom registry index instead of the built-in one.
    #[arg(long)]
    pub index: Option<PathBuf>,
    /// Emit the listing as JSON instead of the human-readable columns.
    #[arg(long)]
    pub json: bool,
}

/// `faucet conformance` arguments.
#[derive(Debug, Parser)]
pub struct ConformanceArgs {
    /// Only score the connector with this system name (e.g. `postgres`); prints
    /// a detailed scorecard. Omit to score every compiled-in connector.
    pub name: Option<String>,
    /// Restrict to `source` or `sink`.
    #[arg(long)]
    pub kind: Option<String>,
    /// Score every compiled-in connector (the default when no NAME is given;
    /// accepted explicitly for clarity in CI).
    #[arg(long)]
    pub all: bool,
    /// Emit the full scorecards as JSON.
    #[arg(long)]
    pub json: bool,
    /// Fail (exit non-zero) if any scored connector is below this maturity tier
    /// — an opt-in CI gate. One of `stable` / `experimental` / `beta` / `draft`.
    #[arg(long, value_name = "TIER")]
    pub min_tier: Option<String>,
    /// Print the connector capability matrix (Markdown) derived from the
    /// registry allowlists and exit — the generated source for the docs-site
    /// capability matrix. Ignores the scoring flags.
    #[arg(long)]
    pub matrix: bool,
}

/// `faucet search` arguments.
#[derive(Debug, Parser)]
pub struct SearchArgs {
    /// Term to match against connector name / description / keywords / crate.
    pub term: String,
    /// Read a custom registry index instead of the built-in one.
    #[arg(long)]
    pub index: Option<PathBuf>,
    /// Emit matches as JSON.
    #[arg(long)]
    pub json: bool,
}

/// `faucet install` arguments.
#[derive(Debug, Parser)]
pub struct InstallArgs {
    /// Connector system name (e.g. `kafka`).
    pub name: String,
    /// Disambiguate when a name exists as both a source and a sink.
    #[arg(long)]
    pub kind: Option<String>,
    /// Read a custom registry index instead of the built-in one.
    #[arg(long)]
    pub index: Option<PathBuf>,
}

/// `faucet new` arguments.
#[derive(Debug, Parser)]
pub struct NewArgs {
    #[command(subcommand)]
    pub target: NewTarget,
}

/// What `faucet new` scaffolds.
#[derive(Debug, Subcommand)]
pub enum NewTarget {
    /// Scaffold a ready-to-build `faucet-source-<name>` / `faucet-sink-<name>`
    /// connector crate following every repo convention.
    Connector(NewConnectorArgs),
}

/// `faucet new connector` arguments.
#[derive(Debug, Parser)]
pub struct NewConnectorArgs {
    /// Connector system name (lowercase, e.g. `acme` or `acme-widgets`). Becomes
    /// the crate name `faucet-<kind>-<name>` and the YAML `type:` value.
    pub name: String,
    /// Whether to scaffold a `source` or a `sink`.
    #[arg(long)]
    pub kind: String,
    /// Also scaffold a `faucet-common-<name>` crate for config shared between a
    /// source/sink pair.
    #[arg(long)]
    pub common: bool,
    /// Directory to write the new crate(s) into. Defaults to the current dir.
    #[arg(long, short = 'o', default_value = ".")]
    pub output: PathBuf,
    /// Overwrite any existing files.
    #[arg(long)]
    pub force: bool,
}
