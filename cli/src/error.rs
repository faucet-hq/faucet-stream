//! CLI-level error type. Wraps every failure mode the binary surfaces so
//! `main()` can render a single, user-readable line per failure.

use std::path::PathBuf;
use thiserror::Error;

/// Convenience alias used by every CLI module.
pub type CliResult<T> = Result<T, CliError>;

/// Top-level error variants for the `faucet` binary.
#[derive(Debug, Error)]
pub enum CliError {
    /// Failed to read a config file from disk.
    #[error("failed to read config file '{path}': {source}")]
    ReadConfig {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// The config file extension is neither `.yaml`/`.yml` nor `.json`.
    #[error(
        "unsupported config extension for '{path}' — use .yaml, .yml, or .json (mixed JSON/YAML in a single file is not allowed)"
    )]
    UnknownExtension { path: PathBuf },

    /// Failed to parse the raw config text after interpolation.
    #[error("failed to parse config '{path}': {message}")]
    ParseConfig { path: PathBuf, message: String },

    /// An `${env:VAR}` reference could not be resolved.
    #[error("missing environment variable '{var}' referenced in config at '{location}'")]
    MissingEnvVar { var: String, location: String },

    /// A `${file:PATH}` reference could not be read.
    #[error("failed to read interpolated file '{}' referenced in config: {source}", path.display())]
    ReadInterpolatedFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error(
        "interpolated file '{}' exceeds the {max_bytes}-byte limit for `${{file:...}}` — \
         this directive is for small token/secret files, not bulk data",
        path.display()
    )]
    InterpolatedFileTooLarge { path: PathBuf, max_bytes: u64 },

    /// A `${row_id.path}` token referenced an unknown matrix row id at
    /// expand-time (or a typo'd load-time prefix that survived to record-time).
    #[error(
        "interpolation '{token}' references unknown id '{id}' (must be a matrix row id, or one of env/file/secret)"
    )]
    UnknownInterpolationId { id: String, token: String },

    /// A `${row_id.path}` resolved at record-time, but the dotted path doesn't
    /// match any field in the parent record.
    #[error("matrix row '{id}' has no field at path '{path}' in this parent record")]
    MissingRecordField { id: String, path: String },

    /// The named connector is unknown (or its feature flag is disabled in this build).
    #[error("unknown {kind} '{name}'. Available: {available}")]
    UnknownConnector {
        kind: &'static str,
        name: String,
        available: String,
    },

    /// The state-store type referenced in the config is unknown or not compiled in.
    #[error("unknown state store '{name}'. Available: {available}")]
    UnknownStateStore { name: String, available: String },

    /// A transform type referenced in the config is not recognised.
    #[error("unknown transform '{name}'. Available: {available}")]
    UnknownTransform { name: String, available: String },

    /// The transform config block could not be deserialized into the expected shape.
    #[error("invalid transform '{name}': {message}")]
    InvalidTransform { name: String, message: String },

    /// A connector config object failed to deserialize.
    #[error("invalid config for {kind} '{name}': {message}")]
    InvalidConnectorConfig {
        kind: &'static str,
        name: String,
        message: String,
    },

    /// A scaffold target already exists.
    #[error("refusing to overwrite existing file '{path}' — pass --force to overwrite")]
    ScaffoldExists { path: PathBuf },

    /// The CLI was invoked with `--from-env` but the required selector env var
    /// (`FAUCET_SOURCE` or `FAUCET_SINK`) is unset.
    #[error(
        "missing required environment variable '{var}' — set it before invoking `faucet run --from-env`"
    )]
    MissingEnvSelector { var: String },

    /// An explicit `--env-file` path does not exist on disk.
    #[error("--env-file path '{}' does not exist", path.display())]
    EnvFileNotFound { path: PathBuf },

    /// `faucet run` invoked with neither a config path nor `--from-env`, and
    /// auto-discovery found no `faucet.{yaml,yml,json}` in cwd.
    #[error(
        "no pipeline config: pass a path, --from-env, or create faucet.yaml (or .yml/.json) in the current directory"
    )]
    NoConfigOrFromEnv,

    /// Both a scalar env var and its `_JSON` counterpart were set for the same field.
    #[error(
        "conflicting environment variables for field '{field}': both '{scalar_var}' and '{json_var}' are set — pick one"
    )]
    EnvConflict {
        field: String,
        scalar_var: String,
        json_var: String,
    },

    /// A `*_JSON` env var did not parse as JSON.
    #[error("environment variable '{var}' is not valid JSON: {message}")]
    InvalidEnvJson { var: String, message: String },

    /// `FAUCET_TRANSFORM_<N>` indices are not contiguous starting at 1.
    #[error(
        "transform env vars must be contiguous starting at FAUCET_TRANSFORM_1; index {missing} is missing"
    )]
    TransformIndexGap { missing: u32 },

    /// A matrix row id collides with a load-time interpolation prefix.
    #[error("matrix row id '{id}' is reserved (env, file, secret, matrix, pipeline)")]
    ReservedRowId { id: String },

    /// Two matrix rows declared the same id.
    #[error("duplicate matrix row id '{id}'")]
    DuplicateRowId { id: String },

    /// A row's `parent:` field names a row that doesn't exist.
    #[error("matrix row '{id}' references unknown parent '{parent}'")]
    UnknownParent { id: String, parent: String },

    /// The parent chain contains a cycle.
    #[error("matrix has a parent cycle through: {}", ids.join(" -> "))]
    ParentCycle { ids: Vec<String> },

    /// A row's `depends_on:` list names a row that doesn't exist.
    #[error("matrix row '{id}' depends on unknown row '{depends_on}'")]
    UnknownDependency { id: String, depends_on: String },

    /// The combined `parent:` + `depends_on:` graph contains a cycle.
    #[error("matrix has a dependency cycle involving: {}", ids.join(", "))]
    DependencyCycle { ids: Vec<String> },

    /// Two parent records of the same matrix row resolved to the same
    /// `parent_key` value, producing a colliding state-key suffix.
    #[error(
        "duplicate state key '{state_key}' for matrix row '{id}': two parent records resolve to the same `parent_key` value — choose a `parent_key` that is unique per record"
    )]
    DuplicateStateKey { id: String, state_key: String },

    /// The state key derived from the pipeline name + row id (+ resolved
    /// parent-key value) is not a valid state-store key. Caught up front at
    /// unit construction rather than mid-run.
    #[error("invalid state key '{state_key}' for row '{id}': {reason}")]
    InvalidStateKey {
        id: String,
        state_key: String,
        reason: String,
    },

    /// One or more matrix invocations failed under `on_error: continue`.
    #[error("{count} pipeline invocation(s) failed (see logs above for details)")]
    PipelineHadFailures { count: usize },

    /// Both `pipeline.nodes` (topology mode) and `matrix:` are non-empty.
    /// They are mutually exclusive: topology mode replaces the matrix.
    #[error(
        "`pipeline.nodes` (topology mode) and `matrix:` are mutually exclusive — set one or the other, not both"
    )]
    MatrixAndNodesBothPresent,

    /// A topology edge references a node id that doesn't exist in `nodes:`.
    #[error("topology edge references unknown node '{name}' (known nodes: {})", known.join(", "))]
    EdgeEndpointMissing { name: String, known: Vec<String> },

    /// A topology graph-structure violation (arity, fan-out, join edges,
    /// cycle, reachability) reported by the core validator.
    #[error("invalid topology: {message}")]
    InvalidTopology { message: String },

    /// One or more topology sink nodes failed under `on_error: continue`.
    #[error("{count} topology node(s) failed (see logs above for details)")]
    TopologyHadFailures { count: usize },

    /// DLQ sink kind is not registered (not compiled in or feature disabled).
    #[error("DLQ sink kind `{kind}` is not registered (in {context})")]
    UnknownDlqSinkKind { kind: String, context: String },

    /// DLQ budget field is set to zero (which is invalid; omit to mean 'unlimited').
    #[error("DLQ {field} must be > 0 (got 0); omit the field to mean 'unlimited'")]
    InvalidDlqBudget { field: &'static str },

    /// A matrix row referenced a named template that doesn't exist in
    /// `pipeline.sources` / `pipeline.sinks` (or the legacy `default`).
    #[error(
        "matrix row '{row_id}' references unknown {kind} template '{name}'. Known {kind} templates: {known}",
        known = if known.is_empty() { String::from("(none defined)") } else { known.join(", ") }
    )]
    UnknownTemplate {
        kind: &'static str,
        name: String,
        row_id: String,
        known: Vec<String>,
    },

    /// A matrix row supplied no `ref:` and the legacy `default` template
    /// doesn't exist either.
    #[error(
        "matrix row '{row_id}' has no {kind}: either set `{kind}: {{ ref: <name> }}` pointing at a `pipeline.{kind}s` template, or declare a legacy `pipeline.{kind}` block"
    )]
    MissingTemplate { kind: &'static str, row_id: String },

    /// Both the legacy `pipeline.source` and `pipeline.sources.default` were
    /// declared (same for sinks). The `default` slot can only be defined once.
    #[error(
        "{kind} template '{name}' is defined twice — declare it either via the singular `pipeline.{kind}` block or in `pipeline.{kind}s`, not both"
    )]
    DuplicateTemplate { kind: &'static str, name: String },

    /// A sink template carries a `transforms:` field, which only sources support.
    #[error(
        "sink template '{name}' has `transforms:` — sinks cannot carry transforms; \
         declare transforms on the source template, pipeline, or matrix row instead"
    )]
    TransformsOnSink { name: String },

    /// A sink template carries `inherit_transforms:`, which only sources support.
    #[error(
        "sink template '{name}' has `inherit_transforms:` — sinks cannot carry the \
         transform-inheritance flag; remove it"
    )]
    InheritTransformsOnSink { name: String },

    /// A cycle was detected resolving `${vars.X}` / `${sources.X.PATH}` /
    /// `${sinks.X.PATH}` references at load time.
    #[error("interpolation cycle: {}", chain.join(" -> "))]
    InterpolationCycle { chain: Vec<String> },

    /// A config-composition include/extends chain contains a cycle.
    #[error("config composition cycle: {}", chain.join(" -> "))]
    CompositionCycle { chain: Vec<String> },

    /// An `extends`/`!include` target file does not exist.
    #[error(
        "config composition: file '{}' referenced by '{}' not found",
        path.display(),
        referenced_by.display()
    )]
    IncludeNotFound {
        path: PathBuf,
        referenced_by: PathBuf,
    },

    /// Composition nesting exceeded the safety cap (extends/!include loop guard).
    #[error(
        "config composition nested deeper than {max} levels — check for an extends/!include loop"
    )]
    CompositionDepthExceeded { max: usize },

    /// An `!include` tag had a non-string payload, an unsupported tag, or its
    /// target failed structural checks.
    #[error("invalid `!include` in '{}': {reason}", path.display())]
    BadInclude { path: PathBuf, reason: String },

    /// `--profile NAME` (or FAUCET_PROFILE) named a profile not declared under `profiles:`.
    #[error(
        "unknown profile '{name}'. Declared profiles: {}",
        if known.is_empty() { String::from("(none — no `profiles:` block)") } else { known.join(", ") }
    )]
    UnknownProfile { name: String, known: Vec<String> },

    /// A `${vars.X}` token referenced an undefined var.
    #[error(
        "interpolation '{token}' references unknown var '{name}' (define it under top-level `vars:`)"
    )]
    UnknownVarsRef { name: String, token: String },

    /// A `${sources.X.PATH}` or `${sinks.X.PATH}` token referenced an
    /// undefined template, or a dotted path that doesn't resolve inside it.
    #[error("interpolation '{token}' could not be resolved: {reason}")]
    UnknownTemplateRef { token: String, reason: String },

    /// A `params:` entry declared `required: true` and the caller supplied no
    /// value (#444).
    #[error(
        "missing required param '{name}'{} — supply it with `--param {name}=<value>` (or a \
         `\"params\"` entry over HTTP)",
        match description { Some(d) => format!(" ({d})"), None => String::new() }
    )]
    MissingParam {
        name: String,
        description: Option<String>,
    },

    /// A supplied param is not declared in the config's `params:` block (#444).
    /// Rejected rather than ignored so a typo can never silently no-op.
    #[error(
        "unknown param '{name}'. Declared params: {}",
        if known.is_empty() { String::from("(none — this config has no `params:` block)") } else { known.join(", ") }
    )]
    UnknownParam { name: String, known: Vec<String> },

    /// A `${param.X}` token referenced a param the config does not declare (#444).
    #[error(
        "interpolation '{token}' references undeclared param '{name}' (declare it under top-level \
         `params:`)"
    )]
    UnknownParamRef { name: String, token: String },

    /// A pipeline-template id / version was not found in the registry (#444).
    /// Distinct from [`CliError::UnknownTemplate`], which is about a
    /// `pipeline.sources` / `pipeline.sinks` connector template.
    #[error(
        "no pipeline template '{id}'{} in the registry — list them with `faucet template list`",
        match version { Some(v) => format!(" at version {v}"), None => String::new() }
    )]
    UnknownPipelineTemplate { id: String, version: Option<u32> },

    /// A connector's `auth: { ref }` named a provider not declared in the
    /// top-level `auth:` catalog.
    #[error(
        "auth references unknown provider '{name}'. Declared providers: {}",
        if known.is_empty() { String::from("(none)") } else { known.join(", ") }
    )]
    UnknownAuthProvider { name: String, known: Vec<String> },

    /// A top-level `auth:` provider spec failed to build.
    #[error("failed to build auth provider '{name}': {message}")]
    AuthProviderBuild { name: String, message: String },

    /// A `--select`/`--only`/`--skip` token matched no matrix row id (#370).
    /// Guards against typos silently producing a partial or empty run.
    #[error(
        "{flag} '{token}' matched no matrix row. Available rows: {}",
        if available.is_empty() { String::from("(none)") } else { available.join(", ") }
    )]
    NoMatchForSelector {
        flag: &'static str,
        token: String,
        available: Vec<String>,
    },

    /// A `--status <tier>` value is not one of the readiness-ladder tiers (#371).
    #[error("unknown status '{value}'. Valid tiers: {}", available.join(", "))]
    UnknownStatus {
        value: String,
        available: Vec<String>,
    },

    /// A `--tag <t>` value matches no row's tags (#376). Typo protection.
    #[error(
        "unknown tag '{tag}'. Tags present in this config: {}",
        if available.is_empty() { String::from("(none — no row declares tags)") } else { available.join(", ") }
    )]
    UnknownTag { tag: String, available: Vec<String> },

    /// A `--include-parents <policy>` value is not `off`/`eligible`/`all` (#377).
    #[error("unknown include_parents policy '{value}' (expected off, eligible, or all)")]
    UnknownIncludeParents { value: String },

    /// Matrix-only selectors were passed for a config with no `matrix:`
    /// (single anonymous invocation) — nothing to select among (#370/#376).
    #[error(
        "selector(s) {flags} require a `matrix:` — this config has a single anonymous invocation (nothing to select)"
    )]
    SelectorsWithoutMatrix { flags: String },

    /// The resolved run set is empty after status gating / tag narrowing / skip
    /// (#371). Not a silent no-op — names each row's status and how to include.
    #[error(
        "no matrix rows selected to run. Rows and their status: {}. \
         Widen the run set with --status <tier>, --select <id>, or --tag <t>",
        rows.join(", ")
    )]
    EmptyRunSet { rows: Vec<String> },

    /// A run-set row structurally depends on an ancestor that is not in the run
    /// set, under the active `include_parents` policy (#377). Lists every
    /// offending `dependent → ancestor (edge)` pair.
    #[error(
        "run-set dependency violation (include_parents={policy}): {}. \
         Select the ancestor by id (--select <id>), or loosen the policy \
         (--include-parents eligible|all)",
        pairs.join("; ")
    )]
    RunSetMissingAncestors {
        pairs: Vec<String>,
        policy: &'static str,
    },

    /// A config-level validation failure that isn't covered by a more specific
    /// variant (e.g. an invalid `quality:` block, or a quality check that
    /// requires a DLQ when none is configured).
    #[error("config error: {0}")]
    Config(String),

    /// Pass-through for failures bubbling up from `faucet-core` or a connector.
    #[error(transparent)]
    Faucet(#[from] faucet_core::FaucetError),

    /// Pass-through I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Observability stack (Prometheus / tracing) failed to install.
    #[error("observability install failed: {0}")]
    Observability(String),

    /// An internal invariant was violated (a bug). Surfaced instead of
    /// silently producing a partial result.
    #[error("internal error: {0}")]
    Internal(String),

    /// A secret-manager directive used a scheme whose backend feature was not
    /// compiled into this binary.
    #[error(
        "secret directive uses scheme '{scheme}' but this binary was built without \
         the `secrets-{scheme}` feature — rebuild with `--features secrets-{scheme}` (or `secrets`)"
    )]
    SecretBackendDisabled { scheme: String },

    /// The secrets manager has no secret at the given reference.
    #[error("secret '{reference}' not found in {scheme}")]
    SecretNotFound { scheme: String, reference: String },

    /// The secret fetch failed (network / API error).
    #[error("failed to fetch secret '{reference}' from {scheme}: {source}")]
    SecretFetchFailed {
        scheme: String,
        reference: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// No ambient credentials were available for the backend.
    #[error("could not authenticate to {scheme}: {hint}")]
    SecretAuthFailed { scheme: String, hint: String },

    /// A `#field` selector was used on a secret that is not JSON.
    #[error("secret '{reference}' from {scheme} is not JSON, but a '#field' selector was used")]
    SecretNotJson { scheme: String, reference: String },

    /// A `#field` selector named a key absent from the secret JSON.
    #[error(
        "secret '{reference}' from {scheme} has no field '{field}' (available: {})",
        if available.is_empty() { String::from("(none — secret is an empty object)") } else { available.join(", ") }
    )]
    SecretFieldMissing {
        scheme: String,
        reference: String,
        field: String,
        available: Vec<String>,
    },

    /// A secret directive was found while loading via the synchronous path.
    #[error(
        "config references a secrets manager (${{vault:…}} / ${{aws-sm:…}} / …) which requires \
         the async load path — load via `faucet run`/`validate`/`preview` rather than the sync API"
    )]
    SecretsRequireAsyncLoad,

    /// One or more `faucet doctor` preflight probes failed. The checklist is
    /// printed by the command; `main` maps this to an exit code equal to the
    /// failed-probe count (clamped to 255).
    #[error("{failed} preflight probe(s) failed")]
    DoctorFailed { failed: usize },

    /// One or more `faucet test` cases failed. The report is printed by the
    /// command; `main` maps this to an exit code equal to the failed-case
    /// count (clamped to 255).
    #[error("{failed} test case(s) failed")]
    TestsFailed { failed: usize },

    /// One or more `faucet backfill` units failed. The per-unit report is
    /// printed by the command (progress is already durably recorded, so
    /// `--resume` retries only the failures); `main` maps this to an exit
    /// code equal to the failed-unit count (clamped to 255).
    #[error("{failed} backfill unit(s) failed")]
    BackfillFailed { failed: usize },

    /// `faucet verify` found differing keys between the source and the
    /// destination (#701). The report is printed by the command; `main` maps
    /// this to an exit code equal to the differing-key count (clamped to 255).
    #[error("{differences} differing key(s) between source and destination")]
    VerifyFailed { differences: usize },

    /// A data-flow policy (#702) refused the config before any data moved:
    /// `validate` / `policy` / `run` / the serve submit path print the report
    /// and `main` maps this to an exit code equal to the violation count
    /// (clamped to 255).
    #[error("{violations} data-flow policy violation(s) — nothing was run")]
    PolicyViolations { violations: usize },

    /// A run budget's `allowed_sinks` (#703) does not name the sink a row
    /// writes to; nothing was run.
    #[error(
        "budget: row '{row}' writes to sink {sink}, which budget.allowed_sinks does not allow \
         (allowed: {allowed})"
    )]
    BudgetSinkNotAllowed {
        row: String,
        sink: String,
        allowed: String,
    },

    /// `faucet rollback` was refused because a later run changed keys the run
    /// touched (#706); nothing was changed. `main` maps this to an exit code
    /// equal to the conflict count (clamped to 255).
    #[error(
        "rollback blocked: {conflicts} key(s) were changed by a later run — pass --force to \
         restore them anyway"
    )]
    RollbackBlocked { conflicts: u64 },

    /// A `faucet serve` startup or runtime failure (bind, auth gate, etc.).
    #[error("serve error: {0}")]
    Serve(String),

    /// `overlap_policy: forbid` saw a tick fire while a run was still in flight.
    #[error("scheduled run overlap with overlap_policy: forbid — previous run still in progress")]
    ScheduleOverlapForbidden,
}

impl From<faucet_core::InstallError> for CliError {
    fn from(e: faucet_core::InstallError) -> Self {
        CliError::Observability(e.to_string())
    }
}

#[cfg(test)]
mod secrets_error_tests {
    use super::*;

    #[test]
    fn secret_errors_render_reference_not_value() {
        let e = CliError::SecretNotFound {
            scheme: "vault".into(),
            reference: "secret/data/app#token".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains("vault"));
        assert!(msg.contains("secret/data/app#token"));

        let e = CliError::SecretFieldMissing {
            scheme: "aws-sm".into(),
            reference: "prod/db".into(),
            field: "password".into(),
            available: vec!["username".into(), "host".into()],
        };
        let msg = e.to_string();
        assert!(msg.contains("password"));
        assert!(msg.contains("username") && msg.contains("host"));

        let e = CliError::SecretBackendDisabled {
            scheme: "azure-kv".into(),
        };
        assert!(e.to_string().contains("secrets-azure-kv"));

        assert!(
            CliError::SecretsRequireAsyncLoad
                .to_string()
                .contains("async")
        );
    }

    #[test]
    fn fetch_auth_notjson_errors_render_safely() {
        let e = CliError::SecretFetchFailed {
            scheme: "vault".into(),
            reference: "secret/data/app#token".into(),
            source: "connection refused".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains("vault") && msg.contains("secret/data/app#token"));

        let e = CliError::SecretAuthFailed {
            scheme: "aws-sm".into(),
            hint: "set AWS_PROFILE".into(),
        };
        assert!(e.to_string().contains("aws-sm") && e.to_string().contains("set AWS_PROFILE"));

        let e = CliError::SecretNotJson {
            scheme: "vault".into(),
            reference: "secret/raw".into(),
        };
        assert!(e.to_string().contains("not JSON"));
    }

    #[test]
    fn field_missing_with_empty_available_has_no_dangling_list() {
        let e = CliError::SecretFieldMissing {
            scheme: "vault".into(),
            reference: "secret/data/app".into(),
            field: "token".into(),
            available: vec![],
        };
        let msg = e.to_string();
        assert!(!msg.ends_with("(available: )"));
        assert!(msg.contains("token"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_env_selector_renders() {
        let e = CliError::MissingEnvSelector {
            var: "FAUCET_SOURCE".to_owned(),
        };
        let msg = e.to_string();
        assert!(msg.contains("FAUCET_SOURCE"));
        assert!(msg.contains("--from-env"));
    }

    #[test]
    fn env_conflict_names_both_vars() {
        let e = CliError::EnvConflict {
            field: "auth".to_owned(),
            scalar_var: "FAUCET_SOURCE_REST_AUTH".to_owned(),
            json_var: "FAUCET_SOURCE_REST_AUTH_JSON".to_owned(),
        };
        let msg = e.to_string();
        assert!(msg.contains("FAUCET_SOURCE_REST_AUTH"));
        assert!(msg.contains("FAUCET_SOURCE_REST_AUTH_JSON"));
    }

    #[test]
    fn invalid_env_json_names_var_and_parse_error() {
        let e = CliError::InvalidEnvJson {
            var: "FAUCET_SOURCE_REST_AUTH_JSON".to_owned(),
            message: "expected value at line 1 column 1".to_owned(),
        };
        let msg = e.to_string();
        assert!(msg.contains("FAUCET_SOURCE_REST_AUTH_JSON"));
        assert!(msg.contains("expected value"));
    }

    #[test]
    fn transform_index_gap_reports_missing_index() {
        let e = CliError::TransformIndexGap { missing: 2 };
        let msg = e.to_string();
        assert!(msg.contains('2'));
        assert!(msg.to_ascii_lowercase().contains("transform"));
    }

    #[test]
    fn unknown_template_lists_known_names() {
        let e = CliError::UnknownTemplate {
            kind: "source",
            name: "users_api".into(),
            row_id: "load_users".into(),
            known: vec!["customers_api".into(), "orders_api".into()],
        };
        let msg = e.to_string();
        assert!(msg.contains("users_api"));
        assert!(msg.contains("load_users"));
        assert!(msg.contains("customers_api"));
    }

    #[test]
    fn duplicate_template_names_kind() {
        let e = CliError::DuplicateTemplate {
            kind: "sink",
            name: "default".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains("sink"));
        assert!(msg.contains("default"));
    }

    #[test]
    fn interpolation_cycle_renders_chain() {
        let e = CliError::InterpolationCycle {
            chain: vec!["vars.a".into(), "vars.b".into(), "vars.a".into()],
        };
        let msg = e.to_string();
        assert!(msg.contains("vars.a"));
        assert!(msg.contains("vars.b"));
    }

    #[test]
    fn composition_cycle_renders_chain() {
        let e = CliError::CompositionCycle {
            chain: vec!["a.yaml".into(), "b.yaml".into(), "a.yaml".into()],
        };
        let msg = e.to_string();
        assert!(msg.contains("a.yaml") && msg.contains("b.yaml"));
        assert!(msg.contains(" -> "));
    }

    #[test]
    fn unknown_profile_lists_known() {
        let e = CliError::UnknownProfile {
            name: "staging".into(),
            known: vec!["dev".into(), "prod".into()],
        };
        let msg = e.to_string();
        assert!(msg.contains("staging") && msg.contains("dev") && msg.contains("prod"));

        let none = CliError::UnknownProfile {
            name: "x".into(),
            known: vec![],
        };
        assert!(none.to_string().contains("no `profiles:` block"));
    }

    #[test]
    fn include_not_found_names_both_paths() {
        let e = CliError::IncludeNotFound {
            path: std::path::PathBuf::from("base.yaml"),
            referenced_by: std::path::PathBuf::from("app.yaml"),
        };
        let msg = e.to_string();
        assert!(msg.contains("base.yaml") && msg.contains("app.yaml"));
    }

    #[test]
    fn composition_depth_exceeds_renders_max() {
        assert!(
            CliError::CompositionDepthExceeded { max: 32 }
                .to_string()
                .contains("32")
        );
    }

    #[test]
    fn bad_include_names_path_and_reason() {
        let e = CliError::BadInclude {
            path: std::path::PathBuf::from("f.yaml"),
            reason: "!include payload must be a string path".into(),
        };
        assert!(e.to_string().contains("f.yaml") && e.to_string().contains("string path"));
    }
}
