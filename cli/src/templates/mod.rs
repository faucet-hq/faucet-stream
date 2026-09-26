//! Pipeline template registry (#444) — register a parameterized config **once**,
//! then trigger runs by `{id, params}`.
//!
//! This module is the single implementation the three surfaces adapt to:
//!
//! | Surface | Register | List / get | Trigger |
//! |---|---|---|---|
//! | HTTP (`faucet serve`) | `POST /v1/templates` | `GET /v1/templates[/{id}]` | `POST /v1/templates/{id}/runs` |
//! | MCP (`--mcp` / `faucet mcp`) | `register_template` | `list_templates` / `get_template` | `run_template` |
//! | CLI | `faucet template register` | `faucet template list` / `show` | `faucet template run` |
//!
//! [`register`] appends a version; [`launch`] makes one live; [`rollback`] returns
//! to the one before it; [`promote`] moves an environment channel;
//! [`set_deprecated`] retires a template; [`resolve_version`] turns a selector into
//! a concrete version; [`materialize`] turns `{id, version, params, env}` back into
//! a ready-to-run config document.
//!
//! ## Builds, releases, and lifecycle
//!
//! Versions are **numeric and auto-incrementing** — every register appends
//! `max + 1` and is otherwise **inert**. Registering a nightly or a feature build
//! moves nobody; only an explicit [`launch`] does. That separation is the point of
//! the model: unpinned callers ride the *blessed* release, not the most recent
//! upload.
//!
//! Over the numbers sits a **closed set of channels**
//! ([`VersionChannel`](crate::serve::history::templates::VersionChannel)) —
//! three derived (`stable` = the launched version and the default, `previous` =
//! the one launched before it, `newest` = the highest number) plus the assignable
//! environments `dev`, `test`, `staging`, `pre-prod`, `canary`, `prod`. The set is
//! closed on purpose: an open tag namespace becomes a second, unreviewable naming
//! system in which `prd` silently creates a channel nobody watches. `latest` is
//! rejected outright as ambiguous between `stable` and `newest`.
//!
//! A **template** (not a version) carries the lifecycle status
//! ([`TemplateStatus`](crate::serve::history::templates::TemplateStatus)):
//! `draft` until something is launched, then `launched`, or `deprecated` once
//! retired. It is derived from the launch log plus a deprecation marker, so it can
//! never disagree with the registry's contents. Everything downstream of `materialize` is the ordinary run
//! path (`load_submission` → `expand` → `run_expanded`), so templates inherit
//! every existing guarantee — matrix/topology expansion, the exactly-once gate,
//! idempotency keys, RBAC, and the audit log — for free.
//!
//! ## What is and isn't persisted
//!
//! The body is stored **verbatim**: `${env:…}` / `${vault:…}` / `${secret:…}`
//! directives stay unresolved and are resolved at trigger time against the
//! *executing server's* environment and credentials — exactly the privilege
//! surface of a normally-submitted config. Caller-supplied `secret: true` param
//! values are never persisted at all; see [`materialize`] for the one place that
//! distinction is enforced.

pub mod store;
#[cfg(feature = "templates")]
pub mod suite;
#[cfg(feature = "templates-sync")]
pub mod sync;

pub use store::{
    LaunchOutcome, Materialize, MaterializedConfig, OverlayChoice, RegisterPreview,
    RegisterRequest, SinkChoice, TemplateStore, deprecation_warning, launch, list_with_state,
    materialize, materialize_for_run, materialize_pair, materialize_pair_overlaid, parse_body,
    preview_register, promote, register, resolve_store_url, resolve_version, rollback,
    set_deprecated, set_version_deprecated, template_state,
};

use crate::error::{CliError, CliResult};

/// Strip comments from a stored template body and re-emit it as canonical YAML.
/// `serde_yaml` parses JSON too (JSON ⊂ YAML), so a JSON-format template
/// normalizes to YAML as well; `${param.*}` tokens are plain strings and pass
/// through unchanged. A pure view transform — it never mutates the stored body.
/// Shared by `faucet template show --clean` and the serve `?clean=true` endpoint.
pub fn clean_config_yaml(body: &str) -> CliResult<String> {
    let value: serde_yaml::Value = serde_yaml::from_str(body)
        .map_err(|e| CliError::Internal(format!("clean-config: parse template body: {e}")))?;
    serde_yaml::to_string(&value)
        .map_err(|e| CliError::Internal(format!("clean-config: re-emit YAML: {e}")))
}
