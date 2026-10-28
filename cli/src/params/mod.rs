//! Typed pipeline parameters — the `params:` block and the `${param.NAME}`
//! interpolation namespace (#444).
//!
//! A config declares the values that change per run:
//!
//! ```yaml
//! version: 1
//! params:
//!   tenant_id: { type: string, required: true, description: "Tenant to sync" }
//!   since:     { default: "1970-01-01" }
//!   api_token: { required: true, secret: true }
//! pipeline:
//!   source:
//!     type: rest
//!     config:
//!       url: "https://api.example.com/${param.tenant_id}/events?since=${param.since}"
//!       auth: { type: bearer, config: { token: "${param.api_token}" } }
//! ```
//!
//! and every runtime supplies them the same way:
//!
//! - `faucet run --param tenant_id=acme` / `faucet validate` (placeholders),
//! - `faucet template run <id> --param tenant_id=acme`,
//! - `POST /v1/templates/{id}/runs` with `{"params": {...}, "env": {...}}`.
//!
//! [`spec`] is the declaration grammar + coercion; [`bind`] is the pre-parse
//! substitution pass. The persistent registry that stores parameterized configs
//! server-side lives in [`crate::templates`] (the `templates` build feature).

pub mod bind;
pub mod list;
pub mod spec;

pub use bind::{
    BindMode, BoundParams, PARAM_ID, PARAMS_KEY, SecretTexts, SuppliedParams, bind_document,
    collect_cli_params, collect_env_overrides, declared, parse_cli_param, resolve,
};
pub use spec::{ParamSpec, ParamType, ParamsSpec};
