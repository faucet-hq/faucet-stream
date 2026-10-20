//! The server's reference policy (#881, RFC 0013): one allow-list for every
//! external resource a config `faucet serve` loads may reach.
//!
//! A body the server's operator wrote (a registered template, the triggers
//! file, a run already admitted) gets [`ReferencePolicy::open`]. A body the
//! request carried gets the caller policy built from the `--allow-*` flags,
//! which allows nothing it does not list:
//!
//! - local paths under `--allow-path-root` (default: the server's working
//!   directory and the system temp directory), for connector fields and
//!   `${file:}`;
//! - environment variables named by `--allow-env` (default: none);
//! - secret-manager schemes named by `--allow-secret-scheme` (default: none);
//! - URLs of the `--allow-url-scheme` schemes (default: any) to the
//!   `--allow-url-host` hosts (default: any host except link-local / metadata
//!   endpoints);
//! - connectors that run a program only with `--allow-subprocess-connectors`,
//!   and never for a tenant.
//!
//! A run for a tenant is further confined to `<--tenant-path-root>/<tenant>`.
//!
//! Connector fields are found through their schema's `x-faucet-resource`
//! annotation ([`faucet_core::resolve`]), so a connector the server has never
//! seen is checked without any serve code change. The server's own
//! credentials and the system trees stay out of reach for every origin.

use crate::serve::load::BodyOrigin;
use faucet_core::resolve::{ReferencePolicy, ResourceKind, ResourceRef, annotated_resources};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

/// The server's reference policies, by body origin.
#[derive(Debug, Clone, PartialEq)]
pub struct ServeReferencePolicy {
    caller: ReferencePolicy,
    tenant_root: Option<PathBuf>,
}

impl Default for ServeReferencePolicy {
    fn default() -> Self {
        Self::from_args(&crate::cli::ReferenceArgs::default())
            .expect("the default reference arguments are valid")
    }
}

/// The path roots a caller-supplied config gets when no `--allow-path-root`
/// is given: the working directory and the system temp directory.
pub fn default_path_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        roots.push(cwd);
    }
    roots.push(std::env::temp_dir());
    roots
}

impl ServeReferencePolicy {
    /// Build the policies from the `faucet serve` flags.
    pub fn from_args(args: &crate::cli::ReferenceArgs) -> Result<Self, String> {
        for scheme in &args.allow_secret_scheme {
            if !crate::secrets::SECRET_SCHEMES.contains(&scheme.as_str()) {
                return Err(format!(
                    "--allow-secret-scheme `{scheme}` is not a secret-manager scheme (one of {})",
                    crate::secrets::SECRET_SCHEMES.join(", ")
                ));
            }
        }
        for (flag, values) in [
            ("--allow-env", &args.allow_env),
            ("--allow-url-host", &args.allow_url_host),
            ("--allow-url-scheme", &args.allow_url_scheme),
        ] {
            if values.iter().any(|v| v.trim().is_empty()) {
                return Err(format!("{flag} takes a non-empty value"));
            }
        }
        let roots = if args.allow_path_root.is_empty() {
            default_path_roots()
        } else {
            args.allow_path_root.clone()
        };
        let mut caller = ReferencePolicy::deny_all()
            .with_path_roots(roots)
            .with_env(args.allow_env.iter().cloned())
            .with_secret_schemes(&args.allow_secret_scheme);
        caller = if args.allow_url_scheme.is_empty() {
            caller.any_url_scheme()
        } else {
            caller.with_url_schemes(&args.allow_url_scheme)
        };
        caller = if args.allow_url_host.is_empty() {
            caller.any_url_host()
        } else {
            caller.with_url_hosts(args.allow_url_host.iter().cloned())
        };
        Ok(Self {
            caller,
            tenant_root: args.tenant_path_root.clone(),
        })
    }

    /// The policy for a caller-supplied body (subprocess connectors off).
    pub fn caller(&self) -> &ReferencePolicy {
        &self.caller
    }

    /// The directory tenant runs are confined to (per tenant id), if set.
    pub fn tenant_root(&self) -> Option<&std::path::Path> {
        self.tenant_root.as_deref()
    }

    /// The policy a body of `origin` loads under, for `tenant` when the run is
    /// one.
    pub fn for_origin(&self, origin: BodyOrigin, tenant: Option<&str>) -> ReferencePolicy {
        match origin {
            BodyOrigin::Trusted | BodyOrigin::ServerFile => ReferencePolicy::open(),
            BodyOrigin::Caller { allow_subprocess } => {
                let mut p = self
                    .caller
                    .clone()
                    .with_subprocess(allow_subprocess && tenant.is_none());
                if let (Some(root), Some(t)) = (&self.tenant_root, tenant) {
                    p = p.with_path_roots([root.join(t)]);
                }
                p
            }
        }
    }
}

fn installed_cell() -> &'static RwLock<Arc<ServeReferencePolicy>> {
    static CELL: OnceLock<RwLock<Arc<ServeReferencePolicy>>> = OnceLock::new();
    CELL.get_or_init(|| RwLock::new(Arc::new(ServeReferencePolicy::default())))
}

/// Install the policy the server was started with, for the load paths that
/// do not carry the server state (MCP, the legacy loader entry points).
pub fn install(policy: ServeReferencePolicy) {
    if let Ok(mut cell) = installed_cell().write() {
        *cell = Arc::new(policy);
    }
}

/// The installed policy (the default one before a server starts).
pub fn installed() -> Arc<ServeReferencePolicy> {
    installed_cell()
        .read()
        .map(|p| p.clone())
        .unwrap_or_else(|_| Arc::new(ServeReferencePolicy::default()))
}

type SchemaCache = Mutex<HashMap<String, Arc<Vec<Value>>>>;

fn schema_cache() -> &'static SchemaCache {
    static CACHE: OnceLock<SchemaCache> = OnceLock::new();
    CACHE.get_or_init(Default::default)
}

/// The config schemas of connector `kind` (its source and its sink, as
/// registered — built-in or plugin) and of a transform of that name, whose
/// config can name resources too (a `sql` transform's reference relations).
fn schemas(kind: &str) -> Arc<Vec<Value>> {
    if let Ok(cache) = schema_cache().lock()
        && let Some(hit) = cache.get(kind)
    {
        return hit.clone();
    }
    let found: Vec<Value> = [
        crate::registry::source_schema(kind).ok(),
        crate::registry::sink_schema(kind).ok(),
        crate::transforms::transform_schema(kind).ok(),
    ]
    .into_iter()
    .flatten()
    .collect();
    let found = Arc::new(found);
    if let Ok(mut cache) = schema_cache().lock() {
        cache.insert(kind.to_string(), found.clone());
    }
    found
}

/// The resources a `kind` connector's `config` names through annotated
/// schema fields.
pub fn connector_resources(kind: &str, config: &Value) -> Vec<ResourceRef> {
    let mut seen = BTreeSet::new();
    schemas(kind)
        .iter()
        .flat_map(|s| annotated_resources(s, config))
        .filter(|r| seen.insert((r.pointer.clone(), r.kind, r.value.clone())))
        .collect()
}

/// Whether connector `kind` runs a program (its schema declares a
/// `command` field).
pub fn runs_program(kind: &str) -> bool {
    schemas(kind)
        .iter()
        .any(|s| faucet_core::resolve::declares(s, ResourceKind::Command))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::ReferenceArgs;

    #[cfg(feature = "transform-sql")]
    #[test]
    fn a_sql_transform_names_its_relation_resources() {
        let config = serde_json::json!({
            "query": "SELECT * FROM batch, meta",
            "relations": [
                {"name": "meta", "source": {"type": "http", "url": "http://169.254.169.254/latest/"}},
                {"name": "dim", "source": {"type": "csv", "path": "/etc/dim.csv"}}
            ]
        });
        let found = connector_resources("sql", &config);
        let urls: Vec<_> = found
            .iter()
            .filter(|r| r.kind == ResourceKind::Url)
            .map(|r| r.value.as_str())
            .collect();
        assert_eq!(urls, vec!["http://169.254.169.254/latest/"]);
        assert!(
            found
                .iter()
                .any(|r| r.kind == ResourceKind::Path && r.value == "/etc/dim.csv")
        );
        let caller = ServeReferencePolicy::default();
        assert!(found.iter().any(|r| caller.caller().check(r).is_err()));
    }

    #[test]
    fn the_default_caller_policy_is_an_allow_list() {
        let p = ServeReferencePolicy::default();
        let c = p.caller();
        assert!(c.check_env("HOME").is_err());
        assert!(c.check_secret("vault", "x").is_err());
        assert!(c.check_path("/etc/passwd").is_err());
        let tmp = std::env::temp_dir().join("x.csv");
        assert!(c.check_path(&tmp.display().to_string()).is_ok());
        assert!(c.check_path("relative.csv").is_ok());
        assert!(c.check_url("https://api.example.com").is_ok());
        assert!(c.check_url("http://169.254.169.254/").is_err());
        assert!(!c.allows_subprocess());
        assert!(p.tenant_root().is_none());
    }

    #[test]
    fn flags_widen_the_caller_policy() {
        let args = ReferenceArgs {
            allow_path_root: vec!["/srv/data".into()],
            allow_url_scheme: vec!["https".into()],
            allow_url_host: vec!["*.example.com".into()],
            allow_env: vec!["APP_*".into()],
            allow_secret_scheme: vec!["vault".into()],
            tenant_path_root: Some("/srv/tenants".into()),
        };
        let p = ServeReferencePolicy::from_args(&args).unwrap();
        let c = p.caller();
        assert!(c.check_env("APP_X").is_ok());
        assert!(c.check_secret("vault", "a").is_ok());
        assert!(c.check_path("/srv/data/x").is_ok());
        assert!(
            c.check_path(&std::env::temp_dir().display().to_string())
                .is_err()
        );
        assert!(c.check_url("https://a.example.com").is_ok());
        assert!(c.check_url("http://a.example.com").is_err());
        let tenant = p.for_origin(
            BodyOrigin::Caller {
                allow_subprocess: true,
            },
            Some("acme"),
        );
        assert!(tenant.check_path("/srv/tenants/acme/in.csv").is_ok());
        assert!(tenant.check_path("/srv/tenants/other/in.csv").is_err());
        assert!(!tenant.allows_subprocess());
        let caller = p.for_origin(
            BodyOrigin::Caller {
                allow_subprocess: true,
            },
            None,
        );
        assert!(caller.allows_subprocess());
        for origin in [BodyOrigin::Trusted, BodyOrigin::ServerFile] {
            assert_eq!(p.for_origin(origin, Some("t")), ReferencePolicy::open());
        }
    }

    #[test]
    fn bad_flags_are_refused() {
        let bad = ReferenceArgs {
            allow_secret_scheme: vec!["env".into()],
            ..Default::default()
        };
        assert!(
            ServeReferencePolicy::from_args(&bad)
                .unwrap_err()
                .contains("--allow-secret-scheme")
        );
        let blank = ReferenceArgs {
            allow_env: vec![" ".into()],
            ..Default::default()
        };
        assert!(
            ServeReferencePolicy::from_args(&blank)
                .unwrap_err()
                .contains("--allow-env")
        );
    }

    #[test]
    fn install_replaces_the_policy() {
        let before = installed();
        let wide = ServeReferencePolicy::from_args(&ReferenceArgs {
            allow_env: vec!["FAUCET_REFERENCES_INSTALL_TEST".into()],
            ..Default::default()
        })
        .unwrap();
        install(wide.clone());
        assert_eq!(*installed(), wide);
        install((*before).clone());
    }

    #[test]
    fn unknown_kinds_have_no_resources() {
        assert!(connector_resources("no-such-kind", &serde_json::json!({"path": "/x"})).is_empty());
        assert!(!runs_program("no-such-kind"));
    }

    #[cfg(feature = "source-csv")]
    #[test]
    fn a_built_in_connector_is_checked_through_its_schema() {
        let found = connector_resources("csv", &serde_json::json!({"path": "/data/in.csv"}));
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].kind, ResourceKind::Path);
        assert!(!runs_program("csv"));
    }

    #[cfg(feature = "source-singer")]
    #[test]
    fn singer_runs_a_program() {
        assert!(runs_program("singer"));
    }
}
