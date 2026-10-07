//! Secrets-manager interpolation for the config layer (#125).
//!
//! Resolution runs as the final config-load stage (after env/file and
//! vars/templates), over the parsed config tree. See the design spec.

pub mod registry;

#[cfg(feature = "secrets-aws-sm")]
mod aws_sm;
#[cfg(feature = "secrets-azure-kv")]
mod azure_kv;
#[cfg(feature = "secrets-gcp-sm")]
mod gcp_sm;
#[cfg(feature = "secrets-vault")]
mod vault;

use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::interpolate::{self, Directive};
use async_trait::async_trait;
use futures::stream::{self, StreamExt, TryStreamExt};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

/// The four secret-manager schemes this layer recognises.
pub const SECRET_SCHEMES: &[&str] = &["vault", "aws-sm", "gcp-sm", "azure-kv"];

/// A `(scheme, reference)` pair, e.g. `("vault", "secret/data/app#token")`.
pub type SecretRef = (String, String);

#[async_trait]
pub trait SecretResolver: Send + Sync {
    /// Scheme handled, e.g. `"vault"`.
    fn scheme(&self) -> &'static str;
    /// Resolve a `path[#field]` reference to its string value.
    async fn resolve(&self, reference: &str) -> CliResult<String>;
}

/// HTTP client for the secret-manager backends: a hung endpoint fails the
/// load instead of hanging it.
#[cfg(any(feature = "secrets-vault", feature = "secrets-gcp-sm"))]
pub(crate) fn secret_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap_or_default()
}

/// Refuse `#field` on a scheme that cannot extract one, instead of letting
/// the `#…` reach the backend as part of the name.
pub(crate) fn reject_field(scheme: &str, reference: &str) -> CliResult<()> {
    if reference.contains('#') {
        return Err(CliError::SecretFetchFailed {
            scheme: scheme.into(),
            reference: reference.into(),
            source: format!(
                "`#field` extraction is supported for vault and aws-sm only, not {scheme}"
            )
            .into(),
        });
    }
    Ok(())
}

/// Split a `path#field` reference into `(path, Some(field))` or `(path, None)`.
#[allow(dead_code)] // used by provider modules added in later tasks
pub(crate) fn split_field(reference: &str) -> (&str, Option<&str>) {
    match reference.split_once('#') {
        Some((path, field)) => (path, Some(field)),
        None => (reference, None),
    }
}

/// Extract `field` from a secret body that must parse as a JSON object.
/// Used by Vault and AWS resolvers for the `#field` selector.
#[allow(dead_code)] // used by provider modules added in later tasks
pub(crate) fn extract_field(
    scheme: &str,
    reference: &str,
    body: &str,
    field: &str,
) -> CliResult<String> {
    let json: Value = serde_json::from_str(body).map_err(|_| CliError::SecretNotJson {
        scheme: scheme.to_owned(),
        reference: reference.to_owned(),
    })?;
    let obj = json.as_object().ok_or_else(|| CliError::SecretNotJson {
        scheme: scheme.to_owned(),
        reference: reference.to_owned(),
    })?;
    match obj.get(field) {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(other) => Ok(other.to_string()),
        None => Err(CliError::SecretFieldMissing {
            scheme: scheme.to_owned(),
            reference: reference.to_owned(),
            field: field.to_owned(),
            available: obj.keys().cloned().collect(),
        }),
    }
}

/// Apply `f` to every string leaf in `value`, recursively.
fn for_each_string<F: FnMut(&str)>(value: &Value, f: &mut F) {
    match value {
        Value::String(s) => f(s),
        Value::Array(a) => a.iter().for_each(|v| for_each_string(v, f)),
        Value::Object(m) => m.values().for_each(|v| for_each_string(v, f)),
        _ => {}
    }
}

/// Mutate every string leaf in `value`, recursively.
fn for_each_string_mut<F: FnMut(&mut String) -> CliResult<()>>(
    value: &mut Value,
    f: &mut F,
) -> CliResult<()> {
    match value {
        Value::String(s) => f(s),
        Value::Array(a) => a.iter_mut().try_for_each(|v| for_each_string_mut(v, f)),
        Value::Object(m) => m.values_mut().try_for_each(|v| for_each_string_mut(v, f)),
        _ => Ok(()),
    }
}

/// Collect every unique secret reference found in a single string.
fn collect_refs_in_str(s: &str, out: &mut BTreeSet<SecretRef>) {
    for (_token, dir) in interpolate::iter_directives(s) {
        if let Directive::LoadTime { prefix, body } = dir
            && SECRET_SCHEMES.contains(&prefix)
        {
            out.insert((prefix.to_owned(), body.to_owned()));
        }
    }
}

/// Collect all unique secret references reachable from a config `Value`.
pub fn collect_refs(value: &Value, out: &mut BTreeSet<SecretRef>) {
    for_each_string(value, &mut |s| collect_refs_in_str(s, out));
}

/// Substitute every secret directive in a `Value` from `cache`. Non-secret
/// directives (`${users.id}`) pass through verbatim.
pub fn substitute(value: &mut Value, cache: &HashMap<SecretRef, String>) -> CliResult<()> {
    for_each_string_mut(value, &mut |s| {
        let new = interpolate::rewrite(s, |body| match interpolate::classify_directive(body) {
            Directive::LoadTime { prefix, body: b } if SECRET_SCHEMES.contains(&prefix) => {
                Ok(Some(
                    cache
                        .get(&(prefix.to_owned(), b.to_owned()))
                        .cloned()
                        .expect("scan collected every secret ref before fetch"),
                ))
            }
            _ => Ok(None),
        })?;
        *s = new;
        Ok(())
    })
}

/// Map of scheme → resolver, built from compiled-in features (or injected in tests).
#[derive(Default, Clone)]
pub struct ResolverSet {
    resolvers: HashMap<&'static str, Arc<dyn SecretResolver>>,
}

impl ResolverSet {
    pub fn insert(&mut self, resolver: Arc<dyn SecretResolver>) {
        self.resolvers.insert(resolver.scheme(), resolver);
    }
    fn get(&self, scheme: &str) -> Option<&Arc<dyn SecretResolver>> {
        self.resolvers.get(scheme)
    }
}

/// Construct the resolver for one scheme, or `SecretBackendDisabled` if the
/// feature was not compiled in. Constructors are cheap (no network / client
/// init) — heavy clients are built lazily on first `resolve`.
fn make_resolver(scheme: &str) -> CliResult<Arc<dyn SecretResolver>> {
    match scheme {
        #[cfg(feature = "secrets-vault")]
        "vault" => Ok(Arc::new(vault::VaultResolver::from_env()?)),
        #[cfg(feature = "secrets-aws-sm")]
        "aws-sm" => Ok(Arc::new(aws_sm::AwsSmResolver::new())),
        #[cfg(feature = "secrets-gcp-sm")]
        "gcp-sm" => Ok(Arc::new(gcp_sm::GcpSmResolver::new())),
        #[cfg(feature = "secrets-azure-kv")]
        "azure-kv" => Ok(Arc::new(azure_kv::AzureKvResolver::new())),
        other => Err(CliError::SecretBackendDisabled {
            scheme: other.to_owned(),
        }),
    }
}

/// Apply a read-only closure to every config `Value` location (mirrors
/// `resolve_config_refs`'s traversal).
fn visit_config_values<F: FnMut(&Value)>(cfg: &PipelineConfig, mut f: F) {
    // The shared `auth:` catalog and the `vars:` block are first-class config
    // locations: a secret placed in either must resolve before the auth catalog
    // is built / vars are consumed (#134).
    if let Some(auth) = cfg.auth.as_ref() {
        for spec in auth.values() {
            f(spec);
        }
    }
    if let Some(vars) = cfg.vars.as_ref() {
        for v in vars.values() {
            f(v);
        }
    }
    // The replication snapshot source config is a first-class connector config
    // (like `pipeline.source.config`); secrets placed there must resolve too.
    if let Some(r) = cfg.replication.as_ref() {
        f(&r.snapshot.source.config);
    }
    for spec in cfg.pipeline.sources.values() {
        f(&spec.config);
    }
    for spec in cfg.pipeline.sinks.values() {
        f(&spec.config);
    }
    if let Some(spec) = cfg.pipeline.source.as_ref() {
        f(&spec.config);
    }
    if let Some(spec) = cfg.pipeline.sink.as_ref() {
        f(&spec.config);
    }
    for t in cfg.pipeline.transforms.iter() {
        f(&t.config);
    }
    if let Some(s) = cfg.pipeline.state.as_ref() {
        f(&s.config);
    }
    if let Some(d) = cfg.pipeline.dlq.as_ref() {
        f(&d.sink.config);
    }
    for row in cfg.matrix.iter() {
        if let Some(p) = row.source.as_ref()
            && let Some(c) = p.config.as_ref()
        {
            f(c);
        }
        if let Some(p) = row.sink.as_ref()
            && let Some(c) = p.config.as_ref()
        {
            f(c);
        }
        if let Some(ts) = row.transforms.as_ref() {
            for t in ts.iter() {
                f(&t.config);
            }
        }
        if let Some(s) = row.state.as_ref() {
            f(&s.config);
        }
        if let Some(Some(d)) = row.dlq.as_ref() {
            f(&d.sink.config);
        }
    }
    let _ = visit_typed_blocks(cfg, |v| {
        f(v);
        Ok(())
    });
}

/// Run `f` over the serialized form of every typed config block that can
/// carry a credential and is not a plain `Value` walked above (CLI-42).
fn visit_typed_blocks(
    cfg: &PipelineConfig,
    mut f: impl FnMut(&Value) -> CliResult<()>,
) -> CliResult<()> {
    let mut each = |v: serde_json::Result<Value>| match v {
        Ok(v) => f(&v),
        Err(_) => Ok(()),
    };
    each(serde_json::to_value(&cfg.pipeline.nodes))?;
    each(serde_json::to_value(&cfg.observability))?;
    each(serde_json::to_value(&cfg.replication))?;
    each(serde_json::to_value(&cfg.verify))?;
    each(serde_json::to_value(&cfg.reconcile))?;
    #[cfg(feature = "masking")]
    each(serde_json::to_value(&cfg.pipeline.masking))?;
    #[cfg(feature = "lineage")]
    each(serde_json::to_value(&cfg.lineage))?;
    #[cfg(feature = "catalog")]
    each(serde_json::to_value(&cfg.catalog))?;
    #[cfg(feature = "notify")]
    each(serde_json::to_value(&cfg.notifications))?;
    Ok(())
}

/// Resolve `f` over `slot`'s serialized form, writing it back only when `f`
/// changed something.
fn walk_typed<T: serde::Serialize + serde::de::DeserializeOwned>(
    slot: &mut T,
    what: &str,
    f: &mut impl FnMut(&mut Value) -> CliResult<()>,
) -> CliResult<()> {
    let mut v = serde_json::to_value(&*slot)
        .map_err(|e| CliError::Internal(format!("serializing `{what}` for secrets: {e}")))?;
    let before = v.clone();
    f(&mut v)?;
    if v != before {
        *slot = serde_json::from_value(v)
            .map_err(|e| CliError::Config(format!("`{what}` after secret resolution: {e}")))?;
    }
    Ok(())
}

/// Apply a mutating, fallible closure to every config `Value` location.
fn visit_config_values_mut<F: FnMut(&mut Value) -> CliResult<()>>(
    cfg: &mut PipelineConfig,
    mut f: F,
) -> CliResult<()> {
    // See `visit_config_values`: the `auth:` catalog and `vars:` block are
    // walked too so secrets resolve there before they are consumed (#134).
    if let Some(auth) = cfg.auth.as_mut() {
        for spec in auth.values_mut() {
            f(spec)?;
        }
    }
    if let Some(vars) = cfg.vars.as_mut() {
        for v in vars.values_mut() {
            f(v)?;
        }
    }
    // The replication snapshot source config is a first-class connector config
    // (like `pipeline.source.config`); secrets placed there must resolve too.
    if let Some(r) = cfg.replication.as_mut() {
        f(&mut r.snapshot.source.config)?;
    }
    for spec in cfg.pipeline.sources.values_mut() {
        f(&mut spec.config)?;
    }
    for spec in cfg.pipeline.sinks.values_mut() {
        f(&mut spec.config)?;
    }
    if let Some(spec) = cfg.pipeline.source.as_mut() {
        f(&mut spec.config)?;
    }
    if let Some(spec) = cfg.pipeline.sink.as_mut() {
        f(&mut spec.config)?;
    }
    for t in cfg.pipeline.transforms.iter_mut() {
        f(&mut t.config)?;
    }
    if let Some(s) = cfg.pipeline.state.as_mut() {
        f(&mut s.config)?;
    }
    if let Some(d) = cfg.pipeline.dlq.as_mut() {
        f(&mut d.sink.config)?;
    }
    for row in cfg.matrix.iter_mut() {
        if let Some(p) = row.source.as_mut()
            && let Some(c) = p.config.as_mut()
        {
            f(c)?;
        }
        if let Some(p) = row.sink.as_mut()
            && let Some(c) = p.config.as_mut()
        {
            f(c)?;
        }
        if let Some(ts) = row.transforms.as_mut() {
            for t in ts.iter_mut() {
                f(&mut t.config)?;
            }
        }
        if let Some(s) = row.state.as_mut() {
            f(&mut s.config)?;
        }
        if let Some(Some(d)) = row.dlq.as_mut() {
            f(&mut d.sink.config)?;
        }
    }
    walk_typed(&mut cfg.pipeline.nodes, "pipeline.nodes", &mut f)?;
    walk_typed(&mut cfg.observability, "observability", &mut f)?;
    walk_typed(&mut cfg.replication, "mirror", &mut f)?;
    walk_typed(&mut cfg.verify, "verify", &mut f)?;
    walk_typed(&mut cfg.reconcile, "reconcile", &mut f)?;
    #[cfg(feature = "masking")]
    walk_typed(&mut cfg.pipeline.masking, "pipeline.masking", &mut f)?;
    #[cfg(feature = "lineage")]
    walk_typed(&mut cfg.lineage, "lineage", &mut f)?;
    #[cfg(feature = "catalog")]
    walk_typed(&mut cfg.catalog, "catalog", &mut f)?;
    #[cfg(feature = "notify")]
    walk_typed(&mut cfg.notifications, "notifications", &mut f)?;
    Ok(())
}

/// Refuse a config that still holds a secret-manager directive after
/// resolution: it sits somewhere the resolver does not reach, and the literal
/// `${vault:…}` would otherwise be sent as the credential.
fn refuse_unresolved(cfg: &PipelineConfig) -> CliResult<()> {
    let Ok(mut doc) = serde_json::to_value(cfg) else {
        return Ok(());
    };
    if let Some(map) = doc.as_object_mut() {
        // A param default is bound into the body before this pass.
        map.remove(crate::params::PARAMS_KEY);
    }
    let mut left = BTreeSet::new();
    collect_refs(&doc, &mut left);
    match left.into_iter().next() {
        None => Ok(()),
        Some((scheme, reference)) => Err(CliError::Config(format!(
            "secret directive `${{{scheme}:{reference}}}` is in a config block secrets are \
             not resolved in; move the value into a connector, auth, vars or param location"
        ))),
    }
}

/// Collect every unique secret reference across the whole config.
pub(crate) fn scan_config(cfg: &PipelineConfig) -> BTreeSet<SecretRef> {
    let mut refs = BTreeSet::new();
    visit_config_values(cfg, |v| collect_refs(v, &mut refs));
    refs
}

/// Parse `path` (tolerating secret directives) and return its unique secret refs.
pub fn scan_path_refs(
    path: &std::path::Path,
    profile: Option<&str>,
) -> CliResult<BTreeSet<SecretRef>> {
    scan_path_refs_with(path, profile, &crate::config::RunInputs::default())
}

/// [`scan_path_refs`] with caller-supplied [`crate::config::RunInputs`] (#444).
///
/// The pre-scan is a *structural* load, so it must bind `${param.*}` the same way
/// the caller's real load will. Without this a bare `faucet validate` on a config
/// declaring a `required` param failed here — in strict mode — before the caller's
/// placeholder binding ever ran.
pub fn scan_path_refs_with(
    path: &std::path::Path,
    profile: Option<&str>,
    inputs: &crate::config::RunInputs,
) -> CliResult<BTreeSet<SecretRef>> {
    let cfg = PipelineConfig::from_path_tolerating_secrets_with(path, profile, inputs)?;
    Ok(scan_config(&cfg))
}

/// Error with `SecretsRequireAsyncLoad` if any secret directive is present.
/// Called by the synchronous `from_path` so secrets never silently survive.
pub fn ensure_no_secret_directives(cfg: &PipelineConfig) -> CliResult<()> {
    if scan_config(cfg).is_empty() {
        Ok(())
    } else {
        Err(CliError::SecretsRequireAsyncLoad)
    }
}

/// Production entry point: resolve all secret directives in `cfg` in place.
/// Builds resolvers only for the schemes actually referenced.
pub async fn resolve_secrets(cfg: &mut PipelineConfig) -> CliResult<()> {
    let refs = scan_config(cfg);
    if refs.is_empty() {
        return refuse_unresolved(cfg);
    }
    let mut set = ResolverSet::default();
    let schemes: BTreeSet<&str> = refs.iter().map(|(s, _)| s.as_str()).collect();
    for scheme in schemes {
        set.insert(make_resolver(scheme)?);
    }
    resolve_secrets_with(cfg, &set).await
}

/// Resolve all secret directives using a caller-supplied resolver set (the
/// seam used by tests to inject fakes).
pub async fn resolve_secrets_with(cfg: &mut PipelineConfig, set: &ResolverSet) -> CliResult<()> {
    let refs = scan_config(cfg);
    if refs.is_empty() {
        return refuse_unresolved(cfg);
    }
    let cache = fetch_all(&refs, set).await?;
    visit_config_values_mut(cfg, |v| substitute(v, &cache))?;
    refuse_unresolved(cfg)
}

/// Resolve every secret directive in an arbitrary document in place — for
/// files that are not pipeline configs (`faucet serve --auth-config`).
pub async fn resolve_value(value: &mut Value) -> CliResult<()> {
    let mut set = ResolverSet::default();
    let mut refs = BTreeSet::new();
    collect_refs(value, &mut refs);
    for scheme in refs
        .iter()
        .map(|(s, _)| s.as_str())
        .collect::<BTreeSet<_>>()
    {
        set.insert(make_resolver(scheme)?);
    }
    resolve_value_with(value, &set).await
}

/// [`resolve_value`] with a caller-supplied resolver set.
pub async fn resolve_value_with(value: &mut Value, set: &ResolverSet) -> CliResult<()> {
    let mut refs = BTreeSet::new();
    collect_refs(value, &mut refs);
    if refs.is_empty() {
        return Ok(());
    }
    let cache = fetch_all(&refs, set).await?;
    substitute(value, &cache)
}

/// Fetch every reference concurrently (bounded), de-duplicated by the result
/// map, registering each resolved value for redaction.
async fn fetch_all(
    refs: &BTreeSet<SecretRef>,
    set: &ResolverSet,
) -> CliResult<HashMap<SecretRef, String>> {
    const MAX_CONCURRENCY: usize = 8;
    let pairs: Vec<(SecretRef, String)> =
        stream::iter(refs.iter().cloned())
            .map(|(scheme, reference)| async move {
                // Clone the Arc so each concurrent future owns its resolver rather
                // than borrowing `set` across the await point.
                let resolver = Arc::clone(set.get(&scheme).ok_or_else(|| {
                    CliError::SecretBackendDisabled {
                        scheme: scheme.clone(),
                    }
                })?);
                let value = resolver.resolve(&reference).await?;
                registry::register(&value);
                Ok::<(SecretRef, String), CliError>(((scheme, reference), value))
            })
            .buffer_unordered(MAX_CONCURRENCY)
            .try_collect()
            .await?;
    Ok(pairs.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn collects_unique_refs_and_ignores_other_directives() {
        let v = json!({
            "a": "${vault:secret/data/app#token}",
            "b": "${aws-sm:prod/db#password}",
            "c": "${vault:secret/data/app#token}",
            "d": "${users.id}",
            "e": "${env:HOME}",
            "nested": ["${gcp-sm:projects/p/secrets/s/versions/latest}"]
        });
        let mut refs = BTreeSet::new();
        collect_refs(&v, &mut refs);
        assert_eq!(refs.len(), 3);
        assert!(refs.contains(&("vault".into(), "secret/data/app#token".into())));
        assert!(refs.contains(&("aws-sm".into(), "prod/db#password".into())));
        assert!(refs.contains(&(
            "gcp-sm".into(),
            "projects/p/secrets/s/versions/latest".into()
        )));
    }

    #[test]
    fn substitutes_from_cache_and_preserves_runtime_refs() {
        let mut v = json!({
            "token": "Bearer ${vault:secret/data/app#token}",
            "path": "/v1/${users.id}"
        });
        let mut cache = HashMap::new();
        cache.insert(
            ("vault".into(), "secret/data/app#token".into()),
            "abc123".into(),
        );
        substitute(&mut v, &cache).unwrap();
        assert_eq!(v["token"], "Bearer abc123");
        assert_eq!(v["path"], "/v1/${users.id}");
    }

    #[test]
    fn extract_field_picks_key_or_errors_with_available() {
        let body = r#"{"username":"u","password":"p"}"#;
        assert_eq!(
            extract_field("aws-sm", "ref", body, "password").unwrap(),
            "p"
        );
        match extract_field("aws-sm", "ref", body, "missing").unwrap_err() {
            CliError::SecretFieldMissing { available, .. } => {
                assert!(available.contains(&"username".to_string()));
            }
            other => panic!("expected SecretFieldMissing, got {other:?}"),
        }
        match extract_field("aws-sm", "ref", "not json", "x").unwrap_err() {
            CliError::SecretNotJson { .. } => {}
            other => panic!("expected SecretNotJson, got {other:?}"),
        }
    }

    struct FakeResolver {
        scheme: &'static str,
        value: String,
    }
    #[async_trait]
    impl SecretResolver for FakeResolver {
        fn scheme(&self) -> &'static str {
            self.scheme
        }
        async fn resolve(&self, _reference: &str) -> CliResult<String> {
            Ok(self.value.clone())
        }
    }

    #[tokio::test]
    async fn resolve_secrets_with_substitutes_via_injected_resolvers() {
        let mut set = ResolverSet::default();
        set.insert(Arc::new(FakeResolver {
            scheme: "vault",
            value: "RESOLVED".into(),
        }));
        let cfg_yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x, auth: { type: bearer, config: { token: "${vault:secret/data/app#token}" } } } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let mut cfg = PipelineConfig::from_text(cfg_yaml, std::path::Path::new("p.yaml")).unwrap();
        resolve_secrets_with(&mut cfg, &set).await.unwrap();
        let token = &cfg.pipeline.source.as_ref().unwrap().config["auth"]["config"]["token"];
        assert_eq!(token, "RESOLVED");
    }

    #[tokio::test]
    async fn typed_blocks_resolve_and_an_unreachable_directive_is_refused() {
        let mut set = ResolverSet::default();
        set.insert(Arc::new(FakeResolver {
            scheme: "vault",
            value: "RESOLVED".into(),
        }));
        let cfg_yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  masking:
    key: "${vault:secret/data/mask#k}"
    rules: [ { name: e, match: { fields: [email] }, action: { type: hash } } ]
lineage:
  namespace: ns
  transport: { type: http, config: { url: "https://lineage/api", auth: { type: bearer, config: { token: "${vault:secret/data/ol#t}" } } } }
"#;
        let mut cfg = PipelineConfig::from_text(cfg_yaml, std::path::Path::new("p.yaml")).unwrap();
        assert_eq!(scan_config(&cfg).len(), 2);
        resolve_secrets_with(&mut cfg, &set).await.unwrap();
        assert_eq!(cfg.pipeline.masking.as_ref().unwrap().key.as_deref(), Some("RESOLVED"));
        let lineage = serde_json::to_string(&cfg.lineage).unwrap();
        assert!(lineage.contains("RESOLVED") && !lineage.contains("vault"), "{lineage}");

        let stray = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
execution: { max_concurrent: 1 }
matrix:
  - id: a
    tags: ["${vault:secret/data/x#y}"]
"#;
        let mut cfg = PipelineConfig::from_text(stray, std::path::Path::new("p.yaml")).unwrap();
        let err = resolve_secrets_with(&mut cfg, &set).await.unwrap_err().to_string();
        assert!(err.contains("not resolved in"), "{err}");
        let err = resolve_secrets(&mut cfg).await.unwrap_err().to_string();
        assert!(err.contains("not resolved in"), "{err}");
    }

    #[test]
    fn field_selectors_are_refused_where_unsupported() {
        assert!(reject_field("gcp-sm", "projects/p/secrets/s/versions/1").is_ok());
        let err = reject_field("gcp-sm", "projects/p/secrets/s/versions/1#user")
            .unwrap_err()
            .to_string();
        assert!(err.contains("vault and aws-sm only"), "{err}");
    }

    #[tokio::test]
    async fn resolve_value_substitutes_any_document() {
        let mut set = ResolverSet::default();
        set.insert(Arc::new(FakeResolver {
            scheme: "vault",
            value: "tok-from-vault".into(),
        }));
        let mut doc = serde_json::json!({"principals": [{"token": "${vault:kv/app#t}"}], "n": 1});
        resolve_value_with(&mut doc, &set).await.unwrap();
        assert_eq!(doc["principals"][0]["token"], "tok-from-vault");
        let mut plain = serde_json::json!({"a": "b"});
        resolve_value(&mut plain).await.unwrap();
        assert_eq!(plain, serde_json::json!({"a": "b"}));
        let mut unknown = serde_json::json!({"a": "${no-such-scheme:x}"});
        resolve_value(&mut unknown).await.unwrap();
        let mut disabled = serde_json::json!({"a": "${azure-kv:v/s}"});
        if !cfg!(feature = "secrets-azure-kv") {
            assert!(resolve_value(&mut disabled).await.is_err());
        }
    }

    #[tokio::test]
    async fn resolve_secrets_resolves_auth_catalog_and_vars_block() {
        // A secret placed in the shared `auth:` catalog and in the `vars:` block
        // must be resolved just like one in a connector config (#134). Without
        // it, `build_auth_catalog` would receive a literal `${vault:…}` token.
        let mut set = ResolverSet::default();
        set.insert(Arc::new(FakeResolver {
            scheme: "vault",
            value: "RESOLVED".into(),
        }));
        let cfg_yaml = r#"
version: 1
vars:
  shared_token: "${vault:secret/data/app#token}"
auth:
  idp: { type: static, config: { token: "${vault:secret/data/idp#token}" } }
pipeline:
  source: { type: rest, config: { base_url: https://x, auth: { ref: idp } } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let mut cfg = PipelineConfig::from_text(cfg_yaml, std::path::Path::new("p.yaml")).unwrap();
        resolve_secrets_with(&mut cfg, &set).await.unwrap();

        let auth_token = &cfg.auth.as_ref().unwrap()["idp"]["config"]["token"];
        assert_eq!(auth_token, "RESOLVED", "auth-catalog secret should resolve");

        let var_value = &cfg.vars.as_ref().unwrap()["shared_token"];
        assert_eq!(var_value, "RESOLVED", "vars-block secret should resolve");
    }

    #[tokio::test]
    async fn scan_config_collects_refs_from_auth_and_vars() {
        // The preflight scan (used by `faucet validate`) must report secret
        // references that live only in the auth catalog or vars block.
        let cfg_yaml = r#"
version: 1
vars:
  v: "${aws-sm:prod/api#key}"
auth:
  idp: { type: static, config: { token: "${vault:secret/data/idp#token}" } }
pipeline:
  source: { type: rest, config: { base_url: https://x, auth: { ref: idp } } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let cfg = PipelineConfig::from_text(cfg_yaml, std::path::Path::new("p.yaml")).unwrap();
        let refs = scan_config(&cfg);
        assert!(refs.contains(&("vault".into(), "secret/data/idp#token".into())));
        assert!(refs.contains(&("aws-sm".into(), "prod/api#key".into())));
    }

    #[tokio::test]
    async fn resolve_secrets_errors_when_backend_not_built() {
        let set = ResolverSet::default();
        let cfg_yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { url: "${vault:secret/x}" } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let mut cfg = PipelineConfig::from_text(cfg_yaml, std::path::Path::new("p.yaml")).unwrap();
        match resolve_secrets_with(&mut cfg, &set).await.unwrap_err() {
            CliError::SecretBackendDisabled { scheme } => assert_eq!(scheme, "vault"),
            other => panic!("expected SecretBackendDisabled, got {other:?}"),
        }
    }

    #[test]
    fn make_resolver_rejects_unknown_scheme() {
        match make_resolver("not-a-scheme") {
            Err(CliError::SecretBackendDisabled { scheme }) => assert_eq!(scheme, "not-a-scheme"),
            Err(other) => panic!("expected SecretBackendDisabled, got {other:?}"),
            Ok(_) => panic!("expected SecretBackendDisabled for an unknown scheme"),
        }
    }

    #[cfg(feature = "secrets-aws-sm")]
    #[test]
    fn make_resolver_builds_compiled_in_aws_backend() {
        // The constructor is cheap (no network) — it must yield a resolver whose
        // scheme matches, proving the feature-gated arm is wired.
        let r = make_resolver("aws-sm").unwrap();
        assert_eq!(r.scheme(), "aws-sm");
    }

    #[tokio::test]
    async fn resolve_secrets_walks_matrix_row_state_dlq_and_transforms() {
        // A matrix row whose own state/dlq/transforms configs hold secrets must
        // have them resolved by the mutating visitor (the per-row branches).
        let mut set = ResolverSet::default();
        set.insert(Arc::new(FakeResolver {
            scheme: "vault",
            value: "R".into(),
        }));
        let cfg_yaml = r#"
version: 1
pipeline:
  source: { type: csv, config: { path: ./in.csv } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
matrix:
  - id: row1
    source: { config: { path: "${vault:secret/src}" } }
    sink:   { config: { path: "${vault:secret/sink}" } }
    state:  { type: file, config: { path: "${vault:secret/state}" } }
    transforms:
      - type: set
        config: { field: tag, value: "${vault:secret/tf}" }
    dlq:
      sink: { type: jsonl, config: { path: "${vault:secret/dlq}" } }
"#;
        let mut cfg = PipelineConfig::from_text(cfg_yaml, std::path::Path::new("p.yaml")).unwrap();
        resolve_secrets_with(&mut cfg, &set).await.unwrap();

        let row = &cfg.matrix[0];
        assert_eq!(
            row.source.as_ref().unwrap().config.as_ref().unwrap()["path"],
            "R"
        );
        assert_eq!(
            row.sink.as_ref().unwrap().config.as_ref().unwrap()["path"],
            "R"
        );
        assert_eq!(row.state.as_ref().unwrap().config["path"], "R");
        assert_eq!(row.transforms.as_ref().unwrap()[0].config["value"], "R");
        let dlq = row.dlq.as_ref().unwrap().as_ref().unwrap();
        assert_eq!(dlq.sink.config["path"], "R");
    }

    #[test]
    fn scan_config_collects_refs_from_matrix_row_state_and_dlq() {
        // The read-only visitor must reach the per-row state/dlq/transforms too.
        let cfg_yaml = r#"
version: 1
pipeline:
  source: { type: csv, config: { path: ./in.csv } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
matrix:
  - id: r
    state: { type: file, config: { path: "${vault:secret/state}" } }
    transforms:
      - type: set
        config: { field: t, value: "${aws-sm:tf/key}" }
    dlq:
      sink: { type: jsonl, config: { path: "${gcp-sm:projects/p/secrets/s/versions/1}" } }
"#;
        let cfg = PipelineConfig::from_text(cfg_yaml, std::path::Path::new("p.yaml")).unwrap();
        let refs = scan_config(&cfg);
        assert!(refs.contains(&("vault".into(), "secret/state".into())));
        assert!(refs.contains(&("aws-sm".into(), "tf/key".into())));
        assert!(refs.contains(&("gcp-sm".into(), "projects/p/secrets/s/versions/1".into())));
    }

    #[test]
    fn scan_config_collects_refs_from_replication_snapshot_source() {
        // A secret directive in the replication snapshot source config must be
        // discovered by the scan, just like one in `pipeline.source`.
        let cfg_yaml = r#"
version: 1
pipeline:
  source: { type: postgres-cdc, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
replication:
  mode: snapshot_then_cdc
  snapshot:
    source:
      type: postgres
      config: { connection_url: "${vault:secret/data/db#url}", query: "SELECT 1" }
"#;
        let cfg = PipelineConfig::from_text(cfg_yaml, std::path::Path::new("p.yaml")).unwrap();
        let refs = scan_config(&cfg);
        assert!(refs.contains(&("vault".into(), "secret/data/db#url".into())));
    }

    #[tokio::test]
    async fn resolve_secrets_noop_when_no_directives() {
        // No secret directives anywhere → resolve_secrets returns Ok without
        // building any resolver (the empty-refs early return).
        let cfg_yaml = r#"
version: 1
pipeline:
  source: { type: csv, config: { path: ./in.csv } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let mut cfg = PipelineConfig::from_text(cfg_yaml, std::path::Path::new("p.yaml")).unwrap();
        resolve_secrets(&mut cfg).await.unwrap();
    }

    #[test]
    fn split_field_splits_on_hash() {
        assert_eq!(split_field("a/b#c"), ("a/b", Some("c")));
        assert_eq!(split_field("a/b"), ("a/b", None));
    }

    #[test]
    fn ensure_no_secret_directives_passes_when_clean() {
        let cfg_yaml = r#"
version: 1
pipeline:
  source: { type: csv, config: { path: ./in.csv } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let cfg = PipelineConfig::from_text(cfg_yaml, std::path::Path::new("p.yaml")).unwrap();
        assert!(ensure_no_secret_directives(&cfg).is_ok());
    }

    #[test]
    fn ensure_no_secret_directives_flags_vault() {
        let cfg_yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { url: "${vault:secret/x}" } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let cfg = PipelineConfig::from_text(cfg_yaml, std::path::Path::new("p.yaml")).unwrap();
        assert!(matches!(
            ensure_no_secret_directives(&cfg),
            Err(CliError::SecretsRequireAsyncLoad)
        ));
    }

    #[test]
    fn path_scan_binds_params_so_a_required_one_does_not_break_the_prescan() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cfg.yaml");
        let mut f = std::fs::File::create(&path).unwrap();
        write!(
            f,
            r#"version: 1
name: prescan
params:
  country: {{ type: string, required: true }}
pipeline:
  source: {{ type: rest, config: {{ token: "${{vault:secret/data/app#token}}" }} }}
  sink: {{ type: jsonl, config: {{ path: "./out-${{param.country}}.jsonl" }} }}
"#
        )
        .unwrap();

        // The default (strict) scan is how `faucet validate` used to fail before
        // it bound params: a `required` param with no value aborts the pre-scan.
        assert!(scan_path_refs(&path, None).is_err());

        // Bound in placeholder mode — the way `validate` loads it — the scan
        // reaches the secret reference it exists to report.
        let inputs = crate::config::RunInputs::placeholders();
        let refs = scan_path_refs_with(&path, None, &inputs).unwrap();
        assert_eq!(
            refs.iter()
                .map(|(s, r)| (s.as_str(), r.as_str()))
                .collect::<Vec<_>>(),
            vec![("vault", "secret/data/app#token")]
        );
    }
}
