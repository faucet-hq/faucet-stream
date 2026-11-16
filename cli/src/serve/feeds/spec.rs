//! The feed definition's rules and the `--feeds` file (#1034). The types
//! themselves live with the run history (`history::feeds`), which stores them.

pub use crate::serve::history::feeds::{
    DEFAULT_STORE, DedupeSpec, DeliverySpec, DeliveryTemplate, FeedSpec, FlushSpec, IngestAuth,
    IngestSpec, MAX_DELIVERIES, MAX_NAME_LEN, MaxBatch, OnDuplicate, RateLimit, RetentionSpec,
    Size, Span, StartSpec, TokenMode, UndeliveredSpec,
};

use super::Source;
use crate::error::{CliError, CliResult};
use crate::serve::history::tenants::validate_slug;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// The `--feeds` file.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FeedsFile {
    /// Schema version; must be `1`.
    pub version: u32,
    /// The feeds. Names are unique per tenant.
    #[serde(default)]
    pub feeds: Vec<FeedSpec>,
}

/// The run labels a delivery run carries; a delivery may not set them.
pub const FEED_LABELS: &[&str] = &["feed", "feed_delivery", "feed_claim"];

fn address_ok(text: &str) -> bool {
    let (ip, prefix) = match text.split_once('/') {
        Some((ip, prefix)) => (ip, Some(prefix)),
        None => (text, None),
    };
    let Ok(ip) = ip.parse::<std::net::IpAddr>() else {
        return false;
    };
    let max = if ip.is_ipv4() { 32 } else { 128 };
    prefix.is_none_or(|p| p.parse::<u8>().is_ok_and(|p| p <= max))
}

fn version_ok(v: &Option<String>) -> Result<(), String> {
    match v {
        None => Ok(()),
        Some(s) => serde_json::from_value::<crate::serve::history::templates::VersionSelector>(
            serde_json::Value::String(s.clone()),
        )
        .map(|_| ())
        .map_err(|e| format!("'{s}': {e}")),
    }
}

fn check_delivery(d: &DeliverySpec, source: Source, problems: &mut Vec<String>) {
    let at = format!("delivery '{}'", d.name);
    let mut bad = |m: String| problems.push(format!("{at}: {m}"));
    if let Err(e) = validate_slug(&d.name, "name", MAX_NAME_LEN) {
        bad(e);
    }
    match (&d.template, &d.pipeline) {
        (Some(_), Some(_)) => bad("set `template` or `pipeline`, not both".into()),
        (None, None) => bad("set `template` (a registered template) or `pipeline`".into()),
        (None, Some(_)) if source == Source::Console => bad(
            "`pipeline` (a file on the server) is only for a delivery in the --feeds file; \
             name a registered `template`"
                .into(),
        ),
        (None, Some(p)) if p.trim().is_empty() => bad("`pipeline` must not be empty".into()),
        (Some(t), None) => {
            if t.id.trim().is_empty() {
                bad("template.id must not be empty".into());
            }
            for (field, v) in [
                ("template.version", &t.version),
                ("template.sink_version", &t.sink_version),
                ("template.overlay_version", &t.overlay_version),
            ] {
                if let Err(e) = version_ok(v) {
                    bad(format!("{field} {e}"));
                }
            }
            if t.sink.is_none() && t.sink_version.is_some() {
                bad("template.sink_version needs template.sink".into());
            }
            if t.overlay.is_none() && t.overlay_version.is_some() {
                bad("template.overlay_version needs template.overlay".into());
            }
        }
        (None, Some(_)) => {}
    }
    for (field, zero) in [
        ("flush.max_events", d.flush.max_events == 0),
        ("flush.max_bytes", d.flush.max_bytes.bytes() == 0),
        ("flush.max_age", d.flush.max_age.secs() == 0),
        ("max_batch.events", d.max_batch.events == 0),
        ("max_batch.bytes", d.max_batch.bytes.bytes() == 0),
        ("retries", d.retries == 0),
        ("timeout_secs", d.timeout_secs == 0),
    ] {
        if zero {
            bad(format!("{field} must be greater than 0"));
        }
    }
    if let Some(k) = d.labels.keys().find(|k| FEED_LABELS.contains(&k.as_str())) {
        bad(format!(
            "label '{k}' is set by the server; choose another key"
        ));
    }
    if let Err(e) = crate::serve::history::labels::validate(&d.labels, &[]) {
        bad(e);
    }
    if let Some(pool) = &d.pool
        && let Err(e) = crate::serve::pools::resolve(Some(pool), None)
    {
        bad(format!("pool: {}", e.api_error().error.message));
    }
}

/// Every problem with `spec`, one line each; empty when it is valid.
/// `source` says where the definition comes from: only a `--feeds` file
/// delivery may name a `pipeline` file.
pub fn problems(spec: &FeedSpec, source: Source) -> Vec<String> {
    let mut out = Vec::new();
    if let Err(e) = validate_slug(&spec.name, "name", MAX_NAME_LEN) {
        out.push(e);
    }
    if let Some(t) = &spec.tenant
        && let Err(e) = validate_slug(t, "tenant", MAX_NAME_LEN)
    {
        out.push(e);
    }
    if let Err(e) = validate_slug(&spec.store, "store", MAX_NAME_LEN) {
        out.push(e);
    }
    let ingest = &spec.ingest;
    if !ingest.auth.has_credential() {
        out.push(
            "ingest.auth: a feed needs a credential — `tokens: managed`, a `signature`, or both"
                .into(),
        );
    }
    if let Some(sig) = &ingest.auth.signature
        && !sig.is_complete()
    {
        out.push("ingest.auth.signature: `header` and `secret` must not be empty".into());
    }
    for a in &ingest.auth.allowed_addresses {
        if !address_ok(a) {
            out.push(format!(
                "ingest.auth.allowed_addresses: '{a}' is not an address or a CIDR range"
            ));
        }
    }
    if ingest.accept.is_empty() || ingest.accept.iter().any(|a| a.trim().is_empty()) {
        out.push("ingest.accept must list at least one content type, none of them empty".into());
    }
    if ingest.max_event_bytes.bytes() == 0 {
        out.push("ingest.max_event_bytes must be greater than 0".into());
    }
    if ingest.max_request_bytes < ingest.max_event_bytes {
        out.push("ingest.max_request_bytes must be at least ingest.max_event_bytes".into());
    }
    if let Some(d) = &ingest.dedupe {
        match (&d.header, &d.body_path) {
            (Some(_), Some(_)) | (None, None) => {
                out.push("ingest.dedupe: set exactly one of `header` and `body_path`".into())
            }
            (Some(k), None) | (None, Some(k)) if k.trim().is_empty() => {
                out.push("ingest.dedupe: the key must not be empty".into())
            }
            _ => {}
        }
        if d.window.secs() == 0 {
            out.push("ingest.dedupe.window must be greater than 0".into());
        }
    }
    if let Some(r) = &ingest.rate_limit
        && (r.events_per_sec == 0 || r.burst == Some(0))
    {
        out.push("ingest.rate_limit: `events_per_sec` and `burst` must be greater than 0".into());
    }
    let r = &spec.retention;
    if r.undelivered.max_age.secs() == 0 || r.undelivered.max_bytes.bytes() == 0 {
        out.push("retention.undelivered: `max_age` and `max_bytes` must be greater than 0".into());
    }
    if r.warn_at.iter().any(|w| !(*w > 0.0 && *w < 1.0))
        || r.warn_at.windows(2).any(|w| w[0] >= w[1])
    {
        out.push("retention.warn_at must be ascending fractions between 0 and 1".into());
    }
    if spec.deliveries.len() > MAX_DELIVERIES {
        out.push(format!(
            "a feed has at most {MAX_DELIVERIES} deliveries; this one has {}",
            spec.deliveries.len()
        ));
    }
    let mut names = std::collections::BTreeSet::new();
    for d in &spec.deliveries {
        if !names.insert(d.name.as_str()) {
            out.push(format!("duplicate delivery name '{}'", d.name));
        }
        check_delivery(d, source, &mut out);
    }
    out
}

/// Validate `spec`; the error lists every problem.
pub fn validate(spec: &FeedSpec, source: Source) -> Result<(), String> {
    let all = problems(spec, source);
    if all.is_empty() {
        Ok(())
    } else {
        Err(all.join("; "))
    }
}

/// Validate a parsed file. `Err` is user-facing and lists every problem.
pub fn check_file(file: &FeedsFile) -> Result<(), String> {
    if file.version != 1 {
        return Err(format!(
            "feeds: unsupported version {} (expected 1)",
            file.version
        ));
    }
    let mut all = Vec::new();
    let mut keys = std::collections::BTreeSet::new();
    for f in &file.feeds {
        for p in problems(f, Source::File) {
            all.push(format!("'{}': {p}", f.name));
        }
        if !keys.insert((f.tenant.as_deref(), f.name.as_str())) {
            all.push(format!("duplicate feed name '{}'", f.name));
        }
    }
    if all.is_empty() {
        Ok(())
    } else {
        Err(format!("feeds: {}", all.join("; ")))
    }
}

/// A loaded `--feeds` file: its feeds and the directory relative `pipeline`
/// paths resolve against.
#[derive(Debug, Clone, Default)]
pub struct LoadedFile {
    pub feeds: Vec<FeedSpec>,
    pub dir: PathBuf,
}

impl LoadedFile {
    /// The file of a delivery's `pipeline`, resolved against the feeds file.
    pub fn pipeline_path(&self, pipeline: &str) -> PathBuf {
        let p = Path::new(pipeline);
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            self.dir.join(p)
        }
    }
}

/// Load + validate a `--feeds` file (YAML or JSON). `${env:}`, `${file:}` and
/// `${secret:}` references are resolved; the file, every `pipeline` file it
/// names and its signature secrets are protected from submitted configs.
pub async fn load_file(path: &Path) -> CliResult<LoadedFile> {
    let text = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| CliError::Serve(format!("reading feeds file {}: {e}", path.display())))?;
    crate::serve::load::protect_file(path);
    let is_json = path
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("json"));
    let mut doc: serde_json::Value = if is_json {
        serde_json::from_str(&text)
            .map_err(|e| CliError::Serve(format!("parsing feeds JSON: {e}")))?
    } else {
        crate::yaml::from_str(&text)
            .map_err(|e| CliError::Serve(format!("parsing feeds YAML: {e}")))?
    };
    crate::serve::load::protect_references(&doc);
    crate::interpolate::interpolate_value(&mut doc)
        .map_err(|e| CliError::Serve(format!("feeds file {}: {e}", path.display())))?;
    let file: FeedsFile = serde_json::from_value(doc)
        .map_err(|e| CliError::Serve(format!("parsing feeds file {}: {e}", path.display())))?;
    check_file(&file).map_err(CliError::Serve)?;
    let loaded = LoadedFile {
        feeds: file.feeds,
        dir: path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from(".")),
    };
    for f in &loaded.feeds {
        if let Some(sig) = &f.ingest.auth.signature {
            crate::secrets::registry::register_server(&sig.secret);
        }
        for d in &f.deliveries {
            if let Some(p) = &d.pipeline {
                let file = loaded.pipeline_path(p);
                if !file.is_file() {
                    return Err(CliError::Serve(format!(
                        "feeds: '{}': delivery '{}': pipeline file {} does not exist",
                        f.name,
                        d.name,
                        file.display()
                    )));
                }
                crate::serve::load::protect_file(&file);
            }
        }
    }
    Ok(loaded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn feed(v: serde_json::Value) -> FeedSpec {
        serde_json::from_value(v).unwrap()
    }

    fn good() -> serde_json::Value {
        json!({
            "name": "orders",
            "ingest": {"auth": {"tokens": "managed"}},
            "deliveries": [{"name": "lake", "template": {"id": "acme/orders", "version": 3}}]
        })
    }

    #[test]
    fn a_good_feed_has_no_problems() {
        assert!(problems(&feed(good()), Source::Console).is_empty());
        validate(&feed(good()), Source::File).unwrap();
        let mut v = good();
        v["ingest"]["auth"] = json!({
            "signature": {"header": "X-Signature", "secret": "s"},
            "allowed_addresses": ["10.0.0.0/8", "192.0.2.7", "2001:db8::/32"]
        });
        v["ingest"]["dedupe"] = json!({"header": "X-Event-Id", "window": "1h"});
        v["ingest"]["rate_limit"] = json!({"events_per_sec": 10, "burst": 50});
        v["deliveries"][0]["pool"] = json!("big");
        assert_eq!(problems(&feed(v), Source::Console), Vec::<String>::new());
    }

    #[test]
    fn every_problem_is_reported_at_once() {
        let v = json!({
            "name": "Bad Name",
            "tenant": "ACME",
            "store": "",
            "ingest": {
                "auth": {"allowed_addresses": ["nowhere", "10.0.0.0/40"]},
                "accept": [],
                "max_event_bytes": 0,
                "dedupe": {"window": 0},
                "rate_limit": {"events_per_sec": 0}
            },
            "retention": {"undelivered": {"max_age": 0}, "warn_at": [0.9, 0.5, 1.5]},
            "deliveries": [
                {"name": "a", "template": {"id": "t"}, "pipeline": "p.yaml"},
                {"name": "a"},
                {"name": "b", "pipeline": "p.yaml"},
                {"name": "c", "template": {"id": " ", "version": "latest", "sink_version": 1, "overlay_version": 2},
                 "flush": {"max_events": 0, "max_bytes": 0, "max_age": 0},
                 "max_batch": {"events": 0, "bytes": 0}, "retries": 0, "timeout_secs": 0,
                 "labels": {"feed": "x", "a:b": "y"}, "pool": "Not A Pool"}
            ]
        });
        let all = problems(&feed(v), Source::Console).join("\n");
        for want in [
            "name 'Bad Name'",
            "tenant 'ACME'",
            "store must not be empty",
            "a feed needs a credential",
            "'nowhere' is not an address",
            "'10.0.0.0/40' is not an address",
            "ingest.accept must list",
            "ingest.max_event_bytes must be greater than 0",
            "ingest.dedupe: set exactly one",
            "ingest.dedupe.window must be greater than 0",
            "ingest.rate_limit",
            "retention.undelivered",
            "retention.warn_at",
            "duplicate delivery name 'a'",
            "delivery 'a': set `template` or `pipeline`, not both",
            "delivery 'a': set `template` (a registered template) or `pipeline`",
            "delivery 'b': `pipeline` (a file on the server) is only for",
            "delivery 'c': template.id must not be empty",
            "delivery 'c': template.version 'latest'",
            "delivery 'c': template.sink_version needs template.sink",
            "delivery 'c': template.overlay_version needs template.overlay",
            "delivery 'c': flush.max_events must be greater than 0",
            "delivery 'c': flush.max_bytes must be greater than 0",
            "delivery 'c': flush.max_age must be greater than 0",
            "delivery 'c': max_batch.events must be greater than 0",
            "delivery 'c': max_batch.bytes must be greater than 0",
            "delivery 'c': retries must be greater than 0",
            "delivery 'c': timeout_secs must be greater than 0",
            "delivery 'c': label 'feed' is set by the server",
            "delivery 'c': run label key 'a:b'",
            "delivery 'c': pool:",
        ] {
            assert!(all.contains(want), "missing {want:?} in:\n{all}");
        }
        assert!(validate(&feed(good()), Source::Console).is_ok());

        let mut v = good();
        v["ingest"]["auth"] = json!({"signature": {"header": "", "secret": "s"}});
        v["ingest"]["max_request_bytes"] = json!(10);
        v["ingest"]["dedupe"] = json!({"header": "h", "body_path": "id"});
        v["ingest"]["rate_limit"] = json!({"events_per_sec": 5, "burst": 0});
        v["deliveries"] = json!(
            (0..17)
                .map(|i| json!({"name": format!("d{i}"), "template": {"id": "t"}}))
                .collect::<Vec<_>>()
        );
        let all = validate(&feed(v), Source::Console).unwrap_err();
        for want in [
            "`header` and `secret` must not be empty",
            "max_request_bytes must be at least",
            "set exactly one",
            "ingest.rate_limit",
            "at most 16 deliveries; this one has 17",
        ] {
            assert!(all.contains(want), "missing {want:?} in:\n{all}");
        }
        let mut v = good();
        v["ingest"]["dedupe"] = json!({"body_path": " "});
        v["deliveries"] = json!([{"name": "p", "pipeline": " "}]);
        let all = validate(&feed(v), Source::File).unwrap_err();
        assert!(all.contains("the key must not be empty"), "{all}");
        assert!(all.contains("`pipeline` must not be empty"), "{all}");
    }

    #[tokio::test]
    async fn a_file_loads_resolves_references_and_names_every_problem() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("p.yaml"), "version: 1\n").unwrap();
        std::fs::write(dir.path().join("secret.txt"), "hmac-secret-from-file").unwrap();
        let ok = dir.path().join("feeds.yaml");
        std::fs::write(
            &ok,
            format!(
                "version: 1\nfeeds:\n  - name: orders\n    ingest:\n      auth:\n        signature: {{ header: X-Sig, secret: \"${{file:{}}}\" }}\n    deliveries:\n      - name: lake\n        pipeline: p.yaml\n        flush: {{ max_age: 5s }}\n  - name: orders\n    tenant: acme\n    ingest: {{ auth: {{ tokens: managed }} }}\n",
                dir.path().join("secret.txt").display()
            ),
        )
        .unwrap();
        let f = load_file(&ok).await.unwrap();
        assert_eq!(f.feeds.len(), 2);
        assert_eq!(
            f.feeds[0].ingest.auth.signature.as_ref().unwrap().secret,
            "hmac-secret-from-file"
        );
        assert_eq!(f.feeds[0].deliveries[0].flush.max_age, Span(5));
        assert_eq!(f.pipeline_path("p.yaml"), dir.path().join("p.yaml"));
        assert_eq!(f.pipeline_path("/abs/p.yaml"), PathBuf::from("/abs/p.yaml"));
        assert!(f.feeds[1].sealed());

        let json = dir.path().join("feeds.json");
        std::fs::write(&json, r#"{"version": 1}"#).unwrap();
        assert!(load_file(&json).await.unwrap().feeds.is_empty());

        let cases = [
            ("version: 2\nfeeds: []\n", "unsupported version 2"),
            (
                "version: 1\nfeeds:\n  - {name: a}\n  - {name: a}\n",
                "'a': ingest.auth: a feed needs a credential",
            ),
            (
                "version: 1\nfeeds:\n  - {name: a, ingest: {auth: {tokens: managed}}}\n  - {name: a, ingest: {auth: {tokens: managed}}}\n",
                "duplicate feed name 'a'",
            ),
            (
                "version: 1\nfeeds:\n  - name: a\n    ingest: {auth: {tokens: managed}}\n    deliveries: [{name: d, pipeline: missing.yaml}]\n",
                "missing.yaml does not exist",
            ),
            ("version: 1\nfeed: []\n", "parsing feeds file"),
            ("version: [\n", "parsing feeds YAML"),
            (
                "version: 1\nfeeds:\n  - name: a\n    ingest: {auth: {signature: {header: h, secret: \"${file:/nonexistent/faucet-feed-secret}\"}}}\n",
                "feeds file",
            ),
        ];
        for (text, want) in cases {
            let p = dir.path().join("bad.yaml");
            std::fs::write(&p, text).unwrap();
            let err = load_file(&p).await.unwrap_err().to_string();
            assert!(err.contains(want), "{want}: {err}");
        }
        let bad_json = dir.path().join("bad.json");
        std::fs::write(&bad_json, "{").unwrap();
        assert!(
            load_file(&bad_json)
                .await
                .unwrap_err()
                .to_string()
                .contains("parsing feeds JSON")
        );
        assert!(
            load_file(&dir.path().join("missing.yaml"))
                .await
                .unwrap_err()
                .to_string()
                .contains("reading feeds file")
        );
    }

    #[tokio::test]
    async fn the_shipped_example_loads() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/feeds");
        let text = std::fs::read_to_string(dir.join("feeds.yaml")).unwrap();
        // The example names its HMAC secret by reference; give it a value.
        let resolved = text.replace("${env:ORDERS_HMAC_SECRET}", "example-secret");
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("feeds.yaml"), resolved).unwrap();
        std::fs::copy(
            dir.join("orders_to_jsonl.yaml"),
            tmp.path().join("orders_to_jsonl.yaml"),
        )
        .unwrap();
        let f = load_file(&tmp.path().join("feeds.yaml")).await.unwrap();
        assert_eq!(f.feeds.len(), 2);
        assert_eq!(f.feeds[0].deliveries[0].name, "archive");
        assert!(f.feeds[1].ingest.url_token);
        let pipeline: serde_json::Value = crate::yaml::from_str(
            &std::fs::read_to_string(dir.join("orders_to_jsonl.yaml")).unwrap(),
        )
        .unwrap();
        assert_eq!(pipeline["pipeline"]["source"]["type"], "feed");
    }

    #[test]
    fn the_file_schema_is_derivable() {
        let schema = serde_json::to_value(schemars::schema_for!(FeedsFile)).unwrap();
        assert!(schema.to_string().contains("deliveries"));
    }
}
