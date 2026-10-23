//! `faucet hub import` (#899): create a draft source template from an API's
//! machine-readable description.
//!
//! - [`refs`] — the spec's documents and `$ref` resolution (pure).
//! - [`openapi`] — the OpenAPI 3.x / Swagger 2.0 mapping (pure).
//! - this module — the I/O shim: fetch the spec (a file, or a URL through
//!   `faucet_core::http` under a `CallContext`), render the template and its
//!   draft suite deterministically, read both back through the same parser
//!   `faucet validate` and `faucet hub lint` use, and write them.

pub mod openapi;
pub mod refs;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;
use url::Url;

pub use openapi::{Decision, ImportOptions, ImportReport, Imported};
pub use refs::SpecSet;

use crate::error::{CliError, CliResult};
use crate::hub::spec::{SourceTemplate, check_slug};
use crate::template_tests::spec::SuiteFile;

/// Largest spec document read, local or remote.
pub const MAX_SPEC_BYTES: u64 = 32 * 1024 * 1024;
/// Most documents one spec may be split across.
pub const MAX_DOCUMENTS: usize = 64;

/// One `faucet hub import openapi` invocation.
#[derive(Debug, Clone)]
pub struct ImportRequest {
    /// Spec location: a file path or an `http(s)://` URL.
    pub spec: String,
    pub options: ImportOptions,
    /// Hub root the files are written under.
    pub out: PathBuf,
    pub dry_run: bool,
    pub force: bool,
    pub allow_remote_refs: bool,
}

/// What an import produced.
#[derive(Debug, Clone)]
pub struct ImportOutput {
    pub imported: Imported,
    pub template_path: PathBuf,
    pub suite_path: PathBuf,
    pub template_yaml: String,
    pub suite_yaml: String,
    /// Files written (empty on a dry run, or when nothing changed).
    pub written: Vec<PathBuf>,
    /// `(file, diff)` for every existing file the import changed.
    pub diffs: Vec<(PathBuf, String)>,
}

impl ImportOutput {
    /// The `--format json` document. Key order follows the rendered YAML.
    pub fn to_json(&self) -> CliResult<String> {
        #[derive(Serialize)]
        struct Out<'a> {
            template_path: String,
            suite_path: String,
            written: Vec<String>,
            report: &'a ImportReport,
            template: serde_yaml_bw::Value,
            suite: serde_yaml_bw::Value,
        }
        let out = Out {
            template_path: self.template_path.display().to_string(),
            suite_path: self.suite_path.display().to_string(),
            written: self
                .written
                .iter()
                .map(|p| p.display().to_string())
                .collect(),
            report: &self.imported.report,
            template: template_value(&self.imported.template)?,
            suite: ordered(&json_of(&self.imported.suite)?),
        };
        serde_json::to_string_pretty(&out)
            .map_err(|e| CliError::Internal(format!("hub import: rendering JSON: {e}")))
    }
}

/// Run an import: load, map, render, verify, write.
pub async fn import_openapi(req: &ImportRequest) -> CliResult<ImportOutput> {
    check_slug("--owner", &req.options.owner, true)?;
    if let Some(n) = &req.options.name {
        check_slug("--name", n, true)?;
    }
    let set = load_spec(&req.spec, req.allow_remote_refs).await?;
    let mut imported = openapi::import(&set, &req.options)?;
    let t = &imported.template;
    let typed = crate::hub::typed::source_findings(t);
    if !typed.is_empty() {
        return Err(CliError::Internal(format!(
            "hub import: the generated template does not validate: {}",
            typed.join("; ")
        )));
    }
    imported.report.lint = crate::hub::catalog::lint_source(t);
    let owner = &req.options.owner;
    let file_name = format!("{}.yaml", t.name);
    let template_yaml = render_template(t, &origin_label(&req.spec))?;
    let back = crate::hub::parse_source_text(&template_yaml, Path::new(&file_name))?;
    if &back != t {
        return Err(CliError::Internal(
            "hub import: the rendered template does not read back to what was generated".into(),
        ));
    }
    check_tokens(&template_yaml, t)?;
    let suite_rel = format!("tests/{owner}/{}/suite.yaml", t.name);
    let suite_yaml = render_suite(&imported.suite, &suite_rel)?;
    if suite_yaml.contains("${") {
        return Err(CliError::Internal(
            "hub import: the draft suite would carry an interpolation directive".into(),
        ));
    }
    SuiteFile::parse(&suite_yaml)?;
    let template_path = req
        .out
        .join(crate::hub::catalog::SOURCE_DIR)
        .join(owner)
        .join(&file_name);
    let suite_path = req.out.join(&suite_rel);
    let mut out = ImportOutput {
        imported,
        template_path,
        suite_path,
        template_yaml,
        suite_yaml,
        written: Vec::new(),
        diffs: Vec::new(),
    };
    if req.dry_run {
        return Ok(out);
    }
    let files = [
        (out.template_path.clone(), out.template_yaml.clone()),
        (out.suite_path.clone(), out.suite_yaml.clone()),
    ];
    let mut refused = Vec::new();
    let mut pending = Vec::new();
    for (path, text) in files {
        match std::fs::read_to_string(&path) {
            Ok(old) if old == text => {}
            Ok(old) => {
                let d = line_diff(&old, &text);
                if req.force {
                    out.diffs.push((path.clone(), d));
                    pending.push((path, text));
                } else {
                    refused.push(format!("{}:\n{d}", path.display()));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => pending.push((path, text)),
            Err(e) => {
                return Err(CliError::Config(format!(
                    "hub import: cannot read the existing {} to compare it ({e}); fix or remove it",
                    path.display()
                )));
            }
        }
    }
    if !refused.is_empty() {
        return Err(CliError::Config(format!(
            "hub import: refusing to overwrite changed files (a changed stream, key or write \
             preference is a new hub version); pass --force to replace them:\n{}",
            refused.join("\n")
        )));
    }
    for (path, text) in pending {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)
                .map_err(|e| CliError::Config(format!("creating {}: {e}", dir.display())))?;
        }
        std::fs::write(&path, &text)
            .map_err(|e| CliError::Config(format!("writing {}: {e}", path.display())))?;
        out.written.push(path);
    }
    Ok(out)
}

/// Every `${…}` in the rendered template must be one the importer wrote: a
/// declared param, the bind's `${bookmark}`, or a parent stream's field.
fn check_tokens(text: &str, t: &SourceTemplate) -> CliResult<()> {
    let mut allowed: BTreeSet<String> = t.params.keys().map(|k| format!("param.{k}")).collect();
    allowed.insert("bookmark".into());
    let parents: BTreeSet<&str> = t
        .streams
        .iter()
        .filter_map(|s| s.parent.as_deref())
        .collect();
    let mut rest = text;
    while let Some(i) = rest.find("${") {
        let after = &rest[i + 2..];
        let end = after.find('}').unwrap_or(after.len());
        let tok = &after[..end];
        let parent_field = tok.split_once('.').is_some_and(|(p, f)| {
            parents.contains(p)
                && !f.is_empty()
                && f.chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        });
        if !allowed.contains(tok) && !parent_field {
            return Err(CliError::Internal(format!(
                "hub import: the generated template would carry the directive `${{{tok}}}`"
            )));
        }
        rest = &after[end..];
    }
    Ok(())
}

/// How the spec is named in the generated file's header: a URL without
/// credentials, or a file's name (so the output does not depend on where the
/// import ran).
fn origin_label(spec: &str) -> String {
    origin_text(spec).replace("${", "$ {")
}

fn origin_text(spec: &str) -> String {
    if is_url(spec) {
        faucet_core::util::redact_uri_credentials(spec)
    } else {
        Path::new(spec)
            .file_name()
            .map(|f| f.to_string_lossy().into_owned())
            .unwrap_or_else(|| spec.to_string())
    }
}

fn is_url(spec: &str) -> bool {
    spec.starts_with("http://") || spec.starts_with("https://")
}

/// Where the spec lives, as a URL (`file://` for a local path).
pub fn location(spec: &str) -> CliResult<Url> {
    if is_url(spec) {
        return Url::parse(spec).map_err(|e| CliError::Config(format!("spec URL {spec}: {e}")));
    }
    let abs =
        std::fs::canonicalize(spec).map_err(|e| CliError::Config(format!("spec {spec}: {e}")))?;
    Url::from_file_path(&abs)
        .map_err(|()| CliError::Config(format!("spec {spec}: not a usable file path")))
}

/// Parse a spec document (JSON or YAML).
pub fn parse_document(text: &str, url: &Url) -> CliResult<Value> {
    let t = text.trim_start();
    if t.starts_with('{') || t.starts_with('[') {
        serde_json::from_str(t)
            .map_err(|e| CliError::Config(format!("spec {url}: invalid JSON: {e}")))
    } else {
        serde_yaml_bw::from_str(text)
            .map_err(|e| CliError::Config(format!("spec {url}: invalid YAML: {e}")))
    }
}

/// Load a spec and every document its `$ref`s name.
pub async fn load_spec(spec: &str, allow_remote_refs: bool) -> CliResult<SpecSet> {
    let root = location(spec)?;
    let fetcher = Fetcher::new()?;
    let doc = parse_document(&fetcher.read(&root).await?, &root)?;
    let mut set = SpecSet::single(root.clone(), doc);
    let mut queue = vec![root.clone()];
    while let Some(url) = queue.pop() {
        let mut found = BTreeSet::new();
        if let Some(d) = set.get(&url) {
            refs::collect_refs(d, &mut found);
        }
        for r in found {
            let Some(mut target) = refs::ref_target(&root, &url, &r, allow_remote_refs)? else {
                continue;
            };
            target.set_fragment(None);
            if !allow_remote_refs {
                contained(&root, &target, &r)?;
            }
            if set.contains(&target) {
                continue;
            }
            if set.len() >= MAX_DOCUMENTS {
                return Err(CliError::Config(format!(
                    "spec: references more than {MAX_DOCUMENTS} documents"
                )));
            }
            let doc = parse_document(&fetcher.read(&target).await?, &target)?;
            set.insert(&target, doc);
            queue.push(target);
        }
    }
    Ok(set)
}

/// A local reference must stay inside the root spec's directory once
/// symlinks are resolved.
fn contained(root: &Url, target: &Url, r: &str) -> CliResult<()> {
    if target.scheme() != "file" {
        return Ok(());
    }
    let resolve = |u: &Url| {
        u.to_file_path()
            .ok()
            .and_then(|p| std::fs::canonicalize(p).ok())
    };
    let dir = root
        .to_file_path()
        .ok()
        .and_then(|p| p.parent().and_then(|d| std::fs::canonicalize(d).ok()));
    match (dir, resolve(target)) {
        (Some(d), Some(t)) if t.starts_with(&d) => Ok(()),
        (_, None) => Err(CliError::Config(format!(
            "spec: `$ref: {r}` names a file that does not exist"
        ))),
        _ => Err(CliError::Config(format!(
            "spec: `$ref: {r}` leaves the spec's directory (pass --allow-remote-refs to follow it)"
        ))),
    }
}

/// Read at most `MAX_SPEC_BYTES` of a regular file.
fn read_local(path: &Path) -> CliResult<String> {
    use std::io::Read as _;
    let fail = |e: std::io::Error| CliError::Config(format!("spec {}: {e}", path.display()));
    let file = std::fs::File::open(path).map_err(fail)?;
    if !file.metadata().map_err(fail)?.is_file() {
        return Err(CliError::Config(format!(
            "spec {}: not a regular file",
            path.display()
        )));
    }
    let mut buf = Vec::new();
    file.take(MAX_SPEC_BYTES + 1)
        .read_to_end(&mut buf)
        .map_err(fail)?;
    if buf.len() as u64 > MAX_SPEC_BYTES {
        return Err(CliError::Config(format!(
            "spec {}: over the {MAX_SPEC_BYTES}-byte limit",
            path.display()
        )));
    }
    String::from_utf8(buf)
        .map_err(|_| CliError::Config(format!("spec {}: not UTF-8 text", path.display())))
}

/// Reads spec documents: local files directly, URLs through
/// `faucet_core::http` (size-limited, one call per document).
struct Fetcher {
    #[cfg(feature = "hub-remote")]
    client: faucet_core::http::Client,
}

impl Fetcher {
    fn new() -> CliResult<Self> {
        Ok(Self {
            #[cfg(feature = "hub-remote")]
            client: faucet_core::http::Client::builder()
                .user_agent(concat!("faucet-stream/", env!("CARGO_PKG_VERSION")))
                .max_response_bytes(Some(MAX_SPEC_BYTES))
                .build()?,
        })
    }

    async fn read(&self, url: &Url) -> CliResult<String> {
        match url.scheme() {
            "file" => {
                let path = url
                    .to_file_path()
                    .map_err(|()| CliError::Config(format!("spec {url}: not a local path")))?;
                read_local(&path)
            }
            "http" | "https" => self.fetch(url).await,
            other => Err(CliError::Config(format!(
                "spec {url}: `{other}:` locations are not supported"
            ))),
        }
    }

    #[cfg(feature = "hub-remote")]
    async fn fetch(&self, url: &Url) -> CliResult<String> {
        let ctx =
            faucet_core::io::CallContext::new().with_timeout(std::time::Duration::from_secs(60));
        let fail =
            |e: faucet_core::FaucetError| CliError::Config(format!("fetching spec {url}: {e}"));
        let resp = self
            .client
            .execute(&ctx, "spec", || self.client.get(url.as_str()))
            .await
            .map_err(fail)?;
        self.client.text(resp).await.map_err(fail)
    }

    #[cfg(not(feature = "hub-remote"))]
    async fn fetch(&self, url: &Url) -> CliResult<String> {
        Err(CliError::Config(format!(
            "spec {url}: fetching a URL needs the `hub-remote` build feature; pass a local file"
        )))
    }
}

/// Key order for rendered documents: identity first, then shape, then the
/// rest alphabetically. Maps of names (`properties`, `params`) and record
/// data (`input`, `records`) are alphabetical, so output never depends on a
/// map's iteration order.
const KEY_ORDER: &[&str] = &[
    "kind",
    "requires_faucet",
    "version",
    "template",
    "sink",
    "name",
    "type",
    "owner",
    "description",
    "docs",
    "tags",
    "params",
    "base_url",
    "path",
    "parent",
    "parent_key",
    "row",
    "source",
    "auth",
    "config",
    "records_path",
    "pagination",
    "transforms",
    "primary_keys",
    "write",
    "streams",
    "suite",
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Keys {
    Ranked,
    Names,
    Data,
}

fn rank(k: &str) -> (usize, &str) {
    (
        KEY_ORDER
            .iter()
            .position(|o| *o == k)
            .unwrap_or(KEY_ORDER.len()),
        k,
    )
}

/// `v` as YAML with the fixed key order at every structural level.
pub fn ordered(v: &Value) -> serde_yaml_bw::Value {
    ordered_as(v, Keys::Ranked)
}

fn ordered_as(v: &Value, mode: Keys) -> serde_yaml_bw::Value {
    match v {
        Value::Null => serde_yaml_bw::Value::Null,
        Value::Bool(b) => serde_yaml_bw::Value::Bool(*b),
        Value::Number(n) => match (n.as_u64(), n.as_i64(), n.as_f64()) {
            (Some(u), _, _) => serde_yaml_bw::Value::Number(u.into()),
            (_, Some(i), _) => serde_yaml_bw::Value::Number(i.into()),
            (_, _, Some(f)) => serde_yaml_bw::Value::Number(f.into()),
            _ => serde_yaml_bw::Value::String(n.to_string()),
        },
        Value::String(s) => serde_yaml_bw::Value::String(s.clone()),
        Value::Array(a) => {
            serde_yaml_bw::Value::Sequence(a.iter().map(|x| ordered_as(x, mode)).collect())
        }
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            if mode == Keys::Ranked {
                keys.sort_by(|a, b| rank(a).cmp(&rank(b)));
            } else {
                keys.sort();
            }
            let mut out = serde_yaml_bw::Mapping::new();
            for k in keys {
                let child = match (mode, k.as_str()) {
                    (Keys::Data, _) => Keys::Data,
                    (Keys::Ranked, "properties" | "params") => Keys::Names,
                    (Keys::Ranked, "input" | "records" | "example") => Keys::Data,
                    _ => Keys::Ranked,
                };
                out.insert(
                    serde_yaml_bw::Value::String(k.clone()),
                    ordered_as(&m[k], child),
                );
            }
            serde_yaml_bw::Value::Mapping(out)
        }
    }
}

fn json_of<T: Serialize>(v: &T) -> CliResult<Value> {
    serde_json::to_value(v).map_err(|e| CliError::Internal(format!("hub import: {e}")))
}

/// The template as a document, without the connector fields that only
/// restate their defaults.
fn template_value(t: &SourceTemplate) -> CliResult<serde_yaml_bw::Value> {
    let mut v = json_of(t)?;
    let prune = |spec: &mut Value| {
        if let Some(o) = spec.as_object_mut() {
            if o.get("transforms") == Some(&Value::Null) {
                o.remove("transforms");
            }
            if o.get("inherit_transforms") == Some(&Value::Bool(true)) {
                o.remove("inherit_transforms");
            }
        }
    };
    if let Some(s) = v.get_mut("source") {
        prune(s);
    }
    if let Some(Value::Object(params)) = v.get_mut("params") {
        for p in params.values_mut().filter_map(Value::as_object_mut) {
            for flag in ["required", "secret"] {
                if p.get(flag) == Some(&Value::Bool(false)) {
                    p.remove(flag);
                }
            }
        }
    }
    if let Some(Value::Object(srcs)) = v.get_mut("sources") {
        srcs.values_mut().for_each(prune);
    }
    Ok(ordered(&v))
}

fn yaml_text(v: &serde_yaml_bw::Value) -> CliResult<String> {
    serde_yaml_bw::to_string(v)
        .map_err(|e| CliError::Internal(format!("hub import: rendering YAML: {e}")))
}

/// The template file: a header naming the spec, then the document.
pub fn render_template(t: &SourceTemplate, origin: &str) -> CliResult<String> {
    Ok(format!(
        "# Generated by `faucet hub import openapi` from {origin}.\n\
         # Review the import report's open decisions before publishing.\n{}",
        yaml_text(&template_value(t)?)?
    ))
}

/// The draft suite file.
pub fn render_suite(s: &SuiteFile, rel: &str) -> CliResult<String> {
    Ok(format!(
        "# Draft `faucet template test` suite from `faucet hub import openapi`.\n\
         # Run it from the hub root: faucet template test {rel}\n{}",
        yaml_text(&ordered(&json_of(s)?))?
    ))
}

/// The human report (stderr).
pub fn render_report(out: &ImportOutput) -> String {
    let r = &out.imported.report;
    let mut s = format!(
        "imported {} (OpenAPI {}) — {} stream(s)\n",
        r.template,
        r.spec_version,
        r.streams.len()
    );
    for st in &r.streams {
        s.push_str(&format!(
            "  {:<20} {}  write: {}{}{}{}\n",
            st.name,
            st.path,
            st.write.join(", "),
            st.pagination
                .as_ref()
                .map(|p| format!("  pagination: {p}"))
                .unwrap_or_default(),
            st.parent
                .as_ref()
                .map(|p| format!("  parent: {p}"))
                .unwrap_or_default(),
            st.incremental
                .as_ref()
                .map(|k| format!("  incremental on {k}"))
                .unwrap_or_default(),
        ));
    }
    for d in &r.decisions {
        s.push_str(&format!(
            "decision: {}{} — {}{}\n",
            d.stream
                .as_ref()
                .map(|x| format!("{x}: "))
                .unwrap_or_default(),
            d.field,
            d.message,
            if d.candidates.is_empty() {
                String::new()
            } else {
                format!(" (candidates: {})", d.candidates.join(", "))
            }
        ));
    }
    for u in &r.unsupported {
        s.push_str(&format!("unsupported: {} — {}\n", u.scope, u.message));
    }
    for k in &r.skipped {
        s.push_str(&format!("skipped: {} — {}\n", k.path, k.reason));
    }
    for l in &r.lint {
        s.push_str(&format!("lint: {l}\n"));
    }
    for (p, d) in &out.diffs {
        s.push_str(&format!("changed {}:\n{d}\n", p.display()));
    }
    if out.written.is_empty() {
        s.push_str("nothing written\n");
    }
    for p in &out.written {
        s.push_str(&format!("wrote {}\n", p.display()));
    }
    if r.decisions.is_empty() && r.unsupported.is_empty() {
        s.push_str("no open decisions\n");
    } else {
        s.push_str(&format!(
            "{} open decision(s): fields left unset, or picked among several candidates — settle each from the docs or a live response\n",
            r.decisions.len() + r.unsupported.len()
        ));
    }
    s
}

/// A line diff of `old` → `new`: removed lines `-`, added lines `+`.
pub fn line_diff(old: &str, new: &str) -> String {
    let a: Vec<&str> = old.lines().collect();
    let b: Vec<&str> = new.lines().collect();
    if a.len().saturating_mul(b.len()) > 4_000_000 {
        return format!("  ({} → {} lines; too large to diff)", a.len(), b.len());
    }
    let mut lcs = vec![vec![0u32; b.len() + 1]; a.len() + 1];
    for i in (0..a.len()).rev() {
        for j in (0..b.len()).rev() {
            lcs[i][j] = if a[i] == b[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    let (mut i, mut j) = (0, 0);
    let mut out = Vec::new();
    while i < a.len() || j < b.len() {
        if i < a.len() && j < b.len() && a[i] == b[j] {
            i += 1;
            j += 1;
        } else if j < b.len() && (i == a.len() || lcs[i][j + 1] >= lcs[i + 1][j]) {
            out.push(format!("+ {}", b[j]));
            j += 1;
        } else {
            out.push(format!("- {}", a[i]));
            i += 1;
        }
    }
    out.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn diffs_show_changed_lines_only() {
        assert_eq!(line_diff("a\nb\nc", "a\nx\nc\nd"), "+ x\n- b\n+ d");
        assert_eq!(line_diff("same", "same"), "");
        let big = "x\n".repeat(2100);
        assert!(line_diff(&big, &big.replace('x', "y")).contains("too large"));
    }

    #[test]
    fn keys_render_in_a_fixed_order() {
        let v = json!({"zeta": 1, "config": {"b": -2, "a": 1.5, "type": "x"}, "kind": "k", "n": null, "t": true});
        let y = serde_yaml_bw::to_string(&ordered(&v)).unwrap();
        assert_eq!(
            y,
            "kind: k\nconfig:\n  type: x\n  a: 1.5\n  b: -2\nn: null\nt: true\nzeta: 1\n"
        );
        let names = json!({"params": {"type": {"type": "string", "default": "x"}, "a": {}}, "input": [{"name": {"type": 1, "a": 2}}]});
        assert_eq!(
            serde_yaml_bw::to_string(&ordered(&names)).unwrap(),
            "params:\n  a: {}\n  type:\n    type: string\n    default: x\ninput:\n- name:\n    a: 2\n    type: 1\n"
        );
    }

    #[test]
    fn documents_parse_as_json_or_yaml() {
        let u = Url::parse("file:///s.yaml").unwrap();
        assert_eq!(parse_document(" {\"a\": 1}", &u).unwrap(), json!({"a": 1}));
        assert_eq!(parse_document("a: 1\n", &u).unwrap(), json!({"a": 1}));
        assert!(
            parse_document("{nope", &u)
                .unwrap_err()
                .to_string()
                .contains("invalid JSON")
        );
        assert!(
            parse_document("a: [", &u)
                .unwrap_err()
                .to_string()
                .contains("invalid YAML")
        );
    }

    #[test]
    fn origins_and_locations() {
        assert_eq!(origin_label("/tmp/x/api.yaml"), "api.yaml");
        assert_eq!(
            origin_label("https://u:p@example.com/openapi.json"),
            faucet_core::util::redact_uri_credentials("https://u:p@example.com/openapi.json")
        );
        assert_eq!(origin_label(""), "");
        assert!(location("https://example.com/o.json").is_ok());
        assert!(location("https://[").is_err());
        assert!(location("/definitely/not/here.yaml").is_err());
    }

    #[tokio::test]
    async fn write_failures_are_reported() {
        let tmp = tempfile::tempdir().unwrap();
        let spec = tmp.path().join("api.json");
        std::fs::write(
            &spec,
            json!({"openapi": "3.0.0", "info": {"title": "W"}, "paths": {"/a": {"get": {"responses": {"200": {
                "content": {"application/json": {"schema": {"type": "array", "items": {"properties": {"id": {}}}}}}}}}}}})
            .to_string(),
        )
        .unwrap();
        let blocker = tmp.path().join("out");
        std::fs::write(&blocker, "a file, not a directory").unwrap();
        let req = ImportRequest {
            spec: spec.display().to_string(),
            options: ImportOptions {
                owner: "acme".into(),
                test_sink: "sink-templates/faucet-hq/sqlite.yaml".into(),
                ..Default::default()
            },
            out: blocker,
            dry_run: false,
            force: false,
            allow_remote_refs: false,
        };
        let e = import_openapi(&req).await.unwrap_err().to_string();
        assert!(e.contains("cannot read the existing"), "{e}");
        let out = import_openapi(&ImportRequest {
            dry_run: true,
            ..req
        })
        .await
        .unwrap();
        let report = render_report(&out);
        assert!(report.contains("decision: params.api_root"), "{report}");
        assert!(report.contains("open decision(s)"), "{report}");
        assert!(!SpecSet::single(Url::parse("file:///x").unwrap(), json!({})).is_empty());
    }

    fn spec_with_ref(r: &str) -> String {
        json!({"openapi": "3.0.0", "info": {"title": "R"}, "paths": {"/a": {"get": {"responses": {"200": {
            "content": {"application/json": {"schema": {"type": "array", "items": {"$ref": r}}}}}}}}}})
        .to_string()
    }

    #[tokio::test]
    async fn local_reads_are_bounded_and_contained() {
        assert!(
            read_local(Path::new("/dev/zero"))
                .unwrap_err()
                .to_string()
                .contains("not a regular file")
        );
        let tmp = tempfile::tempdir().unwrap();
        let bin = tmp.path().join("bin.yaml");
        std::fs::write(&bin, [0xff, 0xfe, 0x00]).unwrap();
        assert!(read_local(&bin).unwrap_err().to_string().contains("UTF-8"));
        let spec_dir = tmp.path().join("spec");
        std::fs::create_dir_all(&spec_dir).unwrap();
        std::fs::write(tmp.path().join("outside.yaml"), "type: object\n").unwrap();
        std::os::unix::fs::symlink(tmp.path().join("outside.yaml"), spec_dir.join("link.yaml"))
            .unwrap();
        std::fs::write(
            spec_dir.join("inside.yaml"),
            "type: object\nproperties: { id: {} }\n",
        )
        .unwrap();
        for (r, needle) in [
            ("link.yaml", "leaves the spec's directory"),
            ("../outside.yaml", "leaves the spec's directory"),
            ("/etc/hosts", "leaves the spec's directory"),
            ("missing.yaml", "does not exist"),
        ] {
            let api = spec_dir.join("api.json");
            std::fs::write(&api, spec_with_ref(r)).unwrap();
            let e = load_spec(api.to_str().unwrap(), false)
                .await
                .unwrap_err()
                .to_string();
            assert!(e.contains(needle), "{r}: {e}");
        }
        let api = spec_dir.join("api.json");
        std::fs::write(&api, spec_with_ref("inside.yaml")).unwrap();
        assert_eq!(
            load_spec(api.to_str().unwrap(), false).await.unwrap().len(),
            2
        );
        std::fs::write(&api, spec_with_ref("link.yaml")).unwrap();
        assert_eq!(
            load_spec(api.to_str().unwrap(), true).await.unwrap().len(),
            2
        );
    }

    #[test]
    fn only_importer_tokens_survive() {
        let t: SourceTemplate = serde_json::from_value(json!({
            "kind": "source-template", "name": "t",
            "params": {"api_root": {"type": "string"}},
            "source": {"type": "rest", "config": {}},
            "streams": [{"name": "p"}, {"name": "c", "parent": "p"}],
        }))
        .unwrap();
        assert!(check_tokens("${param.api_root} ${bookmark} ${p.id} ${p.customer-id}", &t).is_ok());
        for bad in [
            "${env:X}",
            "${param.other}",
            "${c.id}",
            "${p.}",
            "${p.a b}",
            "${file:/x",
        ] {
            assert!(check_tokens(bad, &t).is_err(), "{bad}");
        }
        assert_eq!(origin_label("/x/${env:A}.yaml"), "$ {env:A}.yaml");
    }

    #[tokio::test]
    async fn an_unreadable_existing_file_is_not_overwritten() {
        let tmp = tempfile::tempdir().unwrap();
        let spec = tmp.path().join("api.json");
        std::fs::write(
            &spec,
            spec_with_ref("#/components/schemas/T").replace(
                "\"paths\"",
                "\"components\":{\"schemas\":{\"T\":{\"properties\":{\"id\":{}}}}},\"paths\"",
            ),
        )
        .unwrap();
        let out = tmp.path().join("hub");
        std::fs::create_dir_all(out.join("source-templates/acme/r.yaml")).unwrap();
        let req = ImportRequest {
            spec: spec.display().to_string(),
            options: ImportOptions {
                owner: "acme".into(),
                test_sink: "sink-templates/faucet-hq/sqlite.yaml".into(),
                ..Default::default()
            },
            out,
            dry_run: false,
            force: true,
            allow_remote_refs: false,
        };
        let e = import_openapi(&req).await.unwrap_err().to_string();
        assert!(e.contains("cannot read the existing"), "{e}");
    }

    #[tokio::test]
    async fn unsupported_schemes_and_limits_are_typed() {
        let f = Fetcher::new().unwrap();
        let e = f
            .read(&Url::parse("ftp://example.com/x").unwrap())
            .await
            .unwrap_err();
        assert!(e.to_string().contains("not supported"), "{e}");
        let e = f
            .read(&Url::parse("file:///definitely/not/here").unwrap())
            .await
            .unwrap_err();
        assert!(e.to_string().contains("here"), "{e}");
    }
}
