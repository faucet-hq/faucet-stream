//! OpenAPI 3.x / Swagger 2.0 → `source-template` mapping (#899).
//!
//! Pure: the documents arrive as a [`SpecSet`]; the template, a draft test
//! suite and the import report come out. Every connector field is built from
//! the REST source's own config types and serialized, never spelled by hand
//! as YAML. Where the spec does not settle a choice (two candidate cursor
//! parameters, no detectable key), the field is left unset and the choice is
//! listed as a [`Decision`]; nothing is guessed.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap};

use faucet_core::{
    BackoffFrom, BindFormat, BindTarget, BindValueType, ReplicationBind, ReplicationMethod,
    RetryMatcher, WaitUnit, WriteMode,
};
use faucet_source_rest::{Auth, PaginationStyle};
use serde::Serialize;
use serde_json::{Map, Value, json};

use super::refs::SpecSet;
use crate::config::{ConnectorSpec, TransformSpec};
use crate::error::{CliError, CliResult};
use crate::hub::spec::{SourceTemplate, Stream, StreamSource, TemplateKind, WriteChoice};
use crate::params::{ParamSpec, ParamType, ParamsSpec};
use crate::template_tests::spec::{Auto, Behavioral, Case, Expect, Suite, SuiteFile};

/// More list operations than this need `--include`, so a spec with thousands
/// of endpoints never turns into a template nobody reviewed.
pub const MAX_OPERATIONS: usize = 200;
/// Example records carried into a behavioural case per stream.
const MAX_EXAMPLE_RECORDS: usize = 20;
/// Deepest `allOf` / `oneOf` / `anyOf` nesting followed.
const MAX_SCHEMA_DEPTH: usize = 32;

/// The param every stream's `base_url` reads.
pub const API_ROOT: &str = "api_root";

/// What the caller chose on the command line.
#[derive(Debug, Clone, Default)]
pub struct ImportOptions {
    /// Publisher namespace (`owner:` and the directory).
    pub owner: String,
    /// Template name; default: a slug of `info.title`.
    pub name: Option<String>,
    /// Path globs an operation must match (any of them).
    pub include: Vec<String>,
    /// Path globs that drop an operation.
    pub exclude: Vec<String>,
    /// `servers[]` index or URL for the `api_root` default.
    pub server: Option<String>,
    /// Sink template the draft suite composes with (a path from the hub root).
    pub test_sink: String,
}

/// One open choice the spec did not settle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Decision {
    /// The stream it concerns; `None` for the template as a whole.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
    /// The template field left unset (`primary_keys`, `pagination`, …).
    pub field: String,
    pub message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<String>,
}

/// A spec feature the REST source cannot express.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Finding {
    pub scope: String,
    pub message: String,
}

/// An operation that did not become a stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Skipped {
    pub path: String,
    pub reason: String,
}

/// What became of one stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamSummary {
    pub name: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<String>,
    pub primary_keys: Vec<String>,
    pub write: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incremental: Option<String>,
    pub example_records: usize,
}

/// The import report: the human checklist on stderr, or `report` in
/// `--format json`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ImportReport {
    pub spec_version: String,
    pub template: String,
    pub servers: Vec<String>,
    pub streams: Vec<StreamSummary>,
    pub decisions: Vec<Decision>,
    pub unsupported: Vec<Finding>,
    pub skipped: Vec<Skipped>,
    /// `faucet hub lint` findings on the generated template.
    pub lint: Vec<String>,
}

/// The import result.
#[derive(Debug, Clone)]
pub struct Imported {
    pub template: SourceTemplate,
    pub suite: SuiteFile,
    pub report: ImportReport,
}

#[derive(Debug, Clone)]
struct Loc {
    base: String,
    v: Value,
}

/// A schema with its `$ref`s followed and `allOf` / `oneOf` / `anyOf`
/// merged — the parts the mapping reads.
#[derive(Debug, Clone, Default)]
struct Flat {
    types: BTreeSet<String>,
    nullable: bool,
    format: Option<String>,
    props: BTreeMap<String, Loc>,
    required: BTreeSet<String>,
    items: Option<Loc>,
    xpk: Option<Value>,
    example: Option<Value>,
    description: Option<String>,
    default: Option<Value>,
    minimum: Option<Value>,
    maximum: Option<Value>,
}

impl Flat {
    fn is_array(&self) -> bool {
        self.types.contains("array")
    }
    fn is_object(&self) -> bool {
        self.types.contains("object") || !self.props.is_empty()
    }
    fn is_string(&self) -> bool {
        self.types.is_empty() || self.types.contains("string")
    }
    fn is_integer(&self) -> bool {
        self.types.contains("integer")
    }
    fn uri_like(&self) -> bool {
        matches!(
            self.format.as_deref(),
            Some("uri" | "url" | "uri-reference" | "iri" | "iri-reference")
        )
    }
    fn date_time(&self) -> bool {
        matches!(self.format.as_deref(), Some("date-time" | "datetime"))
    }
}

fn merge(into: &mut Flat, g: Flat) {
    into.types.extend(g.types);
    into.nullable |= g.nullable;
    into.format = into.format.take().or(g.format);
    for (k, v) in g.props {
        into.props.entry(k).or_insert(v);
    }
    into.required.extend(g.required);
    into.items = into.items.take().or(g.items);
    into.xpk = into.xpk.take().or(g.xpk);
    into.example = into.example.take().or(g.example);
    into.description = into.description.take().or(g.description);
    into.default = into.default.take().or(g.default);
    into.minimum = into.minimum.take().or(g.minimum);
    into.maximum = into.maximum.take().or(g.maximum);
}

#[derive(Debug, Clone)]
struct Param {
    name: String,
    location: String,
    required: bool,
    schema: Flat,
}

#[derive(Debug, Clone)]
enum Records {
    Body(Loc),
    At(Vec<String>, Loc, Vec<String>),
    NotList,
    Ambiguous(Vec<String>),
}

/// One `GET` operation that returns a list.
struct Candidate {
    path: String,
    summary: Option<String>,
    params: Vec<Param>,
    records: Option<Vec<String>>,
    records_ambiguous: Vec<String>,
    records_others: Vec<String>,
    item: Flat,
    envelope: Flat,
    headers: BTreeMap<String, Value>,
    limit_headers: BTreeMap<String, Value>,
    responses: BTreeMap<String, Loc>,
    example: Option<Value>,
    security: Vec<BTreeMap<String, Vec<String>>>,
    path_param: Option<String>,
    parent: Option<(usize, String)>,
    name: String,
}

struct Mapper<'a> {
    set: &'a SpecSet,
    v2: bool,
    root_key: String,
    report: ImportReport,
    flat_cache: RefCell<HashMap<(String, String), Flat>>,
    flat_calls: Cell<usize>,
    tainted: RefCell<BTreeSet<String>>,
}

/// Most schema nodes one import may flatten; a spec past this is refused
/// rather than left to run for hours.
const MAX_FLATTEN_NODES: usize = 200_000;

/// A spec-derived string that would become an interpolation directive
/// (`${env:…}`, `${file:…}`) in the generated template.
fn tainted(s: &str) -> bool {
    s.contains("${")
}

/// Import one spec.
pub fn import(set: &SpecSet, opts: &ImportOptions) -> CliResult<Imported> {
    let root = set.root();
    if !root.is_object() {
        return Err(not_a_spec());
    }
    let (v2, version) = spec_version(root)?;
    let mut m = Mapper {
        set,
        v2,
        root_key: set.root_key(),
        report: ImportReport {
            spec_version: version,
            ..Default::default()
        },
        flat_cache: RefCell::new(HashMap::new()),
        flat_calls: Cell::new(0),
        tainted: RefCell::new(BTreeSet::new()),
    };
    m.run(opts)
}

fn not_a_spec() -> CliError {
    CliError::Config(
        "spec: not an OpenAPI 3.x or Swagger 2.0 document (no top-level `openapi:` or \
         `swagger:` field) — is the URL an HTML page rather than the spec?"
            .into(),
    )
}

fn spec_version(root: &Value) -> CliResult<(bool, String)> {
    if let Some(v) = root.get("openapi") {
        let s = scalar_text(v);
        if s.starts_with("3.") {
            return Ok((false, s));
        }
        return Err(CliError::Config(format!(
            "spec: OpenAPI {s} is not supported (3.x and Swagger 2.0 are)"
        )));
    }
    if let Some(v) = root.get("swagger") {
        let s = scalar_text(v);
        if s == "2.0" || s == "2" {
            return Ok((true, "2.0".into()));
        }
        return Err(CliError::Config(format!(
            "spec: Swagger {s} is not supported (3.x and Swagger 2.0 are)"
        )));
    }
    Err(not_a_spec())
}

fn scalar_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn norm(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// `snake_case` slug (stream names); `sep` `-` for template names.
pub fn slug(raw: &str, sep: char) -> String {
    let mut out = String::new();
    let mut prev: Option<char> = None;
    for c in raw.chars() {
        if c.is_ascii_alphanumeric() {
            if c.is_ascii_uppercase()
                && prev.is_some_and(|p| p.is_ascii_lowercase() || p.is_ascii_digit())
            {
                out.push(sep);
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(sep);
        }
        prev = Some(c);
    }
    let mut collapsed = String::new();
    for c in out.chars() {
        if c == sep && (collapsed.is_empty() || collapsed.ends_with(sep)) {
            continue;
        }
        collapsed.push(c);
    }
    let trimmed = collapsed.trim_end_matches(sep);
    trimmed
        .chars()
        .take(64)
        .collect::<String>()
        .trim_end_matches(sep)
        .to_string()
}

fn ident(s: &str) -> bool {
    let mut c = s.chars();
    c.next()
        .is_some_and(|f| f.is_ascii_alphabetic() || f == '_')
        && c.all(|x| x.is_ascii_alphanumeric() || x == '_')
}

/// A JSONPath for a list of object keys; `all` appends `[*]`.
pub fn jsonpath(segs: &[String], all: bool) -> String {
    let mut out = String::from("$");
    for s in segs {
        if ident(s) {
            out.push('.');
            out.push_str(s);
        } else {
            out.push_str("['");
            out.push_str(&s.replace('\\', "\\\\").replace('\'', "\\'"));
            out.push_str("']");
        }
    }
    if all {
        out.push_str("[*]");
    }
    out
}

fn path_params(path: &str) -> Vec<String> {
    path.split('/')
        .filter_map(|s| s.strip_prefix('{').and_then(|s| s.strip_suffix('}')))
        .map(str::to_string)
        .collect()
}

fn static_segments(path: &str) -> Vec<String> {
    path.split('/')
        .filter(|s| !s.is_empty() && !s.starts_with('{'))
        .map(|s| slug(s, '_'))
        .filter(|s| !s.is_empty())
        .collect()
}

const WRAPPERS: &[&str] = &[
    "data",
    "items",
    "results",
    "records",
    "entries",
    "values",
    "value",
    "elements",
    "rows",
    "list",
    "objects",
    "resources",
];
const CURSOR_PARAMS: &[&str] = &[
    "cursor",
    "startingafter",
    "pagetoken",
    "nexttoken",
    "after",
    "continuation",
    "continuationtoken",
    "marker",
    "nextcursor",
    "pagecursor",
];
const CURSOR_FIELDS: &[&str] = &[
    "nextcursor",
    "cursor",
    "nextpagetoken",
    "nexttoken",
    "next",
    "after",
    "endcursor",
    "continuationtoken",
    "nextmarker",
    "marker",
    "nextpagecursor",
];
const NEXT_LINK_FIELDS: &[&str] = &[
    "nextlink",
    "nexturl",
    "nextpageurl",
    "next",
    "odatanextlink",
    "nexthref",
    "nextpage",
];
const PAGE_PARAMS: &[&str] = &["page", "pagenumber", "pageno", "pagenum", "pageindex"];
const SIZE_PARAMS: &[&str] = &[
    "perpage",
    "pagesize",
    "limit",
    "size",
    "maxresults",
    "pagelimit",
    "top",
];
const OFFSET_PARAMS: &[&str] = &["offset", "skip", "start", "startindex"];
const LIMIT_PARAMS: &[&str] = &[
    "limit",
    "top",
    "count",
    "maxresults",
    "pagesize",
    "perpage",
    "size",
];
/// Integer fields that are a total in some APIs and the page's own size in
/// others: never used as `total_path` on their own.
const MAYBE_TOTAL_FIELDS: &[&str] = &["count", "odatacount", "numfound"];
const TOTAL_FIELDS: &[&str] = &[
    "total",
    "totalcount",
    "totalresults",
    "totalitems",
    "totalsize",
];
const TIMESTAMP_FIELDS: &[&str] = &[
    "updatedat",
    "modifiedat",
    "lastmodified",
    "lastmodifiedat",
    "updated",
    "modified",
    "changedat",
    "datemodified",
    "lastupdated",
    "updatetime",
    "modifiedtime",
];
const FILTER_WORDS: &[&str] = &["since", "after", "from", "gte", "gt", "min", "start"];
const APPEND_ONLY: &[&str] = &[
    "events",
    "event",
    "logs",
    "log",
    "auditlogs",
    "auditlog",
    "auditevents",
    "activities",
    "activity",
    "history",
    "changelog",
];
const RESET_HEADERS: &[&str] = &["x-ratelimit-reset", "ratelimit-reset", "x-rate-limit-reset"];
const REMAINING_HEADERS: &[&str] = &[
    "x-ratelimit-remaining",
    "ratelimit-remaining",
    "x-rate-limit-remaining",
];

impl Mapper<'_> {
    fn decide(
        &mut self,
        stream: Option<&str>,
        field: &str,
        message: String,
        candidates: Vec<String>,
    ) {
        self.report.decisions.push(Decision {
            stream: stream.map(str::to_string),
            field: field.to_string(),
            message,
            candidates,
        });
    }

    fn unsupported(&mut self, scope: &str, message: String) {
        self.report.unsupported.push(Finding {
            scope: scope.to_string(),
            message,
        });
    }

    fn skip(&mut self, path: &str, reason: String) {
        self.report.skipped.push(Skipped {
            path: path.to_string(),
            reason,
        });
    }

    fn deref(&self, loc: &Loc) -> CliResult<Loc> {
        let (base, v) = self.set.deref(&loc.base, &loc.v)?;
        Ok(Loc { base, v })
    }

    fn flatten(&self, loc: &Loc, depth: usize) -> CliResult<Flat> {
        let calls = self.flat_calls.get() + 1;
        self.flat_calls.set(calls);
        if calls > MAX_FLATTEN_NODES {
            return Err(CliError::Config(format!(
                "spec: schemas too complex to import (more than {MAX_FLATTEN_NODES} schema nodes)"
            )));
        }
        let key = loc
            .v
            .get("$ref")
            .and_then(Value::as_str)
            .map(|r| (loc.base.clone(), r.to_string()));
        if let Some(k) = &key
            && let Some(f) = self.flat_cache.borrow().get(k)
        {
            return Ok(f.clone());
        }
        let f = self.flatten_uncached(loc, depth)?;
        if let Some(k) = key {
            self.flat_cache.borrow_mut().insert(k, f.clone());
        }
        Ok(f)
    }

    fn flatten_uncached(&self, loc: &Loc, depth: usize) -> CliResult<Flat> {
        if depth > MAX_SCHEMA_DEPTH {
            return Err(CliError::Config(format!(
                "spec: schema composition nested deeper than {MAX_SCHEMA_DEPTH} levels (a recursive `allOf`?)"
            )));
        }
        let Loc { base, v: s } = self.deref(loc)?;
        let mut f = Flat::default();
        match s.get("type") {
            Some(Value::String(t)) if t == "null" => f.nullable = true,
            Some(Value::String(t)) => {
                f.types.insert(t.clone());
            }
            Some(Value::Array(ts)) => {
                for t in ts.iter().filter_map(Value::as_str) {
                    if t == "null" {
                        f.nullable = true;
                    } else {
                        f.types.insert(t.to_string());
                    }
                }
            }
            _ => {}
        }
        if s.get("nullable") == Some(&Value::Bool(true))
            || s.get("x-nullable") == Some(&Value::Bool(true))
        {
            f.nullable = true;
        }
        f.format = s.get("format").and_then(Value::as_str).map(str::to_string);
        f.xpk = s.get("x-primary-key").cloned();
        f.example = s.get("example").cloned().or_else(|| {
            s.get("examples")
                .and_then(Value::as_array)
                .and_then(|a| a.first())
                .cloned()
        });
        f.description = s
            .get("description")
            .and_then(Value::as_str)
            .map(str::to_string);
        f.default = s.get("default").cloned();
        f.minimum = s.get("minimum").cloned();
        f.maximum = s.get("maximum").cloned();
        if let Some(props) = s.get("properties").and_then(Value::as_object) {
            for (k, p) in props {
                if tainted(k) {
                    self.tainted.borrow_mut().insert(k.clone());
                    continue;
                }
                f.props.insert(
                    k.clone(),
                    Loc {
                        base: base.clone(),
                        v: p.clone(),
                    },
                );
            }
        }
        if let Some(req) = s.get("required").and_then(Value::as_array) {
            f.required
                .extend(req.iter().filter_map(Value::as_str).map(str::to_string));
        }
        if let Some(items) = s.get("items") {
            f.items = Some(Loc {
                base: base.clone(),
                v: items.clone(),
            });
        }
        for key in ["allOf", "oneOf", "anyOf"] {
            if let Some(subs) = s.get(key).and_then(Value::as_array) {
                for sub in subs {
                    let g = self.flatten(
                        &Loc {
                            base: base.clone(),
                            v: sub.clone(),
                        },
                        depth + 1,
                    )?;
                    merge(&mut f, g);
                }
            }
        }
        if f.types.is_empty() && f.items.is_some() {
            f.types.insert("array".into());
        }
        Ok(f)
    }

    fn records_of(&self, schema: &Loc) -> CliResult<(Records, Flat)> {
        let f = self.flatten(schema, 0)?;
        if f.is_array() {
            let items = f.items.clone().unwrap_or(Loc {
                base: schema.base.clone(),
                v: json!({}),
            });
            return Ok((Records::Body(items), f));
        }
        if !f.is_object() || f.props.contains_key("id") {
            return Ok((Records::NotList, f));
        }
        let mut cands: Vec<(Vec<String>, Loc)> = Vec::new();
        for (name, p) in &f.props {
            if let Some(items) = self.list_items(name, p)? {
                cands.push((vec![name.clone()], items));
            }
        }
        if cands.is_empty() {
            for (name, p) in &f.props {
                let pf = self.flatten(p, 0)?;
                if pf.is_array() || !pf.is_object() {
                    continue;
                }
                for (q, qp) in &pf.props {
                    if let Some(items) = self.list_items(q, qp)? {
                        cands.push((vec![name.clone(), q.clone()], items));
                    }
                }
            }
        }
        let wrapped: Vec<&(Vec<String>, Loc)> = cands
            .iter()
            .filter(|(segs, _)| {
                segs.last()
                    .is_some_and(|l| WRAPPERS.contains(&norm(l).as_str()))
            })
            .collect();
        let rec = match (cands.len(), wrapped.len()) {
            (0, _) => Records::NotList,
            (1, _) => Records::At(cands[0].0.clone(), cands[0].1.clone(), Vec::new()),
            (_, 1) => Records::At(
                wrapped[0].0.clone(),
                wrapped[0].1.clone(),
                cands.iter().map(|(s, _)| jsonpath(s, true)).collect(),
            ),
            _ => Records::Ambiguous(cands.iter().map(|(s, _)| jsonpath(s, true)).collect()),
        };
        Ok((rec, f))
    }

    /// The items of an array property that holds records: objects, or any
    /// array under a conventional wrapper name.
    fn list_items(&self, name: &str, p: &Loc) -> CliResult<Option<Loc>> {
        let pf = self.flatten(p, 0)?;
        if !pf.is_array() {
            return Ok(None);
        }
        let items = pf.items.clone().unwrap_or(Loc {
            base: p.base.clone(),
            v: json!({}),
        });
        let objects = self.flatten(&items, 0)?.is_object();
        Ok((objects || WRAPPERS.contains(&norm(name).as_str())).then_some(items))
    }

    fn components(&self, v3: &str, v2: &str) -> Vec<(String, Value)> {
        let root = self.set.root();
        let node = if self.v2 {
            root.get(v2)
        } else {
            root.get("components").and_then(|c| c.get(v3))
        };
        let mut out: Vec<(String, Value)> = node
            .and_then(Value::as_object)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn run(&mut self, opts: &ImportOptions) -> CliResult<Imported> {
        let root = self.set.root().clone();
        let paths = root
            .get("paths")
            .and_then(Value::as_object)
            .ok_or_else(|| CliError::Config("spec: no `paths` object".into()))?;
        let mut keys: Vec<&String> = paths.keys().collect();
        keys.sort();
        let selected: Vec<&String> = keys
            .into_iter()
            .filter(|p| {
                (opts.include.is_empty()
                    || opts.include.iter().any(|g| crate::select::glob_match(g, p)))
                    && !opts.exclude.iter().any(|g| crate::select::glob_match(g, p))
            })
            .collect();
        let mut gets = Vec::new();
        for p in selected {
            if tainted(p) {
                self.unsupported(
                    &format!("path {p}"),
                    "contains `${`, which would become an interpolation directive; not imported"
                        .into(),
                );
                continue;
            }
            let item = match self.deref(&Loc {
                base: self.root_key.clone(),
                v: paths[p].clone(),
            }) {
                Ok(i) => i,
                Err(e) => {
                    self.skip(p, e.to_string());
                    continue;
                }
            };
            if let Some(op) = item.v.get("get") {
                gets.push((p.clone(), item.clone(), op.clone()));
            }
        }
        if gets.len() > MAX_OPERATIONS && opts.include.is_empty() {
            return Err(CliError::Config(format!(
                "spec: {} GET operations — more than {MAX_OPERATIONS} needs --include to pick \
                 the endpoints worth syncing (e.g. --include '/v1/customers*')",
                gets.len()
            )));
        }
        let global_security = security_list(root.get("security"));
        let mut cands: Vec<Candidate> = Vec::new();
        for (path, item, op) in gets {
            match self.candidate(&path, &item, &op, &global_security) {
                Ok(Some(c)) => cands.push(c),
                Ok(None) => {}
                Err(e) => self.skip(&path, e.to_string()),
            }
        }
        self.resolve_parents(&mut cands);
        if cands.is_empty() {
            return Err(CliError::Config(
                "spec: no GET operation returns a list of records — nothing to import \
                 (see the skipped operations above, or widen --include)"
                    .into(),
            ));
        }
        assign_names(&mut cands);

        let name = match &opts.name {
            Some(n) => n.clone(),
            None => {
                let title = root
                    .get("info")
                    .and_then(|i| i.get("title"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let s = slug(title, '-');
                if s.is_empty() {
                    return Err(CliError::Config(
                        "spec: `info.title` gives no usable name — pass --name".into(),
                    ));
                }
                s
            }
        };
        let mut params = ParamsSpec::new();
        params.insert(API_ROOT.into(), self.api_root(opts)?);
        let auth = self.auth(&cands, &mut params);
        let retry = self.rate_limits(&cands);
        let nested = nested_fields(self, &cands)?;

        let mut streams = Vec::new();
        let mut configs = Vec::new();
        let mut behavioral = Vec::new();
        for c in &cands {
            let (stream, cfg, summary) = self.stream(c, &cands)?;
            if c.parent.is_none()
                && let Some(records) = self.example_records(c)
            {
                let fakes = fake_params(&params);
                let expected: Vec<Value> = records
                    .iter()
                    .map(|r| encode_fields(r.clone(), &nested))
                    .collect();
                behavioral.push(Behavioral {
                    name: format!("{}-example", c.name),
                    params: fakes,
                    page_size: None,
                    row: Some(c.name.clone()),
                    input: Value::Array(records),
                    expect: json!({ "records": expected }),
                    retries: 0,
                });
            }
            streams.push(stream);
            configs.push(cfg);
            self.report.streams.push(summary);
        }
        for (s, summary) in self.report.streams.iter_mut().enumerate() {
            summary.example_records = behavioral
                .iter()
                .find(|b| b.row.as_deref() == Some(cands[s].name.as_str()))
                .and_then(|b| b.input.as_array().map(Vec::len))
                .unwrap_or(0);
        }

        let mut source_cfg = Map::new();
        source_cfg.insert("base_url".into(), json!(format!("${{param.{API_ROOT}}}")));
        if let Some(a) = auth {
            source_cfg.insert("auth".into(), a);
        }
        if !retry.is_empty() {
            source_cfg.insert("retry_on_response".into(), Value::Array(retry));
        }
        for key in ["records_path", "pagination"] {
            let first = configs[0].get(key).cloned();
            if first.is_some() && configs.iter().all(|c| c.get(key) == first.as_ref()) {
                source_cfg.insert(key.into(), first.unwrap_or(Value::Null));
                for c in &mut configs {
                    c.remove(key);
                }
            }
        }
        check_keys(&source_cfg)?;
        for (s, cfg) in streams.iter_mut().zip(configs) {
            check_keys(&cfg)?;
            s.source.config = Value::Object(cfg);
        }

        let info = root.get("info");
        let title = info
            .and_then(|i| i.get("title"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|t| !t.is_empty() && !tainted(t))
            .unwrap_or(&name);
        let description = info
            .and_then(|i| i.get("summary").or_else(|| i.get("description")))
            .and_then(Value::as_str)
            .and_then(|d| d.lines().map(str::trim).find(|l| !l.is_empty()))
            .filter(|d| !tainted(d))
            .map(|d| format!("{title} — {d}"))
            .unwrap_or_else(|| format!("{title} (imported from its OpenAPI description)"));
        let docs = root
            .get("externalDocs")
            .and_then(|d| d.get("url"))
            .and_then(Value::as_str)
            .filter(|u| (u.starts_with("https://") || u.starts_with("http://")) && !tainted(u))
            .map(str::to_string);
        let source: ConnectorSpec =
            serde_json::from_value(json!({ "type": "rest", "config": Value::Object(source_cfg) }))
                .map_err(|e| CliError::Internal(format!("hub import: source spec: {e}")))?;
        let transforms = if nested.is_empty() {
            Vec::new()
        } else {
            vec![TransformSpec {
                kind: "json_encode".into(),
                config: json!({ "fields": nested }),
            }]
        };
        let template = SourceTemplate {
            kind: TemplateKind::SourceTemplate,
            requires_faucet: Some(crate::requires::default_requirement()),
            version: 1,
            name: name.clone(),
            owner: Some(opts.owner.clone()),
            description: Some(description),
            tags: vec!["rest".into()],
            docs,
            params: params.clone(),
            auth: None,
            source,
            sources: BTreeMap::new(),
            transforms,
            contract: None,
            streams,
            tests: None,
        };
        template.validate()?;
        self.report.template = template.id();
        let names: Vec<String> = self.tainted.borrow().iter().cloned().collect();
        if !names.is_empty() {
            self.unsupported(
                &format!("names {}", names.join(", ")),
                "fields and parameters whose names contain `${` would become interpolation directives; they are ignored".into(),
            );
        }

        let suite = SuiteFile {
            version: 1,
            template: format!("source-templates/{}/{}.yaml", opts.owner, name),
            select: None,
            sink: Some(opts.test_sink.clone()),
            sink_select: None,
            overlay: None,
            overlay_select: None,
            suite: Suite {
                cases: vec![Case {
                    name: "defaults".into(),
                    params: fake_params(&params),
                    expect: Expect::default(),
                    retries: 0,
                }],
                combine: None,
                auto: Some(Auto {
                    enum_coverage: false,
                    required_omitted: true,
                    defaults_baseline: true,
                    retries: 0,
                }),
                behavioral,
            },
            base_dir: None,
        };
        Ok(Imported {
            template,
            suite,
            report: std::mem::take(&mut self.report),
        })
    }

    fn candidate(
        &mut self,
        path: &str,
        item: &Loc,
        op: &Value,
        global_security: &[BTreeMap<String, Vec<String>>],
    ) -> CliResult<Option<Candidate>> {
        if path
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .is_some_and(|s| s.starts_with('{'))
        {
            self.skip(path, "a single-object lookup, not a list".into());
            return Ok(None);
        }
        let op = self.deref(&Loc {
            base: item.base.clone(),
            v: op.clone(),
        })?;
        let params = self.params(item, &op)?;
        let mut responses = BTreeMap::new();
        if let Some(rs) = op.v.get("responses").and_then(Value::as_object) {
            for (code, r) in rs {
                responses.insert(
                    code.clone(),
                    self.deref(&Loc {
                        base: op.base.clone(),
                        v: r.clone(),
                    })?,
                );
            }
        }
        let Some(ok) = pick_success(&responses) else {
            self.skip(path, "no 2xx response is documented".into());
            return Ok(None);
        };
        let resp = responses[&ok].clone();
        let Some((schema, media_example)) = self.json_schema(&resp)? else {
            self.skip(path, format!("response {ok} has no JSON schema"));
            return Ok(None);
        };
        let (records, envelope) = self.records_of(&schema)?;
        let (records, records_ambiguous, records_others, item_loc) = match records {
            Records::NotList => {
                self.skip(path, "the response is a single object, not a list".into());
                return Ok(None);
            }
            Records::Body(i) => (Some(Vec::new()), Vec::new(), Vec::new(), Some(i)),
            Records::At(p, i, others) => (Some(p), Vec::new(), others, Some(i)),
            Records::Ambiguous(c) => (None, c, Vec::new(), None),
        };
        let item = match &item_loc {
            Some(l) => self.flatten(l, 0)?,
            None => Flat::default(),
        };
        let pp = path_params(path);
        if pp.len() > 1 {
            self.skip(
                path,
                format!(
                    "{} path parameters ({}): only a child of a root list is imported",
                    pp.len(),
                    pp.join(", ")
                ),
            );
            return Ok(None);
        }
        let headers = self.headers(&resp)?;
        let mut limit_headers = headers.clone();
        if let Some(r) = responses.get("429") {
            for (k, v) in self.headers(r)? {
                limit_headers.entry(k).or_insert(v);
            }
        }
        let example =
            media_example.or_else(|| self.flatten(&schema, 0).ok().and_then(|f| f.example));
        let security = match op.v.get("security") {
            Some(s) => security_list(Some(s)),
            None => global_security.to_vec(),
        };
        Ok(Some(Candidate {
            path: path.to_string(),
            summary: op
                .v
                .get("summary")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty() && !tainted(s))
                .map(str::to_string),
            params,
            records,
            records_ambiguous,
            records_others,
            item,
            envelope,
            headers,
            limit_headers,
            responses,
            example,
            security,
            path_param: pp.into_iter().next(),
            parent: None,
            name: String::new(),
        }))
    }

    fn headers(&self, resp: &Loc) -> CliResult<BTreeMap<String, Value>> {
        let mut out = BTreeMap::new();
        if let Some(h) = resp.v.get("headers").and_then(Value::as_object) {
            for (k, hv) in h {
                let d = self.deref(&Loc {
                    base: resp.base.clone(),
                    v: hv.clone(),
                })?;
                out.entry(k.to_ascii_lowercase()).or_insert(d.v);
            }
        }
        Ok(out)
    }

    fn params(&self, item: &Loc, op: &Loc) -> CliResult<Vec<Param>> {
        let mut by_key: BTreeMap<(String, String), Param> = BTreeMap::new();
        for owner in [item, op] {
            let Some(list) = owner.v.get("parameters").and_then(Value::as_array) else {
                continue;
            };
            for raw in list {
                let p = self.deref(&Loc {
                    base: owner.base.clone(),
                    v: raw.clone(),
                })?;
                let name =
                    p.v.get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                let location =
                    p.v.get("in")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                if name.is_empty() {
                    continue;
                }
                if tainted(&name) {
                    self.tainted.borrow_mut().insert(name);
                    continue;
                }
                let schema_loc = match p.v.get("schema") {
                    Some(s) if !self.v2 || location == "body" => Loc {
                        base: p.base.clone(),
                        v: s.clone(),
                    },
                    _ => p.clone(),
                };
                let mut schema = self.flatten(&schema_loc, 0)?;
                if schema.default.is_none() {
                    schema.default = p.v.get("default").cloned();
                }
                if schema.description.is_none() {
                    schema.description =
                        p.v.get("description")
                            .and_then(Value::as_str)
                            .map(str::to_string);
                }
                let required = p.v.get("required") == Some(&Value::Bool(true));
                by_key.insert(
                    (location.clone(), name.clone()),
                    Param {
                        name,
                        location,
                        required,
                        schema,
                    },
                );
            }
        }
        Ok(by_key.into_values().collect())
    }

    /// The JSON schema of a response and its example, if any.
    fn json_schema(&self, resp: &Loc) -> CliResult<Option<(Loc, Option<Value>)>> {
        if self.v2 {
            let Some(s) = resp.v.get("schema") else {
                return Ok(None);
            };
            let example = resp
                .v
                .get("examples")
                .and_then(Value::as_object)
                .and_then(|ex| {
                    let mut keys: Vec<&String> = ex.keys().filter(|k| k.contains("json")).collect();
                    keys.sort();
                    keys.first().map(|k| ex[*k].clone())
                });
            return Ok(Some((
                Loc {
                    base: resp.base.clone(),
                    v: s.clone(),
                },
                example,
            )));
        }
        let Some(content) = resp.v.get("content").and_then(Value::as_object) else {
            return Ok(None);
        };
        let mut keys: Vec<&String> = content.keys().collect();
        keys.sort();
        let media_key = keys
            .iter()
            .find(|k| k.as_str() == "application/json")
            .or_else(|| keys.iter().find(|k| k.contains("json")))
            .or_else(|| keys.iter().find(|k| k.as_str() == "*/*"));
        let Some(media) = media_key.map(|k| &content[*k]) else {
            return Ok(None);
        };
        let Some(schema) = media.get("schema") else {
            return Ok(None);
        };
        let mut example = media.get("example").cloned();
        if example.is_none()
            && let Some(ex) = media.get("examples").and_then(Value::as_object)
        {
            let mut names: Vec<&String> = ex.keys().collect();
            names.sort();
            if let Some(first) = names.first() {
                let e = self.deref(&Loc {
                    base: resp.base.clone(),
                    v: ex[*first].clone(),
                })?;
                example = e.v.get("value").cloned();
            }
        }
        Ok(Some((
            Loc {
                base: resp.base.clone(),
                v: schema.clone(),
            },
            example,
        )))
    }

    fn resolve_parents(&mut self, cands: &mut Vec<Candidate>) {
        let mut keep = Vec::new();
        let roots: BTreeMap<String, usize> = cands
            .iter()
            .enumerate()
            .filter(|(_, c)| c.path_param.is_none())
            .map(|(i, c)| (c.path.trim_end_matches('/').to_string(), i))
            .collect();
        for i in 0..cands.len() {
            let Some(p) = cands[i].path_param.clone() else {
                keep.push(true);
                continue;
            };
            let marker = format!("/{{{p}}}");
            let parent_path = cands[i]
                .path
                .split(&marker)
                .next()
                .unwrap_or_default()
                .to_string();
            let Some(&pi) = roots.get(&parent_path) else {
                self.skip(
                    &cands[i].path.clone(),
                    format!("path parameter {{{p}}} does not resolve to a list operation at {parent_path}"),
                );
                keep.push(false);
                continue;
            };
            let parent_props = &cands[pi].item.props;
            let lp = p.to_ascii_lowercase();
            let field = if parent_props.contains_key(&p) {
                Some(p.clone())
            } else if (lp == "id" || lp.ends_with("id")) && parent_props.contains_key("id") {
                if lp != "id" {
                    self.decide(
                        Some(&cands[i].path.clone()),
                        "parent_key",
                        format!("path parameter {{{p}}} is taken to hold the parent's `id`"),
                        Vec::new(),
                    );
                }
                Some("id".to_string())
            } else {
                None
            };
            match field {
                Some(f) => {
                    cands[i].parent = Some((pi, f));
                    keep.push(true);
                }
                None => {
                    let mut props: Vec<String> = parent_props.keys().cloned().collect();
                    props.sort();
                    self.skip(
                        &cands[i].path.clone(),
                        format!(
                            "path parameter {{{p}}}: no field of the parent list at {parent_path} \
                             is known to hold it (fields: {})",
                            props.join(", ")
                        ),
                    );
                    keep.push(false);
                }
            }
        }
        let mut it = keep.into_iter();
        let old: Vec<Candidate> = std::mem::take(cands);
        let mut index = BTreeMap::new();
        for (i, c) in old.into_iter().enumerate() {
            if it.next().unwrap_or(false) {
                index.insert(i, cands.len());
                cands.push(c);
            }
        }
        for c in cands.iter_mut() {
            if let Some((pi, f)) = c.parent.take() {
                c.parent = index.get(&pi).map(|&n| (n, f));
            }
        }
    }

    fn api_root(&mut self, opts: &ImportOptions) -> CliResult<ParamSpec> {
        let root = self.set.root().clone();
        let mut servers: Vec<String> = Vec::new();
        let mut unresolved: Vec<String> = Vec::new();
        if self.v2 {
            if let Some(host) = root.get("host").and_then(Value::as_str) {
                let schemes: Vec<&str> = root
                    .get("schemes")
                    .and_then(Value::as_array)
                    .map(|a| a.iter().filter_map(Value::as_str).collect())
                    .unwrap_or_default();
                let scheme = if schemes.is_empty() || schemes.contains(&"https") {
                    "https"
                } else {
                    schemes[0]
                };
                let base = root.get("basePath").and_then(Value::as_str).unwrap_or("");
                servers.push(format!("{scheme}://{host}{base}"));
            } else if let Some(base) = root.get("basePath").and_then(Value::as_str) {
                servers.push(base.to_string());
            }
        } else if let Some(list) = root.get("servers").and_then(Value::as_array) {
            for s in list {
                let Some(url) = s.get("url").and_then(Value::as_str) else {
                    continue;
                };
                let mut url = url.to_string();
                for var in path_vars(&url) {
                    match s
                        .get("variables")
                        .and_then(|v| v.get(&var))
                        .and_then(|v| v.get("default"))
                        .map(scalar_text)
                    {
                        Some(d) => url = url.replace(&format!("{{{var}}}"), &d),
                        None => {
                            unresolved.push(format!("{url}: variable {{{var}}} has no default"))
                        }
                    }
                }
                servers.push(url);
            }
        }
        let (bad, servers): (Vec<String>, Vec<String>) =
            servers.into_iter().partition(|u| tainted(u));
        for u in bad {
            self.unsupported(
                &format!("server {u}"),
                "contains `${`, which would become an interpolation directive; not used".into(),
            );
        }
        self.report.servers = servers.clone();
        let chosen = match opts.server.as_deref() {
            None => {
                if servers.len() > 1 {
                    self.decide(
                        None,
                        "params.api_root",
                        format!(
                            "the spec lists {} servers; `api_root` defaults to the first (pick another with --server)",
                            servers.len()
                        ),
                        servers.clone(),
                    );
                }
                servers.first().cloned()
            }
            Some(s) => match s.parse::<usize>() {
                Ok(i) => Some(servers.get(i).cloned().ok_or_else(|| {
                    CliError::Config(format!(
                        "--server {i}: the spec lists {} server(s)",
                        servers.len()
                    ))
                })?),
                Err(_) => {
                    if tainted(s) || self.absolute(s)?.is_none() {
                        return Err(CliError::Config(format!(
                            "--server {s}: not an absolute URL (give a full https://… URL or an index into the spec's servers)"
                        )));
                    }
                    Some(s.to_string())
                }
            },
        };
        let chosen = chosen.map(|u| self.absolute(&u)).transpose()?.flatten();
        let mut spec = ParamSpec {
            kind: ParamType::String,
            required: false,
            default: None,
            secret: false,
            description: Some("API root; tests and replays point it at a local server".into()),
            computed: None,
            values: Vec::new(),
        };
        match chosen {
            Some(u) if !u.contains('{') => {
                spec.default = Some(Value::String(u.trim_end_matches('/').to_string()));
            }
            Some(u) => {
                spec.required = true;
                self.decide(
                    None,
                    "params.api_root",
                    format!("server URL {u} keeps a template variable with no default"),
                    unresolved,
                );
            }
            None => {
                spec.required = true;
                self.decide(
                    None,
                    "params.api_root",
                    "the spec names no absolute server URL; every run must pass --param api_root"
                        .into(),
                    servers,
                );
            }
        }
        Ok(spec)
    }

    /// A server URL made absolute against the spec's own location, or `None`
    /// when it cannot be (a relative server in a local file).
    fn absolute(&self, u: &str) -> CliResult<Option<String>> {
        if url::Url::parse(u).is_ok() {
            return Ok(Some(u.to_string()));
        }
        let base = self.set.root_url();
        if matches!(base.scheme(), "http" | "https") {
            return base
                .join(u)
                .map(|j| Some(j.to_string()))
                .map_err(|e| CliError::Config(format!("spec: server URL {u}: {e}")));
        }
        Ok(None)
    }

    fn auth(&mut self, cands: &[Candidate], params: &mut ParamsSpec) -> Option<Value> {
        let schemes: BTreeMap<String, Value> = self
            .components("securitySchemes", "securityDefinitions")
            .into_iter()
            .filter_map(|(k, v)| {
                self.deref(&Loc {
                    base: self.root_key.clone(),
                    v,
                })
                .ok()
                .map(|l| (k, l.v))
            })
            .collect();
        let mut counts: BTreeMap<String, usize> = BTreeMap::new();
        for c in cands {
            if let Some(first) = c
                .security
                .iter()
                .find(|req| req.len() == 1)
                .and_then(|req| req.keys().next())
            {
                *counts.entry(first.clone()).or_default() += 1;
            }
        }
        let Some(top) = counts.values().max().copied() else {
            let mut compound: Vec<String> = cands
                .iter()
                .flat_map(|c| c.security.iter())
                .filter(|r| r.len() > 1)
                .map(|r| r.keys().cloned().collect::<Vec<_>>().join(" + "))
                .collect();
            compound.sort();
            compound.dedup();
            if !compound.is_empty() {
                self.unsupported(
                    &format!("security {}", compound.join(", ")),
                    "every requirement combines several schemes, which the REST source cannot send together; auth left unset"
                        .into(),
                );
            }
            return None;
        };
        let tied: Vec<String> = counts
            .iter()
            .filter(|(_, n)| **n == top)
            .map(|(k, _)| k.clone())
            .collect();
        let chosen = tied[0].clone();
        if tied.len() > 1 {
            self.decide(
                None,
                "source.config.auth",
                format!(
                    "{} schemes are used by the same number of operations; `{chosen}` was picked",
                    tied.len()
                ),
                tied.clone(),
            );
        }
        for c in cands {
            let anonymous = c.security.iter().any(BTreeMap::is_empty);
            let covered = c.security.is_empty()
                || anonymous
                || c.security
                    .iter()
                    .any(|r| r.len() == 1 && r.contains_key(&chosen));
            if !covered {
                let mut needs: Vec<String> = c
                    .security
                    .iter()
                    .map(|r| r.keys().cloned().collect::<Vec<_>>().join(" + "))
                    .collect();
                needs.sort();
                self.decide(
                    Some(&c.path),
                    "auth",
                    format!(
                        "{} needs other credentials than the template's shared `{chosen}` auth",
                        c.path
                    ),
                    needs,
                );
            }
        }
        let scopes: BTreeSet<String> = cands
            .iter()
            .flat_map(|c| c.security.iter())
            .filter_map(|r| r.get(&chosen))
            .flatten()
            .filter(|s| !tainted(s))
            .cloned()
            .collect();
        let Some(scheme) = schemes.get(&chosen) else {
            self.unsupported(
                &format!("security {chosen}"),
                "the security requirement names no declared scheme".into(),
            );
            return None;
        };
        match self.map_scheme(&chosen, scheme, scopes.into_iter().collect(), params) {
            Ok(auth) => serde_json::to_value(auth).ok(),
            Err(msg) => {
                self.unsupported(&format!("security {chosen}"), msg.clone());
                self.decide(
                    None,
                    "source.config.auth",
                    format!("auth left unset: {msg}"),
                    Vec::new(),
                );
                None
            }
        }
    }

    fn map_scheme(
        &self,
        name: &str,
        s: &Value,
        scopes: Vec<String>,
        params: &mut ParamsSpec,
    ) -> Result<Auth, String> {
        let kind = s.get("type").and_then(Value::as_str).unwrap_or_default();
        let secret = |params: &mut ParamsSpec, p: &str, what: &str| {
            params.insert(
                p.to_string(),
                ParamSpec {
                    kind: ParamType::String,
                    required: true,
                    default: None,
                    secret: true,
                    description: Some(format!("{what} (`{name}` in the spec)")),
                    computed: None,
                    values: Vec::new(),
                },
            );
            format!("${{param.{p}}}")
        };
        match kind {
            "http" => match s
                .get("scheme")
                .and_then(Value::as_str)
                .map(str::to_ascii_lowercase)
                .as_deref()
            {
                Some("bearer") => Ok(Auth::Bearer {
                    token: secret(params, "api_token", "Bearer token"),
                }),
                Some("basic") => Ok(Auth::Basic {
                    username: secret(params, "api_username", "Basic-auth user name"),
                    password: secret(params, "api_password", "Basic-auth password"),
                }),
                other => Err(format!(
                    "HTTP auth scheme `{}` is not supported by the REST source (bearer and basic are)",
                    other.unwrap_or("")
                )),
            },
            "basic" => Ok(Auth::Basic {
                username: secret(params, "api_username", "Basic-auth user name"),
                password: secret(params, "api_password", "Basic-auth password"),
            }),
            "apiKey" => {
                let key_name = s
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if tainted(&key_name) {
                    return Err(format!(
                        "the API key's name `{key_name}` contains `${{`, which would become an interpolation directive"
                    ));
                }
                match s.get("in").and_then(Value::as_str) {
                    Some("header") => Ok(Auth::ApiKey {
                        header: key_name,
                        value: secret(params, "api_key", "API key"),
                    }),
                    Some("query") => Ok(Auth::ApiKeyQuery {
                        param: key_name,
                        value: secret(params, "api_key", "API key"),
                    }),
                    other => Err(format!(
                        "an API key sent in `{}` is not supported (header and query are)",
                        other.unwrap_or("")
                    )),
                }
            }
            "oauth2" => {
                let token_url = if self.v2 {
                    (s.get("flow").and_then(Value::as_str) == Some("application"))
                        .then(|| s.get("tokenUrl").and_then(Value::as_str))
                        .flatten()
                } else {
                    s.get("flows")
                        .and_then(|f| f.get("clientCredentials"))
                        .and_then(|f| f.get("tokenUrl"))
                        .and_then(Value::as_str)
                };
                let Some(token_url) = token_url else {
                    return Err(
                        "OAuth2 without a client-credentials flow needs an interactive \
                         authorization, which the REST source does not perform"
                            .into(),
                    );
                };
                if tainted(token_url) {
                    return Err(format!(
                        "OAuth2 token URL {token_url} contains `${{`, which would become an interpolation directive"
                    ));
                }
                let token_url = match self.absolute(token_url) {
                    Ok(Some(u)) => u,
                    _ => return Err(format!("OAuth2 token URL {token_url} is not absolute")),
                };
                Ok(Auth::OAuth2 {
                    token_url,
                    client_id: secret(params, "client_id", "OAuth2 client id"),
                    client_secret: secret(params, "client_secret", "OAuth2 client secret"),
                    scopes,
                    expiry_ratio: faucet_source_rest::DEFAULT_EXPIRY_RATIO,
                })
            }
            other => Err(format!(
                "security scheme type `{other}` is not supported by the REST source"
            )),
        }
    }

    fn rate_limits(&mut self, cands: &[Candidate]) -> Vec<Value> {
        let throttled = cands.iter().any(|c| c.responses.contains_key("429"));
        let mut headers: BTreeMap<String, Value> = BTreeMap::new();
        for c in cands {
            for (k, v) in &c.limit_headers {
                headers.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
        let retry_after = headers.contains_key("retry-after");
        let resets: Vec<String> = RESET_HEADERS
            .iter()
            .filter(|h| headers.contains_key(**h))
            .map(|h| h.to_string())
            .collect();
        if resets.len() > 1 {
            self.decide(
                None,
                "source.config.retry_on_response",
                format!(
                    "several rate-limit reset headers are documented; `{}` is read",
                    resets[0]
                ),
                resets.clone(),
            );
        }
        let reset = resets
            .first()
            .and_then(|h| headers.get(h).map(|v| (h.clone(), v.clone())));
        let remaining = REMAINING_HEADERS.iter().find(|h| headers.contains_key(**h));
        if !throttled && !retry_after && reset.is_none() {
            return Vec::new();
        }
        let backoff = match &reset {
            Some((name, h)) => match wait_unit(h) {
                Some(unit) => Some(BackoffFrom::Header {
                    name: name.clone(),
                    unit,
                }),
                None => {
                    self.decide(
                        None,
                        "source.config.retry_on_response",
                        format!(
                            "rate-limit header {name} is documented without a unit (seconds, epoch seconds, …); \
                             its wait is not read"
                        ),
                        Vec::new(),
                    );
                    None
                }
            },
            None => None,
        };
        let mut rules = Vec::new();
        let base = RetryMatcher {
            status: Vec::new(),
            match_success: false,
            body_path: None,
            values: Vec::new(),
            header: None,
            backoff_from: None,
            backoff_secs: None,
            max_wait_secs: None,
        };
        if throttled || retry_after || reset.is_some() {
            rules.push(RetryMatcher {
                status: vec![429],
                backoff_from: backoff.clone(),
                ..base.clone()
            });
        }
        if let (Some(rem), Some(b)) = (remaining, backoff) {
            rules.push(RetryMatcher {
                match_success: true,
                header: Some(rem.to_string()),
                values: vec![json!("0")],
                backoff_from: Some(b),
                ..base
            });
        }
        rules
            .into_iter()
            .filter_map(|r| serde_json::to_value(r).ok())
            .collect()
    }

    fn example_records(&self, c: &Candidate) -> Option<Vec<Value>> {
        let ex = c.example.as_ref()?;
        let segs = c.records.as_ref()?;
        let mut cur = ex;
        for s in segs {
            cur = cur.get(s)?;
        }
        let records: Vec<Value> = cur
            .as_array()?
            .iter()
            .filter(|r| r.is_object() && !tainted(&faucet_core::util::canonical_json(r)))
            .take(MAX_EXAMPLE_RECORDS)
            .cloned()
            .collect();
        (!records.is_empty()).then_some(records)
    }

    fn stream(
        &mut self,
        c: &Candidate,
        all: &[Candidate],
    ) -> CliResult<(Stream, Map<String, Value>, StreamSummary)> {
        let name = c.name.clone();
        let mut cfg = Map::new();
        let (path, parent, parent_key) = match &c.parent {
            Some((pi, field)) => {
                let p = &all[*pi];
                let pp = c.path_param.clone().unwrap_or_default();
                (
                    c.path
                        .replace(&format!("{{{pp}}}"), &format!("${{{}.{field}}}", p.name)),
                    Some(p.name.clone()),
                    (field != "id").then(|| field.clone()),
                )
            }
            None => (c.path.clone(), None, None),
        };
        cfg.insert("path".into(), json!(path));
        let records_path = match &c.records {
            Some(segs) if segs.is_empty() => None,
            Some(segs) => Some(jsonpath(segs, true)),
            None => {
                self.decide(
                    Some(&name),
                    "records_path",
                    "several arrays in the response could hold the records".into(),
                    c.records_ambiguous.clone(),
                );
                None
            }
        };
        if let Some(rp) = &records_path {
            cfg.insert("records_path".into(), json!(rp));
            if !c.records_others.is_empty() {
                self.decide(
                    Some(&name),
                    "records_path",
                    format!("several arrays could hold the records; `{rp}` (a conventional wrapper name) was picked"),
                    c.records_others.clone(),
                );
            }
        }
        let (pagination, label) = self.pagination(c, &name);
        if let Some(p) = pagination {
            cfg.insert(
                "pagination".into(),
                serde_json::to_value(p)
                    .map_err(|e| CliError::Internal(format!("hub import: pagination: {e}")))?,
            );
        }
        let keys = self.primary_keys(c, &name);
        let append_only = static_segments(&c.path)
            .last()
            .is_some_and(|s| APPEND_ONLY.contains(&norm(s).as_str()));
        let incremental = self.incremental(c, &name, append_only);
        if let Some((field, bind)) = &incremental {
            cfg.insert(
                "replication_method".into(),
                serde_json::to_value(ReplicationMethod::Incremental)
                    .map_err(|e| CliError::Internal(format!("hub import: replication: {e}")))?,
            );
            cfg.insert("replication_key".into(), json!(field));
            cfg.insert(
                "replication_bind".into(),
                serde_json::to_value(bind)
                    .map_err(|e| CliError::Internal(format!("hub import: bind: {e}")))?,
            );
        }
        if let Some(schema) = schema_hint(self, &c.item)? {
            cfg.insert("schema".into(), schema);
        }
        for p in &c.params {
            if p.required
                && p.location == "query"
                && p.schema.default.is_none()
                && !recognized_param(&p.name)
            {
                self.decide(
                    Some(&name),
                    "source.config.query_params",
                    format!("required query parameter `{}` has no default", p.name),
                    Vec::new(),
                );
            }
        }
        let write: Vec<WriteMode> = if append_only {
            vec![WriteMode::Append]
        } else if incremental.is_some() {
            vec![WriteMode::Upsert, WriteMode::Append]
        } else {
            vec![WriteMode::Overwrite, WriteMode::Upsert]
        };
        let summary = StreamSummary {
            name: name.clone(),
            path: c.path.clone(),
            records_path,
            pagination: label,
            primary_keys: keys.clone(),
            write: write.iter().map(|w| w.as_str().to_string()).collect(),
            parent: parent.clone(),
            incremental: incremental.as_ref().map(|(f, _)| f.clone()),
            example_records: 0,
        };
        let stream = Stream {
            name,
            description: c.summary.clone(),
            source: StreamSource::default(),
            transforms: Vec::new(),
            primary_keys: keys,
            write: if write.len() == 1 {
                WriteChoice::One(write[0])
            } else {
                WriteChoice::Many(write)
            },
            parent,
            parent_key,
            inherit_transforms: true,
        };
        Ok((stream, cfg, summary))
    }

    fn pagination(
        &mut self,
        c: &Candidate,
        stream: &str,
    ) -> (Option<PaginationStyle>, Option<String>) {
        let query: Vec<&Param> = c.params.iter().filter(|p| p.location == "query").collect();
        let find = |list: &[&str]| -> Vec<&Param> {
            let mut out: Vec<&Param> = query
                .iter()
                .copied()
                .filter(|p| list.contains(&norm(&p.name).as_str()))
                .collect();
            out.sort_by_key(|p| list.iter().position(|l| *l == norm(&p.name)));
            out
        };
        let mut fields: Vec<(Vec<String>, Flat)> = Vec::new();
        for (k, p) in &c.envelope.props {
            if c.records.as_ref().is_some_and(|r| r.first() == Some(k)) {
                continue;
            }
            let Ok(pf) = self.flatten(p, 0) else { continue };
            if pf.is_object() && !pf.is_array() {
                for (q, qp) in &pf.props {
                    if let Ok(qf) = self.flatten(qp, 0) {
                        fields.push((vec![k.clone(), q.clone()], qf));
                    }
                }
            }
            fields.push((vec![k.clone()], pf));
        }
        let leaf_norm = |segs: &[String]| segs.last().map(|s| norm(s)).unwrap_or_default();
        let mut links: Vec<String> = Vec::new();
        let mut tokens: Vec<String> = Vec::new();
        let mut totals: Vec<String> = Vec::new();
        let mut maybe_totals: Vec<String> = Vec::new();
        let mut ambiguous_next: Vec<String> = Vec::new();
        for (segs, f) in &fields {
            let leaf = leaf_norm(segs);
            let parent_next = segs.len() == 2 && norm(&segs[0]) == "next" && leaf == "href";
            let under_links = segs.len() == 2 && norm(&segs[0]) == "links";
            let raw = segs
                .last()
                .cloned()
                .unwrap_or_default()
                .to_ascii_lowercase();
            let linkish = under_links
                || f.uri_like()
                || raw.contains("link")
                || raw.contains("url")
                || raw.contains("href");
            if f.is_string() && !f.is_object() && leaf == "next" && !linkish {
                ambiguous_next.push(jsonpath(segs, false));
            } else if f.is_string()
                && !f.is_object()
                && (parent_next || NEXT_LINK_FIELDS.contains(&leaf.as_str()) && linkish)
            {
                links.push(jsonpath(segs, false));
            } else if f.is_string()
                && !f.is_object()
                && CURSOR_FIELDS.contains(&leaf.as_str())
                && !linkish
            {
                tokens.push(jsonpath(segs, false));
            } else if f.is_integer() && TOTAL_FIELDS.contains(&leaf.as_str()) {
                totals.push(jsonpath(segs, false));
            } else if f.is_integer() && MAYBE_TOTAL_FIELDS.contains(&leaf.as_str()) {
                maybe_totals.push(jsonpath(segs, false));
            }
        }
        if !ambiguous_next.is_empty() {
            self.decide(
                Some(stream),
                "pagination",
                "a `next` field could be a cursor token or a next-page URL (the spec gives no format)".into(),
                ambiguous_next,
            );
            return (None, None);
        }
        let link_header = c.headers.contains_key("link");
        let cursor_params = find(CURSOR_PARAMS);
        let page_params = find(PAGE_PARAMS);
        let offset_params = find(OFFSET_PARAMS);
        let mut response_side: Vec<&str> = Vec::new();
        if link_header {
            response_side.push("Link header");
        }
        if !links.is_empty() {
            response_side.push("next link in the body");
        }
        if !tokens.is_empty() || !cursor_params.is_empty() {
            response_side.push("cursor");
        }
        if response_side.len() > 1 {
            let mut cands: Vec<String> = response_side.iter().map(|s| s.to_string()).collect();
            cands.extend(links.iter().cloned());
            cands.extend(tokens.iter().cloned());
            self.decide(
                Some(stream),
                "pagination",
                "the response carries more than one pagination signal".into(),
                cands,
            );
            return (None, None);
        }
        match response_side.first().copied() {
            Some("Link header") => {
                return (Some(PaginationStyle::LinkHeader), Some("LinkHeader".into()));
            }
            Some("next link in the body") => {
                if links.len() > 1 {
                    self.decide(
                        Some(stream),
                        "pagination",
                        "several fields could hold the next-page link".into(),
                        links,
                    );
                    return (None, None);
                }
                return (
                    Some(PaginationStyle::NextLinkInBody {
                        next_link_path: links[0].clone(),
                    }),
                    Some("NextLinkInBody".into()),
                );
            }
            Some(_) => {
                return match (cursor_params.as_slice(), tokens.as_slice()) {
                    ([param], [token]) => (
                        Some(PaginationStyle::Cursor {
                            next_token_path: token.clone(),
                            param_name: param.name.clone(),
                        }),
                        Some("Cursor".into()),
                    ),
                    ([], _) => {
                        self.decide(
                            Some(stream),
                            "pagination",
                            "the response has a next-cursor field but no request parameter takes it".into(),
                            tokens,
                        );
                        (None, None)
                    }
                    (_, []) => {
                        self.decide(
                            Some(stream),
                            "pagination",
                            "a cursor parameter is documented but no response field carries the next cursor".into(),
                            cursor_params.iter().map(|p| p.name.clone()).collect(),
                        );
                        (None, None)
                    }
                    _ => {
                        let mut cands: Vec<String> =
                            cursor_params.iter().map(|p| p.name.clone()).collect();
                        cands.extend(tokens);
                        self.decide(
                            Some(stream),
                            "pagination",
                            "more than one cursor parameter or next-cursor field".into(),
                            cands,
                        );
                        (None, None)
                    }
                };
            }
            None => {}
        }
        match (page_params.first(), offset_params.first()) {
            (Some(_), Some(_)) => {
                self.decide(
                    Some(stream),
                    "pagination",
                    "both page-number and offset parameters are documented".into(),
                    page_params
                        .iter()
                        .chain(offset_params.iter())
                        .map(|p| p.name.clone())
                        .collect(),
                );
                (None, None)
            }
            (Some(page), None) => {
                let start = [page.schema.default.as_ref(), page.schema.minimum.as_ref()]
                    .into_iter()
                    .flatten()
                    .find_map(Value::as_u64)
                    .map(|n| if n == 0 { 0 } else { 1 })
                    .unwrap_or(1);
                let sizes: Vec<(String, usize)> = find(SIZE_PARAMS)
                    .into_iter()
                    .filter_map(|p| {
                        p.schema
                            .default
                            .as_ref()
                            .and_then(Value::as_u64)
                            .map(|d| (p.name.clone(), d as usize))
                    })
                    .collect();
                if sizes.len() > 1 {
                    self.decide(
                        Some(stream),
                        "pagination.page_size_param",
                        format!(
                            "several page-size parameters have defaults; `{}` is used",
                            sizes[0].0
                        ),
                        sizes.iter().map(|s| s.0.clone()).collect(),
                    );
                }
                let size = sizes.into_iter().next();
                (
                    Some(PaginationStyle::PageNumber {
                        param_name: page.name.clone(),
                        start_page: start,
                        page_size: size.as_ref().map(|s| s.1),
                        page_size_param: size.map(|s| s.0),
                    }),
                    Some("PageNumber".into()),
                )
            }
            (None, Some(off)) => {
                let limits: Vec<(String, usize)> = find(LIMIT_PARAMS)
                    .into_iter()
                    .filter_map(|p| {
                        p.schema
                            .default
                            .as_ref()
                            .or(p.schema.maximum.as_ref())
                            .and_then(Value::as_u64)
                            .map(|d| (p.name.clone(), d as usize))
                    })
                    .collect();
                if limits.len() > 1 {
                    self.decide(
                        Some(stream),
                        "pagination.limit_param",
                        format!(
                            "several limit parameters have a default or maximum; `{}` is used",
                            limits[0].0
                        ),
                        limits.iter().map(|s| s.0.clone()).collect(),
                    );
                }
                let limit = limits.into_iter().next();
                let Some((limit_param, limit)) = limit else {
                    self.decide(
                        Some(stream),
                        "pagination",
                        format!(
                            "offset parameter `{}` needs a page size, but no limit parameter has a default or maximum",
                            off.name
                        ),
                        find(LIMIT_PARAMS).iter().map(|p| p.name.clone()).collect(),
                    );
                    return (None, None);
                };
                (
                    Some(PaginationStyle::Offset {
                        offset_param: off.name.clone(),
                        limit_param,
                        limit,
                        total_path: self.total_path(stream, totals, maybe_totals),
                        rows_path: None,
                        stop_when_short: false,
                    }),
                    Some("Offset".into()),
                )
            }
            (None, None) => {
                let mut hints: Vec<String> = fields
                    .iter()
                    .filter(|(segs, _)| {
                        let n = leaf_norm(segs);
                        n.starts_with("hasmore")
                            || n.starts_with("next")
                            || n == "paging"
                            || n == "pagination"
                    })
                    .map(|(segs, _)| jsonpath(segs, false))
                    .collect();
                hints.sort();
                let sizes: Vec<String> = find(SIZE_PARAMS)
                    .into_iter()
                    .chain(find(LIMIT_PARAMS))
                    .map(|p| p.name.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                if !hints.is_empty() {
                    self.decide(
                        Some(stream),
                        "pagination",
                        "the response looks paginated but no documented parameter drives it (paging documented in prose?)".into(),
                        hints.into_iter().chain(sizes).collect(),
                    );
                } else if !sizes.is_empty() {
                    self.decide(
                        Some(stream),
                        "pagination",
                        "a page-size parameter is documented but nothing advances to the next page; only the first page would be read".into(),
                        sizes,
                    );
                }
                (None, None)
            }
        }
    }

    /// The offset total: one unambiguous total field, never a `count` that
    /// may be the page size.
    fn total_path(
        &mut self,
        stream: &str,
        totals: Vec<String>,
        maybe: Vec<String>,
    ) -> Option<String> {
        match totals.len() {
            1 => totals.into_iter().next(),
            0 => {
                if !maybe.is_empty() {
                    self.decide(
                        Some(stream),
                        "pagination.total_path",
                        "a count field may be the page size rather than the total; left unset (paging stops on an empty page)".into(),
                        maybe,
                    );
                }
                None
            }
            _ => {
                self.decide(
                    Some(stream),
                    "pagination.total_path",
                    "several fields could hold the total; left unset (paging stops on an empty page)".into(),
                    totals,
                );
                None
            }
        }
    }

    fn primary_keys(&mut self, c: &Candidate, stream: &str) -> Vec<String> {
        match &c.item.xpk {
            Some(Value::String(s)) => return vec![s.clone()],
            Some(Value::Array(a)) if !a.is_empty() && a.iter().all(Value::is_string) => {
                return a
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect();
            }
            _ => {}
        }
        if c.item.props.contains_key("id") {
            return vec!["id".into()];
        }
        let cands: Vec<String> = c
            .item
            .props
            .keys()
            .filter(|k| {
                let l = k.to_ascii_lowercase();
                l.ends_with("_id") || k.ends_with("Id") || l == "uuid" || l == "key"
            })
            .cloned()
            .collect();
        let required: Vec<&String> = cands
            .iter()
            .filter(|k| c.item.required.contains(*k))
            .collect();
        if cands.len() == 1 && required.len() == 1 {
            return vec![cands[0].clone()];
        }
        self.decide(
            Some(stream),
            "primary_keys",
            "no `id`, `x-primary-key` or single required key-like property identifies a record"
                .into(),
            cands,
        );
        Vec::new()
    }

    fn incremental(
        &mut self,
        c: &Candidate,
        stream: &str,
        append_only: bool,
    ) -> Option<(String, ReplicationBind)> {
        let words: &[&str] = if append_only {
            &["updated", "modified", "changed", "created"]
        } else {
            &["updated", "modified", "changed"]
        };
        let filters: Vec<&Param> = c
            .params
            .iter()
            .filter(|p| p.location == "query")
            .filter(|p| {
                let l = p.name.to_ascii_lowercase();
                words.iter().any(|w| l.contains(w)) && FILTER_WORDS.iter().any(|w| l.contains(w))
            })
            .collect();
        if filters.is_empty()
            && let Some(p) = c
                .params
                .iter()
                .find(|p| p.location == "query" && p.name.eq_ignore_ascii_case("since"))
        {
            self.decide(
                Some(stream),
                "replication_bind",
                format!(
                    "filter parameter `{}` does not say which field it compares (created or updated)",
                    p.name
                ),
                Vec::new(),
            );
            return None;
        }
        let stamps: Vec<String> = c
            .item
            .props
            .iter()
            .filter(|(k, _)| {
                let n = norm(k);
                TIMESTAMP_FIELDS.contains(&n.as_str()) || append_only && n.starts_with("created")
            })
            .filter(|(_, p)| {
                self.flatten(p, 0)
                    .map(|f| f.date_time() || f.is_integer() || f.types.is_empty())
                    .unwrap_or(false)
            })
            .map(|(k, _)| k.clone())
            .collect();
        match (filters.as_slice(), stamps.as_slice()) {
            ([], _) => None,
            ([f], [s]) => {
                let text = f
                    .schema
                    .description
                    .clone()
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let format = if f.schema.is_integer() {
                    if text.contains("millisecond") {
                        BindFormat::EpochMs
                    } else if text.contains("second") {
                        BindFormat::EpochS
                    } else {
                        self.decide(
                            Some(stream),
                            "replication_bind",
                            format!(
                                "integer filter parameter `{}` does not say whether it takes epoch seconds or milliseconds",
                                f.name
                            ),
                            Vec::new(),
                        );
                        return None;
                    }
                } else if f.schema.format.as_deref() == Some("date") {
                    BindFormat::Date
                } else {
                    BindFormat::Iso8601
                };
                Some((
                    s.clone(),
                    ReplicationBind {
                        into: BindTarget::Query,
                        name: f.name.clone(),
                        path: None,
                        value_type: BindValueType::String,
                        template: faucet_core::replication::BIND_PLACEHOLDER.to_string(),
                        format,
                        advance_from: None,
                    },
                ))
            }
            ([f], []) => {
                self.decide(
                    Some(stream),
                    "replication_key",
                    format!(
                        "filter parameter `{}` but no timestamp field in the records to bookmark",
                        f.name
                    ),
                    Vec::new(),
                );
                None
            }
            ([_], _) => {
                self.decide(
                    Some(stream),
                    "replication_key",
                    "several timestamp fields could be the replication key".into(),
                    stamps,
                );
                None
            }
            _ => {
                self.decide(
                    Some(stream),
                    "replication_bind",
                    "more than one incremental filter parameter".into(),
                    filters.iter().map(|p| p.name.clone()).collect(),
                );
                None
            }
        }
    }
}

fn recognized_param(name: &str) -> bool {
    let n = norm(name);
    [
        CURSOR_PARAMS,
        PAGE_PARAMS,
        SIZE_PARAMS,
        OFFSET_PARAMS,
        LIMIT_PARAMS,
    ]
    .iter()
    .any(|l| l.contains(&n.as_str()))
}

fn path_vars(url: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = url;
    while let Some(start) = rest.find('{') {
        let after = &rest[start + 1..];
        let Some(end) = after.find('}') else { break };
        out.push(after[..end].to_string());
        rest = &after[end + 1..];
    }
    out
}

fn security_list(v: Option<&Value>) -> Vec<BTreeMap<String, Vec<String>>> {
    v.and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_object)
                .map(|req| {
                    req.iter()
                        .map(|(k, scopes)| {
                            let mut s: Vec<String> = scopes
                                .as_array()
                                .map(|x| {
                                    x.iter()
                                        .filter_map(Value::as_str)
                                        .map(str::to_string)
                                        .collect()
                                })
                                .unwrap_or_default();
                            s.sort();
                            (k.clone(), s)
                        })
                        .collect()
                })
                .collect()
        })
        .unwrap_or_default()
}

fn pick_success(responses: &BTreeMap<String, Loc>) -> Option<String> {
    if responses.contains_key("200") {
        return Some("200".into());
    }
    responses
        .keys()
        .find(|k| k.len() == 3 && k.starts_with('2') && k.chars().all(|c| c.is_ascii_digit()))
        .or_else(|| responses.keys().find(|k| k.eq_ignore_ascii_case("2xx")))
        .cloned()
}

fn wait_unit(header: &Value) -> Option<WaitUnit> {
    let text = format!(
        "{} {}",
        header
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        header
            .get("schema")
            .and_then(|s| s.get("description"))
            .and_then(Value::as_str)
            .unwrap_or_default()
    )
    .to_ascii_lowercase();
    let format = header
        .get("schema")
        .and_then(|s| s.get("format"))
        .or_else(|| header.get("format"))
        .and_then(Value::as_str);
    let epoch = text.contains("epoch") || text.contains("unix") || text.contains("timestamp");
    if format == Some("date-time") {
        Some(WaitUnit::Rfc3339)
    } else if epoch && text.contains("millisecond") {
        Some(WaitUnit::EpochMs)
    } else if epoch {
        Some(WaitUnit::EpochS)
    } else if text.contains("millisecond") {
        Some(WaitUnit::Ms)
    } else if text.contains("second") {
        Some(WaitUnit::Seconds)
    } else {
        None
    }
}

fn assign_names(cands: &mut [Candidate]) {
    let segs: Vec<Vec<String>> = cands.iter().map(|c| static_segments(&c.path)).collect();
    let mut take = vec![1usize; cands.len()];
    let render = |i: usize, take: &[usize]| -> String {
        let s = &segs[i];
        if s.is_empty() {
            return "root".into();
        }
        let n = take[i].min(s.len());
        s[s.len() - n..].join("_")
    };
    loop {
        let names: Vec<String> = (0..cands.len()).map(|i| render(i, &take)).collect();
        let mut grew = false;
        for i in 0..cands.len() {
            let dup = names
                .iter()
                .enumerate()
                .any(|(j, n)| j != i && *n == names[i]);
            if dup && take[i] < segs[i].len() {
                take[i] += 1;
                grew = true;
            }
        }
        if !grew {
            break;
        }
    }
    let names: Vec<String> = (0..cands.len()).map(|i| render(i, &take)).collect();
    let mut used: BTreeSet<String> = BTreeSet::new();
    for (c, rendered) in cands.iter_mut().zip(names) {
        let mut base: String = rendered.chars().take(56).collect();
        if crate::expand::RESERVED_IDS.contains(&base.as_str()) {
            base.push_str("_stream");
        }
        let mut name = base.clone();
        let mut n = 2;
        while used.contains(&name) {
            name = format!("{base}_{n}");
            n += 1;
        }
        used.insert(name.clone());
        c.name = name;
    }
}

/// Nested (object / array) record fields across every stream, sorted — the
/// shared `json_encode` list.
fn nested_fields(m: &Mapper<'_>, cands: &[Candidate]) -> CliResult<Vec<String>> {
    let mut out = BTreeSet::new();
    for c in cands {
        for (k, p) in &c.item.props {
            let f = m.flatten(p, 0)?;
            if f.is_object() || f.is_array() {
                out.insert(k.clone());
            }
        }
    }
    Ok(out.into_iter().collect())
}

fn encode_fields(mut v: Value, fields: &[String]) -> Value {
    if let Value::Object(map) = &mut v {
        for f in fields {
            if let Some(x) = map.get_mut(f)
                && matches!(x, Value::Object(_) | Value::Array(_))
            {
                *x = Value::String(faucet_core::util::canonical_json(x));
            }
        }
    }
    v
}

fn schema_hint(m: &Mapper<'_>, item: &Flat) -> CliResult<Option<Value>> {
    if item.props.is_empty() {
        return Ok(None);
    }
    let mut props = Map::new();
    for (k, p) in &item.props {
        let f = m.flatten(p, 0)?;
        let mut types: Vec<Value> = f.types.iter().map(|t| json!(t)).collect();
        if f.nullable && !types.is_empty() {
            types.push(json!("null"));
        }
        let mut entry = Map::new();
        match types.len() {
            0 => {}
            1 => {
                entry.insert("type".into(), types.remove(0));
            }
            _ => {
                entry.insert("type".into(), Value::Array(types));
            }
        }
        if let Some(fmt) = &f.format {
            entry.insert("format".into(), json!(fmt));
        }
        props.insert(k.clone(), Value::Object(entry));
    }
    let mut schema = Map::new();
    schema.insert("type".into(), json!("object"));
    schema.insert("properties".into(), Value::Object(props));
    if !item.required.is_empty() {
        schema.insert(
            "required".into(),
            Value::Array(item.required.iter().map(|r| json!(r)).collect()),
        );
    }
    Ok(Some(Value::Object(schema)))
}

/// Synthetic values for every param a run must supply.
fn fake_params(params: &ParamsSpec) -> BTreeMap<String, Value> {
    params
        .iter()
        .filter(|(_, p)| p.required && p.default.is_none())
        .map(|(k, p)| {
            let v = if k == API_ROOT {
                "https://api.example.invalid".to_string()
            } else if p.secret {
                format!("suite-{}", k.replace('_', "-"))
            } else {
                format!("suite-{k}")
            };
            (k.clone(), Value::String(v))
        })
        .collect()
}

/// Every key a REST source config may carry, from the connector's schema.
fn rest_keys() -> BTreeSet<String> {
    serde_json::to_value(schemars::schema_for!(faucet_source_rest::RestStreamConfig))
        .ok()
        .and_then(|s| s.get("properties").and_then(Value::as_object).cloned())
        .map(|p| p.keys().cloned().collect())
        .unwrap_or_default()
}

fn check_keys(cfg: &Map<String, Value>) -> CliResult<()> {
    let known = rest_keys();
    match cfg.keys().find(|k| !known.contains(*k)) {
        Some(k) => Err(CliError::Internal(format!(
            "hub import: `{k}` is not a REST source config field"
        ))),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    fn set(v: Value) -> SpecSet {
        SpecSet::single(Url::parse("file:///spec/api.json").unwrap(), v)
    }

    fn opts() -> ImportOptions {
        ImportOptions {
            owner: "acme".into(),
            test_sink: "sink-templates/faucet-hq/sqlite.yaml".into(),
            ..Default::default()
        }
    }

    fn base(paths: Value) -> Value {
        json!({
            "openapi": "3.0.3",
            "info": {"title": "Demo API"},
            "servers": [{"url": "https://api.example.com"}],
            "paths": paths,
        })
    }

    fn ok(schema: Value) -> Value {
        json!({"200": {"description": "ok", "content": {"application/json": {"schema": schema}}}})
    }

    fn list(params: Value, schema: Value) -> Value {
        json!({"get": {"parameters": params, "responses": ok(schema)}})
    }

    fn items(props: Value) -> Value {
        json!({"type": "array", "items": {"type": "object", "properties": props}})
    }

    fn imp(v: Value) -> Imported {
        import(&set(v), &opts()).unwrap()
    }

    fn err(v: Value) -> String {
        import(&set(v), &opts()).unwrap_err().to_string()
    }

    fn stream<'a>(i: &'a Imported, name: &str) -> &'a Stream {
        i.template
            .streams
            .iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("no stream {name}: {:?}", i.template.stream_names()))
    }

    fn cfg(i: &Imported, name: &str) -> Value {
        let mut c = i.template.source.config.clone();
        for (k, v) in stream(i, name).source.config.as_object().unwrap() {
            c[k] = v.clone();
        }
        c
    }

    fn fields(i: &Imported) -> Vec<String> {
        i.report.decisions.iter().map(|d| d.field.clone()).collect()
    }

    fn one(path: &str, op: Value) -> Imported {
        imp(base(json!({ path: op })))
    }

    #[test]
    fn versions_and_shapes() {
        let mut v2 = json!({"swagger": "2.0", "info": {"title": "T"}, "host": "api.example.com",
            "basePath": "/v2", "schemes": ["http", "https"],
            "paths": {"/a": {"get": {"responses": {"200": {"description": "", "schema": items(json!({"id": {"type": "integer"}}))}}}}}});
        let i = imp(v2.clone());
        assert_eq!(i.report.spec_version, "2.0");
        assert_eq!(
            i.template.params[API_ROOT].default,
            Some(json!("https://api.example.com/v2"))
        );
        v2["schemes"] = json!(["http"]);
        assert_eq!(
            imp(v2.clone()).template.params[API_ROOT].default,
            Some(json!("http://api.example.com/v2"))
        );
        v2.as_object_mut().unwrap().remove("host");
        let i = imp(v2.clone());
        assert!(i.template.params[API_ROOT].required);
        assert_eq!(i.report.servers, vec!["/v2".to_string()]);
        assert!(err(json!({"openapi": "4.0.0", "paths": {}})).contains("4.0.0"));
        assert!(err(json!({"swagger": "1.2", "paths": {}})).contains("1.2"));
        assert!(err(json!({"swagger": 2, "paths": {}})).contains("nothing to import"));
        assert!(err(json!({"title": "x"})).contains("HTML page"));
        assert!(err(json!("<html>")).contains("not an OpenAPI"));
        assert!(err(json!({"openapi": 3.1})).contains("no `paths`"));
        assert!(err(base(json!({}))).contains("nothing to import"));
        let mut untitled = base(json!({"/a": list(json!([]), items(json!({"id": {}})))}));
        untitled["info"]["title"] = json!("!!!");
        assert!(err(untitled.clone()).contains("--name"));
        let named = import(
            &set(untitled),
            &ImportOptions {
                name: Some("demo".into()),
                ..opts()
            },
        )
        .unwrap();
        assert_eq!(named.template.name, "demo");
    }

    #[test]
    fn servers_map_to_the_api_root_param() {
        let mut v = base(json!({"/a": list(json!([]), items(json!({"id": {}})))}));
        v["servers"] = json!([
            {"url": "https://{region}.example.com/v1/", "variables": {"region": {"default": "eu"}}},
            {"url": "https://{host}.example.com", "variables": {}},
            {"description": "no url"},
        ]);
        let i = imp(v.clone());
        assert_eq!(
            i.template.params[API_ROOT].default,
            Some(json!("https://eu.example.com/v1"))
        );
        let pick = |s: &str| {
            import(
                &set(v.clone()),
                &ImportOptions {
                    server: Some(s.into()),
                    ..opts()
                },
            )
        };
        let second = pick("1").unwrap();
        assert!(second.template.params[API_ROOT].required);
        assert_eq!(second.report.decisions[0].field, "params.api_root");
        assert!(second.report.decisions[0].candidates[0].contains("{host}"));
        assert!(
            pick("7")
                .unwrap_err()
                .to_string()
                .contains("lists 2 server")
        );
        assert_eq!(
            pick("https://sandbox.example.com").unwrap().template.params[API_ROOT].default,
            Some(json!("https://sandbox.example.com"))
        );
        v["servers"] = json!([{"url": "/api"}]);
        let rel = imp(v.clone());
        assert!(rel.template.params[API_ROOT].required);
        let web = SpecSet::single(
            Url::parse("https://docs.example.com/spec/openapi.json").unwrap(),
            v.clone(),
        );
        let i = import(&web, &opts()).unwrap();
        assert_eq!(
            i.template.params[API_ROOT].default,
            Some(json!("https://docs.example.com/api"))
        );
        v.as_object_mut().unwrap().remove("servers");
        let none = imp(v);
        assert!(none.template.params[API_ROOT].required);
        assert!(fields(&none).contains(&"params.api_root".to_string()));
    }

    fn with_auth(schemes: Value, security: Value) -> Imported {
        let mut v = base(json!({"/a": list(json!([]), items(json!({"id": {}})))}));
        v["components"] = json!({"securitySchemes": schemes});
        v["security"] = security;
        imp(v)
    }

    #[test]
    fn security_schemes_map_to_auth() {
        let auth = |s: Value| {
            let i = with_auth(json!({"s": s}), json!([{"s": ["read"]}]));
            (i.template.source.config.get("auth").cloned(), i)
        };
        let (a, i) = auth(json!({"type": "http", "scheme": "Bearer"}));
        assert_eq!(
            a.unwrap(),
            json!({"type": "bearer", "config": {"token": "${param.api_token}"}})
        );
        assert!(
            i.template.params["api_token"].secret
                && i.template.params["api_token"].default.is_none()
        );
        let (a, _) = auth(json!({"type": "http", "scheme": "basic"}));
        assert_eq!(
            a.unwrap()["config"]["password"],
            json!("${param.api_password}")
        );
        let (a, _) = auth(json!({"type": "basic"}));
        assert_eq!(a.unwrap()["type"], json!("basic"));
        let (a, _) = auth(json!({"type": "apiKey", "in": "header", "name": "X-Key"}));
        assert_eq!(
            a.unwrap(),
            json!({"type": "api_key", "config": {"header": "X-Key", "value": "${param.api_key}"}})
        );
        let (a, _) = auth(json!({"type": "apiKey", "in": "query", "name": "key"}));
        assert_eq!(a.unwrap()["config"]["param"], json!("key"));
        let (a, _) = auth(
            json!({"type": "oauth2", "flows": {"clientCredentials": {"tokenUrl": "https://auth.example.com/token", "scopes": {}}}}),
        );
        let a = a.unwrap();
        assert_eq!(a["type"], json!("oauth2"));
        assert_eq!(a["config"]["scopes"], json!(["read"]));
        for (s, needle) in [
            (
                json!({"type": "apiKey", "in": "cookie", "name": "c"}),
                "cookie",
            ),
            (json!({"type": "http", "scheme": "digest"}), "digest"),
            (
                json!({"type": "oauth2", "flows": {"authorizationCode": {}}}),
                "interactive",
            ),
            (
                json!({"type": "oauth2", "flows": {"clientCredentials": {"tokenUrl": "/token"}}}),
                "not absolute",
            ),
            (json!({"type": "openIdConnect"}), "openIdConnect"),
            (json!({"type": "mutualTLS"}), "mutualTLS"),
        ] {
            let (a, i) = auth(s);
            assert!(a.is_none());
            assert!(
                i.report.unsupported[0].message.contains(needle),
                "{:?}",
                i.report.unsupported
            );
            assert!(fields(&i).contains(&"source.config.auth".to_string()));
        }
        let missing = with_auth(json!({}), json!([{"ghost": []}]));
        assert!(
            missing.report.unsupported[0]
                .message
                .contains("no declared scheme")
        );
        let none = with_auth(json!({}), json!([]));
        assert!(none.template.source.config.get("auth").is_none());
    }

    #[test]
    fn swagger_oauth_and_mixed_requirements() {
        let v = json!({"swagger": "2.0", "info": {"title": "T"}, "host": "h.example.com",
        "securityDefinitions": {"o": {"type": "oauth2", "flow": "application", "tokenUrl": "https://h.example.com/t"},
                                "k": {"type": "apiKey", "in": "header", "name": "K"}},
        "security": [{"o": []}],
        "paths": {
            "/a": {"get": {"responses": {"200": {"description": "", "schema": items(json!({"id": {}}))}}}},
            "/b": {"get": {"security": [{"k": []}], "responses": {"200": {"description": "", "schema": items(json!({"id": {}}))}}}},
            "/c": {"get": {"security": [{}], "responses": {"200": {"description": "", "schema": items(json!({"id": {}}))}}}},
            "/d": {"get": {"security": [{"o": [], "k": []}], "responses": {"200": {"description": "", "schema": items(json!({"id": {}}))}}}},
            "/e": {"get": {"responses": {"200": {"description": "", "schema": items(json!({"id": {}}))}}}},
        }});
        let i = imp(v);
        assert_eq!(i.template.source.config["auth"]["type"], json!("oauth2"));
        let auth: Vec<&Decision> = i
            .report
            .decisions
            .iter()
            .filter(|d| d.field == "auth")
            .collect();
        assert_eq!(auth.len(), 2, "{auth:?}");
        assert_eq!(auth[1].candidates, vec!["k + o".to_string()]);
        let v = json!({"swagger": "2.0", "info": {"title": "T"},
            "securityDefinitions": {"o": {"type": "oauth2", "flow": "implicit"}},
            "security": [{"o": []}],
            "paths": {"/a": {"get": {"responses": {"200": {"description": "", "schema": items(json!({"id": {}}))}}}}}});
        assert!(imp(v).report.unsupported[0].message.contains("interactive"));
    }

    #[test]
    fn records_paths_follow_the_response_shape() {
        let wrap = |schema: Value| one("/things", list(json!([]), schema));
        assert_eq!(
            cfg(&wrap(items(json!({"id": {}}))), "things").get("records_path"),
            None
        );
        let i = wrap(
            json!({"type": "object", "properties": {"data": items(json!({"id": {}})), "meta": {"type": "object"}}}),
        );
        assert_eq!(cfg(&i, "things")["records_path"], json!("$.data[*]"));
        let i = wrap(
            json!({"properties": {"result": {"type": "object", "properties": {"rows": {"type": "array", "items": {"type": "string"}}}}}}),
        );
        assert_eq!(cfg(&i, "things")["records_path"], json!("$.result.rows[*]"));
        let i = wrap(
            json!({"properties": {"a": items(json!({"id": {}})), "b": items(json!({"x": {}}))}}),
        );
        assert!(cfg(&i, "things").get("records_path").is_none());
        assert_eq!(
            i.report.decisions[0].candidates,
            vec!["$.a[*]".to_string(), "$.b[*]".to_string()]
        );
        let i = wrap(
            json!({"properties": {"items": items(json!({"id": {}})), "warnings": items(json!({"x": {}}))}}),
        );
        assert_eq!(cfg(&i, "things")["records_path"], json!("$.items[*]"));
        let i = wrap(json!({"properties": {"odd key": items(json!({"id": {}}))}}));
        assert_eq!(cfg(&i, "things")["records_path"], json!("$['odd key'][*]"));
        let i = wrap(
            json!({"type": ["array", "null"], "items": {"allOf": [{"properties": {"id": {}}}, {"properties": {"x": {"type": "null"}}}]}}),
        );
        assert_eq!(stream(&i, "things").primary_keys, vec!["id".to_string()]);
        let i = wrap(json!({"oneOf": [items(json!({"id": {}}))]}));
        assert_eq!(stream(&i, "things").primary_keys, vec!["id".to_string()]);
        let mut v = base(json!({
            "/one": list(json!([]), json!({"type": "object", "properties": {"id": {}, "tags": items(json!({"x": {}}))}})),
            "/scalars": list(json!([]), json!({"properties": {"tags": {"type": "array", "items": {"type": "string"}}}})),
            "/num": list(json!([]), json!({"type": "integer"})),
            "/ok": list(json!([]), json!({"items": {"$ref": "#/components/schemas/T"}})),
        }));
        v["components"] = json!({"schemas": {"T": {"properties": {"id": {}}}}});
        let i = imp(v);
        assert_eq!(i.template.stream_names(), vec!["ok"]);
        assert_eq!(i.report.skipped.len(), 3);
    }

    #[test]
    fn responses_and_media_types() {
        let resp = |r: Value| one("/x", json!({"get": {"responses": r}}));
        let body = json!({"schema": items(json!({"id": {}}))});
        assert_eq!(
            resp(json!({"201": {"content": {"application/vnd.api+json": body}}}))
                .template
                .streams
                .len(),
            1
        );
        assert_eq!(
            resp(json!({"2XX": {"content": {"*/*": body}}}))
                .template
                .streams
                .len(),
            1
        );
        let none = import(
            &set(base(
                json!({"/x": {"get": {"responses": {"404": {}}}}, "/y": {"get": {"responses": {"200": {"content": {"text/csv": body}}}}}, "/z": {"get": {"responses": {"200": {"content": {"application/json": {}}}}}}}),
            )),
            &opts(),
        );
        assert!(none.unwrap_err().to_string().contains("nothing to import"));
        let i = imp(base(json!({
            "/x": {"get": {"responses": {"404": {}}}},
            "/y": {"get": {"responses": {"200": {"content": {"text/csv": body}}}}},
            "/z": {"get": {"responses": {"200": {"content": {"application/json": {}}}}}},
            "/w": {"get": {"responses": {"200": {}}}},
            "/ok": list(json!([]), items(json!({"id": {}}))),
        })));
        let reasons: Vec<&str> = i.report.skipped.iter().map(|s| s.reason.as_str()).collect();
        assert!(reasons.iter().any(|r| r.contains("no 2xx")), "{reasons:?}");
        assert_eq!(
            reasons
                .iter()
                .filter(|r| r.contains("no JSON schema"))
                .count(),
            3
        );
    }

    #[test]
    fn child_streams_need_a_parent_list() {
        let parent = items(json!({"id": {"type": "integer"}, "slug": {"type": "string"}}));
        let child = items(json!({"id": {}}));
        let i = imp(base(json!({
            "/orgs": list(json!([]), parent.clone()),
            "/orgs/{org_id}/users": list(json!([{"name": "org_id", "in": "path", "required": true}]), child.clone()),
            "/orgs/{slug}/teams": list(json!([]), child.clone()),
            "/orgs/{owner}/repos": list(json!([]), child.clone()),
            "/ghost/{id}/x": list(json!([]), child.clone()),
            "/a/{x}/b/{y}/c": list(json!([]), child.clone()),
            "/orgs/{org_id}": list(json!([]), parent.clone()),
        })));
        let users = stream(&i, "users");
        assert_eq!(users.parent.as_deref(), Some("orgs"));
        assert_eq!(users.parent_key, None);
        assert_eq!(users.source.config["path"], json!("/orgs/${orgs.id}/users"));
        let teams = stream(&i, "teams");
        assert_eq!(teams.parent_key.as_deref(), Some("slug"));
        assert_eq!(i.template.streams.len(), 3);
        let reasons: Vec<String> = i
            .report
            .skipped
            .iter()
            .map(|s| format!("{} {}", s.path, s.reason))
            .collect();
        assert!(
            reasons
                .iter()
                .any(|r| r.starts_with("/orgs/{owner}/repos") && r.contains("fields: id, slug")),
            "{reasons:?}"
        );
        assert!(
            reasons
                .iter()
                .any(|r| r.starts_with("/ghost") && r.contains("does not resolve"))
        );
        assert!(
            reasons
                .iter()
                .any(|r| r.starts_with("/a/") && r.contains("2 path parameters"))
        );
        assert!(
            reasons
                .iter()
                .any(|r| r.starts_with("/orgs/{org_id} ") && r.contains("lookup"))
        );
        assert!(i.suite.suite.behavioral.is_empty());
    }

    fn paged(params: Value, schema: Value, headers: Value) -> (Option<Value>, Imported) {
        let i = one(
            "/things",
            json!({"get": {"parameters": params, "responses": {
            "200": {"headers": headers, "content": {"application/json": {"schema": schema}}}}}}),
        );
        (cfg(&i, "things").get("pagination").cloned(), i)
    }

    fn q(name: &str, schema: Value) -> Value {
        json!({"name": name, "in": "query", "schema": schema})
    }

    fn env(extra: Value) -> Value {
        let mut props = json!({"data": items(json!({"id": {}}))});
        for (k, v) in extra.as_object().unwrap() {
            props[k] = v.clone();
        }
        json!({"type": "object", "properties": props})
    }

    #[test]
    fn pagination_styles() {
        let (p, _) = paged(
            json!([q("starting_after", json!({}))]),
            env(json!({"nextCursor": {"type": "string"}})),
            json!({}),
        );
        assert_eq!(
            p.unwrap(),
            json!({"type": "Cursor", "next_token_path": "$.nextCursor", "param_name": "starting_after"})
        );
        let (p, i) = paged(json!([q("cursor", json!({}))]), env(json!({})), json!({}));
        assert!(p.is_none() && i.report.decisions[0].message.contains("no response field"));
        let (p, i) = paged(
            json!([]),
            env(json!({"next_cursor": {"type": "string"}})),
            json!({}),
        );
        assert!(
            p.is_none()
                && i.report.decisions[0]
                    .message
                    .contains("no request parameter")
        );
        let (p, _) = paged(
            json!([q("page", json!({}))]),
            items(json!({"id": {}})),
            json!({"Link": {}}),
        );
        assert_eq!(p.unwrap(), json!({"type": "LinkHeader"}));
        let (p, _) = paged(
            json!([]),
            env(json!({"@odata.nextLink": {"type": "string"}})),
            json!({}),
        );
        assert_eq!(p.unwrap()["next_link_path"], json!("$['@odata.nextLink']"));
        let (p, _) = paged(
            json!([]),
            env(
                json!({"_links": {"properties": {"next": {"properties": {"href": {"type": "string"}}}}}, "next": {"type": "object", "properties": {"href": {"type": "string"}}}}),
            ),
            json!({}),
        );
        assert_eq!(p.unwrap()["next_link_path"], json!("$.next.href"));
        let (p, i) = paged(
            json!([]),
            env(json!({"next_url": {"type": "string"}, "nextLink": {"type": "string"}})),
            json!({}),
        );
        assert!(p.is_none() && i.report.decisions[0].candidates.len() == 2);
        let (p, i) = paged(
            json!([q("cursor", json!({}))]),
            env(json!({"next_cursor": {}})),
            json!({"link": {}}),
        );
        assert!(
            p.is_none()
                && i.report.decisions[0]
                    .message
                    .contains("more than one pagination signal")
        );
        let (p, _) = paged(
            json!([
                q("page", json!({"type": "integer", "minimum": 0})),
                q("page_size", json!({"type": "integer"}))
            ]),
            items(json!({"id": {}})),
            json!({}),
        );
        assert_eq!(
            p.unwrap(),
            json!({"type": "PageNumber", "param_name": "page", "start_page": 0, "page_size": null, "page_size_param": null})
        );
        let (p, _) = paged(
            json!([q("skip", json!({})), q("top", json!({"maximum": 1000}))]),
            env(json!({"meta": {"properties": {"total_count": {"type": "integer"}}}})),
            json!({}),
        );
        assert_eq!(
            p.unwrap(),
            json!({"type": "Offset", "offset_param": "skip", "limit_param": "top", "limit": 1000, "total_path": "$.meta.total_count", "stop_when_short": false})
        );
        let (p, i) = paged(
            json!([q("offset", json!({})), q("limit", json!({}))]),
            items(json!({"id": {}})),
            json!({}),
        );
        assert!(p.is_none() && i.report.decisions[0].candidates == vec!["limit".to_string()]);
        let (p, i) = paged(
            json!([q("offset", json!({})), q("page", json!({}))]),
            items(json!({"id": {}})),
            json!({}),
        );
        assert!(
            p.is_none()
                && i.report.decisions[0]
                    .message
                    .contains("both page-number and offset")
        );
        let (p, i) = paged(
            json!([]),
            env(json!({"has_more": {"type": "boolean"}})),
            json!({}),
        );
        assert!(p.is_none() && i.report.decisions[0].message.contains("prose"));
        let (p, i) = paged(
            json!([q("cursor", json!({})), q("marker", json!({}))]),
            env(json!({"next_cursor": {}})),
            json!({}),
        );
        assert!(p.is_none() && i.report.decisions[0].candidates.len() == 3);
    }

    #[test]
    fn keys_cursors_and_write_preferences() {
        let i = imp(base(json!({
            "/a": list(json!([]), items(json!({"account_id": {}, "region": {}}))),
            "/b": list(json!([]), json!({"type": "array", "items": {"x-primary-key": ["org", "n"], "properties": {"org": {}, "n": {}}}})),
            "/c": list(json!([]), json!({"type": "array", "items": {"required": ["uuid"], "properties": {"uuid": {}, "x": {}}}})),
            "/d": list(json!([q("updated_since", json!({"type": "integer", "description": "Unix time in seconds"}))]), items(json!({"id": {}, "updated_at": {"type": "integer"}}))),
            "/e": list(json!([q("modified_after", json!({"format": "date"}))]), items(json!({"id": {}, "modifiedAt": {"format": "date-time"}}))),
            "/f": list(json!([q("updated_after", json!({}))]), items(json!({"id": {}}))),
            "/g": list(json!([q("since", json!({}))]), items(json!({"id": {}, "updated_at": {}, "modified": {}}))),
            "/g2": list(json!([q("updated_since", json!({}))]), items(json!({"id": {}, "updated_at": {}, "modified": {}}))),
            "/h": list(json!([q("updated_since", json!({})), q("modified_since", json!({}))]), items(json!({"id": {}, "updated_at": {}}))),
            "/audit/logs": list(json!([q("created_after", json!({"format": "date-time"}))]), items(json!({"id": {}, "created_at": {"format": "date-time"}}))),
            "/req": list(json!([{"name": "account", "in": "query", "required": true}, {"name": "limit", "in": "query", "required": true}]), items(json!({"id": {}}))),
        })));
        assert!(stream(&i, "a").primary_keys.is_empty());
        assert_eq!(
            stream(&i, "b").primary_keys,
            vec!["org".to_string(), "n".to_string()]
        );
        assert_eq!(stream(&i, "c").primary_keys, vec!["uuid".to_string()]);
        assert_eq!(cfg(&i, "d")["replication_bind"]["format"], json!("epoch_s"));
        assert_eq!(
            stream(&i, "d").write,
            WriteChoice::Many(vec![WriteMode::Upsert, WriteMode::Append])
        );
        assert_eq!(cfg(&i, "e")["replication_bind"]["format"], json!("date"));
        assert_eq!(cfg(&i, "e")["replication_key"], json!("modifiedAt"));
        assert!(cfg(&i, "f").get("replication_key").is_none());
        assert!(cfg(&i, "g").get("replication_key").is_none());
        assert!(cfg(&i, "h").get("replication_key").is_none());
        assert_eq!(
            stream(&i, "f").write,
            WriteChoice::Many(vec![WriteMode::Overwrite, WriteMode::Upsert])
        );
        assert_eq!(cfg(&i, "logs")["replication_key"], json!("created_at"));
        assert_eq!(
            stream(&i, "logs").write,
            WriteChoice::One(WriteMode::Append)
        );
        let by: Vec<(String, String)> = i
            .report
            .decisions
            .iter()
            .map(|d| (d.stream.clone().unwrap_or_default(), d.field.clone()))
            .collect();
        for want in [
            ("a", "primary_keys"),
            ("f", "replication_key"),
            ("g", "replication_bind"),
            ("g2", "replication_key"),
            ("h", "replication_bind"),
            ("req", "source.config.query_params"),
        ] {
            assert!(
                by.contains(&(want.0.to_string(), want.1.to_string())),
                "{want:?} not in {by:?}"
            );
        }
        assert_eq!(
            by.iter()
                .filter(|(s, f)| s == "req" && f == "source.config.query_params")
                .count(),
            1
        );
    }

    #[test]
    fn nested_fields_schema_hints_and_examples() {
        let mut v = base(json!({
            "/a": {"get": {"responses": {"200": {"content": {"application/json": {
                "schema": {"properties": {"data": items(json!({"id": {"type": "integer"}, "meta": {"type": "object", "nullable": true}, "n": {"type": ["string", "null"]}, "free": {}}))}},
                "examples": {"z": {"value": {}}, "a": {"$ref": "#/components/examples/A"}}}}}}}},
            "/b": {"get": {"responses": {"200": {"content": {"application/json": {
                "schema": {"type": "array", "example": [{"id": 1, "tags": ["x"]}, 2], "items": {"properties": {"id": {}, "tags": {"type": "array"}}}}}}}}}},
            "/c": {"get": {"responses": {"200": {"content": {"application/json": {
                "schema": {"type": "array", "items": {"properties": {"id": {}}}}, "example": []}}}}}},
        }));
        v["components"] = json!({"examples": {"A": {"value": {"data": [{"id": 7, "meta": {"k": 1}, "n": null}]}}}});
        let i = imp(v);
        assert_eq!(
            i.template.transforms[0].config,
            json!({"fields": ["meta", "tags"]})
        );
        let schema = &cfg(&i, "a")["schema"];
        assert_eq!(
            schema["properties"]["meta"],
            json!({"type": ["object", "null"]})
        );
        assert_eq!(
            schema["properties"]["n"],
            json!({"type": ["string", "null"]})
        );
        assert_eq!(schema["properties"]["free"], json!({}));
        let b = &i.suite.suite.behavioral;
        assert_eq!(b.len(), 2);
        assert_eq!(b[0].expect["records"][0]["meta"], json!("{\"k\":1}"));
        assert_eq!(b[1].input, json!([{"id": 1, "tags": ["x"]}]));
        assert_eq!(b[1].expect["records"][0]["tags"], json!("[\"x\"]"));
        assert_eq!(i.report.streams[0].example_records, 1);
        assert_eq!(i.report.streams[2].example_records, 0);
        let v2 = json!({"swagger": "2.0", "info": {"title": "T"}, "host": "h.example.com",
            "paths": {"/a": {"get": {"parameters": [{"name": "page", "in": "query", "type": "integer", "default": 1}],
                "responses": {"200": {"schema": items(json!({"id": {}})), "examples": {"application/json": [{"id": 3}]}}}}}}});
        let i = imp(v2);
        assert_eq!(i.suite.suite.behavioral[0].input, json!([{"id": 3}]));
        assert_eq!(cfg(&i, "a")["pagination"]["start_page"], json!(1));
    }

    #[test]
    fn rate_limits_become_retry_rules() {
        let rl = |responses: Value| {
            let i = one("/a", json!({"get": {"responses": responses}}));
            (
                i.template.source.config.get("retry_on_response").cloned(),
                i,
            )
        };
        let body = json!({"application/json": {"schema": items(json!({"id": {}}))}});
        let (r, _) = rl(json!({"200": {"content": body}, "429": {}}));
        assert_eq!(r.unwrap(), json!([{"status": [429], "values": []}]));
        let (r, _) = rl(json!({"200": {"content": body, "headers": {"Retry-After": {}}}}));
        assert_eq!(r.unwrap()[0]["status"], json!([429]));
        let (r, _) = rl(json!({"200": {"content": body}}));
        assert!(r.is_none());
        for (h, unit) in [
            (json!({"description": "seconds until reset"}), "seconds"),
            (
                json!({"description": "reset time, unix epoch in milliseconds"}),
                "epoch_ms",
            ),
            (json!({"description": "wait in milliseconds"}), "ms"),
            (json!({"schema": {"format": "date-time"}}), "rfc3339"),
            (
                json!({"schema": {"description": "Epoch seconds"}}),
                "epoch_s",
            ),
        ] {
            let (r, _) = rl(
                json!({"200": {"content": body, "headers": {"X-RateLimit-Reset": h, "X-RateLimit-Remaining": {}}}}),
            );
            let r = r.unwrap();
            assert_eq!(r[0]["backoff_from"]["config"]["unit"], json!(unit));
            assert_eq!(r[1]["match_success"], json!(true));
            assert_eq!(r[1]["header"], json!("x-ratelimit-remaining"));
        }
        let (r, i) = rl(json!({"200": {"content": body, "headers": {"RateLimit-Reset": {}}}}));
        assert_eq!(r.unwrap(), json!([{"status": [429], "values": []}]));
        assert_eq!(
            fields(&i),
            vec!["source.config.retry_on_response".to_string()]
        );
    }

    #[test]
    fn names_filters_and_limits() {
        let it = || items(json!({"id": {}}));
        let i = imp(base(json!({
            "/v1/users": list(json!([]), it()),
            "/v2/users": list(json!([]), it()),
            "/param": list(json!([]), it()),
            "/": list(json!([]), it()),
            "/Users": list(json!([]), it()),
            "/listOrders": {"get": {"summary": "  ", "responses": ok(it())}},
        })));
        let mut names = i.template.stream_names();
        names.sort();
        assert_eq!(
            names,
            vec![
                "list_orders",
                "param_stream",
                "root",
                "users",
                "v1_users",
                "v2_users"
            ]
        );
        assert!(stream(&i, "list_orders").description.is_none());
        let filtered = import(
            &set(base(json!({"/v1/a": list(json!([]), it()), "/v1/b": list(json!([]), it()), "/v2/a": list(json!([]), it())}))),
            &ImportOptions { include: vec!["/v1/*".into()], exclude: vec!["*/b".into()], ..opts() },
        )
        .unwrap();
        assert_eq!(filtered.template.stream_names(), vec!["a"]);
        let mut many = Map::new();
        for n in 0..=MAX_OPERATIONS {
            many.insert(format!("/r{n}"), list(json!([]), it()));
        }
        assert!(err(base(Value::Object(many.clone()))).contains("--include"));
        let narrowed = import(
            &set(base(Value::Object(many))),
            &ImportOptions {
                include: vec!["/r1".into()],
                ..opts()
            },
        )
        .unwrap();
        assert_eq!(narrowed.template.streams.len(), 1);
        let dup = imp(base(
            json!({"/a/{id}/x": list(json!([]), it()), "/a": list(json!([]), it()), "/b/{id}/x": list(json!([]), it()), "/b": list(json!([]), it())}),
        ));
        let mut names = dup.template.stream_names();
        names.sort();
        assert_eq!(names, vec!["a", "a_x", "b", "b_x"]);
    }

    #[test]
    fn shared_values_move_to_the_source() {
        let page = json!([q("page", json!({}))]);
        let i = imp(base(json!({
            "/a": list(page.clone(), env(json!({}))),
            "/b": list(page, env(json!({}))),
        })));
        assert_eq!(i.template.source.config["records_path"], json!("$.data[*]"));
        assert_eq!(
            i.template.source.config["pagination"]["type"],
            json!("PageNumber")
        );
        assert!(stream(&i, "a").source.config.get("pagination").is_none());
    }

    #[test]
    fn parameters_merge_and_resolve() {
        let mut v = base(json!({"/a": {
            "parameters": [{"$ref": "#/components/parameters/Page"}, {"name": "", "in": "query"}],
            "get": {"parameters": [{"name": "page", "in": "query", "schema": {"type": "integer", "default": 0}}], "responses": ok(items(json!({"id": {}})))},
        }}));
        v["components"] = json!({"parameters": {"Page": {"name": "page", "in": "query", "schema": {"type": "integer", "default": 1}}}});
        v["info"]["description"] = json!("\n\nFirst line.\nSecond.");
        v["externalDocs"] = json!({"url": "ftp://nope"});
        let i = imp(v);
        assert_eq!(cfg(&i, "a")["pagination"]["start_page"], json!(0));
        assert_eq!(
            i.template.description.as_deref(),
            Some("Demo API — First line.")
        );
        assert!(i.template.docs.is_none());
        assert_eq!(i.report.template, "acme/demo-api");
        assert_eq!(i.suite.suite.cases[0].params, BTreeMap::new());
    }

    #[test]
    fn deep_compositions_are_refused() {
        let mut v = base(json!({
            "/a": list(json!([]), json!({"$ref": "#/components/schemas/L"})),
            "/b": list(json!([]), items(json!({"id": {}}))),
        }));
        v["components"] =
            json!({"schemas": {"L": {"allOf": [{"$ref": "#/components/schemas/L"}]}}});
        let i = imp(v);
        assert_eq!(i.template.stream_names(), vec!["b"]);
        assert!(i.report.skipped[0].reason.contains("nested deeper"));
    }

    #[test]
    fn edge_shapes() {
        let i = one("/blobs", list(json!([]), json!({"type": "array"})));
        assert!(stream(&i, "blobs").primary_keys.is_empty());
        assert!(cfg(&i, "blobs").get("schema").is_none());
        let v2 = json!({"swagger": "2.0", "info": {"title": "T"}, "host": "h.example.com",
        "paths": {
            "/a": {"get": {"responses": {"200": {"description": "no body"}}}},
            "/b": {"get": {"responses": {"200": {"schema": items(json!({"id": {}})), "examples": {"text/plain": "x", "application/json": [{"id": 1}], "application/hal+json": [{"id": 2}]}}}}},
        }});
        let i = imp(v2);
        assert_eq!(
            i.report.skipped[0].reason,
            "response 200 has no JSON schema"
        );
        assert_eq!(i.suite.suite.behavioral[0].input, json!([{"id": 2}]));
        let mut v = base(json!({"/a": {"$ref": "#/components/pathItems/A"}}));
        v["components"] = json!({"pathItems": {"A": list(json!([]), items(json!({"id": {}})))}});
        assert_eq!(imp(v).template.stream_names(), vec!["a"]);
        let mut v = base(json!({"/a": list(json!([]), items(json!({"id": {}})))}));
        v["components"] = json!({"securitySchemes": {"s": {"$ref": "#/nope"}}});
        v["security"] = json!([{"s": []}]);
        assert!(
            imp(v).report.unsupported[0]
                .message
                .contains("no declared scheme")
        );
    }

    fn decided<'a>(i: &'a Imported, field: &str) -> Vec<&'a Decision> {
        i.report
            .decisions
            .iter()
            .filter(|d| d.field == field)
            .collect()
    }

    #[test]
    fn count_fields_are_never_an_offset_total() {
        let page = json!([q("offset", json!({})), q("limit", json!({"default": 25}))]);
        let (p, i) = paged(
            page.clone(),
            env(json!({"count": {"type": "integer"}})),
            json!({}),
        );
        assert!(p.unwrap()["total_path"].is_null());
        assert_eq!(
            decided(&i, "pagination.total_path")[0].candidates,
            vec!["$.count".to_string()]
        );
        let (p, i) = paged(
            page.clone(),
            env(json!({"count": {"type": "integer"}, "total": {"type": "integer"}})),
            json!({}),
        );
        assert_eq!(p.unwrap()["total_path"], json!("$.total"));
        assert!(decided(&i, "pagination.total_path").is_empty());
        let (p, i) = paged(
            page,
            env(
                json!({"total": {"type": "integer"}, "meta": {"properties": {"total_count": {"type": "integer"}}}}),
            ),
            json!({}),
        );
        assert!(p.unwrap()["total_path"].is_null());
        assert_eq!(decided(&i, "pagination.total_path")[0].candidates.len(), 2);
    }

    #[test]
    fn a_page_size_without_a_driver_is_a_decision() {
        let (p, i) = paged(
            json!([q("limit", json!({"default": 25}))]),
            items(json!({"id": {}})),
            json!({}),
        );
        assert!(p.is_none());
        let d = decided(&i, "pagination");
        assert!(d[0].message.contains("only the first page"), "{d:?}");
        assert_eq!(d[0].candidates, vec!["limit".to_string()]);
        let (_, i) = paged(
            json!([q("per_page", json!({}))]),
            env(json!({"meta": {"properties": {"has_more": {"type": "boolean"}}}})),
            json!({}),
        );
        let d = decided(&i, "pagination");
        assert_eq!(
            d[0].candidates,
            vec!["$.meta.has_more".to_string(), "per_page".to_string()]
        );
    }

    #[test]
    fn interpolation_directives_in_the_spec_are_refused() {
        let mut v = base(json!({
            "/${env:SECRET}": list(json!([]), items(json!({"id": {}}))),
            "/ok": {"get": {"summary": "${file:/etc/passwd}", "parameters": [
                    {"name": "${env:P}", "in": "query", "required": true}],
                "responses": {"200": {"content": {"application/json": {
                    "schema": items(json!({"id": {}, "${env:F}": {"type": "object"}})),
                    "example": [{"id": 1}, {"id": "${env:X}"}]}}}}}},
        }));
        v["servers"] = json!([{"url": "https://${env:HOST}/v1"}]);
        v["info"] = json!({"title": "${env:T}", "description": "${env:D}"});
        v["externalDocs"] = json!({"url": "https://example.com/${env:U}"});
        let i = import(
            &set(v.clone()),
            &ImportOptions {
                name: Some("demo".into()),
                ..opts()
            },
        )
        .unwrap();
        assert_eq!(i.template.stream_names(), vec!["ok"]);
        assert!(i.template.params[API_ROOT].required);
        assert!(i.template.docs.is_none());
        assert_eq!(
            i.template.description.as_deref(),
            Some("demo (imported from its OpenAPI description)")
        );
        assert!(stream(&i, "ok").description.is_none());
        assert!(i.template.transforms.is_empty());
        assert_eq!(i.suite.suite.behavioral[0].input, json!([{"id": 1}]));
        let scopes: Vec<&str> = i
            .report
            .unsupported
            .iter()
            .map(|u| u.scope.as_str())
            .collect();
        assert!(scopes.contains(&"path /${env:SECRET}"), "{scopes:?}");
        assert!(
            scopes.contains(&"server https://${env:HOST}/v1"),
            "{scopes:?}"
        );
        assert!(
            scopes.iter().any(|s| s.starts_with("names ")
                && s.contains("${env:F}")
                && s.contains("${env:P}")),
            "{scopes:?}"
        );
        assert!(decided(&i, "source.config.query_params").is_empty());
        for (scheme, needle) in [
            (
                json!({"type": "apiKey", "in": "header", "name": "${env:K}"}),
                "API key's name",
            ),
            (
                json!({"type": "oauth2", "flows": {"clientCredentials": {"tokenUrl": "https://a.example.com/${env:T}"}}}),
                "token URL",
            ),
        ] {
            let i = with_auth(json!({"s": scheme}), json!([{"s": ["${env:S}"]}]));
            assert!(i.template.source.config.get("auth").is_none());
            assert!(
                i.report.unsupported[0].message.contains(needle),
                "{:?}",
                i.report.unsupported
            );
        }
        let i = with_auth(
            json!({"s": {"type": "oauth2", "flows": {"clientCredentials": {"tokenUrl": "https://a.example.com/t"}}}}),
            json!([{"s": ["read", "${env:S}"]}]),
        );
        assert_eq!(
            i.template.source.config["auth"]["config"]["scopes"],
            json!(["read"])
        );
        let e = import(
            &set(v),
            &ImportOptions {
                server: Some("https://${env:H}".into()),
                name: Some("demo".into()),
                ..opts()
            },
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("--server"), "{e}");
    }

    #[test]
    fn auth_ties_and_compound_requirements_are_reported() {
        let mut v = base(json!({
            "/a": {"get": {"security": [{"b": []}], "responses": ok(items(json!({"id": {}})))}},
            "/c": {"get": {"security": [{"a": []}], "responses": ok(items(json!({"id": {}})))}},
        }));
        v["components"] = json!({"securitySchemes": {
            "a": {"type": "http", "scheme": "bearer"}, "b": {"type": "apiKey", "in": "header", "name": "K"}}});
        let i = imp(v.clone());
        assert_eq!(i.template.source.config["auth"]["type"], json!("bearer"));
        assert_eq!(
            decided(&i, "source.config.auth")[0].candidates,
            vec!["a".to_string(), "b".to_string()]
        );
        v["security"] = json!([{"a": [], "b": []}]);
        v["paths"] = json!({"/x": list(json!([]), items(json!({"id": {}})))});
        let i = imp(v);
        assert!(i.template.source.config.get("auth").is_none());
        assert_eq!(i.report.unsupported[0].scope, "security a + b");
    }

    #[test]
    fn next_links_under_links_and_ambiguous_next_fields() {
        let (p, _) = paged(
            json!([]),
            env(json!({"links": {"properties": {"next": {"type": "string"}}}})),
            json!({}),
        );
        assert_eq!(p.unwrap()["next_link_path"], json!("$.links.next"));
        let (p, _) = paged(
            json!([]),
            env(json!({"_links": {"properties": {"next": {"type": "string"}}}})),
            json!({}),
        );
        assert_eq!(p.unwrap()["next_link_path"], json!("$._links.next"));
        let (p, i) = paged(
            json!([q("cursor", json!({}))]),
            env(json!({"next": {"type": "string"}})),
            json!({}),
        );
        assert!(p.is_none());
        assert_eq!(
            decided(&i, "pagination")[0].candidates,
            vec!["$.next".to_string()]
        );
    }

    #[test]
    fn a_wrapper_picked_among_several_arrays_is_reported() {
        let i = one(
            "/things",
            list(
                json!([]),
                json!({"properties": {
            "data": items(json!({"id": {}})), "included": items(json!({"id": {}}))}}),
            ),
        );
        assert_eq!(cfg(&i, "things")["records_path"], json!("$.data[*]"));
        assert_eq!(
            decided(&i, "records_path")[0].candidates,
            vec!["$.data[*]".to_string(), "$.included[*]".to_string()]
        );
    }

    #[test]
    fn diamond_compositions_finish_and_the_node_budget_holds() {
        let mut schemas = Map::new();
        for n in 0..30 {
            let next = format!("#/components/schemas/S{}", n + 1);
            schemas.insert(
                format!("S{n}"),
                json!({"allOf": [{"$ref": next}, {"$ref": next}]}),
            );
        }
        schemas.insert("S30".into(), json!({"properties": {"id": {}}}));
        let mut v = base(
            json!({"/a": list(json!([]), json!({"type": "array", "items": {"$ref": "#/components/schemas/S0"}}))}),
        );
        v["components"] = json!({"schemas": schemas});
        let started = std::time::Instant::now();
        let i = imp(v);
        assert!(started.elapsed() < std::time::Duration::from_secs(10));
        assert_eq!(stream(&i, "a").primary_keys, vec!["id".to_string()]);
        let s = set(json!({"openapi": "3.0.0", "paths": {}}));
        let m = Mapper {
            set: &s,
            v2: false,
            root_key: s.root_key(),
            report: ImportReport::default(),
            flat_cache: RefCell::new(HashMap::new()),
            flat_calls: Cell::new(MAX_FLATTEN_NODES),
            tainted: RefCell::new(BTreeSet::new()),
        };
        let e = m
            .flatten(
                &Loc {
                    base: s.root_key(),
                    v: json!({}),
                },
                0,
            )
            .unwrap_err();
        assert!(e.to_string().contains("too complex"), "{e}");
    }

    #[test]
    fn incremental_formats_need_documenting() {
        let i = imp(base(json!({
            "/a": list(json!([q("updated_since", json!({"type": "integer"}))]), items(json!({"id": {}, "updated_at": {"type": "integer"}}))),
            "/b": list(json!([q("updated_since", json!({"type": "integer", "description": "epoch milliseconds"}))]), items(json!({"id": {}, "updated_at": {"type": "integer"}}))),
        })));
        assert!(cfg(&i, "a").get("replication_key").is_none());
        assert!(
            decided(&i, "replication_bind")[0]
                .message
                .contains("seconds or milliseconds")
        );
        assert_eq!(
            cfg(&i, "b")["replication_bind"]["format"],
            json!("epoch_ms")
        );
    }

    #[test]
    fn first_of_list_picks_are_reported() {
        let mut v = base(json!({
            "/p": list(json!([q("page", json!({})), q("per_page", json!({"default": 10})), q("limit", json!({"default": 20}))]), items(json!({"id": {}}))),
            "/o": list(json!([q("offset", json!({})), q("limit", json!({"default": 20})), q("top", json!({"maximum": 99}))]), items(json!({"id": {}}))),
            "/p/{p_id}/c": list(json!([]), items(json!({"id": {}}))),
            "/r": {"get": {"responses": {
                "200": {"headers": {"X-RateLimit-Reset": {"description": "seconds"}, "RateLimit-Reset": {}}, "content": {"application/json": {"schema": items(json!({"id": {}}))}}},
            }}},
        }));
        v["servers"] = json!([{"url": "https://a.example.com"}, {"url": "https://b.example.com"}]);
        let i = imp(v);
        assert_eq!(
            i.template.params[API_ROOT].default,
            Some(json!("https://a.example.com"))
        );
        assert_eq!(decided(&i, "params.api_root")[0].candidates.len(), 2);
        assert_eq!(
            cfg(&i, "p")["pagination"]["page_size_param"],
            json!("per_page")
        );
        assert_eq!(decided(&i, "pagination.page_size_param").len(), 1);
        assert_eq!(cfg(&i, "o")["pagination"]["limit_param"], json!("limit"));
        assert_eq!(decided(&i, "pagination.limit_param").len(), 1);
        assert!(
            decided(&i, "source.config.retry_on_response")[0]
                .candidates
                .len()
                == 2
        );
        assert!(decided(&i, "parent_key")[0].message.contains("{p_id}"));
    }

    #[test]
    fn pagination_reads_only_the_success_response() {
        let i = one(
            "/a",
            json!({"get": {"responses": {
                "200": {"content": {"application/json": {"schema": items(json!({"id": {}}))}}},
                "404": {"headers": {"Link": {}, "X-RateLimit-Reset": {"description": "seconds"}}},
            }}}),
        );
        assert!(cfg(&i, "a").get("pagination").is_none());
        assert!(i.template.source.config.get("retry_on_response").is_none());
        let i = one(
            "/a",
            json!({"get": {"responses": {
                "200": {"content": {"application/json": {"schema": items(json!({"id": {}}))}}},
                "429": {"headers": {"X-RateLimit-Reset": {"description": "seconds"}}},
            }}}),
        );
        assert_eq!(
            i.template.source.config["retry_on_response"][0]["backoff_from"]["config"]["unit"],
            json!("seconds")
        );
    }

    #[test]
    fn an_unresolvable_reference_skips_only_its_operation() {
        let i = imp(base(json!({
            "/a": list(json!([]), json!({"$ref": "#/components/schemas/Missing"})),
            "/b": {"$ref": "#/nope"},
            "/c": list(json!([]), items(json!({"id": {}}))),
        })));
        assert_eq!(i.template.stream_names(), vec!["c"]);
        let mut reasons: Vec<&str> = i.report.skipped.iter().map(|s| s.path.as_str()).collect();
        reasons.sort();
        assert_eq!(reasons, vec!["/a", "/b"]);
    }

    #[test]
    fn a_relative_server_flag_with_a_local_spec_is_refused() {
        let v = base(json!({"/a": list(json!([]), items(json!({"id": {}})))}));
        let e = import(
            &set(v),
            &ImportOptions {
                server: Some("/v2".into()),
                ..opts()
            },
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("--server /v2") && e.contains("absolute"), "{e}");
    }

    #[test]
    fn helpers() {
        assert_eq!(slug("Example Store API!", '-'), "example-store-api");
        assert_eq!(slug("listOrders2Go", '_'), "list_orders2_go");
        assert_eq!(slug("__", '_'), "");
        assert_eq!(slug(&"a".repeat(80), '_').len(), 64);
        assert_eq!(
            jsonpath(&["a".into(), "it's".into()], false),
            "$.a['it\\'s']"
        );
        assert_eq!(path_vars("https://{a}.x/{b"), vec!["a".to_string()]);
        assert!(recognized_param("per_page") && !recognized_param("q"));
        assert_eq!(scalar_text(&json!(2)), "2");
        assert_eq!(
            fake_params(&BTreeMap::from([(
                "plain".to_string(),
                ParamSpec {
                    kind: ParamType::String,
                    required: true,
                    default: None,
                    secret: false,
                    description: None,
                    computed: None,
                    values: Vec::new(),
                }
            )])),
            BTreeMap::from([("plain".to_string(), json!("suite-plain"))])
        );
        assert!(check_keys(&Map::from_iter([("nope".to_string(), json!(1))])).is_err());
    }
}
