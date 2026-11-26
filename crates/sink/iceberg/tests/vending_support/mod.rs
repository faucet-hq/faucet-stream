//! Stand-ins for a REST catalog that vends storage credentials and for the
//! object store that accepts only those credentials.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use iceberg::TableCreation;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg_catalog_rest::{
    CommitTableRequest, CommitTableResponse, CreateTableRequest, LoadTableResult,
};
use serde_json::json;
use wiremock::matchers::any;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

pub const BUCKET: &str = "lake";
pub const STATIC_KEY: &str = "STATICKEY";
pub const STATIC_SECRET: &str = "STATICSECRET";
const DELEGATION_HEADER: &str = "x-iceberg-access-delegation";

#[allow(unsafe_code)]
fn clear_aws_env() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let names: Vec<String> = std::env::vars()
            .map(|(k, _)| k)
            .filter(|k| k.starts_with("AWS_"))
            .collect();
        for name in names {
            // SAFETY: runs once, before any storage client reads the environment.
            unsafe { std::env::remove_var(name) };
        }
    });
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Vend {
    Always,
    WhenAsked,
    Never,
}

#[derive(Clone, Debug)]
pub struct S3Call {
    pub method: String,
    pub key: String,
    pub query: String,
    pub generation: Option<u32>,
    pub access_key: Option<String>,
    pub status: u16,
}

#[derive(Clone, Debug)]
pub struct CatalogCall {
    pub method: String,
    pub path: String,
    pub delegation: Option<String>,
    pub status: u16,
}

#[derive(Default)]
struct Store {
    objects: HashMap<String, Vec<u8>>,
    uploads: HashMap<String, BTreeMap<u32, Vec<u8>>>,
    next_upload: u32,
    issued: u32,
    valid_from: u32,
    static_keys: Vec<String>,
    calls: Vec<S3Call>,
}

struct TableEntry {
    creation: serde_json::Value,
    metadata: TableMetadata,
    location: String,
    previous: Option<(TableMetadata, String)>,
    version: u32,
}

struct Catalog {
    vend: Vend,
    namespaces: Vec<String>,
    tables: HashMap<String, TableEntry>,
    fail_loads: u32,
    hang_loads: u32,
    stale_loads: u32,
    conflict: bool,
    calls: Vec<CatalogCall>,
}

#[derive(Clone)]
struct Shared {
    store: Arc<Mutex<Store>>,
    catalog: Arc<Mutex<Catalog>>,
}

pub struct Fakes {
    pub rest: MockServer,
    pub s3: MockServer,
    shared: Shared,
}

fn text(status: u16, body: &str) -> ResponseTemplate {
    ResponseTemplate::new(status).set_body_string(body.to_string())
}

fn s3_error(status: u16, code: &str) -> ResponseTemplate {
    ResponseTemplate::new(status)
        .insert_header("content-type", "application/xml")
        .set_body_string(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code><Message>{code}</Message></Error>"
        ))
}

fn catalog_error(status: u16, kind: &str) -> ResponseTemplate {
    ResponseTemplate::new(status).set_body_json(json!({
        "error": {"message": kind, "type": kind, "code": status}
    }))
}

fn header<'a>(req: &'a Request, name: &str) -> Option<&'a str> {
    req.headers.get(name).and_then(|v| v.to_str().ok())
}

fn access_key(req: &Request) -> Option<String> {
    let auth = header(req, "authorization")?;
    let rest = auth.split("Credential=").nth(1)?;
    Some(rest.split('/').next()?.to_string())
}

fn generation(req: &Request) -> Option<u32> {
    let n: u32 = access_key(req)?.strip_prefix("VK")?.parse().ok()?;
    (header(req, "x-amz-security-token") == Some(format!("VT{n}").as_str())).then_some(n)
}

fn etag(body: &[u8]) -> String {
    let sum = body.iter().fold(0u64, |a, b| {
        a.wrapping_mul(1_099_511_628_211)
            .wrapping_add(u64::from(*b))
    });
    format!("\"{sum:016x}{:08x}\"", body.len())
}

struct S3Responder(Shared);

impl S3Responder {
    fn answer(&self, req: &Request, store: &mut Store) -> ResponseTemplate {
        let generation = generation(req);
        let key_ok = access_key(req).is_some_and(|k| store.static_keys.contains(&k));
        let vended_ok = generation.is_some_and(|n| n >= store.valid_from && n <= store.issued);
        if !(key_ok || vended_ok) {
            return s3_error(403, "AccessDenied");
        }
        let key = req.url.path().to_string();
        let query: HashMap<String, String> = req.url.query_pairs().into_owned().collect();
        match req.method.as_str() {
            "PUT" => match (query.get("uploadId"), query.get("partNumber")) {
                (Some(id), Some(part)) => {
                    let Some(parts) = store.uploads.get_mut(id) else {
                        return s3_error(404, "NoSuchUpload");
                    };
                    parts.insert(part.parse().unwrap_or(0), req.body.clone());
                    ResponseTemplate::new(200).insert_header("etag", etag(&req.body).as_str())
                }
                _ => {
                    store.objects.insert(key, req.body.clone());
                    ResponseTemplate::new(200).insert_header("etag", etag(&req.body).as_str())
                }
            },
            "POST" if query.contains_key("uploads") => {
                store.next_upload += 1;
                let id = format!("upload-{}", store.next_upload);
                store.uploads.insert(id.clone(), BTreeMap::new());
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/xml")
                    .set_body_string(format!(
                        "<InitiateMultipartUploadResult><Bucket>{BUCKET}</Bucket><Key>k</Key><UploadId>{id}</UploadId></InitiateMultipartUploadResult>"
                    ))
            }
            "POST" => match query
                .get("uploadId")
                .and_then(|id| store.uploads.remove(id))
            {
                Some(parts) => {
                    let body: Vec<u8> = parts.into_values().flatten().collect();
                    let tag = etag(&body);
                    store.objects.insert(key, body);
                    ResponseTemplate::new(200)
                        .insert_header("content-type", "application/xml")
                        .set_body_string(format!(
                            "<CompleteMultipartUploadResult><Bucket>{BUCKET}</Bucket><Key>k</Key><ETag>{}</ETag></CompleteMultipartUploadResult>",
                            tag.replace('"', "&quot;")
                        ))
                }
                None => s3_error(404, "NoSuchUpload"),
            },
            "DELETE" => {
                if let Some(id) = query.get("uploadId") {
                    store.uploads.remove(id);
                } else {
                    store.objects.remove(&key);
                }
                ResponseTemplate::new(204)
            }
            "HEAD" | "GET" => {
                let Some(body) = store.objects.get(&key) else {
                    return s3_error(404, "NoSuchKey");
                };
                let base = |status: u16, bytes: Vec<u8>| {
                    ResponseTemplate::new(status)
                        .insert_header("etag", etag(body).as_str())
                        .insert_header("last-modified", "Wed, 21 Oct 2026 07:28:00 GMT")
                        .insert_header("accept-ranges", "bytes")
                        .set_body_bytes(bytes)
                };
                let range = header(req, "range")
                    .and_then(|r| r.strip_prefix("bytes="))
                    .and_then(|r| r.split_once('-'))
                    .and_then(|(a, b)| Some((a.parse::<usize>().ok()?, b.parse::<usize>().ok())));
                match range {
                    Some((start, end)) if start < body.len() => {
                        let end = end.map_or(body.len() - 1, |e| e.min(body.len() - 1));
                        base(206, body[start..=end].to_vec()).insert_header(
                            "content-range",
                            format!("bytes {start}-{end}/{}", body.len()).as_str(),
                        )
                    }
                    Some(_) => s3_error(416, "InvalidRange"),
                    None => base(200, body.clone()),
                }
            }
            _ => s3_error(501, "NotImplemented"),
        }
    }
}

impl Respond for S3Responder {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let mut store = self.0.store.lock().unwrap();
        let (answer, status) = {
            let a = self.answer(req, &mut store);
            let status = status_of(&a);
            (a, status)
        };
        store.calls.push(S3Call {
            method: req.method.to_string(),
            key: req.url.path().to_string(),
            query: req.url.query().unwrap_or_default().to_string(),
            generation: generation(req),
            access_key: access_key(req),
            status,
        });
        answer
    }
}

fn status_of(t: &ResponseTemplate) -> u16 {
    format!("{t:?}")
        .split("status_code: ")
        .nth(1)
        .and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

struct RestResponder(Shared);

impl RestResponder {
    fn vended(&self, req: &Request, vend: Vend) -> HashMap<String, String> {
        let asked =
            header(req, DELEGATION_HEADER).is_some_and(|v| v.contains("vended-credentials"));
        if vend == Vend::Never || (vend == Vend::WhenAsked && !asked) {
            return HashMap::new();
        }
        let mut store = self.0.store.lock().unwrap();
        store.issued += 1;
        let n = store.issued;
        HashMap::from([
            ("s3.access-key-id".to_string(), format!("VK{n}")),
            ("s3.secret-access-key".to_string(), format!("VS{n}")),
            ("s3.session-token".to_string(), format!("VT{n}")),
        ])
    }

    fn put_metadata(&self, location: &str, metadata: &TableMetadata) {
        let key = location.replacen("s3:/", "", 1);
        let body = serde_json::to_vec(metadata).unwrap();
        self.0.store.lock().unwrap().objects.insert(key, body);
    }

    fn loaded(
        &self,
        req: &Request,
        vend: Vend,
        entry: &TableEntry,
        stale: bool,
    ) -> ResponseTemplate {
        let (metadata, location) = match (&entry.previous, stale) {
            (Some((m, l)), true) => (m.clone(), l.clone()),
            _ => (entry.metadata.clone(), entry.location.clone()),
        };
        let result = LoadTableResult {
            metadata_location: Some(location),
            metadata,
            config: self.vended(req, vend),
            storage_credentials: None,
        };
        ResponseTemplate::new(200).set_body_json(result)
    }

    fn create(&self, ns: &str, body: &serde_json::Value) -> Result<TableEntry, String> {
        let request: CreateTableRequest =
            serde_json::from_value(body.clone()).map_err(|e| e.to_string())?;
        let location = request
            .location
            .clone()
            .unwrap_or_else(|| format!("s3://{BUCKET}/wh/{ns}/{}", request.name));
        let creation = TableCreation::builder()
            .name(request.name.clone())
            .location(location.clone())
            .schema(request.schema)
            .partition_spec_opt(request.partition_spec)
            .properties(request.properties)
            .build();
        let metadata = TableMetadataBuilder::from_table_creation(creation)
            .and_then(TableMetadataBuilder::build)
            .map_err(|e| e.to_string())?
            .metadata;
        let metadata_location = format!("{location}/metadata/00000-create.metadata.json");
        self.put_metadata(&metadata_location, &metadata);
        Ok(TableEntry {
            creation: body.clone(),
            metadata,
            location: metadata_location,
            previous: None,
            version: 0,
        })
    }

    fn commit(&self, entry: &mut TableEntry, body: &[u8]) -> ResponseTemplate {
        let request: CommitTableRequest = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return text(400, &e.to_string()),
        };
        for requirement in &request.requirements {
            if requirement.check(Some(&entry.metadata)).is_err() {
                return catalog_error(409, "CommitFailedException");
            }
        }
        let mut builder = TableMetadataBuilder::new_from_metadata(
            entry.metadata.clone(),
            Some(entry.location.clone()),
        );
        for update in request.updates {
            builder = match update.apply(builder) {
                Ok(b) => b,
                Err(e) => return text(400, &e.to_string()),
            };
        }
        let metadata = match builder.build() {
            Ok(r) => r.metadata,
            Err(e) => return text(400, &e.to_string()),
        };
        entry.version += 1;
        let location = format!(
            "{}/metadata/{:05}-commit.metadata.json",
            metadata.location(),
            entry.version
        );
        self.put_metadata(&location, &metadata);
        entry.previous = Some((
            std::mem::replace(&mut entry.metadata, metadata.clone()),
            std::mem::replace(&mut entry.location, location.clone()),
        ));
        ResponseTemplate::new(200).set_body_json(CommitTableResponse {
            metadata_location: location,
            metadata,
        })
    }

    fn answer(&self, req: &Request, catalog: &mut Catalog) -> ResponseTemplate {
        let path = req.url.path().trim_end_matches('/').to_string();
        let parts: Vec<&str> = path.trim_start_matches("/v1/").split('/').collect();
        let method = req.method.as_str();
        match (method, parts.as_slice()) {
            ("GET", ["config"]) => {
                ResponseTemplate::new(200).set_body_json(json!({"defaults": {}, "overrides": {}}))
            }
            ("POST", ["namespaces"]) => {
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();
                let ns = body["namespace"][0]
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                catalog.namespaces.push(ns.clone());
                ResponseTemplate::new(200)
                    .set_body_json(json!({"namespace": [ns], "properties": {}}))
            }
            ("HEAD" | "GET", ["namespaces", ns]) => {
                if !catalog.namespaces.iter().any(|n| n == ns) {
                    return catalog_error(404, "NoSuchNamespaceException");
                }
                match method {
                    "HEAD" => ResponseTemplate::new(204),
                    _ => ResponseTemplate::new(200)
                        .set_body_json(json!({"namespace": [ns], "properties": {}})),
                }
            }
            ("POST", ["namespaces", ns, "tables"]) => {
                if !catalog.namespaces.iter().any(|n| n == ns) {
                    return catalog_error(404, "NoSuchNamespaceException");
                }
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();
                let name = body["name"].as_str().unwrap_or_default().to_string();
                let id = format!("{ns}.{name}");
                if catalog.tables.contains_key(&id) {
                    return catalog_error(409, "AlreadyExistsException");
                }
                match self.create(ns, &body) {
                    Ok(entry) => {
                        let answer = self.loaded(req, catalog.vend, &entry, false);
                        catalog.tables.insert(id, entry);
                        answer
                    }
                    Err(e) => text(400, &e),
                }
            }
            (_, ["namespaces", ns, "tables", table]) => {
                let id = format!("{ns}.{table}");
                let vend = catalog.vend;
                match method {
                    "HEAD" => match catalog.tables.contains_key(&id) {
                        true => ResponseTemplate::new(204),
                        false => catalog_error(404, "NoSuchTableException"),
                    },
                    "GET" => {
                        if catalog.fail_loads > 0 {
                            catalog.fail_loads -= 1;
                            return catalog_error(500, "InternalServerError");
                        }
                        if catalog.hang_loads > 0 {
                            catalog.hang_loads -= 1;
                            return catalog_error(500, "InternalServerError")
                                .set_delay(std::time::Duration::from_secs(120));
                        }
                        let stale = catalog.stale_loads > 0;
                        catalog.stale_loads = catalog.stale_loads.saturating_sub(1);
                        match catalog.tables.get(&id) {
                            Some(entry) => self.loaded(req, vend, entry, stale),
                            None => catalog_error(404, "NoSuchTableException"),
                        }
                    }
                    "POST" => {
                        if catalog.conflict {
                            return catalog_error(409, "CommitFailedException");
                        }
                        match catalog.tables.get_mut(&id) {
                            Some(entry) => self.commit(entry, &req.body),
                            None => catalog_error(404, "NoSuchTableException"),
                        }
                    }
                    _ => text(405, "method"),
                }
            }
            _ => text(404, "no such route"),
        }
    }
}

impl Respond for RestResponder {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let mut catalog = self.0.catalog.lock().unwrap();
        let answer = self.answer(req, &mut catalog);
        catalog.calls.push(CatalogCall {
            method: req.method.to_string(),
            path: req.url.path().to_string(),
            delegation: header(req, DELEGATION_HEADER).map(str::to_string),
            status: status_of(&answer),
        });
        answer
    }
}

impl Fakes {
    pub async fn start(vend: Vend) -> Self {
        clear_aws_env();
        let shared = Shared {
            store: Arc::new(Mutex::new(Store {
                valid_from: 1,
                ..Default::default()
            })),
            catalog: Arc::new(Mutex::new(Catalog {
                vend,
                namespaces: Vec::new(),
                tables: HashMap::new(),
                fail_loads: 0,
                hang_loads: 0,
                stale_loads: 0,
                conflict: false,
                calls: Vec::new(),
            })),
        };
        let rest = MockServer::start().await;
        let s3 = MockServer::start().await;
        Mock::given(any())
            .respond_with(RestResponder(shared.clone()))
            .mount(&rest)
            .await;
        Mock::given(any())
            .respond_with(S3Responder(shared.clone()))
            .mount(&s3)
            .await;
        Self { rest, s3, shared }
    }

    /// The `catalog` block of a connector config, with `extra` properties.
    pub fn catalog(&self, extra: serde_json::Value) -> serde_json::Value {
        let mut properties = json!({
            "s3.endpoint": self.s3.uri(),
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
            "s3.disable-ec2-metadata": "true",
            "s3.disable-config-load": "true",
        });
        if let Some(extra) = extra.as_object() {
            for (k, v) in extra {
                properties[k] = v.clone();
            }
        }
        json!({"type": "rest", "uri": self.rest.uri(), "properties": properties})
    }

    /// The `catalog` block without the settings that keep the storage off the
    /// host's own AWS configuration.
    pub fn catalog_using_the_host(&self) -> serde_json::Value {
        let mut catalog = self.catalog(json!({}));
        let props = catalog["properties"].as_object_mut().unwrap();
        props.remove("s3.disable-ec2-metadata");
        props.remove("s3.disable-config-load");
        catalog
    }

    pub fn static_keys() -> serde_json::Value {
        json!({"s3.access-key-id": STATIC_KEY, "s3.secret-access-key": STATIC_SECRET})
    }

    pub fn set_vend(&self, vend: Vend) {
        self.shared.catalog.lock().unwrap().vend = vend;
    }

    /// The bytes of every stored object whose key contains `contains`.
    pub fn objects(&self, contains: &str) -> Vec<(String, Vec<u8>)> {
        self.shared
            .store
            .lock()
            .unwrap()
            .objects
            .iter()
            .filter(|(k, _)| k.contains(contains))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn expire_issued(&self) {
        let mut store = self.shared.store.lock().unwrap();
        store.valid_from = store.issued + 1;
    }

    pub fn accept_static_key(&self, key: &str) {
        self.shared
            .store
            .lock()
            .unwrap()
            .static_keys
            .push(key.to_string());
    }

    pub fn fail_next_loads(&self, n: u32) {
        self.shared.catalog.lock().unwrap().fail_loads = n;
    }

    /// The next `n` table loads get no answer for two minutes.
    pub fn hang_next_loads(&self, n: u32) {
        self.shared.catalog.lock().unwrap().hang_loads = n;
    }

    pub fn stale_next_loads(&self, n: u32) {
        self.shared.catalog.lock().unwrap().stale_loads = n;
    }

    pub fn conflict_on_commit(&self) {
        self.shared.catalog.lock().unwrap().conflict = true;
    }

    pub fn drop_table(&self, id: &str) {
        self.shared.catalog.lock().unwrap().tables.remove(id);
    }

    /// Replace `id` with a new table of the same name (another UUID).
    pub fn replace_table(&self, id: &str) {
        let responder = RestResponder(self.shared.clone());
        let mut catalog = self.shared.catalog.lock().unwrap();
        let body = catalog.tables[id].creation.clone();
        let ns = id.split('.').next().unwrap();
        let entry = responder.create(ns, &body).unwrap();
        catalog.tables.insert(id.to_string(), entry);
    }

    /// Create the table `ns.name` (and its namespace) with a one-column schema.
    pub fn create_table(&self, ns: &str, name: &str) {
        let responder = RestResponder(self.shared.clone());
        let body = json!({
            "name": name,
            "schema": {"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "id", "required": false, "type": "long"}
            ]}
        });
        let entry = responder.create(ns, &body).unwrap();
        let mut catalog = self.shared.catalog.lock().unwrap();
        catalog.namespaces.push(ns.to_string());
        catalog.tables.insert(format!("{ns}.{name}"), entry);
    }

    pub fn metadata(&self, id: &str) -> TableMetadata {
        self.shared.catalog.lock().unwrap().tables[id]
            .metadata
            .clone()
    }

    pub fn snapshots(&self, id: &str) -> usize {
        self.shared
            .catalog
            .lock()
            .unwrap()
            .tables
            .get(id)
            .map_or(0, |t| t.metadata.snapshots().count())
    }

    pub fn s3_calls(&self) -> Vec<S3Call> {
        self.shared.store.lock().unwrap().calls.clone()
    }

    pub fn catalog_calls(&self) -> Vec<CatalogCall> {
        self.shared.catalog.lock().unwrap().calls.clone()
    }

    pub fn uploads(&self) -> Vec<S3Call> {
        self.s3_calls()
            .into_iter()
            .filter(|c| c.method == "PUT" || c.method == "POST")
            .collect()
    }

    pub fn issued(&self) -> u32 {
        self.shared.store.lock().unwrap().issued
    }

    pub fn object_count(&self, contains: &str) -> usize {
        self.shared
            .store
            .lock()
            .unwrap()
            .objects
            .keys()
            .filter(|k| k.contains(contains))
            .count()
    }
}
