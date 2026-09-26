//! Staged loads: where a page's Parquet file is written, what it is named,
//! and how it is uploaded (Files API for Unity Catalog volumes; object store
//! for cloud locations behind the `staging` feature).

use std::sync::Arc;

use async_trait::async_trait;
use faucet_common_databricks::{StatementClient, backoff_delay};
use faucet_core::FaucetError;

/// A parsed `staging.location`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageLocation {
    /// `/Volumes/<catalog>/<schema>/<volume>[/prefix]`.
    Volume { root: String },
    /// `s3://bucket/prefix`.
    S3 { bucket: String, prefix: String },
    /// `gs://bucket/prefix`.
    Gcs { bucket: String, prefix: String },
    /// `abfss://container@account.dfs.core.windows.net/prefix`.
    Azure {
        container: String,
        account: String,
        host: String,
        prefix: String,
    },
}

fn bucket_and_prefix(rest: &str) -> (String, String) {
    match rest.split_once('/') {
        Some((b, p)) => (b.to_owned(), p.trim_matches('/').to_owned()),
        None => (rest.to_owned(), String::new()),
    }
}

impl StageLocation {
    /// Parse and validate a staging location.
    pub fn parse(location: &str) -> Result<Self, FaucetError> {
        let loc = location.trim();
        let bad = |why: &str| {
            FaucetError::Config(format!(
                "databricks sink: staging location `{loc}` {why} (use /Volumes/<catalog>/<schema>/<volume>/…, \
                 s3://…, gs://…, or abfss://<container>@<account>.dfs.core.windows.net/…)"
            ))
        };
        if let Some(rest) = loc.strip_prefix("/Volumes/") {
            let root = format!("/Volumes/{}", rest.trim_matches('/'));
            if root.split('/').filter(|s| !s.is_empty()).count() < 4 {
                return Err(bad("must name a catalog, schema and volume"));
            }
            return Ok(StageLocation::Volume { root });
        }
        let (scheme, rest) = loc.split_once("://").ok_or_else(|| bad("is not a URI"))?;
        let (bucket, prefix) = bucket_and_prefix(rest);
        if bucket.is_empty() {
            return Err(bad("has no bucket/container"));
        }
        match scheme {
            "s3" | "s3a" => Ok(StageLocation::S3 { bucket, prefix }),
            "gs" => Ok(StageLocation::Gcs { bucket, prefix }),
            "abfss" | "abfs" => {
                let (container, host) = bucket
                    .split_once('@')
                    .ok_or_else(|| bad("must be abfss://<container>@<account>.<host>/…"))?;
                let account = host.split('.').next().unwrap_or_default();
                if container.is_empty() || account.is_empty() {
                    return Err(bad("must be abfss://<container>@<account>.<host>/…"));
                }
                Ok(StageLocation::Azure {
                    container: container.to_owned(),
                    account: account.to_owned(),
                    host: host.to_owned(),
                    prefix,
                })
            }
            _ => Err(bad("has an unsupported scheme")),
        }
    }

    /// Whether uploads go through the Files API.
    pub fn is_volume(&self) -> bool {
        matches!(self, StageLocation::Volume { .. })
    }

    fn prefix(&self) -> &str {
        match self {
            StageLocation::Volume { root } => root,
            StageLocation::S3 { prefix, .. }
            | StageLocation::Gcs { prefix, .. }
            | StageLocation::Azure { prefix, .. } => prefix,
        }
    }

    /// The upload path for `rel`: the absolute volume path, or the object key
    /// within the bucket.
    pub fn object_path(&self, rel: &str) -> String {
        let p = self.prefix();
        if p.is_empty() {
            rel.to_owned()
        } else {
            format!("{p}/{rel}")
        }
    }

    /// The URI the warehouse reads `rel` from.
    pub fn uri(&self, rel: &str) -> String {
        let key = self.object_path(rel);
        match self {
            StageLocation::Volume { .. } => key,
            StageLocation::S3 { bucket, .. } => format!("s3://{bucket}/{key}"),
            StageLocation::Gcs { bucket, .. } => format!("gs://{bucket}/{key}"),
            StageLocation::Azure {
                container, host, ..
            } => format!("abfss://{container}@{host}/{key}"),
        }
    }
}

/// FNV-1a 64 — a stable, dependency-free name hash.
pub fn fnv64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Keep a path segment URL- and filesystem-safe.
pub fn sanitize(s: &str) -> String {
    let out: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.is_empty() { "_".into() } else { out }
}

/// A staged file: its directory (relative to the location) and file name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedName {
    pub dir: String,
    pub file: String,
}

impl StagedName {
    pub fn rel(&self) -> String {
        format!("{}/{}", self.dir, self.file)
    }
}

/// Exactly-once pages are named by `(table, scope, seq)` only, so every
/// attempt of a page writes — and reads — the same file.
pub fn eo_name(table: &str, scope: &str, seq: u64) -> StagedName {
    StagedName {
        dir: format!(
            "_faucet/{}/{:016x}",
            sanitize(table),
            fnv64(scope.as_bytes())
        ),
        file: format!("{seq:020}.parquet"),
    }
}

/// Other pages are named by run, write sequence and content: two identical
/// pages never share a file (so neither is skipped as already loaded), while
/// re-submitting one `COPY INTO` after an ambiguous failure reuses its file and
/// is skipped by `COPY INTO`'s load tracking.
pub fn run_name(table: &str, run_id: &str, seq: u64, body: &[u8]) -> StagedName {
    StagedName {
        dir: format!("_faucet/{}/{}", sanitize(table), sanitize(run_id)),
        file: format!("part-{seq:06}-{:016x}.parquet", fnv64(body)),
    }
}

/// Encode string cells as an all-`Utf8`, nullable Parquet file.
pub fn encode_parquet(
    names: &[String],
    rows: &[Vec<Option<String>>],
) -> Result<Vec<u8>, FaucetError> {
    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    let err = |e: &dyn std::fmt::Display| {
        FaucetError::Sink(format!("databricks sink: parquet encode failed: {e}"))
    };
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|n| Field::new(n, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<ArrayRef> = (0..names.len())
        .map(|i| {
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.get(i).and_then(|c| c.as_deref()))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|e| err(&e))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props)).map_err(|e| err(&e))?;
    w.write(&batch).map_err(|e| err(&e))?;
    w.close().map_err(|e| err(&e))?;
    Ok(buf)
}

/// Uploads and removes staged files.
#[async_trait]
pub trait Stager: Send + Sync {
    /// Write (or overwrite) `path` with `body`.
    async fn put(&self, path: &str, body: Vec<u8>) -> Result<(), FaucetError>;
    /// Remove `path`; a missing file is not an error.
    async fn delete(&self, path: &str) -> Result<(), FaucetError>;
}

/// Unity Catalog volume uploads through the Files API
/// (`PUT /api/2.0/fs/files/Volumes/…?overwrite=true`).
pub struct FilesApiStager {
    client: StatementClient,
}

impl FilesApiStager {
    pub fn new(client: StatementClient) -> Self {
        Self { client }
    }

    fn url(&self, path: &str) -> Result<reqwest::Url, FaucetError> {
        let mut url = reqwest::Url::parse(self.client.base_url())
            .map_err(|e| FaucetError::Config(format!("databricks sink: bad workspace URL: {e}")))?;
        {
            let mut segs = url.path_segments_mut().map_err(|_| {
                FaucetError::Config("databricks sink: workspace URL cannot be a base".into())
            })?;
            segs.pop_if_empty();
            segs.extend(["api", "2.0", "fs", "files"]);
            segs.extend(path.split('/').filter(|s| !s.is_empty()));
        }
        Ok(url)
    }

    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<reqwest::StatusCode, FaucetError> {
        let mut url = self.url(path)?;
        if body.is_some() {
            url.query_pairs_mut().append_pair("overwrite", "true");
        }
        let opts = self.client.options().clone();
        let mut attempt = 0u32;
        loop {
            let auth = self.client.authorization().await?;
            let mut req = self
                .client
                .http()
                .request(method.clone(), url.clone())
                .header("Authorization", auth);
            if let Some(b) = &body {
                req = req
                    .header("Content-Type", "application/octet-stream")
                    .body(b.clone());
            }
            let outcome = req.send().await;
            let retry = match &outcome {
                Ok(r) => r.status().as_u16() == 429 || r.status().is_server_error(),
                Err(_) => true,
            };
            if retry && attempt < opts.max_retries {
                tokio::time::sleep(backoff_delay(opts.retry_backoff, attempt)).await;
                attempt += 1;
                continue;
            }
            let resp = outcome.map_err(|e| {
                FaucetError::Sink(format!("databricks sink: Files API {method} {path}: {e}"))
            })?;
            let status = resp.status();
            if status.is_success() || (method == reqwest::Method::DELETE && status.as_u16() == 404)
            {
                return Ok(status);
            }
            let text = resp.text().await.unwrap_or_default();
            return Err(FaucetError::Sink(format!(
                "databricks sink: Files API {method} {path}: HTTP {status}: {text}"
            )));
        }
    }
}

#[async_trait]
impl Stager for FilesApiStager {
    async fn put(&self, path: &str, body: Vec<u8>) -> Result<(), FaucetError> {
        self.send(reqwest::Method::PUT, path, Some(body)).await?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), FaucetError> {
        self.send(reqwest::Method::DELETE, path, None).await?;
        Ok(())
    }
}

/// Cloud-location uploads through `object_store`.
#[cfg(feature = "staging")]
pub struct ObjectStoreStager {
    store: Arc<dyn object_store::ObjectStore>,
}

#[cfg(feature = "staging")]
impl ObjectStoreStager {
    pub fn new(store: Arc<dyn object_store::ObjectStore>) -> Self {
        Self { store }
    }
}

#[cfg(feature = "staging")]
#[async_trait]
impl Stager for ObjectStoreStager {
    async fn put(&self, path: &str, body: Vec<u8>) -> Result<(), FaucetError> {
        use object_store::ObjectStoreExt;
        self.store
            .put(&object_store::path::Path::from(path), body.into())
            .await
            .map(|_| ())
            .map_err(|e| FaucetError::Sink(format!("databricks sink: upload `{path}`: {e}")))
    }

    async fn delete(&self, path: &str) -> Result<(), FaucetError> {
        use object_store::ObjectStoreExt;
        match self
            .store
            .delete(&object_store::path::Path::from(path))
            .await
        {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(FaucetError::Sink(format!(
                "databricks sink: delete `{path}`: {e}"
            ))),
        }
    }
}

/// Build the object store for a cloud location from ambient credentials.
#[cfg(feature = "staging")]
pub fn build_object_store(
    loc: &StageLocation,
) -> Result<Arc<dyn object_store::ObjectStore>, FaucetError> {
    let err = |e: object_store::Error| {
        FaucetError::Config(format!("databricks sink: staging store: {e}"))
    };
    Ok(match loc {
        StageLocation::S3 { bucket, .. } => Arc::new(
            object_store::aws::AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map_err(err)?,
        ),
        StageLocation::Gcs { bucket, .. } => Arc::new(
            object_store::gcp::GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map_err(err)?,
        ),
        StageLocation::Azure {
            container, account, ..
        } => Arc::new(
            object_store::azure::MicrosoftAzureBuilder::from_env()
                .with_account(account)
                .with_container_name(container)
                .build()
                .map_err(err)?,
        ),
        StageLocation::Volume { .. } => {
            return Err(FaucetError::Config(
                "databricks sink: volume locations upload through the Files API".into(),
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_location_kind() {
        assert_eq!(
            StageLocation::parse("/Volumes/main/s/stage/faucet/").unwrap(),
            StageLocation::Volume {
                root: "/Volumes/main/s/stage/faucet".into()
            }
        );
        assert_eq!(
            StageLocation::parse("s3://b/p/q/").unwrap(),
            StageLocation::S3 {
                bucket: "b".into(),
                prefix: "p/q".into()
            }
        );
        assert_eq!(
            StageLocation::parse("gs://b").unwrap(),
            StageLocation::Gcs {
                bucket: "b".into(),
                prefix: String::new()
            }
        );
        assert_eq!(
            StageLocation::parse("abfss://c@acct.dfs.core.windows.net/x").unwrap(),
            StageLocation::Azure {
                container: "c".into(),
                account: "acct".into(),
                host: "acct.dfs.core.windows.net".into(),
                prefix: "x".into()
            }
        );
    }

    #[test]
    fn rejects_bad_locations() {
        for bad in [
            "/Volumes/main/s",
            "nope",
            "s3:///x",
            "ftp://b/x",
            "abfss://c/x",
            "abfss://@acct.dfs/x",
        ] {
            assert!(StageLocation::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn paths_and_uris() {
        let v = StageLocation::parse("/Volumes/m/s/v").unwrap();
        assert!(v.is_volume());
        assert_eq!(v.object_path("a/b.parquet"), "/Volumes/m/s/v/a/b.parquet");
        assert_eq!(v.uri("a/b.parquet"), "/Volumes/m/s/v/a/b.parquet");
        let s3 = StageLocation::parse("s3://b/p").unwrap();
        assert!(!s3.is_volume());
        assert_eq!(s3.object_path("f"), "p/f");
        assert_eq!(s3.uri("f"), "s3://b/p/f");
        let gs = StageLocation::parse("gs://b").unwrap();
        assert_eq!(gs.object_path("f"), "f");
        assert_eq!(gs.uri("f"), "gs://b/f");
        let az = StageLocation::parse("abfss://c@a.dfs.core.windows.net/p").unwrap();
        assert_eq!(az.uri("f"), "abfss://c@a.dfs.core.windows.net/p/f");
    }

    #[test]
    fn names_are_deterministic() {
        assert_eq!(fnv64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(sanitize("a b/c"), "a_b_c");
        assert_eq!(sanitize(""), "_");
        let a = eo_name("orders", "p::r", 7);
        assert_eq!(a, eo_name("orders", "p::r", 7));
        assert_ne!(a.dir, eo_name("orders", "p::other", 7).dir);
        assert_eq!(a.file, "00000000000000000007.parquet");
        assert!(a.rel().starts_with("_faucet/orders/"));
        let r1 = run_name("orders", "run 1", 3, b"abc");
        assert_eq!(r1, run_name("orders", "run 1", 3, b"abc"));
        assert_ne!(r1.file, run_name("orders", "run 1", 3, b"abd").file);
        assert_ne!(r1.file, run_name("orders", "run 1", 4, b"abc").file);
        assert!(r1.file.starts_with("part-000003-"));
        assert_eq!(r1.dir, "_faucet/orders/run_1");
    }

    #[test]
    fn parquet_round_trips_string_cells() {
        use arrow::array::Array;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let names = vec!["id".to_string(), "n".to_string()];
        let rows = vec![
            vec![Some("1".into()), None],
            vec![Some("2".into()), Some("x".into())],
        ];
        let bytes = encode_parquet(&names, &rows).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.map(Result::unwrap).collect();
        let b = &batches[0];
        assert_eq!(b.num_rows(), 2);
        let n = b
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert!(n.is_null(0));
        assert_eq!(n.value(1), "x");
    }

    fn client(base: &str) -> StatementClient {
        StatementClient::new(
            reqwest::Client::new(),
            base,
            "wh",
            faucet_core::AuthSpec::Inline(faucet_common_databricks::DatabricksAuth::Pat {
                token: "t".into(),
            }),
            None,
            faucet_common_databricks::StatementOptions {
                max_retries: 1,
                retry_backoff: std::time::Duration::from_millis(1),
                ..Default::default()
            },
            faucet_common_databricks::ErrorSide::Sink,
        )
    }

    #[test]
    fn files_api_url_encodes_and_rejects_bad_bases() {
        let s = FilesApiStager::new(client("https://h.example/"));
        assert_eq!(
            s.url("/Volumes/a/b c/f.parquet").unwrap().as_str(),
            "https://h.example/api/2.0/fs/files/Volumes/a/b%20c/f.parquet"
        );
        assert!(FilesApiStager::new(client("not a url")).url("/x").is_err());
        assert!(FilesApiStager::new(client("mailto:x")).url("/x").is_err());
    }

    #[tokio::test]
    async fn files_api_transport_errors_are_retried_then_reported() {
        let s = FilesApiStager::new(client("http://127.0.0.1:1"));
        let err = s.put("/Volumes/a/b/c/f", vec![1]).await.unwrap_err();
        assert!(err.to_string().contains("Files API PUT"), "{err}");
        assert!(s.delete("/Volumes/a/b/c/f").await.is_err());
    }

    #[cfg(feature = "staging")]
    #[test]
    fn object_stores_build_per_scheme() {
        for loc in [
            "s3://b/p",
            "gs://b/p",
            "abfss://c@acct.dfs.core.windows.net/p",
        ] {
            let _ = build_object_store(&StageLocation::parse(loc).unwrap());
        }
        assert!(build_object_store(&StageLocation::parse("/Volumes/a/b/c").unwrap()).is_err());
    }
}
