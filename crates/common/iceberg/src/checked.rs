//! Storage that checks every location it opens, and the endpoints a catalog
//! hands it, against the reference policy of a config the host does not
//! trust: table, manifest and data-file locations come from the catalog, not
//! from the config, so they are checked when they are used.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use faucet_core::resolve::ReferencePolicy;
use futures::stream::{BoxStream, StreamExt};
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use serde::{Deserialize, Serialize};

/// Storage properties naming an endpoint the storage connects to.
const ENDPOINT_PROPS: [&str; 3] = ["s3.endpoint", "gcs.service.path", "adls.endpoint"];

fn refused(what: &str, reason: impl std::fmt::Display) -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::DataInvalid,
        format!("iceberg: {what} is refused for this config: {reason}"),
    )
}

/// `inner` when the task's reference policy allows any path, else `inner`
/// wrapped so every location and vended endpoint is checked against it.
pub(crate) fn checked(inner: Arc<dyn StorageFactory>) -> Arc<dyn StorageFactory> {
    match faucet_core::http::current_reference_policy().filter(|p| p.path_roots().is_some()) {
        Some(policy) => Arc::new(CheckedFactory {
            inner: Some(inner),
            policy: Some(policy),
        }),
        None => inner,
    }
}

/// A [`StorageFactory`] whose storage checks locations and endpoints.
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct CheckedFactory {
    #[serde(skip)]
    inner: Option<Arc<dyn StorageFactory>>,
    #[serde(skip)]
    policy: Option<Arc<ReferencePolicy>>,
}

fn check_endpoints(
    policy: &ReferencePolicy,
    props: &HashMap<String, String>,
) -> iceberg::Result<()> {
    for key in ENDPOINT_PROPS {
        if let Some(v) = props.get(key).filter(|v| !v.trim().is_empty()) {
            policy
                .check_url(v)
                .map_err(|r| refused(&format!("the storage endpoint `{key}`"), r))?;
        }
    }
    Ok(())
}

#[typetag::serde(name = "faucet-checked")]
impl StorageFactory for CheckedFactory {
    fn build(&self, config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        let (Some(inner), Some(policy)) = (&self.inner, &self.policy) else {
            return Err(refused("storage", "it was not built in this process"));
        };
        check_endpoints(policy, config.props())?;
        Ok(Arc::new(CheckedStorage {
            inner: Some(inner.build(config)?),
            policy: Some(Arc::clone(policy)),
        }))
    }
}

/// Storage that refuses locations the policy does not allow.
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct CheckedStorage {
    #[serde(skip)]
    inner: Option<Arc<dyn Storage>>,
    #[serde(skip)]
    policy: Option<Arc<ReferencePolicy>>,
}

impl CheckedStorage {
    fn at(&self, path: &str) -> iceberg::Result<&Arc<dyn Storage>> {
        let (Some(inner), Some(policy)) = (&self.inner, &self.policy) else {
            return Err(refused("storage", "it was not built in this process"));
        };
        policy
            .check_path(path)
            .map_err(|r| refused(&format!("the location '{path}'"), r))?;
        Ok(inner)
    }
}

#[faucet_core::async_trait]
#[typetag::serde(name = "faucet-checked-storage")]
impl Storage for CheckedStorage {
    async fn exists(&self, path: &str) -> iceberg::Result<bool> {
        self.at(path)?.exists(path).await
    }

    async fn metadata(&self, path: &str) -> iceberg::Result<FileMetadata> {
        self.at(path)?.metadata(path).await
    }

    async fn read(&self, path: &str) -> iceberg::Result<Bytes> {
        self.at(path)?.read(path).await
    }

    async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn FileRead>> {
        self.at(path)?.reader(path).await
    }

    async fn write(&self, path: &str, bs: Bytes) -> iceberg::Result<()> {
        self.at(path)?.write(path, bs).await
    }

    async fn writer(&self, path: &str) -> iceberg::Result<Box<dyn FileWrite>> {
        self.at(path)?.writer(path).await
    }

    async fn delete(&self, path: &str) -> iceberg::Result<()> {
        self.at(path)?.delete(path).await
    }

    async fn delete_prefix(&self, path: &str) -> iceberg::Result<()> {
        self.at(path)?.delete_prefix(path).await
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> iceberg::Result<()> {
        let paths: Vec<String> = paths.collect().await;
        let mut inner = None;
        for p in &paths {
            inner = Some(Arc::clone(self.at(p)?));
        }
        match inner {
            Some(inner) => {
                inner
                    .delete_stream(futures::stream::iter(paths).boxed())
                    .await
            }
            None => Ok(()),
        }
    }

    fn new_input(&self, path: &str) -> iceberg::Result<InputFile> {
        self.at(path)?.new_input(path)
    }

    fn new_output(&self, path: &str) -> iceberg::Result<OutputFile> {
        self.at(path)?.new_output(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::io::LocalFsStorageFactory;

    fn policy(root: &std::path::Path) -> Arc<ReferencePolicy> {
        Arc::new(
            ReferencePolicy::deny_all()
                .with_path_roots(vec![root.to_path_buf()])
                .any_url_scheme()
                .any_url_host(),
        )
    }

    #[tokio::test]
    async fn catalog_locations_outside_the_roots_are_refused_for_callers() {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        std::fs::create_dir(root.join("in")).unwrap();
        std::fs::create_dir(root.join("out")).unwrap();
        std::fs::write(root.join("in/m.avro"), b"in").unwrap();
        std::fs::write(root.join("out/m.avro"), b"out").unwrap();
        let p = policy(&root.join("in"));

        let trusted = checked(Arc::new(LocalFsStorageFactory));
        assert!(!format!("{trusted:?}").contains("CheckedFactory"));
        let factory = faucet_core::http::with_reference_policy(Arc::clone(&p), async {
            checked(Arc::new(LocalFsStorageFactory))
        })
        .await;
        assert!(format!("{factory:?}").contains("CheckedFactory"));
        let storage = factory
            .build(&StorageConfig::from_props(HashMap::new()))
            .unwrap();

        let inside = root.join("in/m.avro").display().to_string();
        let outside = root.join("out/m.avro").display().to_string();
        assert_eq!(&storage.read(&inside).await.unwrap()[..], b"in");
        assert!(storage.exists(&inside).await.unwrap());
        assert_eq!(storage.metadata(&inside).await.unwrap().size, 2);
        assert!(storage.new_input(&inside).is_ok());
        assert!(storage.reader(&inside).await.is_ok());
        for e in [
            storage.read(&outside).await.err(),
            storage.exists(&outside).await.err(),
            storage.metadata(&outside).await.err(),
            storage.reader(&outside).await.err(),
            storage
                .write(&outside, Bytes::from_static(b"x"))
                .await
                .err(),
            storage.writer(&outside).await.err(),
            storage.delete(&outside).await.err(),
            storage.delete_prefix(&outside).await.err(),
            storage.new_input(&format!("file://{outside}")).err(),
            storage.new_output(&outside).err(),
            storage
                .delete_stream(futures::stream::iter([inside.clone(), outside.clone()]).boxed())
                .await
                .err(),
        ] {
            let e = e.expect("refused").to_string();
            assert!(e.contains("outside the allowed path roots"), "{e}");
        }
        assert_eq!(std::fs::read(root.join("out/m.avro")).unwrap(), b"out");
        let written = root.join("in/w.avro").display().to_string();
        storage
            .write(&written, Bytes::from_static(b"w"))
            .await
            .unwrap();
        assert!(storage.new_output(&written).is_ok());
        assert!(
            storage
                .writer(&root.join("in/w2.avro").display().to_string())
                .await
                .is_ok()
        );
        storage
            .delete_stream(futures::stream::iter([written.clone()]).boxed())
            .await
            .unwrap();
        storage
            .delete_stream(futures::stream::empty().boxed())
            .await
            .unwrap();
        storage
            .delete_prefix(&root.join("in/zz").display().to_string())
            .await
            .unwrap();
        storage
            .delete(&root.join("in/w2.avro").display().to_string())
            .await
            .ok();

        let vended = StorageConfig::from_props(HashMap::from([(
            "s3.endpoint".to_string(),
            "http://169.254.169.254/".to_string(),
        )]));
        let e = factory.build(&vended).unwrap_err().to_string();
        assert!(e.contains("s3.endpoint"), "{e}");

        let unbuilt = CheckedFactory::default();
        assert!(
            unbuilt
                .build(&StorageConfig::from_props(HashMap::new()))
                .is_err()
        );
        assert!(CheckedStorage::default().read(&inside).await.is_err());
    }
}
