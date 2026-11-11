//! An object store that keeps every key it serves under one table's root, for
//! configs the host does not trust: the Delta log can name files (checkpoint
//! sidecars, add actions, deletion vectors) anywhere in the store it is read
//! through, and the reader would follow them.

use deltalake::logstore::object_store::{
    self, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, path::Path,
};
use futures::stream::{BoxStream, StreamExt};
use std::ops::Range;
use std::sync::Arc;

/// The text every refusal carries.
pub(crate) const OUTSIDE: &str = "is outside the Delta table";

/// `inner`, serving only keys under `root`.
#[derive(Debug)]
pub(crate) struct Confined {
    inner: Arc<dyn ObjectStore>,
    root: Path,
}

impl Confined {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>, root: Path) -> Self {
        Self { inner, root }
    }

    fn check(&self, location: &Path) -> object_store::Result<()> {
        check(&self.root, location)
    }

    fn scope<'a>(&'a self, prefix: Option<&'a Path>) -> object_store::Result<&'a Path> {
        match prefix {
            None => Ok(&self.root),
            Some(p) => self.check(p).map(|()| p),
        }
    }
}

fn check(root: &Path, location: &Path) -> object_store::Result<()> {
    if location.prefix_matches(root) {
        return Ok(());
    }
    Err(object_store::Error::PermissionDenied {
        path: location.to_string(),
        source: format!("'{location}' {OUTSIDE} at '{root}'").into(),
    })
}

impl std::fmt::Display for Confined {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Confined({}, {})", self.inner, self.root)
    }
}

#[faucet_core::async_trait]
impl ObjectStore for Confined {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.check(location)?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.check(location)?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.check(location)?;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        self.check(location)?;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let root = self.root.clone();
        let checked = locations
            .map(move |p| {
                let p = p?;
                check(&root, &p).map(|()| p)
            })
            .boxed();
        self.inner.delete_stream(checked)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        match self.scope(prefix) {
            Ok(p) => self.inner.list(Some(p)),
            Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
        }
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        match self.scope(prefix) {
            Ok(p) => self.inner.list_with_offset(Some(p), offset),
            Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let p = self.scope(prefix)?;
        self.inner.list_with_delimiter(Some(p)).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.check(from)?;
        self.check(to)?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.check(from)?;
        self.check(to)?;
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::logstore::object_store::ObjectStoreExt as _;
    use deltalake::logstore::object_store::memory::InMemory;
    use futures::TryStreamExt as _;

    fn p(s: &str) -> Path {
        Path::from(s)
    }

    #[tokio::test]
    async fn keys_outside_the_table_are_refused() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        inner.put(&p("t/a"), "a".into()).await.unwrap();
        inner.put(&p("other/b"), "b".into()).await.unwrap();
        inner.put(&p("tt/c"), "c".into()).await.unwrap();
        let s = Confined::new(Arc::clone(&inner), p("t"));
        assert!(format!("{s}").contains("Confined"));

        assert_eq!(s.get(&p("t/a")).await.unwrap().bytes().await.unwrap(), "a");
        let e = s.get(&p("other/b")).await.unwrap_err().to_string();
        assert!(e.contains(OUTSIDE), "{e}");
        assert!(s.get(&p("tt/c")).await.is_err(), "segment-wise prefix");
        assert!(s.head(&p("other/b")).await.is_err());
        assert!(s.get_ranges(&p("other/b"), &[0..1, 0..1]).await.is_err());
        assert_eq!(s.get_ranges(&p("t/a"), &[0..1, 0..1]).await.unwrap()[0], "a");

        s.put(&p("t/n"), "n".into()).await.unwrap();
        assert!(s.put(&p("other/n"), "n".into()).await.is_err());
        assert!(s.put_multipart(&p("other/m")).await.is_err());
        s.put_multipart(&p("t/m"))
            .await
            .unwrap()
            .abort()
            .await
            .unwrap();
        assert!(s.copy(&p("t/a"), &p("other/x")).await.is_err());
        assert!(s.copy(&p("other/b"), &p("t/x")).await.is_err());
        s.copy(&p("t/a"), &p("t/x")).await.unwrap();
        assert!(s.rename(&p("t/x"), &p("other/y")).await.is_err());
        s.rename(&p("t/x"), &p("t/y")).await.unwrap();

        let all: Vec<_> = s.list(None).try_collect().await.unwrap();
        assert!(all.iter().all(|m| m.location.prefix_matches(&p("t"))));
        assert!(
            s.list(Some(&p("other")))
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
        assert!(
            s.list_with_offset(Some(&p("other")), &p("other/a"))
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
        let after: Vec<_> = s
            .list_with_offset(None, &p("t/a"))
            .try_collect()
            .await
            .unwrap();
        assert!(after.iter().all(|m| m.location.prefix_matches(&p("t"))));
        assert!(s.list_with_delimiter(Some(&p("other"))).await.is_err());
        let top = s.list_with_delimiter(None).await.unwrap();
        assert!(
            top.objects
                .iter()
                .all(|m| m.location.prefix_matches(&p("t")))
        );

        let deleted: Vec<_> = s
            .delete_stream(futures::stream::iter([Ok(p("t/y")), Ok(p("other/b"))]).boxed())
            .collect()
            .await;
        assert!(deleted[0].is_ok() && deleted[1].is_err());
        assert!(inner.head(&p("other/b")).await.is_ok());
    }
}
