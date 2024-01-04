use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::Result as OSResult;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    PutOptions, PutResult,
};
use tokio::io::AsyncWrite;

#[derive(Debug, Default)]
pub struct ReadMetrics {
    read_count: AtomicUsize,
    read_bytes: AtomicUsize,
}

impl ReadMetrics {
    fn record_read(&self, bytes: usize) {
        self.read_count.fetch_add(1, Ordering::Relaxed);
        self.read_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn metrics(&self) -> (usize, usize) {
        (
            self.read_count.load(Ordering::Relaxed),
            self.read_bytes.load(Ordering::Relaxed),
        )
    }

    pub fn reset(&self) {
        self.read_count.store(0, Ordering::Relaxed);
        self.read_bytes.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct MeteredObjectStore {
    metrics: Arc<ReadMetrics>,
    inner: Arc<dyn ObjectStore>,
}

impl MeteredObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, metrics: Arc<ReadMetrics>) -> Self {
        Self { metrics, inner }
    }
}

impl std::fmt::Display for MeteredObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for MeteredObjectStore {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
        let res = self.inner.get_opts(location, options).await?;
        self.metrics.record_read(res.meta.size);
        Ok(res)
    }

    async fn get_range(&self, location: &Path, range: std::ops::Range<usize>) -> OSResult<Bytes> {
        let bytes = self.inner.get_range(location, range).await?;
        self.metrics.record_read(bytes.len());
        Ok(bytes)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<usize>],
    ) -> OSResult<Vec<Bytes>> {
        let bytes = self.inner.get_ranges(location, ranges).await?;
        for b in &bytes {
            self.metrics.record_read(b.len());
        }
        Ok(bytes)
    }

    async fn put(&self, location: &Path, bytes: Bytes) -> OSResult<PutResult> {
        self.inner.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: Bytes,
        options: PutOptions,
    ) -> OSResult<PutResult> {
        self.inner.put_opts(location, bytes, options).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> OSResult<(MultipartId, Box<dyn AsyncWrite + Send + Unpin>)> {
        self.inner.put_multipart(location).await
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> OSResult<()> {
        self.inner.abort_multipart(location, multipart_id).await
    }

    async fn head(&self, location: &Path) -> OSResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OSResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OSResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OSResult<()> {
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> OSResult<()> {
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OSResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
