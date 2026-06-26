//! An `ObjectStore` wrapper that records write-path bytes and request
//! counts, layered over a base store, so a benchmark can measure how many
//! bytes and requests reach object storage. Single PUTs and multipart
//! upload parts are both counted, so the total is complete however the store
//! writes an object; `multipart_count` reports how many multipart uploads
//! occurred, as a diagnostic (their bytes and parts are already in the
//! totals).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use taquba::object_store::path::Path;
use taquba::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
};

/// Records write-path bytes and request counts across single PUTs and
/// multipart upload parts. Reads, lists and deletes pass through unchanged.
#[derive(Debug)]
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    // Shared with the multipart-upload wrapper so streamed parts count too.
    put_bytes: Arc<AtomicU64>,
    put_count: Arc<AtomicU64>,
    multipart_count: AtomicU64,
}

impl CountingStore {
    /// Wrap `inner`, recording every write.
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            put_bytes: Arc::new(AtomicU64::new(0)),
            put_count: Arc::new(AtomicU64::new(0)),
            multipart_count: AtomicU64::new(0),
        }
    }

    /// Total bytes written, across single PUTs and multipart parts.
    pub fn put_bytes(&self) -> u64 {
        self.put_bytes.load(Ordering::Relaxed)
    }

    /// Total write requests, counting each single PUT and each
    /// multipart part as one.
    pub fn put_count(&self) -> u64 {
        self.put_count.load(Ordering::Relaxed)
    }

    /// Number of multipart uploads started, a diagnostic; their bytes and
    /// parts are already included in `put_bytes` and `put_count`.
    pub fn multipart_count(&self) -> u64 {
        self.multipart_count.load(Ordering::Relaxed)
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

// Only the write entry points record; the no-default methods are delegated
// and the remaining trait methods keep their defaults, which route through
// these (for example `put` calls `put_opts`), so every write is counted and
// reads stay untouched.
#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.put_bytes
            .fetch_add(payload.content_length() as u64, Ordering::Relaxed);
        self.put_count.fetch_add(1, Ordering::Relaxed);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.multipart_count.fetch_add(1, Ordering::Relaxed);
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(CountingUpload {
            inner,
            put_bytes: self.put_bytes.clone(),
            put_count: self.put_count.clone(),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Wraps a multipart upload so each part's bytes and a per-part request are
/// recorded into the store's shared totals.
#[derive(Debug)]
struct CountingUpload {
    inner: Box<dyn MultipartUpload>,
    put_bytes: Arc<AtomicU64>,
    put_count: Arc<AtomicU64>,
}

#[async_trait]
impl MultipartUpload for CountingUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.put_bytes
            .fetch_add(data.content_length() as u64, Ordering::Relaxed);
        self.put_count.fetch_add(1, Ordering::Relaxed);
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        self.inner.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}
