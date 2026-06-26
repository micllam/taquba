//! An `ObjectStore` wrapper that adds random tail latency to the write
//! path, layered over a base store. It injects object-store PUT tail
//! latency as a controllable variable so its effect on e2e latency and
//! backlog can be studied locally with no cloud cost.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use taquba::object_store::path::Path;
use taquba::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

/// Delays each write by a random duration in `[0, max_jitter]`,
/// right-skewed so most delays are small and a few are close to the
/// maximum, approximating object-store tail latency. Reads, lists and
/// deletes pass through unchanged. Jitter is applied to single PUTs and to
/// the start of a multipart upload, not to each multipart part, so the data
/// streamed during a multipart upload is not delayed; the latency-sensitive
/// single-PUT write path is covered.
#[derive(Debug)]
pub struct JitterStore {
    inner: Arc<dyn ObjectStore>,
    max_jitter: Duration,
}

impl JitterStore {
    /// Wrap `inner`, delaying each write by up to `max_jitter`.
    pub fn new(inner: Arc<dyn ObjectStore>, max_jitter: Duration) -> Self {
        Self { inner, max_jitter }
    }

    async fn jitter(&self) {
        // Square a uniform [0, 1) for a right-skewed tail.
        let u: f64 = rand::random();
        let delay = self.max_jitter.mul_f64(u * u);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
}

impl std::fmt::Display for JitterStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JitterStore({})", self.inner)
    }
}

// Only the write entry points add jitter and the no-default methods are
// delegated; the remaining trait methods keep their defaults, which route
// through these (for example `put` calls `put_opts`, `get` calls
// `get_opts`), so reads stay jitter-free and writes are covered.
#[async_trait]
impl ObjectStore for JitterStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.jitter().await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.jitter().await;
        self.inner.put_multipart_opts(location, opts).await
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
