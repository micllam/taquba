//! Helpers shared by the test modules of the crate.

pub(crate) use std::collections::HashMap;
pub(crate) use std::sync::Arc;
pub(crate) use std::time::Duration;

pub(crate) use slatedb::object_store::ObjectStore;
pub(crate) use slatedb::object_store::memory::InMemory;

pub(crate) use slatedb::object_store::Error as StoreError;

pub(crate) use crate::clock::{Clock, MockClock};
pub(crate) use crate::effects::SettlementEffects;
pub(crate) use crate::job::{Claim, JobRecord, JobStatus};
pub(crate) use crate::keys::{KeyTag, tag_prefix};
pub(crate) use crate::lease::LeaseHandle;
pub(crate) use crate::options::{
    EnqueueOptions, OpenOptions, PRIORITY_HIGH, PRIORITY_LOW, QueueConfig,
};
pub(crate) use crate::queue::{CancelOutcome, NackOutcome, Queue, WaitOutcome, WakeOutcome};

pub(crate) fn make_store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

/// OpenOptions that disable retry backoff so nack tests can re-claim
/// immediately. Production defaults are exponential, so the "claim
/// straight after nack" assertion needs an explicit opt-out.
pub(crate) fn no_backoff_opts() -> OpenOptions {
    OpenOptions {
        default_queue_config: QueueConfig {
            retry_backoff_base: Duration::ZERO,
            retry_backoff_max: Duration::ZERO,
            ..QueueConfig::default()
        },
        ..OpenOptions::default()
    }
}

/// OpenOptions with a small offload threshold so tests exercise the
/// offload path.
pub(crate) fn offload_opts() -> OpenOptions {
    OpenOptions {
        payload_offload_threshold: Some(64),
        default_queue_config: QueueConfig {
            retry_backoff_base: Duration::ZERO,
            retry_backoff_max: Duration::ZERO,
            ..QueueConfig::default()
        },
        ..OpenOptions::default()
    }
}

/// Number of objects under `prefix` in `store`. Payload objects for
/// a queue opened at `"test"` live under `"test-payloads"`.
pub(crate) async fn object_count(store: &Arc<dyn ObjectStore>, prefix: &str) -> usize {
    store
        .list_with_delimiter(Some(&slatedb::object_store::path::Path::from(prefix)))
        .await
        .unwrap()
        .objects
        .len()
}

use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use slatedb::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
    path::Path as StorePath,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// In-memory object store whose `put` and `delete` requests fail with
/// a synthetic service-unavailable error while the corresponding flag
/// is set. Reads and lists are unaffected.
#[derive(Debug)]
pub(crate) struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    fail_puts: AtomicBool,
    fail_deletes: AtomicBool,
    /// Puts permitted before every later put fails. `usize::MAX`
    /// disables the countdown, leaving `fail_puts` as the only cause
    /// of failure.
    puts_before_failure: AtomicUsize,
}

impl FaultStore {
    pub(crate) fn wrap() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(InMemory::new()),
            fail_puts: AtomicBool::new(false),
            fail_deletes: AtomicBool::new(false),
            puts_before_failure: AtomicUsize::new(usize::MAX),
        })
    }

    pub(crate) fn fail_puts(&self, fail: bool) {
        self.fail_puts.store(fail, Ordering::SeqCst);
    }

    pub(crate) fn fail_deletes(&self, fail: bool) {
        self.fail_deletes.store(fail, Ordering::SeqCst);
    }

    /// Permit `n` further puts, then fail every put after them.
    pub(crate) fn fail_puts_after(&self, n: usize) {
        self.puts_before_failure.store(n, Ordering::SeqCst);
    }

    /// Whether this put fails, consuming one permitted put when it
    /// does not. Callers of the payload store issue puts
    /// sequentially, so a read followed by a store is sufficient.
    pub(crate) fn put_fails(&self) -> bool {
        if self.fail_puts.load(Ordering::SeqCst) {
            return true;
        }
        match self.puts_before_failure.load(Ordering::SeqCst) {
            usize::MAX => false,
            0 => true,
            remaining => {
                self.puts_before_failure
                    .store(remaining - 1, Ordering::SeqCst);
                false
            }
        }
    }

    pub(crate) fn synthetic_503() -> StoreError {
        StoreError::Generic {
            store: "FaultStore",
            source: "synthetic 503 Service Unavailable".into(),
        }
    }
}

impl std::fmt::Display for FaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        if self.put_fails() {
            return Err(Self::synthetic_503());
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        if self.put_fails() {
            return Err(Self::synthetic_503());
        }
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &StorePath, options: GetOptions) -> StoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    // Deletion reaches the trait through `delete_stream`, so the fault
    // is injected by replacing each location's result with an error.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<StorePath>>,
    ) -> BoxStream<'static, StoreResult<StorePath>> {
        if self.fail_deletes.load(Ordering::SeqCst) {
            return locations
                .map(|location| location.and(Err(Self::synthetic_503())))
                .boxed();
        }
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&StorePath>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&StorePath>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> StoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
