//! Read-only, cross-process observation of a queue's store.
//!
//! [`QueueReader`] opens the same object-store path as a live
//! [`Queue`](crate::Queue) and serves the queue's read-only API from a
//! second process: dashboards, CLIs and health checks that must observe
//! a queue they do not own. The reader is observation only: it takes no
//! writes, holds no clock and offers no lease view, and its view lags
//! the writer by the writer's flush interval plus the reader's
//! [`ReaderOptions::manifest_poll_interval`].

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use slatedb::config::DbReaderOptions;
use slatedb::object_store::ObjectStore;
use slatedb::{DbReader, DbReaderMode};

use crate::error::Result;
use crate::history::JobAttempt;
use crate::job::{JobRecord, JobStatus};
use crate::payload_store::PayloadStore;
use crate::queue::{JobPage, KvPage};
use crate::stats::{QueueMergeOperator, QueueStats};

/// How a [`QueueReader`] follows the writer's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReaderMode {
    /// Maintain a checkpoint against the latest store state, refreshed
    /// on every manifest poll, so the objects the reader's view
    /// references are protected from garbage collection while it pages
    /// through them. Checkpoint refreshes are manifest writes, so this
    /// mode requires write credentials to the bucket; it never touches
    /// the writer's epoch and does not fence the writer.
    #[default]
    ManagedCheckpoint,
    /// Follow the latest manifest without a checkpoint. Performs no
    /// object-store writes, so read-only credentials suffice, but
    /// nothing protects the view from garbage collection: a read
    /// against an aged view can fail on a collected object and must be
    /// retried.
    FollowLatest,
}

/// Options for [`QueueReader::open_with_options`].
#[derive(Clone)]
pub struct ReaderOptions {
    /// How the reader follows the writer's state. Defaults to
    /// [`ReaderMode::ManagedCheckpoint`].
    pub mode: ReaderMode,
    /// How often the reader polls for new manifest and WAL data.
    /// Defaults to 10 seconds. Together with the writer's flush
    /// interval this bounds the reader's lag behind the writer.
    pub manifest_poll_interval: Duration,
    /// Expiry granted to the managed checkpoint on each refresh.
    /// Defaults to 10 minutes; must be at least twice
    /// [`Self::manifest_poll_interval`]. Ignored under
    /// [`ReaderMode::FollowLatest`].
    pub checkpoint_lifetime: Duration,
    /// Object store for offloaded payloads. Must match the writer's
    /// [`OpenOptions::payload_store`](crate::OpenOptions::payload_store);
    /// `None` (the default) uses the object store the reader is opened
    /// on.
    pub payload_store: Option<Arc<dyn ObjectStore>>,
    /// Path prefix for offloaded payload objects. Must match the
    /// writer's [`OpenOptions::payload_path`](crate::OpenOptions::payload_path);
    /// `None` (the default) uses `"{path}-payloads"`.
    pub payload_path: Option<String>,
}

impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            mode: ReaderMode::default(),
            manifest_poll_interval: Duration::from_secs(10),
            checkpoint_lifetime: Duration::from_secs(10 * 60),
            payload_store: None,
            payload_path: None,
        }
    }
}

/// A read-only view of a queue store, openable from a process other
/// than the writer's.
///
/// Serves the same read-only API as [`Queue`](crate::Queue): job counts,
/// queue and job listings, job lookup, attempt histories and the user
/// KV namespace. The two surfaces share their implementations, so a
/// query returns the same result through either, up to the reader's
/// lag.
///
/// # Semantics of a lagging view
///
/// The reader observes whole commits or nothing: a transaction's writes
/// become visible together. The view lags the writer by up to the
/// writer's flush interval plus
/// [`ReaderOptions::manifest_poll_interval`].
///
/// A job reported `Claimed` means a claim was taken and no settlement
/// is visible yet. It says nothing about liveness: the lease is state
/// inside the writer process, so the reader offers no lease view, and a
/// claimed record can belong to a process that no longer runs.
///
/// [`Error::PayloadMissing`](crate::Error::PayloadMissing) from a
/// reader can be transient: a job removal deletes the payload object
/// after its record, so a reader whose view still holds the record can
/// find the object gone. The condition clears once the view advances
/// past the removal, bounded by the reader's lag.
///
/// # Compatibility and failure modes
///
/// Reader and writer must run the same taquba minor version: the
/// on-disk layout may change between minors and no layout stamp is
/// stored. Opening a reader against a path no writer has ever
/// created fails on the missing manifest; a health check racing the
/// first deployment must expect that error.
///
/// # Observable outcomes
///
/// The queue's own records are swept by retention, so a consumer keeps
/// its outcomes readable across processes by settling them into the
/// user KV namespace:
/// [`Queue::ack_with`](crate::Queue::ack_with) writes outcome entries
/// atomically with the settlement (and
/// [`Queue::enqueue_with_kv`](crate::Queue::enqueue_with_kv) maps
/// caller identifiers to job ids at submit), and [`Self::kv_get`] /
/// [`Self::kv_scan`] serve them here.
pub struct QueueReader {
    reader: DbReader,
    payload_store: Arc<PayloadStore>,
}

impl QueueReader {
    /// Open a reader with default options.
    pub async fn open(object_store: Arc<dyn ObjectStore>, path: &str) -> Result<Self> {
        Self::open_with_options(object_store, path, ReaderOptions::default()).await
    }

    /// Open a reader with explicit options.
    pub async fn open_with_options(
        object_store: Arc<dyn ObjectStore>,
        path: &str,
        opts: ReaderOptions,
    ) -> Result<Self> {
        let payload_store = Arc::new(PayloadStore::new(
            opts.payload_store.unwrap_or_else(|| object_store.clone()),
            opts.payload_path
                .unwrap_or_else(|| format!("{path}-payloads")),
        ));
        let mode = match opts.mode {
            ReaderMode::ManagedCheckpoint => DbReaderMode::ManagedCheckpoint,
            ReaderMode::FollowLatest => DbReaderMode::FollowLatest,
        };
        let reader = DbReader::builder(path, object_store)
            .with_reader_mode(mode)
            // Stats counters and attempt histories are merge operands;
            // without the operator their reads fail.
            .with_merge_operator(Arc::new(QueueMergeOperator))
            .with_options(DbReaderOptions {
                manifest_poll_interval: opts.manifest_poll_interval,
                checkpoint_lifetime: opts.checkpoint_lifetime,
                ..DbReaderOptions::default()
            })
            .build()
            .await?;
        Ok(Self {
            reader,
            payload_store,
        })
    }

    /// Return a snapshot of job counts for the given queue.
    pub async fn stats(&self, queue: &str) -> Result<QueueStats> {
        crate::read::stats(&self.reader, queue).await
    }

    /// Return the names of all queues that have ever had at least one job.
    pub async fn list_queues(&self) -> Result<Vec<String>> {
        crate::read::list_queues(&self.reader).await
    }

    /// Return a page of the given queue's jobs in one lifecycle state.
    ///
    /// Ordering, cursor and paging semantics are those of
    /// [`Queue::list_jobs`](crate::Queue::list_jobs).
    pub async fn list_jobs(
        &self,
        queue: &str,
        status: JobStatus,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<JobPage> {
        crate::read::list_jobs(
            &self.reader,
            &self.payload_store,
            queue,
            status,
            cursor,
            limit,
        )
        .await
    }

    /// Return a page of dead-letter jobs for the given queue.
    ///
    /// Cursor and ordering semantics are those of
    /// [`Queue::dead_jobs`](crate::Queue::dead_jobs).
    pub async fn dead_jobs(
        &self,
        queue: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<JobRecord>> {
        crate::read::dead_jobs(&self.reader, &self.payload_store, queue, after, limit).await
    }

    /// Look up a job by ID regardless of its current state.
    ///
    /// Returns `None` if the ID was never enqueued or has since been
    /// expunged. The index and the record are two plain reads of the
    /// reader's view.
    pub async fn get_job(&self, id: &str) -> Result<Option<JobRecord>> {
        crate::read::get_job(&self.reader, &self.payload_store, id).await
    }

    /// Return a job's recorded delivery history, in write order.
    ///
    /// Entry and lifetime semantics are those of
    /// [`Queue::attempt_history`](crate::Queue::attempt_history).
    pub async fn attempt_history(&self, id: &str) -> Result<Vec<JobAttempt>> {
        crate::read::attempt_history(&self.reader, id).await
    }

    /// Read a value from the user KV namespace.
    pub async fn kv_get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        crate::read::kv_get(&self.reader, key).await
    }

    /// List entries of the user KV namespace under `prefix`, in
    /// ascending byte order of the keys.
    ///
    /// Cursor and paging semantics are those of
    /// [`Queue::kv_scan`](crate::Queue::kv_scan).
    pub async fn kv_scan(
        &self,
        prefix: &[u8],
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<KvPage> {
        crate::read::kv_scan(&self.reader, prefix, cursor, limit).await
    }

    /// Close the reader, stopping its manifest polling and releasing
    /// its managed checkpoint.
    pub async fn close(&self) -> Result<()> {
        self.reader.close().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::queue::{OpenOptions, Queue};
    use slatedb::object_store::ObjectStoreExt;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::path::Path;

    fn make_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    #[tokio::test]
    async fn a_reader_sees_the_store_a_writer_flushed() {
        let store = make_store();
        let q = Queue::open(store.clone(), "test").await.unwrap();
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        q.kv_put(b"outcome/1", b"ok").await.unwrap();

        let reader = QueueReader::open(store, "test").await.unwrap();
        let job = reader.get_job(&id).await.unwrap().unwrap();
        assert_eq!(job.id, id);
        assert_eq!(job.payload, b"payload");
        assert_eq!(job.status, JobStatus::Pending);

        let page = reader
            .list_jobs("work", JobStatus::Pending, None, 10)
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, id);

        assert_eq!(reader.list_queues().await.unwrap(), vec!["work"]);
        assert_eq!(
            reader.kv_get(b"outcome/1").await.unwrap().unwrap().as_ref(),
            b"ok"
        );
        let kv_page = reader.kv_scan(b"outcome/", None, 10).await.unwrap();
        assert_eq!(kv_page.entries.len(), 1);

        reader.close().await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn reader_stats_apply_the_merge_operator() {
        let store = make_store();
        let q = Queue::open(store.clone(), "test").await.unwrap();
        for _ in 0..3 {
            q.enqueue("work", b"x".to_vec()).await.unwrap();
        }

        let reader = QueueReader::open(store, "test").await.unwrap();
        let stats = reader.stats("work").await.unwrap();
        assert_eq!(stats.pending, 3);
        assert_eq!(stats.claimed, 0);
        assert_eq!(stats.done, 0);

        reader.close().await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_reader_does_not_fence_the_writer() {
        let store = make_store();
        let q = Queue::open(store.clone(), "test").await.unwrap();
        q.enqueue("work", b"a".to_vec()).await.unwrap();

        let reader = QueueReader::open(store, "test").await.unwrap();
        assert_eq!(reader.stats("work").await.unwrap().pending, 1);

        // The writer keeps writing after the reader opened and refreshed
        // its checkpoint: claims, settlements and enqueues all succeed.
        q.enqueue("work", b"b".to_vec()).await.unwrap();
        let claim = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&claim).await.unwrap();
        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.pending, 1);
        assert_eq!(stats.done, 1);

        reader.close().await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_reader_reports_a_missing_payload_object() {
        let store = make_store();
        let opts = OpenOptions {
            payload_offload_threshold: Some(64),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(store.clone(), "test", opts)
            .await
            .unwrap();
        let id = q.enqueue("work", vec![7u8; 512]).await.unwrap();
        let payload_ref = q.get_job(&id).await.unwrap().unwrap().payload_ref.unwrap();
        store
            .delete(&Path::from(format!("test-payloads/{payload_ref}")))
            .await
            .unwrap();

        let reader = QueueReader::open(store, "test").await.unwrap();
        assert!(matches!(
            reader.get_job(&id).await,
            Err(Error::PayloadMissing { .. })
        ));
        // A reader cannot confirm a removal from its lagging view, so
        // the listing reports the missing payload.
        assert!(matches!(
            reader.list_jobs("work", JobStatus::Pending, None, 10).await,
            Err(Error::PayloadMissing { .. })
        ));

        reader.close().await.unwrap();
        q.close().await.unwrap();
    }
}
