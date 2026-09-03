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
use futures_util::{Stream, StreamExt};
use slatedb::config::DbReaderOptions;
use slatedb::manifest::SsTableId;
use slatedb::object_store::ObjectStore;
use slatedb::{DbReader, DbReaderMode};

use crate::error::{Error, Result};
use crate::history::JobAttempt;
use crate::job::{JobRecord, JobStatus};
use crate::kv::KvPage;
use crate::liveness::{StoreActivity, WriterHeartbeat};
use crate::payload_store::PayloadStore;
use crate::queue::JobPage;
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
#[non_exhaustive]
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
    /// Object store for the write-ahead log. Must match the writer's
    /// [`OpenOptions::wal_object_store`](crate::OpenOptions::wal_object_store);
    /// `None` (the default) uses the object store the reader is opened
    /// on. Without it a reader of a writer with a separate WAL store
    /// cannot see transitions not yet flushed to the primary store.
    pub wal_object_store: Option<Arc<dyn ObjectStore>>,
    /// When `true`, the reader reads no WAL at open or on refresh and
    /// observes only state flushed to the primary store. Lowers the
    /// cost of opening and refreshing a reader, for deployments with
    /// many readers whose queries tolerate the additional lag.
    /// Defaults to `false`.
    pub skip_wal_replay: bool,
}

impl ReaderOptions {
    /// Set [`Self::mode`].
    #[must_use]
    pub fn mode(mut self, mode: ReaderMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set [`Self::manifest_poll_interval`].
    #[must_use]
    pub fn manifest_poll_interval(mut self, manifest_poll_interval: Duration) -> Self {
        self.manifest_poll_interval = manifest_poll_interval;
        self
    }

    /// Set [`Self::checkpoint_lifetime`].
    #[must_use]
    pub fn checkpoint_lifetime(mut self, checkpoint_lifetime: Duration) -> Self {
        self.checkpoint_lifetime = checkpoint_lifetime;
        self
    }

    /// Set [`Self::payload_store`].
    #[must_use]
    pub fn payload_store(mut self, payload_store: impl Into<Option<Arc<dyn ObjectStore>>>) -> Self {
        self.payload_store = payload_store.into();
        self
    }

    /// Set [`Self::payload_path`].
    #[must_use]
    pub fn payload_path(mut self, payload_path: impl Into<Option<String>>) -> Self {
        self.payload_path = payload_path.into();
        self
    }

    /// Set [`Self::wal_object_store`].
    #[must_use]
    pub fn wal_object_store(
        mut self,
        wal_object_store: impl Into<Option<Arc<dyn ObjectStore>>>,
    ) -> Self {
        self.wal_object_store = wal_object_store.into();
        self
    }

    /// Set [`Self::skip_wal_replay`].
    #[must_use]
    pub fn skip_wal_replay(mut self, skip_wal_replay: bool) -> Self {
        self.skip_wal_replay = skip_wal_replay;
        self
    }
}

impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            mode: ReaderMode::default(),
            manifest_poll_interval: Duration::from_secs(10),
            checkpoint_lifetime: Duration::from_secs(10 * 60),
            payload_store: None,
            payload_path: None,
            wal_object_store: None,
            skip_wal_replay: false,
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
/// stored. Opening a reader against a path no writer has ever created
/// fails with [`Error::StoreNotInitialized`](crate::Error::StoreNotInitialized);
/// a health check racing the first deployment must expect that error.
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
            None,
        ));
        let mode = match opts.mode {
            ReaderMode::ManagedCheckpoint => DbReaderMode::ManagedCheckpoint,
            ReaderMode::FollowLatest => DbReaderMode::FollowLatest,
        };
        let mut builder = DbReader::builder(path, object_store.clone())
            .with_reader_mode(mode)
            // Stats counters and attempt histories are merge operands;
            // without the operator their reads fail.
            .with_merge_operator(Arc::new(QueueMergeOperator))
            .with_options(DbReaderOptions {
                manifest_poll_interval: opts.manifest_poll_interval,
                checkpoint_lifetime: opts.checkpoint_lifetime,
                skip_wal_replay: opts.skip_wal_replay,
                ..DbReaderOptions::default()
            });
        if let Some(wal_object_store) = opts.wal_object_store {
            builder = builder.with_wal_object_store(wal_object_store);
        }
        let reader = builder.build().await;
        let reader = match reader {
            Ok(reader) => reader,
            // A missing manifest and a corrupt one share an error kind;
            // an empty path identifies the never-written store.
            Err(e)
                if e.kind() == slatedb::ErrorKind::Data
                    && store_path_is_empty(&object_store, path).await =>
            {
                return Err(Error::StoreNotInitialized {
                    path: path.to_string(),
                });
            }
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            reader,
            payload_store,
        })
    }

    /// Return store-level activity read from this reader's view of the
    /// manifest and the durable sequence number.
    ///
    /// [`StoreActivity::last_flush_at_ms`] and
    /// [`StoreActivity::writer_epoch`] answer a display query with no
    /// waiting. A destructive operation judges liveness by reading
    /// [`StoreActivity::durable_seq`] more than once, a few
    /// [`ReaderOptions::manifest_poll_interval`]s apart, and treating
    /// advance as proof of live commits; that judgment involves no
    /// clock comparison.
    pub fn last_store_activity(&self) -> StoreActivity {
        let status = self.reader.status();
        let manifest = &status.current_manifest;
        // L0 SSTs are written by the writer's memtable flusher, newest
        // at the front; their ids embed the writer clock's timestamp.
        let last_flush_at_ms = manifest.l0().front().and_then(|view| match view.sst.id {
            SsTableId::Compacted(id) => Some(id.timestamp_ms()),
            SsTableId::Wal(_) => None,
        });
        StoreActivity {
            last_flush_at_ms,
            writer_epoch: manifest.writer_epoch(),
            durable_seq: status.durable_seq,
        }
    }

    /// Return the most recent liveness beat committed by a writer with
    /// [`OpenOptions::liveness_heartbeat`](crate::OpenOptions::liveness_heartbeat)
    /// enabled, or `None` when no writer has ever written one.
    ///
    /// See [`WriterHeartbeat`] for what a beat proves and how to judge
    /// its staleness.
    pub async fn writer_heartbeat(&self) -> Result<Option<WriterHeartbeat>> {
        crate::read::writer_heartbeat(&self.reader).await
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

    /// Every job of `queue` in `status` as one stream, read `page_size`
    /// jobs at a time; see [`Queue::jobs`](crate::Queue::jobs).
    pub fn jobs<'a>(
        &'a self,
        queue: &'a str,
        status: JobStatus,
        page_size: usize,
    ) -> impl Stream<Item = Result<JobRecord>> + 'a {
        crate::read::jobs(&self.reader, &self.payload_store, queue, status, page_size)
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

    /// Every entry of the user KV namespace under `prefix` as one
    /// stream, read `page_size` entries at a time; see
    /// [`Queue::kv_entries`](crate::Queue::kv_entries).
    pub fn kv_entries<'a>(
        &'a self,
        prefix: &'a [u8],
        page_size: usize,
    ) -> impl Stream<Item = Result<(Vec<u8>, Bytes)>> + 'a {
        crate::read::kv_entries(&self.reader, prefix, page_size)
    }

    /// Close the reader, stopping its manifest polling and releasing
    /// its managed checkpoint.
    pub async fn close(&self) -> Result<()> {
        self.reader.close().await?;
        Ok(())
    }
}

/// Whether the store path holds no objects at all. Consulted only after
/// an open failure, to separate a never-written store from one whose
/// manifest cannot be read.
async fn store_path_is_empty(store: &Arc<dyn ObjectStore>, path: &str) -> bool {
    let prefix = slatedb::object_store::path::Path::from(path);
    let mut listing = store.list(Some(&prefix));
    listing.next().await.is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::options::OpenOptions;
    use crate::queue::Queue;
    use slatedb::object_store::ObjectStoreExt;
    use slatedb::object_store::path::Path;

    use crate::test_util::make_store;

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
    async fn a_reader_reads_the_wal_from_the_writers_wal_store() {
        let store = make_store();
        let wal_store = make_store();
        let q = Queue::open_with_options(
            store.clone(),
            "test",
            OpenOptions::default().wal_object_store(wal_store.clone()),
        )
        .await
        .unwrap();
        // Durable but not yet flushed to the primary store: the record
        // is only readable through the WAL store.
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        let reader = QueueReader::open_with_options(
            store,
            "test",
            ReaderOptions::default().wal_object_store(wal_store),
        )
        .await
        .unwrap();
        let job = reader.get_job(&id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Pending);

        reader.close().await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_reader_skipping_wal_replay_observes_only_flushed_state() {
        let store = make_store();
        let q = Queue::open(store.clone(), "test").await.unwrap();
        // Durable in the WAL and unflushed to the primary store.
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        let skipping = ReaderOptions::default().skip_wal_replay(true);
        let reader = QueueReader::open_with_options(store.clone(), "test", skipping.clone())
            .await
            .unwrap();
        assert!(reader.get_job(&id).await.unwrap().is_none());
        reader.close().await.unwrap();

        // The close flushes the memtable, so a new reader observes the
        // job without reading the WAL.
        q.close().await.unwrap();
        let reader = QueueReader::open_with_options(store, "test", skipping)
            .await
            .unwrap();
        let job = reader.get_job(&id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Pending);
        reader.close().await.unwrap();
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
    async fn opening_a_reader_on_a_never_written_store_is_store_not_initialized() {
        let store = make_store();
        let err = match QueueReader::open(store, "test").await {
            Err(e) => e,
            Ok(_) => panic!("open succeeded on a never-written store"),
        };
        assert!(matches!(err, Error::StoreNotInitialized { ref path } if path == "test"));
        assert!(!err.is_permanent());
    }

    #[tokio::test]
    async fn an_unreadable_store_with_objects_is_a_storage_error() {
        let store = make_store();
        store
            .put(&Path::from("test/unrelated"), b"x".to_vec().into())
            .await
            .unwrap();
        let err = match QueueReader::open(store, "test").await {
            Err(e) => e,
            Ok(_) => panic!("open succeeded on an unreadable store"),
        };
        assert!(matches!(err, Error::Storage(_)));
    }

    #[tokio::test]
    async fn a_writer_heartbeat_is_visible_to_a_reader() {
        let store = make_store();
        let clock = crate::MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            liveness_heartbeat: Some(Duration::from_secs(3600)),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(store.clone(), "test", opts)
            .await
            .unwrap();

        let reader = QueueReader::open(store, "test").await.unwrap();
        let beat = reader.writer_heartbeat().await.unwrap().unwrap();
        assert_eq!(beat.counter, 1);
        assert_eq!(beat.at_ms, 1_700_000_000_000);
        assert_eq!(beat.interval, Duration::from_secs(3600));
        assert!(!beat.closed);

        reader.close().await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_reopened_writer_continues_the_heartbeat_counter() {
        let store = make_store();
        let opts = || OpenOptions {
            clock: Arc::new(crate::MockClock::new(1_700_000_000_000)),
            liveness_heartbeat: Some(Duration::from_secs(3600)),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(store.clone(), "test", opts())
            .await
            .unwrap();
        let reader = QueueReader::open(store.clone(), "test").await.unwrap();
        let first = reader.writer_heartbeat().await.unwrap().unwrap();
        reader.close().await.unwrap();
        q.close().await.unwrap();

        let q = Queue::open_with_options(store.clone(), "test", opts())
            .await
            .unwrap();
        let reader = QueueReader::open(store, "test").await.unwrap();
        let second = reader.writer_heartbeat().await.unwrap().unwrap();
        assert_eq!(first.counter, 1);
        // The first close's closing beat took counter 2.
        assert_eq!(second.counter, 3);
        assert!(!second.closed);
        assert!(second.writer_epoch > first.writer_epoch);

        reader.close().await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_clean_close_writes_a_closed_beat() {
        let store = make_store();
        let opts = OpenOptions {
            clock: Arc::new(crate::MockClock::new(1_700_000_000_000)),
            liveness_heartbeat: Some(Duration::from_secs(3600)),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(store.clone(), "test", opts)
            .await
            .unwrap();
        q.close().await.unwrap();

        let reader = QueueReader::open(store, "test").await.unwrap();
        let beat = reader.writer_heartbeat().await.unwrap().unwrap();
        assert!(beat.closed);
        assert_eq!(beat.counter, 2);

        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn no_heartbeat_is_reported_when_not_configured() {
        let store = make_store();
        let q = Queue::open(store.clone(), "test").await.unwrap();
        q.enqueue("work", b"x".to_vec()).await.unwrap();

        let reader = QueueReader::open(store, "test").await.unwrap();
        assert!(reader.writer_heartbeat().await.unwrap().is_none());

        reader.close().await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn store_activity_reports_the_epoch_and_advances_with_commits() {
        let store = make_store();
        let q = Queue::open(store.clone(), "test").await.unwrap();
        q.enqueue("work", b"a".to_vec()).await.unwrap();

        let reader_a = QueueReader::open(store.clone(), "test").await.unwrap();
        let before = reader_a.last_store_activity();
        assert!(before.writer_epoch >= 1);

        for _ in 0..3 {
            q.enqueue("work", b"b".to_vec()).await.unwrap();
        }
        let reader_b = QueueReader::open(store, "test").await.unwrap();
        let after = reader_b.last_store_activity();
        assert!(after.durable_seq > before.durable_seq);
        assert_eq!(after.writer_epoch, before.writer_epoch);

        reader_a.close().await.unwrap();
        reader_b.close().await.unwrap();
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
