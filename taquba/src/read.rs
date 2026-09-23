//! The read-only query API: [`QueueView`], one method for each query, over
//! [`Handle`], the enum of the two store handles a view reads through (the
//! writer's [`Db`] or a [`DbReader`]).

use std::future::Future;
use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::stream::{self, Stream, TryStreamExt};
use slatedb::{Db, DbIterator, DbReader, IsolationLevel};

use crate::error::{Error, Result};
use crate::history::{JobAttempt, decode_history};
use crate::job::{JobRecord, JobStatus};
use crate::keys::{
    KeyTag, QueueName, attempt_history_key, claimed_prefix, dead_key, dead_prefix, heartbeat_key,
    job_index_key, parse_stats_key, pending_prefix, stats_key, tag_prefix, user_scoped_key,
};
use crate::kv::{KvPage, KvRange};
use crate::liveness::{HeartbeatRecord, WriterHeartbeat};
use crate::payload_store::PayloadStore;
use crate::queue::JobPage;
use crate::stats::{QueueStats, metric_name};
use crate::txn::get_indexed_job;

/// The store handle a view reads through: the writer's [`Db`] or a
/// [`DbReader`].
#[derive(Clone)]
pub(crate) enum Handle {
    Writer(Arc<Db>),
    Reader(Arc<DbReader>),
}

impl Handle {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(match self {
            Handle::Writer(db) => db.get(key).await?,
            Handle::Reader(reader) => reader.get(key).await?,
        })
    }

    async fn scan_prefix(
        &self,
        prefix: Vec<u8>,
        range: (Bound<Bytes>, Bound<Bytes>),
    ) -> Result<DbIterator> {
        Ok(match self {
            Handle::Writer(db) => db.scan_prefix(prefix, range).await?,
            Handle::Reader(reader) => reader.scan_prefix(prefix, range).await?,
        })
    }
}

/// The read-only queries of a store. [`Queue::view`](crate::Queue::view)
/// returns the writer's live view, and
/// [`QueueReader::view`](crate::QueueReader::view) returns a reader's
/// lagging view, with the same methods on both.
#[derive(Clone)]
pub struct QueueView {
    handle: Handle,
    payloads: Arc<PayloadStore>,
}

impl QueueView {
    pub(crate) fn new(handle: Handle, payloads: Arc<PayloadStore>) -> Self {
        Self { handle, payloads }
    }

    /// Return a snapshot of job counts for the given queue.
    pub async fn stats(&self, queue: &str) -> Result<QueueStats> {
        let queue = QueueName::new(queue)?;
        Ok(QueueStats {
            pending: self.count_for(&queue, JobStatus::Pending).await?,
            claimed: self.count_for(&queue, JobStatus::Claimed).await?,
            done: self.count_for(&queue, JobStatus::Done).await?,
            dead: self.count_for(&queue, JobStatus::Dead).await?,
            scheduled: self.count_for(&queue, JobStatus::Scheduled).await?,
            queue: queue.into_string(),
        })
    }

    async fn count_for(&self, queue: &QueueName, status: JobStatus) -> Result<i64> {
        let key = stats_key(queue, metric_name(status));
        match self.handle.get(&key).await? {
            None => Ok(0),
            Some(bytes) => bytes
                .as_ref()
                .try_into()
                .map(i64::from_le_bytes)
                .map_err(|_| Error::InvalidState),
        }
    }

    /// Return the names of all queues that have ever had at least one job.
    pub async fn list_queues(&self) -> Result<Vec<String>> {
        // The stats key space has a key per queue and metric.
        let mut seen = std::collections::HashSet::new();
        let mut queues = Vec::new();
        let mut iter = self
            .handle
            .scan_prefix(
                tag_prefix(KeyTag::Stats).to_vec(),
                (Bound::Unbounded, Bound::Unbounded),
            )
            .await?;
        while let Some(kv) = iter.next().await? {
            let Some((queue, _metric)) = parse_stats_key(&kv.key) else {
                continue;
            };
            if seen.insert(queue.clone()) {
                queues.push(queue);
            }
        }
        Ok(queues)
    }

    /// Return a page of the given queue's jobs in one lifecycle state.
    ///
    /// Jobs are returned in the scan order of the state's key space:
    ///
    /// - `Pending`: claim order (priority, then enqueue order).
    /// - `Scheduled`: `run_at` order, soonest first.
    /// - `Claimed`: enqueue order, as in [`QueueView::dead_jobs`].
    /// - `Done`: completion-time order, oldest first. Done records exist
    ///   only on queues with
    ///   [`QueueConfig::keep_done_jobs`](crate::QueueConfig::keep_done_jobs)
    ///   set.
    /// - `Dead`: enqueue order, as in [`QueueView::dead_jobs`].
    ///
    /// The claimed listing is not ordered by lease expiry: a renewal changes an
    /// expiry without changing a key, and an expiry-ordered page boundary moves
    /// under a listing.
    ///
    /// `cursor` is an opaque resume token: pass `None` to start from the
    /// beginning, or [`JobPage::next_cursor`] from the previous page to
    /// continue. A cursor identifies a scan position and remains valid when the
    /// job it was taken at leaves the state. The listing is not a snapshot: a
    /// job that changes state between page reads is absent from every page or
    /// appears on two pages.
    ///
    /// A listed record is in its stored form: an inline payload is included,
    /// and an offloaded payload is not, with [`JobRecord::payload_ref`] set.
    /// [`QueueView::get_job`] returns the record with its payload. The listing
    /// is exhausted when [`JobPage::next_cursor`] is `None`.
    ///
    /// The pending, claimed and dead key spaces group by queue, so those
    /// scans cover only the requested queue. The scheduled and done
    /// listings scan a key space that leads with a timestamp for the
    /// background sweeps, so they cover every queue and filter on the
    /// queue name.
    pub async fn list_jobs(
        &self,
        queue: &str,
        status: JobStatus,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<JobPage> {
        let queue = QueueName::new(queue)?;
        let empty = JobPage {
            jobs: Vec::new(),
            next_cursor: None,
        };
        if limit == 0 {
            return Ok(empty);
        }
        // `filter_queue` enables the queue-name filter on each scanned
        // record and is set only for the key spaces that cover every
        // queue.
        let (prefix, filter_queue) = match status {
            JobStatus::Pending => (pending_prefix(&queue), false),
            JobStatus::Dead => (dead_prefix(&queue), false),
            JobStatus::Claimed => (claimed_prefix(&queue), false),
            JobStatus::Scheduled => (tag_prefix(KeyTag::Scheduled).to_vec(), true),
            JobStatus::Done => (tag_prefix(KeyTag::Done).to_vec(), true),
        };
        let start = match cursor {
            None => Bound::Unbounded,
            // A cursor from a different key space does not identify a position
            // within this prefix, and nothing follows it here.
            Some(c) if !c.starts_with(&prefix) => return Ok(empty),
            Some(c) => Bound::Excluded(Bytes::copy_from_slice(&c[prefix.len()..])),
        };

        // The scanned key of the last row is the cursor of the next page.
        let mut jobs = Vec::with_capacity(limit);
        let mut last_key = None;
        let mut more = false;
        let mut iter = self
            .handle
            .scan_prefix(prefix, (start, Bound::Unbounded))
            .await?;
        while let Some(kv) = iter.next().await? {
            let job = JobRecord::decode(&kv.key, &kv.value)?;
            if filter_queue && job.queue != queue {
                continue;
            }
            if jobs.len() == limit {
                more = true;
                break;
            }
            jobs.push(job);
            last_key = Some(kv.key);
        }
        let next_cursor = more.then(|| last_key.expect("a full page has a last row").to_vec());
        Ok(JobPage { jobs, next_cursor })
    }

    /// Every job of `queue` in `status`, in the order [`QueueView::list_jobs`]
    /// pages them, as one stream that reads `page_size` jobs at a time. A
    /// consumer that stops reading does not fetch a further page. The listing
    /// semantics are those of `list_jobs`.
    pub fn jobs<'a>(
        &'a self,
        queue: &'a str,
        status: JobStatus,
        page_size: usize,
    ) -> impl Stream<Item = Result<JobRecord>> + 'a {
        pages(move |cursor| async move {
            let page = self
                .list_jobs(queue, status, cursor.as_deref(), page_size)
                .await?;
            Ok((page.jobs, page.next_cursor))
        })
    }

    /// Return a page of dead-letter jobs for the given queue.
    ///
    /// `after` is an exclusive cursor. Pass `None` to start from the beginning,
    /// or the `id` of the last job of the previous page to resume. `limit` caps
    /// the number of jobs returned.
    ///
    /// Jobs are returned in ULID order, which corresponds to the order in
    /// which they were originally enqueued, in the stored form of
    /// [`QueueView::list_jobs`].
    pub async fn dead_jobs(
        &self,
        queue: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<JobRecord>> {
        // Dead keys are the queue's dead prefix followed by the job id,
        // so an id cursor converts to the key cursor of the equivalent
        // `list_jobs` call.
        let queue = QueueName::new(queue)?;
        let cursor = after.map(|id| dead_key(&queue, id));
        Ok(self
            .list_jobs(&queue, JobStatus::Dead, cursor.as_deref(), limit)
            .await?
            .jobs)
    }

    /// Look up a job by ID regardless of its current state.
    ///
    /// Returns `None` for an ID that was never enqueued or whose records
    /// are removed. The writer's view reads the index and the record
    /// from one snapshot, and a reader's view reads them as two plain
    /// reads of its lagging view.
    pub async fn get_job(&self, id: &str) -> Result<Option<JobRecord>> {
        match &self.handle {
            Handle::Reader(reader) => {
                let Some(current_key) = reader.get(&job_index_key(id)).await? else {
                    return Ok(None);
                };
                let Some(bytes) = reader.get(&current_key).await? else {
                    return Ok(None);
                };
                let mut job = JobRecord::decode(&current_key, &bytes)?;
                self.payloads.materialize(&mut job).await?;
                Ok(Some(job))
            }
            Handle::Writer(db) => {
                let txn = db.begin(IsolationLevel::Snapshot).await?;
                let found = get_indexed_job(&txn, id).await?;
                txn.rollback();

                let Some((index_key, _, mut job)) = found else {
                    return Ok(None);
                };
                match self.payloads.materialize(&mut job).await {
                    Ok(()) => Ok(Some(job)),
                    Err(Error::PayloadMissing { id }) => {
                        // The record can be read just before a record-removing
                        // transaction commits, with the object fetch running
                        // just after that commit's payload-object deletion.
                        // Re-check the index so a job removed in that window
                        // is reported as absent.
                        if db.get(&index_key).await?.is_none() {
                            Ok(None)
                        } else {
                            Err(Error::PayloadMissing { id })
                        }
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Return a job's recorded delivery history, in write order.
    ///
    /// Each settlement of a claim appends one [`JobAttempt`]: an ack on a
    /// queue with
    /// [`QueueConfig::keep_done_jobs`](crate::QueueConfig::keep_done_jobs) set,
    /// a [`Queue::nack`](crate::Queue::nack), a
    /// [`Queue::dead_letter`](crate::Queue::dead_letter) and the reaper's
    /// handling of an expired lease.
    /// [`Queue::requeue_dead_job`](crate::Queue::requeue_dead_job) appends an
    /// [`AttemptOutcome::Requeued`](crate::AttemptOutcome::Requeued) marker and
    /// keeps the prior entries.
    ///
    /// The transaction that removes the job's last record also removes its
    /// history, so a job for which [`QueueView::get_job`] returns `None`
    /// has an empty history. An ack on a queue without retention removes
    /// the history and does not record the completed attempt. A later job
    /// enqueued with the same id through
    /// [`EnqueueOptions::id_override`](crate::EnqueueOptions::id_override)
    /// starts with an empty history.
    pub async fn attempt_history(&self, id: &str) -> Result<Vec<JobAttempt>> {
        match self.handle.get(&attempt_history_key(id)).await? {
            None => Ok(Vec::new()),
            Some(bytes) => decode_history(&bytes),
        }
    }

    /// The stored liveness beat in its public form, or `None` when no
    /// writer has ever written one.
    pub(crate) async fn writer_heartbeat(&self) -> Result<Option<WriterHeartbeat>> {
        match self.handle.get(&heartbeat_key()).await? {
            Some(bytes) => {
                let record: HeartbeatRecord = rmp_serde::from_slice(&bytes)?;
                Ok(Some(record.into_public()))
            }
            None => Ok(None),
        }
    }

    /// Read a value from the user KV namespace.
    ///
    /// Caller-supplied keys are internally scoped under a reserved
    /// user key tag and cannot collide with Taquba's internal layout.
    pub async fn kv_get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.handle.get(&user_scoped_key(key)).await
    }

    /// List entries of the user KV namespace under `prefix` within
    /// `range`, in ascending byte order of the keys.
    ///
    /// An empty `prefix` lists the whole namespace, and `..` lists every
    /// key within the prefix. The bounds of `range`, a [`KvRange`], are
    /// keys in the caller namespace: `key..` begins at `key`, and
    /// `(Bound::Excluded(key), Bound::Unbounded)` begins after it, which
    /// continues a listing from the last key of a page. The page contains
    /// the keys within the prefix that the range contains. A bound
    /// outside the prefix is correct as given, and a range without such
    /// a key returns an empty page. The listing is not a snapshot, so an
    /// entry written or deleted between page reads is missed or observed
    /// depending on the position of its key.
    ///
    /// Only caller-namespace entries are returned, and Taquba's internal
    /// key spaces are never visible here. This is the enumeration and
    /// export primitive for the namespace: a full sweep (`prefix = b""`,
    /// `..`, continued while [`KvPage::more`]) observes every entry that
    /// existed for the whole sweep.
    pub async fn kv_scan(
        &self,
        prefix: &[u8],
        range: impl KvRange,
        limit: usize,
    ) -> Result<KvPage> {
        let empty = KvPage {
            entries: Vec::new(),
            more: false,
        };
        let Some(range) = subrange(prefix, range) else {
            return Ok(empty);
        };
        if limit == 0 {
            return Ok(empty);
        }
        let mut entries = Vec::with_capacity(limit);
        let mut more = false;
        let mut iter = self
            .handle
            .scan_prefix(user_scoped_key(prefix), range)
            .await?;
        while let Some(kv) = iter.next().await? {
            if entries.len() == limit {
                more = true;
                break;
            }
            // A stored key includes the one-byte user tag, and a caller sees
            // the caller namespace, so the tag is stripped here.
            entries.push((kv.key[1..].to_vec(), kv.value));
        }
        Ok(KvPage { entries, more })
    }

    /// Every entry of the user KV namespace under `prefix` within
    /// `range`, in ascending byte order of the keys, as one stream that
    /// reads through [`QueueView::kv_scan`] `page_size` entries at a
    /// time. A consumer that stops reading does not fetch a further
    /// page. The listing semantics are those of `kv_scan`.
    pub fn kv_entries<'a>(
        &'a self,
        prefix: &'a [u8],
        range: impl KvRange,
        page_size: usize,
    ) -> impl Stream<Item = Result<(Vec<u8>, Bytes)>> + 'a {
        let start = range.start_bound().map(Vec::from);
        let end = range.end_bound().map(Vec::from);
        pages(move |cursor| {
            let (start, end) = (start.clone(), end.clone());
            async move {
                // A page follows the last key of the page before it.
                let start = match &cursor {
                    Some(last) => Bound::Excluded(last.as_slice()),
                    None => start.as_ref().map(Vec::as_slice),
                };
                let range = (start, end.as_ref().map(Vec::as_slice));
                let page = self.kv_scan(prefix, range, page_size).await?;
                let next = page
                    .more
                    .then(|| page.entries[page.entries.len() - 1].0.clone());
                Ok((page.entries, next))
            }
        })
    }
}

/// The position of a bound relative to the keys within a prefix.
enum Side {
    /// Before every key within the prefix.
    Before,
    /// A bound on the part of the key after the prefix.
    Within(Bound<Bytes>),
    /// After every key within the prefix.
    After,
}

fn side(prefix: &[u8], bound: Bound<&[u8]>) -> Side {
    let key = match bound {
        Bound::Unbounded => return Side::Within(Bound::Unbounded),
        Bound::Included(key) | Bound::Excluded(key) => key,
    };
    if key.starts_with(prefix) {
        Side::Within(bound.map(|key| Bytes::copy_from_slice(&key[prefix.len()..])))
    } else if key < prefix {
        Side::Before
    } else {
        Side::After
    }
}

/// The bounds of `range` on the part of the key after `prefix`, as the
/// scan takes them, or `None` for a range without a key within the
/// prefix. The store rejects an empty range, and the test of the bounds
/// here is the store's test.
fn subrange(prefix: &[u8], range: impl KvRange) -> Option<(Bound<Bytes>, Bound<Bytes>)> {
    let start = match side(prefix, range.start_bound()) {
        Side::Before => Bound::Unbounded,
        Side::Within(bound) => bound,
        Side::After => return None,
    };
    let end = match side(prefix, range.end_bound()) {
        Side::Before => return None,
        Side::Within(bound) => bound,
        Side::After => Bound::Unbounded,
    };
    let non_empty = match (&start, &end) {
        (Bound::Included(a), Bound::Included(b)) => a <= b,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => a < b,
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
    };
    non_empty.then_some((start, end))
}

/// The items of a paged read as one stream, fetched one page at a
/// time: `fetch` takes the cursor of the page to read, `None` for the
/// first, and returns the page's items with the cursor of the next
/// page, `None` once the read is exhausted. A consumer that stops
/// reading fetches no further page.
fn pages<T, F, Fut>(fetch: F) -> impl Stream<Item = Result<T>>
where
    F: FnMut(Option<Vec<u8>>) -> Fut,
    Fut: Future<Output = Result<(Vec<T>, Option<Vec<u8>>)>>,
{
    // The outer `Option` is `None` once the read is exhausted; the
    // inner one is the cursor `fetch` takes.
    stream::try_unfold((Some(None), fetch), |(cursor, mut fetch)| async move {
        let Some(cursor) = cursor else {
            return Ok::<_, Error>(None);
        };
        let (items, next) = fetch(cursor).await?;
        Ok(Some((
            stream::iter(items.into_iter().map(Ok::<T, Error>)),
            (next.map(Some), fetch),
        )))
    })
    .try_flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::ClaimOutcome;
    use crate::reader::QueueReader;
    use crate::test_util::*;

    #[tokio::test]
    async fn kv_entries_cross_page_boundaries_in_key_order() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        for i in 0..5u8 {
            q.kv_put(&[b'p', b'/', b'0' + i], b"v").await.unwrap();
        }
        q.kv_put(b"q/0", b"v").await.unwrap();

        let keys: Vec<Vec<u8>> = q
            .view()
            .kv_entries(b"p/", .., 2)
            .map_ok(|(key, _)| key)
            .try_collect()
            .await
            .unwrap();
        let expected: Vec<Vec<u8>> = (0..5u8).map(|i| vec![b'p', b'/', b'0' + i]).collect();
        assert_eq!(keys, expected);

        let from_third: Vec<Vec<u8>> = q
            .view()
            .kv_entries(b"p/", b"p/2".., 2)
            .map_ok(|(key, _)| key)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(from_third, expected[2..]);
    }

    #[tokio::test]
    async fn jobs_cross_page_boundaries_in_listing_order() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let mut ids = Vec::new();
        for i in 0..5u8 {
            ids.push(q.enqueue("alpha", vec![i]).await.unwrap());
        }
        q.enqueue("beta", b"x".to_vec()).await.unwrap();

        let listed: Vec<String> = q
            .view()
            .jobs("alpha", JobStatus::Pending, 2)
            .map_ok(|job| job.id)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed, ids);
    }

    #[tokio::test]
    async fn test_list_queues() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue("alpha", b"1".to_vec()).await.unwrap();
        q.enqueue("beta", b"2".to_vec()).await.unwrap();
        q.enqueue("gamma", b"3".to_vec()).await.unwrap();

        let mut queues = q.view().list_queues().await.unwrap();
        queues.sort();
        assert_eq!(queues, vec!["alpha", "beta", "gamma"]);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_dead_jobs_pagination() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        // Create 5 dead jobs.
        let mut ids = Vec::new();
        for _ in 0..5 {
            let id = q
                .enqueue_with(
                    "work",
                    b"x".to_vec(),
                    EnqueueOptions {
                        max_attempts: Some(1),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let job = q
                .claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .unwrap();
            q.nack(&job, "fail").await.unwrap();
            ids.push(id);
        }

        // First page of 2 returns the first two.
        let p1 = q.view().dead_jobs("work", None, 2).await.unwrap();
        assert_eq!(p1.len(), 2);
        assert_eq!(p1[0].id, ids[0]);
        assert_eq!(p1[1].id, ids[1]);

        // Resume from the last cursor.
        let p2 = q
            .view()
            .dead_jobs("work", Some(&p1[1].id), 2)
            .await
            .unwrap();
        assert_eq!(p2.len(), 2);
        assert_eq!(p2[0].id, ids[2]);
        assert_eq!(p2[1].id, ids[3]);

        let p3 = q
            .view()
            .dead_jobs("work", Some(&p2[1].id), 2)
            .await
            .unwrap();
        assert_eq!(p3.len(), 1);
        assert_eq!(p3[0].id, ids[4]);

        // limit=0 returns nothing.
        assert!(
            q.view()
                .dead_jobs("work", None, 0)
                .await
                .unwrap()
                .is_empty()
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn list_jobs_pages_pending_in_claim_order() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let low = q
            .enqueue_with(
                "work",
                b"low".to_vec(),
                EnqueueOptions {
                    priority: Some(PRIORITY_LOW),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let normal_a = q.enqueue("work", b"a".to_vec()).await.unwrap();
        let normal_b = q.enqueue("work", b"b".to_vec()).await.unwrap();
        let high = q
            .enqueue_with(
                "work",
                b"high".to_vec(),
                EnqueueOptions {
                    priority: Some(PRIORITY_HIGH),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let mut ids = Vec::new();
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let page = q
                .view()
                .list_jobs("work", JobStatus::Pending, cursor.as_deref(), 2)
                .await
                .unwrap();
            ids.extend(page.jobs.iter().map(|j| j.id.clone()));
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        assert_eq!(ids, vec![high, normal_a, normal_b, low]);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn list_jobs_orders_scheduled_by_run_at() {
        let initial = 1_700_000_000_000u64;
        let opts = OpenOptions {
            clock: Arc::new(MockClock::new(initial)),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let at = |secs: u64| std::time::UNIX_EPOCH + Duration::from_millis(initial + secs * 1_000);
        let later = q
            .enqueue_with(
                "work",
                b"later".to_vec(),
                EnqueueOptions {
                    run_at: Some(at(7200)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let sooner = q
            .enqueue_with(
                "work",
                b"sooner".to_vec(),
                EnqueueOptions {
                    run_at: Some(at(3600)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let page = q
            .view()
            .list_jobs("work", JobStatus::Scheduled, None, 10)
            .await
            .unwrap();
        let ids: Vec<_> = page.jobs.iter().map(|j| j.id.clone()).collect();
        assert_eq!(ids, vec![sooner, later]);
        assert!(page.next_cursor.is_none());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn list_jobs_orders_claimed_by_id_stably_under_renewal() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        // Claim order is the reverse of expiry order, so a listing
        // ordered by lease expiry would return these in reverse order.
        let mut handles = Vec::new();
        for secs in [90, 60, 30] {
            q.enqueue("work", vec![secs as u8]).await.unwrap();
            let claim = q
                .claim("work", Duration::from_secs(secs))
                .await
                .unwrap()
                .unwrap();
            handles.push(claim);
        }
        let [ca, cb, cc] = <[Claim; 3]>::try_from(handles).unwrap();
        let (a, b, c) = (ca.id.clone(), cb.id.clone(), cc.id.clone());

        let ids =
            |page: &JobPage| -> Vec<String> { page.jobs.iter().map(|j| j.id.clone()).collect() };
        let page = q
            .view()
            .list_jobs("work", JobStatus::Claimed, None, 10)
            .await
            .unwrap();
        assert_eq!(ids(&page), vec![a.clone(), b.clone(), c.clone()]);

        // A renewal leaves the ordering alone.
        let renewed = q.renew_lease(&ca, Duration::from_secs(600)).unwrap();
        let page = q
            .view()
            .list_jobs("work", JobStatus::Claimed, None, 10)
            .await
            .unwrap();
        assert_eq!(ids(&page), vec![a.clone(), b, c]);
        assert_eq!(q.lease_expiry("work", &a), Some(renewed));

        drop((cb, cc));
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn list_jobs_pages_claimed_one_queue_at_a_time() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let mut expected = Vec::new();
        // Alternate claims between the two queues, so a listing that
        // scanned a key space covering both would page the other
        // queue's rows in.
        for i in 0..3u8 {
            let id = q.enqueue("qa", vec![i]).await.unwrap();
            q.enqueue("qb", vec![i]).await.unwrap();
            expected.push(id);
            let lease = Duration::from_secs(30);
            q.claim("qa", lease).await.unwrap().unwrap();
            clock.advance(Duration::from_millis(1));
            q.claim("qb", lease).await.unwrap().unwrap();
            clock.advance(Duration::from_millis(1));
        }

        let mut ids = Vec::new();
        let mut cursor: Option<Vec<u8>> = None;
        let mut pages = 0;
        loop {
            let page = q
                .view()
                .list_jobs("qa", JobStatus::Claimed, cursor.as_deref(), 1)
                .await
                .unwrap();
            assert!(page.jobs.len() <= 1);
            assert!(page.jobs.iter().all(|j| j.status == JobStatus::Claimed));
            ids.extend(page.jobs.iter().map(|j| j.id.clone()));
            pages += 1;
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        assert_eq!(ids, expected);
        assert_eq!(pages, 3);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn list_jobs_done_lists_only_kept_records() {
        let mut opts = OpenOptions::default();
        opts.queue_configs.insert(
            "kept".to_string(),
            QueueConfig {
                keep_done_jobs: Some(Duration::from_secs(3600)),
                ..QueueConfig::default()
            },
        );
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let kept = q.enqueue("kept", b"k".to_vec()).await.unwrap();
        q.enqueue("gone", b"g".to_vec()).await.unwrap();
        let lease = Duration::from_secs(30);
        let job = q.claim("kept", lease).await.unwrap().unwrap();
        q.ack(&job).await.unwrap();
        let job = q.claim("gone", lease).await.unwrap().unwrap();
        q.ack(&job).await.unwrap();

        let page = q
            .view()
            .list_jobs("kept", JobStatus::Done, None, 10)
            .await
            .unwrap();
        let ids: Vec<_> = page.jobs.iter().map(|j| j.id.clone()).collect();
        assert_eq!(ids, vec![kept]);
        let page = q
            .view()
            .list_jobs("gone", JobStatus::Done, None, 10)
            .await
            .unwrap();
        assert!(page.jobs.is_empty());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn list_jobs_dead_matches_dead_jobs() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        for i in 0..3u8 {
            q.enqueue("work", vec![i]).await.unwrap();
        }
        let lease = Duration::from_secs(30);
        while let Some(job) = q.claim("work", lease).await.unwrap() {
            q.dead_letter(&job, "failed").await.unwrap();
        }

        let via_dead_jobs: Vec<_> = q
            .view()
            .dead_jobs("work", None, 10)
            .await
            .unwrap()
            .into_iter()
            .map(|j| j.id)
            .collect();
        assert_eq!(via_dead_jobs.len(), 3);
        let page = q
            .view()
            .list_jobs("work", JobStatus::Dead, None, 10)
            .await
            .unwrap();
        let via_list: Vec<_> = page.jobs.into_iter().map(|j| j.id).collect();
        assert_eq!(via_list, via_dead_jobs);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn list_jobs_returns_an_offloaded_record_without_its_payload() {
        let q = Queue::open_with_options(make_store(), "test", offload_opts())
            .await
            .unwrap();
        let payload = vec![9u8; 512];
        let id = q.enqueue("work", payload.clone()).await.unwrap();

        let page = q
            .view()
            .list_jobs("work", JobStatus::Pending, None, 10)
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, id);
        assert!(page.jobs[0].payload_ref.is_some());
        assert!(page.jobs[0].payload.is_empty());
        let job = q.view().get_job(&id).await.unwrap().unwrap();
        assert_eq!(job.payload, payload);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn list_jobs_limit_zero_and_foreign_cursor_return_empty_pages() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        q.enqueue("work", b"x".to_vec()).await.unwrap();
        q.enqueue("work", b"y".to_vec()).await.unwrap();

        let zero = q
            .view()
            .list_jobs("work", JobStatus::Pending, None, 0)
            .await
            .unwrap();
        assert!(zero.jobs.is_empty());
        assert!(zero.next_cursor.is_none());

        let first = q
            .view()
            .list_jobs("work", JobStatus::Pending, None, 1)
            .await
            .unwrap();
        assert_eq!(first.jobs.len(), 1);
        let cursor = first.next_cursor.expect("a second pending entry exists");
        let dead = q
            .view()
            .list_jobs("work", JobStatus::Dead, Some(&cursor), 10)
            .await
            .unwrap();
        assert!(dead.jobs.is_empty());
        assert!(dead.next_cursor.is_none());
        q.close().await.unwrap();
    }

    // One function over a view, called with the writer's view and with
    // a reader's view of the same store.
    async fn snapshot(
        r: &QueueView,
        id: &str,
    ) -> (QueueStats, Vec<String>, Vec<String>, Vec<String>) {
        let stats = r.stats("work").await.unwrap();
        let queues = r.list_queues().await.unwrap();
        let page = r
            .list_jobs("work", JobStatus::Pending, None, 10)
            .await
            .unwrap();
        let streamed: Vec<JobRecord> = r
            .jobs("work", JobStatus::Pending, 1)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            page.jobs.iter().map(|j| &j.id).collect::<Vec<_>>(),
            streamed.iter().map(|j| &j.id).collect::<Vec<_>>()
        );
        let dead: Vec<String> = r
            .dead_jobs("work", None, 10)
            .await
            .unwrap()
            .into_iter()
            .map(|j| j.id)
            .collect();
        let job = r.get_job(id).await.unwrap().unwrap();
        assert_eq!(job.id, id);
        let history = r.attempt_history(&dead[0]).await.unwrap();
        assert_eq!(history.len(), 1);
        let value = r.kv_get(b"k/1").await.unwrap().unwrap();
        assert_eq!(value.as_ref(), b"v");
        let kv_page = r.kv_scan(b"k/", .., 10).await.unwrap();
        let entries: Vec<(Vec<u8>, Bytes)> =
            r.kv_entries(b"k/", .., 1).try_collect().await.unwrap();
        assert_eq!(kv_page.entries, entries);
        assert_eq!(entries.len(), 2);
        (
            stats,
            queues,
            page.jobs.into_iter().map(|j| j.id).collect(),
            dead,
        )
    }

    #[tokio::test]
    async fn a_view_reads_the_same_state_through_the_writer_and_a_reader() {
        let store = make_store();
        let q = Queue::open(store.clone(), "test").await.unwrap();
        let id = q.enqueue("work", b"p".to_vec()).await.unwrap();
        q.enqueue("work", b"q".to_vec()).await.unwrap();
        let doomed = q.enqueue("work", b"d".to_vec()).await.unwrap();
        let ClaimOutcome::Claimed(job) = q
            .claim_by_id(&doomed, Duration::from_secs(30))
            .await
            .unwrap()
        else {
            panic!("the job is pending");
        };
        q.dead_letter(&job, "failed").await.unwrap();
        q.kv_put(b"k/1", b"v").await.unwrap();
        q.kv_put(b"k/2", b"w").await.unwrap();

        let reader = QueueReader::open(store, "test").await.unwrap();
        let through_writer = snapshot(q.view(), &id).await;
        let through_reader = snapshot(reader.view(), &id).await;
        assert_eq!(through_writer, through_reader);
        assert_eq!(through_writer.0.pending, 2);
        assert_eq!(through_writer.3, vec![doomed]);

        reader.close().await.unwrap();
        q.close().await.unwrap();
    }
}
