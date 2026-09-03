//! Bodies of the read-only query API, shared between
//! [`Queue`](crate::Queue) and [`QueueReader`](crate::QueueReader).
//!
//! Every query both types expose lives here as a free function over
//! [`ReadHandle`], with the two types as thin delegating callers. A new
//! read method on either type is added here and delegated from both, so
//! the two surfaces cannot drift.

use std::future::Future;
use std::ops::Bound;

use bytes::Bytes;
use futures_util::stream::{self, Stream, TryStreamExt};
use slatedb::{Db, DbIterator, DbReader};

use crate::error::{Error, Result};
use crate::history::{JobAttempt, decode_history};
use crate::job::{JobRecord, JobStatus};
use crate::keys::{
    KeyTag, attempt_history_key, claimed_prefix, dead_key, dead_prefix, heartbeat_key,
    job_index_key, parse_stats_key, pending_prefix, stats_key, tag_prefix, user_scoped_key,
};
use crate::kv::KvPage;
use crate::liveness::{HeartbeatRecord, WriterHeartbeat};
use crate::payload_store::PayloadStore;
use crate::queue::{JobPage, validate_queue_name};
use crate::stats::{QueueStats, metric_name};

/// Uniform point-read and prefix-scan access to a store, implemented by
/// the writer's [`Db`] and the standalone [`DbReader`].
pub(crate) trait ReadHandle: Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    async fn scan_prefix(&self, prefix: Vec<u8>, start: Bound<Bytes>) -> Result<DbIterator>;

    /// Whether the record listed at `key` has been removed from the
    /// store. Consulted by [`list_jobs`] when a listed record's payload
    /// object is absent: the row of a record confirmed removed is
    /// omitted from the page; otherwise the missing payload is
    /// reported. A reader answers `false` unconditionally, because it
    /// re-reads the same lagging view its scan used and so cannot
    /// confirm a removal; the missing payload it reports resolves once
    /// its view advances past the removal.
    async fn job_removed_since_scan(&self, key: &[u8]) -> Result<bool>;
}

impl ReadHandle for Db {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(Db::get(self, key).await?)
    }

    async fn scan_prefix(&self, prefix: Vec<u8>, start: Bound<Bytes>) -> Result<DbIterator> {
        Ok(Db::scan_prefix(self, prefix, (start, Bound::Unbounded)).await?)
    }

    async fn job_removed_since_scan(&self, key: &[u8]) -> Result<bool> {
        Ok(Db::get(self, key).await?.is_none())
    }
}

impl ReadHandle for DbReader {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(DbReader::get(self, key).await?)
    }

    async fn scan_prefix(&self, prefix: Vec<u8>, start: Bound<Bytes>) -> Result<DbIterator> {
        Ok(DbReader::scan_prefix(self, prefix, (start, Bound::Unbounded)).await?)
    }

    async fn job_removed_since_scan(&self, _key: &[u8]) -> Result<bool> {
        Ok(false)
    }
}

/// Body of `stats`: assemble a [`QueueStats`] snapshot from the queue's
/// per-state counters.
pub(crate) async fn stats<H: ReadHandle>(handle: &H, queue: &str) -> Result<QueueStats> {
    validate_queue_name(queue)?;
    Ok(QueueStats {
        queue: queue.to_string(),
        pending: count_for(handle, queue, JobStatus::Pending).await?,
        claimed: count_for(handle, queue, JobStatus::Claimed).await?,
        done: count_for(handle, queue, JobStatus::Done).await?,
        dead: count_for(handle, queue, JobStatus::Dead).await?,
        scheduled: count_for(handle, queue, JobStatus::Scheduled).await?,
    })
}

async fn count_for<H: ReadHandle>(handle: &H, queue: &str, status: JobStatus) -> Result<i64> {
    let key = stats_key(queue, metric_name(status));
    match handle.get(&key).await? {
        None => Ok(0),
        Some(bytes) => bytes
            .as_ref()
            .try_into()
            .map(i64::from_le_bytes)
            .map_err(|_| Error::InvalidState),
    }
}

/// Body of `list_queues`: distinct queue names, discovered from the
/// stats key space. A queue appears once it has had at least one job.
pub(crate) async fn list_queues<H: ReadHandle>(handle: &H) -> Result<Vec<String>> {
    let mut seen = std::collections::HashSet::new();
    let mut queues = Vec::new();
    let mut iter = handle
        .scan_prefix(tag_prefix(KeyTag::Stats).to_vec(), Bound::Unbounded)
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

/// Body of `list_jobs`: one page of a queue's jobs in one lifecycle
/// state, in the scan order of that state's key space.
pub(crate) async fn list_jobs<H: ReadHandle>(
    handle: &H,
    payloads: &PayloadStore,
    queue: &str,
    status: JobStatus,
    cursor: Option<&[u8]>,
    limit: usize,
) -> Result<JobPage> {
    validate_queue_name(queue)?;
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
        JobStatus::Pending => (pending_prefix(queue), false),
        JobStatus::Dead => (dead_prefix(queue), false),
        JobStatus::Claimed => (claimed_prefix(queue), false),
        JobStatus::Scheduled => (tag_prefix(KeyTag::Scheduled).to_vec(), true),
        JobStatus::Done => (tag_prefix(KeyTag::Done).to_vec(), true),
    };
    let start = match cursor {
        None => Bound::Unbounded,
        // A cursor from a different key space does not identify a
        // position under this prefix; nothing follows it here.
        Some(c) if !c.starts_with(&prefix) => return Ok(empty),
        Some(c) => Bound::Excluded(Bytes::copy_from_slice(&c[prefix.len()..])),
    };

    // Each row includes the key it was scanned at, which is both the
    // key its record lives under and the position the cursor
    // resumes from.
    let mut page: Vec<(Bytes, JobRecord)> = Vec::with_capacity(limit);
    let mut more = false;
    let mut iter = handle.scan_prefix(prefix, start).await?;
    while let Some(kv) = iter.next().await? {
        let job = JobRecord::decode(&kv.key, &kv.value)?;
        if filter_queue && job.queue != queue {
            continue;
        }
        if page.len() == limit {
            more = true;
            break;
        }
        page.push((kv.key, job));
    }
    let next_cursor = more.then(|| page[page.len() - 1].0.to_vec());

    let mut jobs = Vec::with_capacity(page.len());
    for (record_key, mut job) in page {
        match payloads.materialize(&mut job).await {
            Ok(()) => jobs.push(job),
            Err(Error::PayloadMissing { id }) => {
                // The scan can list a record just before a
                // record-removing transaction commits, with the object
                // fetch running just after that commit's payload-object
                // deletion. Whether the removal is observable decides
                // the report; see [`ReadHandle::job_removed_since_scan`].
                if !handle.job_removed_since_scan(&record_key).await? {
                    return Err(Error::PayloadMissing { id });
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(JobPage { jobs, next_cursor })
}

/// Body of `dead_jobs`: one page of a queue's dead-letter jobs in ULID
/// order, which is the order they were originally enqueued in.
pub(crate) async fn dead_jobs<H: ReadHandle>(
    handle: &H,
    payloads: &PayloadStore,
    queue: &str,
    after: Option<&str>,
    limit: usize,
) -> Result<Vec<JobRecord>> {
    // Dead keys are the queue's dead prefix followed by the job id,
    // so an id cursor converts to the key cursor of the equivalent
    // `list_jobs` call.
    let cursor = after.map(|id| dead_key(queue, id));
    Ok(list_jobs(
        handle,
        payloads,
        queue,
        JobStatus::Dead,
        cursor.as_deref(),
        limit,
    )
    .await?
    .jobs)
}

/// Body of `attempt_history`: a job's recorded delivery history, in
/// write order. A job without a history key has an empty history.
pub(crate) async fn attempt_history<H: ReadHandle>(
    handle: &H,
    id: &str,
) -> Result<Vec<JobAttempt>> {
    match handle.get(&attempt_history_key(id)).await? {
        None => Ok(Vec::new()),
        Some(bytes) => decode_history(&bytes),
    }
}

/// Body of the reader's `get_job`: two plain point reads, the index and
/// then the record it names. The writer's `Queue::get_job` keeps its own
/// body, which reads both from one transaction snapshot and re-checks
/// the index when the payload object is absent.
pub(crate) async fn get_job<H: ReadHandle>(
    handle: &H,
    payloads: &PayloadStore,
    id: &str,
) -> Result<Option<JobRecord>> {
    let Some(current_key) = handle.get(&job_index_key(id)).await? else {
        return Ok(None);
    };
    let Some(bytes) = handle.get(&current_key).await? else {
        return Ok(None);
    };
    let mut job = JobRecord::decode(&current_key, &bytes)?;
    payloads.materialize(&mut job).await?;
    Ok(Some(job))
}

/// Body of `writer_heartbeat`: the stored liveness beat decoded to its
/// public form, or `None` when no writer has ever written one.
pub(crate) async fn writer_heartbeat<H: ReadHandle>(handle: &H) -> Result<Option<WriterHeartbeat>> {
    match handle.get(&heartbeat_key()).await? {
        Some(bytes) => {
            let record: HeartbeatRecord = rmp_serde::from_slice(&bytes)?;
            Ok(Some(record.into_public()))
        }
        None => Ok(None),
    }
}

/// Body of `kv_get`: one point read under the user key tag.
pub(crate) async fn kv_get<H: ReadHandle>(handle: &H, key: &[u8]) -> Result<Option<Bytes>> {
    handle.get(&user_scoped_key(key)).await
}

/// Body of `kv_scan`: one page of the user KV namespace under `prefix`,
/// in ascending byte order of the keys.
pub(crate) async fn kv_scan<H: ReadHandle>(
    handle: &H,
    prefix: &[u8],
    cursor: Option<&[u8]>,
    limit: usize,
) -> Result<KvPage> {
    let empty = KvPage {
        entries: Vec::new(),
        next_cursor: None,
    };
    if limit == 0 {
        return Ok(empty);
    }
    let scoped_prefix = user_scoped_key(prefix);
    let start = match cursor {
        None => Bound::Unbounded,
        // A cursor from a different prefix does not identify a
        // position under this one; nothing follows it here.
        Some(c) if !c.starts_with(&scoped_prefix) => return Ok(empty),
        Some(c) => Bound::Excluded(Bytes::copy_from_slice(&c[scoped_prefix.len()..])),
    };

    let mut page: Vec<(Bytes, Bytes)> = Vec::with_capacity(limit);
    let mut more = false;
    let mut iter = handle.scan_prefix(scoped_prefix, start).await?;
    while let Some(kv) = iter.next().await? {
        if page.len() == limit {
            more = true;
            break;
        }
        page.push((kv.key, kv.value));
    }
    let next_cursor = more.then(|| page[page.len() - 1].0.to_vec());
    // Stored keys carry the one-byte user tag; callers see their own
    // namespace, so it is stripped here. Cursors keep the stored form.
    let entries = page
        .into_iter()
        .map(|(k, v)| (k[1..].to_vec(), v))
        .collect();
    Ok(KvPage {
        entries,
        next_cursor,
    })
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

/// Body of `kv_entries`: every entry under `prefix`, in ascending key
/// order, read through [`kv_scan`] `page_size` entries at a time.
pub(crate) fn kv_entries<'a, H: ReadHandle>(
    handle: &'a H,
    prefix: &'a [u8],
    page_size: usize,
) -> impl Stream<Item = Result<(Vec<u8>, Bytes)>> + 'a {
    pages(move |cursor| async move {
        let page = kv_scan(handle, prefix, cursor.as_deref(), page_size).await?;
        Ok((page.entries, page.next_cursor))
    })
}

/// Body of `jobs`: every job of `queue` in `status`, in the order
/// [`list_jobs`] pages them, read `page_size` jobs at a time.
pub(crate) fn jobs<'a, H: ReadHandle>(
    handle: &'a H,
    payloads: &'a PayloadStore,
    queue: &'a str,
    status: JobStatus,
    page_size: usize,
) -> impl Stream<Item = Result<JobRecord>> + 'a {
    pages(move |cursor| async move {
        let page = list_jobs(
            handle,
            payloads,
            queue,
            status,
            cursor.as_deref(),
            page_size,
        )
        .await?;
        Ok((page.jobs, page.next_cursor))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::*;

    #[tokio::test]
    async fn kv_entries_cross_page_boundaries_in_key_order() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        for i in 0..5u8 {
            q.kv_put(&[b'p', b'/', b'0' + i], b"v").await.unwrap();
        }
        q.kv_put(b"q/0", b"v").await.unwrap();

        let keys: Vec<Vec<u8>> = q
            .kv_entries(b"p/", 2)
            .map_ok(|(key, _)| key)
            .try_collect()
            .await
            .unwrap();
        let expected: Vec<Vec<u8>> = (0..5u8).map(|i| vec![b'p', b'/', b'0' + i]).collect();
        assert_eq!(keys, expected);
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

        let mut queues = q.list_queues().await.unwrap();
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
        let p1 = q.dead_jobs("work", None, 2).await.unwrap();
        assert_eq!(p1.len(), 2);
        assert_eq!(p1[0].id, ids[0]);
        assert_eq!(p1[1].id, ids[1]);

        // Resume from the last cursor.
        let p2 = q.dead_jobs("work", Some(&p1[1].id), 2).await.unwrap();
        assert_eq!(p2.len(), 2);
        assert_eq!(p2[0].id, ids[2]);
        assert_eq!(p2[1].id, ids[3]);

        let p3 = q.dead_jobs("work", Some(&p2[1].id), 2).await.unwrap();
        assert_eq!(p3.len(), 1);
        assert_eq!(p3[0].id, ids[4]);

        // limit=0 returns nothing.
        assert!(q.dead_jobs("work", None, 0).await.unwrap().is_empty());

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
            .list_jobs("work", JobStatus::Claimed, None, 10)
            .await
            .unwrap();
        assert_eq!(ids(&page), vec![a.clone(), b.clone(), c.clone()]);

        // A renewal leaves the ordering alone.
        let renewed = q.renew_lease(&ca, Duration::from_secs(600)).unwrap();
        let page = q
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
            .list_jobs("kept", JobStatus::Done, None, 10)
            .await
            .unwrap();
        let ids: Vec<_> = page.jobs.iter().map(|j| j.id.clone()).collect();
        assert_eq!(ids, vec![kept]);
        let page = q
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
            .dead_jobs("work", None, 10)
            .await
            .unwrap()
            .into_iter()
            .map(|j| j.id)
            .collect();
        assert_eq!(via_dead_jobs.len(), 3);
        let page = q
            .list_jobs("work", JobStatus::Dead, None, 10)
            .await
            .unwrap();
        let via_list: Vec<_> = page.jobs.into_iter().map(|j| j.id).collect();
        assert_eq!(via_list, via_dead_jobs);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn list_jobs_materializes_offloaded_payloads() {
        let q = Queue::open_with_options(make_store(), "test", offload_opts())
            .await
            .unwrap();
        let payload = vec![9u8; 512];
        let id = q.enqueue("work", payload.clone()).await.unwrap();

        let page = q
            .list_jobs("work", JobStatus::Pending, None, 10)
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].id, id);
        assert!(page.jobs[0].payload_ref.is_some());
        assert_eq!(page.jobs[0].payload, payload);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn list_jobs_limit_zero_and_foreign_cursor_return_empty_pages() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        q.enqueue("work", b"x".to_vec()).await.unwrap();
        q.enqueue("work", b"y".to_vec()).await.unwrap();

        let zero = q
            .list_jobs("work", JobStatus::Pending, None, 0)
            .await
            .unwrap();
        assert!(zero.jobs.is_empty());
        assert!(zero.next_cursor.is_none());

        let first = q
            .list_jobs("work", JobStatus::Pending, None, 1)
            .await
            .unwrap();
        assert_eq!(first.jobs.len(), 1);
        let cursor = first.next_cursor.expect("a second pending entry exists");
        let dead = q
            .list_jobs("work", JobStatus::Dead, Some(&cursor), 10)
            .await
            .unwrap();
        assert!(dead.jobs.is_empty());
        assert!(dead.next_cursor.is_none());
        q.close().await.unwrap();
    }
}
