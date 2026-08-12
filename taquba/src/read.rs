//! Bodies of the read-only query API, shared between
//! [`Queue`](crate::Queue) and [`QueueReader`](crate::QueueReader).
//!
//! Every query both types expose lives here as a free function over
//! [`ReadHandle`], with the two types as thin delegating callers. A new
//! read method on either type is added here and delegated from both, so
//! the two surfaces cannot drift.

use std::ops::Bound;

use bytes::Bytes;
use slatedb::{Db, DbIterator, DbReader};

use crate::error::{Error, Result};
use crate::history::{JobAttempt, decode_history};
use crate::job::{JobRecord, JobStatus};
use crate::keys::{
    KeyTag, attempt_history_key, claimed_prefix, dead_key, dead_prefix, job_index_key,
    parse_stats_key, pending_prefix, stats_key, tag_prefix, user_scoped_key,
};
use crate::payload_store::PayloadStore;
use crate::queue::{JobPage, KvPage, validate_queue_name};
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

/// Fetch an offloaded payload into `job.payload`. No-op for records
/// whose payload is inline.
pub(crate) async fn materialize_payload(
    payloads: &PayloadStore,
    job: &mut JobRecord,
) -> Result<()> {
    if let Some(ref payload_ref) = job.payload_ref {
        job.payload = payloads.get(payload_ref, &job.id).await?;
    }
    Ok(())
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
        let job: JobRecord = rmp_serde::from_slice(&kv.value)?;
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
        match materialize_payload(payloads, &mut job).await {
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
    let mut job: JobRecord = rmp_serde::from_slice(&bytes)?;
    materialize_payload(payloads, &mut job).await?;
    Ok(Some(job))
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
