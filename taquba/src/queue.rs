use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use slatedb::object_store::ObjectStore;
use slatedb::{Db, IsolationLevel};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, instrument, warn};
use ulid::Ulid;

use crate::error::{Error, Result};
use crate::job::{JobRecord, JobStatus};
use crate::reaper::{reap_expired, sweep_dead, sweep_done};
use crate::scheduler::{promote_due_jobs, schedule_loop};
use crate::stats::{CounterMergeOperator, QueueStats, read_stats, update_stats};

const DEFAULT_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(30);

/// High-priority bucket. Jobs at this priority are dequeued before normal and low.
pub const PRIORITY_HIGH: u32 = 100;
/// Default priority. FIFO ordering is preserved within the same priority level.
pub const PRIORITY_NORMAL: u32 = 1_000;
/// Low-priority bucket. Jobs at this priority are dequeued after high and normal.
pub const PRIORITY_LOW: u32 = 10_000;

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before epoch")
        .as_millis() as u64
}

pub(crate) fn pending_key(queue: &str, priority: u32, id: &str) -> String {
    format!("pending:{}:{:010}:{}", queue, priority, id)
}

pub(crate) fn pending_prefix(queue: &str) -> String {
    format!("pending:{}:", queue)
}

pub(crate) fn dead_key(queue: &str, id: &str) -> String {
    format!("dead:{}:{}", queue, id)
}

pub(crate) fn claimed_key(queue: &str, lease_expires_at: u64, id: &str) -> String {
    // Timestamp comes before queue so the prefix scan in the reaper is sorted
    // globally by expiry, allowing a single early-exit instead of a per-queue
    // walk.
    format!("claimed:{:020}:{}:{}", lease_expires_at, queue, id)
}

pub(crate) fn done_key(queue: &str, id: &str) -> String {
    format!("done:{}:{}", queue, id)
}

pub(crate) fn scheduled_key(queue: &str, run_at: u64, id: &str) -> String {
    // Same layout reasoning as claimed_key.
    format!("scheduled:{:020}:{}:{}", run_at, queue, id)
}

pub(crate) fn job_index_key(id: &str) -> String {
    format!("jobindex:{}", id)
}

pub(crate) fn dedup_index_key(queue: &str, key: &str) -> String {
    format!("dedup:{}:{}", queue, key)
}

/// Compute the retry delay for the next attempt after a nack.
///
/// Exponential backoff: `min(base * 2^(attempts - 1), max)`. If `base` is zero,
/// returns zero (re-queue immediately).
pub(crate) fn backoff_delay(attempts: u32, base: Duration, max: Duration) -> Duration {
    if base.is_zero() {
        return Duration::ZERO;
    }
    let mult = 2u32.saturating_pow(attempts.saturating_sub(1));
    base.saturating_mul(mult).min(max)
}

/// Configuration applied to a specific queue (or used as the default for all queues).
///
/// Construct via [`QueueConfig::default`] and override as required:
///
/// ```ignore
/// QueueConfig {
///     max_attempts: 10,
///     ..QueueConfig::default()
/// }
/// ```
#[derive(Debug, Clone)]
pub struct QueueConfig {
    /// Maximum delivery attempts before a job is dead-lettered.
    pub max_attempts: u32,
    /// How long a claimed job's lease lasts. Used by [`Queue::claim_next`].
    pub lease_duration: Duration,
    /// Default priority assigned to jobs enqueued without an explicit priority.
    /// Lower numbers are dequeued first. Use the [`PRIORITY_HIGH`], [`PRIORITY_NORMAL`],
    /// and [`PRIORITY_LOW`] constants, or any `u32` value.
    pub default_priority: u32,
    /// Base delay for exponential retry backoff after a [`Queue::nack`].
    /// The delay for attempt `N` is `min(retry_backoff_base * 2^(N - 1), retry_backoff_max)`.
    /// Set to [`Duration::ZERO`] to disable backoff and re-queue immediately.
    pub retry_backoff_base: Duration,
    /// Upper bound on the retry backoff delay. Ignored when `retry_backoff_base`
    /// is zero.
    pub retry_backoff_max: Duration,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            lease_duration: DEFAULT_LEASE_DURATION,
            default_priority: PRIORITY_NORMAL,
            retry_backoff_base: Duration::from_secs(1),
            retry_backoff_max: Duration::from_secs(300),
        }
    }
}

/// Configuration for opening a [`Queue`] instance.
pub struct OpenOptions {
    /// How often the background reaper scans for expired leases. Defaults to 5s.
    /// The same loop also performs done- and dead-job retention sweeps.
    pub reaper_interval: Duration,
    /// How often the background scheduler promotes due jobs to pending. Defaults to 1s.
    pub scheduler_interval: Duration,
    /// Default configuration applied to any queue not listed in
    /// [`Self::queue_configs`].
    pub default_queue_config: QueueConfig,
    /// Per-queue overrides. Keys are queue names.
    pub queue_configs: HashMap<String, QueueConfig>,
    /// If `Some(duration)`, completed jobs are written to the `done:` keyspace
    /// and retained for `duration`. The reaper purges them once
    /// `enqueued_at + duration` has passed.
    ///
    /// If `None` (default), [`Queue::ack`] deletes the job outright.
    ///
    /// The success counter in [`QueueStats::done`] is incremented either way.
    pub keep_done_jobs: Option<Duration>,
    /// Maximum age of a dead-letter job before the retention sweep purges it.
    /// Default is 7 days, which gives operators time to inspect or requeue
    /// without leaking storage. `None` disables the sweep entirely: dead
    /// jobs accumulate without bound.
    pub dead_retention: Option<Duration>,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            reaper_interval: Duration::from_secs(5),
            scheduler_interval: Duration::from_secs(1),
            default_queue_config: QueueConfig::default(),
            queue_configs: HashMap::new(),
            keep_done_jobs: None,
            dead_retention: Some(Duration::from_secs(7 * 24 * 3600)),
        }
    }
}

/// Per-call overrides for [`Queue::enqueue_with`].
///
/// Every field is `Option`; leave a field as `None` (the default) to inherit
/// the queue's configured value. Construct via [`EnqueueOptions::default`] +
/// struct-update syntax so adding new fields in future versions is non-breaking:
///
/// ```
/// use std::time::{Duration, SystemTime};
/// use taquba::EnqueueOptions;
///
/// let opts = EnqueueOptions {
///     run_at: Some(SystemTime::now() + Duration::from_secs(60)),
///     ..EnqueueOptions::default()
/// };
/// ```
#[derive(Debug, Clone, Default)]
pub struct EnqueueOptions {
    /// Override the queue's default `max_attempts` for just this job.
    pub max_attempts: Option<u32>,
    /// Override the queue's `default_priority`. Use [`PRIORITY_HIGH`],
    /// [`PRIORITY_NORMAL`], [`PRIORITY_LOW`], or any `u32`; lower wins.
    pub priority: Option<u32>,
    /// Earliest time at which the job may be claimed. If the value is in the
    /// past or `None`, the job is written straight to pending; otherwise it
    /// waits in the scheduled key space until promoted by the background
    /// scheduler.
    pub run_at: Option<std::time::SystemTime>,
    /// Block creation if a pending or scheduled job with the same key already
    /// exists; in that case the existing job's ID is returned. The key is
    /// released when the job is claimed, so re-enqueueing after processing
    /// begins is allowed.
    pub dedup_key: Option<String>,
    /// Arbitrary string-keyed metadata to attach to the job. Stored alongside
    /// the payload and surfaced as [`JobRecord::headers`]. Useful for fields
    /// that should stay separable from the opaque payload, e.g. webhook
    /// delivery metadata (URL, HTTP headers, signing key id) or cron-style
    /// metadata (schedule name, nominal fire time). Defaults to empty.
    pub headers: HashMap<String, String>,
}

/// A durable task queue backed by object storage.
///
/// `Queue` persists all job state to an object store via SlateDB.
///
/// # Lifecycle
///
/// Open with [`Queue::open`] or [`Queue::open_with_options`], use the queue, then call
/// [`Queue::close`] to flush state and shut down background tasks cleanly.
///
/// # Background tasks
///
/// Two background tasks run while the queue is open:
///
/// - **Reaper**: scans for jobs whose lease has expired and re-queues them so they
///   are retried by another worker. Interval is configurable via [`OpenOptions::reaper_interval`].
/// - **Scheduler**: promotes jobs whose `run_at` time has passed from the scheduled
///   state to pending. Interval is configurable via [`OpenOptions::scheduler_interval`].
///
/// # Concurrency
///
/// `Queue` is `Send + Sync` and cheap to clone behind an [`Arc`]. All workers must run
/// in the same process: SlateDB's single-writer constraint means the queue cannot be
/// shared across processes.
pub struct Queue {
    db: Arc<Db>,
    reaper_shutdown: watch::Sender<bool>,
    reaper_handle: JoinHandle<()>,
    scheduler_shutdown: watch::Sender<bool>,
    scheduler_handle: JoinHandle<()>,
    default_queue_config: QueueConfig,
    queue_configs: HashMap<String, QueueConfig>,
    keep_done_jobs: Option<Duration>,
    /// In-process wakeup signal so workers blocked on an empty queue can resume
    /// the moment a job becomes claimable, without waiting out their poll
    /// interval.
    job_available: Arc<tokio::sync::Notify>,
}

impl Queue {
    /// Open a queue with default settings.
    pub async fn open(object_store: Arc<dyn ObjectStore>, path: &str) -> Result<Self> {
        Self::open_with_options(object_store, path, OpenOptions::default()).await
    }

    /// Open a queue with explicit options.
    pub async fn open_with_options(
        object_store: Arc<dyn ObjectStore>,
        path: &str,
        opts: OpenOptions,
    ) -> Result<Self> {
        let db = Arc::new(
            Db::builder(path, object_store)
                .with_merge_operator(Arc::new(CounterMergeOperator))
                .build()
                .await?,
        );
        let job_available = Arc::new(tokio::sync::Notify::new());
        let (reaper_shutdown, reaper_rx) = watch::channel(false);
        let reaper_handle = tokio::spawn(crate::reaper::reap_loop(
            db.clone(),
            opts.reaper_interval,
            opts.keep_done_jobs,
            opts.dead_retention,
            job_available.clone(),
            reaper_rx,
        ));
        let (scheduler_shutdown, scheduler_rx) = watch::channel(false);
        let scheduler_handle = tokio::spawn(schedule_loop(
            db.clone(),
            opts.scheduler_interval,
            job_available.clone(),
            scheduler_rx,
        ));
        Ok(Self {
            db,
            reaper_shutdown,
            reaper_handle,
            scheduler_shutdown,
            scheduler_handle,
            default_queue_config: opts.default_queue_config,
            queue_configs: opts.queue_configs,
            keep_done_jobs: opts.keep_done_jobs,
            job_available,
        })
    }

    fn queue_config(&self, queue: &str) -> &QueueConfig {
        self.queue_configs
            .get(queue)
            .unwrap_or(&self.default_queue_config)
    }

    /// Look up the configured lease duration for a queue.
    pub fn queue_lease_duration(&self, queue: &str) -> Duration {
        self.queue_config(queue).lease_duration
    }

    /// Enqueue a job using the queue's configured defaults for everything
    /// (max_attempts, priority, no schedule, no dedup). Equivalent to
    /// [`Self::enqueue_with`] with [`EnqueueOptions::default`].
    pub async fn enqueue(&self, queue: &str, payload: Vec<u8>) -> Result<String> {
        self.enqueue_with(queue, payload, EnqueueOptions::default())
            .await
    }

    /// Enqueue a job with one or more options overridden.
    ///
    /// Any field of [`EnqueueOptions`] left as `None` falls back to the queue's
    /// configured default.
    ///
    /// ```no_run
    /// # use std::time::{Duration, SystemTime};
    /// # async fn ex(q: &taquba::Queue) -> taquba::Result<()> {
    /// use taquba::{EnqueueOptions, PRIORITY_HIGH};
    ///
    /// q.enqueue_with("email", b"to=alice".to_vec(), EnqueueOptions {
    ///     priority: Some(PRIORITY_HIGH),
    ///     run_at: Some(SystemTime::now() + Duration::from_secs(300)),
    ///     dedup_key: Some("welcome:user-42".to_string()),
    ///     ..EnqueueOptions::default()
    /// }).await?;
    /// # Ok(()) }
    /// ```
    ///
    /// When `dedup_key` is `Some` and a pending job with the same key already
    /// exists, this returns the existing job's ID without creating a new one.
    /// When `run_at` is in the past or is now, the job is written straight to
    /// pending; otherwise it waits in the scheduled key space until the
    /// background scheduler promotes it.
    #[instrument(skip(self, payload), fields(queue, job_id))]
    pub async fn enqueue_with(
        &self,
        queue: &str,
        payload: Vec<u8>,
        opts: EnqueueOptions,
    ) -> Result<String> {
        let cfg = self.queue_config(queue);
        let max_attempts = opts.max_attempts.unwrap_or(cfg.max_attempts);
        let priority = opts.priority.unwrap_or(cfg.default_priority);

        // A `run_at` that is at-or-before now is just an immediate enqueue.
        let run_at = opts.run_at.and_then(|when| {
            let ms = when
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            (ms > now_ms()).then_some(ms)
        });

        let id = Ulid::new().to_string();
        let (status, key) = match run_at {
            Some(ms) => (JobStatus::Scheduled, scheduled_key(queue, ms, &id)),
            None => (JobStatus::Pending, pending_key(queue, priority, &id)),
        };
        let job = JobRecord {
            id,
            queue: queue.to_string(),
            payload,
            headers: opts.headers,
            status,
            attempts: 0,
            max_attempts,
            enqueued_at: now_ms(),
            claimed_at: None,
            lease_expires_at: None,
            run_at,
            priority,
            last_error: None,
            dedup_key: opts.dedup_key.clone(),
            completed_at: None,
            failed_at: None,
        };

        match opts.dedup_key {
            Some(dk) => self.write_unique(job, key, dk).await,
            None => self.write_new(job, key).await,
        }
    }

    /// Persist and announce a brand-new job. Used by the non-dedup path of
    /// [`Self::enqueue_with`].
    async fn write_new(&self, job: JobRecord, key: String) -> Result<String> {
        let value = rmp_serde::to_vec_named(&job)?;
        let JobRecord {
            id,
            queue,
            status,
            priority,
            run_at,
            ..
        } = job;

        let txn = self.db.begin(IsolationLevel::Snapshot).await?;
        txn.put(key.as_bytes(), &value)?;
        txn.put(job_index_key(&id).as_bytes(), key.as_bytes())?;
        update_stats(&txn, &queue, &[(status, 1)])?;
        txn.commit().await?;

        // Workers can claim a Pending job immediately; a Scheduled job becomes
        // claimable later via the scheduler loop, which fires its own notify.
        if matches!(status, JobStatus::Pending) {
            self.job_available.notify_waiters();
        }

        debug!(queue = %queue, job_id = %id, priority, ?run_at, "job enqueued");
        Ok(id)
    }

    /// Dedup-aware variant: writes a pending or scheduled job behind a
    /// `dedup:` index entry, or returns the existing ID if the index already
    /// points somewhere. Retries on transaction conflict.
    async fn write_unique(&self, job: JobRecord, key: String, dedup_key: String) -> Result<String> {
        let dkey = dedup_index_key(&job.queue, &dedup_key);
        let value = rmp_serde::to_vec_named(&job)?;
        let JobRecord {
            id, queue, status, ..
        } = job;

        loop {
            let txn = self.db.begin(IsolationLevel::Snapshot).await?;

            if let Some(bytes) = txn.get(dkey.as_bytes()).await? {
                // A pending or scheduled job with this key already exists.
                txn.rollback();
                return String::from_utf8(bytes.to_vec()).map_err(|_| Error::InvalidState);
            }

            txn.put(key.as_bytes(), &value)?;
            txn.put(job_index_key(&id).as_bytes(), key.as_bytes())?;
            txn.put(dkey.as_bytes(), id.as_bytes())?;
            update_stats(&txn, &queue, &[(status, 1)])?;

            match txn.commit().await {
                Ok(_) => {
                    if matches!(status, JobStatus::Pending) {
                        self.job_available.notify_waiters();
                    }
                    debug!(queue = %queue, job_id = %id, dedup_key, "unique job enqueued");
                    return Ok(id);
                }
                Err(e) if e.kind() == slatedb::ErrorKind::Transaction => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Claim the next pending job using the configured default lease duration.
    pub async fn claim_next(&self, queue: &str) -> Result<Option<JobRecord>> {
        let lease_duration = self.queue_config(queue).lease_duration;
        self.claim(queue, lease_duration).await
    }

    /// Block up to `max_wait` for a job to become claimable on any queue.
    ///
    /// Returns when either an in-process enqueue / promotion / requeue fires
    /// the wakeup notification, or the timeout elapses. The wakeup is
    /// queue-agnostic: callers must follow up with a [`Self::claim`] call to
    /// see if anything is actually available on their queue.
    pub async fn wait_for_jobs(&self, max_wait: Duration) {
        let notified = self.job_available.notified();
        tokio::pin!(notified);
        tokio::select! {
            _ = &mut notified => {}
            _ = tokio::time::sleep(max_wait) => {}
        }
    }

    /// Claim the next pending job, waiting up to `max_wait` for one to appear.
    ///
    /// Workers should prefer this over a polling [`Self::claim_next`] +
    /// [`tokio::time::sleep`] loop: when an enqueue or scheduled-job promotion
    /// happens in the same process, the wakeup is delivered via an in-memory
    /// notify so the worker resumes immediately, without waiting out the poll
    /// interval. Only when nothing is available does the call fall back to
    /// the timeout, returning `None`.
    ///
    /// The `lease_duration` controls how long the resulting claim is held.
    pub async fn claim_with_wait(
        &self,
        queue: &str,
        lease_duration: Duration,
        max_wait: Duration,
    ) -> Result<Option<JobRecord>> {
        // Subscribe to the wakeup *before* the first claim attempt so we don't
        // miss a notification published between the empty-scan and the wait.
        let notified = self.job_available.notified();
        tokio::pin!(notified);

        if let Some(job) = self.claim(queue, lease_duration).await? {
            return Ok(Some(job));
        }
        tokio::select! {
            _ = &mut notified => {}
            _ = tokio::time::sleep(max_wait) => return Ok(None),
        }
        // Wakeup might have been for a different queue, or another worker may
        // have stolen the job; return whatever a fresh claim sees.
        self.claim(queue, lease_duration).await
    }

    /// Claim the next pending job with an explicit lease duration.
    /// Returns `None` if the queue is empty.
    #[instrument(skip(self), fields(queue))]
    pub async fn claim(&self, queue: &str, lease_duration: Duration) -> Result<Option<JobRecord>> {
        let prefix = pending_prefix(queue);
        loop {
            let txn = self.db.begin(IsolationLevel::Snapshot).await?;

            let mut iter = txn.scan_prefix(prefix.as_bytes()).await?;
            let kv = match iter.next().await? {
                Some(kv) => kv,
                None => return Ok(None),
            };
            drop(iter);

            let pending_key_bytes = kv.key.clone();
            let mut job: JobRecord = rmp_serde::from_slice(&kv.value)?;

            let now = now_ms();
            let lease_expires_at = now + lease_duration.as_millis() as u64;
            job.status = JobStatus::Claimed;
            job.claimed_at = Some(now);
            job.lease_expires_at = Some(lease_expires_at);
            job.attempts += 1;

            // Take the dedup_key off the record BEFORE serializing the
            // claimed-state copy. If we left it on, a later nack would put a
            // record back into pending still carrying the key, and the next
            // claim would try to delete a `dedup:` index that may by now
            // belong to a *different* job, corrupting the dedup invariant.
            let dedup_key_to_release = job.dedup_key.take();
            let claimed = claimed_key(&job.queue, lease_expires_at, &job.id);
            let value = rmp_serde::to_vec_named(&job)?;

            txn.delete(&pending_key_bytes)?;
            txn.put(claimed.as_bytes(), &value)?;
            txn.put(job_index_key(&job.id).as_bytes(), claimed.as_bytes())?;
            if let Some(dk) = dedup_key_to_release.as_deref() {
                txn.delete(dedup_index_key(&job.queue, dk).as_bytes())?;
            }
            update_stats(
                &txn,
                queue,
                &[(JobStatus::Pending, -1), (JobStatus::Claimed, 1)],
            )?;

            match txn.commit().await {
                Ok(_) => {
                    debug!(queue = queue, job_id = %job.id, attempt = job.attempts, "job claimed");
                    return Ok(Some(job));
                }
                Err(e) if e.kind() == slatedb::ErrorKind::Transaction => {
                    warn!(queue = queue, "claim transaction conflict, retrying");
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Acknowledge successful completion.
    ///
    /// By default the job is deleted outright; the success counter in
    /// [`QueueStats::done`] is still incremented.
    ///
    /// Set [`OpenOptions::keep_done_jobs`] to retain completed jobs for a
    /// bounded duration.
    #[instrument(skip(self, job), fields(queue = %job.queue, job_id = %job.id))]
    pub async fn ack(&self, job: &JobRecord) -> Result<()> {
        let lease_expires_at = job.lease_expires_at.ok_or(Error::InvalidState)?;
        let claimed = claimed_key(&job.queue, lease_expires_at, &job.id);

        let txn = self.db.begin(IsolationLevel::Snapshot).await?;
        txn.delete(claimed.as_bytes())?;

        if self.keep_done_jobs.is_some() {
            let mut done_job = job.clone();
            done_job.status = JobStatus::Done;
            done_job.completed_at = Some(now_ms());
            let value = rmp_serde::to_vec_named(&done_job)?;
            let done = done_key(&job.queue, &job.id);
            txn.put(done.as_bytes(), &value)?;
            txn.put(job_index_key(&job.id).as_bytes(), done.as_bytes())?;
        } else {
            // Default: drop the index pointer too; the ID is no longer
            // findable via get_job, but the queue stays small.
            txn.delete(job_index_key(&job.id).as_bytes())?;
        }
        update_stats(
            &txn,
            &job.queue,
            &[(JobStatus::Claimed, -1), (JobStatus::Done, 1)],
        )?;
        txn.commit().await?;

        debug!(queue = %job.queue, job_id = %job.id, "job acked");
        Ok(())
    }

    /// Report failure. Re-queues if attempts < max_attempts, otherwise dead-letters.
    ///
    /// Re-queued jobs honour the queue's `retry_backoff_base` and `retry_backoff_max`:
    /// when the backoff is non-zero, the job is parked in the scheduled key space and
    /// the background scheduler promotes it once the delay has elapsed. With zero
    /// backoff the job goes straight back to pending.
    #[instrument(skip(self, job), fields(queue = %job.queue, job_id = %job.id))]
    pub async fn nack(&self, mut job: JobRecord, error: &str) -> Result<()> {
        let lease_expires_at = job.lease_expires_at.ok_or(Error::InvalidState)?;
        let claimed = claimed_key(&job.queue, lease_expires_at, &job.id);
        job.last_error = Some(error.to_string());

        let txn = self.db.begin(IsolationLevel::Snapshot).await?;
        txn.delete(claimed.as_bytes())?;

        if job.attempts >= job.max_attempts {
            job.status = JobStatus::Dead;
            job.failed_at = Some(now_ms());
            let dead = dead_key(&job.queue, &job.id);
            let value = rmp_serde::to_vec_named(&job)?;
            txn.put(dead.as_bytes(), &value)?;
            txn.put(job_index_key(&job.id).as_bytes(), dead.as_bytes())?;
            update_stats(
                &txn,
                &job.queue,
                &[(JobStatus::Claimed, -1), (JobStatus::Dead, 1)],
            )?;
            warn!(
                queue = %job.queue,
                job_id = %job.id,
                attempts = job.attempts,
                "job dead-lettered"
            );
        } else {
            let cfg = self.queue_config(&job.queue);
            let backoff =
                backoff_delay(job.attempts, cfg.retry_backoff_base, cfg.retry_backoff_max);
            job.claimed_at = None;
            job.lease_expires_at = None;

            if backoff.is_zero() {
                job.status = JobStatus::Pending;
                let priority = job.priority;
                let pending = pending_key(&job.queue, priority, &job.id);
                let value = rmp_serde::to_vec_named(&job)?;
                txn.put(pending.as_bytes(), &value)?;
                txn.put(job_index_key(&job.id).as_bytes(), pending.as_bytes())?;
                update_stats(
                    &txn,
                    &job.queue,
                    &[(JobStatus::Pending, 1), (JobStatus::Claimed, -1)],
                )?;
                debug!(
                    queue = %job.queue,
                    job_id = %job.id,
                    attempts = job.attempts,
                    "job re-queued"
                );
            } else {
                let run_at = now_ms() + backoff.as_millis() as u64;
                job.status = JobStatus::Scheduled;
                job.run_at = Some(run_at);
                let scheduled = scheduled_key(&job.queue, run_at, &job.id);
                let value = rmp_serde::to_vec_named(&job)?;
                txn.put(scheduled.as_bytes(), &value)?;
                txn.put(job_index_key(&job.id).as_bytes(), scheduled.as_bytes())?;
                update_stats(
                    &txn,
                    &job.queue,
                    &[(JobStatus::Claimed, -1), (JobStatus::Scheduled, 1)],
                )?;
                debug!(
                    queue = %job.queue,
                    job_id = %job.id,
                    attempts = job.attempts,
                    backoff_ms = backoff.as_millis() as u64,
                    "job scheduled for retry"
                );
            }
        }

        let immediate_retry = matches!(job.status, JobStatus::Pending);
        txn.commit().await?;
        if immediate_retry {
            // Backoff path doesn't need a wake: the scheduler loop will fire
            // notify_waiters() when it promotes the job.
            self.job_available.notify_waiters();
        }
        Ok(())
    }

    /// Dead-letter a claimed job immediately, regardless of its `attempts`.
    /// Use this when the failure is *known* to be permanent and retrying
    /// would be wasted work.
    ///
    /// Unlike [`Self::nack`], this does not increment `attempts` or schedule
    /// a backoff: the job goes straight to the dead-letter set.
    /// [`worker::run_worker`](crate::worker::run_worker) and
    /// [`worker::run_worker_concurrent`](crate::worker::run_worker_concurrent)
    /// call this automatically when a worker returns
    /// [`worker::PermanentFailure`](crate::worker::PermanentFailure).
    #[instrument(skip(self, job), fields(queue = %job.queue, job_id = %job.id))]
    pub async fn dead_letter(&self, mut job: JobRecord, reason: &str) -> Result<()> {
        let lease_expires_at = job.lease_expires_at.ok_or(Error::InvalidState)?;
        let claimed = claimed_key(&job.queue, lease_expires_at, &job.id);
        job.last_error = Some(reason.to_string());
        job.status = JobStatus::Dead;
        job.failed_at = Some(now_ms());
        job.claimed_at = None;
        job.lease_expires_at = None;

        let txn = self.db.begin(IsolationLevel::Snapshot).await?;
        txn.delete(claimed.as_bytes())?;
        let dead = dead_key(&job.queue, &job.id);
        let value = rmp_serde::to_vec_named(&job)?;
        txn.put(dead.as_bytes(), &value)?;
        txn.put(job_index_key(&job.id).as_bytes(), dead.as_bytes())?;
        update_stats(
            &txn,
            &job.queue,
            &[(JobStatus::Claimed, -1), (JobStatus::Dead, 1)],
        )?;
        txn.commit().await?;

        warn!(
            queue = %job.queue,
            job_id = %job.id,
            attempts = job.attempts,
            "job dead-lettered (permanent failure)"
        );
        Ok(())
    }

    /// Return a snapshot of job counts for the given queue.
    pub async fn stats(&self, queue: &str) -> Result<QueueStats> {
        read_stats(&self.db, queue).await
    }

    /// Return the names of all queues that have ever had at least one job.
    pub async fn list_queues(&self) -> Result<Vec<String>> {
        let mut seen = std::collections::HashSet::new();
        let mut queues = Vec::new();
        let mut iter = self.db.scan_prefix(b"stats:").await?;
        while let Some(kv) = iter.next().await? {
            let key_str = match std::str::from_utf8(&kv.key) {
                Ok(s) => s,
                Err(_) => continue,
            };
            // Key: "stats:{queue}:{metric}".
            let without_prefix = key_str.strip_prefix("stats:").unwrap_or(key_str);
            if let Some(idx) = without_prefix.rfind(':') {
                let queue = &without_prefix[..idx];
                if seen.insert(queue.to_string()) {
                    queues.push(queue.to_string());
                }
            }
        }
        Ok(queues)
    }

    /// Return a page of dead-letter jobs for the given queue.
    ///
    /// `after` is an exclusive cursor; pass `None` to start from the
    /// beginning or the `id` of the last job from the previous page to
    /// resume. `limit` caps the number of jobs returned.
    ///
    /// Jobs are returned in ULID order, which corresponds to the order in
    /// which they were originally enqueued.
    pub async fn dead_jobs(
        &self,
        queue: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<JobRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let prefix = format!("dead:{}:", queue);
        let mut jobs = Vec::with_capacity(limit);
        let mut iter = self.db.scan_prefix(prefix.as_bytes()).await?;
        while let Some(kv) = iter.next().await? {
            if let Some(after_id) = after {
                // Skip until we pass the cursor.
                let key_str = std::str::from_utf8(&kv.key).unwrap_or("");
                let id = key_str.rsplit(':').next().unwrap_or("");
                if id <= after_id {
                    continue;
                }
            }
            let job: JobRecord = rmp_serde::from_slice(&kv.value)?;
            jobs.push(job);
            if jobs.len() >= limit {
                break;
            }
        }
        Ok(jobs)
    }

    /// Move a dead-letter job back to the pending queue for a fresh attempt.
    ///
    /// Resets `attempts` to 0 and clears `last_error` so the job gets a full
    /// retry budget.
    #[instrument(skip(self, job), fields(queue = %job.queue, job_id = %job.id))]
    pub async fn requeue_dead_job(&self, mut job: JobRecord) -> Result<()> {
        if job.status != JobStatus::Dead {
            return Err(Error::InvalidState);
        }
        let dead = dead_key(&job.queue, &job.id);
        let priority = job.priority;
        job.status = JobStatus::Pending;
        job.attempts = 0;
        job.last_error = None;
        job.claimed_at = None;
        job.lease_expires_at = None;
        job.failed_at = None;
        let pending = pending_key(&job.queue, priority, &job.id);
        let value = rmp_serde::to_vec_named(&job)?;

        let txn = self.db.begin(IsolationLevel::Snapshot).await?;
        txn.delete(dead.as_bytes())?;
        txn.put(pending.as_bytes(), &value)?;
        txn.put(job_index_key(&job.id).as_bytes(), pending.as_bytes())?;
        update_stats(
            &txn,
            &job.queue,
            &[(JobStatus::Pending, 1), (JobStatus::Dead, -1)],
        )?;
        txn.commit().await?;
        self.job_available.notify_waiters();

        debug!(queue = %job.queue, job_id = %job.id, "dead job re-queued");
        Ok(())
    }

    /// Extend the lease on a claimed job. Updates `job.lease_expires_at` in place.
    ///
    /// Call this periodically for long-running jobs to prevent the reaper from
    /// treating them as abandoned and re-queuing them.
    #[instrument(skip(self, job), fields(queue = %job.queue, job_id = %job.id))]
    pub async fn renew_lease(&self, job: &mut JobRecord, extension: Duration) -> Result<()> {
        let old_expiry = job.lease_expires_at.ok_or(Error::InvalidState)?;
        let old_claimed = claimed_key(&job.queue, old_expiry, &job.id);

        let new_expiry = now_ms() + extension.as_millis() as u64;
        job.lease_expires_at = Some(new_expiry);
        let new_claimed = claimed_key(&job.queue, new_expiry, &job.id);
        let value = rmp_serde::to_vec_named(job)?;

        let txn = self.db.begin(IsolationLevel::Snapshot).await?;
        txn.delete(old_claimed.as_bytes())?;
        txn.put(new_claimed.as_bytes(), &value)?;
        txn.put(job_index_key(&job.id).as_bytes(), new_claimed.as_bytes())?;
        txn.commit().await?;

        debug!(queue = %job.queue, job_id = %job.id, new_expiry, "lease renewed");
        Ok(())
    }

    /// Look up a job by ID regardless of its current state.
    ///
    /// Returns `None` if the ID was never enqueued or has since been expunged.
    pub async fn get_job(&self, id: &str) -> Result<Option<JobRecord>> {
        let index_key = job_index_key(id);
        let current_key = match self.db.get(index_key.as_bytes()).await? {
            None => return Ok(None),
            Some(bytes) => match String::from_utf8(bytes.to_vec()) {
                Ok(s) => s,
                Err(_) => return Err(Error::InvalidState),
            },
        };
        match self.db.get(current_key.as_bytes()).await? {
            None => Ok(None),
            Some(bytes) => Ok(Some(rmp_serde::from_slice(&bytes)?)),
        }
    }

    /// Cancel a pending or scheduled job by ID.
    ///
    /// Returns `true` if the job was found and cancelled, `false` if it was not
    /// found or is in a non-cancellable state (claimed, done, or dead).
    pub async fn cancel(&self, id: &str) -> Result<bool> {
        loop {
            let txn = self.db.begin(IsolationLevel::Snapshot).await?;

            let index_key = job_index_key(id);
            let current_key = match txn.get(index_key.as_bytes()).await? {
                None => {
                    txn.rollback();
                    return Ok(false);
                }
                Some(bytes) => match String::from_utf8(bytes.to_vec()) {
                    Ok(s) => s,
                    Err(_) => {
                        txn.rollback();
                        return Err(Error::InvalidState);
                    }
                },
            };

            let job: JobRecord = match txn.get(current_key.as_bytes()).await? {
                None => {
                    txn.rollback();
                    return Ok(false);
                }
                Some(bytes) => rmp_serde::from_slice(&bytes)?,
            };

            let is_scheduled = matches!(job.status, JobStatus::Scheduled);
            let is_pending = matches!(job.status, JobStatus::Pending);
            if !is_pending && !is_scheduled {
                txn.rollback();
                return Ok(false);
            }

            txn.delete(current_key.as_bytes())?;
            txn.delete(index_key.as_bytes())?;
            if let Some(ref dk) = job.dedup_key {
                txn.delete(dedup_index_key(&job.queue, dk).as_bytes())?;
            }
            if is_scheduled {
                update_stats(&txn, &job.queue, &[(JobStatus::Scheduled, -1)])?;
            } else {
                update_stats(&txn, &job.queue, &[(JobStatus::Pending, -1)])?;
            }

            match txn.commit().await {
                Ok(_) => {
                    debug!(job_id = %id, "job cancelled");
                    return Ok(true);
                }
                Err(e) if e.kind() == slatedb::ErrorKind::Transaction => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Enqueue multiple jobs atomically in a single transaction.
    ///
    /// All jobs use the queue's configured `max_attempts` and `default_priority`.
    /// Returns the IDs in the same order as `payloads`.
    pub async fn enqueue_batch(&self, queue: &str, payloads: Vec<Vec<u8>>) -> Result<Vec<String>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let cfg = self.queue_config(queue);
        let max_attempts = cfg.max_attempts;
        let priority = cfg.default_priority;
        let now = now_ms();

        let mut ids = Vec::with_capacity(payloads.len());
        let txn = self.db.begin(IsolationLevel::Snapshot).await?;

        // Use a monotonic generator so IDs in a single batch sort in insertion
        // order even when produced inside the same millisecond; `Ulid::new()`
        // alone is not monotonic and would break batch FIFO assertions.
        let mut id_gen = ulid::Generator::new();

        for payload in payloads {
            let id = id_gen
                .generate()
                .expect("monotonic ULID generator overflowed within one ms")
                .to_string();
            let job = JobRecord {
                id: id.clone(),
                queue: queue.to_string(),
                payload,
                headers: HashMap::new(),
                status: JobStatus::Pending,
                attempts: 0,
                max_attempts,
                enqueued_at: now,
                claimed_at: None,
                lease_expires_at: None,
                run_at: None,
                priority,
                last_error: None,
                dedup_key: None,
                completed_at: None,
                failed_at: None,
            };
            let key = pending_key(queue, priority, &id);
            let value = rmp_serde::to_vec_named(&job)?;
            txn.put(key.as_bytes(), &value)?;
            txn.put(job_index_key(&id).as_bytes(), key.as_bytes())?;
            ids.push(id);
        }

        update_stats(&txn, queue, &[(JobStatus::Pending, ids.len() as i64)])?;
        txn.commit().await?;
        self.job_available.notify_waiters();

        debug!(queue = queue, count = ids.len(), "batch enqueued");
        Ok(ids)
    }

    /// Trigger an immediate reap sweep (primarily useful in tests and tooling).
    pub async fn reap_now(&self) -> Result<()> {
        let count = reap_expired(&self.db).await?;
        if count > 0 {
            self.job_available.notify_waiters();
        }
        Ok(())
    }

    /// Trigger an immediate scheduled-job promotion sweep (primarily useful in tests).
    pub async fn promote_scheduled_now(&self) -> Result<()> {
        let count = promote_due_jobs(&self.db).await?;
        if count > 0 {
            self.job_available.notify_waiters();
        }
        Ok(())
    }

    /// Trigger an immediate done-job retention sweep (primarily useful in tests
    /// and tooling). Deletes any `done:` entries whose retention window has
    /// expired. The `retention` argument overrides the value configured on the
    /// instance so callers can run a one-off purge.
    pub async fn sweep_done_now(&self, retention: Duration) -> Result<()> {
        sweep_done(&self.db, retention).await
    }

    /// Trigger an immediate dead-job retention sweep.
    pub async fn sweep_dead_now(&self, retention: Duration) -> Result<()> {
        sweep_dead(&self.db, retention).await
    }

    /// Shut down the background reaper and scheduler, then close the underlying database.
    pub async fn close(self) -> Result<()> {
        let _ = self.reaper_shutdown.send(true);
        let _ = self.reaper_handle.await;
        let _ = self.scheduler_shutdown.send(true);
        let _ = self.scheduler_handle.await;
        self.db.close().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    fn make_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    /// OpenOptions that disable retry backoff so nack tests can re-claim
    /// immediately. Production defaults are exponential, so the "claim
    /// straight after nack" assertion needs an explicit opt-out.
    fn no_backoff_opts() -> OpenOptions {
        OpenOptions {
            default_queue_config: QueueConfig {
                retry_backoff_base: Duration::ZERO,
                retry_backoff_max: Duration::ZERO,
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        }
    }

    #[tokio::test]
    async fn test_enqueue_and_claim() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let id = q.enqueue("email", b"hello".to_vec()).await.unwrap();
        let job = q
            .claim("email", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(job.id, id);
        assert_eq!(job.queue, "email");
        assert_eq!(job.payload, b"hello");
        assert_eq!(job.status, JobStatus::Claimed);
        assert_eq!(job.attempts, 1);
        assert!(job.claimed_at.is_some());
        assert!(job.lease_expires_at.is_some());

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_claim_empty_queue_returns_none() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        assert!(
            q.claim("email", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_ack_moves_job_to_done() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue("email", b"hello".to_vec()).await.unwrap();
        let job = q
            .claim("email", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();

        assert!(
            q.claim("email", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_nack_requeues_job() {
        let q = Queue::open_with_options(make_store(), "test", no_backoff_opts())
            .await
            .unwrap();

        q.enqueue_with(
            "email",
            b"hello".to_vec(),
            EnqueueOptions {
                max_attempts: Some(3),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let job = q
            .claim("email", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.attempts, 1);

        q.nack(job, "transient error").await.unwrap();

        let retried = q
            .claim("email", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retried.attempts, 2);
        assert_eq!(retried.last_error.as_deref(), Some("transient error"));
        assert_eq!(retried.status, JobStatus::Claimed);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_nack_dead_letters_after_max_attempts() {
        let q = Queue::open_with_options(make_store(), "test", no_backoff_opts())
            .await
            .unwrap();

        q.enqueue_with(
            "email",
            b"hello".to_vec(),
            EnqueueOptions {
                max_attempts: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        for _ in 0..2 {
            let job = q
                .claim("email", Duration::from_secs(30))
                .await
                .unwrap()
                .unwrap();
            q.nack(job, "persistent error").await.unwrap();
        }
        assert!(
            q.claim("email", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_fifo_ordering() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let id_a = q.enqueue("work", b"first".to_vec()).await.unwrap();
        let id_b = q.enqueue("work", b"second".to_vec()).await.unwrap();
        let id_c = q.enqueue("work", b"third".to_vec()).await.unwrap();

        let j1 = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let j2 = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let j3 = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(j1.id, id_a);
        assert_eq!(j2.id, id_b);
        assert_eq!(j3.id, id_c);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_queue_isolation() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let id_email = q.enqueue("email", b"email job".to_vec()).await.unwrap();
        let id_resize = q.enqueue("resize", b"resize job".to_vec()).await.unwrap();

        let email_job = q
            .claim("email", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let resize_job = q
            .claim("resize", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(email_job.id, id_email);
        assert_eq!(resize_job.id, id_resize);
        assert!(
            q.claim("email", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            q.claim("resize", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_reaper_requeues_expired_job() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue_with(
            "work",
            b"payload".to_vec(),
            EnqueueOptions {
                max_attempts: Some(3),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let job = q
            .claim("work", Duration::from_millis(0))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.attempts, 1);

        assert!(
            q.claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        q.reap_now().await.unwrap();

        let reclaimed = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reclaimed.id, job.id);
        assert_eq!(reclaimed.attempts, 2);
        assert_eq!(reclaimed.status, JobStatus::Claimed);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_reaper_dead_letters_after_max_attempts() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue_with(
            "work",
            b"payload".to_vec(),
            EnqueueOptions {
                max_attempts: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let _job = q
            .claim("work", Duration::from_millis(0))
            .await
            .unwrap()
            .unwrap();
        q.reap_now().await.unwrap();

        let _job = q
            .claim("work", Duration::from_millis(0))
            .await
            .unwrap()
            .unwrap();
        q.reap_now().await.unwrap();

        assert!(
            q.claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_reaper_skips_active_leases() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(300))
            .await
            .unwrap()
            .unwrap();

        q.reap_now().await.unwrap();

        assert!(
            q.claim("work", Duration::from_secs(300))
                .await
                .unwrap()
                .is_none()
        );

        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_reaper_ignores_already_acked_job() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_millis(0))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();

        q.reap_now().await.unwrap();

        assert!(
            q.claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_stats_track_job_lifecycle() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue("email", b"a".to_vec()).await.unwrap();
        q.enqueue("email", b"b".to_vec()).await.unwrap();

        let s = q.stats("email").await.unwrap();
        assert_eq!(s.pending, 2);
        assert_eq!(s.claimed, 0);

        let job = q
            .claim("email", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let s = q.stats("email").await.unwrap();
        assert_eq!(s.pending, 1);
        assert_eq!(s.claimed, 1);

        q.ack(&job).await.unwrap();
        let s = q.stats("email").await.unwrap();
        assert_eq!(s.pending, 1);
        assert_eq!(s.claimed, 0);
        assert_eq!(s.done, 1);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_stats_nack_dead_letter() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue_with(
            "email",
            b"x".to_vec(),
            EnqueueOptions {
                max_attempts: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let job = q
            .claim("email", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.nack(job, "fail").await.unwrap();

        let s = q.stats("email").await.unwrap();
        assert_eq!(s.pending, 0);
        assert_eq!(s.claimed, 0);
        assert_eq!(s.dead, 1);

        q.close().await.unwrap();
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
    async fn test_dead_jobs_and_requeue() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let id = q
            .enqueue_with(
                "work",
                b"payload".to_vec(),
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
        q.nack(job, "fatal").await.unwrap();

        let dead = q.dead_jobs("work", None, 100).await.unwrap();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].id, id);
        assert_eq!(dead[0].status, JobStatus::Dead);

        // Requeue and verify it's workable again
        q.requeue_dead_job(dead.into_iter().next().unwrap())
            .await
            .unwrap();

        let revived = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(revived.id, id);
        assert_eq!(revived.attempts, 1); // fresh attempt after reset
        assert!(revived.last_error.is_none());

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_per_queue_config() {
        let mut opts = OpenOptions::default();
        opts.queue_configs.insert(
            "fast".to_string(),
            QueueConfig {
                max_attempts: 1,
                lease_duration: Duration::from_secs(5),
                ..QueueConfig::default()
            },
        );
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        // "fast" queue inherits max_attempts=1
        q.enqueue("fast", b"x".to_vec()).await.unwrap();
        let job = q.claim_next("fast").await.unwrap().unwrap();
        assert_eq!(job.max_attempts, 1);
        // Lease is 5s
        let lease_expires_at = job.lease_expires_at.unwrap();
        let claimed_at = job.claimed_at.unwrap();
        assert!(lease_expires_at - claimed_at <= 5_001); // within 5s + 1ms tolerance

        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_priority_ordering() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        // Enqueue in reverse priority order to prove ordering is by priority, not insertion.
        let id_low = q
            .enqueue_with(
                "jobs",
                b"low".to_vec(),
                EnqueueOptions {
                    priority: Some(PRIORITY_LOW),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let id_normal = q
            .enqueue_with(
                "jobs",
                b"normal".to_vec(),
                EnqueueOptions {
                    priority: Some(PRIORITY_NORMAL),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let id_high = q
            .enqueue_with(
                "jobs",
                b"high".to_vec(),
                EnqueueOptions {
                    priority: Some(PRIORITY_HIGH),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let j1 = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let j2 = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let j3 = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(j1.id, id_high);
        assert_eq!(j2.id, id_normal);
        assert_eq!(j3.id, id_low);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_priority_fifo_within_same_priority() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        // Two jobs at the same priority must come out in insertion (FIFO) order.
        let id_first = q
            .enqueue_with(
                "jobs",
                b"first".to_vec(),
                EnqueueOptions {
                    priority: Some(PRIORITY_NORMAL),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let id_second = q
            .enqueue_with(
                "jobs",
                b"second".to_vec(),
                EnqueueOptions {
                    priority: Some(PRIORITY_NORMAL),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let j1 = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let j2 = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(j1.id, id_first);
        assert_eq!(j2.id, id_second);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_priority_preserved_after_nack() {
        let q = Queue::open_with_options(make_store(), "test", no_backoff_opts())
            .await
            .unwrap();

        // A high-priority job that is nacked should still come back before a normal job.
        let id_high = q
            .enqueue_with(
                "jobs",
                b"high".to_vec(),
                EnqueueOptions {
                    priority: Some(PRIORITY_HIGH),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let _id_normal = q
            .enqueue_with(
                "jobs",
                b"normal".to_vec(),
                EnqueueOptions {
                    priority: Some(PRIORITY_NORMAL),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let job = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, id_high);

        q.nack(job, "retry me").await.unwrap();

        // High-priority job should be claimed again before the normal one.
        let reclaimed = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reclaimed.id, id_high);
        assert_eq!(reclaimed.priority, PRIORITY_HIGH);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_priority_stored_on_job_record() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue_with(
            "jobs",
            b"x".to_vec(),
            EnqueueOptions {
                priority: Some(PRIORITY_HIGH),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let job = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(job.priority, PRIORITY_HIGH);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_enqueue_at_future_not_immediately_claimable() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let run_at = std::time::SystemTime::now() + Duration::from_secs(3600);
        q.enqueue_with(
            "jobs",
            b"future".to_vec(),
            EnqueueOptions {
                run_at: Some(run_at),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Job is not yet claimable.
        assert!(
            q.claim("jobs", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        let s = q.stats("jobs").await.unwrap();
        assert_eq!(s.scheduled, 1);
        assert_eq!(s.pending, 0);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_enqueue_at_past_is_immediately_pending() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let run_at = std::time::SystemTime::now() - Duration::from_secs(1);
        q.enqueue_with(
            "jobs",
            b"past".to_vec(),
            EnqueueOptions {
                run_at: Some(run_at),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // A past run_at goes straight to pending.
        let job = q.claim("jobs", Duration::from_secs(30)).await.unwrap();
        assert!(job.is_some());

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_promote_scheduled_now() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        // Enqueue a job with a 1ms run_at (already in the past by the time we promote).
        let run_at = std::time::SystemTime::now() + Duration::from_millis(1);
        let id = q
            .enqueue_with(
                "jobs",
                b"soon".to_vec(),
                EnqueueOptions {
                    run_at: Some(run_at),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Not yet promoted.
        assert!(
            q.claim("jobs", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        // Small sleep to ensure run_at has passed, then trigger a manual promotion.
        tokio::time::sleep(Duration::from_millis(5)).await;
        q.promote_scheduled_now().await.unwrap();

        let s = q.stats("jobs").await.unwrap();
        assert_eq!(s.scheduled, 0);
        assert_eq!(s.pending, 1);

        let job = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, id);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_enqueue_in_convenience() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue_with(
            "jobs",
            b"delayed".to_vec(),
            EnqueueOptions {
                run_at: Some(std::time::SystemTime::now() + Duration::from_secs(3600)),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let s = q.stats("jobs").await.unwrap();
        assert_eq!(s.scheduled, 1);
        assert_eq!(s.pending, 0);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_scheduled_job_preserves_priority() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let run_at = std::time::SystemTime::now() + Duration::from_millis(1);
        q.enqueue_with(
            "jobs",
            b"normal".to_vec(),
            EnqueueOptions {
                run_at: Some(run_at),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        // Enqueue a high-priority immediate job after the scheduled one.
        q.enqueue_with(
            "jobs",
            b"high".to_vec(),
            EnqueueOptions {
                priority: Some(PRIORITY_HIGH),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(5)).await;
        q.promote_scheduled_now().await.unwrap();

        // High-priority should come first even though scheduled was enqueued first.
        let j1 = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(j1.payload, b"high");

        let j2 = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(j2.payload, b"normal");

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_dead_letter_skips_attempts_check() {
        // dead_letter() should move a job claimed -> dead unconditionally,
        // without bumping attempts or honouring max_attempts.
        let q = Queue::open_with_options(
            make_store(),
            "test",
            OpenOptions {
                queue_configs: HashMap::from([(
                    "work".to_string(),
                    QueueConfig {
                        max_attempts: 5,
                        ..QueueConfig::default()
                    },
                )]),
                ..OpenOptions::default()
            },
        )
        .await
        .unwrap();

        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let claimed = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.attempts, 1);

        q.dead_letter(claimed, "permanent failure").await.unwrap();

        let job = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Dead);
        assert_eq!(job.attempts, 1, "attempts should not be incremented");
        assert_eq!(job.last_error.as_deref(), Some("permanent failure"));
        assert!(job.failed_at.is_some());

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.dead, 1);
        assert_eq!(stats.claimed, 0);
    }

    #[tokio::test]
    async fn test_run_worker_dead_letters_on_permanent_failure() {
        // A Worker returning PermanentFailure should dead-letter immediately,
        // skipping the retry/backoff path that a plain error takes.
        use crate::worker::{PermanentFailure, Worker, WorkerError, run_worker};

        struct PermanentFailWorker;
        impl Worker for PermanentFailWorker {
            async fn process(&self, _job: &JobRecord) -> std::result::Result<(), WorkerError> {
                Err(PermanentFailure::new("HTTP 410 Gone").into())
            }
        }

        let q = Arc::new(Queue::open(make_store(), "test").await.unwrap());
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let q2 = q.clone();
        let handle = tokio::spawn(async move {
            run_worker(
                &q2,
                "work",
                &PermanentFailWorker,
                Duration::from_millis(10),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
        });

        // Wait for the dead counter to tick, then shut down.
        loop {
            let s = q.stats("work").await.unwrap();
            if s.dead > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let _ = shutdown_tx.send(());
        let _ = handle.await;

        let job = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Dead);
        assert_eq!(
            job.attempts, 1,
            "PermanentFailure should not consume retries"
        );
        assert_eq!(job.last_error.as_deref(), Some("HTTP 410 Gone"));
    }

    #[tokio::test]
    async fn test_worker_trait() {
        use crate::worker::{Worker, WorkerError, run_worker};

        struct EchoWorker;
        impl Worker for EchoWorker {
            async fn process(&self, _job: &JobRecord) -> std::result::Result<(), WorkerError> {
                Ok(())
            }
        }

        let q = Arc::new(Queue::open(make_store(), "test").await.unwrap());
        q.enqueue("work", b"hello".to_vec()).await.unwrap();

        // Drive the worker via a oneshot shutdown so the in-flight job finishes
        // cleanly instead of being aborted mid-claim.
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let q2 = q.clone();
        let handle = tokio::spawn(async move {
            run_worker(
                &q2,
                "work",
                &EchoWorker,
                Duration::from_millis(10),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
        });

        // Wait for the queue to drain, then signal shutdown.
        loop {
            let s = q.stats("work").await.unwrap();
            if s.pending == 0 && s.claimed == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let _ = shutdown_tx.send(());
        let _ = handle.await;

        // Job should now be done, queue empty
        assert!(
            q.claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        // Can't call q.close() since q is in an Arc and there may be a strong reference
        // held by the spawned task still shutting down; just drop.
    }

    #[tokio::test]
    async fn test_get_job_tracks_lifecycle() {
        // Opt in to keeping done jobs so get_job can resolve them after ack.
        let opts = OpenOptions {
            keep_done_jobs: Some(Duration::from_secs(60)),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        // Pending
        let job = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Pending);

        // Claimed
        let claimed = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let job = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Claimed);

        // Done
        q.ack(&claimed).await.unwrap();
        let job = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Done);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_ack_deletes_job_by_default() {
        // Default config: ack drops the job entirely. The done counter still
        // increments, but the ID is no longer findable via get_job.
        let q = Queue::open(make_store(), "test").await.unwrap();

        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();

        assert!(
            q.get_job(&id).await.unwrap().is_none(),
            "ack must drop the index by default"
        );
        let s = q.stats("work").await.unwrap();
        assert_eq!(s.done, 1, "done counter still tracks throughput");
        assert_eq!(s.pending, 0);
        assert_eq!(s.claimed, 0);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_done_retention_sweeps_old_jobs() {
        // Open with a tight retention so the sweep clears the entry quickly.
        let opts = OpenOptions {
            keep_done_jobs: Some(Duration::from_millis(20)),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();
        // Visible immediately after ack.
        assert!(q.get_job(&id).await.unwrap().is_some());

        tokio::time::sleep(Duration::from_millis(30)).await;
        q.sweep_done_now(Duration::from_millis(20)).await.unwrap();

        assert!(
            q.get_job(&id).await.unwrap().is_none(),
            "retention sweep must purge expired done jobs"
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_done_retention_uses_completion_time_not_enqueue_time() {
        let opts = OpenOptions {
            keep_done_jobs: Some(Duration::from_millis(500)),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let id = q
            .enqueue_with(
                "work",
                b"weekly".to_vec(),
                EnqueueOptions {
                    run_at: Some(std::time::SystemTime::now() + Duration::from_millis(200)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Wait past the schedule, promote, claim, ack.
        tokio::time::sleep(Duration::from_millis(220)).await;
        q.promote_scheduled_now().await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        let elapsed_since_enqueue = now_ms().saturating_sub(job.enqueued_at);
        assert!(
            elapsed_since_enqueue > 200,
            "enqueued_at should be well over 200ms old (was {elapsed_since_enqueue}ms)"
        );
        q.ack(&job).await.unwrap();

        // Sweep right after ack: completion is fresh, so the record survives.
        q.sweep_done_now(Duration::from_millis(500)).await.unwrap();
        let kept = q.get_job(&id).await.unwrap().expect(
            "fresh completion must survive the sweep regardless of how long ago the job was enqueued",
        );
        assert!(
            kept.completed_at.is_some(),
            "ack must stamp completed_at when keep_done_jobs is set"
        );

        // After the retention window elapses the record is purged as expected.
        tokio::time::sleep(Duration::from_millis(550)).await;
        q.sweep_done_now(Duration::from_millis(500)).await.unwrap();
        assert!(q.get_job(&id).await.unwrap().is_none());

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_dead_retention_sweep_boundary() {
        // Drive a job to dead-letter, then exercise both sides of the
        // retention cutoff: a long-retention sweep must leave it alone, and a
        // sweep with a tighter window must purge it (along with its index
        // pointer and the `dead` counter).
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue_with(
            "work",
            b"payload".to_vec(),
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
        let id = job.id.clone();
        q.nack(job, "fatal").await.unwrap();

        let dead = q.dead_jobs("work", None, 100).await.unwrap();
        assert_eq!(dead.len(), 1);
        assert!(dead[0].failed_at.is_some(), "failed_at must be stamped");
        assert_eq!(q.stats("work").await.unwrap().dead, 1);

        // Above the cutoff: long retention keeps the job.
        q.sweep_dead_now(Duration::from_secs(3600)).await.unwrap();
        assert_eq!(q.dead_jobs("work", None, 100).await.unwrap().len(), 1);

        // Below the cutoff: tight retention purges it. Counter and index
        // pointer must both be cleaned up too.
        tokio::time::sleep(Duration::from_millis(30)).await;
        q.sweep_dead_now(Duration::from_millis(20)).await.unwrap();
        assert!(q.dead_jobs("work", None, 100).await.unwrap().is_empty());
        assert_eq!(
            q.stats("work").await.unwrap().dead,
            0,
            "dead counter must reflect the sweep"
        );
        assert!(q.get_job(&id).await.unwrap().is_none());

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_requeue_dead_resets_failed_at() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue_with(
            "work",
            b"payload".to_vec(),
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
        q.nack(job, "fatal").await.unwrap();

        let dead = q.dead_jobs("work", None, 100).await.unwrap().pop().unwrap();
        assert!(dead.failed_at.is_some());

        q.requeue_dead_job(dead).await.unwrap();
        let pending = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert!(
            pending.failed_at.is_none(),
            "requeue must clear failed_at so a re-fail starts a fresh retention window"
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_job_returns_none_for_unknown_id() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        assert!(q.get_job("nonexistent").await.unwrap().is_none());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_job_after_nack_to_dead() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue_with(
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
        let id = job.id.clone();
        q.nack(job, "fatal").await.unwrap();

        let dead = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(dead.status, JobStatus::Dead);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_renew_lease() {
        // Covers the three things `renew_lease` has to get right: the new
        // expiry replaces the old one, the reaper sees the renewed lease and
        // skips the job, and the `jobindex:` pointer is updated so `get_job`
        // resolves through the new `claimed:{ts}:...` key (not a dangling
        // pointer at the old timestamp).
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let mut job = q
            .claim("work", Duration::from_millis(1))
            .await
            .unwrap()
            .unwrap();
        let original_expiry = job.lease_expires_at.unwrap();

        q.renew_lease(&mut job, Duration::from_secs(30))
            .await
            .unwrap();
        let new_expiry = job.lease_expires_at.unwrap();
        assert!(new_expiry > original_expiry, "renewed expiry must be later");

        // Reaper skips the renewed lease.
        q.reap_now().await.unwrap();
        assert!(
            q.claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        // get_job resolves through the new claimed key, not the old one.
        let fetched = q.get_job(&job.id).await.unwrap().unwrap();
        assert_eq!(fetched.status, JobStatus::Claimed);
        assert_eq!(fetched.lease_expires_at.unwrap(), new_expiry);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cancel_pending_job() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        let cancelled = q.cancel(&id).await.unwrap();
        assert!(cancelled);

        // No longer claimable.
        assert!(
            q.claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        // No longer findable by ID.
        assert!(q.get_job(&id).await.unwrap().is_none());

        // Stats reflect the removal.
        assert_eq!(q.stats("work").await.unwrap().pending, 0);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cancel_scheduled_job() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let id = q
            .enqueue_with(
                "work",
                b"payload".to_vec(),
                EnqueueOptions {
                    run_at: Some(std::time::SystemTime::now() + Duration::from_secs(3600)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(q.stats("work").await.unwrap().scheduled, 1);
        let cancelled = q.cancel(&id).await.unwrap();
        assert!(cancelled);
        assert_eq!(q.stats("work").await.unwrap().scheduled, 0);
        assert!(q.get_job(&id).await.unwrap().is_none());

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cancel_claimed_job_returns_false() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        // Cannot cancel a job that is currently being worked.
        let cancelled = q.cancel(&job.id).await.unwrap();
        assert!(!cancelled);

        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cancel_nonexistent_returns_false() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        assert!(!q.cancel("does-not-exist").await.unwrap());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_enqueue_batch_atomic() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let payloads = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let ids = q.enqueue_batch("work", payloads).await.unwrap();
        assert_eq!(ids.len(), 3);

        let s = q.stats("work").await.unwrap();
        assert_eq!(s.pending, 3);

        // All jobs are findable and ordered FIFO.
        let j1 = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let j2 = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let j3 = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(j1.id, ids[0]);
        assert_eq!(j2.id, ids[1]);
        assert_eq!(j3.id, ids[2]);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_enqueue_batch_empty_is_noop() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let ids = q.enqueue_batch("work", vec![]).await.unwrap();
        assert!(ids.is_empty());
        assert_eq!(q.stats("work").await.unwrap().pending, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_enqueue_unique_deduplicates() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let id1 = q
            .enqueue_with(
                "work",
                b"first".to_vec(),
                EnqueueOptions {
                    dedup_key: Some("my-key".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        // Second call with the same key must return the existing ID.
        let id2 = q
            .enqueue_with(
                "work",
                b"second".to_vec(),
                EnqueueOptions {
                    dedup_key: Some("my-key".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(id1, id2);
        assert_eq!(q.stats("work").await.unwrap().pending, 1);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_enqueue_unique_allows_reenqueue_after_claim() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let id1 = q
            .enqueue_with(
                "work",
                b"payload".to_vec(),
                EnqueueOptions {
                    dedup_key: Some("my-key".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Claim the job, which releases the dedup key.
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, id1);

        // Now a new enqueue with the same key is accepted.
        let id2 = q
            .enqueue_with(
                "work",
                b"payload".to_vec(),
                EnqueueOptions {
                    dedup_key: Some("my-key".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_ne!(id1, id2);
        assert_eq!(q.stats("work").await.unwrap().pending, 1);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_enqueue_unique_nack_then_reenqueue_does_not_corrupt_dedup() {
        let q = Queue::open_with_options(make_store(), "test", no_backoff_opts())
            .await
            .unwrap();

        let id1 = q
            .enqueue_with(
                "work",
                b"payload".to_vec(),
                EnqueueOptions {
                    dedup_key: Some("user-42".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Claim and nack the first job; with no backoff it goes back to pending.
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        // After claim, dedup_key must be cleared on the record so a future
        // claim doesn't try to release the (now reused) index.
        assert!(job.dedup_key.is_none());
        q.nack(job, "transient").await.unwrap();

        // A fresh enqueue_unique with the same key should be accepted now
        // (claim released the index) and create a different job.
        let id2 = q
            .enqueue_with(
                "work",
                b"payload".to_vec(),
                EnqueueOptions {
                    dedup_key: Some("user-42".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_ne!(id1, id2);

        // Drain both jobs; both must complete and the second job's dedup
        // index must remain intact while it sits in pending.
        let j1 = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        // While j1 is claimed (and may be the retry of id1), a third
        // enqueue_unique with the same key must STILL be blocked by id2's
        // index entry.
        let id3 = q
            .enqueue_with(
                "work",
                b"payload".to_vec(),
                EnqueueOptions {
                    dedup_key: Some("user-42".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            id3, id2,
            "id2's dedup index must still block the third enqueue while id2 is pending"
        );
        q.ack(&j1).await.unwrap();

        let j2 = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&j2).await.unwrap();

        assert_eq!(q.stats("work").await.unwrap().pending, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_nack_with_backoff_parks_in_scheduled() {
        // Default config has retry_backoff_base = 1s, so a nack should move the
        // job into the scheduled space rather than immediately back to pending.
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue_with(
            "work",
            b"payload".to_vec(),
            EnqueueOptions {
                max_attempts: Some(3),
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
        q.nack(job, "transient").await.unwrap();

        let s = q.stats("work").await.unwrap();
        assert_eq!(s.pending, 0, "must not be pending immediately");
        assert_eq!(s.claimed, 0);
        assert_eq!(s.scheduled, 1, "must be parked in scheduled");

        // Not yet claimable.
        assert!(
            q.claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_nack_backoff_promoted_after_run_at() {
        // Use a tiny backoff so the test doesn't sleep for long.
        let opts = OpenOptions {
            default_queue_config: QueueConfig {
                retry_backoff_base: Duration::from_millis(10),
                retry_backoff_max: Duration::from_millis(10),
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue_with(
            "work",
            b"payload".to_vec(),
            EnqueueOptions {
                max_attempts: Some(5),
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
        let id = job.id.clone();
        q.nack(job, "boom").await.unwrap();

        // Wait past the backoff and trigger promotion.
        tokio::time::sleep(Duration::from_millis(20)).await;
        q.promote_scheduled_now().await.unwrap();

        let retried = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retried.id, id);
        assert_eq!(retried.attempts, 2);
        assert_eq!(retried.last_error.as_deref(), Some("boom"));

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_backoff_delay_calculation() {
        let base = Duration::from_secs(1);
        let max = Duration::from_secs(60);

        assert_eq!(backoff_delay(1, base, max), Duration::from_secs(1));
        assert_eq!(backoff_delay(2, base, max), Duration::from_secs(2));
        assert_eq!(backoff_delay(3, base, max), Duration::from_secs(4));
        assert_eq!(backoff_delay(4, base, max), Duration::from_secs(8));
        // Caps at max.
        assert_eq!(backoff_delay(20, base, max), max);
        // Zero base: no backoff regardless of attempts.
        assert_eq!(
            backoff_delay(5, Duration::ZERO, Duration::from_secs(10)),
            Duration::ZERO
        );
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
            q.nack(job, "fail").await.unwrap();
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
    async fn test_worker_finishes_in_flight_job_on_shutdown() {
        use crate::worker::{Worker, WorkerError, run_worker};
        use std::sync::atomic::{AtomicBool, Ordering};

        // Worker that takes 100ms to process, long enough that shutdown
        // fires while the job is in flight.
        struct SlowWorker {
            finished: Arc<AtomicBool>,
        }
        impl Worker for SlowWorker {
            async fn process(&self, _job: &JobRecord) -> std::result::Result<(), WorkerError> {
                tokio::time::sleep(Duration::from_millis(100)).await;
                self.finished.store(true, Ordering::SeqCst);
                Ok(())
            }
        }

        let q = Arc::new(Queue::open(make_store(), "test").await.unwrap());
        q.enqueue("work", b"x".to_vec()).await.unwrap();

        let finished = Arc::new(AtomicBool::new(false));
        let worker = SlowWorker {
            finished: finished.clone(),
        };
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let q2 = q.clone();
        let handle = tokio::spawn(async move {
            run_worker(
                &q2,
                "work",
                &worker,
                Duration::from_millis(50),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
        });

        // Wait for the worker to claim the job, then immediately request shutdown.
        loop {
            if q.stats("work").await.unwrap().claimed == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let _ = shutdown_tx.send(());
        let _ = handle.await;

        assert!(
            finished.load(Ordering::SeqCst),
            "in-flight job must finish before shutdown returns"
        );
        // And the job was acked, not left in claimed: for the reaper.
        assert_eq!(q.stats("work").await.unwrap().claimed, 0);
        assert_eq!(q.stats("work").await.unwrap().done, 1);
    }

    #[tokio::test]
    async fn test_claim_with_wait_wakes_or_times_out() {
        // Both arms of the internal `select!`: the timeout branch returns
        // None when nothing arrives, and the notify branch wakes immediately
        // when an enqueue happens, well before max_wait elapses.
        let q = Arc::new(Queue::open(make_store(), "test").await.unwrap());

        // Idle queue with a short max_wait: returns None.
        let timed_out = q
            .claim_with_wait("work", Duration::from_secs(30), Duration::from_millis(50))
            .await
            .unwrap();
        assert!(timed_out.is_none());

        // Live wakeup: spawn a waiter with a long max_wait, enqueue, expect
        // a fast resolution.
        let q2 = q.clone();
        let waiter = tokio::spawn(async move {
            let start = std::time::Instant::now();
            let job = q2
                .claim_with_wait("work", Duration::from_secs(30), Duration::from_secs(10))
                .await
                .unwrap();
            (start.elapsed(), job)
        });

        // Give the waiter time to subscribe to the notify, then enqueue.
        tokio::time::sleep(Duration::from_millis(20)).await;
        q.enqueue("work", b"hello".to_vec()).await.unwrap();

        let (elapsed, job) = waiter.await.unwrap();
        assert!(job.is_some(), "claim_with_wait must wake on enqueue");
        assert!(
            elapsed < Duration::from_millis(500),
            "expected fast wake; took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn test_concurrent_worker() {
        use crate::worker::{Worker, WorkerError, run_worker_concurrent};

        struct EchoWorker;
        impl Worker for EchoWorker {
            async fn process(&self, _job: &JobRecord) -> std::result::Result<(), WorkerError> {
                tokio::time::sleep(Duration::from_millis(5)).await;
                Ok(())
            }
        }

        let q = Arc::new(Queue::open(make_store(), "test").await.unwrap());
        let ids = q
            .enqueue_batch(
                "work",
                vec![
                    b"a".to_vec(),
                    b"b".to_vec(),
                    b"c".to_vec(),
                    b"d".to_vec(),
                    b"e".to_vec(),
                ],
            )
            .await
            .unwrap();
        assert_eq!(ids.len(), 5);

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let q2 = q.clone();
        let handle = tokio::spawn(async move {
            run_worker_concurrent(
                &q2,
                "work",
                Arc::new(EchoWorker),
                3,
                Duration::from_millis(10),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
        });

        loop {
            let s = q.stats("work").await.unwrap();
            if s.pending == 0 && s.claimed == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = shutdown_tx.send(());
        let _ = handle.await;

        assert_eq!(q.stats("work").await.unwrap().done, 5);
    }
}
