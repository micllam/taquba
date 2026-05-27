use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use slatedb::object_store::ObjectStore;
use slatedb::{Db, IsolationLevel};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, instrument, warn};
use ulid::Ulid;

use crate::clock::{Clock, default_clock};
use crate::error::{Error, Result};
use crate::job::{JobRecord, JobStatus};
use crate::reaper::{Reaper, reap_expired};
use crate::scheduler::{Scheduler, promote_due_jobs};
use crate::stats::{CounterMergeOperator, QueueStats, read_stats, update_stats};

const DEFAULT_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(30);

/// Outcome of [`Queue::cancel`], reflecting which lifecycle branch the
/// job was in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The job was `Pending` or `Scheduled` and has been removed from the
    /// queue. No worker will ever see it.
    Removed,
    /// The job was `Claimed`; the cancellation has been requested via the
    /// persisted [`JobRecord::cancel_requested`] flag and the in-process
    /// [`JobRecord::cancel_token`] has been fired. The worker is still
    /// running and will eventually `ack` / `nack` / `dead_letter` the
    /// job according to its own logic.
    Requested,
    /// No job with this ID was found, or it was already in a terminal
    /// state (`Done` / `Dead`).
    NotFound,
}

impl CancelOutcome {
    /// `true` if the call acted on the job (either removed or requested).
    pub fn acted(self) -> bool {
        !matches!(self, CancelOutcome::NotFound)
    }
}

/// High-priority bucket. Jobs at this priority are dequeued before normal and low.
pub const PRIORITY_HIGH: u32 = 100;
/// Default priority. FIFO ordering is preserved within the same priority level.
pub const PRIORITY_NORMAL: u32 = 1_000;
/// Low-priority bucket. Jobs at this priority are dequeued after high and normal.
pub const PRIORITY_LOW: u32 = 10_000;

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

/// Reserved prefix for the user-facing KV namespace.
///
/// Caller-supplied keys are internally scoped under this prefix so they
/// cannot collide with Taquba's own key layout (`pending:`, `claimed:`,
/// `dead:`, `done:`, `scheduled:`, `jobindex:`, `dedup:`, `stats:`).
const USR_PREFIX: &[u8] = b"usr:";

/// Maximum size of a single value in the user KV namespace.
///
/// The KV namespace is sized for coordination state (pointers, status
/// markers, dedup records, small lifecycle records), not bulk payload.
/// Values exceeding this cap return [`Error::KvValueTooLarge`].
///
/// Store large blobs in the underlying [`ObjectStore`] under a
/// content-addressed key and put only the pointer in KV.
pub const MAX_KV_VALUE_SIZE: usize = 256 * 1024;

fn user_scoped_key(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(USR_PREFIX.len() + key.len());
    out.extend_from_slice(USR_PREFIX);
    out.extend_from_slice(key);
    out
}

/// Maximum byte length of a caller-supplied
/// [`EnqueueOptions::id_override`]. Enforces a sane cap on key sizes
/// independently of the underlying object store's path limits.
const MAX_ID_OVERRIDE_LEN: usize = 128;

/// Validate a caller-supplied job id. Caller-supplied ids must be
/// 1-[`MAX_ID_OVERRIDE_LEN`] bytes of `[A-Za-z0-9_-]`. `:` is reserved
/// as the key delimiter in `pending:`/`scheduled:`/`claimed:` keys, and
/// other non-alphanumeric bytes are rejected up front to keep ids safe
/// for object-store paths and log lines downstream.
fn validate_id_override(id: &str) -> Result<()> {
    if id.is_empty() {
        return Err(Error::InvalidId {
            id: id.to_string(),
            reason: "id must not be empty",
        });
    }
    if id.len() > MAX_ID_OVERRIDE_LEN {
        return Err(Error::InvalidId {
            id: id.to_string(),
            reason: "id exceeds maximum length of 128 bytes",
        });
    }
    if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(Error::InvalidId {
            id: id.to_string(),
            reason: "id must contain only `[A-Za-z0-9_-]`",
        });
    }
    Ok(())
}

/// Outcome of [`Queue::enqueue_with_kv`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnqueueResult {
    /// A new job was enqueued. The string is its freshly-allocated id.
    /// The accompanying `kv_writes` map was applied atomically.
    New(String),
    /// A pending or scheduled job with the same `dedup_key` already
    /// existed; no new job was written and **no KV writes were applied**.
    /// The string is the existing job's id.
    AlreadyEnqueued(String),
}

impl EnqueueResult {
    /// The id of the underlying job, whether newly enqueued or pre-existing.
    pub fn id(&self) -> &str {
        match self {
            Self::New(id) | Self::AlreadyEnqueued(id) => id,
        }
    }
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
    /// If `Some(duration)`, completed jobs on this queue are written to the
    /// `done:` keyspace and retained for `duration`. The reaper purges them
    /// once `completed_at + duration` has passed.
    ///
    /// If `None` (default), [`Queue::ack`] deletes successful jobs outright.
    ///
    /// The success counter in [`QueueStats::done`] is incremented either way.
    pub keep_done_jobs: Option<Duration>,
    /// Maximum age of a dead-letter job on this queue before the retention
    /// sweep purges it. Default is 7 days, which gives operators time to
    /// inspect or requeue without leaking storage. `None` disables the
    /// sweep for this queue: dead jobs accumulate without bound.
    pub dead_retention: Option<Duration>,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            lease_duration: DEFAULT_LEASE_DURATION,
            default_priority: PRIORITY_NORMAL,
            retry_backoff_base: Duration::from_secs(1),
            retry_backoff_max: Duration::from_secs(300),
            keep_done_jobs: None,
            dead_retention: Some(Duration::from_secs(7 * 24 * 3600)),
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
    /// [`Self::queue_configs`]. Retention policies
    /// ([`QueueConfig::keep_done_jobs`], [`QueueConfig::dead_retention`])
    /// live on `QueueConfig`, so per-queue overrides can pick different
    /// retention windows for, say, ephemeral webhook deliveries vs.
    /// long-running workflows.
    pub default_queue_config: QueueConfig,
    /// Per-queue overrides. Keys are queue names.
    pub queue_configs: HashMap<String, QueueConfig>,
    /// Time source for every state-transition timestamp and every
    /// time-based comparison (retention cutoffs, scheduled-job
    /// promotion). Defaults to [`SystemClock`](crate::SystemClock).
    /// Substitute [`MockClock`](crate::MockClock) in tests to advance
    /// time deterministically.
    pub clock: Arc<dyn Clock>,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            reaper_interval: Duration::from_secs(5),
            scheduler_interval: Duration::from_secs(1),
            default_queue_config: QueueConfig::default(),
            queue_configs: HashMap::new(),
            clock: default_clock(),
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
    /// Override the job id that the queue would otherwise generate.
    ///
    /// When `None` (the default), the queue assigns a monotonic ULID.
    /// When `Some`, the supplied id is used as the job's id.
    ///
    /// Useful when callers need the id to be known *before* the enqueue
    /// returns.
    ///
    /// Uniqueness is the caller's responsibility: a duplicate id silently
    /// overwrites the prior job's record. ULID generation guarantees
    /// uniqueness for the `None` path; caller-supplied ids must be
    /// globally unique within the queue's lifetime.
    ///
    /// Constraints (enforced; violations return [`Error::InvalidId`]):
    ///
    /// - 1-128 bytes long.
    /// - Characters limited to `[A-Za-z0-9_-]`.
    ///
    /// Prefer ULID-shaped ids when FIFO-within-priority claim ordering
    /// matters: `pending` and `scheduled` keys end with the id, so claim
    /// order follows id sort.
    pub id_override: Option<String>,
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
    clock: Arc<dyn Clock>,
    /// In-process wakeup signal so workers blocked on an empty queue can resume
    /// the moment a job becomes claimable, without waiting out their poll
    /// interval.
    job_available: Arc<tokio::sync::Notify>,
    /// In-process cancellation tokens for currently claimed jobs. Populated
    /// by every `claim*` path, cleared on `ack` / `nack` / `dead_letter`.
    /// `Queue::cancel` fires the token while the job is `Claimed`; the
    /// persisted `cancel_requested` flag carries the request across
    /// reaper-driven requeues and re-claims.
    claimed_tokens: Arc<std::sync::Mutex<HashMap<String, tokio_util::sync::CancellationToken>>>,
    /// Wakeup fired whenever any job reaches a terminal state: `Done`
    /// (acked, kept or not), `Dead` (dead-lettered by worker, exhausted
    /// retry, or reaper), or `Pending` / `Scheduled` jobs removed via
    /// [`Self::cancel`]. Drives [`Self::wait_for_completion`].
    completion_notify: Arc<tokio::sync::Notify>,
}

/// Outcome of [`Queue::wait_for_completion`].
///
/// The terminal-record case (`Completed(Some(record))`) carries the
/// final [`JobRecord`] when taquba retained one on the way out.
/// `Completed(None)` means the job terminated but no record survived
/// the transition. It depends on both the kind of transition and the
/// queue's configuration:
///
/// | Transition                                         | Retained?                                   |
/// |----------------------------------------------------|---------------------------------------------|
/// | Worker `ack` (success)                             | Only if [`QueueConfig::keep_done_jobs`] is set |
/// | Worker `nack` past `max_attempts` (Dead)           | Always                                      |
/// | Worker [`Queue::dead_letter`] (permanent failure)  | Always                                      |
/// | Reaper dead-letter (lease expired past max_attempts) | Always                                    |
/// | [`Queue::cancel`] removing a `Pending`/`Scheduled` job | Never                                    |
///
/// # Disambiguating `Completed(None)`
///
/// With the default configuration, `Completed(None)` is reachable from
/// **two** distinct paths: a successful `ack` whose record was deleted,
/// and a Pending/Scheduled cancellation. Callers that need to tell them
/// apart should set [`QueueConfig::keep_done_jobs`]. That option keeps
/// successful records around for a bounded retention window, which,
/// beyond resolving the ambiguity, also lets the caller inspect
/// `last_error`, `completed_at`, `attempts`, and the original `payload`
/// on every successful run, not just the final status.
///
/// Most callers don't need to distinguish: they enqueued the job
/// themselves and know they didn't cancel it, so `Completed(None)`
/// unambiguously means "succeeded, record not kept".
#[derive(Debug)]
pub enum WaitOutcome {
    /// The job reached a terminal state (`Done`, `Dead`, or removed
    /// via `cancel`) while the call was waiting, or was already
    /// terminal on entry. The inner is `Some` only when taquba kept
    /// the terminal record; see the type-level doc for the retention
    /// matrix.
    Completed(Option<Box<JobRecord>>),
    /// The wait elapsed before the job reached a terminal state. The
    /// job is still pending, scheduled, or claimed somewhere.
    TimedOut,
    /// No job with this ID was present at the start of the call.
    NotFound,
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
        let completion_notify = Arc::new(tokio::sync::Notify::new());
        let (reaper_shutdown, reaper_rx) = watch::channel(false);
        let reaper = Reaper {
            db: db.clone(),
            interval: opts.reaper_interval,
            default_queue_config: opts.default_queue_config.clone(),
            queue_configs: opts.queue_configs.clone(),
            clock: opts.clock.clone(),
            job_available: job_available.clone(),
            completion_notify: completion_notify.clone(),
        };
        let reaper_handle = tokio::spawn(reaper.run(reaper_rx));
        let (scheduler_shutdown, scheduler_rx) = watch::channel(false);
        let scheduler = Scheduler {
            db: db.clone(),
            interval: opts.scheduler_interval,
            clock: opts.clock.clone(),
            job_available: job_available.clone(),
        };
        let scheduler_handle = tokio::spawn(scheduler.run(scheduler_rx));
        Ok(Self {
            db,
            reaper_shutdown,
            reaper_handle,
            scheduler_shutdown,
            scheduler_handle,
            default_queue_config: opts.default_queue_config,
            queue_configs: opts.queue_configs,
            clock: opts.clock,
            job_available,
            claimed_tokens: Arc::new(std::sync::Mutex::new(HashMap::new())),
            completion_notify,
        })
    }

    /// Current time in milliseconds since the UNIX epoch, as read
    /// from this queue's configured [`Clock`].
    pub(crate) fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    /// Register a freshly-claimed job's cancellation token. Called from
    /// every `claim*` path after the transaction commits. The token is
    /// fired immediately if `cancel_requested` was already persisted;
    /// this handles the case where `Queue::cancel` ran during a previous
    /// lease that subsequently expired and was reaped back to pending.
    fn install_cancel_token(&self, job: &mut JobRecord) {
        let token = tokio_util::sync::CancellationToken::new();
        if job.cancel_requested {
            token.cancel();
        }
        self.claimed_tokens
            .lock()
            .expect("claimed_tokens mutex poisoned")
            .insert(job.id.clone(), token.clone());
        job.cancel_token = Some(token);
    }

    /// Drop a claimed job's token. Called from `ack` / `nack` / `dead_letter`
    /// once the claim is settled.
    fn clear_cancel_token(&self, job_id: &str) {
        self.claimed_tokens
            .lock()
            .expect("claimed_tokens mutex poisoned")
            .remove(job_id);
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

    /// Look up the configured `keep_done_jobs` retention for a queue.
    /// `None` means [`Self::ack`] deletes successful jobs outright on that queue.
    pub fn queue_keep_done_jobs(&self, queue: &str) -> Option<Duration> {
        self.queue_config(queue).keep_done_jobs
    }

    /// Look up the configured dead-letter retention for a queue.
    /// `None` means the dead-letter sweep is disabled for that queue.
    pub fn queue_dead_retention(&self, queue: &str) -> Option<Duration> {
        self.queue_config(queue).dead_retention
    }

    /// The [`Clock`] this queue was opened with. Returned as a cheap
    /// `Arc` clone so downstream crates can share the same time
    /// source for their own timestamp work.
    pub fn clock(&self) -> Arc<dyn Clock> {
        self.clock.clone()
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
        let (job, key) = self.prepare_job_record(queue, payload, opts)?;
        match self.write_job(job, key, HashMap::new()).await? {
            EnqueueResult::New(id) | EnqueueResult::AlreadyEnqueued(id) => Ok(id),
        }
    }

    /// Enqueue a job AND apply a set of writes to the user KV namespace
    /// in a single transaction.
    ///
    /// On success ([`EnqueueResult::New`]), the job is enqueued and every
    /// entry in `kv_writes` is applied atomically. On a `dedup_key` hit
    /// ([`EnqueueResult::AlreadyEnqueued`]), **no KV writes are applied**
    /// and the existing job's id is returned.
    ///
    /// Caller-supplied KV keys are internally scoped under a reserved
    /// `usr:` prefix so they cannot collide with Taquba's internal layout.
    /// Each value is validated against [`MAX_KV_VALUE_SIZE`] up front;
    /// oversized values return [`Error::KvValueTooLarge`] before the
    /// transaction begins. Conflict retries are handled internally.
    ///
    /// ```no_run
    /// # use std::collections::HashMap;
    /// # use taquba::{EnqueueOptions, EnqueueResult};
    /// # async fn ex(q: &taquba::Queue) -> taquba::Result<()> {
    /// let mut kv = HashMap::new();
    /// kv.insert(b"runs/abc".to_vec(), b"submitted".to_vec());
    /// let outcome = q.enqueue_with_kv(
    ///     "workflow-steps",
    ///     b"step-0-payload".to_vec(),
    ///     EnqueueOptions {
    ///         dedup_key: Some("run:abc:0".to_string()),
    ///         ..Default::default()
    ///     },
    ///     kv,
    /// ).await?;
    /// match outcome {
    ///     EnqueueResult::New(id) => println!("submitted: {id}"),
    ///     EnqueueResult::AlreadyEnqueued(id) => println!("already running: {id}"),
    /// }
    /// # Ok(()) }
    /// ```
    #[instrument(skip(self, payload, kv_writes), fields(queue, job_id))]
    pub async fn enqueue_with_kv(
        &self,
        queue: &str,
        payload: Vec<u8>,
        opts: EnqueueOptions,
        kv_writes: HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<EnqueueResult> {
        for value in kv_writes.values() {
            if value.len() > MAX_KV_VALUE_SIZE {
                return Err(Error::KvValueTooLarge {
                    size: value.len(),
                    max: MAX_KV_VALUE_SIZE,
                });
            }
        }

        let (job, key) = self.prepare_job_record(queue, payload, opts)?;
        self.write_job(job, key, kv_writes).await
    }

    /// Read a value from the user KV namespace.
    ///
    /// Caller-supplied keys are internally scoped under a reserved
    /// `usr:` prefix and cannot collide with Taquba's internal layout.
    pub async fn kv_get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.db.get(user_scoped_key(key)).await?)
    }

    /// Delete a value from the user KV namespace.
    ///
    /// Caller-supplied keys are internally scoped under a reserved
    /// `usr:` prefix and cannot collide with Taquba's internal layout.
    pub async fn kv_delete(&self, key: &[u8]) -> Result<()> {
        self.db.delete(user_scoped_key(key)).await?;
        Ok(())
    }

    /// Resolve [`EnqueueOptions`] against the queue's defaults and build
    /// the [`JobRecord`] + its primary key. Shared by [`Self::enqueue_with`]
    /// and [`Self::enqueue_with_kv`]; the two methods only diverge in how
    /// they persist the prepared record.
    fn prepare_job_record(
        &self,
        queue: &str,
        payload: Vec<u8>,
        opts: EnqueueOptions,
    ) -> Result<(JobRecord, String)> {
        let cfg = self.queue_config(queue);
        let max_attempts = opts.max_attempts.unwrap_or(cfg.max_attempts);
        let priority = opts.priority.unwrap_or(cfg.default_priority);

        // A `run_at` that is at or before now is just an immediate enqueue.
        let run_at = opts.run_at.and_then(|when| {
            let ms = when
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            (ms > self.now_ms()).then_some(ms)
        });

        let id = match opts.id_override {
            Some(supplied) => {
                validate_id_override(&supplied)?;
                supplied
            }
            None => Ulid::new().to_string(),
        };

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
            enqueued_at: self.now_ms(),
            claimed_at: None,
            lease_expires_at: None,
            run_at,
            priority,
            last_error: None,
            dedup_key: opts.dedup_key,
            completed_at: None,
            failed_at: None,
            cancel_requested: false,
            cancel_token: None,
        };

        Ok((job, key))
    }

    /// Persist a prepared [`JobRecord`], optionally checking a dedup index
    /// and optionally applying additional KV writes, all in a single
    /// transaction. Retries on transaction conflict.
    ///
    /// Returns [`EnqueueResult::AlreadyEnqueued`] (with **no** KV writes
    /// applied) if `job.dedup_key` is set and a pending or scheduled job
    /// with the same dedup key already exists. Otherwise writes the
    /// record + job index + (when set) dedup index + every entry in
    /// `kv_writes`, and returns [`EnqueueResult::New`].
    async fn write_job(
        &self,
        job: JobRecord,
        key: String,
        kv_writes: HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<EnqueueResult> {
        let dkey = job
            .dedup_key
            .as_ref()
            .map(|dk| dedup_index_key(&job.queue, dk));
        let value = rmp_serde::to_vec_named(&job)?;
        let JobRecord {
            id, queue, status, ..
        } = job;

        loop {
            let txn = self.db.begin(IsolationLevel::Snapshot).await?;

            if let Some(ref dkey) = dkey {
                if let Some(bytes) = txn.get(dkey.as_bytes()).await? {
                    txn.rollback();
                    let existing =
                        String::from_utf8(bytes.to_vec()).map_err(|_| Error::InvalidState)?;
                    return Ok(EnqueueResult::AlreadyEnqueued(existing));
                }
            }

            txn.put(key.as_bytes(), &value)?;
            txn.put(job_index_key(&id).as_bytes(), key.as_bytes())?;
            if let Some(ref dkey) = dkey {
                txn.put(dkey.as_bytes(), id.as_bytes())?;
            }
            update_stats(&txn, &queue, &[(status, 1)])?;

            for (k, v) in &kv_writes {
                txn.put(user_scoped_key(k), v)?;
            }

            match txn.commit().await {
                Ok(_) => {
                    // Workers can claim a Pending job immediately; a Scheduled
                    // job becomes claimable later via the scheduler loop,
                    // which fires its own notify.
                    if matches!(status, JobStatus::Pending) {
                        self.job_available.notify_waiters();
                    }
                    debug!(queue = %queue, job_id = %id, "job enqueued");
                    return Ok(EnqueueResult::New(id));
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
        // `enable()` registers the future as a waiter right away;
        // `notify_waiters()` only wakes already-registered waiters, so a
        // merely-constructed `Notified` would not catch a notification
        // published during the `claim` await below.
        let notified = self.job_available.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

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

            let now = self.now_ms();
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

            // Register the cancellation token *before* committing. Once the
            // commit lands, the job is observable as `Claimed` and a
            // concurrent `request_cancel` will look up its token in
            // `claimed_tokens` to fire it. If we registered the token only
            // *after* the commit, a `request_cancel` racing that window
            // would find nothing, persist `cancel_requested = true`, and
            // the worker's live token would never fire => the cancellation
            // would be silently lost until the lease expired. Registering
            // first closes that window; on a commit conflict we unregister
            // and retry.
            self.install_cancel_token(&mut job);
            match txn.commit().await {
                Ok(_) => {
                    debug!(queue = queue, job_id = %job.id, attempt = job.attempts, "job claimed");
                    return Ok(Some(job));
                }
                Err(e) if e.kind() == slatedb::ErrorKind::Transaction => {
                    warn!(queue = queue, "claim transaction conflict, retrying");
                    self.clear_cancel_token(&job.id);
                    continue;
                }
                Err(e) => {
                    self.clear_cancel_token(&job.id);
                    return Err(e.into());
                }
            }
        }
    }

    /// Acknowledge successful completion.
    ///
    /// By default the job is deleted outright; the success counter in
    /// [`QueueStats::done`] is still incremented.
    ///
    /// Set [`QueueConfig::keep_done_jobs`] (per-queue, or on
    /// [`OpenOptions::default_queue_config`] for an instance-wide default)
    /// to retain completed jobs for a bounded duration.
    #[instrument(skip(self, job), fields(queue = %job.queue, job_id = %job.id))]
    pub async fn ack(&self, job: &JobRecord) -> Result<()> {
        let lease_expires_at = job.lease_expires_at.ok_or(Error::InvalidState)?;
        let claimed = claimed_key(&job.queue, lease_expires_at, &job.id);
        let keep_done = self.queue_keep_done_jobs(&job.queue).is_some();
        let done_record = if keep_done {
            let mut done_job = job.clone();
            done_job.status = JobStatus::Done;
            done_job.completed_at = Some(self.now_ms());
            Some((
                done_key(&job.queue, &job.id),
                rmp_serde::to_vec_named(&done_job)?,
            ))
        } else {
            None
        };

        loop {
            let txn = self.db.begin(IsolationLevel::Snapshot).await?;
            txn.delete(claimed.as_bytes())?;
            if let Some((ref done_k, ref done_v)) = done_record {
                txn.put(done_k.as_bytes(), done_v)?;
                txn.put(job_index_key(&job.id).as_bytes(), done_k.as_bytes())?;
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
            match txn.commit().await {
                Ok(_) => break,
                Err(e) if e.kind() == slatedb::ErrorKind::Transaction => continue,
                Err(e) => return Err(e.into()),
            }
        }

        self.clear_cancel_token(&job.id);
        self.completion_notify.notify_waiters();
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
            job.failed_at = Some(self.now_ms());
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
                let run_at = self.now_ms() + backoff.as_millis() as u64;
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
        let became_dead = matches!(job.status, JobStatus::Dead);
        let job_id = job.id.clone();
        txn.commit().await?;
        self.clear_cancel_token(&job_id);
        if immediate_retry {
            // Backoff path doesn't need a wake: the scheduler loop will fire
            // notify_waiters() when it promotes the job.
            self.job_available.notify_waiters();
        }
        if became_dead {
            // Retries exhausted: terminal transition. Wake completion waiters.
            self.completion_notify.notify_waiters();
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
        job.failed_at = Some(self.now_ms());
        job.claimed_at = None;
        job.lease_expires_at = None;
        let dead = dead_key(&job.queue, &job.id);
        let value = rmp_serde::to_vec_named(&job)?;

        loop {
            let txn = self.db.begin(IsolationLevel::Snapshot).await?;
            txn.delete(claimed.as_bytes())?;
            txn.put(dead.as_bytes(), &value)?;
            txn.put(job_index_key(&job.id).as_bytes(), dead.as_bytes())?;
            update_stats(
                &txn,
                &job.queue,
                &[(JobStatus::Claimed, -1), (JobStatus::Dead, 1)],
            )?;
            match txn.commit().await {
                Ok(_) => break,
                Err(e) if e.kind() == slatedb::ErrorKind::Transaction => continue,
                Err(e) => return Err(e.into()),
            }
        }

        self.clear_cancel_token(&job.id);
        self.completion_notify.notify_waiters();
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
        // Revival clears any prior cancel request: the operator chose to
        // start this job afresh.
        job.cancel_requested = false;
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

        let new_expiry = self.now_ms() + extension.as_millis() as u64;
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

    /// Wait until the given job reaches a terminal state, or until
    /// `timeout` elapses.
    ///
    /// Wake-up is notification-based: every terminal transition in the
    /// queue (`ack`, `nack` past `max_attempts`, `dead_letter`,
    /// `cancel`-Removed, reaper dead-letter) fires a shared
    /// [`tokio::sync::Notify`] that this method listens on. There is no
    /// per-job polling. Transient transitions (a `nack` that re-queues
    /// for retry, the reaper re-queuing an expired lease, the scheduler
    /// promoting a scheduled job) do **not** wake the wait: they are
    /// not terminal.
    ///
    /// See [`WaitOutcome`] for the full retention matrix that determines
    /// whether `Completed` carries a record.
    ///
    /// # Multiple waiters per job
    ///
    /// Several tasks may wait on the same job ID concurrently; each
    /// receives an equivalent outcome when the terminal transition fires.
    ///
    /// # Already-terminal jobs
    ///
    /// If the job is already terminal (`Done` with `keep_done_jobs`, or
    /// `Dead`) at call time, this returns immediately with the kept
    /// record. There is no need to subscribe before enqueueing as the
    /// pre-check covers it.
    ///
    /// # Across-process semantics
    ///
    /// The completion signal is in-process. A wait in process A on a job
    /// being worked in process B is not supported; taquba is
    /// single-process by design.
    pub async fn wait_for_completion(&self, id: &str, timeout: Duration) -> Result<WaitOutcome> {
        // Single loop. First iteration distinguishes `NotFound` (the
        // job ID was never present) from `Completed(None)` (the job
        // terminated while we waited and was not retained); subsequent
        // iterations treat `get_job == None` as the latter.
        let deadline = tokio::time::Instant::now() + timeout;
        let mut first = true;
        loop {
            // Subscribe *before* the storage check, and `enable()` the
            // future so it is registered as a waiter immediately.
            // `notify_waiters()` only wakes already-registered waiters; a
            // `Notified` that has merely been constructed (but not polled
            // or enabled) is *not* registered, so a terminal transition
            // racing the `get_job` await below would otherwise be missed
            // and the call would stall until `timeout`.
            let notified = self.completion_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            match self.get_job(id).await? {
                None if first => return Ok(WaitOutcome::NotFound),
                None => return Ok(WaitOutcome::Completed(None)),
                Some(job) if matches!(job.status, JobStatus::Done | JobStatus::Dead) => {
                    return Ok(WaitOutcome::Completed(Some(Box::new(job))));
                }
                Some(_) => {}
            }
            first = false;

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok(WaitOutcome::TimedOut);
            }
            tokio::select! {
                _ = &mut notified => {}
                _ = tokio::time::sleep(remaining) => return Ok(WaitOutcome::TimedOut),
            }
        }
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

    /// Cancel a job, handling every lifecycle state.
    ///
    /// - **`Pending` or `Scheduled`**: removes the job from the queue
    ///   immediately. Returns [`CancelOutcome::Removed`].
    /// - **`Claimed` (a worker is processing it)**: persists a
    ///   `cancel_requested` flag on the job record and fires the
    ///   in-process [`tokio_util::sync::CancellationToken`] exposed on
    ///   [`JobRecord::cancel_token`]. Returns
    ///   [`CancelOutcome::Requested`]. Workers that `select!` on the
    ///   token can short-circuit cooperatively; workers that ignore it
    ///   run to completion. The persisted flag ensures that if the
    ///   worker's lease expires and the reaper requeues the job, the
    ///   next claim's token starts pre-cancelled.
    /// - **`Done` / `Dead` / unknown**: returns [`CancelOutcome::NotFound`].
    ///
    /// Cooperative cancellation does not abort a running worker; futures
    /// cannot be safely cancelled mid-await. Watch
    /// [`JobRecord::cancel_token`] in your worker to opt in to early exit.
    pub async fn cancel(&self, id: &str) -> Result<CancelOutcome> {
        loop {
            let txn = self.db.begin(IsolationLevel::Snapshot).await?;

            let index_key = job_index_key(id);
            let current_key = match txn.get(index_key.as_bytes()).await? {
                None => {
                    txn.rollback();
                    return Ok(CancelOutcome::NotFound);
                }
                Some(bytes) => match String::from_utf8(bytes.to_vec()) {
                    Ok(s) => s,
                    Err(_) => {
                        txn.rollback();
                        return Err(Error::InvalidState);
                    }
                },
            };

            let mut job: JobRecord = match txn.get(current_key.as_bytes()).await? {
                None => {
                    txn.rollback();
                    return Ok(CancelOutcome::NotFound);
                }
                Some(bytes) => rmp_serde::from_slice(&bytes)?,
            };

            let (msg, outcome, remove_from_registry) = match job.status {
                JobStatus::Pending | JobStatus::Scheduled => {
                    let is_scheduled = matches!(job.status, JobStatus::Scheduled);
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
                    (
                        "pending/scheduled job cancelled",
                        CancelOutcome::Removed,
                        true,
                    )
                }
                JobStatus::Claimed => {
                    if job.cancel_requested {
                        // Persisted flag already set; nothing to commit. We
                        // still re-fire the in-process token below in case a
                        // new worker claim missed it.
                        txn.rollback();
                        if let Some(token) = self
                            .claimed_tokens
                            .lock()
                            .expect("claimed_tokens mutex poisoned")
                            .get(id)
                            .cloned()
                        {
                            token.cancel();
                        }
                        debug!(job_id = %id, "cancel re-requested on claimed job");
                        return Ok(CancelOutcome::Requested);
                    }
                    job.cancel_requested = true;
                    let value = rmp_serde::to_vec_named(&job)?;
                    txn.put(current_key.as_bytes(), &value)?;
                    (
                        "claimed job cancellation requested",
                        CancelOutcome::Requested,
                        false,
                    )
                }
                JobStatus::Done | JobStatus::Dead => {
                    txn.rollback();
                    return Ok(CancelOutcome::NotFound);
                }
            };

            match txn.commit().await {
                Ok(_) => {
                    // Fire (and optionally remove) any in-process token. We
                    // do this even on the Removed path: in race scenarios
                    // (lease expired + reaper requeued just before we got
                    // here), the token of a now-stale claim may still be
                    // watched by a worker; firing it lets the worker
                    // observe the cancellation cooperatively.
                    let token = {
                        let mut guard = self
                            .claimed_tokens
                            .lock()
                            .expect("claimed_tokens mutex poisoned");
                        if remove_from_registry {
                            guard.remove(id)
                        } else {
                            guard.get(id).cloned()
                        }
                    };
                    if let Some(token) = token {
                        token.cancel();
                    }
                    // Removed = terminal (job is gone). Requested = not yet
                    // terminal; the worker will fire the notify when it
                    // eventually acks / nacks / dead-letters.
                    if matches!(outcome, CancelOutcome::Removed) {
                        self.completion_notify.notify_waiters();
                    }
                    debug!(job_id = %id, "{msg}");
                    return Ok(outcome);
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
        let now = self.now_ms();

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
                cancel_requested: false,
                cancel_token: None,
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
        let count = reap_expired(&self.db, self.clock.as_ref(), &self.completion_notify).await?;
        if count > 0 {
            self.job_available.notify_waiters();
        }
        Ok(())
    }

    /// Trigger an immediate scheduled-job promotion sweep (primarily useful in tests).
    pub async fn promote_scheduled_now(&self) -> Result<()> {
        let count = promote_due_jobs(&self.db, self.clock.as_ref()).await?;
        if count > 0 {
            self.job_available.notify_waiters();
        }
        Ok(())
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
    use crate::clock::MockClock;
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
    async fn clock_accessor_returns_the_configured_clock() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        assert_eq!(q.clock().now_ms(), 1_700_000_000_000);
        clock.advance(Duration::from_secs(60));
        assert_eq!(q.clock().now_ms(), 1_700_000_060_000);

        q.close().await.unwrap();
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
    async fn test_enqueue_with_id_override_uses_supplied_id() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let returned = q
            .enqueue_with(
                "email",
                b"hello".to_vec(),
                EnqueueOptions {
                    id_override: Some("user-42-welcome".to_string()),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(returned, "user-42-welcome");

        let job = q
            .claim("email", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, "user-42-welcome");

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_enqueue_with_kv_id_override_uses_supplied_id() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let kv = HashMap::from([(b"meta/key".to_vec(), b"value".to_vec())]);
        let outcome = q
            .enqueue_with_kv(
                "email",
                b"hello".to_vec(),
                EnqueueOptions {
                    id_override: Some("custom-id-01HXYZ".to_string()),
                    ..EnqueueOptions::default()
                },
                kv,
            )
            .await
            .unwrap();
        assert_eq!(outcome, EnqueueResult::New("custom-id-01HXYZ".to_string()));

        let job = q.get_job("custom-id-01HXYZ").await.unwrap().unwrap();
        assert_eq!(job.id, "custom-id-01HXYZ");

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_enqueue_with_invalid_id_override_rejected() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let bad_ids: &[(&str, &str)] = &[
            ("", "empty"),
            ("has:colon", "delimiter"),
            ("has space", "space"),
            ("has/slash", "slash"),
        ];
        for (bad, label) in bad_ids {
            let err = q
                .enqueue_with(
                    "email",
                    b"x".to_vec(),
                    EnqueueOptions {
                        id_override: Some((*bad).to_string()),
                        ..EnqueueOptions::default()
                    },
                )
                .await
                .unwrap_err();
            assert!(
                matches!(err, Error::InvalidId { .. }),
                "expected InvalidId for {label} (id={bad:?}), got {err:?}"
            );
        }

        let too_long = "a".repeat(MAX_ID_OVERRIDE_LEN + 1);
        let err = q
            .enqueue_with(
                "email",
                b"x".to_vec(),
                EnqueueOptions {
                    id_override: Some(too_long),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidId { .. }));

        // No job should have been written for any of the rejected ids.
        assert!(
            q.claim("email", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_enqueue_without_id_override_generates_ulid() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let id = q
            .enqueue_with("email", b"hello".to_vec(), EnqueueOptions::default())
            .await
            .unwrap();
        Ulid::from_string(&id).expect("default enqueue should produce a parseable ULID");

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
            default_queue_config: QueueConfig {
                keep_done_jobs: Some(Duration::from_secs(60)),
                ..QueueConfig::default()
            },
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

    #[tokio::test(start_paused = true)]
    async fn test_done_retention_sweeps_old_jobs() {
        // `MockClock` virtualises the retention cutoff (`now_ms` reads
        // the clock instead of `SystemTime::now()`); `start_paused`
        // virtualises the reaper's `tokio::time::sleep` tick. Together,
        // the test runs in zero wall-clock time.
        let clock = MockClock::new(1_700_000_000_000);
        let reaper_interval = Duration::from_millis(10);
        let retention = Duration::from_millis(20);
        let opts = OpenOptions {
            reaper_interval,
            default_queue_config: QueueConfig {
                keep_done_jobs: Some(retention),
                ..QueueConfig::default()
            },
            clock: Arc::new(clock.clone()),
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

        clock.advance(retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;

        assert!(
            q.get_job(&id).await.unwrap().is_none(),
            "retention sweep must purge expired done jobs"
        );

        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_retention_is_per_queue_on_ack_and_sweep() {
        // Two queues sharing one Queue instance, with very different
        // retention policies. The default-config queue ("transient") drops
        // jobs on ack; the per-queue override ("kept") retains them. Then
        // the same background reaper sweep must respect each queue's window.
        let clock = MockClock::new(1_700_000_000_000);
        let reaper_interval = Duration::from_millis(10);
        let kept_retention = Duration::from_millis(50);

        let opts = OpenOptions {
            reaper_interval,
            default_queue_config: QueueConfig {
                keep_done_jobs: None,
                ..QueueConfig::default()
            },
            queue_configs: HashMap::from([(
                "kept".to_string(),
                QueueConfig {
                    keep_done_jobs: Some(kept_retention),
                    ..QueueConfig::default()
                },
            )]),
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let kept_id = q.enqueue("kept", b"a".to_vec()).await.unwrap();
        let transient_id = q.enqueue("transient", b"b".to_vec()).await.unwrap();

        let kept_job = q
            .claim("kept", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let transient_job = q
            .claim("transient", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&kept_job).await.unwrap();
        q.ack(&transient_job).await.unwrap();

        // The "transient" queue has no retention: ack dropped the record.
        assert!(
            q.get_job(&transient_id).await.unwrap().is_none(),
            "queues without keep_done_jobs must drop on ack"
        );
        // The "kept" queue has retention: ack preserved the record.
        assert!(
            q.get_job(&kept_id).await.unwrap().is_some(),
            "queues with keep_done_jobs must retain on ack"
        );

        // Fire a reaper tick before the retention window has elapsed:
        // the kept record must survive.
        tokio::time::sleep(reaper_interval * 2).await;
        assert!(
            q.get_job(&kept_id).await.unwrap().is_some(),
            "reaper sweep before retention elapses must not purge"
        );

        // Advance the test clock past the retention window; the next
        // reaper tick purges the record.
        clock.advance(kept_retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;
        assert!(
            q.get_job(&kept_id).await.unwrap().is_none(),
            "reaper sweep after retention elapses must purge"
        );

        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_dead_retention_is_per_queue() {
        // Two queues with different dead-letter retention windows. The
        // same reaper sweep purges the short-window queue's record while
        // leaving the long-window one intact.
        let clock = MockClock::new(1_700_000_000_000);
        let reaper_interval = Duration::from_millis(10);
        let ephemeral_retention = Duration::from_millis(50);

        let opts = OpenOptions {
            reaper_interval,
            default_queue_config: QueueConfig {
                dead_retention: Some(Duration::from_secs(3600)),
                ..QueueConfig::default()
            },
            queue_configs: HashMap::from([(
                "ephemeral".to_string(),
                QueueConfig {
                    dead_retention: Some(ephemeral_retention),
                    ..QueueConfig::default()
                },
            )]),
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        for queue in ["ephemeral", "durable"] {
            q.enqueue_with(
                queue,
                b"x".to_vec(),
                EnqueueOptions {
                    max_attempts: Some(1),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let job = q
                .claim(queue, Duration::from_secs(30))
                .await
                .unwrap()
                .unwrap();
            q.nack(job, "fatal").await.unwrap();
        }

        assert_eq!(q.dead_jobs("ephemeral", None, 100).await.unwrap().len(), 1);
        assert_eq!(q.dead_jobs("durable", None, 100).await.unwrap().len(), 1);

        clock.advance(ephemeral_retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;

        assert_eq!(
            q.dead_jobs("ephemeral", None, 100).await.unwrap().len(),
            0,
            "short-retention queue must be purged"
        );
        assert_eq!(
            q.dead_jobs("durable", None, 100).await.unwrap().len(),
            1,
            "long-retention queue must be untouched by the same sweep"
        );

        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_done_retention_uses_completion_time_not_enqueue_time() {
        // Both the scheduler (`run_at < now_ms`) and the retention sweep
        // (`completed_at < now_ms - retention`) compare against the queue's
        // clock, so virtualising it via `MockClock` is enough to drive
        // both deterministically.
        let initial = 1_700_000_000_000_u64;
        let clock = MockClock::new(initial);
        let reaper_interval = Duration::from_millis(10);
        let retention = Duration::from_millis(50);
        let schedule_delay = Duration::from_millis(220);
        let opts = OpenOptions {
            reaper_interval,
            default_queue_config: QueueConfig {
                keep_done_jobs: Some(retention),
                ..QueueConfig::default()
            },
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        // Construct `run_at` from the mock clock so it is comparable to
        // the queue's `now_ms` without relying on the system clock.
        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial) + schedule_delay;
        let id = q
            .enqueue_with(
                "work",
                b"weekly".to_vec(),
                EnqueueOptions {
                    run_at: Some(run_at),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Advance past the schedule, promote, claim, ack.
        clock.advance(schedule_delay + Duration::from_millis(20));
        q.promote_scheduled_now().await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        let elapsed_since_enqueue = q.now_ms().saturating_sub(job.enqueued_at);
        assert!(
            elapsed_since_enqueue > schedule_delay.as_millis() as u64,
            "enqueued_at should be well over {}ms old (was {elapsed_since_enqueue}ms)",
            schedule_delay.as_millis(),
        );
        q.ack(&job).await.unwrap();

        // Fire a reaper tick right after ack: completion is fresh
        // relative to the retention window, so the record survives even
        // though `enqueued_at` is now far older than the retention.
        tokio::time::sleep(reaper_interval * 2).await;
        let kept = q.get_job(&id).await.unwrap().expect(
            "fresh completion must survive the sweep regardless of how long ago the job was enqueued",
        );
        assert!(
            kept.completed_at.is_some(),
            "ack must stamp completed_at when keep_done_jobs is set"
        );

        // Advance past the retention window; the next reaper tick purges
        // the record.
        clock.advance(retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;
        assert!(q.get_job(&id).await.unwrap().is_none());

        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_dead_retention_sweep_boundary() {
        // Drive a job to dead-letter, then exercise both sides of the
        // retention cutoff with a single configured window: a reaper tick
        // before the cutoff has elapsed must leave the job alone; one
        // after it elapses must purge it (along with its index pointer
        // and the `dead` counter).
        let clock = MockClock::new(1_700_000_000_000);
        let reaper_interval = Duration::from_millis(10);
        let retention = Duration::from_millis(50);
        let opts = OpenOptions {
            reaper_interval,
            default_queue_config: QueueConfig {
                dead_retention: Some(retention),
                ..QueueConfig::default()
            },
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

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

        // Fire a reaper tick before the retention cutoff has elapsed:
        // the dead record must survive.
        tokio::time::sleep(reaper_interval * 2).await;
        assert_eq!(q.dead_jobs("work", None, 100).await.unwrap().len(), 1);

        // Advance the test clock past the cutoff. The next reaper tick
        // purges the record; the counter and index pointer must also be
        // cleaned up.
        clock.advance(retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;
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

        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Removed);

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
        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Removed);
        assert_eq!(q.stats("work").await.unwrap().scheduled, 0);
        assert!(q.get_job(&id).await.unwrap().is_none());

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cancel_claimed_job_fires_token() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        let token = job.cancel_token.clone().expect("claim returned a token");
        assert!(!token.is_cancelled());

        // Cooperative cancel: token fires, persisted flag is set.
        assert_eq!(q.cancel(&job.id).await.unwrap(), CancelOutcome::Requested);
        assert!(token.is_cancelled());

        // Worker can still ack normally; cancellation is cooperative.
        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cancel_terminal_job_is_not_found() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();
        // Once Done (or fully deleted on default ack), cancel is a no-op.
        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::NotFound);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cancel_persists_across_reaper_requeue() {
        // Claim -> cancel -> drop the job back to pending via the reaper
        // (lease elapsed) -> re-claim sees cancel_requested and a pre-fired token.
        //
        // Disable the auto-reaper so the cancel definitely happens while
        // the job is Claimed; trigger the requeue manually with reap_now.
        let opts = OpenOptions {
            reaper_interval: Duration::from_secs(3600),
            ..no_backoff_opts()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job1 = q
            .claim("work", Duration::from_millis(50))
            .await
            .unwrap()
            .unwrap();
        let first_token = job1.cancel_token.clone().unwrap();
        assert_eq!(q.cancel(&job1.id).await.unwrap(), CancelOutcome::Requested,);
        assert!(first_token.is_cancelled());
        assert!(
            q.get_job(&job1.id).await.unwrap().unwrap().cancel_requested,
            "cancel_requested must persist on the claimed record",
        );

        // Force lease expiry, then trigger the reaper.
        tokio::time::sleep(Duration::from_millis(100)).await;
        q.reap_now().await.unwrap();

        let job2 = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job1.id, job2.id);
        assert!(job2.cancel_requested);
        let second_token = job2
            .cancel_token
            .clone()
            .expect("re-claim returned a token");
        assert!(
            second_token.is_cancelled(),
            "re-claim should surface a pre-cancelled token",
        );

        q.ack(&job2).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cancel_token_used_in_worker_select() {
        // Verify a worker can `select!` on the token to short-circuit a slow
        // tool invocation.
        let q = Queue::open(make_store(), "test").await.unwrap();
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let token = job.cancel_token.clone().unwrap();

        // External cooperative cancel.
        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Requested);

        // Worker-side: short-circuit on token.
        let took_path = tokio::select! {
            biased;
            _ = token.cancelled() => "cancelled",
            _ = tokio::time::sleep(Duration::from_secs(5)) => "slept",
        };
        assert_eq!(took_path, "cancelled");

        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wait_for_completion_unknown_id_is_not_found() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let outcome = q
            .wait_for_completion("does-not-exist", Duration::from_millis(50))
            .await
            .unwrap();
        assert!(matches!(outcome, WaitOutcome::NotFound), "{outcome:?}");
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wait_for_completion_pending_times_out() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let outcome = q
            .wait_for_completion(&id, Duration::from_millis(100))
            .await
            .unwrap();
        assert!(matches!(outcome, WaitOutcome::TimedOut), "{outcome:?}");
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wait_for_completion_wakes_on_ack() {
        // Default config does not keep done jobs: ack deletes the record.
        // Caller still sees `Completed` because the wait observes the
        // index entry disappearing.
        let q = Arc::new(Queue::open(make_store(), "test").await.unwrap());
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        let waiter_q = q.clone();
        let waiter_id = id.clone();
        let waiter = tokio::spawn(async move {
            waiter_q
                .wait_for_completion(&waiter_id, Duration::from_secs(5))
                .await
                .unwrap()
        });

        // Give the waiter a moment to subscribe.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();

        // Default ack deletes the record outright, so no inner record.
        assert!(
            matches!(waiter.await.unwrap(), WaitOutcome::Completed(None)),
            "expected Completed(None) on default ack",
        );
        assert!(q.get_job(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_wait_for_completion_with_kept_done_jobs() {
        // When `keep_done_jobs` is set, the terminal `Done` record is
        // retrievable via `get_job` after the wait returns.
        let base = no_backoff_opts();
        let opts = OpenOptions {
            default_queue_config: QueueConfig {
                keep_done_jobs: Some(Duration::from_secs(60)),
                ..base.default_queue_config.clone()
            },
            ..base
        };
        let q = Arc::new(
            Queue::open_with_options(make_store(), "test", opts)
                .await
                .unwrap(),
        );
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        let waiter_q = q.clone();
        let waiter_id = id.clone();
        let waiter = tokio::spawn(async move {
            waiter_q
                .wait_for_completion(&waiter_id, Duration::from_secs(5))
                .await
                .unwrap()
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();

        match waiter.await.unwrap() {
            WaitOutcome::Completed(Some(record)) => {
                assert_eq!(record.id, id);
                assert_eq!(record.status, JobStatus::Done);
            }
            other => panic!("expected Completed(Some(Done)), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_wait_for_completion_wakes_on_dead_letter() {
        let q = Arc::new(Queue::open(make_store(), "test").await.unwrap());
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        let waiter_q = q.clone();
        let waiter_id = id.clone();
        let waiter = tokio::spawn(async move {
            waiter_q
                .wait_for_completion(&waiter_id, Duration::from_secs(5))
                .await
                .unwrap()
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.dead_letter(job, "permanent").await.unwrap();

        match waiter.await.unwrap() {
            WaitOutcome::Completed(Some(record)) => {
                assert_eq!(record.id, id);
                assert_eq!(record.status, JobStatus::Dead);
                assert_eq!(record.last_error.as_deref(), Some("permanent"));
            }
            other => panic!("expected Completed(Some(Dead)), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_wait_for_completion_wakes_on_cancel_removed() {
        let q = Arc::new(Queue::open(make_store(), "test").await.unwrap());
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        let waiter_q = q.clone();
        let waiter_id = id.clone();
        let waiter = tokio::spawn(async move {
            waiter_q
                .wait_for_completion(&waiter_id, Duration::from_secs(5))
                .await
                .unwrap()
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Removed);

        // Cancel of Pending removes the record outright.
        assert!(
            matches!(waiter.await.unwrap(), WaitOutcome::Completed(None)),
            "expected Completed(None) after Pending cancel",
        );
        assert!(q.get_job(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_wait_for_completion_does_not_wake_on_cancel_requested() {
        // A `Claimed` cancel fires the token but the job is still in flight;
        // `wait_for_completion` should keep waiting until the worker
        // actually settles the claim.
        let q = Arc::new(Queue::open(make_store(), "test").await.unwrap());
        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let id = job.id.clone();

        let waiter_q = q.clone();
        let waiter_id = id.clone();
        let waiter = tokio::spawn(async move {
            waiter_q
                .wait_for_completion(&waiter_id, Duration::from_millis(200))
                .await
                .unwrap()
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Requested);

        assert!(
            matches!(waiter.await.unwrap(), WaitOutcome::TimedOut),
            "claimed cancel should not wake the completion waiter",
        );
        q.ack(&job).await.unwrap();
    }

    #[tokio::test]
    async fn test_wait_for_completion_returns_immediately_when_already_terminal() {
        // Job is already Dead before any waiter calls in. The pre-check
        // path should return Completed(Some(Dead)) without subscribing.
        let q = Queue::open(make_store(), "test").await.unwrap();
        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let id = job.id.clone();
        q.dead_letter(job, "permanent").await.unwrap();

        // Even with a zero timeout, the already-terminal case must return.
        match q
            .wait_for_completion(&id, Duration::from_millis(0))
            .await
            .unwrap()
        {
            WaitOutcome::Completed(Some(record)) => {
                assert_eq!(record.id, id);
                assert_eq!(record.status, JobStatus::Dead);
            }
            other => panic!("expected Completed(Some(Dead)), got {other:?}"),
        }
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wait_for_completion_fan_out_to_multiple_waiters() {
        // Several waiters on the same job all wake on a single terminal
        // transition.
        let q = Arc::new(Queue::open(make_store(), "test").await.unwrap());
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        let mut waiters = Vec::new();
        for _ in 0..4 {
            let q = q.clone();
            let id = id.clone();
            waiters.push(tokio::spawn(async move {
                q.wait_for_completion(&id, Duration::from_secs(5))
                    .await
                    .unwrap()
            }));
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.dead_letter(job, "permanent").await.unwrap();

        for waiter in waiters {
            match waiter.await.unwrap() {
                WaitOutcome::Completed(Some(record)) => {
                    assert_eq!(record.id, id);
                    assert_eq!(record.status, JobStatus::Dead);
                }
                other => panic!("waiter saw {other:?}, expected Completed(Some(Dead))"),
            }
        }
    }

    #[tokio::test]
    async fn test_wait_for_completion_wakes_on_reaper_dead_letter() {
        // Disable auto-reaper so we control the timing precisely.
        let opts = OpenOptions {
            reaper_interval: Duration::from_secs(3600),
            default_queue_config: QueueConfig {
                max_attempts: 1,
                retry_backoff_base: Duration::ZERO,
                retry_backoff_max: Duration::ZERO,
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let q = Arc::new(
            Queue::open_with_options(make_store(), "test", opts)
                .await
                .unwrap(),
        );
        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_millis(10))
            .await
            .unwrap()
            .unwrap();
        let id = job.id.clone();
        drop(job);

        let waiter_q = q.clone();
        let waiter_id = id.clone();
        let waiter = tokio::spawn(async move {
            waiter_q
                .wait_for_completion(&waiter_id, Duration::from_secs(5))
                .await
                .unwrap()
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        q.reap_now().await.unwrap();

        match waiter.await.unwrap() {
            WaitOutcome::Completed(Some(record)) => {
                assert_eq!(record.id, id);
                assert_eq!(record.status, JobStatus::Dead);
                assert_eq!(record.last_error.as_deref(), Some("lease expired"));
            }
            other => panic!("expected Completed(Some(Dead)), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_cancel_nonexistent_is_not_found() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        assert_eq!(
            q.cancel("does-not-exist").await.unwrap(),
            CancelOutcome::NotFound,
        );
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

    #[tokio::test]
    async fn test_enqueue_with_kv_new_writes_apply() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let mut kv = HashMap::new();
        kv.insert(b"runs/abc".to_vec(), b"submitted".to_vec());

        let outcome = q
            .enqueue_with_kv("work", b"payload".to_vec(), EnqueueOptions::default(), kv)
            .await
            .unwrap();
        let id = match outcome {
            EnqueueResult::New(id) => id,
            other => panic!("expected New, got {other:?}"),
        };

        let s = q.stats("work").await.unwrap();
        assert_eq!(s.pending, 1);

        let claimed = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.id, id);
        assert_eq!(claimed.payload, b"payload");

        let v = q.kv_get(b"runs/abc").await.unwrap();
        assert_eq!(v.as_deref(), Some(b"submitted".as_slice()));

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_enqueue_with_kv_dedup_hit_skips_kv_writes() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let first_outcome = q
            .enqueue_with_kv(
                "work",
                b"first".to_vec(),
                EnqueueOptions {
                    dedup_key: Some("run-abc".into()),
                    ..Default::default()
                },
                HashMap::from([(b"runs/abc".to_vec(), b"first-record".to_vec())]),
            )
            .await
            .unwrap();
        let first_id = match first_outcome {
            EnqueueResult::New(id) => id,
            other => panic!("expected New, got {other:?}"),
        };

        let second_outcome = q
            .enqueue_with_kv(
                "work",
                b"second".to_vec(),
                EnqueueOptions {
                    dedup_key: Some("run-abc".into()),
                    ..Default::default()
                },
                HashMap::from([(b"runs/abc".to_vec(), b"second-record".to_vec())]),
            )
            .await
            .unwrap();
        match second_outcome {
            EnqueueResult::AlreadyEnqueued(id) => assert_eq!(id, first_id),
            other => panic!("expected AlreadyEnqueued, got {other:?}"),
        }

        // Only one job was enqueued.
        let s = q.stats("work").await.unwrap();
        assert_eq!(s.pending, 1);

        // First write applied; second was a dedup hit so it did NOT
        // overwrite the KV value.
        let v = q.kv_get(b"runs/abc").await.unwrap();
        assert_eq!(v.as_deref(), Some(b"first-record".as_slice()));

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_enqueue_with_kv_rejects_oversized_value() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let oversized = vec![0u8; MAX_KV_VALUE_SIZE + 1];
        let err = q
            .enqueue_with_kv(
                "work",
                b"x".to_vec(),
                EnqueueOptions::default(),
                HashMap::from([(b"big".to_vec(), oversized)]),
            )
            .await
            .unwrap_err();
        match err {
            Error::KvValueTooLarge { size, max } => {
                assert_eq!(size, MAX_KV_VALUE_SIZE + 1);
                assert_eq!(max, MAX_KV_VALUE_SIZE);
            }
            other => panic!("expected KvValueTooLarge, got {other:?}"),
        }
        // Nothing enqueued: validation runs before the transaction.
        assert_eq!(q.stats("work").await.unwrap().pending, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kv_keys_cannot_collide_with_internal_layout() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        // Enqueue a real job so the internal `pending:` keyspace is in use.
        q.enqueue("work", b"payload".to_vec()).await.unwrap();

        // A user key that *looks* like an internal prefix is scoped under
        // `usr:` and cannot interfere with queue state.
        q.enqueue_with_kv(
            "other",
            b"sentinel".to_vec(),
            EnqueueOptions::default(),
            HashMap::from([(
                b"pending:work:0000000001:fake-id".to_vec(),
                b"trickery".to_vec(),
            )]),
        )
        .await
        .unwrap();

        // The original job is still claimable from the original queue.
        let s = q.stats("work").await.unwrap();
        assert_eq!(s.pending, 1);
        let claimed = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.payload, b"payload");

        // The user-visible key still reads back fine.
        let v = q.kv_get(b"pending:work:0000000001:fake-id").await.unwrap();
        assert_eq!(v.as_deref(), Some(b"trickery".as_slice()));

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kv_delete_removes_value() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        q.enqueue_with_kv(
            "work",
            b"x".to_vec(),
            EnqueueOptions::default(),
            HashMap::from([(b"runs/xyz".to_vec(), b"active".to_vec())]),
        )
        .await
        .unwrap();

        assert_eq!(
            q.kv_get(b"runs/xyz").await.unwrap().as_deref(),
            Some(b"active".as_slice())
        );

        q.kv_delete(b"runs/xyz").await.unwrap();
        assert!(q.kv_get(b"runs/xyz").await.unwrap().is_none());

        q.close().await.unwrap();
    }
}
