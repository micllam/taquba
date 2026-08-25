use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use slatedb::config::{ScanOptions, Settings};
use slatedb::object_store::ObjectStore;
use slatedb::{Db, DbTransaction, IsolationLevel};
use tracing::{debug, instrument, warn};
use ulid::Ulid;

use crate::background::BackgroundTask;
use crate::claim_cursor::ClaimCursor;
use crate::clock::{Clock, default_clock};
use crate::completion::CompletionWaiters;
use crate::effects::{PreparedEffects, PreparedJob};
use crate::error::{Error, Result};
use crate::history::{AttemptOutcome, JobAttempt, append_attempt};
use crate::job::{Claim, JobRecord, JobStatus};
use crate::keys::{
    MAX_QUEUE_NAME_LEN, attempt_history_key, claimed_key, dead_key, dedup_index_key, job_index_key,
    pending_prefix, user_scoped_key,
};
use crate::lease_registry::{LeaseRegistry, Renewal};
use crate::payload_store::PayloadStore;
use crate::queue_core::{QueueConfigs, QueueCore};
use crate::reaper::Reaper;
use crate::scheduler::Scheduler;
use crate::stats::{QueueMergeOperator, QueueStats, update_stats};
use crate::txn::ClaimEnd;
use crate::txn::{
    Commit, Durability, commit, get_indexed_job, put_job_record, stage_claim_end, stage_to_pending,
    take_claim,
};

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
    /// [`Claim::cancel_token`] has been fired. The worker is still
    /// running and will eventually `ack` / `nack` / `dead_letter` the
    /// job according to its own logic.
    Requested,
    /// No job with this ID was found, or it was already in a terminal
    /// state (`Done` / `Dead`).
    NotFound,
}

/// Outcome of [`Queue::nack_with`], reflecting which settlement branch
/// the failure took.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NackOutcome {
    /// Attempts remained, so the job was re-queued (immediately or
    /// after backoff) and the effects were discarded.
    Retried,
    /// Attempts were exhausted, so the job was dead-lettered and the
    /// effects were applied. The results align index-wise with the
    /// effects' enqueues.
    DeadLettered(Vec<EnqueueResult>),
}

/// Outcome of [`Queue::wake_scheduled`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeOutcome {
    /// The job was `Scheduled` and has been moved to pending. It is
    /// claimable immediately.
    Woken,
    /// A job with this ID exists but is not `Scheduled` (it is pending,
    /// claimed, done or dead). Nothing was changed.
    NotScheduled,
    /// No job with this ID was found.
    NotFound,
}

/// One page of a job listing. Returned by [`Queue::list_jobs`].
#[derive(Debug, Clone)]
pub struct JobPage {
    /// Jobs on this page, in the scan order of the listed state's key
    /// space (see [`Queue::list_jobs`]).
    pub jobs: Vec<JobRecord>,
    /// Opaque resume token: pass it as the `cursor` of the next
    /// [`Queue::list_jobs`] call to continue the listing. `None` when no
    /// further entries existed at scan time.
    pub next_cursor: Option<Vec<u8>>,
}

/// One page of a user KV listing. Returned by [`Queue::kv_scan`].
#[derive(Debug, Clone)]
pub struct KvPage {
    /// Entries on this page as `(key, value)` pairs, in ascending byte
    /// order of the keys. Keys are in the caller namespace, without the
    /// internal user key tag.
    pub entries: Vec<(Vec<u8>, Bytes)>,
    /// Opaque resume token: pass it as the `cursor` of the next
    /// [`Queue::kv_scan`] call to continue the listing. `None` when no
    /// further entries existed at scan time.
    pub next_cursor: Option<Vec<u8>>,
}

/// High-priority bucket. Jobs at this priority are dequeued before normal and low.
pub const PRIORITY_HIGH: u32 = 100;
/// Default priority. FIFO ordering is preserved within the same priority level.
pub const PRIORITY_NORMAL: u32 = 1_000;
/// Low-priority bucket. Jobs at this priority are dequeued after high and normal.
pub const PRIORITY_LOW: u32 = 10_000;

/// Maximum size of a single value in the user KV namespace.
///
/// The KV namespace is sized for coordination state (pointers, status
/// markers, dedup records, small lifecycle records), not bulk payload.
/// Values exceeding this cap return [`Error::KvValueTooLarge`].
///
/// Store large blobs in the underlying [`ObjectStore`] under a
/// content-addressed key and put only the pointer in KV.
pub const MAX_KV_VALUE_SIZE: usize = 256 * 1024;

/// Validate a user KV value against [`MAX_KV_VALUE_SIZE`].
pub(crate) fn validate_kv_value_size(value: &[u8]) -> Result<()> {
    if value.len() > MAX_KV_VALUE_SIZE {
        return Err(Error::KvValueTooLarge {
            size: value.len(),
            max: MAX_KV_VALUE_SIZE,
        });
    }
    Ok(())
}

/// Validate a queue name against the key encoding's one-byte length
/// field. Called at every public entry point that accepts a queue name.
pub(crate) fn validate_queue_name(queue: &str) -> Result<()> {
    if queue.len() > MAX_QUEUE_NAME_LEN {
        return Err(Error::InvalidQueueName {
            queue: queue.to_string(),
            reason: "queue name exceeds the maximum length of 255 bytes",
        });
    }
    Ok(())
}

/// Default value of [`OpenOptions::payload_offload_threshold`]: payloads
/// larger than this are stored as objects in the payload object store
/// instead of inline in the job record.
pub const DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD: usize = 256 * 1024;

/// Maximum byte length of a caller-supplied
/// [`EnqueueOptions::id_override`]. Enforces a sane cap on key sizes
/// independently of the underlying object store's path limits.
const MAX_ID_OVERRIDE_LEN: usize = 128;

/// Validate a caller-supplied job id. Caller-supplied ids must be
/// 1-[`MAX_ID_OVERRIDE_LEN`] bytes of `[A-Za-z0-9_-]`, keeping ids safe
/// for object-store paths and log lines downstream.
pub(crate) fn validate_id_override(id: &str) -> Result<()> {
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

    /// The id of the underlying job, by value.
    pub fn into_id(self) -> String {
        match self {
            Self::New(id) | Self::AlreadyEnqueued(id) => id,
        }
    }
}

/// Generate a claim token. A ULID's low 64 bits fall inside its 80-bit
/// random component, so tokens are distinct across claims of the same
/// job. The value identifies a claim and is not ordered, so it fences
/// only against this queue's own state and is not a fencing token for
/// anything outside it.
fn new_claim_token() -> u64 {
    Ulid::new().0 as u64
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
/// QueueConfig::default().max_attempts(10)
/// ```
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct QueueConfig {
    /// Maximum delivery attempts before a job is dead-lettered. Attempts
    /// count claims: a job interrupted by a process restart is requeued
    /// at the next open, and its next claim consumes an attempt.
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
    /// done key space and retained for `duration`. The reaper purges them
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

impl QueueConfig {
    /// Set [`Self::max_attempts`].
    #[must_use]
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Set [`Self::lease_duration`].
    #[must_use]
    pub fn lease_duration(mut self, lease_duration: Duration) -> Self {
        self.lease_duration = lease_duration;
        self
    }

    /// Set [`Self::default_priority`].
    #[must_use]
    pub fn default_priority(mut self, default_priority: u32) -> Self {
        self.default_priority = default_priority;
        self
    }

    /// Set [`Self::retry_backoff_base`].
    #[must_use]
    pub fn retry_backoff_base(mut self, retry_backoff_base: Duration) -> Self {
        self.retry_backoff_base = retry_backoff_base;
        self
    }

    /// Set [`Self::retry_backoff_max`].
    #[must_use]
    pub fn retry_backoff_max(mut self, retry_backoff_max: Duration) -> Self {
        self.retry_backoff_max = retry_backoff_max;
        self
    }

    /// Set [`Self::keep_done_jobs`].
    #[must_use]
    pub fn keep_done_jobs(mut self, keep_done_jobs: impl Into<Option<Duration>>) -> Self {
        self.keep_done_jobs = keep_done_jobs.into();
        self
    }

    /// Set [`Self::dead_retention`].
    #[must_use]
    pub fn dead_retention(mut self, dead_retention: impl Into<Option<Duration>>) -> Self {
        self.dead_retention = dead_retention.into();
        self
    }
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
#[non_exhaustive]
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
    /// Override SlateDB's WAL flush interval. `None` keeps SlateDB's
    /// own default.
    ///
    /// The transitions that await durability (`enqueue`, `ack`,
    /// `nack`, `dead_letter`) block until the next flush tick, so this
    /// value is the lower bound on their per-operation latency.
    /// `claim` and the background sweeps commit without awaiting the
    /// flush and are not bound by it.
    ///
    /// Does not affect durability semantics: the awaiting transitions
    /// wait for the flush whatever the interval is, and a non-awaiting
    /// transition lost in a crash is redone on recovery, so
    /// at-least-once delivery is preserved regardless of the interval
    /// chosen.
    pub flush_interval: Option<Duration>,
    /// How often the background metrics sampler reads per-queue depth and
    /// the oldest-pending age and emits them as gauges. `None` (the default)
    /// disables the sampler. Has no effect unless the crate is built with the
    /// `metrics` feature; event counters and latency histograms are emitted
    /// inline regardless of this setting.
    pub metrics_sample_interval: Option<Duration>,
    /// Interval on which the writer commits a liveness heartbeat that
    /// [`crate::QueueReader::writer_heartbeat`] reads from another
    /// process. `None` (the default) writes no heartbeat.
    ///
    /// A beat is an ordinary store commit, so a fresh beat proves the
    /// process that owns the store is alive; it proves nothing about
    /// that process's workers. A writer that lost the store to a
    /// successor stops producing observable beats at its next flush,
    /// and each failed beat is logged at error level and counted as
    /// `taquba_heartbeat_failures_total` (`metrics` feature). The first
    /// beat is committed during open, and a clean [`Queue::close`]
    /// commits a final beat marked closed, so a stale closed beat
    /// indicates a deliberate shutdown rather than a vanished writer.
    /// The steady-state cost is one durable commit per interval, whose
    /// WAL, L0 and compaction churn is negligible.
    ///
    /// A beat awaits durability, so successive beats land the interval
    /// plus one commit latency apart. Choose an interval well above
    /// [`Self::flush_interval`], so the cadence readers observe stays
    /// close to the declared interval by which they judge staleness.
    pub liveness_heartbeat: Option<Duration>,
    /// Payload size in bytes above which an enqueued payload is offloaded:
    /// written once as an object in the payload object store, with the
    /// record storing [`JobRecord::payload_ref`] instead of inline bytes.
    /// State transitions then rewrite only the small record, and claims
    /// fetch the payload from the object store. Defaults to
    /// [`DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD`]; `None` disables offloading,
    /// keeping every payload inline regardless of size.
    pub payload_offload_threshold: Option<usize>,
    /// Object store for offloaded payloads. `None` (the default) uses the
    /// object store the queue is opened on. Configuring a separate store
    /// places payload bytes in a different bucket or account from the
    /// queue's own state.
    pub payload_store: Option<Arc<dyn ObjectStore>>,
    /// Path prefix for offloaded payload objects within the payload
    /// object store. `None` (the default) uses `"{path}-payloads"`, a
    /// sibling of the path the queue is opened at, which cannot overlap
    /// SlateDB's own layout. A custom value that shares the object
    /// store with the queue must not equal or nest within the queue's
    /// `path`.
    pub payload_path: Option<String>,
}

impl OpenOptions {
    /// Set [`Self::reaper_interval`].
    #[must_use]
    pub fn reaper_interval(mut self, reaper_interval: Duration) -> Self {
        self.reaper_interval = reaper_interval;
        self
    }

    /// Set [`Self::scheduler_interval`].
    #[must_use]
    pub fn scheduler_interval(mut self, scheduler_interval: Duration) -> Self {
        self.scheduler_interval = scheduler_interval;
        self
    }

    /// Set [`Self::default_queue_config`].
    #[must_use]
    pub fn default_queue_config(mut self, default_queue_config: QueueConfig) -> Self {
        self.default_queue_config = default_queue_config;
        self
    }

    /// Set the configuration of one queue in [`Self::queue_configs`].
    #[must_use]
    pub fn queue_config(mut self, queue: impl Into<String>, config: QueueConfig) -> Self {
        self.queue_configs.insert(queue.into(), config);
        self
    }

    /// Set [`Self::queue_configs`].
    #[must_use]
    pub fn queue_configs(mut self, queue_configs: HashMap<String, QueueConfig>) -> Self {
        self.queue_configs = queue_configs;
        self
    }

    /// Set [`Self::clock`].
    #[must_use]
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Set [`Self::flush_interval`].
    #[must_use]
    pub fn flush_interval(mut self, flush_interval: impl Into<Option<Duration>>) -> Self {
        self.flush_interval = flush_interval.into();
        self
    }

    /// Set [`Self::metrics_sample_interval`].
    #[must_use]
    pub fn metrics_sample_interval(
        mut self,
        metrics_sample_interval: impl Into<Option<Duration>>,
    ) -> Self {
        self.metrics_sample_interval = metrics_sample_interval.into();
        self
    }

    /// Set [`Self::liveness_heartbeat`].
    #[must_use]
    pub fn liveness_heartbeat(mut self, liveness_heartbeat: impl Into<Option<Duration>>) -> Self {
        self.liveness_heartbeat = liveness_heartbeat.into();
        self
    }

    /// Set [`Self::payload_offload_threshold`].
    #[must_use]
    pub fn payload_offload_threshold(
        mut self,
        payload_offload_threshold: impl Into<Option<usize>>,
    ) -> Self {
        self.payload_offload_threshold = payload_offload_threshold.into();
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
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            reaper_interval: Duration::from_secs(5),
            scheduler_interval: Duration::from_secs(1),
            default_queue_config: QueueConfig::default(),
            queue_configs: HashMap::new(),
            clock: default_clock(),
            flush_interval: None,
            metrics_sample_interval: None,
            liveness_heartbeat: None,
            payload_offload_threshold: Some(DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD),
            payload_store: None,
            payload_path: None,
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
/// let opts = EnqueueOptions::default().run_at(SystemTime::now() + Duration::from_secs(60));
/// ```
#[non_exhaustive]
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
    /// Duplicate caller-supplied ids are rejected with
    /// [`Error::DuplicateJobId`] while the existing job is still indexed.
    /// ULID generation guarantees uniqueness for the `None` path.
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

impl EnqueueOptions {
    /// Set [`Self::max_attempts`].
    #[must_use]
    pub fn max_attempts(mut self, max_attempts: impl Into<Option<u32>>) -> Self {
        self.max_attempts = max_attempts.into();
        self
    }

    /// Set [`Self::priority`].
    #[must_use]
    pub fn priority(mut self, priority: impl Into<Option<u32>>) -> Self {
        self.priority = priority.into();
        self
    }

    /// Set [`Self::run_at`].
    #[must_use]
    pub fn run_at(mut self, run_at: impl Into<Option<std::time::SystemTime>>) -> Self {
        self.run_at = run_at.into();
        self
    }

    /// Set [`Self::dedup_key`].
    #[must_use]
    pub fn dedup_key(mut self, dedup_key: impl Into<Option<String>>) -> Self {
        self.dedup_key = dedup_key.into();
        self
    }

    /// Set [`Self::headers`].
    #[must_use]
    pub fn headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers = headers;
        self
    }

    /// Set one entry of [`Self::headers`].
    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// Set [`Self::id_override`].
    #[must_use]
    pub fn id_override(mut self, id_override: impl Into<Option<String>>) -> Self {
        self.id_override = id_override.into();
        self
    }
}

/// One enqueue carried by [`SettlementEffects`].
#[derive(Debug, Clone)]
pub struct EnqueueRequest {
    /// Queue the job is enqueued on.
    pub queue: String,
    /// Job payload.
    pub payload: Vec<u8>,
    /// Per-job options; `run_at`, `dedup_key`, `priority`, and
    /// `id_override` are all honoured exactly as in
    /// [`Queue::enqueue_with`].
    pub options: EnqueueOptions,
}

/// Effects applied in the same transaction as a settlement: an
/// acknowledgement via [`Queue::ack_with`], a dead-letter via
/// [`Queue::dead_letter_with`] or [`Queue::nack_with`], or a
/// pending-job removal via [`Queue::cancel_with`]. Either the
/// settlement and every effect commit together or nothing does. A
/// branch that applies no effects ([`Queue::nack_with`] while attempts
/// remain, [`Queue::cancel_with`] other than
/// [`CancelOutcome::Removed`]) commits without them. A key named in
/// both `kv_writes` and `kv_deletes` is rejected with
/// [`Error::ConflictingKvEffect`].
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct SettlementEffects {
    /// Jobs enqueued atomically with the settlement.
    pub enqueues: Vec<EnqueueRequest>,
    /// Writes applied to the caller KV namespace, as in
    /// [`Queue::enqueue_with_kv`]. Values are size-capped at
    /// [`MAX_KV_VALUE_SIZE`].
    pub kv_writes: HashMap<Vec<u8>, Vec<u8>>,
    /// Keys deleted from the caller KV namespace.
    pub kv_deletes: Vec<Vec<u8>>,
}

impl SettlementEffects {
    /// Set [`Self::enqueues`].
    #[must_use]
    pub fn enqueues(mut self, enqueues: Vec<EnqueueRequest>) -> Self {
        self.enqueues = enqueues;
        self
    }

    /// Add one request to [`Self::enqueues`].
    #[must_use]
    pub fn enqueue(mut self, request: EnqueueRequest) -> Self {
        self.enqueues.push(request);
        self
    }

    /// Set [`Self::kv_writes`].
    #[must_use]
    pub fn kv_writes(mut self, kv_writes: HashMap<Vec<u8>, Vec<u8>>) -> Self {
        self.kv_writes = kv_writes;
        self
    }

    /// Add one write to [`Self::kv_writes`].
    #[must_use]
    pub fn kv_put(mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        self.kv_writes.insert(key.into(), value.into());
        self
    }

    /// Set [`Self::kv_deletes`].
    #[must_use]
    pub fn kv_deletes(mut self, kv_deletes: Vec<Vec<u8>>) -> Self {
        self.kv_deletes = kv_deletes;
        self
    }

    /// Add one key to [`Self::kv_deletes`].
    #[must_use]
    pub fn kv_delete(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.kv_deletes.push(key.into());
        self
    }
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
/// Background tasks run while the queue is open:
///
/// - **Reaper**: re-queues or dead-letters jobs whose lease has expired and
///   runs the done and dead-letter retention sweeps
///   ([`OpenOptions::reaper_interval`]).
/// - **Scheduler**: promotes jobs whose `run_at` has passed from the
///   scheduled state to pending ([`OpenOptions::scheduler_interval`]).
/// - **Metrics sampler**, when [`OpenOptions::metrics_sample_interval`] is
///   set: emits per-queue depth and oldest-pending-age gauges.
/// - **Liveness heartbeat**, when [`OpenOptions::liveness_heartbeat`] is
///   set: commits a beat that a [`QueueReader`](crate::QueueReader) reads
///   from another process.
///
/// # Concurrency
///
/// `Queue` is `Send + Sync` and cheap to clone behind an [`Arc`]. All workers must run
/// in the same process: SlateDB's single-writer constraint means the queue cannot be
/// shared across processes.
pub struct Queue {
    pub(crate) core: Arc<QueueCore>,
    reaper: Arc<Reaper>,
    reaper_task: BackgroundTask,
    scheduler: Arc<Scheduler>,
    scheduler_task: BackgroundTask,
    /// `Some` only when built with the `metrics` feature and
    /// `OpenOptions::metrics_sample_interval` was set.
    metrics_sampler: Option<BackgroundTask>,
    /// `Some` only when `OpenOptions::liveness_heartbeat` was set.
    /// Stopping returns the task so `close` can commit the closing
    /// beat with the task's counter.
    heartbeat: Option<BackgroundTask<crate::liveness::HeartbeatTask>>,
}

/// Outcome of [`Queue::wait_for_completion`].
///
/// The terminal variants name the transition that ended the job. A
/// transition observed while waiting delivers the final [`JobRecord`]
/// as the settlement wrote it, with the payload inline, whether or not
/// the queue retains the record afterwards:
///
/// | Transition                                             | Outcome |
/// |--------------------------------------------------------|---------|
/// | Worker `ack` (success)                                 | `Done(record)` |
/// | Worker `nack` past `max_attempts`                      | `Dead(record)` |
/// | Worker [`Queue::dead_letter`] (permanent failure)      | `Dead(record)` |
/// | Reaper dead-letter (lease expired past `max_attempts`) | `Dead(record)` |
/// | [`Queue::cancel`] removing a `Pending`/`Scheduled` job | `Cancelled` |
///
/// A job that was already terminal when the call began is reported
/// from its retained record (`Done` only under
/// [`QueueConfig::keep_done_jobs`], `Dead` always); a job whose record
/// was deleted before the call began is `NotFound`.
#[derive(Debug, Clone)]
pub enum WaitOutcome {
    /// The job was acknowledged.
    Done(Box<JobRecord>),
    /// The job was dead-lettered. The dead record is always kept.
    Dead(Box<JobRecord>),
    /// The job was removed by [`Queue::cancel`] before it was claimed.
    /// No record survives the removal.
    Cancelled,
    /// The wait elapsed before the job reached a terminal state. The
    /// job is still pending, scheduled, or claimed somewhere.
    TimedOut,
    /// No job with this ID was present at the start of the call.
    NotFound,
}

/// The committed outcome of [`Queue::settle_claim`]'s transaction.
struct SettledClaim<'e> {
    /// The record as written.
    job: JobRecord,
    end: ClaimEnd<'e>,
    /// The pending key when the job returned to pending.
    pending_key: Option<Vec<u8>>,
    /// The effects' results; `None` when the transition discarded them.
    results: Option<Vec<EnqueueResult>>,
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
        crate::obs::describe();
        let payload_store = Arc::new(PayloadStore::new(
            opts.payload_store.unwrap_or_else(|| object_store.clone()),
            opts.payload_path
                .unwrap_or_else(|| format!("{path}-payloads")),
            opts.payload_offload_threshold,
        ));
        let mut settings = Settings::default();
        if let Some(flush_interval) = opts.flush_interval {
            settings.flush_interval = Some(flush_interval);
        }
        #[cfg_attr(not(feature = "metrics"), allow(unused_mut))]
        let mut builder = Db::builder(path, object_store)
            .with_merge_operator(Arc::new(QueueMergeOperator))
            .with_settings(settings);
        #[cfg(feature = "metrics")]
        {
            builder = builder.with_metrics_recorder(crate::obs::slatedb_recorder());
        }
        let db = Arc::new(builder.build().await?);
        let core = Arc::new(QueueCore {
            db,
            clock: opts.clock,
            configs: QueueConfigs::new(opts.default_queue_config, opts.queue_configs),
            claim_cursor: ClaimCursor::new(),
            lease_registry: LeaseRegistry::new(),
            completion_waiters: Arc::new(CompletionWaiters::default()),
            payload_store,
            id_gen: std::sync::Mutex::new(ulid::Generator::new()),
        });
        crate::claim_cursor::restore_cursor_state(&core).await?;
        // A claimed record found at open belongs to a process that no
        // longer holds the store, so its claim is void and the job is
        // re-queued immediately. Runs after `restore_cursor_state` so
        // each re-queued job's pending insert is recorded against the
        // restored bound.
        crate::reaper::requeue_interrupted_claims(&core).await?;
        let reaper = Arc::new(Reaper::new(core.clone()));
        let reaper_task = BackgroundTask::spawn_periodic(opts.reaper_interval, reaper.clone());
        let scheduler = Arc::new(Scheduler::new(core.clone()));
        let scheduler_task =
            BackgroundTask::spawn_periodic(opts.scheduler_interval, scheduler.clone());

        #[cfg(feature = "metrics")]
        let metrics_sampler = opts.metrics_sample_interval.map(|interval| {
            let sampler = crate::metrics_sampler::MetricsSampler::new(core.clone());
            BackgroundTask::spawn_periodic(interval, sampler)
        });
        #[cfg(not(feature = "metrics"))]
        let metrics_sampler: Option<BackgroundTask> = None;

        let heartbeat = match opts.liveness_heartbeat {
            Some(interval) => {
                let task = crate::liveness::HeartbeatTask::start(core.clone(), interval).await?;
                Some(BackgroundTask::spawn(interval, |ticker| task.run(ticker)))
            }
            None => None,
        };

        Ok(Self {
            core,
            reaper,
            reaper_task,
            scheduler,
            scheduler_task,
            metrics_sampler,
            heartbeat,
        })
    }

    /// Current time in milliseconds since the UNIX epoch, as read
    /// from this queue's configured [`Clock`].
    pub(crate) fn now_ms(&self) -> u64 {
        self.core.now_ms()
    }

    /// Generate a job id without enqueuing anything.
    ///
    /// For callers that need the id before the enqueue commits, to write
    /// a record pointing at the job in the same transaction; pass it as
    /// [`EnqueueOptions::id_override`]. Ids increase with call order and
    /// take their timestamp from this queue's [`Clock`].
    pub fn next_job_id(&self) -> String {
        self.core.next_job_id()
    }

    pub(crate) fn queue_config(&self, queue: &str) -> &QueueConfig {
        self.core.configs.get(queue)
    }

    /// Look up the configured lease duration for a queue.
    pub fn queue_lease_duration(&self, queue: &str) -> Duration {
        self.queue_config(queue).lease_duration
    }

    /// Build the lease handle for a claim: the capability the worker
    /// loops pass to [`Worker::process`](crate::worker::Worker::process).
    /// The handle extends the lease and exposes the claim's cancellation
    /// token but cannot settle the job, so a handler never holds a claim
    /// and a queue together. Callers running `Worker::process` from
    /// their own claim loop build the handle here.
    pub fn lease_handle(&self, claim: &Claim) -> crate::lease::LeaseHandle {
        crate::lease::LeaseHandle::new(
            self.core.lease_registry.clone(),
            self.core.clock.clone(),
            claim.queue.clone(),
            claim.id.clone(),
            claim.token(),
            claim.cancel_token().clone(),
        )
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
        self.core.clock.clone()
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
    /// let opts = EnqueueOptions::default()
    ///     .priority(PRIORITY_HIGH)
    ///     .run_at(SystemTime::now() + Duration::from_secs(300))
    ///     .dedup_key("welcome:user-42".to_string());
    /// q.enqueue_with("email", b"to=alice".to_vec(), opts).await?;
    /// # Ok(()) }
    /// ```
    ///
    /// When `dedup_key` is `Some` and a pending job with the same key already
    /// exists, this returns the existing job's ID without creating a new one.
    /// When `run_at` is in the past or is now, the job is written straight to
    /// pending; otherwise it waits in the scheduled key space until the
    /// background scheduler promotes it.
    ///
    /// Queue names are limited to [`crate::MAX_QUEUE_NAME_LEN`] bytes;
    /// longer names return [`Error::InvalidQueueName`].
    #[instrument(skip(self, payload), fields(queue, job_id))]
    pub async fn enqueue_with(
        &self,
        queue: &str,
        payload: Vec<u8>,
        opts: EnqueueOptions,
    ) -> Result<String> {
        let prepared = self.core.prepare_job_record(queue, payload, opts)?;
        self.write_job(prepared, HashMap::new())
            .await
            .map(EnqueueResult::into_id)
    }

    /// Enqueue a job AND apply a set of writes to the user KV namespace
    /// in a single transaction.
    ///
    /// On success ([`EnqueueResult::New`]), the job is enqueued and every
    /// entry in `kv_writes` is applied atomically. On a `dedup_key` hit
    /// ([`EnqueueResult::AlreadyEnqueued`]), **no KV writes are applied**
    /// and the existing job's id is returned. Because a dedup hit
    /// discards `kv_writes`, derive them deterministically from the
    /// dedup key: a producer that retries after a crash then converges
    /// on the winning submission's writes rather than diverging from
    /// them. This is not an upsert; a KV write that must apply
    /// regardless of the dedup outcome belongs in [`Self::kv_put`].
    ///
    /// Caller-supplied KV keys are internally scoped under a reserved
    /// user key tag so they cannot collide with Taquba's internal layout.
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
    ///     EnqueueOptions::default().dedup_key("run:abc:0".to_string()),
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
            validate_kv_value_size(value)?;
        }

        let prepared = self.core.prepare_job_record(queue, payload, opts)?;
        self.write_job(prepared, kv_writes).await
    }

    /// Read a value from the user KV namespace.
    ///
    /// Caller-supplied keys are internally scoped under a reserved
    /// user key tag and cannot collide with Taquba's internal layout.
    pub async fn kv_get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        crate::read::kv_get(self.core.db.as_ref(), key).await
    }

    /// Write a value to the user KV namespace.
    ///
    /// Caller-supplied keys are internally scoped under a reserved
    /// user key tag and cannot collide with Taquba's internal layout.
    /// Values above [`MAX_KV_VALUE_SIZE`] return
    /// [`Error::KvValueTooLarge`]; unlike job payloads, user KV values
    /// are never offloaded to the payload store, so the cap is a hard
    /// error. Store larger values as objects under caller-owned keys
    /// and keep only the pointer in KV. The write is durable before the
    /// call returns.
    ///
    /// This is the standalone form; to couple a KV write with a queue
    /// transition in one transaction, use [`Self::enqueue_with_kv`] or
    /// [`SettlementEffects::kv_writes`] via [`Self::ack_with`].
    pub async fn kv_put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        validate_kv_value_size(value)?;
        self.core.db.put(user_scoped_key(key), value).await?;
        Ok(())
    }

    /// Delete a value from the user KV namespace.
    ///
    /// Caller-supplied keys are internally scoped under a reserved
    /// user key tag and cannot collide with Taquba's internal layout.
    pub async fn kv_delete(&self, key: &[u8]) -> Result<()> {
        self.core.db.delete(user_scoped_key(key)).await?;
        Ok(())
    }

    /// Delete a value from the user KV namespace only if its current
    /// value equals `expected`.
    ///
    /// Returns `true` when the value matched and was deleted, `false`
    /// when the key was absent or held a different value (nothing is
    /// changed in that case). The read and the delete execute in one
    /// transaction, so no concurrent write can be interleaved between
    /// the compare and the delete: either this call deletes the value
    /// it compared against, or it reports `false`. The delete is
    /// durable before the call returns `true`.
    ///
    /// Use this to consume a value that a concurrent writer may replace,
    /// where an unconditional [`Self::kv_delete`] could delete a newer
    /// value than the one read.
    pub async fn kv_compare_delete(&self, key: &[u8], expected: &[u8]) -> Result<bool> {
        self.kv_compare_then(key, Some(expected), |txn, scoped| txn.delete(scoped))
            .await
    }

    /// Write a value to the user KV namespace only if its current state
    /// matches `expected`.
    ///
    /// `expected` is the compare arm: `Some(v)` requires the key to
    /// currently hold exactly `v`; `None` requires the key to be
    /// absent. Returns `true` when the state matched and the write was
    /// applied, `false` when it did not (nothing is changed in that
    /// case). The read and the write execute in one transaction, so no
    /// concurrent write can be interleaved between the compare and the
    /// write: either this call replaces the state it compared against,
    /// or it reports `false`. The write is durable before the call
    /// returns `true`.
    ///
    /// Values above [`MAX_KV_VALUE_SIZE`] return
    /// [`Error::KvValueTooLarge`].
    ///
    /// This is the read-modify-write primitive for the namespace: read
    /// a value, compute its successor and call
    /// `kv_compare_put(key, Some(&read), &next)` in a retry loop, or
    /// claim a key exclusively with `kv_compare_put(key, None, &init)`.
    /// Transaction conflicts with concurrent writers are retried
    /// internally, but a contended key serializes its writers; state
    /// with many independent writers scales better split across
    /// multiple keys than concentrated in one key.
    pub async fn kv_compare_put(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<bool> {
        validate_kv_value_size(value)?;
        self.kv_compare_then(key, expected, |txn, scoped| txn.put(scoped, value))
            .await
    }

    /// Compare the current state of the user KV key against `expected`
    /// (`None` requires absence) and, when it matches, stage `write` in
    /// the same transaction and commit durably. Returns whether the
    /// state matched; conflicts are retried.
    async fn kv_compare_then(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        write: impl Fn(&DbTransaction, &[u8]) -> std::result::Result<(), slatedb::Error>,
    ) -> Result<bool> {
        let scoped = user_scoped_key(key);
        loop {
            let txn = self.core.db.begin(IsolationLevel::Snapshot).await?;
            let matched = match (txn.get(&scoped).await?, expected) {
                (Some(current), Some(e)) => current.as_ref() == e,
                (None, None) => true,
                _ => false,
            };
            if !matched {
                txn.rollback();
                return Ok(false);
            }
            write(&txn, &scoped)?;
            match commit(txn, Durability::Awaited).await? {
                Commit::Committed => return Ok(true),
                Commit::Conflict => continue,
            }
        }
    }

    /// List entries of the user KV namespace under `prefix`, in
    /// ascending byte order of the keys.
    ///
    /// An empty `prefix` lists the whole namespace. `cursor` is an
    /// opaque resume token: pass `None` to start from the beginning, or
    /// [`KvPage::next_cursor`] from the previous page to continue. A
    /// cursor identifies a scan position, not an entry, so it remains
    /// valid when the entry it was taken at is deleted. The listing is
    /// not a snapshot: an entry written or deleted between page reads
    /// may be missed or observed depending on its key's position
    /// relative to the cursor.
    ///
    /// Only caller-namespace entries are returned; Taquba's internal
    /// key spaces are never visible here. This is the enumeration and
    /// export primitive for the namespace: a full sweep
    /// (`prefix = b""`, follow `next_cursor` to exhaustion) observes
    /// every entry that existed for the whole sweep.
    pub async fn kv_scan(
        &self,
        prefix: &[u8],
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<KvPage> {
        crate::read::kv_scan(self.core.db.as_ref(), prefix, cursor, limit).await
    }

    /// Fetch the offloaded payloads of `jobs` concurrently, bounding a
    /// batch's wall time by the slowest object rather than the sum of
    /// the fetches. Jobs with inline payloads are untouched. On a fetch
    /// failure, one error is returned after every fetch has settled.
    async fn materialize_payloads(&self, jobs: &mut [Claim]) -> Result<()> {
        let store = &self.core.payload_store;
        let fetched =
            futures_util::future::join_all(jobs.iter_mut().map(|c| store.materialize(c.job_mut())))
                .await;
        fetched.into_iter().collect()
    }

    /// Persist a prepared [`JobRecord`], optionally checking a dedup index
    /// and caller-supplied id uniqueness, and optionally applying
    /// additional KV writes, all in a single transaction. Retries on
    /// transaction conflict.
    ///
    /// Returns [`EnqueueResult::AlreadyEnqueued`] (with **no** KV writes
    /// applied) if `job.dedup_key` is set and a pending or scheduled job
    /// with the same dedup key already exists. Returns
    /// [`Error::DuplicateJobId`] if `id_override` was used and the id is
    /// already indexed. Otherwise writes the record + job index + (when set)
    /// dedup index + every entry in `kv_writes`, and returns
    /// [`EnqueueResult::New`].
    async fn write_job(
        &self,
        mut prepared: PreparedJob,
        kv_writes: HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<EnqueueResult> {
        self.core.payload_store.offload(&mut prepared.job).await?;
        let result = self.write_job_txn(&prepared, &kv_writes).await;
        // A payload object is live only when a new record committed;
        // on a dedup downgrade or an error the record does not exist,
        // so remove the object written above.
        if !matches!(result, Ok(EnqueueResult::New(_))) {
            self.core.payload_store.delete_for(&prepared.job).await;
        }
        result
    }

    /// The transaction loop of [`Self::write_job`], after any payload
    /// offload has happened.
    async fn write_job_txn(
        &self,
        prepared: &PreparedJob,
        kv_writes: &HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<EnqueueResult> {
        let timer = crate::obs::start();
        loop {
            let txn = self.core.db.begin(IsolationLevel::Snapshot).await?;

            let staged = match self.core.stage_job_writes(&txn, prepared).await? {
                Ok(staged) => staged,
                Err(already_enqueued) => {
                    txn.rollback();
                    return Ok(EnqueueResult::AlreadyEnqueued(already_enqueued));
                }
            };

            for (k, v) in kv_writes {
                txn.put(user_scoped_key(k), v)?;
            }

            match commit(txn, Durability::Awaited).await? {
                Commit::Committed => {
                    crate::obs::enqueued(&staged.queue, 1, timer);
                    self.core.note_staged_job(&staged);
                    return Ok(EnqueueResult::New(staged.id));
                }
                Commit::Conflict => continue,
            }
        }
    }

    /// Claim the next pending job using the configured default lease duration.
    pub async fn claim_next(&self, queue: &str) -> Result<Option<Claim>> {
        let lease_duration = self.queue_config(queue).lease_duration;
        self.claim(queue, lease_duration).await
    }

    /// Block up to `max_wait` for a job to become claimable on `queue`.
    ///
    /// The wakeup is queue-scoped and delivered to one waiter per
    /// inserted job, so a pool of waiting workers does not contend on
    /// the claim path when a single job arrives. To wait on several
    /// queues at once, `select!` over one call per queue. Returning
    /// does not guarantee a job is still available
    /// (another worker may claim it first); follow up with a claim
    /// call and wait again if it returns `None`.
    pub async fn wait_for_jobs_on(&self, queue: &str, max_wait: Duration) {
        let wakeup = self.core.claim_cursor.wakeup_for(queue);
        let notified = wakeup.notified();
        tokio::pin!(notified);
        // `enable` consumes a permit left by an insert that landed
        // before this waiter subscribed, so the wait returns
        // immediately instead of sleeping past an already-available
        // job.
        notified.as_mut().enable();
        let _ = tokio::time::timeout(max_wait, notified).await;
    }

    /// Claim the next pending job, waiting up to `max_wait` for one to appear.
    ///
    /// Workers should prefer this over a polling [`Self::claim_next`] +
    /// [`tokio::time::sleep`] loop: when a job lands on `queue` (enqueue,
    /// retry requeue, dead-job requeue, scheduled-job promotion, lease
    /// reap), the wakeup is delivered via an in-memory notify so the
    /// worker resumes immediately, without waiting out the poll interval.
    /// Wakeups are queue-scoped and delivered to one waiter per inserted
    /// job, so a pool of waiting workers does not contend on the claim path
    /// when a single job arrives. Only when nothing is available within
    /// `max_wait` does the call return `None`.
    ///
    /// The `lease_duration` controls how long the resulting claim is held.
    pub async fn claim_with_wait(
        &self,
        queue: &str,
        lease_duration: Duration,
        max_wait: Duration,
    ) -> Result<Option<Claim>> {
        let deadline = tokio::time::Instant::now() + max_wait;
        loop {
            if let Some(job) = self.claim(queue, lease_duration).await? {
                // Pass the wakeup on: the wait below may have consumed a
                // permit another waiter needs, and when a backlog
                // remains each delivered job should wake one more
                // worker.
                self.core.claim_cursor.wakeup_for(queue).notify_one();
                return Ok(Some(job));
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            // An insert between the empty scan and this wait leaves a
            // permit that the wait consumes, so no insert is missed.
            // A wake does not reserve the job: another worker may claim
            // it first, in which case the loop waits out the remaining
            // time. A stale permit costs one extra pass.
            self.wait_for_jobs_on(queue, deadline - now).await;
        }
    }

    /// Claim the next pending job with an explicit lease duration.
    /// Returns `None` if the queue is empty.
    ///
    /// The claim commit does not await WAL durability. If the process
    /// crashes before the claim is flushed, the job is still pending on
    /// recovery and is redelivered immediately rather than after its
    /// lease expires; at-least-once delivery is unaffected. Any later
    /// durable commit (ack, nack, enqueue) flushes preceding WAL
    /// entries, so a settled job's claim is always durable.
    ///
    /// Same-queue claim attempts serialise through an in-process
    /// `tokio::sync::Mutex`, avoiding the transaction-conflict
    /// retry that would otherwise resolve which worker takes the
    /// head of the pending key space. The lock is per-queue, so different
    /// queues' claim paths still run in parallel.
    ///
    /// A per-queue in-memory cursor records the most recently
    /// claimed key and is used as the start bound on the next
    /// scan. This lets steady-state claims skip over the
    /// tombstones left by previously claimed (and deleted)
    /// pending entries. When the cursor scan yields nothing
    /// inside the queue's prefix (cursor exhausted, or an older
    /// job has been requeued by `nack` behind the cursor), the
    /// claim falls back to a front prefix scan and resets the
    /// cursor. When the front scan also finds nothing, the queue is
    /// marked empty in memory and subsequent claims return `None`
    /// without scanning until the next pending insert, so polling
    /// an empty queue does not re-walk the tombstone band left by
    /// previously claimed jobs.
    #[instrument(skip(self), fields(queue))]
    pub async fn claim(&self, queue: &str, lease_duration: Duration) -> Result<Option<Claim>> {
        Ok(self.claim_batch(queue, 1, lease_duration).await?.pop())
    }

    /// Claim up to `max_jobs` pending jobs in one transaction.
    ///
    /// Jobs are returned in claim order (priority, then enqueue order)
    /// and share one lease started at the same instant: size batches so
    /// the lease covers processing the whole batch, or renew leases as
    /// the batch progresses. Returns an empty `Vec` when the queue is
    /// empty and fewer than `max_jobs` jobs when the queue runs out.
    ///
    /// One batch costs one claim-lock hold, one transaction, and one
    /// commit regardless of size, so a fetcher that claims batches and
    /// dispatches jobs to local workers contends far less on a busy
    /// queue than one [`Self::claim`] call per job.
    /// [`run_worker_concurrent`](crate::run_worker_concurrent) is that
    /// pattern built in: it claims batches sized to its free capacity.
    /// Durability, serialisation, and cursor semantics are those of
    /// [`Self::claim`].
    #[instrument(skip(self), fields(queue, max_jobs))]
    pub async fn claim_batch(
        &self,
        queue: &str,
        max_jobs: usize,
        lease_duration: Duration,
    ) -> Result<Vec<Claim>> {
        validate_queue_name(queue)?;
        if max_jobs == 0 {
            return Ok(Vec::new());
        }
        // Empty check before taking the claim lock: a queue known to be
        // empty answers from in-process state without contending with
        // claims that have work to do. A stale answer here is safe in
        // both directions; emptiness is only ever revoked by an insert,
        // and a stale "not empty" just falls through to the locked scan.
        if self.core.claim_cursor.begin_claim(queue).known_empty {
            return Ok(Vec::new());
        }
        let mut jobs = {
            let lock = self.core.claim_cursor.claim_lock_for(queue);
            let _guard = lock.lock().await;
            self.claim_batch_locked(queue, max_jobs, lease_duration)
                .await?
        };
        // Offloaded payloads are fetched after the claim lock is
        // released, so other claims on the queue proceed during the
        // object-store reads. On a fetch failure the claim has already
        // committed: the affected jobs stay claimed until their leases
        // expire and are then redelivered. Their cancel tokens stay
        // registered, so a cancel during that window still fires the
        // token and persists the request.
        self.materialize_payloads(&mut jobs).await?;
        Ok(jobs)
    }

    /// The scan-and-claim transaction of [`Self::claim_batch`]. The
    /// caller holds the queue's claim lock for the duration of the
    /// call; offloaded payloads are not fetched here, so the lock is
    /// never held across a payload read.
    async fn claim_batch_locked(
        &self,
        queue: &str,
        max_jobs: usize,
        lease_duration: Duration,
    ) -> Result<Vec<Claim>> {
        let prefix = pending_prefix(queue);
        let prefix_bytes = prefix.as_slice();
        let timer = crate::obs::start();
        loop {
            // The scan state (and its pending-insert epoch) is read
            // before the transaction begins, so any insert the snapshot
            // could miss bumps the epoch after this read and revokes the
            // emptiness recorded below.
            let scan = self.core.claim_cursor.begin_claim(queue);
            if scan.known_empty {
                return Ok(Vec::new());
            }
            let txn = self.core.db.begin(IsolationLevel::Snapshot).await?;

            let mut candidates = Vec::new();
            // Set when the scan ran out of pending keys before filling
            // the batch, proving nothing is live beyond the candidates.
            let mut drained = false;
            // SlateDB leaves block caching off for scans. This scan takes at
            // most `max_jobs` entries from one prefix and the next claim
            // resumes where it stopped, so uncached it re-reads the same
            // block once a compacted sorted run overlaps the prefix.
            let scan_options = ScanOptions::default().with_cache_blocks(true);
            let mut iter = match scan.scan_from.clone() {
                // Resume from the recorded bound (after the last claimed key,
                // or a key inserted behind it). The subrange is relative to the
                // prefix, so scan_prefix ends at the prefix upper bound natively
                // and a drained queue is detected without scanning beyond the
                // prefix.
                Some(sf) => {
                    let suffix = sf.key.slice(prefix_bytes.len()..);
                    let start = if sf.inclusive {
                        Bound::Included(suffix)
                    } else {
                        Bound::Excluded(suffix)
                    };
                    txn.scan_prefix_with_options(
                        prefix_bytes,
                        (start, Bound::Unbounded),
                        &scan_options,
                    )
                    .await?
                }
                // Front scan: bound unknown (cold start or process
                // restart), so pre-existing keys may be live anywhere
                // in the prefix.
                None => {
                    txn.scan_prefix_with_options(prefix_bytes, .., &scan_options)
                        .await?
                }
            };
            while candidates.len() < max_jobs {
                match iter.next().await? {
                    Some(c) => candidates.push(c),
                    None => {
                        drained = true;
                        break;
                    }
                }
            }
            if candidates.is_empty() {
                // Every live pending key sorts at or after a known bound
                // (inserts landing behind it move it back), so an empty
                // bound scan proves the queue is empty without re-walking
                // the tombstone band from the front.
                self.core.claim_cursor.mark_empty(queue, scan.epoch);
                return Ok(Vec::new());
            }

            let now = self.now_ms();
            let lease_expires_at = now + lease_duration.as_millis() as u64;
            let last_pending_key = candidates
                .last()
                .expect("candidates checked non-empty above")
                .key
                .clone();

            let mut jobs = Vec::with_capacity(candidates.len());
            for kv in &candidates {
                let mut job = JobRecord::decode(&kv.key, &kv.value)?;
                job.status = JobStatus::Claimed;
                job.claimed_at = Some(now);
                job.attempts += 1;

                // Take the dedup_key off the record BEFORE serializing the
                // claimed-state copy. If we left it on, a later nack would put a
                // record back into pending still carrying the key, and the next
                // claim would try to delete a dedup index that may by now
                // belong to a *different* job, corrupting the dedup invariant.
                let dedup_key_to_release = job.dedup_key.take();
                let token = new_claim_token();
                let claimed = claimed_key(&job.queue, &job.id);
                let value = job.stored_bytes()?;

                txn.delete(&kv.key)?;
                put_job_record(&txn, &claimed, &job_index_key(&job.id), &value)?;
                // A cancellation requested during an earlier claim of
                // the job is persisted on the record and fires this
                // claim's token immediately.
                let cancel = tokio_util::sync::CancellationToken::new();
                if job.cancel_requested {
                    cancel.cancel();
                }
                // Registered before the commit, so a failed commit
                // leaves a stale entry, discarded when due; a missing
                // entry would leave the claim invisible to the reaper
                // until the next open, and a cancellation racing the
                // commit would find no token to fire.
                self.core.lease_registry.insert(
                    &job.queue,
                    &job.id,
                    lease_expires_at,
                    token,
                    cancel.clone(),
                );
                if let Some(dk) = dedup_key_to_release.as_deref() {
                    txn.delete(dedup_index_key(&job.queue, dk))?;
                }
                jobs.push(Claim::new(job, token, cancel));
            }
            let count = jobs.len() as i64;
            update_stats(
                &txn,
                queue,
                &[(JobStatus::Pending, -count), (JobStatus::Claimed, count)],
            )?;

            // Claims commit without awaiting WAL durability. The claimed
            // state only matters across a restart, where either version
            // of it recovers: a claim lost with the unflushed WAL leaves
            // the job pending, and a durable one is requeued at open,
            // the difference being only that the durable claim has
            // consumed an attempt.
            match commit(txn, Durability::Deferred).await? {
                Commit::Committed => {
                    self.core
                        .claim_cursor
                        .advance(queue, last_pending_key, &scan);
                    if drained {
                        // The scan ran dry inside this snapshot, so
                        // nothing is left after taking these jobs; record
                        // emptiness so the next poll short-circuits. Any
                        // insert since the epoch read revokes it.
                        self.core.claim_cursor.mark_empty(queue, scan.epoch);
                    }
                    // The claim histogram measures the claim
                    // transaction; offloaded payload fetches happen
                    // after the claim lock is released and are not
                    // included.
                    crate::obs::claimed(queue, jobs.len() as u64, timer);
                    debug!(queue = queue, count = jobs.len(), "jobs claimed");
                    return Ok(jobs);
                }
                Commit::Conflict => {
                    warn!(queue = queue, "claim transaction conflict, retrying");
                    continue;
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
    pub async fn ack(&self, claim: &Claim) -> Result<()> {
        self.ack_with(claim, SettlementEffects::default())
            .await
            .map(|_| ())
    }

    /// Acknowledge successful completion and apply `effects` in the
    /// same transaction.
    ///
    /// Either the acknowledgement and every effect land together or
    /// nothing does. In particular, if the job's claim is no longer
    /// present (its lease expired and the reaper requeued it), the call
    /// fails with [`Error::ClaimLost`] and no effect is applied, so
    /// a follow-up job exists only if this settlement won.
    ///
    /// Each enqueue in [`SettlementEffects::enqueues`] behaves exactly like
    /// [`Self::enqueue_with`]: a `dedup_key` hit downgrades that
    /// request to [`EnqueueResult::AlreadyEnqueued`] without affecting
    /// the ack or the other effects, and a future `run_at` lands the
    /// job in the scheduled key space. The returned results align
    /// index-wise with `effects.enqueues`. KV writes and deletes
    /// behave like [`Self::enqueue_with_kv`] and [`Self::kv_delete`].
    #[instrument(skip(self, claim, effects), fields(queue = %claim.queue, job_id = %claim.id))]
    pub async fn ack_with(
        &self,
        claim: &Claim,
        effects: SettlementEffects,
    ) -> Result<Vec<EnqueueResult>> {
        let timer = crate::obs::start();
        let keep = self.queue_keep_done_jobs(&claim.queue).is_some();
        let (job, results) = self
            .settle_claim(claim, effects, |_, _| ClaimEnd::Done { keep })
            .await?;
        crate::obs::completed(&job.queue, timer);
        debug!(queue = %job.queue, job_id = %job.id, "job acked");
        Ok(results.unwrap_or_default())
    }

    /// Report failure. Re-queues if attempts < max_attempts, otherwise dead-letters.
    ///
    /// Re-queued jobs honour the queue's `retry_backoff_base` and `retry_backoff_max`:
    /// when the backoff is non-zero, the job is parked in the scheduled key space and
    /// the background scheduler promotes it once the delay has elapsed. With zero
    /// backoff the job goes straight back to pending.
    pub async fn nack(&self, claim: &Claim, error: &str) -> Result<()> {
        self.nack_with(claim, error, SettlementEffects::default())
            .await
            .map(|_| ())
    }

    /// Report failure and apply `effects` in the same transaction when
    /// the failure dead-letters the job.
    ///
    /// Behaves like [`Self::nack`]. While attempts remain the job is
    /// re-queued, the effects are discarded and the call returns
    /// [`NackOutcome::Retried`]; a later settlement supplies its own
    /// effects. Once attempts are exhausted the job is dead-lettered
    /// and the effects are applied atomically with that transition,
    /// exactly as in [`Self::ack_with`], and the call returns
    /// [`NackOutcome::DeadLettered`].
    #[instrument(skip(self, claim, effects), fields(queue = %claim.queue, job_id = %claim.id))]
    pub async fn nack_with(
        &self,
        claim: &Claim,
        error: &str,
        effects: SettlementEffects,
    ) -> Result<NackOutcome> {
        let (job, results) = self
            .settle_claim(claim, effects, |stored, now| {
                if stored.attempts >= stored.max_attempts {
                    return ClaimEnd::Dead { error };
                }
                let cfg = self.queue_config(&stored.queue);
                let backoff = backoff_delay(
                    stored.attempts,
                    cfg.retry_backoff_base,
                    cfg.retry_backoff_max,
                );
                ClaimEnd::Retry {
                    run_at: (!backoff.is_zero()).then(|| now + backoff.as_millis() as u64),
                    outcome: AttemptOutcome::Retried,
                    error: Some(error),
                }
            })
            .await?;
        match results {
            Some(results) => {
                crate::obs::dead_lettered(&job.queue);
                Ok(NackOutcome::DeadLettered(results))
            }
            None => {
                crate::obs::nacked(&job.queue);
                Ok(NackOutcome::Retried)
            }
        }
    }

    /// Dead-letter a claimed job immediately, regardless of its `attempts`.
    /// Use this when the failure is *known* to be permanent and retrying
    /// would be wasted work.
    ///
    /// Unlike [`Self::nack`], this does not increment `attempts` or schedule
    /// a backoff: the job goes straight to the dead-letter set.
    /// [`worker::run_worker`](crate::worker::run_worker) and
    /// [`worker::run_worker_concurrent`](crate::worker::run_worker_concurrent)
    /// dead-letter through [`Self::dead_letter_with`] when a worker
    /// returns [`worker::PermanentFailure`](crate::worker::PermanentFailure).
    pub async fn dead_letter(&self, claim: &Claim, reason: &str) -> Result<()> {
        self.dead_letter_with(claim, reason, SettlementEffects::default())
            .await
            .map(|_| ())
    }

    /// Dead-letter a claimed job and apply `effects` in the same
    /// transaction.
    ///
    /// Behaves like [`Self::dead_letter`]; the effects behave exactly
    /// as in [`Self::ack_with`], and the returned results align
    /// index-wise with the effects' enqueues.
    #[instrument(skip(self, claim, effects), fields(queue = %claim.queue, job_id = %claim.id))]
    pub async fn dead_letter_with(
        &self,
        claim: &Claim,
        reason: &str,
        effects: SettlementEffects,
    ) -> Result<Vec<EnqueueResult>> {
        let (job, results) = self
            .settle_claim(claim, effects, |_, _| ClaimEnd::Dead { error: reason })
            .await?;
        crate::obs::dead_lettered(&job.queue);
        Ok(results.unwrap_or_default())
    }

    /// The settlement of a claim, shared by [`Self::ack_with`],
    /// [`Self::nack_with`] and [`Self::dead_letter_with`]. The effects
    /// are prepared once before the retry loop. Every iteration fences
    /// the claim, chooses the transition from the stored record and
    /// the current time with `end_for`, stages it and, when the
    /// transition is terminal, stages the effects. Returns the written
    /// record and the effects' results, `None` when the transition
    /// discarded them.
    async fn settle_claim<'e>(
        &self,
        claim: &Claim,
        effects: SettlementEffects,
        end_for: impl Fn(&JobRecord, u64) -> ClaimEnd<'e>,
    ) -> Result<(JobRecord, Option<Vec<EnqueueResult>>)> {
        let prepared = self.core.prepare_effects(effects).await?;
        let token = claim.token();
        let (queue, id) = (claim.queue.as_str(), claim.id.as_str());

        let settled: Result<SettledClaim<'e>> = async {
            loop {
                let txn = self.core.db.begin(IsolationLevel::Snapshot).await?;
                // The returned record is the base for the written
                // record; the claim's copy predates a cancel committed
                // during the delivery.
                let mut job = take_claim(&txn, &self.core.lease_registry, queue, id, token).await?;
                let now = self.now_ms();
                let end = end_for(&job, now);
                let pending_key = stage_claim_end(&txn, &mut job, &end, now)?;
                let staged = if end.is_terminal() {
                    Some(self.core.stage_effects(&txn, &prepared).await?)
                } else {
                    None
                };
                match commit(txn, Durability::Awaited).await? {
                    Commit::Committed => {
                        return Ok(SettledClaim {
                            job,
                            end,
                            pending_key,
                            results: staged.map(|s| self.core.note_staged_effects(s)),
                        });
                    }
                    Commit::Conflict => continue,
                }
            }
        }
        .await;

        self.core
            .finish_effects(
                prepared,
                settled.as_ref().ok().and_then(|s| s.results.as_deref()),
            )
            .await;
        let settled = settled?;
        self.core
            .finish_claim_end(
                &settled.job,
                &settled.end,
                token,
                settled.pending_key.as_deref(),
                Some(claim),
            )
            .await;
        Ok((settled.job, settled.results))
    }

    /// Return a snapshot of job counts for the given queue.
    pub async fn stats(&self, queue: &str) -> Result<QueueStats> {
        crate::read::stats(self.core.db.as_ref(), queue).await
    }

    /// Return the names of all queues that have ever had at least one job.
    pub async fn list_queues(&self) -> Result<Vec<String>> {
        crate::read::list_queues(self.core.db.as_ref()).await
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
        crate::read::dead_jobs(
            self.core.db.as_ref(),
            &self.core.payload_store,
            queue,
            after,
            limit,
        )
        .await
    }

    /// Return a page of the given queue's jobs in one lifecycle state.
    ///
    /// Jobs are returned in the scan order of the state's key space:
    ///
    /// - `Pending`: claim order (priority, then enqueue order).
    /// - `Scheduled`: `run_at` order, soonest first.
    /// - `Claimed`: enqueue order, as in [`Queue::dead_jobs`].
    /// - `Done`: completion-time order, oldest first. Done records exist
    ///   only on queues with [`QueueConfig::keep_done_jobs`] set.
    /// - `Dead`: enqueue order, as in [`Queue::dead_jobs`].
    ///
    /// `cursor` is an opaque resume token: pass `None` to start from the
    /// beginning, or [`JobPage::next_cursor`] from the previous page to
    /// continue. A cursor identifies a scan position, not a job, so it
    /// remains valid when the job it was taken at leaves the state. The
    /// listing is not a snapshot: a job that changes state between page
    /// reads may appear on no page or on two pages.
    ///
    /// A page can hold fewer than `limit` jobs while more remain,
    /// because a job removed between the key scan and its payload
    /// fetch is omitted from the page. The listing is exhausted only
    /// when [`JobPage::next_cursor`] is `None`.
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
        crate::read::list_jobs(
            self.core.db.as_ref(),
            &self.core.payload_store,
            queue,
            status,
            cursor,
            limit,
        )
        .await
    }

    /// Return a job's recorded delivery history, in write order.
    ///
    /// Each settlement of a claim appends one [`JobAttempt`]: an ack on a
    /// queue with [`QueueConfig::keep_done_jobs`] set, a [`Self::nack`], a
    /// [`Self::dead_letter`] and the reaper's handling of an expired
    /// lease. [`Self::requeue_dead_job`] appends an
    /// [`AttemptOutcome::Requeued`] marker and keeps the prior entries.
    ///
    /// The history shares the job's lifetime: it is removed in the same
    /// transaction that removes the job's last record, so a job for which
    /// [`Self::get_job`] returns `None` has an empty history. An ack on a
    /// queue without retention therefore removes the history rather than
    /// recording the completed attempt.
    pub async fn attempt_history(&self, id: &str) -> Result<Vec<JobAttempt>> {
        crate::read::attempt_history(self.core.db.as_ref(), id).await
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
        job.attempts = 0;
        job.last_error = None;
        job.claimed_at = None;
        job.failed_at = None;
        // Revival clears any prior cancel request: the operator chose to
        // start this job afresh.
        job.cancel_requested = false;

        let txn = self.core.db.begin(IsolationLevel::Snapshot).await?;
        txn.get(&dead)
            .await?
            .ok_or_else(|| Error::JobNotFound(job.id.clone()))?;
        txn.delete(&dead)?;
        let pending = stage_to_pending(&txn, &mut job, JobStatus::Dead)?;
        // The history is kept across the revival; the marker separates
        // entries recorded before it from the reset attempt counter.
        append_attempt(
            &txn,
            &job.id,
            &JobAttempt {
                attempt: 0,
                claimed_at: None,
                recorded_at: self.now_ms(),
                outcome: AttemptOutcome::Requeued,
                error: None,
            },
        )?;
        txn.commit().await?;
        self.core
            .claim_cursor
            .note_pending_insert(&job.queue, &pending);

        debug!(queue = %job.queue, job_id = %job.id, "dead job re-queued");
        Ok(())
    }

    /// Extend the lease on a claimed job, returning the new expiry as
    /// epoch milliseconds.
    ///
    /// Call this periodically for long-running jobs to prevent the reaper from
    /// treating them as abandoned and re-queuing them.
    ///
    /// The lease is process state, so renewal is a synchronous memory
    /// operation with no durable write. The claim is unchanged and
    /// stays valid for settlement; [`Self::lease_expiry`] reports the
    /// current value.
    ///
    /// Fails with [`Error::ClaimLost`] once the claim has ended or the
    /// reaper has begun re-queuing the expired lease. Fails with
    /// [`Error::CancelRequested`] once [`Self::cancel`] has been called
    /// on the job, leaving the lease to expire.
    ///
    /// This method serves callers that call [`Self::claim`] /
    /// [`Self::claim_batch`] directly and hold the [`Claim`]. Inside
    /// a [`Worker::process`](crate::worker::Worker::process) hook the
    /// claim stays with the worker loop; extend the lease there through
    /// the [`crate::LeaseHandle`] the hook receives.
    #[instrument(skip(self, claim), fields(queue = %claim.queue, job_id = %claim.id))]
    pub fn renew_lease(&self, claim: &Claim, extension: Duration) -> Result<u64> {
        let job = claim.job();
        if claim.cancel_token().is_cancelled() {
            return Err(Error::CancelRequested);
        }
        let new_expiry = self.now_ms() + extension.as_millis() as u64;
        if self.core.lease_registry.renew(
            &job.queue,
            &job.id,
            claim.token(),
            new_expiry,
            Renewal::Set,
        )? {
            crate::obs::renewed(&job.queue);
        }
        debug!(queue = %job.queue, job_id = %job.id, new_expiry, "lease renewed");
        Ok(new_expiry)
    }

    /// The current lease expiry of a claimed job, as epoch milliseconds.
    ///
    /// The lease is process state, so this is a synchronous read of the
    /// in-memory lease registry and reflects any renewal. Returns `None`
    /// when no live lease for the job exists in this process, including
    /// when the job is in any state other than `Claimed`.
    pub fn lease_expiry(&self, queue: &str, id: &str) -> Option<u64> {
        self.core
            .lease_registry
            .current(queue, id)
            .map(|(expires_at, _)| expires_at)
    }

    /// Wait until the given job reaches a terminal state, or until
    /// `timeout` elapses.
    ///
    /// Wake-up is notification-based: every terminal transition in the
    /// queue (`ack`, `nack` past `max_attempts`, `dead_letter`,
    /// `cancel`-Removed, reaper dead-letter) delivers its outcome to the
    /// tasks waiting on that job. There is no per-job polling.
    /// Transient transitions (a `nack` that re-queues for retry, the
    /// reaper re-queuing an expired lease, the scheduler promoting a
    /// scheduled job) do **not** wake the wait: they are not terminal.
    ///
    /// See [`WaitOutcome`] for the transition each variant reports and
    /// whether it carries a record.
    ///
    /// # Multiple waiters per job
    ///
    /// Several tasks may wait on the same job ID concurrently; each
    /// receives the same outcome when the terminal transition fires.
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
        // Registered before the storage read: a terminal transition
        // that commits after the read then reaches the registration,
        // and one that commits before it is visible in the read.
        let mut registration = self.core.completion_waiters.register(id);

        match self.get_job(id).await? {
            Some(job) => match job.status {
                JobStatus::Done => return Ok(WaitOutcome::Done(Box::new(job))),
                JobStatus::Dead => return Ok(WaitOutcome::Dead(Box::new(job))),
                _ => {}
            },
            // A transition that removed the record between the
            // registration and the read has delivered its outcome, or
            // is about to; the registration is consulted before the ID
            // is reported absent.
            None => {
                return Ok(registration.try_outcome().unwrap_or(WaitOutcome::NotFound));
            }
        }

        match tokio::time::timeout(timeout, registration.receiver()).await {
            // The sender is consumed only by a settlement, so the
            // channel cannot close without an outcome.
            Ok(delivered) => Ok(delivered.unwrap_or(WaitOutcome::TimedOut)),
            Err(_) => Ok(WaitOutcome::TimedOut),
        }
    }

    /// Look up a job by ID regardless of its current state.
    ///
    /// Returns `None` if the ID was never enqueued or has since been expunged.
    pub async fn get_job(&self, id: &str) -> Result<Option<JobRecord>> {
        // The index and the record are read from one snapshot.
        let txn = self.core.db.begin(IsolationLevel::Snapshot).await?;
        let found = get_indexed_job(&txn, id).await?;
        txn.rollback();

        let Some((index_key, _, mut job)) = found else {
            return Ok(None);
        };
        match self.core.payload_store.materialize(&mut job).await {
            Ok(()) => Ok(Some(job)),
            Err(Error::PayloadMissing { id }) => {
                // The record can be read just before a record-removing
                // transaction commits, with the object fetch running
                // just after that commit's payload-object deletion.
                // Re-check the index so a job removed in that window
                // is reported as absent.
                if self.core.db.get(&index_key).await?.is_none() {
                    Ok(None)
                } else {
                    Err(Error::PayloadMissing { id })
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Cancel a job, handling every lifecycle state.
    ///
    /// - **`Pending` or `Scheduled`**: removes the job from the queue
    ///   immediately. Returns [`CancelOutcome::Removed`].
    /// - **`Claimed` (a worker is processing it)**: persists a
    ///   `cancel_requested` flag on the job record and fires the
    ///   in-process [`tokio_util::sync::CancellationToken`] exposed on
    ///   [`Claim::cancel_token`] and
    ///   [`LeaseHandle::cancel_token`](crate::LeaseHandle::cancel_token).
    ///   Returns [`CancelOutcome::Requested`]. Workers that `select!`
    ///   on the token can short-circuit cooperatively; workers that
    ///   ignore it run to completion. The persisted flag ensures that
    ///   if the worker's lease expires and the reaper requeues the job,
    ///   the next claim's token starts pre-cancelled.
    /// - **`Done` / `Dead` / unknown**: returns [`CancelOutcome::NotFound`].
    ///
    /// Cooperative cancellation does not abort a running worker; futures
    /// cannot be safely cancelled mid-await. A worker observes the token
    /// to exit early.
    pub async fn cancel(&self, id: &str) -> Result<CancelOutcome> {
        self.cancel_with(id, SettlementEffects::default())
            .await
            .map(|(outcome, _)| outcome)
    }

    /// Cancel a job and apply `effects` in the same transaction as its
    /// removal.
    ///
    /// Behaves like [`Self::cancel`]. On [`CancelOutcome::Removed`]
    /// the effects are applied atomically with the removal, exactly as
    /// in [`Self::ack_with`], and the returned results align
    /// index-wise with the effects' enqueues. On every other outcome
    /// the effects are discarded and the results are empty; a claimed
    /// job's terminal settlement supplies its own effects.
    pub async fn cancel_with(
        &self,
        id: &str,
        effects: SettlementEffects,
    ) -> Result<(CancelOutcome, Vec<EnqueueResult>)> {
        let prepared = self.core.prepare_effects(effects).await?;
        let outcome = self.cancel_txn(id, &prepared).await;

        self.core
            .finish_effects(
                prepared,
                match &outcome {
                    Ok((CancelOutcome::Removed, results)) => Some(results.as_slice()),
                    _ => None,
                },
            )
            .await;
        outcome
    }

    /// The transaction loop of [`Self::cancel_with`]: resolve the job,
    /// apply the transition its state allows and commit, including the
    /// post-commit notifications of the committed outcome.
    async fn cancel_txn(
        &self,
        id: &str,
        prepared: &PreparedEffects,
    ) -> Result<(CancelOutcome, Vec<EnqueueResult>)> {
        loop {
            let txn = self.core.db.begin(IsolationLevel::Snapshot).await?;

            let Some((index_key, current_key, mut job)) = get_indexed_job(&txn, id).await? else {
                txn.rollback();
                return Ok((CancelOutcome::NotFound, Vec::new()));
            };

            let (msg, outcome, staged) = match job.status {
                JobStatus::Pending | JobStatus::Scheduled => {
                    let is_scheduled = matches!(job.status, JobStatus::Scheduled);
                    txn.delete(&current_key)?;
                    txn.delete(&index_key)?;
                    // A nacked job waiting out its backoff has attempt
                    // history; it is removed with the record.
                    txn.delete(attempt_history_key(id))?;
                    if let Some(ref dk) = job.dedup_key {
                        txn.delete(dedup_index_key(&job.queue, dk))?;
                    }
                    if is_scheduled {
                        update_stats(&txn, &job.queue, &[(JobStatus::Scheduled, -1)])?;
                    } else {
                        update_stats(&txn, &job.queue, &[(JobStatus::Pending, -1)])?;
                    }
                    let staged = self.core.stage_effects(&txn, prepared).await?;
                    (
                        "pending/scheduled job cancelled",
                        CancelOutcome::Removed,
                        Some(staged),
                    )
                }
                JobStatus::Claimed => {
                    if job.cancel_requested {
                        // The flag is already persisted. The token
                        // is fired again because a re-claim since
                        // the first request holds a fresh one.
                        txn.rollback();
                        self.core.lease_registry.cancel(&job.queue, id);
                        debug!(job_id = %id, "cancel re-requested on claimed job");
                        return Ok((CancelOutcome::Requested, Vec::new()));
                    }
                    job.cancel_requested = true;
                    let value = job.stored_bytes()?;
                    txn.put(&current_key, &value)?;
                    (
                        "claimed job cancellation requested",
                        CancelOutcome::Requested,
                        None,
                    )
                }
                JobStatus::Done | JobStatus::Dead => {
                    txn.rollback();
                    return Ok((CancelOutcome::NotFound, Vec::new()));
                }
            };

            match commit(txn, Durability::Awaited).await? {
                Commit::Committed => {
                    // Fired on the Removed path as well: the
                    // worker of a claim the reaper requeued just
                    // before this call may still observe the token.
                    // That claim's end removes the entry.
                    self.core.lease_registry.cancel(&job.queue, id);
                    let results = staged
                        .map(|s| self.core.note_staged_effects(s))
                        .unwrap_or_default();
                    // Removed = terminal (job is gone). Requested = not yet
                    // terminal; the worker's settlement delivers the
                    // outcome when it acks / nacks / dead-letters.
                    if matches!(outcome, CancelOutcome::Removed) {
                        // The record is deleted, so its payload object
                        // (if any) is removed here, after the commit.
                        self.core.payload_store.delete_for(&job).await;
                        self.core
                            .completion_waiters
                            .settle(id, || WaitOutcome::Cancelled);
                    }
                    debug!(job_id = %id, "{msg}");
                    return Ok((outcome, results));
                }
                Commit::Conflict => continue,
            }
        }
    }

    /// Move a `Scheduled` job to pending immediately, before its `run_at`,
    /// optionally attaching `wake_payload` bytes to the record.
    ///
    /// This is the targeted counterpart of the scheduler's due-job
    /// promotion: the same transition (scheduled to pending), applied to one
    /// job by ID at the caller's initiative instead of at `run_at`. On
    /// [`WakeOutcome::Woken`] the job is claimable immediately and any
    /// worker waiting on the queue is notified.
    ///
    /// The wake stamps [`JobRecord::woken_at`], so a worker can
    /// distinguish an early wake from ordinary promotion at `run_at`
    /// regardless of whether bytes were attached. `wake_payload` is
    /// stored on [`JobRecord::wake_payload`]. Both values persist on the
    /// record across later transitions, so redelivery after a lease
    /// expiry observes them again. The payload contributes to the
    /// serialized record that is rewritten on each transition; it is
    /// intended for coordination data, not bulk payload.
    ///
    /// Exactly one caller wins the transition: a concurrent scheduler
    /// promotion, `wake_scheduled` call, or [`Self::cancel`] and this call
    /// conflict on the scheduled record, and the loser observes
    /// [`WakeOutcome::NotScheduled`] or [`WakeOutcome::NotFound`]. The
    /// commit is durable before the call returns.
    pub async fn wake_scheduled(
        &self,
        id: &str,
        wake_payload: Option<Vec<u8>>,
    ) -> Result<WakeOutcome> {
        loop {
            let txn = self.core.db.begin(IsolationLevel::Snapshot).await?;

            let Some((_, current_key, mut job)) = get_indexed_job(&txn, id).await? else {
                txn.rollback();
                return Ok(WakeOutcome::NotFound);
            };

            if job.status != JobStatus::Scheduled {
                txn.rollback();
                return Ok(WakeOutcome::NotScheduled);
            }

            txn.delete(&current_key)?;
            job.woken_at = Some(self.now_ms());
            job.wake_payload = wake_payload.clone();
            let pending = stage_to_pending(&txn, &mut job, JobStatus::Scheduled)?;

            match commit(txn, Durability::Awaited).await? {
                Commit::Committed => {
                    self.core
                        .claim_cursor
                        .note_pending_insert(&job.queue, &pending);
                    debug!(job_id = %id, queue = %job.queue, "scheduled job woken");
                    return Ok(WakeOutcome::Woken);
                }
                Commit::Conflict => continue,
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
        let timer = crate::obs::start();

        let mut prepared = payloads
            .into_iter()
            .map(|payload| {
                self.core
                    .prepare_job_record(queue, payload, EnqueueOptions::default())
            })
            .collect::<Result<Vec<_>>>()?;
        self.core.offload_prepared(&mut prepared).await?;

        let write = async {
            loop {
                let txn = self.core.db.begin(IsolationLevel::Snapshot).await?;
                let mut staged = Vec::with_capacity(prepared.len());
                for prepared_job in &prepared {
                    match self.core.stage_job_writes(&txn, prepared_job).await? {
                        Ok(staged_job) => staged.push(staged_job),
                        // Batch jobs have no dedup key.
                        Err(_) => return Err(Error::InvalidState),
                    }
                }
                match commit(txn, Durability::Awaited).await? {
                    Commit::Committed => return Ok(staged),
                    Commit::Conflict => continue,
                }
            }
        };
        let staged = match write.await {
            Ok(staged) => staged,
            Err(err) => {
                self.core.discard_prepared(&prepared).await;
                return Err(err);
            }
        };
        crate::obs::enqueued(queue, staged.len() as u64, timer);
        // Batch ids are monotonic ULIDs at one priority, so the first
        // staged job holds the batch's smallest pending key.
        if let Some(key) = staged.first().and_then(|s| s.pending_key.as_ref()) {
            self.core
                .claim_cursor
                .note_pending_inserts(queue, key, staged.len());
        }

        debug!(queue = queue, count = staged.len(), "batch enqueued");
        Ok(staged.into_iter().map(|s| s.id).collect())
    }

    /// Trigger an immediate reap sweep (primarily useful in tests and tooling).
    pub async fn reap_now(&self) -> Result<()> {
        self.reaper.reap_expired().await
    }

    /// Trigger an immediate scheduled-job promotion sweep (primarily useful in tests).
    pub async fn promote_scheduled_now(&self) -> Result<()> {
        self.scheduler.promote_due_jobs().await
    }

    /// Shut down the background reaper and scheduler, persist each
    /// queue's claim-scan state, then close the underlying database.
    ///
    /// The persisted state lets the next open resume claims at the
    /// recorded bound instead of re-scanning the tombstone band left
    /// by previously claimed jobs, so the first claim after a clean
    /// restart costs the same as a warm one. With
    /// [`OpenOptions::liveness_heartbeat`] set, a final beat marked
    /// closed is committed best-effort, so readers can distinguish
    /// this close from a writer that stopped beating.
    pub async fn close(self) -> Result<()> {
        tokio::join!(self.reaper_task.stop(), self.scheduler_task.stop(), async {
            if let Some(sampler) = self.metrics_sampler {
                sampler.stop().await;
            }
        });
        if let Some(heartbeat) = self.heartbeat
            && let Some(task) = heartbeat.stop().await
        {
            task.write_closing_beat().await;
        }
        crate::claim_cursor::persist_cursor_state(&self.core).await?;
        self.core.db.close().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::pending_key;
    use crate::test_util::*;

    #[tokio::test(start_paused = true)]
    async fn test_kv_compare_put_stalls_during_store_outage_without_partial_state() {
        let store = FaultStore::wrap();
        let q = Queue::open(store.clone(), "test").await.unwrap();
        q.kv_put(b"slot", b"v1").await.unwrap();

        store.fail_puts(true);
        // The compare-miss arm is read-only and completes despite the
        // write fault.
        assert!(!q.kv_compare_put(b"slot", Some(b"v0"), b"v2").await.unwrap());
        // The matched arm awaits durability. SlateDB retries transient
        // store errors with backoff instead of failing the flush, so the
        // call must stall rather than report success. Paused runtime time
        // drives the retry backoff virtually; the elapsed timeout drops
        // the in-flight call, simulating a crash mid-outage.
        let stalled = tokio::time::timeout(
            Duration::from_secs(30),
            q.kv_compare_put(b"slot", Some(b"v1"), b"v2"),
        )
        .await;
        assert!(stalled.is_err());
        drop(q);

        store.fail_puts(false);
        let q = Queue::open(store, "test").await.unwrap();
        assert_eq!(
            q.kv_get(b"slot").await.unwrap().as_deref(),
            Some(b"v1".as_slice())
        );
        assert!(q.kv_compare_put(b"slot", Some(b"v1"), b"v2").await.unwrap());
        assert_eq!(
            q.kv_get(b"slot").await.unwrap().as_deref(),
            Some(b"v2".as_slice())
        );
        q.close().await.unwrap();
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
        Ulid::from_string(&id).expect("a generated id is a ULID");
        assert_eq!(job.queue, "email");
        assert_eq!(job.payload, b"hello");
        assert_eq!(job.status, JobStatus::Claimed);
        assert_eq!(job.attempts, 1);
        assert!(job.claimed_at.is_some());
        assert!(q.lease_expiry("email", &job.id).is_some());

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
    async fn test_enqueue_with_duplicate_id_override_rejected() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let id = q
            .enqueue_with(
                "email",
                b"first".to_vec(),
                EnqueueOptions {
                    id_override: Some("duplicate-id".to_string()),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(id, "duplicate-id");

        let err = q
            .enqueue_with(
                "email",
                b"second".to_vec(),
                EnqueueOptions {
                    id_override: Some("duplicate-id".to_string()),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::DuplicateJobId { id } if id == "duplicate-id"));

        let job = q
            .claim("email", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, "duplicate-id");
        assert_eq!(job.payload, b"first");
        assert!(
            q.claim("email", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_enqueue_with_kv_duplicate_id_override_rejects_kv_writes() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue_with(
            "email",
            b"first".to_vec(),
            EnqueueOptions {
                id_override: Some("duplicate-kv-id".to_string()),
                ..EnqueueOptions::default()
            },
        )
        .await
        .unwrap();

        let err = q
            .enqueue_with_kv(
                "email",
                b"second".to_vec(),
                EnqueueOptions {
                    id_override: Some("duplicate-kv-id".to_string()),
                    ..EnqueueOptions::default()
                },
                HashMap::from([(b"meta/duplicate".to_vec(), b"written".to_vec())]),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::DuplicateJobId { id } if id == "duplicate-kv-id"));
        assert!(q.kv_get(b"meta/duplicate").await.unwrap().is_none());

        let job = q
            .claim("email", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, "duplicate-kv-id");
        assert_eq!(job.payload, b"first");

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

        q.nack(&job, "transient error").await.unwrap();

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
        q.nack(&job, "fail").await.unwrap();

        let s = q.stats("email").await.unwrap();
        assert_eq!(s.pending, 0);
        assert_eq!(s.claimed, 0);
        assert_eq!(s.dead, 1);

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
        q.nack(&job, "fatal").await.unwrap();

        let dead = q.dead_jobs("work", None, 100).await.unwrap();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].id, id);
        assert_eq!(dead[0].status, JobStatus::Dead);
        assert!(dead[0].failed_at.is_some());

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
        assert!(
            revived.failed_at.is_none(),
            "requeue must clear failed_at so a re-fail starts a fresh retention window"
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_per_queue_config() {
        let initial = 1_700_000_000_000u64;
        let mut opts = OpenOptions {
            clock: Arc::new(MockClock::new(initial)),
            ..OpenOptions::default()
        };
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
        assert_eq!(q.lease_expiry("fast", &job.id), Some(initial + 5_000));
        assert_eq!(job.claimed_at, Some(initial));

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
    async fn test_ids_increase_within_one_millisecond() {
        // A clock that never advances puts every id in one millisecond,
        // which is the case a non-monotonic id source orders arbitrarily.
        let clock = MockClock::new(1_700_000_000_000);
        let q = Queue::open_with_options(
            make_store(),
            "test",
            OpenOptions {
                clock: Arc::new(clock.clone()),
                ..OpenOptions::default()
            },
        )
        .await
        .unwrap();

        let ids: Vec<String> = (0..10).map(|_| q.next_job_id()).collect();

        // The first ten characters of a ULID are its millisecond timestamp.
        assert!(
            ids.iter().all(|id| id[..10] == ids[0][..10]),
            "every id must carry the frozen clock's millisecond"
        );
        assert!(
            ids.windows(2).all(|w| w[0] < w[1]),
            "ids must increase with generation order"
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_fifo_holds_across_a_claim_batch() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let mut enqueued = Vec::new();
        for i in 0..20 {
            enqueued.push(
                q.enqueue("jobs", format!("job-{i}").into_bytes())
                    .await
                    .unwrap(),
            );
        }

        let claimed = q
            .claim_batch("jobs", 20, Duration::from_secs(30))
            .await
            .unwrap();
        let claimed_ids: Vec<String> = claimed.into_iter().map(|c| c.into_job().id).collect();
        assert_eq!(claimed_ids, enqueued);

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

        q.nack(&job, "retry me").await.unwrap();

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
    async fn test_kv_put_roundtrip_and_size_cap() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.kv_put(b"config", b"v1").await.unwrap();
        assert_eq!(
            q.kv_get(b"config").await.unwrap().as_deref(),
            Some(b"v1".as_slice())
        );

        let oversized = vec![0u8; MAX_KV_VALUE_SIZE + 1];
        assert!(matches!(
            q.kv_put(b"blob", &oversized).await,
            Err(Error::KvValueTooLarge { .. })
        ));

        q.kv_delete(b"config").await.unwrap();
        assert!(q.kv_get(b"config").await.unwrap().is_none());

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kv_compare_delete() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.kv_put(b"latch", b"v1").await.unwrap();

        assert!(!q.kv_compare_delete(b"latch", b"v2").await.unwrap());
        assert_eq!(
            q.kv_get(b"latch").await.unwrap().as_deref(),
            Some(b"v1".as_slice())
        );

        assert!(q.kv_compare_delete(b"latch", b"v1").await.unwrap());
        assert!(q.kv_get(b"latch").await.unwrap().is_none());

        assert!(!q.kv_compare_delete(b"latch", b"v1").await.unwrap());

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kv_compare_put() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        assert!(!q.kv_compare_put(b"slot", Some(b"v1"), b"v2").await.unwrap());
        assert!(q.kv_get(b"slot").await.unwrap().is_none());

        assert!(q.kv_compare_put(b"slot", None, b"v1").await.unwrap());
        assert_eq!(
            q.kv_get(b"slot").await.unwrap().as_deref(),
            Some(b"v1".as_slice())
        );

        assert!(!q.kv_compare_put(b"slot", None, b"v2").await.unwrap());
        assert_eq!(
            q.kv_get(b"slot").await.unwrap().as_deref(),
            Some(b"v1".as_slice())
        );

        assert!(!q.kv_compare_put(b"slot", Some(b"v0"), b"v2").await.unwrap());
        assert_eq!(
            q.kv_get(b"slot").await.unwrap().as_deref(),
            Some(b"v1".as_slice())
        );

        assert!(q.kv_compare_put(b"slot", Some(b"v1"), b"v2").await.unwrap());
        assert_eq!(
            q.kv_get(b"slot").await.unwrap().as_deref(),
            Some(b"v2".as_slice())
        );

        let oversized = vec![0u8; MAX_KV_VALUE_SIZE + 1];
        assert!(matches!(
            q.kv_compare_put(b"slot", Some(b"v2"), &oversized).await,
            Err(Error::KvValueTooLarge { .. })
        ));

        q.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_kv_compare_put_loses_no_updates_under_contention() {
        let q = Arc::new(Queue::open(make_store(), "test").await.unwrap());
        q.kv_put(b"counter", &0u64.to_be_bytes()).await.unwrap();

        let mut handles = Vec::new();
        for _ in 0..4 {
            let q = Arc::clone(&q);
            handles.push(tokio::spawn(async move {
                for _ in 0..25 {
                    loop {
                        let current = q.kv_get(b"counter").await.unwrap().unwrap();
                        let n = u64::from_be_bytes(current.as_ref().try_into().unwrap());
                        let next = (n + 1).to_be_bytes();
                        if q.kv_compare_put(b"counter", Some(current.as_ref()), &next)
                            .await
                            .unwrap()
                        {
                            break;
                        }
                    }
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let total = q.kv_get(b"counter").await.unwrap().unwrap();
        assert_eq!(u64::from_be_bytes(total.as_ref().try_into().unwrap()), 100);

        let q = Arc::try_unwrap(q).unwrap_or_else(|_| panic!("queue still shared"));
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kv_scan_pages_and_filters_by_prefix() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        for i in 0..5u8 {
            q.kv_put(&[b"runs/".as_slice(), &[b'0' + i]].concat(), &[i])
                .await
                .unwrap();
        }
        q.kv_put(b"config", b"c").await.unwrap();

        let page = q.kv_scan(b"runs/", None, 3).await.unwrap();
        assert_eq!(page.entries.len(), 3);
        assert_eq!(page.entries[0].0, b"runs/0");
        assert!(page.next_cursor.is_some());

        let rest = q
            .kv_scan(b"runs/", page.next_cursor.as_deref(), 10)
            .await
            .unwrap();
        assert_eq!(rest.entries.len(), 2);
        assert_eq!(rest.entries[1].0, b"runs/4");
        assert!(rest.next_cursor.is_none());

        let all = q.kv_scan(b"", None, 100).await.unwrap();
        assert_eq!(all.entries.len(), 6);
        assert_eq!(all.entries[0].0, b"config");

        let empty = q.kv_scan(b"", None, 0).await.unwrap();
        assert!(empty.entries.is_empty() && empty.next_cursor.is_none());

        let foreign = q
            .kv_scan(b"other/", page.next_cursor.as_deref(), 10)
            .await
            .unwrap();
        assert!(foreign.entries.is_empty() && foreign.next_cursor.is_none());

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kv_scan_excludes_internal_keys() {
        let initial = 1_700_000_000_000u64;
        let opts = OpenOptions {
            clock: Arc::new(MockClock::new(initial)),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue("jobs", b"payload".to_vec()).await.unwrap();
        q.enqueue_with(
            "jobs",
            b"later".to_vec(),
            EnqueueOptions {
                run_at: Some(std::time::UNIX_EPOCH + Duration::from_millis(initial + 3_600_000)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        q.kv_put(b"only", b"entry").await.unwrap();

        let page = q.kv_scan(b"", None, 100).await.unwrap();
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].0, b"only");

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kv_state_survives_crash_reopen() {
        let store = make_store();
        let q = Queue::open(store.clone(), "test").await.unwrap();
        q.kv_put(b"standalone", b"v1").await.unwrap();
        let mut kv = HashMap::new();
        kv.insert(b"coupled".to_vec(), b"v2".to_vec());
        q.enqueue_with_kv("jobs", b"p".to_vec(), EnqueueOptions::default(), kv)
            .await
            .unwrap();
        drop(q);

        let q = Queue::open(store, "test").await.unwrap();
        assert_eq!(
            q.kv_get(b"standalone").await.unwrap().as_deref(),
            Some(b"v1".as_slice())
        );
        assert_eq!(
            q.kv_get(b"coupled").await.unwrap().as_deref(),
            Some(b"v2".as_slice())
        );
        assert_eq!(q.stats("jobs").await.unwrap().pending, 1);
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

        q.dead_letter(&claimed, "permanent failure").await.unwrap();

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

    #[tokio::test]
    async fn test_get_job_returns_none_for_unknown_id() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        assert!(q.get_job("nonexistent").await.unwrap().is_none());
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
        let initial = 1_700_000_000_000u64;
        let opts = OpenOptions {
            clock: Arc::new(MockClock::new(initial)),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let id = q
            .enqueue_with(
                "work",
                b"payload".to_vec(),
                EnqueueOptions {
                    run_at: Some(
                        std::time::UNIX_EPOCH + Duration::from_millis(initial + 3_600_000),
                    ),
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

        let token = job.cancel_token().clone();
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
    async fn a_cancel_requested_during_the_delivery_survives_a_nack() {
        let q = Queue::open_with_options(make_store(), "test", no_backoff_opts())
            .await
            .unwrap();

        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let claim = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Requested);

        q.nack(&claim, "transient").await.unwrap();

        let requeued = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(requeued.status, JobStatus::Pending);
        assert!(
            requeued.cancel_requested,
            "the nack must not overwrite the persisted cancel request",
        );

        let reclaim = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert!(reclaim.cancel_requested);
        assert!(
            reclaim.cancel_token().is_cancelled(),
            "the re-claim must surface a pre-cancelled token",
        );

        q.ack(&reclaim).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_cancel_requested_during_the_delivery_survives_onto_a_kept_done_record() {
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
        let claim = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Requested);

        q.ack(&claim).await.unwrap();

        let done = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(done.status, JobStatus::Done);
        assert!(
            done.cancel_requested,
            "the done record is written from the stored record, not the claim's copy",
        );
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_cancel_requested_during_the_delivery_survives_a_dead_letter() {
        let q = Queue::open_with_options(make_store(), "test", no_backoff_opts())
            .await
            .unwrap();

        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let claim = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Requested);

        q.dead_letter(&claim, "permanent").await.unwrap();

        let dead = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(dead.status, JobStatus::Dead);
        assert!(dead.cancel_requested);
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
        q.nack(&job, "transient").await.unwrap();

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

        // Enqueue a real job so the internal pending key space is in use.
        q.enqueue("work", b"payload".to_vec()).await.unwrap();

        // A user key that matches a real internal key byte-for-byte is
        // scoped under the user tag and cannot interfere with queue state.
        q.enqueue_with_kv(
            "other",
            b"sentinel".to_vec(),
            EnqueueOptions::default(),
            HashMap::from([(pending_key("work", 1, "fake-id"), b"trickery".to_vec())]),
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
        let v = q.kv_get(&pending_key("work", 1, "fake-id")).await.unwrap();
        assert_eq!(v.as_deref(), Some(b"trickery".as_slice()));

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn an_exhausted_nack_clears_claimed_at_on_the_dead_record() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let id = q
            .enqueue_with(
                "work",
                b"job".to_vec(),
                EnqueueOptions {
                    max_attempts: Some(1),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .unwrap();
        let job = q
            .claim("work", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        assert!(job.claimed_at.is_some());

        q.nack(&job, "fatal").await.unwrap();
        let dead = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(dead.status, JobStatus::Dead);
        assert!(dead.claimed_at.is_none());
        q.close().await.unwrap();
    }

    // Every claimed record must hold a registry entry; a record
    // without one is invisible to the reaper until the next open.
}
