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
use crate::claim_cursor::{ClaimCursor, CursorState, ScanFrom};
use crate::clock::{Clock, default_clock};
use crate::completion::CompletionWaiters;
use crate::error::{Error, Result};
use crate::history::{AttemptOutcome, JobAttempt, append_attempt};
use crate::job::{Claim, JobRecord, JobStatus};
use crate::keys::{
    KeyTag, MAX_QUEUE_NAME_LEN, attempt_history_key, claimed_key, cursor_key, dead_key,
    dedup_index_key, done_key, job_index_key, pending_key, pending_prefix, scheduled_key,
    tag_prefix, user_scoped_key,
};
use crate::lease_registry::{LeaseRegistry, Renewal};
use crate::payload_store::PayloadStore;
use crate::queue_core::{QueueConfigs, QueueCore};
use crate::reaper::Reaper;
use crate::scheduler::Scheduler;
use crate::stats::{QueueMergeOperator, QueueStats, update_stats};
use crate::txn::{
    Commit, Durability, commit, put_job_record, stage_dead_letter, stage_to_pending, take_claim,
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

/// On-disk form of one queue's claim-scan state, stored under
/// [`cursor_key`]. Written only by a clean [`Queue::close`]; the next
/// open deletes the record before serving traffic, so a record is
/// never observed after the state it describes could have changed.
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedCursor {
    /// Queue the state belongs to; carried in the record so the
    /// reader does not parse it out of the key.
    queue: String,
    /// Scan bound key, when one was established.
    bound_key: Option<Vec<u8>>,
    /// Whether the bound key itself may be live.
    bound_inclusive: bool,
    /// Whether a full scan had proven the queue empty at close.
    known_empty: bool,
}

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
fn validate_kv_value_size(value: &[u8]) -> Result<()> {
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
/// QueueConfig {
///     max_attempts: 10,
///     ..QueueConfig::default()
/// }
/// ```
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
    core: Arc<QueueCore>,
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
    /// Source of job ids. Pending keys sort by id within a priority, so
    /// ids must increase with enqueue order, including inside one
    /// millisecond. One generator per store suffices: a store has a
    /// single writer process.
    id_gen: std::sync::Mutex<ulid::Generator>,
}

/// The record a settlement delivers to completion waiters: the stored
/// record with the payload the claim carried, so an offloaded payload is
/// inline as `get_job` returns it.
fn delivered_record(stored: &JobRecord, claim: &Claim) -> JobRecord {
    let mut delivered = stored.clone();
    if delivered.payload_ref.is_some() {
        delivered.payload = claim.job().payload.clone();
    }
    delivered
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

/// A job record prepared by [`Queue::prepare_job_record`], paired with
/// its primary key, awaiting staging into a transaction.
struct PreparedJob {
    job: JobRecord,
    key: Vec<u8>,
    id_override_used: bool,
}

/// [`SettlementEffects`] validated and prepared by [`Queue::prepare_effects`],
/// awaiting staging into a settlement transaction.
#[derive(Default)]
struct PreparedEffects {
    prepared_jobs: Vec<PreparedJob>,
    kv_writes: HashMap<Vec<u8>, Vec<u8>>,
    kv_deletes: Vec<Vec<u8>>,
}

/// Identity of a job staged by [`Queue::stage_job_writes`], retained
/// for post-commit bookkeeping.
struct StagedJob {
    id: String,
    queue: String,
    /// `Some` when the job landed in the pending key space, in which
    /// case the commit must be followed by a cursor insert note, which
    /// also wakes a waiting worker.
    pending_key: Option<Vec<u8>>,
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
        });
        restore_cursor_state(&core).await?;
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
            id_gen: std::sync::Mutex::new(ulid::Generator::new()),
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
        let at = std::time::UNIX_EPOCH + Duration::from_millis(self.now_ms());
        let mut generator = self.id_gen.lock().expect("id generator mutex poisoned");
        match generator.generate_from_datetime(at) {
            Ok(id) => id.to_string(),
            // Unreachable short of 2^80 ids inside one millisecond.
            Err(_) => Ulid::from_datetime(at).to_string(),
        }
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
        let (job, key, id_override_used) = self.prepare_job_record(queue, payload, opts)?;
        self.write_job(job, key, id_override_used, HashMap::new())
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
            validate_kv_value_size(value)?;
        }

        let (job, key, id_override_used) = self.prepare_job_record(queue, payload, opts)?;
        self.write_job(job, key, id_override_used, kv_writes).await
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

    /// Resolve [`EnqueueOptions`] against the queue's defaults and build
    /// the [`JobRecord`] + its primary key. Shared by [`Self::enqueue_with`]
    /// and [`Self::enqueue_with_kv`]; the two methods only diverge in how
    /// they persist the prepared record.
    fn prepare_job_record(
        &self,
        queue: &str,
        payload: Vec<u8>,
        opts: EnqueueOptions,
    ) -> Result<(JobRecord, Vec<u8>, bool)> {
        validate_queue_name(queue)?;
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

        let (id, id_override_used) = match opts.id_override {
            Some(supplied) => {
                validate_id_override(&supplied)?;
                (supplied, true)
            }
            None => (self.next_job_id(), false),
        };

        let (status, key) = match run_at {
            Some(ms) => (JobStatus::Scheduled, scheduled_key(queue, ms, &id)),
            None => (JobStatus::Pending, pending_key(queue, priority, &id)),
        };

        let mut job = JobRecord::new_pending(
            id,
            queue.to_string(),
            payload,
            max_attempts,
            priority,
            self.now_ms(),
        );
        job.headers = opts.headers;
        job.status = status;
        job.run_at = run_at;
        job.dedup_key = opts.dedup_key;

        Ok((job, key, id_override_used))
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

    /// Release prepared effects once their settlement has ended:
    /// delete the payload objects of follow-up jobs that no committed
    /// record points at. `results` aligns index-wise with the prepared
    /// jobs: an [`EnqueueResult::AlreadyEnqueued`] entry marks a dedup
    /// downgrade whose object is unreferenced. `None` means no
    /// follow-up record committed (the settlement failed or took a
    /// branch that discards the effects), so every offloaded object is
    /// deleted. Every settlement path ends with this call, on every
    /// branch.
    async fn finish_effects(&self, prepared: PreparedEffects, results: Option<&[EnqueueResult]>) {
        match results {
            Some(results) => {
                for (prepared, result) in prepared.prepared_jobs.iter().zip(results) {
                    if matches!(result, EnqueueResult::AlreadyEnqueued(_)) {
                        self.core.payload_store.delete_for(&prepared.job).await;
                    }
                }
            }
            None => {
                for prepared in &prepared.prepared_jobs {
                    self.core.payload_store.delete_for(&prepared.job).await;
                }
            }
        }
    }

    /// Validate `effects` and prepare them for staging: size-check the
    /// KV writes, build the follow-up job records and offload their
    /// payloads. Runs once, before a settlement's transaction loop, so
    /// the follow-up ids stay stable across conflict retries and a
    /// committed record never points at an unwritten object. The
    /// caller passes the result to [`Self::finish_effects`] once the
    /// settlement has ended.
    async fn prepare_effects(&self, effects: SettlementEffects) -> Result<PreparedEffects> {
        for value in effects.kv_writes.values() {
            validate_kv_value_size(value)?;
        }
        if let Some(key) = effects
            .kv_deletes
            .iter()
            .find(|k| effects.kv_writes.contains_key(*k))
        {
            return Err(Error::ConflictingKvEffect { key: key.clone() });
        }
        let mut prepared_jobs = Vec::with_capacity(effects.enqueues.len());
        for request in effects.enqueues {
            let (job, key, id_override_used) =
                self.prepare_job_record(&request.queue, request.payload, request.options)?;
            prepared_jobs.push(PreparedJob {
                job,
                key,
                id_override_used,
            });
        }
        self.core
            .payload_store
            .offload_all(prepared_jobs.iter_mut().map(|p| &mut p.job))
            .await?;
        Ok(PreparedEffects {
            prepared_jobs,
            kv_writes: effects.kv_writes,
            kv_deletes: effects.kv_deletes,
        })
    }

    /// Add prepared effects to a caller-owned settlement transaction.
    /// Called inside every iteration of the settlement's retry loop.
    /// A dedup hit downgrades that enqueue to
    /// [`EnqueueResult::AlreadyEnqueued`] without affecting the rest.
    /// After the transaction commits, the caller must pass each staged
    /// value to [`Self::note_staged_job`].
    async fn stage_effects(
        &self,
        txn: &DbTransaction,
        prepared: &PreparedEffects,
    ) -> Result<(Vec<EnqueueResult>, Vec<StagedJob>)> {
        let mut staged = Vec::with_capacity(prepared.prepared_jobs.len());
        let mut results = Vec::with_capacity(prepared.prepared_jobs.len());
        for prepared_job in &prepared.prepared_jobs {
            match self.stage_job_writes(txn, prepared_job).await? {
                Ok(staged_job) => {
                    results.push(EnqueueResult::New(staged_job.id.clone()));
                    staged.push(staged_job);
                }
                Err(existing) => results.push(EnqueueResult::AlreadyEnqueued(existing)),
            }
        }
        for (k, v) in &prepared.kv_writes {
            txn.put(user_scoped_key(k), v)?;
        }
        for k in &prepared.kv_deletes {
            txn.delete(user_scoped_key(k))?;
        }
        Ok((results, staged))
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
        mut job: JobRecord,
        key: Vec<u8>,
        id_override_used: bool,
        kv_writes: HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<EnqueueResult> {
        self.core.payload_store.offload(&mut job).await?;
        let prepared = PreparedJob {
            job,
            key,
            id_override_used,
        };
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

            let staged = match self.stage_job_writes(&txn, prepared).await? {
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
                    self.note_staged_job(&staged);
                    return Ok(EnqueueResult::New(staged.id));
                }
                Commit::Conflict => continue,
            }
        }
    }

    /// Add one prepared job's writes (record, job index, dedup index,
    /// stats delta) to a caller-owned transaction. Returns
    /// `Ok(Err(existing_id))` on a dedup hit, in which case no writes
    /// were added and the caller decides whether to roll back; the
    /// outer `Err` is reserved for real failures. After the
    /// transaction commits, the caller must pass the staged value to
    /// [`Self::note_staged_job`].
    async fn stage_job_writes(
        &self,
        txn: &DbTransaction,
        prepared: &PreparedJob,
    ) -> Result<std::result::Result<StagedJob, String>> {
        let PreparedJob {
            job,
            key,
            id_override_used,
        } = prepared;
        let dkey = job
            .dedup_key
            .as_ref()
            .map(|dk| dedup_index_key(&job.queue, dk));

        if let Some(ref dkey) = dkey
            && let Some(bytes) = txn.get(&dkey).await?
        {
            let existing = String::from_utf8(bytes.to_vec()).map_err(|_| Error::InvalidState)?;
            return Ok(Err(existing));
        }

        if *id_override_used && txn.get(job_index_key(&job.id)).await?.is_some() {
            return Err(Error::DuplicateJobId { id: job.id.clone() });
        }

        let value = job.stored_bytes()?;
        put_job_record(txn, key, &job_index_key(&job.id), &value)?;
        if let Some(ref dkey) = dkey {
            txn.put(dkey, job.id.as_bytes())?;
        }
        update_stats(txn, &job.queue, &[(job.status, 1)])?;

        Ok(Ok(StagedJob {
            id: job.id.clone(),
            queue: job.queue.clone(),
            pending_key: matches!(job.status, JobStatus::Pending).then(|| key.clone()),
        }))
    }

    /// Post-commit bookkeeping for one staged job: a Pending job is
    /// recorded on the claim cursor, which wakes a waiting worker; a
    /// Scheduled job becomes claimable later via the scheduler loop,
    /// which records its own insert.
    fn note_staged_job(&self, staged: &StagedJob) {
        if let Some(ref pending_key) = staged.pending_key {
            self.core
                .claim_cursor
                .note_pending_insert(&staged.queue, pending_key);
        }
        debug!(queue = %staged.queue, job_id = %staged.id, "job enqueued");
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
        let job = claim.job();
        let prepared = self.prepare_effects(effects).await?;

        let timer = crate::obs::start();
        let token = claim.token();
        let keep_done = self.queue_keep_done_jobs(&job.queue).is_some();
        let completed_at = self.now_ms();
        let done_record = if keep_done {
            // Stored form: an offloaded payload stays in its object, which
            // is retained with the done record and deleted by the
            // retention sweep.
            let mut done_job = job.stored_clone();
            done_job.completed_at = Some(completed_at);
            Some((
                done_key(completed_at, &job.queue, &job.id),
                done_job.stored_bytes()?,
            ))
        } else {
            None
        };

        let outcome = self
            .ack_txn(job, token, completed_at, done_record.as_ref(), &prepared)
            .await;

        self.finish_effects(prepared, outcome.as_ref().ok().map(|r| r.as_slice()))
            .await;
        let results = outcome?;
        // After the commit and token-fenced, so a removal that runs
        // after a re-claim leaves the new claim's entry.
        self.core.lease_registry.remove(&job.queue, &job.id, token);

        // The acked job's record is gone unless a done record was kept;
        // without one, its payload object is removed here, after the
        // commit.
        if !keep_done {
            self.core.payload_store.delete_for(job).await;
        }

        crate::obs::completed(&job.queue, timer);
        self.core.completion_waiters.settle(&job.id, || {
            // The claim's copy carries the payload as delivered to the
            // worker, so the record matches what `get_job` returns.
            let mut delivered = job.clone();
            delivered.status = JobStatus::Done;
            delivered.completed_at = Some(completed_at);
            WaitOutcome::Done(Box::new(delivered))
        });
        debug!(queue = %job.queue, job_id = %job.id, "job acked");
        Ok(results)
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
        let prepared = self.prepare_effects(effects).await?;
        let token = claim.token();
        let (queue, id) = (claim.job().queue.as_str(), claim.job().id.as_str());

        let settled = self.nack_txn(queue, id, token, error, &prepared).await;

        self.finish_effects(
            prepared,
            match &settled {
                Ok((_, Some(results))) => Some(results.as_slice()),
                _ => None,
            },
        )
        .await;
        let (job, results) = settled?;

        let immediate_retry = matches!(job.status, JobStatus::Pending);
        let became_dead = matches!(job.status, JobStatus::Dead);
        if became_dead {
            crate::obs::dead_lettered(&job.queue);
        } else {
            crate::obs::nacked(&job.queue);
        }
        self.core.lease_registry.remove(&job.queue, &job.id, token);
        if immediate_retry {
            // The backoff path needs no insert note here: the
            // scheduler records the insert itself when it promotes the
            // job.
            let pending = pending_key(&job.queue, job.priority, &job.id);
            self.core
                .claim_cursor
                .note_pending_insert(&job.queue, &pending);
        }
        if became_dead {
            // Retries exhausted: terminal transition.
            self.core.completion_waiters.settle(&job.id, || {
                WaitOutcome::Dead(Box::new(delivered_record(&job, claim)))
            });
        }
        match results {
            Some(results) => Ok(NackOutcome::DeadLettered(results)),
            None => Ok(NackOutcome::Retried),
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
        let prepared = self.prepare_effects(effects).await?;
        let token = claim.token();
        let (queue, id) = (claim.job().queue.as_str(), claim.job().id.as_str());
        let failed_at = self.now_ms();

        let settled = self
            .dead_letter_txn(queue, id, token, failed_at, reason, &prepared)
            .await;

        self.finish_effects(
            prepared,
            settled.as_ref().ok().map(|(_, results)| results.as_slice()),
        )
        .await;
        let (job, results) = settled?;

        crate::obs::dead_lettered(&job.queue);
        self.core.lease_registry.remove(&job.queue, &job.id, token);
        self.core.completion_waiters.settle(&job.id, || {
            WaitOutcome::Dead(Box::new(delivered_record(&job, claim)))
        });
        Ok(results)
    }

    /// The transaction loop of [`Self::ack_with`]: fence the claim,
    /// write or remove the record, stage the effects and commit.
    async fn ack_txn(
        &self,
        job: &JobRecord,
        token: u64,
        completed_at: u64,
        done_record: Option<&(Vec<u8>, Vec<u8>)>,
        prepared: &PreparedEffects,
    ) -> Result<Vec<EnqueueResult>> {
        loop {
            let txn = self.core.db.begin(IsolationLevel::Snapshot).await?;
            take_claim(&txn, &self.core.lease_registry, &job.queue, &job.id, token).await?;
            if let Some((done_k, done_v)) = done_record {
                put_job_record(&txn, done_k, &job_index_key(&job.id), done_v)?;
                append_attempt(
                    &txn,
                    &job.id,
                    &JobAttempt {
                        attempt: job.attempts,
                        claimed_at: job.claimed_at,
                        recorded_at: completed_at,
                        outcome: AttemptOutcome::Completed,
                        error: None,
                    },
                )?;
            } else {
                // Default: drop the index pointer too; the ID is no longer
                // findable via get_job, but the queue stays small. The
                // attempt history shares the record's lifetime.
                txn.delete(job_index_key(&job.id))?;
                txn.delete(attempt_history_key(&job.id))?;
            }
            update_stats(
                &txn,
                &job.queue,
                &[(JobStatus::Claimed, -1), (JobStatus::Done, 1)],
            )?;

            let (results, staged) = self.stage_effects(&txn, prepared).await?;

            match commit(txn, Durability::Awaited).await? {
                Commit::Committed => {
                    for staged_job in &staged {
                        self.note_staged_job(staged_job);
                    }
                    return Ok(results);
                }
                Commit::Conflict => continue,
            }
        }
    }

    /// The transaction loop of [`Self::nack_with`]: fence the claim,
    /// dead-letter or requeue the job and commit. Returns the written
    /// record and, on the dead-letter branch, the effects' results.
    async fn nack_txn(
        &self,
        queue: &str,
        id: &str,
        token: u64,
        error: &str,
        prepared: &PreparedEffects,
    ) -> Result<(JobRecord, Option<Vec<EnqueueResult>>)> {
        loop {
            let txn = self.core.db.begin(IsolationLevel::Snapshot).await?;
            // The returned record is the base for the written record;
            // the claim's copy predates a cancel committed during the
            // delivery.
            let mut job = take_claim(&txn, &self.core.lease_registry, queue, id, token).await?;
            let now = self.now_ms();

            let staged = if job.attempts >= job.max_attempts {
                stage_dead_letter(&txn, &mut job, now, error)?;
                Some(self.stage_effects(&txn, prepared).await?)
            } else {
                job.last_error = Some(error.to_string());
                append_attempt(
                    &txn,
                    &job.id,
                    &JobAttempt {
                        attempt: job.attempts,
                        claimed_at: job.claimed_at.take(),
                        recorded_at: now,
                        outcome: AttemptOutcome::Retried,
                        error: Some(error.to_string()),
                    },
                )?;
                let cfg = self.queue_config(&job.queue);
                let backoff =
                    backoff_delay(job.attempts, cfg.retry_backoff_base, cfg.retry_backoff_max);

                if backoff.is_zero() {
                    stage_to_pending(&txn, &mut job, JobStatus::Claimed)?;
                    debug!(
                        queue = %job.queue,
                        job_id = %job.id,
                        attempts = job.attempts,
                        "job re-queued"
                    );
                } else {
                    let run_at = now + backoff.as_millis() as u64;
                    job.status = JobStatus::Scheduled;
                    job.run_at = Some(run_at);
                    let scheduled = scheduled_key(&job.queue, run_at, &job.id);
                    let value = job.stored_bytes()?;
                    put_job_record(&txn, &scheduled, &job_index_key(&job.id), &value)?;
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
                None
            };

            match commit(txn, Durability::Awaited).await? {
                Commit::Committed => {
                    let results = staged.map(|(results, staged_jobs)| {
                        for staged_job in &staged_jobs {
                            self.note_staged_job(staged_job);
                        }
                        results
                    });
                    return Ok((job, results));
                }
                Commit::Conflict => continue,
            }
        }
    }

    /// The transaction loop of [`Self::dead_letter_with`]: fence the
    /// claim, dead-letter the job, stage the effects and commit.
    async fn dead_letter_txn(
        &self,
        queue: &str,
        id: &str,
        token: u64,
        failed_at: u64,
        reason: &str,
        prepared: &PreparedEffects,
    ) -> Result<(JobRecord, Vec<EnqueueResult>)> {
        loop {
            let txn = self.core.db.begin(IsolationLevel::Snapshot).await?;
            let mut job = take_claim(&txn, &self.core.lease_registry, queue, id, token).await?;
            stage_dead_letter(&txn, &mut job, failed_at, reason)?;
            let (results, staged) = self.stage_effects(&txn, prepared).await?;
            match commit(txn, Durability::Awaited).await? {
                Commit::Committed => {
                    for staged_job in &staged {
                        self.note_staged_job(staged_job);
                    }
                    return Ok((job, results));
                }
                Commit::Conflict => continue,
            }
        }
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
        let prepared = self.prepare_effects(effects).await?;
        let outcome = self.cancel_txn(id, &prepared).await;

        self.finish_effects(
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
                    let (results, staged) = self.stage_effects(&txn, prepared).await?;
                    (
                        "pending/scheduled job cancelled",
                        CancelOutcome::Removed,
                        Some((results, staged)),
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
                    let results = match staged {
                        Some((results, staged_jobs)) => {
                            for staged_job in &staged_jobs {
                                self.note_staged_job(staged_job);
                            }
                            results
                        }
                        None => Vec::new(),
                    };
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

        let mut prepared = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let (job, key, id_override_used) =
                self.prepare_job_record(queue, payload, EnqueueOptions::default())?;
            prepared.push(PreparedJob {
                job,
                key,
                id_override_used,
            });
        }
        self.core
            .payload_store
            .offload_all(prepared.iter_mut().map(|p| &mut p.job))
            .await?;

        let write = async {
            loop {
                let txn = self.core.db.begin(IsolationLevel::Snapshot).await?;
                let mut staged = Vec::with_capacity(prepared.len());
                for prepared_job in &prepared {
                    match self.stage_job_writes(&txn, prepared_job).await? {
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
                for prepared_job in &prepared {
                    self.core.payload_store.delete_for(&prepared_job.job).await;
                }
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
        persist_cursor_state(&self.core).await?;
        self.core.db.close().await?;
        Ok(())
    }
}

/// Resolve a job id through its index entry within `txn`: returns the
/// index key, the record's current key and the decoded record, or
/// `None` when the id is not indexed or the indexed key holds no
/// record.
async fn get_indexed_job(
    txn: &DbTransaction,
    id: &str,
) -> Result<Option<(Vec<u8>, Bytes, JobRecord)>> {
    let index_key = job_index_key(id);
    let Some(current_key) = txn.get(&index_key).await? else {
        return Ok(None);
    };
    let Some(bytes) = txn.get(&current_key).await? else {
        return Ok(None);
    };
    let job = JobRecord::decode(&current_key, &bytes)?;
    Ok(Some((index_key, current_key, job)))
}

/// Write each queue's claim-scan state under its cursor key. Runs
/// after the background tasks have stopped; `close` consumes the
/// handle, so the exported state cannot change between the export and
/// the database closing.
async fn persist_cursor_state(core: &QueueCore) -> Result<()> {
    let states = core.claim_cursor.export();
    if states.is_empty() {
        return Ok(());
    }
    let txn = core.db.begin(IsolationLevel::Snapshot).await?;
    for (queue, state) in states {
        let record = PersistedCursor {
            queue: queue.clone(),
            bound_key: state.scan_from.as_ref().map(|sf| sf.key.to_vec()),
            bound_inclusive: state.scan_from.is_some_and(|sf| sf.inclusive),
            known_empty: state.known_empty,
        };
        txn.put(cursor_key(&queue), &rmp_serde::to_vec_named(&record)?)?;
    }
    txn.commit().await?;
    Ok(())
}

/// Restore the claim cursor from cursor records persisted by the
/// previous clean close, then durably delete them before the queue
/// serves traffic. A record is valid only as of the close that wrote
/// it: once inserts resume the live bound can move behind the
/// persisted one, so a crash before the delete is durable would leave
/// a record whose stale bound lets a later open strand jobs behind it.
async fn restore_cursor_state(core: &QueueCore) -> Result<()> {
    let txn = core.db.begin(IsolationLevel::Snapshot).await?;
    let mut records = Vec::new();
    {
        let mut iter = txn.scan_prefix(tag_prefix(KeyTag::Cursor), ..).await?;
        while let Some(kv) = iter.next().await? {
            let record: PersistedCursor = rmp_serde::from_slice(&kv.value)?;
            records.push((kv.key, record));
        }
    }
    if records.is_empty() {
        return Ok(());
    }
    for (key, record) in records {
        core.claim_cursor.restore(
            &record.queue,
            CursorState {
                scan_from: record.bound_key.map(|key| ScanFrom {
                    key: Bytes::from(key),
                    inclusive: record.bound_inclusive,
                }),
                known_empty: record.known_empty,
            },
        );
        txn.delete(&key)?;
    }
    txn.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;
    use crate::lease::LeaseHandle;
    use slatedb::object_store::memory::InMemory;

    fn make_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    use futures_core::stream::BoxStream;
    use futures_util::StreamExt;
    use slatedb::object_store::{
        CopyOptions, Error as StoreError, GetOptions, GetResult, ListResult, MultipartUpload,
        ObjectMeta, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
        path::Path as StorePath,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// In-memory object store whose `put` and `delete` requests fail with
    /// a synthetic service-unavailable error while the corresponding flag
    /// is set. Reads and lists are unaffected.
    #[derive(Debug)]
    struct FaultStore {
        inner: Arc<dyn ObjectStore>,
        fail_puts: AtomicBool,
        fail_deletes: AtomicBool,
        /// Puts permitted before every later put fails. `usize::MAX`
        /// disables the countdown, leaving `fail_puts` as the only cause
        /// of failure.
        puts_before_failure: AtomicUsize,
    }

    impl FaultStore {
        fn wrap() -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(InMemory::new()),
                fail_puts: AtomicBool::new(false),
                fail_deletes: AtomicBool::new(false),
                puts_before_failure: AtomicUsize::new(usize::MAX),
            })
        }

        fn fail_puts(&self, fail: bool) {
            self.fail_puts.store(fail, Ordering::SeqCst);
        }

        fn fail_deletes(&self, fail: bool) {
            self.fail_deletes.store(fail, Ordering::SeqCst);
        }

        /// Permit `n` further puts, then fail every put after them.
        fn fail_puts_after(&self, n: usize) {
            self.puts_before_failure.store(n, Ordering::SeqCst);
        }

        /// Whether this put fails, consuming one permitted put when it
        /// does not. Callers of the payload store issue puts
        /// sequentially, so a read followed by a store is sufficient.
        fn put_fails(&self) -> bool {
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

        fn synthetic_503() -> StoreError {
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

        async fn get_opts(
            &self,
            location: &StorePath,
            options: GetOptions,
        ) -> StoreResult<GetResult> {
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

    #[tokio::test(start_paused = true)]
    async fn ack_with_applies_no_effects_when_a_crash_interrupts_a_stalled_settlement() {
        let store = FaultStore::wrap();
        let clock = MockClock::new(1_700_000_000_000);
        let opts = || OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(store.clone(), "test", opts())
            .await
            .unwrap();
        let effects = || SettlementEffects {
            enqueues: vec![EnqueueRequest {
                queue: "next".to_string(),
                payload: b"follow".to_vec(),
                options: EnqueueOptions::default(),
            }],
            kv_writes: HashMap::from([(b"runs/1".to_vec(), b"done".to_vec())]),
            kv_deletes: Vec::new(),
        };
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        // A durable barrier write, so the claim record is flushed before
        // faults are enabled and the crash loses only the settlement.
        q.kv_put(b"barrier", b"x").await.unwrap();

        // The settlement stalls on the unavailable store (SlateDB retries
        // transient put errors with backoff, driven virtually by the
        // paused runtime); the elapsed timeout drops the in-flight call
        // and the queue is dropped without a close, simulating a crash
        // mid-outage.
        store.fail_puts(true);
        let stalled =
            tokio::time::timeout(Duration::from_secs(30), q.ack_with(&job, effects())).await;
        assert!(stalled.is_err());
        drop(q);

        store.fail_puts(false);
        let q = Queue::open_with_options(store, "test", opts())
            .await
            .unwrap();
        // None of the settlement's effects survived the crash.
        assert!(q.kv_get(b"runs/1").await.unwrap().is_none());
        assert!(
            q.claim("next", Duration::from_secs(5))
                .await
                .unwrap()
                .is_none()
        );
        // The job is still owned by the crashed claim; expire the lease
        // and redeliver.
        clock.advance(Duration::from_secs(60));
        q.reap_now().await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.payload, b"job");
        let results = q.ack_with(&job, effects()).await.unwrap();
        assert!(matches!(results[0], EnqueueResult::New(_)));
        assert_eq!(
            q.kv_get(b"runs/1").await.unwrap().as_deref(),
            Some(b"done".as_slice())
        );
        let follow = q
            .claim("next", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(follow.payload, b"follow");
        assert!(
            q.claim("next", Duration::from_secs(5))
                .await
                .unwrap()
                .is_none()
        );
        q.close().await.unwrap();
    }

    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn metrics_sampler_emits_pending_depth_gauge() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // Only this test installs a global recorder (the obs unit test uses a
        // local one), so the install succeeds and the snapshotter observes the
        // sampler running in its spawned task.
        let _ = recorder.install();

        let q = Queue::open_with_options(
            make_store(),
            "test",
            OpenOptions {
                metrics_sample_interval: Some(Duration::from_millis(25)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        for _ in 0..3 {
            q.enqueue("gsamp", vec![0u8; 8]).await.unwrap();
        }

        let mut gauge = None;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            for (composite, _unit, _desc, value) in snapshotter.snapshot().into_vec() {
                let key = composite.key();
                let ours = key.name() == "taquba_pending_jobs"
                    && key
                        .labels()
                        .any(|l| l.key() == "queue" && l.value() == "gsamp");
                if ours && let DebugValue::Gauge(g) = value {
                    gauge = Some(g.into_inner());
                }
            }
            if gauge == Some(3.0) {
                break;
            }
        }
        assert_eq!(gauge, Some(3.0), "sampler should report 3 pending jobs");
        q.close().await.unwrap();
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
    async fn claim_finds_job_enqueued_after_empty_polls() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        assert!(q.claim("work", lease).await.unwrap().is_none());
        assert!(q.claim("work", lease).await.unwrap().is_none());

        q.enqueue("work", b"job".to_vec()).await.unwrap();

        let job = q.claim("work", lease).await.unwrap().unwrap();
        assert_eq!(job.payload, b"job");
        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn claim_finds_batch_enqueued_after_queue_drained() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        q.enqueue("work", b"first".to_vec()).await.unwrap();
        let first = q.claim("work", lease).await.unwrap().unwrap();
        q.ack(&first).await.unwrap();
        assert!(q.claim("work", lease).await.unwrap().is_none());
        assert!(q.claim("work", lease).await.unwrap().is_none());

        q.enqueue_batch("work", vec![b"second".to_vec()])
            .await
            .unwrap();

        let second = q.claim("work", lease).await.unwrap().unwrap();
        assert_eq!(second.payload, b"second");
        q.ack(&second).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn enqueue_wakes_one_waiting_worker_per_job() {
        let q = Arc::new(Queue::open(make_store(), "test").await.unwrap());
        let lease = Duration::from_secs(5);
        let max_wait = Duration::from_secs(60);

        let mut waiters = Vec::new();
        for _ in 0..3 {
            let q = q.clone();
            waiters.push(tokio::spawn(async move {
                q.claim_with_wait("work", lease, max_wait).await.unwrap()
            }));
        }
        tokio::task::yield_now().await;

        q.enqueue("work", b"job".to_vec()).await.unwrap();

        let mut claimed = 0;
        for handle in waiters {
            if let Some(job) = handle.await.unwrap() {
                claimed += 1;
                q.ack(&job).await.unwrap();
            }
        }
        assert_eq!(claimed, 1, "exactly one waiter wakes with the job");
    }

    #[tokio::test(start_paused = true)]
    async fn batch_enqueue_wakes_one_waiting_worker_per_job() {
        let q = Arc::new(Queue::open(make_store(), "test").await.unwrap());
        let lease = Duration::from_secs(5);
        let max_wait = Duration::from_secs(60);

        let mut waiters = Vec::new();
        for _ in 0..3 {
            let q = q.clone();
            waiters.push(tokio::spawn(async move {
                q.claim_with_wait("work", lease, max_wait).await.unwrap()
            }));
        }
        tokio::task::yield_now().await;

        q.enqueue_batch("work", vec![b"a".to_vec(), b"b".to_vec()])
            .await
            .unwrap();

        let mut claimed = 0;
        for handle in waiters {
            if let Some(job) = handle.await.unwrap() {
                claimed += 1;
                q.ack(&job).await.unwrap();
            }
        }
        assert_eq!(claimed, 2, "one waiter wakes per inserted job");
    }

    #[tokio::test(start_paused = true)]
    async fn claim_with_wait_waits_full_deadline_despite_stale_permit() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);

        // A successful claim_with_wait passes the wakeup on, leaving a
        // stale permit behind when no task is waiting.
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q
            .claim_with_wait("work", lease, Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();

        let start = tokio::time::Instant::now();
        let next = q
            .claim_with_wait("work", lease, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(next.is_none());
        assert!(
            start.elapsed() >= Duration::from_secs(5),
            "stale permit must not end the wait early",
        );
        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn wait_for_jobs_on_consumes_permit_from_earlier_insert() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        q.enqueue("work", b"job".to_vec()).await.unwrap();

        let start = tokio::time::Instant::now();
        q.wait_for_jobs_on("work", Duration::from_secs(60)).await;
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "insert before the wait must wake it via the stored permit",
        );

        let job = q.claim("work", Duration::from_secs(5)).await.unwrap();
        q.ack(&job.unwrap()).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn claim_batch_claims_in_order_up_to_max() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        for payload in [b"a", b"b", b"c", b"d", b"e"] {
            q.enqueue("work", payload.to_vec()).await.unwrap();
        }

        let first = q.claim_batch("work", 3, lease).await.unwrap();
        assert_eq!(
            first
                .iter()
                .map(|j| j.payload.as_slice())
                .collect::<Vec<_>>(),
            [b"a", b"b", b"c"],
        );
        for job in &first {
            assert_eq!(job.status, JobStatus::Claimed);
            assert_eq!(job.attempts, 1);
            assert!(q.lease_expiry("work", &job.id).is_some());
        }

        let rest = q.claim_batch("work", 3, lease).await.unwrap();
        assert_eq!(
            rest.iter()
                .map(|j| j.payload.as_slice())
                .collect::<Vec<_>>(),
            [b"d", b"e"],
        );

        for job in first.iter().chain(rest.iter()) {
            q.ack(job).await.unwrap();
        }
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn claim_batch_zero_max_claims_nothing() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        q.enqueue("work", b"job".to_vec()).await.unwrap();

        assert!(
            q.claim_batch("work", 0, Duration::from_secs(5))
                .await
                .unwrap()
                .is_empty(),
        );

        let job = q
            .claim("work", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn partial_claim_batch_marks_empty_until_next_enqueue() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        q.enqueue("work", b"only".to_vec()).await.unwrap();

        let batch = q.claim_batch("work", 8, lease).await.unwrap();
        assert_eq!(batch.len(), 1);
        assert!(q.claim("work", lease).await.unwrap().is_none());

        q.enqueue("work", b"next".to_vec()).await.unwrap();
        let next = q.claim("work", lease).await.unwrap().unwrap();
        assert_eq!(next.payload, b"next");

        q.ack(&batch[0]).await.unwrap();
        q.ack(&next).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn claim_finds_job_requeued_by_nack_after_empty_poll() {
        let q = Queue::open_with_options(make_store(), "test", no_backoff_opts())
            .await
            .unwrap();
        let lease = Duration::from_secs(5);
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();
        assert!(q.claim("work", lease).await.unwrap().is_none());

        q.nack(&job, "retry").await.unwrap();

        let retried = q.claim("work", lease).await.unwrap().unwrap();
        assert_eq!(retried.payload, b"job");
        assert_eq!(retried.attempts, 2);
        q.ack(&retried).await.unwrap();
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
            q.nack(&job, "persistent error").await.unwrap();
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
        q.nack(&job, "fail").await.unwrap();

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
        q.nack(&job, "fatal").await.unwrap();

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
        let lease_expires_at = q.lease_expiry("fast", &job.id).unwrap();
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

    #[tokio::test(start_paused = true)]
    async fn test_promote_scheduled_now() {
        let initial = 1_700_000_000_000u64;
        let clock = MockClock::new(initial);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial + 100);
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

        // Advance past `run_at` and trigger a manual promotion.
        clock.advance(Duration::from_millis(200));
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
    async fn test_wake_scheduled_promotes_before_run_at() {
        let initial = 1_700_000_000_000u64;
        let clock = MockClock::new(initial);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial + 60_000);
        let id = q
            .enqueue_with(
                "jobs",
                b"waiting".to_vec(),
                EnqueueOptions {
                    run_at: Some(run_at),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(
            q.claim("jobs", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        let outcome = q
            .wake_scheduled(&id, Some(b"signal".to_vec()))
            .await
            .unwrap();
        assert_eq!(outcome, WakeOutcome::Woken);

        let s = q.stats("jobs").await.unwrap();
        assert_eq!(s.scheduled, 0);
        assert_eq!(s.pending, 1);

        let job = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, id);
        assert_eq!(job.wake_payload.as_deref(), Some(b"signal".as_slice()));
        assert_eq!(job.woken_at, Some(initial));
        assert!(job.run_at.is_none());

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wake_scheduled_without_payload() {
        let initial = 1_700_000_000_000u64;
        let clock = MockClock::new(initial);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial + 60_000);
        let id = q
            .enqueue_with(
                "jobs",
                b"waiting".to_vec(),
                EnqueueOptions {
                    run_at: Some(run_at),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(
            q.wake_scheduled(&id, None).await.unwrap(),
            WakeOutcome::Woken
        );

        let job = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert!(job.wake_payload.is_none());
        assert_eq!(job.woken_at, Some(initial));

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wake_scheduled_non_scheduled_outcomes() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let id = q.enqueue("jobs", b"p".to_vec()).await.unwrap();
        assert_eq!(
            q.wake_scheduled(&id, None).await.unwrap(),
            WakeOutcome::NotScheduled
        );

        let job = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            q.wake_scheduled(&job.id, None).await.unwrap(),
            WakeOutcome::NotScheduled
        );

        assert_eq!(
            q.wake_scheduled("01ARZ3NDEKTSV4RRFFQ69G5FAV", None)
                .await
                .unwrap(),
            WakeOutcome::NotFound
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wake_scheduled_after_cancel_is_not_found() {
        let initial = 1_700_000_000_000u64;
        let clock = MockClock::new(initial);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial + 60_000);
        let id = q
            .enqueue_with(
                "jobs",
                b"waiting".to_vec(),
                EnqueueOptions {
                    run_at: Some(run_at),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Removed);
        assert_eq!(
            q.wake_scheduled(&id, None).await.unwrap(),
            WakeOutcome::NotFound
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wake_scheduled_after_promotion_is_not_scheduled() {
        let initial = 1_700_000_000_000u64;
        let clock = MockClock::new(initial);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial + 100);
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

        clock.advance(Duration::from_millis(200));
        q.promote_scheduled_now().await.unwrap();

        assert_eq!(
            q.wake_scheduled(&id, Some(b"late".to_vec())).await.unwrap(),
            WakeOutcome::NotScheduled
        );

        let job = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert!(job.wake_payload.is_none());
        assert!(job.woken_at.is_none());

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wake_payload_persists_across_redelivery() {
        let initial = 1_700_000_000_000u64;
        let clock = MockClock::new(initial);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial + 60_000);
        let id = q
            .enqueue_with(
                "jobs",
                b"waiting".to_vec(),
                EnqueueOptions {
                    run_at: Some(run_at),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(
            q.wake_scheduled(&id, Some(b"signal".to_vec()))
                .await
                .unwrap(),
            WakeOutcome::Woken
        );

        let job = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.nack(&job, "worker failed").await.unwrap();

        // The retry backoff moves the job back to `scheduled`; promote it
        // and verify the redelivered record still carries the wake payload.
        clock.advance(Duration::from_secs(5));
        q.promote_scheduled_now().await.unwrap();

        let job = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, id);
        assert_eq!(job.attempts, 2);
        assert_eq!(job.wake_payload.as_deref(), Some(b"signal".as_slice()));
        assert_eq!(job.woken_at, Some(initial));

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
        let q = Queue::open(make_store(), "test").await.unwrap();

        q.enqueue("jobs", b"payload".to_vec()).await.unwrap();
        q.enqueue_with(
            "jobs",
            b"later".to_vec(),
            EnqueueOptions {
                run_at: Some(std::time::SystemTime::now() + Duration::from_secs(3600)),
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

    #[tokio::test(start_paused = true)]
    async fn test_scheduled_job_preserves_priority() {
        let initial = 1_700_000_000_000u64;
        let clock = MockClock::new(initial);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial + 1);
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

        clock.advance(Duration::from_millis(5));
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
    async fn test_run_worker_dead_letters_on_permanent_failure() {
        // A Worker returning PermanentFailure should dead-letter immediately,
        // skipping the retry/backoff path that a plain error takes.
        use crate::worker::{PermanentFailure, Worker, WorkerError, run_worker};

        struct PermanentFailWorker;
        impl Worker for PermanentFailWorker {
            async fn process(
                &self,
                _job: &JobRecord,
                _lease: &LeaseHandle,
            ) -> std::result::Result<(), WorkerError> {
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
            async fn process(
                &self,
                _job: &JobRecord,
                _lease: &LeaseHandle,
            ) -> std::result::Result<(), WorkerError> {
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
            q.nack(&job, "fatal").await.unwrap();
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
        q.nack(&job, "fatal").await.unwrap();

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
        q.nack(&job, "fatal").await.unwrap();

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

    #[tokio::test(start_paused = true)]
    async fn test_requeue_dead_rejects_stale_record_after_retention_sweep() {
        let clock = MockClock::new(1_700_000_000_000);
        let reaper_interval = Duration::from_millis(10);
        let retention = Duration::from_millis(50);
        let q = Queue::open_with_options(
            make_store(),
            "test",
            OpenOptions {
                reaper_interval,
                default_queue_config: QueueConfig {
                    dead_retention: Some(retention),
                    ..Default::default()
                },
                clock: Arc::new(clock.clone()),
                ..Default::default()
            },
        )
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
        q.nack(&job, "fatal").await.unwrap();

        let dead = q.dead_jobs("work", None, 100).await.unwrap().pop().unwrap();
        clock.advance(retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;

        assert!(q.dead_jobs("work", None, 100).await.unwrap().is_empty());
        let err = q.requeue_dead_job(dead).await.unwrap_err();
        assert!(matches!(err, Error::JobNotFound(_)));
        assert_eq!(q.stats("work").await.unwrap().pending, 0);
        assert_eq!(q.stats("work").await.unwrap().dead, 0);

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
        q.nack(&job, "fatal").await.unwrap();

        let dead = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(dead.status, JobStatus::Dead);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_renew_lease() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_millis(1))
            .await
            .unwrap()
            .unwrap();
        let original_expiry = q.lease_expiry("work", &job.id).unwrap();

        let new_expiry = q.renew_lease(&job, Duration::from_secs(30)).unwrap();
        assert!(new_expiry > original_expiry, "renewed expiry must be later");

        // Reaper skips the renewed lease even once the original expiry
        // has passed.
        clock.advance(Duration::from_secs(1));
        q.reap_now().await.unwrap();
        assert!(
            q.claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        let fetched = q.get_job(&job.id).await.unwrap().unwrap();
        assert_eq!(fetched.status, JobStatus::Claimed);
        // The claim still holds, so the original handle settles it.
        q.ack(&job).await.unwrap();

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn renewal_is_refused_once_cancellation_is_requested() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let lease = q.lease_handle(&job);
        lease.ensure_at_least(Duration::from_secs(60)).unwrap();

        assert_eq!(q.cancel(&job.id).await.unwrap(), CancelOutcome::Requested);

        let expiry = q.lease_expiry("work", &job.id);
        assert!(matches!(
            q.renew_lease(&job, Duration::from_secs(600)),
            Err(Error::CancelRequested)
        ));
        assert!(matches!(
            lease.ensure_at_least(Duration::from_secs(600)),
            Err(Error::CancelRequested)
        ));
        assert_eq!(q.lease_expiry("work", &job.id), expiry);

        // The claim is still held, so the delivery settles as usual.
        q.nack(&job, "cancelled").await.unwrap();

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn renewal_leaves_the_claim_settleable() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        clock.advance(Duration::from_secs(10));
        let renewed = q.renew_lease(&job, Duration::from_secs(60)).unwrap();
        assert_eq!(q.lease_expiry("work", &job.id), Some(renewed));

        // The claim taken before the renewal keeps its token, so it
        // still settles the delivery.
        q.ack(&job).await.unwrap();

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.done, 1);
        assert_eq!(stats.claimed, 0);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn reaping_leaves_no_claim_state() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        // Two jobs, one with retries left and one out of attempts, so the
        // requeue and dead-letter branches are both covered.
        let retried = q.enqueue("work", b"a".to_vec()).await.unwrap();
        let doomed = q
            .enqueue_with(
                "work",
                b"b".to_vec(),
                EnqueueOptions {
                    max_attempts: Some(1),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .unwrap();
        let claimed = q
            .claim_batch("work", 2, Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(claimed.len(), 2);

        clock.advance(Duration::from_secs(31));
        q.reap_now().await.unwrap();

        for id in [&retried, &doomed] {
            assert!(q.core.lease_registry.current("work", id).is_none());
            assert!(
                q.core
                    .db
                    .get(&claimed_key("work", id))
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.claimed, 0);
        assert_eq!(stats.pending, 1);
        assert_eq!(stats.dead, 1);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn stale_settlement_after_a_reclaim_is_rejected() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let stale = q
            .claim("work", Duration::from_millis(1))
            .await
            .unwrap()
            .unwrap();

        clock.advance(Duration::from_millis(2));
        q.reap_now().await.unwrap();

        // The re-claim writes the same claimed key the stale copy names,
        // so only the claim token separates the two deliveries.
        let fresh = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fresh.id, id);

        assert!(matches!(q.ack(&stale).await, Err(Error::ClaimLost)));
        assert!(matches!(
            q.nack(&stale, "late failure").await,
            Err(Error::ClaimLost)
        ));
        assert!(matches!(
            q.dead_letter(&stale, "late permanent failure").await,
            Err(Error::ClaimLost)
        ));

        // The live claim is untouched by the rejected settlements.
        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.claimed, 1);
        assert_eq!(stats.done, 0);
        assert_eq!(stats.dead, 0);
        q.ack(&fresh).await.unwrap();

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn settling_after_renewal_leaves_no_lease_entry() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let renewed = q.renew_lease(&job, Duration::from_secs(60)).unwrap();

        // The ack removes the lease entry the renewal moved.
        q.ack(&job).await.unwrap();
        assert!(q.core.lease_registry.current("work", &job.id).is_none());
        assert!(q.lease_expiry("work", &job.id).is_none());

        // Nothing is left to come due, so the reaper requeues nothing,
        // even past the renewed expiry.
        assert!(renewed > clock.now_ms());
        clock.advance(Duration::from_secs(61));
        q.reap_now().await.unwrap();
        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.done, 1);
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.dead, 0);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn settlement_is_rejected_when_the_registry_entry_outlives_the_claim() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let claim = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&claim).await.unwrap();

        // The registry lags the store: an entry is removed only after
        // the commit that ends its claim, so a settlement transaction
        // begun inside that lag passes the token check and conflicts
        // with nothing. Recreate the lagging entry and require the
        // in-transaction record read to reject the settlement.
        q.core.lease_registry.insert(
            "work",
            &claim.id,
            clock.now_ms() + 30_000,
            claim.token(),
            claim.cancel_token().clone(),
        );
        assert!(matches!(
            q.nack(&claim, "late failure").await,
            Err(Error::ClaimLost)
        ));
        assert!(matches!(q.ack(&claim).await, Err(Error::ClaimLost)));

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.done, 1);
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.dead, 0);
        assert_eq!(stats.claimed, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn lease_expiry_reports_the_renewed_expiry_not_the_claim_time_one() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let claim_time_expiry = q.lease_expiry("work", &job.id).unwrap();

        clock.advance(Duration::from_secs(10));
        let renewed = q.renew_lease(&job, Duration::from_secs(60)).unwrap();
        assert!(renewed > claim_time_expiry);
        assert_eq!(q.lease_expiry("work", &job.id), Some(renewed));

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn stale_settlement_after_reaper_requeue_is_rejected() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let stale = q
            .claim("work", Duration::from_millis(1))
            .await
            .unwrap()
            .unwrap();

        clock.advance(Duration::from_millis(2));
        q.reap_now().await.unwrap();

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.pending, 1);
        assert_eq!(stats.claimed, 0);

        assert!(matches!(q.ack(&stale).await, Err(Error::ClaimLost)));
        assert!(matches!(
            q.nack(&stale, "late failure").await,
            Err(Error::ClaimLost)
        ));
        assert!(matches!(
            q.dead_letter(&stale, "late permanent failure").await,
            Err(Error::ClaimLost)
        ));

        assert!(matches!(
            q.renew_lease(&stale, Duration::from_secs(30)),
            Err(Error::ClaimLost)
        ));

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.pending, 1);
        assert_eq!(stats.claimed, 0);
        assert_eq!(stats.done, 0);
        assert_eq!(stats.dead, 0);

        let fresh = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fresh.id, id);
        assert_eq!(fresh.attempts, 2);
        q.ack(&fresh).await.unwrap();

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.claimed, 0);
        assert_eq!(stats.done, 1);
        assert_eq!(stats.dead, 0);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn stale_settlement_after_reaper_dead_letter_is_rejected() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
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
                    max_attempts: Some(1),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let stale = q
            .claim("work", Duration::from_millis(1))
            .await
            .unwrap()
            .unwrap();

        clock.advance(Duration::from_millis(2));
        q.reap_now().await.unwrap();

        let dead = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(dead.status, JobStatus::Dead);
        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.claimed, 0);
        assert_eq!(stats.dead, 1);

        assert!(matches!(q.ack(&stale).await, Err(Error::ClaimLost)));
        assert!(matches!(
            q.nack(&stale, "late failure").await,
            Err(Error::ClaimLost)
        ));
        assert!(matches!(
            q.dead_letter(&stale, "late permanent failure").await,
            Err(Error::ClaimLost)
        ));

        assert!(matches!(
            q.renew_lease(&stale, Duration::from_secs(30)),
            Err(Error::ClaimLost)
        ));

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.claimed, 0);
        assert_eq!(stats.done, 0);
        assert_eq!(stats.dead, 1);
        assert!(
            q.claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn ack_succeeds_on_expired_lease_before_reaper_runs() {
        // Settlement is fenced on the claim token; the claim stays
        // settleable past its lease expiry until the reaper requeues
        // the job.
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_millis(1))
            .await
            .unwrap()
            .unwrap();

        clock.advance(Duration::from_secs(5));

        q.ack(&job).await.unwrap();

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.claimed, 0);
        assert_eq!(stats.done, 1);
        assert_eq!(stats.dead, 0);
        assert!(
            q.claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

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
    async fn test_cancel_persists_across_reaper_requeue() {
        // Claim -> cancel -> drop the job back to pending via the reaper
        // (lease elapsed) -> re-claim sees cancel_requested and a pre-fired token.
        //
        // Disable the auto-reaper so the cancel definitely happens while
        // the job is Claimed; trigger the requeue manually with reap_now.
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
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
        let first_token = job1.cancel_token().clone();
        assert_eq!(q.cancel(&job1.id).await.unwrap(), CancelOutcome::Requested,);
        assert!(first_token.is_cancelled());
        assert!(
            q.get_job(&job1.id).await.unwrap().unwrap().cancel_requested,
            "cancel_requested must persist on the claimed record",
        );

        // Force lease expiry, then trigger the reaper.
        clock.advance(Duration::from_millis(100));
        q.reap_now().await.unwrap();

        let job2 = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job1.id, job2.id);
        assert!(job2.cancel_requested);
        assert!(
            job2.cancel_token().is_cancelled(),
            "re-claim should surface a pre-cancelled token",
        );

        q.ack(&job2).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_reaped_claim_leaves_no_cancel_token_entry() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            reaper_interval: Duration::from_secs(3600),
            ..no_backoff_opts()
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
        let token = claim.cancel_token().clone();

        clock.advance(Duration::from_secs(31));
        q.reap_now().await.unwrap();
        assert!(q.core.lease_registry.current("work", &id).is_none());
        assert!(!q.core.lease_registry.cancel("work", &id));
        assert!(!token.is_cancelled());

        // The requeued job holds no entry to fire.
        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Removed);
        assert_eq!(q.core.lease_registry.len(), 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_reclaim_after_a_nack_is_cancellable() {
        let q = Queue::open_with_options(make_store(), "test", no_backoff_opts())
            .await
            .unwrap();

        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let first = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let first_token = first.cancel_token().clone();
        q.nack(&first, "transient").await.unwrap();

        let second = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert!(!second.cancel_token().is_cancelled());

        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Requested);
        assert!(second.cancel_token().is_cancelled());
        assert!(!first_token.is_cancelled());

        q.ack(&second).await.unwrap();
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
        let token = job.cancel_token().clone();

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

    /// Poll `fut` once with a no-op waker. A pending result shows the
    /// future reached its first await, which for `wait_for_completion`
    /// is past the waiter registration.
    fn poll_once<F: std::future::Future>(fut: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        fut.poll(&mut cx)
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

    #[tokio::test(start_paused = true)]
    async fn test_wait_for_completion_pending_times_out() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let outcome = q
            .wait_for_completion(&id, Duration::from_secs(60))
            .await
            .unwrap();
        assert!(matches!(outcome, WaitOutcome::TimedOut), "{outcome:?}");
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wait_for_completion_wakes_on_ack() {
        // Default retention deletes the record on ack; the waiter
        // receives it from the settlement.
        let q = Queue::open(make_store(), "test").await.unwrap();
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        let waiter = q.wait_for_completion(&id, Duration::from_secs(5));
        tokio::pin!(waiter);
        assert!(poll_once(waiter.as_mut()).is_pending());

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();

        match waiter.await.unwrap() {
            WaitOutcome::Done(record) => {
                assert_eq!(record.id, id);
                assert_eq!(record.status, JobStatus::Done);
                assert!(record.completed_at.is_some());
                assert_eq!(record.payload, b"payload");
            }
            other => panic!("expected Done(record), got {other:?}"),
        }
        assert!(q.get_job(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_wait_for_completion_wakes_on_exhausted_nack() {
        let opts = OpenOptions {
            default_queue_config: QueueConfig {
                max_attempts: 1,
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        let waiter = q.wait_for_completion(&id, Duration::from_secs(5));
        tokio::pin!(waiter);
        assert!(poll_once(waiter.as_mut()).is_pending());

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.nack(&job, "transient").await.unwrap();

        match waiter.await.unwrap() {
            WaitOutcome::Dead(record) => {
                assert_eq!(record.id, id);
                assert_eq!(record.status, JobStatus::Dead);
                assert_eq!(record.last_error.as_deref(), Some("transient"));
            }
            other => panic!("expected Dead(record), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_wait_for_completion_wakes_on_cancel_removed() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        let waiter = q.wait_for_completion(&id, Duration::from_secs(5));
        tokio::pin!(waiter);
        assert!(poll_once(waiter.as_mut()).is_pending());

        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Removed);

        assert!(matches!(waiter.await.unwrap(), WaitOutcome::Cancelled));
        assert!(q.get_job(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_wait_for_completion_does_not_wake_on_cancel_requested() {
        // A `Claimed` cancel fires the token but the job is still in
        // flight; the wait continues until the worker settles the claim.
        let q = Queue::open(make_store(), "test").await.unwrap();
        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let id = job.id.clone();

        let waiter = q.wait_for_completion(&id, Duration::from_secs(5));
        tokio::pin!(waiter);
        assert!(poll_once(waiter.as_mut()).is_pending());

        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Requested);
        assert!(poll_once(waiter.as_mut()).is_pending());

        q.ack(&job).await.unwrap();
        assert!(matches!(waiter.await.unwrap(), WaitOutcome::Done(_)));
    }

    #[tokio::test]
    async fn test_wait_for_completion_returns_immediately_when_already_terminal() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let id = job.id.clone();
        q.dead_letter(&job, "permanent").await.unwrap();

        match q
            .wait_for_completion(&id, Duration::from_millis(0))
            .await
            .unwrap()
        {
            WaitOutcome::Dead(record) => {
                assert_eq!(record.id, id);
                assert_eq!(record.status, JobStatus::Dead);
            }
            other => panic!("expected Dead(record), got {other:?}"),
        }
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wait_for_completion_fan_out_to_multiple_waiters() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        let mut waiters = Vec::new();
        for _ in 0..4 {
            let mut waiter = Box::pin(q.wait_for_completion(&id, Duration::from_secs(5)));
            assert!(poll_once(waiter.as_mut()).is_pending());
            waiters.push(waiter);
        }

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.dead_letter(&job, "permanent").await.unwrap();

        for waiter in waiters {
            match waiter.await.unwrap() {
                WaitOutcome::Dead(record) => {
                    assert_eq!(record.id, id);
                    assert_eq!(record.status, JobStatus::Dead);
                    assert_eq!(record.last_error.as_deref(), Some("permanent"));
                }
                other => panic!("waiter saw {other:?}, expected Dead(record)"),
            }
        }
    }

    #[tokio::test]
    async fn test_wait_for_completion_delivers_offloaded_payloads_inline() {
        // Covers the three settlements that hold a stored record: ack,
        // worker dead-letter and reaper dead-letter.
        let clock = Arc::new(MockClock::new(1_000_000));
        let opts = OpenOptions {
            reaper_interval: Duration::from_secs(3600),
            clock: clock.clone(),
            default_queue_config: QueueConfig {
                max_attempts: 1,
                ..offload_opts().default_queue_config
            },
            ..offload_opts()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let payload = vec![7u8; 256];

        let id = q.enqueue("work", payload.clone()).await.unwrap();
        let waiter = q.wait_for_completion(&id, Duration::from_secs(5));
        tokio::pin!(waiter);
        assert!(poll_once(waiter.as_mut()).is_pending());
        let job = q
            .claim("work", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        assert!(job.payload_ref.is_some());
        q.ack(&job).await.unwrap();
        match waiter.await.unwrap() {
            WaitOutcome::Done(record) => assert_eq!(record.payload, payload),
            other => panic!("expected Done(record), got {other:?}"),
        }

        let id = q.enqueue("work", payload.clone()).await.unwrap();
        let waiter = q.wait_for_completion(&id, Duration::from_secs(5));
        tokio::pin!(waiter);
        assert!(poll_once(waiter.as_mut()).is_pending());
        let job = q
            .claim("work", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        q.dead_letter(&job, "permanent").await.unwrap();
        match waiter.await.unwrap() {
            WaitOutcome::Dead(record) => assert_eq!(record.payload, payload),
            other => panic!("expected Dead(record), got {other:?}"),
        }

        let id = q.enqueue("work", payload.clone()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        drop(job);
        let waiter = q.wait_for_completion(&id, Duration::from_secs(5));
        tokio::pin!(waiter);
        assert!(poll_once(waiter.as_mut()).is_pending());
        clock.advance(Duration::from_secs(11));
        q.reap_now().await.unwrap();
        match waiter.await.unwrap() {
            WaitOutcome::Dead(record) => {
                assert_eq!(record.payload, payload);
                assert_eq!(record.last_error.as_deref(), Some("lease expired"));
            }
            other => panic!("expected Dead(record), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_wait_for_completion_reports_a_removal_that_races_the_read() {
        // An outcome delivered after the registration takes precedence
        // over `NotFound` when the record is gone at the read.
        let q = Queue::open(make_store(), "test").await.unwrap();
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let mut registration = q.core.completion_waiters.register(&id);
        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Removed);
        assert!(matches!(
            registration.try_outcome(),
            Some(WaitOutcome::Cancelled)
        ));
        assert!(q.core.completion_waiters.inner_is_empty());
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
        q.nack(&job, "transient").await.unwrap();

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
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
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
        q.nack(&job, "boom").await.unwrap();

        // Advance past the backoff and trigger promotion.
        clock.advance(Duration::from_millis(20));
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
    async fn test_worker_finishes_in_flight_job_on_shutdown() {
        use crate::worker::{Worker, WorkerError, run_worker};
        use std::sync::atomic::{AtomicBool, Ordering};

        // Worker that takes 100ms to process, long enough that shutdown
        // fires while the job is in flight.
        struct SlowWorker {
            finished: Arc<AtomicBool>,
        }
        impl Worker for SlowWorker {
            async fn process(
                &self,
                _job: &JobRecord,
                _lease: &LeaseHandle,
            ) -> std::result::Result<(), WorkerError> {
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
    async fn worker_process_can_renew_its_own_lease() {
        use crate::worker::{Worker, WorkerError, run_worker};

        struct RenewingWorker {
            queue: Arc<Queue>,
            clock: MockClock,
        }
        impl Worker for RenewingWorker {
            async fn process(
                &self,
                _job: &JobRecord,
                lease: &LeaseHandle,
            ) -> std::result::Result<(), WorkerError> {
                lease.ensure_at_least(Duration::from_secs(60)).unwrap();
                self.clock.advance(Duration::from_secs(2));
                self.queue.reap_now().await.unwrap();
                Ok(())
            }
        }

        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            default_queue_config: QueueConfig {
                lease_duration: Duration::from_secs(1),
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let q = Arc::new(
            Queue::open_with_options(make_store(), "test", opts)
                .await
                .unwrap(),
        );
        q.enqueue("work", b"x".to_vec()).await.unwrap();

        let worker = RenewingWorker {
            queue: q.clone(),
            clock: clock.clone(),
        };
        run_worker(
            &q,
            "work",
            &worker,
            Duration::from_millis(10),
            std::future::ready(()),
        )
        .await
        .unwrap();

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.done, 1, "the ack after a renewal must succeed");
        assert_eq!(stats.pending, 0, "the renewed job must not be requeued");
        assert_eq!(stats.claimed, 0);
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
            async fn process(
                &self,
                _job: &JobRecord,
                _lease: &LeaseHandle,
            ) -> std::result::Result<(), WorkerError> {
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

    #[tokio::test]
    async fn ack_with_applies_enqueue_and_kv_effects_atomically() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        q.enqueue_with_kv(
            "work",
            b"first".to_vec(),
            EnqueueOptions::default(),
            HashMap::from([(b"runs/1".to_vec(), b"active".to_vec())]),
        )
        .await
        .unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();

        let results = q
            .ack_with(
                &job,
                SettlementEffects {
                    enqueues: vec![EnqueueRequest {
                        queue: "next".to_string(),
                        payload: b"second".to_vec(),
                        options: EnqueueOptions::default(),
                    }],
                    kv_writes: HashMap::from([(b"runs/2".to_vec(), b"done".to_vec())]),
                    kv_deletes: vec![b"runs/1".to_vec()],
                },
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], EnqueueResult::New(_)));

        let follow_up = q.claim("next", lease).await.unwrap().unwrap();
        assert_eq!(follow_up.payload, b"second");
        q.ack(&follow_up).await.unwrap();
        assert!(q.kv_get(b"runs/1").await.unwrap().is_none());
        assert_eq!(
            q.kv_get(b"runs/2").await.unwrap().as_deref(),
            Some(b"done".as_slice()),
        );
        assert_eq!(q.stats("work").await.unwrap().done, 1);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn ack_with_applies_no_effects_when_the_claim_is_gone() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.ack(&job).await.unwrap();

        let err = q
            .ack_with(
                &job,
                SettlementEffects {
                    enqueues: vec![EnqueueRequest {
                        queue: "next".to_string(),
                        payload: b"x".to_vec(),
                        options: EnqueueOptions::default(),
                    }],
                    kv_writes: HashMap::from([(b"k".to_vec(), b"v".to_vec())]),
                    kv_deletes: Vec::new(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ClaimLost));
        assert!(q.claim("next", lease).await.unwrap().is_none());
        assert!(q.kv_get(b"k").await.unwrap().is_none());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn ack_with_dedup_hit_downgrades_one_request() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        let existing_id = q
            .enqueue_with(
                "next",
                b"existing".to_vec(),
                EnqueueOptions {
                    dedup_key: Some("dk".to_string()),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .unwrap();
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();

        let results = q
            .ack_with(
                &job,
                SettlementEffects {
                    enqueues: vec![
                        EnqueueRequest {
                            queue: "next".to_string(),
                            payload: b"dup".to_vec(),
                            options: EnqueueOptions {
                                dedup_key: Some("dk".to_string()),
                                ..EnqueueOptions::default()
                            },
                        },
                        EnqueueRequest {
                            queue: "next".to_string(),
                            payload: b"fresh".to_vec(),
                            options: EnqueueOptions::default(),
                        },
                    ],
                    ..SettlementEffects::default()
                },
            )
            .await
            .unwrap();
        assert!(matches!(&results[0], EnqueueResult::AlreadyEnqueued(id) if *id == existing_id));
        assert!(matches!(&results[1], EnqueueResult::New(_)));
        assert_eq!(q.stats("next").await.unwrap().pending, 2);
        assert_eq!(q.stats("work").await.unwrap().done, 1);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn dead_letter_with_applies_enqueue_and_kv_effects_atomically() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        assert!(q.claim("notify", lease).await.unwrap().is_none());
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();

        let results = q
            .dead_letter_with(
                &job,
                "bad input",
                SettlementEffects {
                    enqueues: vec![EnqueueRequest {
                        queue: "notify".to_string(),
                        payload: b"failed".to_vec(),
                        options: EnqueueOptions::default(),
                    }],
                    kv_writes: HashMap::from([(b"runs/1".to_vec(), b"failed".to_vec())]),
                    kv_deletes: Vec::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], EnqueueResult::New(_)));

        assert_eq!(q.stats("work").await.unwrap().dead, 1);
        assert_eq!(q.stats("notify").await.unwrap().pending, 1);
        assert_eq!(
            q.kv_get(b"runs/1").await.unwrap().as_deref(),
            Some(b"failed".as_slice()),
        );
        let follow_up = q.claim("notify", lease).await.unwrap().unwrap();
        assert_eq!(follow_up.payload, b"failed");
        q.ack(&follow_up).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn dead_letter_with_applies_no_effects_when_the_claim_is_gone() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.ack(&job).await.unwrap();

        let err = q
            .dead_letter_with(
                &job,
                "late failure",
                SettlementEffects {
                    enqueues: vec![EnqueueRequest {
                        queue: "notify".to_string(),
                        payload: b"x".to_vec(),
                        options: EnqueueOptions::default(),
                    }],
                    kv_writes: HashMap::from([(b"k".to_vec(), b"v".to_vec())]),
                    kv_deletes: Vec::new(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ClaimLost));
        assert_eq!(q.stats("notify").await.unwrap().pending, 0);
        assert!(q.kv_get(b"k").await.unwrap().is_none());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn nack_with_discards_effects_while_attempts_remain() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();

        let outcome = q
            .nack_with(
                &job,
                "transient",
                SettlementEffects {
                    enqueues: vec![EnqueueRequest {
                        queue: "notify".to_string(),
                        payload: b"x".to_vec(),
                        options: EnqueueOptions::default(),
                    }],
                    kv_writes: HashMap::from([(b"k".to_vec(), b"v".to_vec())]),
                    kv_deletes: Vec::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome, NackOutcome::Retried);

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.scheduled, 1);
        assert_eq!(stats.dead, 0);
        assert_eq!(q.stats("notify").await.unwrap().pending, 0);
        assert!(q.kv_get(b"k").await.unwrap().is_none());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn nack_with_applies_effects_when_it_dead_letters() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        assert!(q.claim("notify", lease).await.unwrap().is_none());
        q.enqueue_with(
            "work",
            b"job".to_vec(),
            EnqueueOptions {
                max_attempts: Some(1),
                ..EnqueueOptions::default()
            },
        )
        .await
        .unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();

        let outcome = q
            .nack_with(
                &job,
                "final failure",
                SettlementEffects {
                    enqueues: vec![EnqueueRequest {
                        queue: "notify".to_string(),
                        payload: b"failed".to_vec(),
                        options: EnqueueOptions::default(),
                    }],
                    kv_writes: HashMap::from([(b"runs/1".to_vec(), b"failed".to_vec())]),
                    kv_deletes: Vec::new(),
                },
            )
            .await
            .unwrap();
        let NackOutcome::DeadLettered(results) = outcome else {
            panic!("expected a dead-lettering nack, got {outcome:?}");
        };
        assert!(matches!(results[0], EnqueueResult::New(_)));

        assert_eq!(q.stats("work").await.unwrap().dead, 1);
        assert_eq!(q.stats("notify").await.unwrap().pending, 1);
        assert_eq!(
            q.kv_get(b"runs/1").await.unwrap().as_deref(),
            Some(b"failed".as_slice()),
        );
        let follow_up = q.claim("notify", lease).await.unwrap().unwrap();
        assert_eq!(follow_up.payload, b"failed");
        q.ack(&follow_up).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_with_applies_effects_with_the_removal() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        assert!(q.claim("notify", lease).await.unwrap().is_none());
        let id = q.enqueue("work", b"job".to_vec()).await.unwrap();

        let (outcome, results) = q
            .cancel_with(
                &id,
                SettlementEffects {
                    enqueues: vec![EnqueueRequest {
                        queue: "notify".to_string(),
                        payload: b"cancelled".to_vec(),
                        options: EnqueueOptions::default(),
                    }],
                    kv_writes: HashMap::new(),
                    kv_deletes: vec![b"runs/1".to_vec()],
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome, CancelOutcome::Removed);
        assert!(matches!(results[0], EnqueueResult::New(_)));

        assert_eq!(q.stats("work").await.unwrap().pending, 0);
        assert_eq!(q.stats("notify").await.unwrap().pending, 1);
        assert!(q.get_job(&id).await.unwrap().is_none());
        let follow_up = q.claim("notify", lease).await.unwrap().unwrap();
        assert_eq!(follow_up.payload, b"cancelled");
        q.ack(&follow_up).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_with_discards_effects_on_a_claimed_job() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        let id = q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();

        let (outcome, results) = q
            .cancel_with(
                &id,
                SettlementEffects {
                    enqueues: vec![EnqueueRequest {
                        queue: "notify".to_string(),
                        payload: b"x".to_vec(),
                        options: EnqueueOptions::default(),
                    }],
                    kv_writes: HashMap::from([(b"k".to_vec(), b"v".to_vec())]),
                    kv_deletes: Vec::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome, CancelOutcome::Requested);
        assert!(results.is_empty());
        assert_eq!(q.stats("notify").await.unwrap().pending, 0);
        assert!(q.kv_get(b"k").await.unwrap().is_none());

        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_with_discards_effects_on_an_unknown_job() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let (outcome, results) = q
            .cancel_with(
                "missing",
                SettlementEffects {
                    kv_writes: HashMap::from([(b"k".to_vec(), b"v".to_vec())]),
                    ..SettlementEffects::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome, CancelOutcome::NotFound);
        assert!(results.is_empty());
        assert!(q.kv_get(b"k").await.unwrap().is_none());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_with_deletes_the_payload_object_of_a_discarded_follow_up() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();
        let id = q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        let (outcome, _) = q
            .cancel_with(
                &id,
                SettlementEffects {
                    enqueues: vec![EnqueueRequest {
                        queue: "notify".to_string(),
                        payload: vec![1u8; 256],
                        options: EnqueueOptions::default(),
                    }],
                    ..SettlementEffects::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome, CancelOutcome::Requested);
        assert_eq!(
            object_count(&store, "test-payloads").await,
            0,
            "the discarded follow-up's offloaded object is removed"
        );

        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_key_both_written_and_deleted_is_rejected_before_the_settlement() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();

        let effects = SettlementEffects {
            kv_writes: HashMap::from([(b"k".to_vec(), b"v".to_vec())]),
            kv_deletes: vec![b"k".to_vec()],
            ..SettlementEffects::default()
        };
        let err = q.ack_with(&job, effects.clone()).await.unwrap_err();
        assert!(matches!(err, Error::ConflictingKvEffect { ref key } if key == b"k"));
        assert!(q.kv_get(b"k").await.unwrap().is_none());
        assert_eq!(
            q.stats("work").await.unwrap().claimed,
            1,
            "the claim is untouched"
        );

        let err = q.nack_with(&job, "e", effects.clone()).await.unwrap_err();
        assert!(matches!(err, Error::ConflictingKvEffect { .. }));
        let err = q.dead_letter_with(&job, "e", effects).await.unwrap_err();
        assert!(matches!(err, Error::ConflictingKvEffect { .. }));

        q.ack(&job).await.unwrap();
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

    fn offloaded_follow_up() -> SettlementEffects {
        SettlementEffects {
            enqueues: vec![EnqueueRequest {
                queue: "notify".to_string(),
                payload: vec![1u8; 256],
                options: EnqueueOptions::default(),
            }],
            ..SettlementEffects::default()
        }
    }

    #[tokio::test]
    async fn nack_with_deletes_the_payload_object_of_a_discarded_follow_up() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        let outcome = q
            .nack_with(&job, "transient", offloaded_follow_up())
            .await
            .unwrap();
        assert_eq!(outcome, NackOutcome::Retried);
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_lost_claim_deletes_the_payload_object_of_a_prepared_follow_up() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();

        let err = q
            .dead_letter_with(&job, "late", offloaded_follow_up())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ClaimLost));
        assert_eq!(object_count(&store, "test-payloads").await, 0);

        let err = q
            .nack_with(&job, "late", offloaded_follow_up())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ClaimLost));
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_with_deletes_the_payload_object_when_the_job_is_unknown() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let (outcome, _) = q
            .cancel_with("no-such-job", offloaded_follow_up())
            .await
            .unwrap();
        assert_eq!(outcome, CancelOutcome::NotFound);
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn ack_with_schedules_a_future_effect() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();

        q.ack_with(
            &job,
            SettlementEffects {
                enqueues: vec![EnqueueRequest {
                    queue: "next".to_string(),
                    payload: b"later".to_vec(),
                    options: EnqueueOptions {
                        run_at: Some(std::time::SystemTime::now() + Duration::from_secs(300)),
                        ..EnqueueOptions::default()
                    },
                }],
                ..SettlementEffects::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(q.stats("next").await.unwrap().scheduled, 1);
        assert!(q.claim("next", lease).await.unwrap().is_none());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cursor_bound_persists_across_a_clean_close() {
        let store = make_store();
        let lease = Duration::from_secs(5);
        let q = Queue::open(store.clone(), "test").await.unwrap();
        q.enqueue("work", b"first".to_vec()).await.unwrap();
        q.enqueue("work", b"second".to_vec()).await.unwrap();
        let first = q.claim("work", lease).await.unwrap().unwrap();
        q.ack(&first).await.unwrap();
        q.close().await.unwrap();

        let q = Queue::open(store, "test").await.unwrap();
        let scan = q.core.claim_cursor.begin_claim("work");
        assert!(scan.scan_from.is_some());
        assert!(!scan.known_empty);
        assert!(
            q.core.db.get(cursor_key("work")).await.unwrap().is_none(),
            "the cursor record is consumed at open",
        );

        let second = q.claim("work", lease).await.unwrap().unwrap();
        assert_eq!(second.payload, b"second");
        q.ack(&second).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cursor_emptiness_persists_across_a_clean_close() {
        let store = make_store();
        let lease = Duration::from_secs(5);
        let q = Queue::open(store.clone(), "test").await.unwrap();
        q.enqueue("work", b"only".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.ack(&job).await.unwrap();
        assert!(q.claim("work", lease).await.unwrap().is_none());
        q.close().await.unwrap();

        let q = Queue::open(store, "test").await.unwrap();
        assert!(q.core.claim_cursor.begin_claim("work").known_empty);

        q.enqueue("work", b"revives".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();
        assert_eq!(job.payload, b"revives");
        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn restored_bound_moves_back_for_an_insert_behind_it() {
        let store = make_store();
        let lease = Duration::from_secs(5);
        let q = Queue::open(store.clone(), "test").await.unwrap();
        q.enqueue("work", b"normal-1".to_vec()).await.unwrap();
        q.enqueue("work", b"normal-2".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.ack(&job).await.unwrap();
        q.close().await.unwrap();

        // A high-priority job sorts before the restored bound, which
        // sits in the normal-priority band.
        let q = Queue::open(store, "test").await.unwrap();
        q.enqueue_with(
            "work",
            b"urgent".to_vec(),
            EnqueueOptions {
                priority: Some(PRIORITY_HIGH),
                ..EnqueueOptions::default()
            },
        )
        .await
        .unwrap();

        let job = q.claim("work", lease).await.unwrap().unwrap();
        assert_eq!(job.payload, b"urgent");
        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    /// OpenOptions with a small offload threshold so tests exercise the
    /// offload path.
    fn offload_opts() -> OpenOptions {
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
    async fn object_count(store: &Arc<dyn ObjectStore>, prefix: &str) -> usize {
        store
            .list_with_delimiter(Some(&slatedb::object_store::path::Path::from(prefix)))
            .await
            .unwrap()
            .objects
            .len()
    }

    #[tokio::test]
    async fn offloaded_payload_round_trips_through_claim() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let payload = vec![7u8; 1024];
        let id = q.enqueue("work", payload.clone()).await.unwrap();
        assert_eq!(object_count(&store, "test-payloads").await, 1);

        let read = q.get_job(&id).await.unwrap().unwrap();
        assert!(read.payload_ref.is_some());
        assert_eq!(read.payload, payload);

        let job = q.claim("work", Duration::from_secs(30)).await.unwrap();
        let job = job.unwrap();
        assert!(job.payload_ref.is_some());
        assert_eq!(job.payload, payload);

        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn offloaded_payload_survives_nack_and_reclaim() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let payload = vec![3u8; 512];
        q.enqueue("work", payload.clone()).await.unwrap();

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.nack(&job, "retry").await.unwrap();
        assert_eq!(
            object_count(&store, "test-payloads").await,
            1,
            "a nack must not rewrite or remove the payload object"
        );

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.payload, payload);
        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    /// OpenOptions offloading to `payloads`, leaving the queue's own store
    /// healthy so only payload-object requests are subject to faults.
    fn faulty_payload_opts(payloads: Arc<dyn ObjectStore>) -> OpenOptions {
        OpenOptions {
            payload_store: Some(payloads),
            ..offload_opts()
        }
    }

    #[tokio::test]
    async fn enqueue_writes_no_record_when_the_payload_object_write_fails() {
        let payloads = FaultStore::wrap();
        let payload_store: Arc<dyn ObjectStore> = payloads.clone();
        let q = Queue::open_with_options(
            make_store(),
            "test",
            faulty_payload_opts(payload_store.clone()),
        )
        .await
        .unwrap();

        payloads.fail_puts(true);
        let err = q.enqueue("work", vec![9u8; 256]).await.unwrap_err();

        assert!(
            matches!(err, Error::PayloadStore(StoreError::Generic { store, .. }) if store == "FaultStore"),
            "unexpected error: {err:?}"
        );
        assert_eq!(
            q.stats("work").await.unwrap().pending,
            0,
            "a record must not be written when its payload object was not"
        );
        assert_eq!(object_count(&payload_store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn enqueue_batch_removes_earlier_payload_objects_when_a_later_write_fails() {
        let payloads = FaultStore::wrap();
        let payload_store: Arc<dyn ObjectStore> = payloads.clone();
        let q = Queue::open_with_options(
            make_store(),
            "test",
            faulty_payload_opts(payload_store.clone()),
        )
        .await
        .unwrap();

        // The third payload write fails, after two objects have been written.
        payloads.fail_puts_after(2);
        let err = q
            .enqueue_batch("work", vec![vec![1u8; 256], vec![2u8; 256], vec![3u8; 256]])
            .await
            .unwrap_err();

        assert!(
            matches!(err, Error::PayloadStore(StoreError::Generic { store, .. }) if store == "FaultStore"),
            "unexpected error: {err:?}"
        );
        assert_eq!(
            object_count(&payload_store, "test-payloads").await,
            0,
            "objects written before the failure must be removed, since no record points at them"
        );
        assert_eq!(q.stats("work").await.unwrap().pending, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn ack_settles_the_job_when_the_payload_object_delete_fails() {
        let payloads = FaultStore::wrap();
        let payload_store: Arc<dyn ObjectStore> = payloads.clone();
        let q = Queue::open_with_options(
            make_store(),
            "test",
            faulty_payload_opts(payload_store.clone()),
        )
        .await
        .unwrap();

        let id = q.enqueue("work", vec![4u8; 256]).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        payloads.fail_deletes(true);
        q.ack(&job).await.unwrap();

        assert!(
            q.get_job(&id).await.unwrap().is_none(),
            "the payload delete is best-effort and must not prevent settlement"
        );
        assert_eq!(
            object_count(&payload_store, "test-payloads").await,
            1,
            "a failed delete leaves an unreferenced object, the record having been removed first"
        );
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn ack_with_deletes_the_payload_object_of_a_deduplicated_follow_up() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        q.enqueue_with(
            "next",
            vec![1u8; 256],
            EnqueueOptions {
                dedup_key: Some("dk".to_string()),
                ..EnqueueOptions::default()
            },
        )
        .await
        .unwrap();
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        assert_eq!(object_count(&store, "test-payloads").await, 1);

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let results = q
            .ack_with(
                &job,
                SettlementEffects {
                    enqueues: vec![EnqueueRequest {
                        queue: "next".to_string(),
                        payload: vec![2u8; 256],
                        options: EnqueueOptions {
                            dedup_key: Some("dk".to_string()),
                            ..EnqueueOptions::default()
                        },
                    }],
                    ..SettlementEffects::default()
                },
            )
            .await
            .unwrap();

        assert!(matches!(&results[0], EnqueueResult::AlreadyEnqueued(_)));
        assert_eq!(
            object_count(&store, "test-payloads").await,
            1,
            "the downgraded follow-up's object is removed, leaving only the existing job's"
        );
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn ack_without_done_retention_deletes_the_payload_object() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        q.enqueue("work", vec![1u8; 256]).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn done_retention_keeps_the_payload_object_until_the_sweep() {
        let clock = MockClock::new(1_700_000_000_000);
        let reaper_interval = Duration::from_millis(10);
        let retention = Duration::from_millis(20);
        let store = make_store();
        let opts = OpenOptions {
            reaper_interval,
            default_queue_config: QueueConfig {
                keep_done_jobs: Some(retention),
                ..QueueConfig::default()
            },
            clock: Arc::new(clock.clone()),
            payload_offload_threshold: Some(64),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(store.clone(), "test", opts)
            .await
            .unwrap();

        let payload = vec![9u8; 256];
        let id = q.enqueue("work", payload.clone()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();

        // The done record is kept, so the payload object stays and the
        // record read materializes it.
        assert_eq!(object_count(&store, "test-payloads").await, 1);
        let done = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(done.payload, payload);

        clock.advance(retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;

        assert!(q.get_job(&id).await.unwrap().is_none());
        assert_eq!(
            object_count(&store, "test-payloads").await,
            0,
            "the retention sweep must delete the payload object with the record"
        );
        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn dead_retention_sweep_deletes_the_payload_object() {
        let clock = MockClock::new(1_700_000_000_000);
        let reaper_interval = Duration::from_millis(10);
        let retention = Duration::from_millis(20);
        let store = make_store();
        let opts = OpenOptions {
            reaper_interval,
            default_queue_config: QueueConfig {
                dead_retention: Some(retention),
                ..QueueConfig::default()
            },
            clock: Arc::new(clock.clone()),
            payload_offload_threshold: Some(64),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(store.clone(), "test", opts)
            .await
            .unwrap();

        q.enqueue("work", vec![5u8; 256]).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.dead_letter(&job, "permanent").await.unwrap();
        assert_eq!(object_count(&store, "test-payloads").await, 1);

        clock.advance(retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;

        assert!(q.dead_jobs("work", None, 10).await.unwrap().is_empty());
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn dead_lettered_payload_survives_requeue_and_redelivery() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let payload = vec![8u8; 512];
        q.enqueue("work", payload.clone()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.dead_letter(&job, "permanent").await.unwrap();

        let dead = q.dead_jobs("work", None, 10).await.unwrap();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].payload, payload, "dead_jobs materializes payloads");

        q.requeue_dead_job(dead.into_iter().next().unwrap())
            .await
            .unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.payload, payload);
        q.ack(&job).await.unwrap();
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_id_rejection_preserves_the_existing_payload_object() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let payload = vec![1u8; 256];
        let opts_with_id = || EnqueueOptions {
            id_override: Some("fixed-id".to_string()),
            ..EnqueueOptions::default()
        };
        q.enqueue_with("work", payload.clone(), opts_with_id())
            .await
            .unwrap();
        let err = q
            .enqueue_with("work", vec![2u8; 256], opts_with_id())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::DuplicateJobId { .. }));

        // The rejected enqueue's object is removed; the live job's object
        // is untouched and still readable.
        assert_eq!(object_count(&store, "test-payloads").await, 1);
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.payload, payload);
        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn dedup_hit_removes_the_new_payload_object() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let opts_with_dedup = || EnqueueOptions {
            dedup_key: Some("once".to_string()),
            ..EnqueueOptions::default()
        };
        let first = q
            .enqueue_with("work", vec![1u8; 256], opts_with_dedup())
            .await
            .unwrap();
        let second = q
            .enqueue_with("work", vec![2u8; 256], opts_with_dedup())
            .await
            .unwrap();
        assert_eq!(first, second, "dedup returns the existing job's id");
        assert_eq!(object_count(&store, "test-payloads").await, 1);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn payloads_at_or_below_the_threshold_stay_inline() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let id = q.enqueue("work", vec![0u8; 64]).await.unwrap();
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        let job = q.get_job(&id).await.unwrap().unwrap();
        assert!(job.payload_ref.is_none());
        assert_eq!(job.payload.len(), 64);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn disabled_offload_keeps_large_payloads_inline() {
        let store = make_store();
        let opts = OpenOptions {
            payload_offload_threshold: None,
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(store.clone(), "test", opts)
            .await
            .unwrap();

        let id = q.enqueue("work", vec![0u8; 1024 * 1024]).await.unwrap();
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        let job = q.get_job(&id).await.unwrap().unwrap();
        assert!(job.payload_ref.is_none());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_of_a_pending_job_deletes_the_payload_object() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let id = q.enqueue("work", vec![4u8; 256]).await.unwrap();
        assert_eq!(object_count(&store, "test-payloads").await, 1);
        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Removed);
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn separate_payload_store_receives_the_payload_objects() {
        let queue_store = make_store();
        let payload_store = make_store();
        let opts = OpenOptions {
            payload_offload_threshold: Some(64),
            payload_store: Some(payload_store.clone()),
            payload_path: Some("blobs".to_string()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(queue_store.clone(), "test", opts)
            .await
            .unwrap();

        let payload = vec![6u8; 256];
        q.enqueue("work", payload.clone()).await.unwrap();
        assert_eq!(object_count(&payload_store, "blobs").await, 1);
        assert_eq!(object_count(&queue_store, "test-payloads").await, 0);

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.payload, payload);
        q.ack(&job).await.unwrap();
        assert_eq!(object_count(&payload_store, "blobs").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn batch_enqueue_offloads_each_oversized_payload() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let payloads = vec![vec![1u8; 256], vec![2u8; 16], vec![3u8; 256]];
        q.enqueue_batch("work", payloads.clone()).await.unwrap();
        assert_eq!(
            object_count(&store, "test-payloads").await,
            2,
            "only the two oversized payloads offload"
        );

        for expected in payloads {
            let job = q
                .claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(job.payload, expected);
            q.ack(&job).await.unwrap();
        }
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn scheduled_job_offloads_and_materializes_after_promotion() {
        let initial = 1_700_000_000_000;
        let clock = MockClock::new(initial);
        let store = make_store();
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            payload_offload_threshold: Some(64),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(store.clone(), "test", opts)
            .await
            .unwrap();

        let payload = vec![7u8; 256];
        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial + 60_000);
        let id = q
            .enqueue_with(
                "work",
                payload.clone(),
                EnqueueOptions {
                    run_at: Some(run_at),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .unwrap();

        // The payload offloads at enqueue even though the record lands
        // in the scheduled key space.
        assert_eq!(object_count(&store, "test-payloads").await, 1);
        let scheduled = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(scheduled.status, JobStatus::Scheduled);
        assert_eq!(scheduled.payload, payload);

        clock.advance(Duration::from_millis(60_001));
        q.promote_scheduled_now().await.unwrap();

        // Promotion moves the record without touching the object; the
        // claim materializes the payload.
        assert_eq!(object_count(&store, "test-payloads").await, 1);
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.payload, payload);
        q.ack(&job).await.unwrap();
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn reaper_dead_letter_preserves_the_payload_object() {
        let clock = MockClock::new(1_700_000_000_000);
        let store = make_store();
        let opts = OpenOptions {
            default_queue_config: QueueConfig {
                max_attempts: 1,
                ..QueueConfig::default()
            },
            clock: Arc::new(clock.clone()),
            payload_offload_threshold: Some(64),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(store.clone(), "test", opts)
            .await
            .unwrap();

        let payload = vec![2u8; 256];
        let id = q.enqueue("work", payload.clone()).await.unwrap();
        let job = q
            .claim("work", Duration::from_millis(10))
            .await
            .unwrap()
            .unwrap();
        drop(job);
        clock.advance(Duration::from_millis(20));
        q.reap_now().await.unwrap();

        let dead = q.dead_jobs("work", None, 10).await.unwrap();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].id, id);
        assert_eq!(dead[0].payload, payload);
        assert_eq!(
            object_count(&store, "test-payloads").await,
            1,
            "reaper-driven dead-letter must preserve the payload object"
        );
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn external_payload_deletion_surfaces_payload_missing() {
        use slatedb::object_store::ObjectStoreExt;

        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let id = q.enqueue("work", vec![1u8; 256]).await.unwrap();
        let objects = store
            .list_with_delimiter(Some(&slatedb::object_store::path::Path::from(
                "test-payloads",
            )))
            .await
            .unwrap()
            .objects;
        assert_eq!(objects.len(), 1);
        store.delete(&objects[0].location).await.unwrap();

        // The record is live, so the missing object is a real loss.
        let err = q.get_job(&id).await.unwrap_err();
        assert!(matches!(err, Error::PayloadMissing { id: ref e } if *e == id));

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn ack_effect_enqueues_offload_and_clean_up_on_dedup_downgrade() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        // An existing job holds the dedup key the follow-up enqueue will hit.
        q.enqueue_with(
            "next",
            vec![1u8; 16],
            EnqueueOptions {
                dedup_key: Some("once".to_string()),
                ..EnqueueOptions::default()
            },
        )
        .await
        .unwrap();

        q.enqueue("work", vec![2u8; 16]).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let results = q
            .ack_with(
                &job,
                SettlementEffects {
                    enqueues: vec![
                        EnqueueRequest {
                            queue: "next".to_string(),
                            payload: vec![3u8; 256],
                            options: EnqueueOptions {
                                dedup_key: Some("once".to_string()),
                                ..EnqueueOptions::default()
                            },
                        },
                        EnqueueRequest {
                            queue: "next".to_string(),
                            payload: vec![4u8; 256],
                            options: EnqueueOptions::default(),
                        },
                    ],
                    ..SettlementEffects::default()
                },
            )
            .await
            .unwrap();

        assert!(matches!(results[0], EnqueueResult::AlreadyEnqueued(_)));
        assert!(matches!(results[1], EnqueueResult::New(_)));
        // The dedup-downgraded follow-up job's payload object is removed;
        // the committed follow-up job's object remains.
        assert_eq!(object_count(&store, "test-payloads").await, 1);

        let follow_up_ids = match &results[1] {
            EnqueueResult::New(id) => id.clone(),
            _ => unreachable!(),
        };
        let follow_up = q.get_job(&follow_up_ids).await.unwrap().unwrap();
        assert_eq!(follow_up.payload, vec![4u8; 256]);
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
        let q = Queue::open(make_store(), "test").await.unwrap();
        let later = q
            .enqueue_with(
                "work",
                b"later".to_vec(),
                EnqueueOptions {
                    run_at: Some(std::time::SystemTime::now() + Duration::from_secs(7200)),
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
                    run_at: Some(std::time::SystemTime::now() + Duration::from_secs(3600)),
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
    async fn list_jobs_filters_claimed_by_queue() {
        let opts = OpenOptions {
            clock: Arc::new(MockClock::new(1_700_000_000_000)),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let a1 = q.enqueue("qa", b"1".to_vec()).await.unwrap();
        let a2 = q.enqueue("qa", b"2".to_vec()).await.unwrap();
        q.enqueue("qb", b"3".to_vec()).await.unwrap();
        let lease = Duration::from_secs(30);
        q.claim("qa", lease).await.unwrap().unwrap();
        q.claim("qa", lease).await.unwrap().unwrap();
        q.claim("qb", lease).await.unwrap().unwrap();

        let page = q
            .list_jobs("qa", JobStatus::Claimed, None, 10)
            .await
            .unwrap();
        let ids: Vec<_> = page.jobs.iter().map(|j| j.id.clone()).collect();
        assert_eq!(ids, vec![a1, a2]);
        assert!(page.jobs.iter().all(|j| j.status == JobStatus::Claimed));
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

    #[tokio::test]
    async fn attempt_history_records_retries_then_completion() {
        let opts = OpenOptions {
            default_queue_config: QueueConfig {
                keep_done_jobs: Some(Duration::from_secs(3600)),
                retry_backoff_base: Duration::ZERO,
                retry_backoff_max: Duration::ZERO,
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let id = q.enqueue("work", b"x".to_vec()).await.unwrap();
        let lease = Duration::from_secs(30);

        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.nack(&job, "timeout").await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.nack(&job, "connection reset").await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.ack(&job).await.unwrap();

        let history = q.attempt_history(&id).await.unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].attempt, 1);
        assert_eq!(history[0].outcome, AttemptOutcome::Retried);
        assert_eq!(history[0].error.as_deref(), Some("timeout"));
        assert!(history[0].claimed_at.is_some());
        assert_eq!(history[1].attempt, 2);
        assert_eq!(history[1].error.as_deref(), Some("connection reset"));
        assert_eq!(history[2].attempt, 3);
        assert_eq!(history[2].outcome, AttemptOutcome::Completed);
        assert_eq!(history[2].error, None);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn attempt_history_removed_when_ack_expunges_the_record() {
        let q = Queue::open_with_options(make_store(), "test", no_backoff_opts())
            .await
            .unwrap();
        let id = q.enqueue("work", b"x".to_vec()).await.unwrap();
        let lease = Duration::from_secs(30);

        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.nack(&job, "failed").await.unwrap();
        assert_eq!(q.attempt_history(&id).await.unwrap().len(), 1);

        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.ack(&job).await.unwrap();
        assert!(q.attempt_history(&id).await.unwrap().is_empty());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn attempt_history_records_dead_letter_on_nack_at_attempt_limit() {
        let opts = OpenOptions {
            default_queue_config: QueueConfig {
                max_attempts: 1,
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let id = q.enqueue("work", b"x".to_vec()).await.unwrap();

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.nack(&job, "failed").await.unwrap();
        assert_eq!(
            q.get_job(&id).await.unwrap().unwrap().status,
            JobStatus::Dead
        );

        let history = q.attempt_history(&id).await.unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].attempt, 1);
        assert_eq!(history[0].outcome, AttemptOutcome::DeadLettered);
        assert_eq!(history[0].error.as_deref(), Some("failed"));
        assert!(history[0].claimed_at.is_some());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn attempt_history_survives_requeue_with_marker() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let id = q.enqueue("work", b"x".to_vec()).await.unwrap();
        let lease = Duration::from_secs(30);

        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.dead_letter(&job, "unroutable").await.unwrap();

        let dead = q.get_job(&id).await.unwrap().unwrap();
        q.requeue_dead_job(dead).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.dead_letter(&job, "still unroutable").await.unwrap();

        let history = q.attempt_history(&id).await.unwrap();
        let outcomes: Vec<_> = history.iter().map(|a| a.outcome).collect();
        assert_eq!(
            outcomes,
            vec![
                AttemptOutcome::DeadLettered,
                AttemptOutcome::Requeued,
                AttemptOutcome::DeadLettered,
            ]
        );
        assert_eq!(history[0].error.as_deref(), Some("unroutable"));
        assert_eq!(history[1].attempt, 0);
        assert_eq!(history[1].claimed_at, None);
        assert_eq!(history[2].attempt, 1);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn attempt_history_records_lease_expiries() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            default_queue_config: QueueConfig {
                max_attempts: 2,
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let id = q.enqueue("work", b"x".to_vec()).await.unwrap();
        let lease = Duration::from_secs(10);

        q.claim("work", lease).await.unwrap().unwrap();
        clock.advance(lease + Duration::from_secs(1));
        q.reap_now().await.unwrap();

        q.claim("work", lease).await.unwrap().unwrap();
        clock.advance(lease + Duration::from_secs(1));
        q.reap_now().await.unwrap();

        let history = q.attempt_history(&id).await.unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].outcome, AttemptOutcome::LeaseExpired);
        assert_eq!(history[0].attempt, 1);
        assert_eq!(history[0].error, None);
        assert_eq!(history[1].outcome, AttemptOutcome::DeadLettered);
        assert_eq!(history[1].attempt, 2);
        assert_eq!(history[1].error.as_deref(), Some("lease expired"));
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_of_scheduled_job_removes_attempt_history() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let id = q.enqueue("work", b"x".to_vec()).await.unwrap();

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        // Default backoff is non-zero, so the nacked job waits in
        // `Scheduled` with one history entry.
        q.nack(&job, "failed").await.unwrap();
        assert_eq!(q.attempt_history(&id).await.unwrap().len(), 1);

        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Removed);
        assert!(q.attempt_history(&id).await.unwrap().is_empty());
        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn dead_retention_sweep_removes_attempt_history() {
        let clock = MockClock::new(1_700_000_000_000);
        let reaper_interval = Duration::from_millis(10);
        let retention = Duration::from_millis(20);
        let opts = OpenOptions {
            reaper_interval,
            clock: Arc::new(clock.clone()),
            default_queue_config: QueueConfig {
                dead_retention: Some(retention),
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let id = q.enqueue("work", b"x".to_vec()).await.unwrap();

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.dead_letter(&job, "failed").await.unwrap();
        assert_eq!(q.attempt_history(&id).await.unwrap().len(), 1);

        clock.advance(retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;

        assert!(q.get_job(&id).await.unwrap().is_none());
        assert!(q.attempt_history(&id).await.unwrap().is_empty());
        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn done_retention_sweep_removes_attempt_history() {
        let clock = MockClock::new(1_700_000_000_000);
        let reaper_interval = Duration::from_millis(10);
        let retention = Duration::from_millis(20);
        let opts = OpenOptions {
            reaper_interval,
            clock: Arc::new(clock.clone()),
            default_queue_config: QueueConfig {
                keep_done_jobs: Some(retention),
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let id = q.enqueue("work", b"x".to_vec()).await.unwrap();

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();
        assert_eq!(q.attempt_history(&id).await.unwrap().len(), 1);

        clock.advance(retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;

        assert!(q.get_job(&id).await.unwrap().is_none());
        assert!(q.attempt_history(&id).await.unwrap().is_empty());
        q.close().await.unwrap();
    }

    // Every claimed record must hold a registry entry; a record
    // without one is invisible to the reaper until the next open.
    async fn assert_every_claim_has_a_lease_entry(q: &Queue) {
        let mut iter = q
            .core
            .db
            .scan_prefix(tag_prefix(KeyTag::Claimed), ..)
            .await
            .unwrap();
        let mut claims = 0;
        while let Some(kv) = iter.next().await.unwrap() {
            let job = JobRecord::decode(&kv.key, &kv.value).unwrap();
            assert!(
                q.core.lease_registry.current(&job.queue, &job.id).is_some(),
                "no lease entry for {}/{}",
                job.queue,
                job.id
            );
            claims += 1;
        }
        assert!(claims > 0, "no claims to check");
    }

    #[tokio::test]
    async fn every_live_claim_has_a_lease_entry() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        for i in 0..3u8 {
            q.enqueue("work", vec![i]).await.unwrap();
        }
        let claims = q
            .claim_batch("work", 3, Duration::from_secs(30))
            .await
            .unwrap();
        assert_every_claim_has_a_lease_entry(&q).await;

        let renewed = q.renew_lease(&claims[0], Duration::from_secs(90)).unwrap();
        assert_every_claim_has_a_lease_entry(&q).await;
        assert!(
            q.core
                .lease_registry
                .contains("work", &claims[0].id, renewed)
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn an_unreapable_job_does_not_block_the_leases_behind_it() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let poison = q.enqueue("work", b"a".to_vec()).await.unwrap();
        let healthy = q.enqueue("work", b"b".to_vec()).await.unwrap();
        let claims = q
            .claim_batch("work", 2, Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(claims.len(), 2);

        // The poisoned record sorts first, both jobs sharing an expiry.
        q.core
            .db
            .put(claimed_key("work", &poison), b"not messagepack")
            .await
            .unwrap();

        clock.advance(Duration::from_secs(31));
        q.reap_now().await.unwrap();

        assert_eq!(
            q.get_job(&healthy).await.unwrap().unwrap().status,
            JobStatus::Pending
        );
        // The poisoned job's entry is kept for a later tick; the
        // healthy job's was removed by its requeue.
        assert_eq!(q.core.lease_registry.len(), 1);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_claim_held_at_close_is_requeued_at_open() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = || OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", opts())
            .await
            .unwrap();
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let claim = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        // Drop the claim without settling it, as a crashed worker would.
        drop(claim);
        q.close().await.unwrap();

        // A claim present at open belongs to a process that no longer
        // holds the store, so it is requeued immediately, before its
        // lease expires.
        let q = Queue::open_with_options(store, "test", opts())
            .await
            .unwrap();
        let job = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Pending);
        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.pending, 1);
        assert_eq!(stats.claimed, 0);
        let history = q.attempt_history(&id).await.unwrap();
        assert_eq!(history.last().unwrap().outcome, AttemptOutcome::Interrupted);

        // The requeued job is claimable at once: its pending insert is
        // recorded against the restored clean-close bound. The next
        // attempt is consumed by the re-claim.
        let reclaim = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reclaim.id, id);
        assert_eq!(reclaim.attempts, 2);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_claim_out_of_attempts_is_dead_lettered_at_open() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = || OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", opts())
            .await
            .unwrap();
        let id = q
            .enqueue_with(
                "work",
                b"payload".to_vec(),
                EnqueueOptions {
                    max_attempts: Some(1),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .unwrap();
        let claim = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        drop(claim);
        q.close().await.unwrap();

        let q = Queue::open_with_options(store, "test", opts())
            .await
            .unwrap();
        let job = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Dead);
        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.dead, 1);
        assert_eq!(stats.claimed, 0);
        q.close().await.unwrap();
    }
}
