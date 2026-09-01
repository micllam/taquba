//! Caller-facing configuration: the per-queue and open-time
//! configuration of a [`Queue`](crate::Queue) and the per-call enqueue
//! overrides.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use slatedb::object_store::ObjectStore;

use crate::clock::{Clock, default_clock};

const DEFAULT_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(30);

/// High-priority bucket. Jobs at this priority are dequeued before normal and low.
pub const PRIORITY_HIGH: u32 = 100;
/// Default priority. FIFO ordering is preserved within the same priority level.
pub const PRIORITY_NORMAL: u32 = 1_000;
/// Low-priority bucket. Jobs at this priority are dequeued after high and normal.
pub const PRIORITY_LOW: u32 = 10_000;

/// Default value of [`OpenOptions::payload_offload_threshold`]: payloads
/// larger than this are stored as objects in the payload object store
/// instead of inline in the job record.
pub const DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD: usize = 256 * 1024;

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
    /// How long a claimed job's lease lasts. Used by [`Queue::claim_next`](crate::Queue::claim_next).
    pub lease_duration: Duration,
    /// Default priority assigned to jobs enqueued without an explicit priority.
    /// Lower numbers are dequeued first. Use the [`PRIORITY_HIGH`], [`PRIORITY_NORMAL`],
    /// and [`PRIORITY_LOW`] constants, or any `u32` value.
    pub default_priority: u32,
    /// Base delay for exponential retry backoff after a [`Queue::nack`](crate::Queue::nack).
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
    /// If `None` (default), [`Queue::ack`](crate::Queue::ack) deletes successful jobs outright.
    ///
    /// The success counter in [`QueueStats::done`](crate::QueueStats::done) is incremented either way.
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

/// Configuration for opening a [`Queue`](crate::Queue) instance.
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
    /// beat is committed during open, and a clean [`Queue::close`](crate::Queue::close)
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
    /// record storing [`JobRecord::payload_ref`](crate::JobRecord::payload_ref) instead of inline bytes.
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
    /// Object store for the write-ahead log. `None` (the default) keeps
    /// the WAL on the object store the queue is opened on.
    ///
    /// The transitions that await durability block on a WAL flush, so a
    /// WAL store with lower write latency lowers their latency floor
    /// (see [`Self::flush_interval`]); the manifest and compacted data
    /// stay on the primary store. WAL objects live under the queue's
    /// `path` within this store. A recent transition exists only in the
    /// WAL until flushed to the primary store, so its durability is the
    /// WAL store's. Every open of the same path must configure the same
    /// pair of stores, and a [`QueueReader`](crate::QueueReader) must
    /// receive this store via
    /// [`ReaderOptions::wal_object_store`](crate::ReaderOptions::wal_object_store).
    pub wal_object_store: Option<Arc<dyn ObjectStore>>,
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

    /// Set [`Self::wal_object_store`].
    #[must_use]
    pub fn wal_object_store(
        mut self,
        wal_object_store: impl Into<Option<Arc<dyn ObjectStore>>>,
    ) -> Self {
        self.wal_object_store = wal_object_store.into();
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
            wal_object_store: None,
        }
    }
}

/// Per-call overrides for [`Queue::enqueue_with`](crate::Queue::enqueue_with).
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
    /// the payload and surfaced as [`JobRecord::headers`](crate::JobRecord::headers). Useful for fields
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
    /// [`Error::DuplicateJobId`](crate::Error::DuplicateJobId) while the existing job is still indexed.
    /// ULID generation guarantees uniqueness for the `None` path.
    ///
    /// Constraints (enforced; violations return [`Error::InvalidId`](crate::Error::InvalidId)):
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
