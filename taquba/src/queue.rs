use std::sync::Arc;
use std::time::Duration;

use slatedb::Db;
use slatedb::config::Settings;
use slatedb::object_store::ObjectStore;
use tokio::sync::oneshot;
use tracing::{debug, instrument, warn};

use crate::background::{BackgroundTask, Ticker};
use crate::claim_cursor::ClaimCursor;
use crate::clock::Clock;
use crate::completion::{CompletionWaiters, Registration};
use crate::effects::{EnqueueRequest, PreparedEffects, SettlementEffects};
use crate::error::{Error, Result};
use crate::history::{AttemptOutcome, JobAttempt, append_attempt};
use crate::job::{Claim, JobRecord, JobStatus};
use crate::keys::QueueName;
use crate::lease_registry::{LeaseRegistry, Renewal};
use crate::options::{EnqueueOptions, OpenOptions, QueueConfig};
use crate::payload_store::PayloadStore;
use crate::queue_core::{QueueConfigs, QueueCore};
use crate::reaper::Reaper;
use crate::scheduler::Scheduler;
use crate::stats::QueueMergeOperator;
use crate::time_bound::TimeBound;
use crate::txn::ClaimEnd;
use crate::txn::{
    Attempt, Durability, retry, stage_claim_end, stage_remove, stage_to_pending, take_claim,
};
use crate::view::{Handle, QueueView};

/// Outcome of [`Queue::cancel`], reflecting which lifecycle branch the job was
/// in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The job was `Pending` or `Scheduled`, and the call removed it from the
    /// queue. No worker will ever see it.
    Removed,
    /// The job was `Claimed`. The call requested the cancellation via the
    /// persisted [`JobRecord::cancel_requested`] flag and fired the in-process
    /// [`Claim::cancel_token`]. The worker is still running and will eventually
    /// `ack` / `nack` / `dead_letter` the job according to its own logic.
    Requested,
    /// No job with this ID was found, or it was already in a terminal state
    /// (`Done` / `Dead`).
    NotFound,
}

/// Outcome of [`Queue::nack_with`], reflecting which settlement branch the
/// failure took.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NackOutcome {
    /// Attempts remained, so the job was re-queued (immediately or after
    /// backoff) and the effects were discarded.
    Retried,
    /// Attempts were exhausted, so the job was dead-lettered and the effects
    /// were applied. The results align index-wise with the effects' enqueues.
    DeadLettered(Vec<EnqueueResult>),
}

/// Outcome of [`Queue::wake_scheduled`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeOutcome {
    /// The job was `Scheduled`, and the call moved it to pending. It is
    /// claimable immediately.
    Woken,
    /// A job with this ID exists but is not `Scheduled` (it is pending,
    /// claimed, done or dead). Nothing was changed.
    NotScheduled,
    /// No job with this ID was found.
    NotFound,
}

/// Outcome of [`Queue::claim_by_id`].
#[derive(Debug)]
pub enum ClaimOutcome {
    /// The job was `Pending` or `Scheduled` and is now claimed by the caller.
    Claimed(Box<Claim>),
    /// A job with this ID exists but is claimed, done or dead. Nothing was
    /// changed.
    NotClaimable,
    /// No job with this ID was found.
    NotFound,
}

/// One page of a job listing. Returned by
/// [`QueueView::list_jobs`](crate::QueueView::list_jobs).
#[derive(Debug, Clone)]
pub struct JobPage {
    /// Jobs on this page, in the scan order of the listed state's key space
    /// (see [`QueueView::list_jobs`](crate::QueueView::list_jobs)).
    pub jobs: Vec<JobRecord>,
    /// Opaque resume token: pass it as the `cursor` of the next
    /// [`QueueView::list_jobs`](crate::QueueView::list_jobs) call to continue
    /// the listing. `None` when no further entries existed at scan time.
    pub next_cursor: Option<Vec<u8>>,
}

/// Maximum byte length of a caller-supplied [`EnqueueOptions::id_override`].
/// Enforces a sane cap on key sizes independently of the underlying object
/// store's path limits.
const MAX_ID_OVERRIDE_LEN: usize = 128;

/// Validate a caller-supplied job id. Caller-supplied ids must be
/// 1-[`MAX_ID_OVERRIDE_LEN`] bytes of `[A-Za-z0-9_-]`. The bound keeps ids safe
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

/// Outcome of [`Queue::enqueue_with_effects`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnqueueResult {
    /// A new job was enqueued, with its effects. The string is the id of the
    /// new job.
    New(String),
    /// A pending or scheduled job with the same `dedup_key` already existed. No
    /// new job was written and the effects were not applied. The string is the
    /// existing job's id.
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

/// A durable task queue backed by object storage.
///
/// `Queue` persists all job state to an object store via SlateDB.
///
/// # Lifecycle
///
/// Open with [`Queue::open`] or [`Queue::open_with_options`], use the queue,
/// then call [`Queue::close`] to flush state and shut down background tasks
/// cleanly.
///
/// # Background tasks
///
/// Background tasks run while the queue and its store are open:
///
/// - **Reaper**: re-queues or dead-letters jobs whose lease expired and runs
///   the done and dead-letter retention sweeps
///   ([`OpenOptions::reaper_interval`]).
/// - **Scheduler**: promotes jobs whose `run_at` is past from the
///   scheduled state to pending ([`OpenOptions::scheduler_interval`]).
/// - **Metrics sampler**, when [`OpenOptions::metrics_sample_interval`] is
///   set: emits per-queue depth and oldest-pending-age gauges.
/// - **Liveness heartbeat**, when [`OpenOptions::liveness_heartbeat`] is
///   set: commits a beat that a [`QueueReader`](crate::QueueReader) reads
///   from another process.
/// - **Close watcher**: once the store closes, fires the cancellation token of
///   every claim and fails later renewals with [`Error::ClaimLost`].
///
/// # Concurrency
///
/// `Queue` is `Send + Sync` and is shared through an [`Arc`], whose clone
/// increments a reference count. All workers must run in the same process:
/// SlateDB's single-writer constraint means the queue cannot be shared across
/// processes.
pub struct Queue {
    pub(crate) core: Arc<QueueCore>,
    reaper_task: BackgroundTask,
    scheduler_task: BackgroundTask,
    /// `Some` only when built with the `metrics` feature and
    /// `OpenOptions::metrics_sample_interval` was set.
    metrics_sampler: Option<BackgroundTask>,
    /// `Some` only when `OpenOptions::liveness_heartbeat` was set. Stopping
    /// returns the task so `close` can commit the closing beat with the task's
    /// counter.
    heartbeat: Option<BackgroundTask<crate::liveness::HeartbeatTask>>,
    close_watcher: BackgroundTask,
}

/// Outcome of [`Queue::wait_for_completion`].
///
/// The terminal variants identify the transition that ended the job. A
/// transition observed while waiting delivers the final [`JobRecord`] as the
/// settlement wrote it, with the payload inline, whether or not the queue
/// retains the record afterwards:
///
/// | Transition                                             | Outcome |
/// |--------------------------------------------------------|---------|
/// | Worker `ack` (success)                                 | `Done(record)` |
/// | Worker `nack` past `max_attempts`                      | `Dead(record)` |
/// | Worker [`Queue::dead_letter`] (permanent failure)      | `Dead(record)` |
/// | Reaper dead-letter (lease expired past `max_attempts`) | `Dead(record)` |
/// | [`Queue::cancel`] removing a `Pending`/`Scheduled` job | `Cancelled` |
///
/// A job that was already terminal when the call began is reported from its
/// retained record (`Done` only under [`QueueConfig::keep_done_jobs`], `Dead`
/// always). A job whose record was deleted before the call began is `NotFound`.
#[derive(Debug, Clone)]
pub enum WaitOutcome {
    /// The job was acknowledged.
    Done(Box<JobRecord>),
    /// The job was dead-lettered. The dead record is always kept.
    Dead(Box<JobRecord>),
    /// The job was removed by [`Queue::cancel`] before it was claimed. No
    /// record remains after the removal.
    Cancelled,
    /// No job with this ID was present at the start of the call.
    NotFound,
}

/// The state of a job at the start of a completion wait: its outcome when
/// already terminal, otherwise the registration its outcome will arrive on.
enum Completion {
    Settled(WaitOutcome),
    Pending(Registration),
}

/// The outcome a registration received. The sender is consumed only by a
/// settlement, so the channel cannot close without an outcome.
fn delivered(received: std::result::Result<WaitOutcome, oneshot::error::RecvError>) -> WaitOutcome {
    received.expect("a completion sender is consumed only by a settlement")
}

/// The committed outcome of [`Queue::settle_claim`]'s transaction.
struct SettledClaim<'e> {
    /// The record as written.
    job: JobRecord,
    end: ClaimEnd<'e>,
    /// The pending key when the job returned to pending.
    pending_key: Option<Vec<u8>>,
    /// The effects' results, or `None` when the transition discarded them.
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
        if !opts.in_process_compactor {
            settings.compactor_options = None;
        }
        if !opts.in_process_garbage_collector {
            settings.garbage_collector_options = None;
        }
        let mut builder = Db::builder(path, object_store)
            .with_merge_operator(Arc::new(QueueMergeOperator))
            .with_settings(settings);
        if let Some(wal_object_store) = opts.wal_object_store {
            builder = builder.with_wal_object_store(wal_object_store);
        }
        #[cfg(feature = "metrics")]
        {
            builder = builder.with_metrics_recorder(crate::obs::slatedb_recorder());
        }
        // A configuration for a queue name over the limit of the key encoding
        // does not apply to any job, so the names are validated here.
        for queue in opts.queue_configs.keys() {
            QueueName::new(queue.as_str())?;
        }
        let db = Arc::new(builder.build().await?);
        let core = Arc::new(QueueCore {
            view: QueueView::new(Handle::Writer(db.clone()), payload_store.clone()),
            db,
            clock: opts.clock,
            configs: QueueConfigs::new(opts.default_queue_config, opts.queue_configs),
            claim_cursor: ClaimCursor::new(),
            scheduled_bound: TimeBound::new(),
            done_bound: TimeBound::new(),
            lease_registry: LeaseRegistry::new(),
            completion_waiters: Arc::new(CompletionWaiters::default()),
            payload_store,
            id_gen: std::sync::Mutex::new(ulid::Generator::new()),
            closed: tokio_util::sync::CancellationToken::new(),
        });
        let close_watcher = BackgroundTask::spawn(&core.closed, |shutdown| {
            crate::queue_core::watch_store_close(core.clone(), shutdown)
        });
        crate::claim_cursor::restore_cursor_state(&core).await?;
        // A claimed record found at open belongs to a process that no longer
        // owns the store, so its claim is void and the job is re-queued
        // immediately. Runs after `restore_cursor_state` so each re-queued
        // job's pending insert is recorded against the restored bound.
        core.requeue_interrupted_claims().await?;
        let reaper_task = BackgroundTask::spawn_periodic(
            &core.closed,
            opts.reaper_interval,
            Reaper::new(core.clone()),
        );
        let scheduler_task = BackgroundTask::spawn_periodic(
            &core.closed,
            opts.scheduler_interval,
            Scheduler::new(core.clone()),
        );

        #[cfg(feature = "metrics")]
        let metrics_sampler = opts.metrics_sample_interval.map(|interval| {
            let sampler = crate::metrics_sampler::MetricsSampler::new(core.clone());
            BackgroundTask::spawn_periodic(&core.closed, interval, sampler)
        });
        #[cfg(not(feature = "metrics"))]
        let metrics_sampler: Option<BackgroundTask> = None;

        let heartbeat = match opts.liveness_heartbeat {
            Some(interval) => {
                let task = crate::liveness::HeartbeatTask::start(core.clone(), interval).await?;
                Some(BackgroundTask::spawn(&core.closed, move |shutdown| {
                    task.run(Ticker::new(interval, shutdown))
                }))
            }
            None => None,
        };

        Ok(Self {
            core,
            reaper_task,
            scheduler_task,
            metrics_sampler,
            heartbeat,
            close_watcher,
        })
    }

    /// The writer's live view of the store: the read-only queries, with a write
    /// visible to the next read.
    pub fn view(&self) -> &QueueView {
        &self.core.view
    }

    /// Current time as a Unix timestamp in milliseconds, as read from this
    /// queue's configured [`Clock`].
    pub(crate) fn now_ms(&self) -> u64 {
        self.core.now_ms()
    }

    /// Generate a job id without enqueuing anything.
    ///
    /// For callers that need the id before the enqueue commits, to write a
    /// record pointing at the job in the same transaction. Pass it as
    /// [`EnqueueOptions::id_override`]. Ids increase with call order and take
    /// their timestamp from this queue's [`Clock`].
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

    /// Build the lease handle for a claim: the capability the worker loops pass
    /// to [`Worker::process`](crate::worker::Worker::process). The handle
    /// extends the lease and exposes the claim's cancellation token but cannot
    /// settle the job, so a handler never has a claim and a queue together.
    /// Callers running `Worker::process` from their own claim loop build the
    /// handle here.
    pub fn lease_handle(&self, claim: &Claim) -> crate::lease::LeaseHandle {
        crate::lease::LeaseHandle::new(
            self.core.lease_registry.clone(),
            self.core.clock.clone(),
            claim.queue.clone(),
            claim.id.clone(),
            claim.claim_id(),
            claim.cancel_token().clone(),
        )
    }

    /// Wait until the store closes for any reason, including a fence by a
    /// second writer.
    pub(crate) async fn store_closed(&self) {
        self.core.closed.cancelled().await
    }

    /// Look up the configured `keep_done_jobs` retention for a queue. `None`
    /// means [`Self::ack`] deletes successful jobs outright on that queue.
    pub fn queue_keep_done_jobs(&self, queue: &str) -> Option<Duration> {
        self.queue_config(queue).keep_done_jobs
    }

    /// Look up the configured dead-letter retention for a queue. `None` means
    /// the dead-letter sweep is disabled for that queue.
    pub fn queue_dead_retention(&self, queue: &str) -> Option<Duration> {
        self.queue_config(queue).dead_retention
    }

    /// The [`Clock`] this queue was opened with, as an `Arc` clone that shares
    /// the time source with downstream crates for their own timestamp work.
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
    /// pending. Otherwise it waits in the scheduled key space until the
    /// background scheduler promotes it.
    ///
    /// Queue names are limited to [`crate::MAX_QUEUE_NAME_LEN`] bytes, and
    /// longer names return [`Error::InvalidQueueName`].
    #[instrument(skip(self, payload), fields(queue, job_id))]
    pub async fn enqueue_with(
        &self,
        queue: &str,
        payload: Vec<u8>,
        opts: EnqueueOptions,
    ) -> Result<String> {
        let prepared = self.core.prepare_job_record(queue, payload, opts)?;
        self.core
            .write_job(prepared, PreparedEffects::default())
            .await
            .map(|(result, _)| result.into_id())
    }

    /// Enqueue a job and apply `effects` in one transaction.
    ///
    /// When the job is new ([`EnqueueResult::New`]), the job and every effect
    /// commit together: the enqueues, the KV writes, the KV deletes and the
    /// expiry entries, which are recorded on their
    /// [`ExpiryIndex`](crate::ExpiryIndex) once the transaction commits. On a
    /// `dedup_key` hit ([`EnqueueResult::AlreadyEnqueued`]) the effects are not
    /// applied and the existing job's id is returned. Because a dedup hit
    /// discards the effects, derive them from the dedup key: a producer that
    /// retries after a crash then converges on the winning submission's writes.
    /// A write that must apply regardless of the dedup outcome belongs in
    /// [`Self::kv_put`] or [`Self::commit_effects`]. The second value returned
    /// contains one [`EnqueueResult`] per enqueue of `effects`, in order, as
    /// [`Self::ack_with`] returns them, and is empty on a dedup hit.
    ///
    /// Caller-supplied KV keys are internally scoped under a reserved user key
    /// tag so they cannot collide with Taquba's internal layout. Each value is
    /// validated against [`MAX_KV_VALUE_SIZE`](crate::MAX_KV_VALUE_SIZE) up
    /// front, and oversized values return [`Error::KvValueTooLarge`] before the
    /// transaction begins. Conflict retries are handled internally.
    ///
    /// ```no_run
    /// # use taquba::{EnqueueOptions, EnqueueResult};
    /// # use taquba::SettlementEffects;
    /// # async fn ex(q: &taquba::Queue) -> taquba::Result<()> {
    /// let effects = SettlementEffects::default().kv_put(b"runs/abc", b"submitted");
    /// let (outcome, _) = q.enqueue_with_effects(
    ///     "workflow-steps",
    ///     b"step-0-payload".to_vec(),
    ///     EnqueueOptions::default().dedup_key("run:abc:0".to_string()),
    ///     effects,
    /// ).await?;
    /// match outcome {
    ///     EnqueueResult::New(id) => println!("submitted: {id}"),
    ///     EnqueueResult::AlreadyEnqueued(id) => println!("already running: {id}"),
    /// }
    /// # Ok(()) }
    /// ```
    #[instrument(skip(self, payload, effects), fields(queue, job_id))]
    pub async fn enqueue_with_effects(
        &self,
        queue: &str,
        payload: Vec<u8>,
        opts: EnqueueOptions,
        effects: SettlementEffects,
    ) -> Result<(EnqueueResult, Vec<EnqueueResult>)> {
        let prepared = self.core.prepare_job_record(queue, payload, opts)?;
        let effects = self.core.prepare_effects(effects).await?;
        self.core.write_job(prepared, effects).await
    }

    /// Apply `effects` as one transaction without a job transition: the
    /// enqueues, the KV writes and the KV deletes commit together or not at
    /// all. Returns one [`EnqueueResult`] per enqueue, in order. An enqueue
    /// whose `dedup_key` matches a pending or scheduled job is reported as
    /// [`EnqueueResult::AlreadyEnqueued`] while the rest of the effects still
    /// apply, unlike [`Self::enqueue_with_effects`], whose effects belong to
    /// the one job it enqueues.
    ///
    /// The effects of a claim's own settlement belong on [`Self::ack_with`] and
    /// the other `*_with` settlements. This method is for state that a layer
    /// keeps separate from the queue and must move in one step without a
    /// transition of its own, such as reconciling the state of a job the reaper
    /// dead-lettered.
    pub async fn commit_effects(&self, effects: SettlementEffects) -> Result<Vec<EnqueueResult>> {
        let committed = self.core.commit_effects_where(effects, &[]).await?;
        Ok(committed.expect("a commit without a compare is unconditional"))
    }

    /// Apply `effects` as [`Self::commit_effects`] does, only if the current
    /// state of every user KV key of `compares` matches its expected state:
    /// `Some(v)` requires the key to contain exactly `v`, and `None` requires
    /// the key to be absent.
    ///
    /// Returns the enqueue results when every state matched and the effects
    /// were applied, and `None` when one did not (nothing is changed in that
    /// case). The compares and the effects execute in one transaction, so no
    /// concurrent write can be interleaved between a compare and the commit:
    /// either this call applies the effects to the state it compared against,
    /// or it reports `None`. The effects are durable before the call returns
    /// `Some`, and they can write or delete a compared key.
    pub async fn kv_compare_commit(
        &self,
        compares: &[(&[u8], Option<&[u8]>)],
        effects: SettlementEffects,
    ) -> Result<Option<Vec<EnqueueResult>>> {
        self.core.commit_effects_where(effects, compares).await
    }

    /// Claim the next pending job using the configured default lease duration.
    pub async fn claim_next(&self, queue: &str) -> Result<Option<Claim>> {
        let lease_duration = self.queue_config(queue).lease_duration;
        self.claim(queue, lease_duration).await
    }

    /// Block up to `max_wait` for a job to become claimable on `queue`.
    ///
    /// The wakeup is queue-scoped and delivered to one waiter per inserted job,
    /// so a pool of waiting workers does not contend on the claim path when a
    /// single job arrives. To wait on several queues at once, `select!` over
    /// one call per queue. Returning does not guarantee a job is still
    /// available, because another worker can claim it first. Follow up with a
    /// claim call, and wait again if it returns `None`.
    pub async fn wait_for_jobs_on(&self, queue: &str, max_wait: Duration) {
        let wakeup = self.core.claim_cursor.wakeup_for(queue);
        let notified = wakeup.notified();
        tokio::pin!(notified);
        // `enable` consumes a permit left by an insert written before this
        // waiter subscribed, so the wait returns immediately and does not sleep
        // past an already-available job.
        notified.as_mut().enable();
        let _ = tokio::time::timeout(max_wait, notified).await;
    }

    /// Claim the next pending job, waiting up to `max_wait` for one to appear.
    ///
    /// This replaces a polling [`Self::claim_next`] + [`tokio::time::sleep`]
    /// loop: when a job is written to `queue` (enqueue, retry requeue, dead-job
    /// requeue, scheduled-job promotion, lease reap), the wakeup is delivered
    /// via an in-memory notify so the worker resumes immediately, without
    /// waiting out the poll interval. Wakeups are queue-scoped and delivered to
    /// one waiter per inserted job, so a pool of waiting workers does not
    /// contend on the claim path when a single job arrives. Only when nothing
    /// is available within `max_wait` does the call return `None`.
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
                // Pass the wakeup on: the wait below can consume a permit that
                // another waiter needs, and while a backlog remains each
                // delivered job wakes one more worker.
                self.core.claim_cursor.wakeup_for(queue).notify_one();
                return Ok(Some(job));
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            // An insert between the empty scan and this wait leaves a permit
            // that the wait consumes, so no insert is missed. A wake does not
            // reserve the job: another worker can claim it first, and then the
            // loop waits out the remaining time. A stale permit costs one extra
            // pass.
            self.wait_for_jobs_on(queue, deadline - now).await;
        }
    }

    /// Claim the next pending job with an explicit lease duration. Returns
    /// `None` if the queue is empty.
    ///
    /// The claim commit does not await WAL durability. If the process crashes
    /// before the claim is flushed, the job is still pending on recovery and is
    /// redelivered immediately, without waiting for its lease to expire.
    /// At-least-once delivery is unaffected. Any later durable commit (ack,
    /// nack, enqueue) flushes preceding WAL entries, so a settled job's claim
    /// is always durable.
    ///
    /// Same-queue claim attempts serialise through an in-process
    /// `tokio::sync::Mutex`, so a transaction-conflict retry does not decide
    /// which worker takes the head of the pending key space. The lock is
    /// per-queue, so different queues' claim paths still run in parallel.
    ///
    /// A per-queue in-memory cursor records the most recently claimed key and
    /// is used as the start bound on the next scan. This lets steady-state
    /// claims skip over the tombstones left by previously claimed (and deleted)
    /// pending entries. When the cursor scan ends without a key inside the
    /// queue's prefix (cursor exhausted, or an older job was requeued by `nack`
    /// before the cursor), the claim falls back to a front prefix scan and
    /// resets the cursor. When the front scan also ends without a key, the
    /// queue is marked empty in memory and subsequent claims return `None`
    /// without scanning until the next pending insert, so polling an empty
    /// queue does not re-read the tombstone band left by previously claimed
    /// jobs.
    #[instrument(skip(self), fields(queue))]
    pub async fn claim(&self, queue: &str, lease_duration: Duration) -> Result<Option<Claim>> {
        Ok(self.claim_batch(queue, 1, lease_duration).await?.pop())
    }

    /// Claim up to `max_jobs` pending jobs in one transaction.
    ///
    /// Jobs are returned in claim order (priority, then enqueue order) and
    /// share one lease with one start instant. Size batches so the lease covers
    /// processing the whole batch, or renew leases as the batch progresses.
    /// Returns an empty `Vec` when the queue is empty and fewer than `max_jobs`
    /// jobs when the queue runs out. Fails with the store's error once the
    /// store closes for any reason, including a fence by a second writer.
    ///
    /// One batch costs one claim-lock hold, one transaction and one commit
    /// regardless of size, so a fetcher that claims batches and dispatches jobs
    /// to local workers contends far less on a busy queue than one
    /// [`Self::claim`] call per job.
    /// [`run_worker_concurrent`](crate::run_worker_concurrent) is that pattern
    /// built in: it claims batches sized to its free capacity. Durability,
    /// serialisation and cursor semantics are those of [`Self::claim`].
    #[instrument(skip(self), fields(queue, max_jobs))]
    pub async fn claim_batch(
        &self,
        queue: &str,
        max_jobs: usize,
        lease_duration: Duration,
    ) -> Result<Vec<Claim>> {
        let queue = QueueName::new(queue)?;
        if max_jobs == 0 {
            return Ok(Vec::new());
        }
        self.core
            .claim_batch(&queue, max_jobs, lease_duration)
            .await
    }

    /// Claim one pending or scheduled job by ID with an explicit lease
    /// duration.
    ///
    /// This is the targeted counterpart of [`Self::claim`]: the same transition
    /// (to claimed), applied to a single job by ID at the caller's initiative
    /// and without the scan, for a producer that performs the job it created or
    /// an operator that runs one job now. A scheduled job is claimed before its
    /// `run_at`. The claim consumes an attempt, releases the job's dedup key
    /// and registers its lease exactly as a scan claim does, so the reaper
    /// requeues it when the lease expires and the next open requeues it after a
    /// crash. The claim commits without awaiting WAL durability, as
    /// [`Self::claim`] does.
    ///
    /// Exactly one caller wins the transition: a concurrent scan claim,
    /// scheduler promotion, [`Self::cancel`] and this call conflict on the
    /// record, and the loser observes [`ClaimOutcome::NotClaimable`] or
    /// [`ClaimOutcome::NotFound`].
    #[instrument(skip(self), fields(job_id = %id))]
    pub async fn claim_by_id(&self, id: &str, lease_duration: Duration) -> Result<ClaimOutcome> {
        self.core.claim_by_id(id, lease_duration).await
    }

    /// Acknowledge successful completion.
    ///
    /// By default the job is deleted outright. The success counter in
    /// [`QueueStats::done`](crate::QueueStats::done) is still incremented.
    ///
    /// Set [`QueueConfig::keep_done_jobs`] (per-queue, or on
    /// [`OpenOptions::default_queue_config`] for an instance-wide default) to
    /// retain completed jobs for a bounded duration.
    pub async fn ack(&self, claim: &Claim) -> Result<()> {
        self.ack_with(claim, SettlementEffects::default())
            .await
            .map(|_| ())
    }

    /// Acknowledge successful completion and apply `effects` in the same
    /// transaction.
    ///
    /// Either the acknowledgement and every effect land together or nothing
    /// does. In particular, if the job's claim is no longer present (its lease
    /// expired and the reaper requeued it), the call fails with
    /// [`Error::ClaimLost`] and no effect is applied, so a follow-up job exists
    /// only if this settlement won.
    ///
    /// Each enqueue in [`SettlementEffects::enqueues`] behaves exactly like
    /// [`Self::enqueue_with`]: a `dedup_key` hit downgrades that request to
    /// [`EnqueueResult::AlreadyEnqueued`] without affecting the ack or the
    /// other effects, and a future `run_at` lands the job in the scheduled key
    /// space. The returned results align index-wise with `effects.enqueues`. KV
    /// writes and deletes behave like [`Self::kv_put`] and [`Self::kv_delete`].
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

    /// Report failure. Re-queues if attempts < max_attempts, otherwise
    /// dead-letters.
    ///
    /// Re-queued jobs honour the queue's `retry_backoff_base` and
    /// `retry_backoff_max`: when the backoff is non-zero, the job waits in the
    /// scheduled key space and the background scheduler promotes it once the
    /// delay elapses. With zero backoff the job goes straight back to pending.
    pub async fn nack(&self, claim: &Claim, error: &str) -> Result<()> {
        self.nack_with(claim, error, SettlementEffects::default())
            .await
            .map(|_| ())
    }

    /// Report failure and apply `effects` in the same transaction when the
    /// failure dead-letters the job.
    ///
    /// Behaves like [`Self::nack`]. While attempts remain the job is re-queued,
    /// the effects are discarded and the call returns [`NackOutcome::Retried`].
    /// A later settlement passes its own effects. Once attempts are exhausted
    /// the job is dead-lettered and the effects are applied atomically with
    /// that transition, exactly as in [`Self::ack_with`], and the call returns
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
                if stored.is_last_attempt() {
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

    /// Dead-letter a claimed job immediately, regardless of its `attempts`. Use
    /// this when the failure is *known* to be permanent and a retry cannot
    /// succeed.
    ///
    /// Unlike [`Self::nack`], this does not increment `attempts` or schedule a
    /// backoff: the job goes straight to the dead-letter set.
    /// [`worker::run_worker`](crate::worker::run_worker) and
    /// [`worker::run_worker_concurrent`](crate::worker::run_worker_concurrent)
    /// dead-letter through [`Self::dead_letter_with`] when a worker returns
    /// [`worker::PermanentFailure`](crate::worker::PermanentFailure).
    pub async fn dead_letter(&self, claim: &Claim, reason: &str) -> Result<()> {
        self.dead_letter_with(claim, reason, SettlementEffects::default())
            .await
            .map(|_| ())
    }

    /// Dead-letter a claimed job and apply `effects` in the same transaction.
    ///
    /// Behaves like [`Self::dead_letter`]. The effects behave exactly as in
    /// [`Self::ack_with`], and the returned results align index-wise with the
    /// effects' enqueues.
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
    /// [`Self::nack_with`] and [`Self::dead_letter_with`]. The effects are
    /// prepared once before the retry loop. Every iteration fences the claim,
    /// chooses the transition from the stored record and the current time with
    /// `end_for`, stages it and, when the transition is terminal, stages the
    /// effects. Returns the written record and the effects' results, `None`
    /// when the transition discarded them.
    async fn settle_claim<'e>(
        &self,
        claim: &Claim,
        effects: SettlementEffects,
        end_for: impl Fn(&JobRecord, u64) -> ClaimEnd<'e>,
    ) -> Result<(JobRecord, Option<Vec<EnqueueResult>>)> {
        let prepared = self.core.prepare_effects(effects).await?;
        let claim_id = claim.claim_id();
        let (queue, id) = (&claim.queue, claim.id.as_str());

        let settled: Result<SettledClaim<'e>> = {
            let (prepared, end_for) = (&prepared, &end_for);
            retry(&self.core.db, Durability::Awaited, |txn| async move {
                // The returned record is the base for the written record. The
                // claim's copy predates a cancel committed during the delivery.
                let mut job =
                    take_claim(&txn, &self.core.lease_registry, queue, id, claim_id).await?;
                let now = self.now_ms();
                let end = end_for(&job, now);
                let pending_key = stage_claim_end(&txn, &mut job, &end, now)?;
                let staged = if end.is_terminal() {
                    Some(self.core.stage_effects(&txn, prepared).await?)
                } else {
                    None
                };
                Ok(Attempt::Commit(txn, (job, end, pending_key, staged)))
            })
            .await
        }
        .map(|(job, end, pending_key, staged)| SettledClaim {
            job,
            end,
            pending_key,
            results: staged.map(|s| self.core.note_staged_effects(s)),
        });

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
                claim_id,
                settled.pending_key.as_deref(),
                Some(claim),
            )
            .await;
        Ok((settled.job, settled.results))
    }

    /// Move the dead-letter job `id` back to the pending queue for a fresh
    /// attempt.
    ///
    /// Resets `attempts` to 0 and clears `last_error`, so the job is delivered
    /// up to its `max_attempts` again. The stored record is read inside the
    /// transaction, and the commit is durable before the call returns. A job
    /// without a record, removed by the retention sweep or by a concurrent
    /// revival, is [`Error::JobNotFound`], and a job in any state other than
    /// `Dead` is [`Error::InvalidState`].
    #[instrument(skip(self), fields(job_id = %id))]
    pub async fn requeue_dead_job(&self, id: &str) -> Result<()> {
        let (job, pending): (JobRecord, Vec<u8>) = self
            .core
            .transition_by_id(
                id,
                Durability::Awaited,
                || Err(Error::JobNotFound(id.to_string())),
                |txn, current_key, mut job| async move {
                    if job.status != JobStatus::Dead {
                        txn.rollback();
                        return Err(Error::InvalidState);
                    }
                    txn.delete(&current_key)?;
                    job.attempts = 0;
                    job.last_error = None;
                    job.claimed_at = None;
                    job.failed_at = None;
                    // Revival clears any prior cancel request: the operator
                    // chose to start this job afresh.
                    job.cancel_requested = false;
                    let pending = stage_to_pending(&txn, &mut job, JobStatus::Dead)?;
                    // The history is kept across the revival. The marker
                    // separates entries recorded before it from the reset
                    // attempt counter.
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
                    Ok(Attempt::Commit(txn, (job, pending)))
                },
            )
            .await?;
        self.core
            .claim_cursor
            .note_pending_insert(&job.queue, &pending);
        debug!(queue = %job.queue, job_id = %job.id, "dead job re-queued");
        Ok(())
    }

    /// Extend the lease on a claimed job, returning the new expiry as epoch
    /// milliseconds.
    ///
    /// Call this periodically for long-running jobs to prevent the reaper from
    /// treating them as abandoned and re-queuing them.
    ///
    /// The lease is process state, so renewal is a synchronous memory operation
    /// without a durable write. The claim is unchanged and stays valid for
    /// settlement, and [`Self::lease_expiry`] reports the current value.
    ///
    /// Fails with [`Error::ClaimLost`] once the claim ended, the reaper began
    /// re-queuing the expired lease or the store closed. Fails with
    /// [`Error::CancelRequested`] once [`Self::cancel`] was called on the job,
    /// and the lease then expires.
    ///
    /// This method is for callers that call [`Self::claim`] /
    /// [`Self::claim_batch`] directly and hold the [`Claim`]. Inside a
    /// [`Worker::process`](crate::worker::Worker::process) hook the claim stays
    /// with the worker loop. Extend the lease there through the
    /// [`crate::LeaseHandle`] that the hook receives.
    #[instrument(skip(self, claim), fields(queue = %claim.queue, job_id = %claim.id))]
    pub fn renew_lease(&self, claim: &Claim, extension: Duration) -> Result<u64> {
        let job = claim.job();
        let new_expiry = self.now_ms() + extension.as_millis() as u64;
        if self.core.lease_registry.renew(
            &job.queue,
            &job.id,
            claim.claim_id(),
            claim.cancel_token(),
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
    /// in-memory lease registry and reflects any renewal. Returns `None` when
    /// no live lease for the job exists in this process, including when the job
    /// is in any state other than `Claimed`.
    pub fn lease_expiry(&self, queue: &str, id: &str) -> Option<u64> {
        let queue = QueueName::new(queue).ok()?;
        self.core
            .lease_registry
            .current(&queue, id)
            .map(|(expires_at, _)| expires_at)
    }

    /// Wait until the given job reaches a terminal state.
    ///
    /// Wake-up is notification-based: every terminal transition in the queue
    /// (`ack`, `nack` past `max_attempts`, `dead_letter`, `cancel`-Removed,
    /// reaper dead-letter) delivers its outcome to the tasks waiting on that
    /// job. There is no per-job polling. Transient transitions (a `nack` that
    /// re-queues for retry, the reaper re-queuing an expired lease, the
    /// scheduler promoting a scheduled job) do **not** wake the wait: they are
    /// not terminal.
    ///
    /// See [`WaitOutcome`] for the transition each variant reports and whether
    /// it contains a record. [`Self::wait_for_completion_timeout`] bounds the
    /// wait.
    ///
    /// # Multiple waiters per job
    ///
    /// Several tasks can wait on the same job ID concurrently, and each
    /// receives the same outcome when the terminal transition fires.
    ///
    /// # Already-terminal jobs
    ///
    /// If the job is already terminal (`Done` with `keep_done_jobs`, or `Dead`)
    /// at call time, this returns immediately with the kept record. A
    /// subscription before the enqueue is unnecessary, because the pre-check
    /// covers it.
    ///
    /// # Across-process semantics
    ///
    /// The completion signal is in-process. A wait in process A on a job worked
    /// in process B is not supported, because taquba is single-process by
    /// design.
    pub async fn wait_for_completion(&self, id: &str) -> Result<WaitOutcome> {
        match self.completion(id).await? {
            Completion::Settled(outcome) => Ok(outcome),
            Completion::Pending(mut registration) => Ok(delivered(registration.receiver().await)),
        }
    }

    /// [`Self::wait_for_completion`] bounded by `timeout`: `None` when the
    /// timeout elapses before the job reaches a terminal state.
    pub async fn wait_for_completion_timeout(
        &self,
        id: &str,
        timeout: Duration,
    ) -> Result<Option<WaitOutcome>> {
        match self.completion(id).await? {
            Completion::Settled(outcome) => Ok(Some(outcome)),
            Completion::Pending(mut registration) => {
                Ok(tokio::time::timeout(timeout, registration.receiver())
                    .await
                    .ok()
                    .map(delivered))
            }
        }
    }

    async fn completion(&self, id: &str) -> Result<Completion> {
        // Registered before the storage read: a terminal transition that
        // commits after the read then reaches the registration, and one that
        // commits before it is visible in the read.
        let mut registration = self.core.completion_waiters.register(id);

        match self.view().get_job(id).await? {
            Some(job) => match job.status {
                JobStatus::Done => Ok(Completion::Settled(WaitOutcome::Done(Box::new(job)))),
                JobStatus::Dead => Ok(Completion::Settled(WaitOutcome::Dead(Box::new(job)))),
                _ => Ok(Completion::Pending(registration)),
            },
            // A transition that removed the record between the registration and
            // the read delivered its outcome already, or is about to. The
            // registration is consulted before the ID is reported absent.
            None => Ok(Completion::Settled(
                registration.try_outcome().unwrap_or(WaitOutcome::NotFound),
            )),
        }
    }

    /// Cancel a job, handling every lifecycle state.
    ///
    /// - **`Pending` or `Scheduled`**: removes the job, its attempt history and
    ///   its dedup key in one transaction. Returns [`CancelOutcome::Removed`].
    /// - **`Claimed` (a worker is processing it)**: persists a
    ///   `cancel_requested` flag on the job record and fires the
    ///   in-process [`tokio_util::sync::CancellationToken`] exposed on
    ///   [`Claim::cancel_token`] and
    ///   [`LeaseHandle::cancel_token`](crate::LeaseHandle::cancel_token).
    ///   Returns [`CancelOutcome::Requested`].
    /// - **`Done` / `Dead` / unknown**: returns [`CancelOutcome::NotFound`].
    ///
    /// Cancellation does not abort a running worker. A worker that uses
    /// `select!` on the token stops at a point where its own state is
    /// consistent. A worker that ignores the token runs to completion. A
    /// renewal of a cancelled claim fails with [`Error::CancelRequested`], so
    /// the worker keeps the claim at most until its lease expires. The flag is
    /// stored, so every later claim of the job starts with its token already
    /// fired until [`Self::requeue_dead_job`] clears the flag.
    pub async fn cancel(&self, id: &str) -> Result<CancelOutcome> {
        self.cancel_with(id, SettlementEffects::default())
            .await
            .map(|(outcome, _)| outcome)
    }

    /// Cancel a job and apply `effects` in the same transaction as its removal.
    ///
    /// Behaves like [`Self::cancel`]. On [`CancelOutcome::Removed`] the effects
    /// are applied atomically with the removal, exactly as in
    /// [`Self::ack_with`], and the returned results align index-wise with the
    /// effects' enqueues. On every other outcome the effects are discarded and
    /// the results are empty. A claimed job's terminal settlement passes its
    /// own effects.
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

    /// The transaction loop of [`Self::cancel_with`]: resolve the job, apply
    /// the transition its state allows and commit, including the post-commit
    /// notifications of the committed outcome.
    async fn cancel_txn(
        &self,
        id: &str,
        prepared: &PreparedEffects,
    ) -> Result<(CancelOutcome, Vec<EnqueueResult>)> {
        // `Err` is an outcome reached without a commit.
        let committed = self
            .core
            .transition_by_id(
                id,
                Durability::Awaited,
                || Ok(Err(CancelOutcome::NotFound)),
                |txn, current_key, mut job| async move {
                    let (msg, outcome, staged) = match job.status {
                        JobStatus::Pending | JobStatus::Scheduled => {
                            stage_remove(&txn, &current_key, &job)?;
                            let staged = self.core.stage_effects(&txn, prepared).await?;
                            (
                                "pending/scheduled job cancelled",
                                CancelOutcome::Removed,
                                Some(staged),
                            )
                        }
                        JobStatus::Claimed => {
                            if job.cancel_requested {
                                // The flag is already persisted. The token is
                                // fired again, because a re-claim after the
                                // first request has a fresh token.
                                txn.rollback();
                                self.core.lease_registry.cancel(&job.queue, id);
                                debug!(job_id = %id, "cancel re-requested on claimed job");
                                return Ok(Attempt::Abort(Err(CancelOutcome::Requested)));
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
                            return Ok(Attempt::Abort(Err(CancelOutcome::NotFound)));
                        }
                    };
                    Ok(Attempt::Commit(txn, Ok((job, msg, outcome, staged))))
                },
            )
            .await?;
        let (job, msg, outcome, staged) = match committed {
            Ok(committed) => committed,
            Err(outcome) => return Ok((outcome, Vec::new())),
        };
        // Fired on the Removed path as well: the worker of a claim the reaper
        // requeued just before this call can still observe the token. That
        // claim's end removes the entry.
        self.core.lease_registry.cancel(&job.queue, id);
        let results = staged
            .map(|s| self.core.note_staged_effects(s))
            .unwrap_or_default();
        // Removed = terminal (job is gone). Requested = not yet terminal, and
        // the worker's settlement delivers the outcome when it acks / nacks /
        // dead-letters.
        if matches!(outcome, CancelOutcome::Removed) {
            // The record is deleted, so its payload object (if any) is removed
            // here, after the commit.
            self.core.payload_store.delete_for(&job).await;
            self.core
                .completion_waiters
                .settle(id, || WaitOutcome::Cancelled);
        }
        debug!(job_id = %id, "{msg}");
        Ok((outcome, results))
    }

    /// Move a `Scheduled` job to pending immediately, before its `run_at`,
    /// optionally attaching `wake_payload` bytes to the record.
    ///
    /// This is the targeted counterpart of the scheduler's due-job promotion:
    /// the same transition (scheduled to pending), applied to one job by ID
    /// when the caller asks, without waiting for `run_at`. On
    /// [`WakeOutcome::Woken`] the job is claimable immediately and any worker
    /// waiting on the queue is notified.
    ///
    /// The wake sets [`JobRecord::woken_at`], which distinguishes an early wake
    /// from ordinary promotion at `run_at` regardless of whether bytes were
    /// attached. `wake_payload` is stored on [`JobRecord::wake_payload`]. Both
    /// values persist on the record across later transitions, so redelivery
    /// after a lease expiry observes them again. The payload is part of the
    /// serialized record that each transition rewrites. It is intended for
    /// coordination data, and bulk payload does not belong in it.
    ///
    /// Exactly one caller wins the transition: a concurrent scheduler
    /// promotion, `wake_scheduled` call, or [`Self::cancel`] and this call
    /// conflict on the scheduled record, and the loser observes
    /// [`WakeOutcome::NotScheduled`] or [`WakeOutcome::NotFound`]. The commit
    /// is durable before the call returns.
    pub async fn wake_scheduled(
        &self,
        id: &str,
        wake_payload: Option<Vec<u8>>,
    ) -> Result<WakeOutcome> {
        // `Err` is an outcome reached without a commit.
        let wake_payload = &wake_payload;
        let woken: std::result::Result<(JobRecord, Vec<u8>), WakeOutcome> = self
            .core
            .transition_by_id(
                id,
                Durability::Awaited,
                || Ok(Err(WakeOutcome::NotFound)),
                |txn, current_key, mut job| async move {
                    if job.status != JobStatus::Scheduled {
                        txn.rollback();
                        return Ok(Attempt::Abort(Err(WakeOutcome::NotScheduled)));
                    }
                    txn.delete(&current_key)?;
                    job.woken_at = Some(self.now_ms());
                    job.wake_payload = wake_payload.clone();
                    let pending = stage_to_pending(&txn, &mut job, JobStatus::Scheduled)?;
                    Ok(Attempt::Commit(txn, Ok((job, pending))))
                },
            )
            .await?;
        match woken {
            Ok((job, pending)) => {
                self.core
                    .claim_cursor
                    .note_pending_insert(&job.queue, &pending);
                debug!(job_id = %id, queue = %job.queue, "scheduled job woken");
                Ok(WakeOutcome::Woken)
            }
            Err(outcome) => Ok(outcome),
        }
    }

    /// Enqueue multiple jobs atomically in a single transaction.
    ///
    /// All jobs use the queue's configured `max_attempts` and
    /// `default_priority`. Returns the IDs in the same order as `payloads`. The
    /// enqueues are committed as the effects of [`Self::commit_effects`].
    pub async fn enqueue_batch(&self, queue: &str, payloads: Vec<Vec<u8>>) -> Result<Vec<String>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let timer = crate::obs::start();
        let enqueues = payloads
            .into_iter()
            .map(|payload| EnqueueRequest {
                queue: queue.to_string(),
                payload,
                options: EnqueueOptions::default(),
            })
            .collect();
        let results = self
            .commit_effects(SettlementEffects::default().enqueues(enqueues))
            .await?;
        crate::obs::enqueue_committed(queue, timer);
        debug!(queue = queue, count = results.len(), "batch enqueued");
        // A batch job does not have a dedup key, so every result is `New`.
        Ok(results
            .into_iter()
            .map(|result| match result {
                EnqueueResult::New(id) | EnqueueResult::AlreadyEnqueued(id) => id,
            })
            .collect())
    }

    /// Trigger an immediate reap sweep (primarily useful in tests and tooling).
    pub async fn reap_now(&self) -> Result<()> {
        self.core.reap_expired().await
    }

    /// Trigger an immediate scheduled-job promotion sweep (primarily useful in
    /// tests).
    pub async fn promote_scheduled_now(&self) -> Result<()> {
        self.core.promote_due_jobs().await
    }

    /// Shut down the background reaper and scheduler, persist each queue's
    /// claim-scan state, then close the underlying database.
    ///
    /// The persisted state lets the next open resume claims at the recorded
    /// bound without a scan of the tombstone band left by previously claimed
    /// jobs. It is written best-effort: a failed write is logged, the database
    /// still closes and the next open scans from the front of the prefix. With
    /// [`OpenOptions::liveness_heartbeat`] set, a final beat marked closed is
    /// committed best-effort as well, so readers can distinguish this close
    /// from a writer that stopped committing beats.
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
        if let Err(e) = crate::claim_cursor::persist_cursor_state(&self.core).await {
            warn!("persisting the claim-scan state failed: {e}");
        }
        self.core.db.close().await?;
        self.close_watcher.stop().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
