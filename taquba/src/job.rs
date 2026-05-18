use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

/// A single job stored in a Taquba queue.
///
/// Returned by [`Queue::claim`](crate::Queue::claim),
/// [`Queue::get_job`](crate::Queue::get_job),
/// [`Queue::dead_jobs`](crate::Queue::dead_jobs), and the worker trait.
/// Mostly read-only from the caller's perspective; fields are mutated by
/// Taquba as the job moves through its lifecycle (see [`JobStatus`]).
///
/// All timestamp fields (`enqueued_at`, `claimed_at`, `lease_expires_at`,
/// `run_at`, `completed_at`, `failed_at`) are wall-clock milliseconds since
/// the UNIX epoch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    /// Unique [ULID](https://github.com/ulid/spec) assigned at enqueue time.
    /// Lexicographically sorted by enqueue time within the same millisecond.
    pub id: String,
    /// Logical queue this job belongs to.
    pub queue: String,
    /// Application-defined payload.
    pub payload: Vec<u8>,
    /// Optional string-keyed metadata stored alongside the payload. Useful for
    /// data that benefits from being separable from the opaque body, e.g. HTTP
    /// headers or a target URL for a webhook delivery, or a schedule name and
    /// nominal run time for a cron-style job. Set via
    /// [`EnqueueOptions::headers`](crate::EnqueueOptions::headers); defaults to
    /// empty.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
    /// Current lifecycle state.
    pub status: JobStatus,
    /// How many delivery attempts have been started so far. Incremented on
    /// each [`Queue::claim`](crate::Queue::claim).
    pub attempts: u32,
    /// Maximum delivery attempts before the job is dead-lettered. Defaults to
    /// the queue's configured value (see [`QueueConfig::max_attempts`](crate::QueueConfig)).
    pub max_attempts: u32,
    /// When the job was first enqueued. Preserved across
    /// [`Queue::requeue_dead_job`](crate::Queue::requeue_dead_job) so the
    /// original enqueue time survives a re-fail cycle.
    pub enqueued_at: u64,
    /// When the most recent claim happened, if the job has ever been claimed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_at: Option<u64>,
    /// When the current lease expires. `Some` only while `status == Claimed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_expires_at: Option<u64>,
    /// Earliest time at which a scheduled job becomes claimable. `Some` only
    /// while `status == Scheduled`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_at: Option<u64>,
    /// Priority bucket; lower numbers are claimed first. See
    /// [`PRIORITY_HIGH`](crate::PRIORITY_HIGH),
    /// [`PRIORITY_NORMAL`](crate::PRIORITY_NORMAL), and
    /// [`PRIORITY_LOW`](crate::PRIORITY_LOW).
    pub priority: u32,
    /// The most recent error message reported via
    /// [`Queue::nack`](crate::Queue::nack), or a Taquba-generated message
    /// (e.g. `"lease expired"`) when the reaper dead-letters a job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Set when [`EnqueueOptions::dedup_key`](crate::EnqueueOptions::dedup_key)
    /// was supplied at enqueue. Cleared when the job is first claimed so the
    /// same key can be re-used for a new job after the current one starts
    /// processing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedup_key: Option<String>,
    /// When the job was successfully acked. `Some` only when the record was
    /// kept (see [`QueueConfig::keep_done_jobs`](crate::QueueConfig::keep_done_jobs)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<u64>,
    /// When the job entered the dead-letter state. Used by the background
    /// retention sweep to age out old dead jobs without depending on
    /// `enqueued_at` (which is stale after a requeue / re-fail cycle).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_at: Option<u64>,
    /// Whether [`Queue::cancel`](crate::Queue::cancel) has been called while
    /// this job was `Claimed`. Persisted so that a re-claim after lease
    /// expiry surfaces a pre-cancelled [`Self::cancel_token`] instead of
    /// resetting state silently.
    ///
    /// Workers do not need to read this directly; they should watch
    /// [`Self::cancel_token`] instead.
    #[serde(default, skip_serializing_if = "is_false")]
    pub cancel_requested: bool,
    /// In-process cooperative cancellation token. Populated when the job
    /// is returned from any `Queue::claim*` call and `None` for jobs read
    /// via [`Queue::get_job`](crate::Queue::get_job),
    /// [`Queue::dead_jobs`](crate::Queue::dead_jobs), or any other
    /// non-claim path.
    ///
    /// Workers may `select!` on this token to short-circuit when an
    /// external [`Queue::cancel`](crate::Queue::cancel) fires. Acks
    /// normally to clear the job; the queue treats cancellation as a
    /// request, never as a forced abort.
    ///
    /// Not persisted: `tokio_util::sync::CancellationToken` is an
    /// in-process primitive. After a worker crashes and the reaper
    /// requeues the job, the next claim creates a fresh token (which is
    /// immediately fired if [`Self::cancel_requested`] is `true`).
    #[serde(skip, default)]
    pub cancel_token: Option<CancellationToken>,
}

fn is_false(v: &bool) -> bool {
    !*v
}

/// The lifecycle state of a [`JobRecord`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JobStatus {
    /// Waiting in the queue, ready to be claimed by the next worker.
    Pending,
    /// Waiting for its `run_at` time. Promoted to `Pending` by the background
    /// scheduler. Also used while a nacked job sits in the retry-backoff
    /// window.
    Scheduled,
    /// Held under a lease by a worker. Will be re-queued by the reaper if the
    /// lease expires before [`Queue::ack`](crate::Queue::ack) or
    /// [`Queue::nack`](crate::Queue::nack).
    Claimed,
    /// Successfully completed. Only persisted if
    /// [`QueueConfig::keep_done_jobs`](crate::QueueConfig::keep_done_jobs) is
    /// set on the job's queue; otherwise the record is deleted on ack.
    Done,
    /// Exhausted all retry attempts and was moved to the dead-letter queue.
    /// Inspected via [`Queue::dead_jobs`](crate::Queue::dead_jobs); revived
    /// via [`Queue::requeue_dead_job`](crate::Queue::requeue_dead_job).
    Dead,
}
