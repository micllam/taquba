use serde::{Deserialize, Serialize};

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
    /// kept (see [`OpenOptions::keep_done_jobs`](crate::OpenOptions::keep_done_jobs)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<u64>,
    /// When the job entered the dead-letter state. Used by the background
    /// retention sweep to age out old dead jobs without depending on
    /// `enqueued_at` (which is stale after a requeue / re-fail cycle).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_at: Option<u64>,
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
    /// [`OpenOptions::keep_done_jobs`](crate::OpenOptions::keep_done_jobs) is
    /// set; otherwise the record is deleted on ack.
    Done,
    /// Exhausted all retry attempts and was moved to the dead-letter queue.
    /// Inspected via [`Queue::dead_jobs`](crate::Queue::dead_jobs); revived
    /// via [`Queue::requeue_dead_job`](crate::Queue::requeue_dead_job).
    Dead,
}
