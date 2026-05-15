use std::future::Future;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::context::JobContext;
use crate::error::Result;

/// A unit of durable background work.
///
/// Implement this trait to define a job type: the struct fields are the
/// typed input, [`Output`](Self::Output) is the typed result, and
/// [`run`](Self::run) is the work. The remaining methods have defaults that
/// work without configuration; override them to customize idempotency, retry
/// limits, and error classification.
///
/// A `Job` must round-trip through [`serde`]: the runner serializes the
/// input to enqueue it and the output to persist it. It must also be
/// `Send + Sync + 'static` so the runner can dispatch it across worker tasks.
///
/// # Example
///
/// ```
/// use serde::{Serialize, Deserialize};
/// use taquba_jobs::{ErrorKind, Job, JobContext};
///
/// #[derive(Serialize, Deserialize)]
/// struct ResizeImage {
///     bucket: String,
///     key: String,
/// }
///
/// #[derive(Debug, thiserror::Error)]
/// #[error("resize failed: {0}")]
/// struct ResizeError(String);
///
/// impl Job for ResizeImage {
///     const NAME: &'static str = "media.resize-image";
///     type Output = u64; // bytes written
///     type Error = ResizeError;
///
///     async fn run(&self, _ctx: JobContext<'_>) -> Result<u64, ResizeError> {
///         // ... do the work ...
///         Ok(4096)
///     }
///
///     fn idempotency_key(&self) -> Option<String> {
///         Some(format!("resize:{}:{}", self.bucket, self.key))
///     }
///
///     fn classify(&self, _err: &ResizeError) -> ErrorKind {
///         ErrorKind::Transient
///     }
/// }
/// ```
pub trait Job: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// A stable, unique type tag for this job.
    ///
    /// Stored in a reserved header on every enqueued job so the runner can
    /// dispatch the opaque payload back to the right handler. Must be unique
    /// across all job types registered on a single [`JobRunner`](crate::JobRunner),
    /// and stable across releases; changing it strands in-flight jobs of the
    /// old name in the dead-letter queue.
    const NAME: &'static str;

    /// The typed value produced by a successful run. Persisted to object
    /// storage so it can be retrieved via [`JobHandle`](crate::JobHandle).
    type Output: Serialize + DeserializeOwned + Send + 'static;

    /// The error type [`run`](Self::run) returns on failure.
    ///
    /// The error's [`Display`](std::fmt::Display) output is recorded as the
    /// job's failure message; the error value itself is *not* persisted, so
    /// callers awaiting the job see a [`JobError`](crate::JobError) carrying
    /// the message and classification rather than this concrete type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Execute the job.
    ///
    /// Return `Ok` to complete the job (its output is persisted and the queue
    /// job is acked). Return `Err` to fail it; the error is passed to
    /// [`classify`](Self::classify) to decide whether the job retries or is
    /// dead-lettered.
    ///
    /// Handlers must be idempotent: taquba delivers at-least-once, so a job
    /// may run more than once if a lease expires before it finishes.
    fn run(
        &self,
        ctx: JobContext<'_>,
    ) -> impl Future<Output = std::result::Result<Self::Output, Self::Error>> + Send;

    /// An optional idempotency key for this job instance.
    ///
    /// When `Some`, a submission whose key matches an already-pending or
    /// scheduled job is collapsed onto that existing job rather than creating
    /// a new one; the returned [`JobHandle`](crate::JobHandle) points at the
    /// original. The default is `None`: every submission runs.
    ///
    /// To opt in to hash-based deduplication, return
    /// [`payload_idempotency_key`] for this job.
    fn idempotency_key(&self) -> Option<String> {
        None
    }

    /// Override the queue's default maximum delivery attempts for this job.
    ///
    /// `None` (the default) inherits the queue's configured `max_attempts`.
    fn max_attempts(&self) -> Option<u32> {
        None
    }

    /// Classify a failure from [`run`](Self::run) as transient or permanent.
    ///
    /// The default treats every error as [`ErrorKind::Transient`], so jobs
    /// retry up to their attempt limit before being dead-lettered. Override
    /// to send known-unrecoverable failures (validation errors, auth
    /// failures) straight to the dead-letter queue.
    fn classify(&self, _error: &Self::Error) -> ErrorKind {
        ErrorKind::Transient
    }
}

/// Whether a failed job should be retried or dead-lettered immediately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// The failure may succeed on retry (network blip, rate limit, transient
    /// downstream outage). The job is re-queued with backoff until its
    /// attempt limit is reached, then dead-lettered.
    Transient,
    /// The failure will not succeed on retry (validation error, auth failure,
    /// malformed input). The job is dead-lettered immediately.
    Permanent,
}

/// Derive a stable idempotency key by hashing a job's serialized form.
///
/// A convenience for jobs that want hash-based deduplication without
/// hand-writing a key: return this from
/// [`Job::idempotency_key`]. Two submissions of an identical job value
/// collapse onto a single execution.
///
/// This is opt-in by design: collapsing identical submissions silently
/// discards intentional duplicate work, so it is never the default.
///
/// ```
/// # use serde::{Serialize, Deserialize};
/// # use taquba_jobs::{Job, JobContext, payload_idempotency_key};
/// # #[derive(Serialize, Deserialize)]
/// # struct SendDigest { user_id: u64 }
/// # #[derive(Debug, thiserror::Error)]
/// # #[error("err")]
/// # struct E;
/// impl Job for SendDigest {
///     const NAME: &'static str = "email.send-digest";
///     type Output = ();
///     type Error = E;
///     async fn run(&self, _ctx: JobContext<'_>) -> Result<(), E> { Ok(()) }
///     fn idempotency_key(&self) -> Option<String> {
///         payload_idempotency_key(self).ok()
///     }
/// }
/// ```
pub fn payload_idempotency_key<J: Job>(job: &J) -> Result<String> {
    use sha2::{Digest, Sha256};

    let bytes = rmp_serde::to_vec_named(job)?;
    let digest = Sha256::digest(&bytes);
    let mut hex = String::with_capacity(2 + J::NAME.len() + 64);
    hex.push_str(J::NAME);
    hex.push(':');
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    Ok(hex)
}
