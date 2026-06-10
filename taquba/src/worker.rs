use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, warn};

use crate::error::Result;
use crate::job::JobRecord;
use crate::queue::Queue;

/// Boxed error type returned from [`Worker::process`].
pub type WorkerError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Marker error: returning this from [`Worker::process`] dead-letters the job
/// immediately rather than retrying. The runner records the error's `Display`
/// output in the job's `last_error` field.
///
/// Use when the failure is *known* not to recover on retry.
///
/// ```rust,ignore
/// async fn process(&self, job: &JobRecord) -> Result<(), WorkerError> {
///     match http.send(...).await {
///         Ok(_) => Ok(()),
///         Err(e) if e.is_4xx() => Err(PermanentFailure::new(e.to_string()).into()),
///         Err(e) => Err(e.into()),
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct PermanentFailure {
    /// Human-readable reason; recorded on the job's `last_error` field.
    pub reason: String,
}

impl PermanentFailure {
    /// Build a [`PermanentFailure`] with a human-readable reason.
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for PermanentFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for PermanentFailure {}

/// Implement this trait to define how a job is processed.
///
/// # Example
///
/// ```rust,ignore
/// struct EmailWorker;
///
/// impl taquba::Worker for EmailWorker {
///     async fn process(&self, job: &taquba::JobRecord) -> Result<(), taquba::WorkerError> {
///         let to = std::str::from_utf8(&job.payload)?;
///         send_email(to).await?;
///         Ok(())
///     }
/// }
/// ```
pub trait Worker: Send + Sync {
    /// Process a single claimed job.
    ///
    /// Return `Ok(())` to ack the job (mark it complete) or `Err(_)` to nack
    /// it (re-queue with backoff, or dead-letter once `attempts` exceeds
    /// `max_attempts`). The returned error is converted to a string via
    /// `Display` and stored on the job's `last_error` field.
    ///
    /// `process` is called sequentially for each job in [`run_worker`], or
    /// concurrently up to the configured limit in [`run_worker_concurrent`].
    /// Implementations must be idempotent: Taquba guarantees at-least-once
    /// delivery, not exactly-once.
    fn process(
        &self,
        job: &JobRecord,
    ) -> impl Future<Output = std::result::Result<(), WorkerError>> + Send;
}

/// Run a polling worker loop: claim the next job, call [`Worker::process`],
/// then ack on success or nack on failure.
///
/// `shutdown` is any future that resolves when the worker should stop. Common
/// choices:
/// - `tokio::signal::ctrl_c()` - exit on Ctrl-C
/// - `async move { rx.await.ok(); }` - exit when a oneshot fires
/// - `std::future::pending::<()>()` - never exit
///
/// Shutdown is only honoured at safe points: between jobs and while the queue
/// is idle. An in-flight `process` call is always allowed to finish so the
/// claim does not get abandoned to the reaper (which would waste a retry on
/// every graceful restart).
///
/// `poll_interval` is the maximum time the loop will wait on an empty queue
/// before re-checking. In-process inserts wake the loop immediately via the
/// queue-scoped notify (one waiting worker per inserted job), so this only
/// bounds the latency of out-of-band events (e.g. a scheduled job becoming
/// due).
pub async fn run_worker<W, F>(
    queue_handle: &Queue,
    queue: &str,
    worker: &W,
    poll_interval: Duration,
    shutdown: F,
) -> Result<()>
where
    W: Worker,
    F: Future<Output = ()>,
{
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        match queue_handle.claim_next(queue).await? {
            Some(job) => {
                // Process is uncancellable: no select around it. Even if
                // shutdown was signalled while we were claiming, we finish
                // the job we just took the lease on.
                match worker.process(&job).await {
                    Ok(()) => queue_handle.ack(&job).await?,
                    Err(e) if e.downcast_ref::<PermanentFailure>().is_some() => {
                        queue_handle.dead_letter(job, &e.to_string()).await?
                    }
                    Err(e) => queue_handle.nack(job, &e.to_string()).await?,
                }
                if check_shutdown(shutdown.as_mut()) {
                    debug!(queue = queue, "worker shutdown requested");
                    return Ok(());
                }
            }
            None => {
                // Empty queue: wait for new work, the poll timeout, or
                // shutdown. This is the only point where shutdown can
                // interrupt the loop.
                tokio::select! {
                    biased;
                    _ = &mut shutdown => {
                        debug!(queue = queue, "worker shutdown requested");
                        return Ok(());
                    }
                    _ = queue_handle.wait_for_jobs_on(queue, poll_interval) => {}
                }
            }
        }
    }
}

/// Run a concurrent polling worker loop that processes up to `concurrency` jobs
/// simultaneously.
///
/// Behaves like [`run_worker`] but spawns each job onto a [`tokio::task::JoinSet`]
/// so up to `concurrency` jobs run in parallel. On shutdown the loop stops
/// claiming new work and waits for the in-flight set to drain before returning.
///
/// Claim errors propagate and terminate the loop. Ack / nack errors and panics
/// inside spawned tasks are logged but do not terminate the loop.
pub async fn run_worker_concurrent<W, F>(
    queue_handle: &Arc<Queue>,
    queue: &str,
    worker: Arc<W>,
    concurrency: usize,
    poll_interval: Duration,
    shutdown: F,
) -> Result<()>
where
    W: Worker + 'static,
    F: Future<Output = ()>,
{
    assert!(concurrency > 0, "concurrency must be at least 1");
    let mut set = tokio::task::JoinSet::new();
    let mut shutdown = std::pin::pin!(shutdown);

    'main: loop {
        // Reap completed tasks (non-blocking) and log any panics.
        while let Some(result) = set.try_join_next() {
            if let Err(e) = result {
                warn!(queue = queue, "worker task panicked: {e}");
            }
        }

        // If at capacity, wait for one slot to free up. Shutdown can interrupt
        // this wait; any spawned tasks already running will be drained at the
        // bottom of the loop.
        if set.len() >= concurrency {
            tokio::select! {
                biased;
                _ = &mut shutdown => break 'main,
                r = set.join_next() => {
                    if let Some(Err(e)) = r {
                        warn!(queue = queue, "worker task panicked: {e}");
                    }
                }
            }
            continue;
        }

        // Try a non-blocking claim. If the queue is non-empty, spawn the job
        // and loop. If empty, wait for new work or shutdown.
        match queue_handle.claim_next(queue).await? {
            Some(job) => {
                let q = queue_handle.clone();
                let w = worker.clone();
                let queue_owned = queue.to_string();
                set.spawn(async move {
                    match w.process(&job).await {
                        Ok(()) => {
                            if let Err(e) = q.ack(&job).await {
                                warn!(queue = %queue_owned, job_id = %job.id, "ack failed: {e}");
                            }
                        }
                        Err(e) if e.downcast_ref::<PermanentFailure>().is_some() => {
                            if let Err(se) = q.dead_letter(job, &e.to_string()).await {
                                warn!(queue = %queue_owned, "dead_letter failed: {se}");
                            }
                        }
                        Err(e) => {
                            if let Err(se) = q.nack(job, &e.to_string()).await {
                                warn!(queue = %queue_owned, "nack failed: {se}");
                            }
                        }
                    }
                });
                if check_shutdown(shutdown.as_mut()) {
                    break 'main;
                }
            }
            None => {
                tokio::select! {
                    biased;
                    _ = &mut shutdown => break 'main,
                    _ = queue_handle.wait_for_jobs_on(queue, poll_interval) => {}
                }
            }
        }
    }

    debug!(
        queue = queue,
        in_flight = set.len(),
        "draining workers on shutdown"
    );
    while let Some(result) = set.join_next().await {
        if let Err(e) = result {
            warn!(queue = queue, "worker task panicked during drain: {e}");
        }
    }
    Ok(())
}

/// Non-blocking peek at a pinned shutdown future. Returns true if the future
/// has already resolved, false otherwise. Used to honour shutdown between jobs
/// without putting `process` inside a `select!` (which would cancel it if the
/// shutdown signal landed while a claim was in flight).
fn check_shutdown<F: Future<Output = ()>>(shutdown: std::pin::Pin<&mut F>) -> bool {
    use std::task::{Context, Poll};
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);
    matches!(shutdown.poll(&mut cx), Poll::Ready(()))
}
