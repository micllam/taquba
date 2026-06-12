use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, warn};

use crate::error::{Error, Result};
use crate::job::JobRecord;
use crate::queue::{AckEffects, Queue};

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
/// Implement exactly one of [`Worker::process`] and
/// [`Worker::process_with_effects`]: each default delegates to the
/// other, so a type implementing neither fails to compile at its
/// first use (the two default futures embed each other, which rustc
/// rejects as a layout cycle).
pub trait Worker: Send + Sync {
    /// Process a single claimed job.
    ///
    /// Return `Ok(())` to ack the job (mark it complete) or `Err(_)` to nack
    /// it (re-queue with backoff, or dead-letter once `attempts` exceeds
    /// `max_attempts`). The returned error is converted to a string via
    /// `Display` and stored on the job's `last_error` field.
    ///
    /// Processing is called sequentially for each job in [`run_worker`], or
    /// concurrently up to the configured limit in [`run_worker_concurrent`].
    /// Implementations must be idempotent: Taquba guarantees at-least-once
    /// delivery, not exactly-once.
    fn process(
        &self,
        job: &JobRecord,
    ) -> impl Future<Output = std::result::Result<(), WorkerError>> + Send {
        async move { self.process_with_effects(job).await.map(|_| ()) }
    }

    /// Process a single claimed job and return effects to apply
    /// atomically with its acknowledgement.
    ///
    /// Like [`Self::process`], but an `Ok` return carries
    /// [`AckEffects`] that the worker loop passes to
    /// [`crate::Queue::ack_with`], so follow-up enqueues and caller KV
    /// changes land in the same transaction as the ack. Errors behave
    /// exactly as in [`Self::process`].
    fn process_with_effects(
        &self,
        job: &JobRecord,
    ) -> impl Future<Output = std::result::Result<AckEffects, WorkerError>> + Send {
        async move { self.process(job).await.map(|()| AckEffects::default()) }
    }
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
///
/// Errors from the claim path terminate the loop and propagate.
/// Settlement failures do not: they affect one job, not the loop. In
/// particular, when a job outlives its lease and the reaper requeues it,
/// the late settlement fails with [`Error::ClaimLost`]; the loop logs it
/// and continues, and the redelivered attempt settles the job instead.
/// Size leases to cover processing time, or extend them from within
/// `process` via [`Queue::renew_lease`], so this stays rare.
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
                process_and_settle(queue_handle, queue, worker, job).await;
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
/// so up to `concurrency` jobs run in parallel. Jobs are claimed in batches
/// sized to the free capacity via [`Queue::claim_batch`], so a backlog costs
/// one claim transaction per batch instead of per job; each job is still
/// processed and acked individually. On shutdown the loop stops claiming new
/// work and waits for the in-flight set to drain before returning.
///
/// Claim errors propagate and terminate the loop. Settlement failures and
/// panics inside spawned tasks are logged but do not terminate the loop;
/// see [`run_worker`] for the [`Error::ClaimLost`] case.
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

        // Claim up to the free capacity in one transaction. If the queue
        // is non-empty, spawn each claimed job and loop. If empty, wait
        // for new work or shutdown.
        let free = concurrency - set.len();
        let lease = queue_handle.queue_config(queue).lease_duration;
        let jobs = queue_handle.claim_batch(queue, free, lease).await?;
        if jobs.is_empty() {
            tokio::select! {
                biased;
                _ = &mut shutdown => break 'main,
                _ = queue_handle.wait_for_jobs_on(queue, poll_interval) => {}
            }
            continue;
        }
        for job in jobs {
            let q = queue_handle.clone();
            let w = worker.clone();
            let queue_owned = queue.to_string();
            set.spawn(async move {
                process_and_settle(&q, &queue_owned, w.as_ref(), job).await;
            });
        }
        if check_shutdown(shutdown.as_mut()) {
            break 'main;
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

/// Process one claimed job and apply its settlement (ack with effects,
/// nack, or dead-letter). Settlement failures are absorbed: they affect
/// one job, not the loop. A [`Error::ClaimLost`] means the job outlived
/// its lease and the reaper requeued it, so the redelivered attempt
/// settles it instead; any other settlement failure leaves the claim to
/// the reaper.
async fn process_and_settle<W: Worker>(
    queue_handle: &Queue,
    queue: &str,
    worker: &W,
    job: JobRecord,
) {
    let job_id = job.id.clone();
    let settlement = match worker.process_with_effects(&job).await {
        Ok(effects) => queue_handle.ack_with(&job, effects).await.map(|_| ()),
        Err(e) if e.downcast_ref::<PermanentFailure>().is_some() => {
            queue_handle.dead_letter(job, &e.to_string()).await
        }
        Err(e) => queue_handle.nack(job, &e.to_string()).await,
    };
    match settlement {
        Ok(()) => {}
        Err(Error::ClaimLost) => warn!(
            queue = queue,
            job_id = %job_id,
            "job lost its claim during processing; the redelivered attempt settles it"
        ),
        Err(e) => warn!(queue = queue, job_id = %job_id, "settlement failed: {e}"),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;
    use crate::queue::{OpenOptions, Queue, QueueConfig};
    use slatedb::object_store::memory::InMemory;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingWorker {
        processed: Arc<AtomicUsize>,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
    }

    impl Worker for CountingWorker {
        async fn process(&self, _job: &JobRecord) -> std::result::Result<(), WorkerError> {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            self.processed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct ChainingWorker {
        processed: Arc<AtomicUsize>,
    }

    impl Worker for ChainingWorker {
        async fn process_with_effects(
            &self,
            job: &JobRecord,
        ) -> std::result::Result<AckEffects, WorkerError> {
            self.processed.fetch_add(1, Ordering::SeqCst);
            if job.payload == b"first" {
                Ok(AckEffects {
                    enqueues: vec![crate::queue::EnqueueRequest {
                        queue: job.queue.clone(),
                        payload: b"second".to_vec(),
                        options: crate::queue::EnqueueOptions::default(),
                    }],
                    ..AckEffects::default()
                })
            } else {
                Ok(AckEffects::default())
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn worker_effects_chain_a_follow_up_job() {
        let queue = Queue::open(Arc::new(InMemory::new()), "test")
            .await
            .unwrap();
        queue.enqueue("work", b"first".to_vec()).await.unwrap();

        let processed = Arc::new(AtomicUsize::new(0));
        let worker = ChainingWorker {
            processed: processed.clone(),
        };
        let all_processed = {
            let processed = processed.clone();
            async move {
                while processed.load(Ordering::SeqCst) < 2 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        };
        run_worker(
            &queue,
            "work",
            &worker,
            Duration::from_millis(50),
            all_processed,
        )
        .await
        .unwrap();

        assert_eq!(processed.load(Ordering::SeqCst), 2);
        let stats = queue.stats("work").await.unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.done, 2);
        queue.close().await.unwrap();
    }

    struct LeaseLosingWorker {
        queue: Arc<Queue>,
        clock: MockClock,
        lease: Duration,
        calls: Arc<AtomicUsize>,
    }

    impl Worker for LeaseLosingWorker {
        async fn process(&self, _job: &JobRecord) -> std::result::Result<(), WorkerError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                // First attempt: outlive the lease and let the reaper
                // requeue the job, so the loop's ack finds the claim
                // gone and fails with ClaimLost.
                self.clock.advance(self.lease + Duration::from_millis(1));
                self.queue.reap_now().await?;
            }
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn worker_loop_survives_settlement_on_a_lost_claim() {
        let clock = MockClock::new(1_700_000_000_000);
        let lease = Duration::from_secs(30);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            default_queue_config: QueueConfig {
                lease_duration: lease,
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let queue = Arc::new(
            Queue::open_with_options(Arc::new(InMemory::new()), "test", opts)
                .await
                .unwrap(),
        );
        queue.enqueue("work", b"job".to_vec()).await.unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let worker = LeaseLosingWorker {
            queue: queue.clone(),
            clock,
            lease,
            calls: calls.clone(),
        };
        let second_attempt_done = {
            let calls = calls.clone();
            async move {
                while calls.load(Ordering::SeqCst) < 2 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        };
        run_worker(
            &queue,
            "work",
            &worker,
            Duration::from_millis(50),
            second_attempt_done,
        )
        .await
        .unwrap();

        // The first attempt's settlement lost the claim; the loop kept
        // running and the redelivered attempt settled the job.
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let stats = queue.stats("work").await.unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.claimed, 0);
        assert_eq!(stats.done, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_worker_fills_capacity_from_a_backlog() {
        let queue = Arc::new(
            Queue::open(Arc::new(InMemory::new()), "test")
                .await
                .unwrap(),
        );
        queue
            .enqueue_batch("work", vec![vec![0u8; 8]; 10])
            .await
            .unwrap();

        let processed = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let worker = Arc::new(CountingWorker {
            processed: processed.clone(),
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_in_flight: max_in_flight.clone(),
        });

        let all_processed = {
            let processed = processed.clone();
            async move {
                while processed.load(Ordering::SeqCst) < 10 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        };
        run_worker_concurrent(
            &queue,
            "work",
            worker,
            4,
            Duration::from_millis(50),
            all_processed,
        )
        .await
        .unwrap();

        assert_eq!(processed.load(Ordering::SeqCst), 10);
        assert_eq!(
            max_in_flight.load(Ordering::SeqCst),
            4,
            "a batch claim fills the free capacity without exceeding it",
        );
    }
}
