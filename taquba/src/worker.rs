use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, warn};

use crate::error::{Error, Result};
use crate::job::{Claim, JobRecord};
use crate::lease::LeaseHandle;
use crate::queue::{Queue, SettlementEffects};

/// Boxed error type returned from [`Worker::process`].
pub type WorkerError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Marker error: returning this from [`Worker::process`] dead-letters the job
/// immediately rather than retrying. The runner records the error's `Display`
/// output in the job's `last_error` field.
///
/// Use when the failure is *known* not to recover on retry.
///
/// ```rust,ignore
/// async fn process(&self, job: &JobRecord, _lease: &LeaseHandle) -> Result<(), WorkerError> {
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

/// Wrapper error attaching [`SettlementEffects`] to a failure returned from
/// [`Worker::process`] or [`Worker::process_with_effects`].
///
/// The worker loop settles the wrapped error exactly as it would settle
/// the error unwrapped, and applies `effects` atomically with the
/// settlement when that settlement dead-letters the job: a wrapped
/// [`PermanentFailure`] dead-letters through
/// [`crate::Queue::dead_letter_with`], and any other wrapped error is
/// reported through [`crate::Queue::nack_with`], whose effects apply
/// only once the job's attempts are exhausted. Effects on a retried
/// failure are discarded; attach them on every attempt.
///
/// ```rust,ignore
/// Err(FailWith::new(PermanentFailure::new("bad input"), effects).into())
/// ```
#[derive(Debug)]
pub struct FailWith {
    /// The failure itself. Settlement routing (dead-letter or retry)
    /// follows this error.
    pub error: WorkerError,
    /// Effects applied atomically with a dead-lettering settlement of
    /// this failure.
    pub effects: SettlementEffects,
}

impl FailWith {
    /// Attach `effects` to `error`.
    pub fn new(error: impl Into<WorkerError>, effects: SettlementEffects) -> Self {
        Self {
            error: error.into(),
            effects,
        }
    }
}

impl std::fmt::Display for FailWith {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for FailWith {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error.as_ref())
    }
}

/// Implement this trait to define how a job is processed.
///
/// # Example
///
/// ```rust,ignore
/// struct EmailWorker;
///
/// impl taquba::Worker for EmailWorker {
///     async fn process(
///         &self,
///         job: &taquba::JobRecord,
///         _lease: &taquba::LeaseHandle,
///     ) -> Result<(), taquba::WorkerError> {
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
    /// `lease` extends the claim's lease for long-running work; see
    /// [`LeaseHandle::ensure_at_least`]. The claim itself stays with
    /// the worker loop, which settles the job when this returns, so an
    /// implementation cannot settle its own job mid-execution.
    ///
    /// Processing is called sequentially for each job in [`run_worker`], or
    /// concurrently up to the configured limit in [`run_worker_concurrent`].
    /// Implementations must be idempotent: Taquba guarantees at-least-once
    /// delivery, not exactly-once.
    fn process(
        &self,
        job: &JobRecord,
        lease: &LeaseHandle,
    ) -> impl Future<Output = std::result::Result<(), WorkerError>> + Send {
        async move { self.process_with_effects(job, lease).await.map(|_| ()) }
    }

    /// Process a single claimed job and return effects to apply
    /// atomically with its acknowledgement.
    ///
    /// Like [`Self::process`], but an `Ok` return supplies
    /// [`SettlementEffects`] that the worker loop passes to
    /// [`crate::Queue::ack_with`], so follow-up enqueues and caller KV
    /// changes land in the same transaction as the ack. Errors behave
    /// exactly as in [`Self::process`].
    fn process_with_effects(
        &self,
        job: &JobRecord,
        lease: &LeaseHandle,
    ) -> impl Future<Output = std::result::Result<SettlementEffects, WorkerError>> + Send {
        async move {
            self.process(job, lease)
                .await
                .map(|()| SettlementEffects::default())
        }
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
/// every graceful restart). The drain has no internal bound; the bound is
/// the process supervisor's kill timeout. A killed process is recovered at the
/// next open, where every claimed job is requeued immediately and its
/// next claim consumes one attempt.
///
/// `poll_interval` is the maximum time the loop will wait on an empty queue
/// before re-checking. In-process inserts wake the loop immediately via the
/// queue-scoped notify (one waiting worker per inserted job), so this only
/// bounds the latency of out-of-band events (e.g. a scheduled job becoming
/// due).
///
/// Errors from the claim path terminate the loop and propagate.
/// Settlement failures do not: they affect only the one job. In
/// particular, when a job outlives its lease and the reaper requeues it,
/// the late settlement fails with [`Error::ClaimLost`]; the loop logs it
/// and continues, and the redelivered attempt settles the job instead.
/// Size the queue's lease to cover processing time so late settlements
/// are rare, or have a long-running [`Worker::process`] extend it
/// through the [`LeaseHandle`] it was given: renewal advances the lease
/// and leaves the claim valid for the settlement the loop performs
/// afterwards.
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
/// work and waits for the in-flight set to drain before returning, without
/// an internal bound; see [`run_worker`] on recovery from a supervisor kill.
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
            note_task_result(queue, result);
        }

        // If at capacity, wait for one slot to free up. Shutdown can interrupt
        // this wait; any spawned tasks already running will be drained at the
        // bottom of the loop.
        if set.len() >= concurrency {
            tokio::select! {
                biased;
                _ = &mut shutdown => break 'main,
                r = set.join_next() => {
                    if let Some(result) = r {
                        note_task_result(queue, result);
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
        note_task_result(queue, result);
    }
    Ok(())
}

/// Log a spawned worker task that ended by panic. A settlement failure
/// is logged by the task itself.
fn note_task_result(queue: &str, result: std::result::Result<(), tokio::task::JoinError>) {
    if let Err(e) = result {
        warn!(queue = queue, "worker task panicked: {e}");
    }
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
    claim: Claim,
) {
    let job_id = claim.id.clone();
    let lease = queue_handle.lease_handle(&claim);
    let settlement = match worker.process_with_effects(claim.job(), &lease).await {
        Ok(effects) => queue_handle.ack_with(&claim, effects).await.map(|_| ()),
        Err(e) => {
            let (error, effects) = match e.downcast::<FailWith>() {
                Ok(wrapped) => (wrapped.error, wrapped.effects),
                Err(e) => (e, SettlementEffects::default()),
            };
            if error.downcast_ref::<PermanentFailure>().is_some() {
                queue_handle
                    .dead_letter_with(&claim, &error.to_string(), effects)
                    .await
                    .map(|_| ())
            } else {
                queue_handle
                    .nack_with(&claim, &error.to_string(), effects)
                    .await
                    .map(|_| ())
            }
        }
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
        async fn process(
            &self,
            _job: &JobRecord,
            _lease: &LeaseHandle,
        ) -> std::result::Result<(), WorkerError> {
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
            _lease: &LeaseHandle,
        ) -> std::result::Result<SettlementEffects, WorkerError> {
            self.processed.fetch_add(1, Ordering::SeqCst);
            if job.payload == b"first" {
                Ok(SettlementEffects {
                    enqueues: vec![crate::queue::EnqueueRequest {
                        queue: job.queue.clone(),
                        payload: b"second".to_vec(),
                        options: crate::queue::EnqueueOptions::default(),
                    }],
                    ..SettlementEffects::default()
                })
            } else {
                Ok(SettlementEffects::default())
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
        async fn process(
            &self,
            _job: &JobRecord,
            _lease: &LeaseHandle,
        ) -> std::result::Result<(), WorkerError> {
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

    struct EffectfulFailureWorker {
        permanent: bool,
        attempts_seen: Arc<AtomicUsize>,
    }

    impl Worker for EffectfulFailureWorker {
        async fn process(
            &self,
            job: &JobRecord,
            _lease: &LeaseHandle,
        ) -> std::result::Result<(), WorkerError> {
            self.attempts_seen.fetch_add(1, Ordering::SeqCst);
            let effects = SettlementEffects {
                enqueues: vec![crate::queue::EnqueueRequest {
                    queue: "notify".to_string(),
                    payload: job.payload.clone(),
                    options: crate::queue::EnqueueOptions::default(),
                }],
                ..SettlementEffects::default()
            };
            let error: WorkerError = if self.permanent {
                Box::new(PermanentFailure::new("permanent failure"))
            } else {
                "transient failure".into()
            };
            Err(Box::new(FailWith::new(error, effects)))
        }
    }

    async fn run_one_failing_attempt(queue: &Queue, worker: &EffectfulFailureWorker) {
        let one_attempt_done = {
            let seen = worker.attempts_seen.clone();
            async move {
                while seen.load(Ordering::SeqCst) < 1 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        };
        run_worker(
            queue,
            "work",
            worker,
            Duration::from_millis(50),
            one_attempt_done,
        )
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_wrapped_permanent_failure_dead_letters_with_its_effects() {
        let queue = Queue::open(Arc::new(InMemory::new()), "test")
            .await
            .unwrap();
        queue.enqueue("work", b"job".to_vec()).await.unwrap();

        let worker = EffectfulFailureWorker {
            permanent: true,
            attempts_seen: Arc::new(AtomicUsize::new(0)),
        };
        run_one_failing_attempt(&queue, &worker).await;

        assert_eq!(queue.stats("work").await.unwrap().dead, 1);
        assert_eq!(queue.stats("notify").await.unwrap().pending, 1);
        queue.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_wrapped_permanent_failure_settles_as_the_unwrapped_error_does() {
        let queue = Queue::open(Arc::new(InMemory::new()), "test")
            .await
            .unwrap();
        let id = queue.enqueue("work", b"job".to_vec()).await.unwrap();

        let worker = EffectfulFailureWorker {
            permanent: true,
            attempts_seen: Arc::new(AtomicUsize::new(0)),
        };
        run_one_failing_attempt(&queue, &worker).await;

        let dead = queue.get_job(&id).await.unwrap().unwrap();
        assert_eq!(dead.attempts, 1, "a wrapped failure consumes one attempt");
        assert_eq!(dead.last_error.as_deref(), Some("permanent failure"));
        queue.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_wrapped_transient_failure_retries_without_its_effects() {
        let queue = Queue::open(Arc::new(InMemory::new()), "test")
            .await
            .unwrap();
        queue.enqueue("work", b"job".to_vec()).await.unwrap();

        let worker = EffectfulFailureWorker {
            permanent: false,
            attempts_seen: Arc::new(AtomicUsize::new(0)),
        };
        run_one_failing_attempt(&queue, &worker).await;

        let stats = queue.stats("work").await.unwrap();
        assert_eq!(stats.dead, 0);
        assert_eq!(stats.scheduled + stats.pending, 1);
        assert_eq!(queue.stats("notify").await.unwrap().pending, 0);
        queue.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_wrapped_transient_failure_on_the_final_attempt_applies_its_effects() {
        let queue = Queue::open(Arc::new(InMemory::new()), "test")
            .await
            .unwrap();
        queue
            .enqueue_with(
                "work",
                b"job".to_vec(),
                crate::queue::EnqueueOptions {
                    max_attempts: Some(1),
                    ..crate::queue::EnqueueOptions::default()
                },
            )
            .await
            .unwrap();

        let worker = EffectfulFailureWorker {
            permanent: false,
            attempts_seen: Arc::new(AtomicUsize::new(0)),
        };
        run_one_failing_attempt(&queue, &worker).await;

        assert_eq!(queue.stats("work").await.unwrap().dead, 1);
        assert_eq!(queue.stats("notify").await.unwrap().pending, 1);
        queue.close().await.unwrap();
    }
}
