use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use taquba::object_store::ObjectStore;
use taquba::{
    Clock, EnqueueOptions, EnqueueResult, JobRecord, PermanentFailure, Queue, Worker, WorkerError,
    run_worker_concurrent,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::context::{JobContext, State};
use crate::error::{Error, Result};
use crate::handle::JobHandle;
use crate::job::{ErrorKind, Job};
use crate::result_store::{ResultStore, StoredOutcome};

/// Reserved header key carrying a job's [`Job::NAME`] so the dispatch worker
/// can route an opaque payload back to the right handler.
pub(crate) const JOB_TYPE_HEADER: &str = "taquba_jobs.type";

const DEFAULT_QUEUE_NAME: &str = "jobs";
const DEFAULT_CONCURRENCY: usize = 16;
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Prefix for the durable per-submission dedup record in Taquba's user
/// KV namespace. The full stored key is
/// `{JOBS_KV_PREFIX}{idempotency_key}`.
const JOBS_KV_PREFIX: &[u8] = b"jobs/dedup/";

fn dedup_kv_key(idempotency_key: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(JOBS_KV_PREFIX.len() + idempotency_key.len());
    k.extend_from_slice(JOBS_KV_PREFIX);
    k.extend_from_slice(idempotency_key.as_bytes());
    k
}

/// Persisted alongside a submission via [`Queue::enqueue_with_kv`] so a
/// later submission with the same `idempotency_key` can both detect a
/// payload change *and* short-circuit to a cached result once the
/// original job has completed.
///
/// - `input_hash`: SHA-256 of the serialized payload. Lets a
///   re-submission catch the "same key, different payload" case (see
///   [`Error::InputMismatch`]).
/// - `job_id`: the id assigned to the submitted job. Lets a
///   re-submission look up the result blob even after the queue's
///   dedup window has closed (i.e. after the job acked).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct JobSubmissionRecord {
    input_hash: [u8; 32],
    job_id: String,
}

fn hash_payload(payload: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(payload);
    hasher.finalize().into()
}

/// The shared, cheaply-cloneable core that knows how to enqueue a job and
/// where its results live. Held by the runner, every [`JobHandle`], and
/// (by reference) every [`JobContext`].
///
/// [`JobHandle`]: crate::JobHandle
#[derive(Clone)]
pub(crate) struct Submitter {
    queue: Arc<Queue>,
    queue_name: Arc<str>,
    results: ResultStore,
    state: Arc<State>,
    clock: Arc<dyn Clock>,
    /// Window after a job reaches a terminal state during which its
    /// outcome blob is retained. `None` disables retention entirely (no
    /// terminal marker is written and no sweeper runs).
    result_retention: Option<Duration>,
}

impl Submitter {
    pub(crate) fn queue(&self) -> &Arc<Queue> {
        &self.queue
    }

    pub(crate) fn queue_name(&self) -> &str {
        &self.queue_name
    }

    pub(crate) fn results(&self) -> &ResultStore {
        &self.results
    }

    pub(crate) fn state(&self) -> &State {
        &self.state
    }

    /// Record that `job_id` has reached a terminal state. When result
    /// retention is enabled, writes a terminal marker the sweeper will
    /// use to schedule the blob's deletion. A failure here is logged
    /// but not propagated: the job has already acked, and leaving the
    /// blob un-marked just means it gets retained instead of swept.
    async fn note_terminal(&self, job_id: &str) {
        if self.result_retention.is_none() {
            return;
        }
        if let Err(err) = self
            .results
            .write_terminal_marker(job_id, self.clock.now_ms())
            .await
        {
            tracing::warn!(
                job_id = %job_id,
                "failed to write terminal marker: {err}"
            );
        }
    }

    /// Result-retention sweep loop. Runs only when
    /// [`JobRunnerBuilder::result_retention`] was set; the first tick
    /// fires immediately so a fresh runner catches markers left behind
    /// by an earlier process, then ticks every `retention` until
    /// `shutdown` is cancelled.
    async fn run_sweep(&self, shutdown: CancellationToken) {
        let Some(retention) = self.result_retention else {
            return;
        };
        let mut ticker = tokio::time::interval(retention);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    if let Err(err) = self.sweep_expired_results(retention).await {
                        tracing::warn!("result retention sweep failed: {err}");
                    }
                }
            }
        }
    }

    /// One pass of result retention: list every terminal marker and,
    /// for each marker older than `retention`, delete the job's result
    /// blob and then the marker. Returns the number of blobs cleared.
    /// Errors on individual entries are logged and skipped so a
    /// transient failure on one marker doesn't stall the rest of the
    /// sweep.
    async fn sweep_expired_results(&self, retention: Duration) -> Result<usize> {
        let now_ms = self.clock.now_ms();
        let retention_ms = retention.as_millis() as u64;
        let cutoff = now_ms.saturating_sub(retention_ms);
        let markers = self.results.list_terminal_markers().await?;
        let mut cleared = 0usize;
        for marker in markers {
            if marker.terminal_at_ms >= cutoff {
                continue;
            }
            if let Err(err) = self.results.delete(&marker.job_id).await {
                tracing::warn!(
                    job_id = %marker.job_id,
                    "result delete failed during sweep: {err}",
                );
                continue;
            }
            if let Err(err) = self.results.delete_terminal_marker(&marker).await {
                tracing::warn!(
                    job_id = %marker.job_id,
                    "delete_terminal_marker failed during sweep: {err}",
                );
                // Result blob is gone but the marker remains; the next
                // pass will retry the marker delete (the result delete
                // is a no-op on the now-missing blob).
                continue;
            }
            cleared += 1;
        }
        Ok(cleared)
    }

    pub(crate) async fn submit<J: Job>(&self, job: J, opts: SubmitOptions) -> Result<JobHandle<J>> {
        let payload = rmp_serde::to_vec_named(&job)?;

        let mut headers = opts.headers;
        if headers.contains_key(JOB_TYPE_HEADER) {
            return Err(Error::ReservedHeader(JOB_TYPE_HEADER.to_string()));
        }
        headers.insert(JOB_TYPE_HEADER.to_string(), J::NAME.to_string());

        let dedup_key = job.idempotency_key();
        let enqueue_opts = EnqueueOptions {
            max_attempts: opts.max_attempts.or_else(|| job.max_attempts()),
            priority: opts.priority,
            run_at: opts.run_at,
            dedup_key: dedup_key.clone(),
            headers,
            ..EnqueueOptions::default()
        };

        let (id, newly_submitted) = match dedup_key {
            Some(idem_key) => {
                self.submit_idempotent(idem_key, payload, enqueue_opts)
                    .await?
            }
            None => {
                let id = self
                    .queue
                    .enqueue_with(&self.queue_name, payload, enqueue_opts)
                    .await?;
                (id, true)
            }
        };

        tracing::debug!(
            job_id = %id,
            job_type = J::NAME,
            newly_submitted,
            "job submitted",
        );
        Ok(JobHandle::new(id, self.clone(), newly_submitted))
    }

    /// Submit a job whose `idempotency_key` is known. Detects mismatched
    /// re-submissions (same key, different payload) via a SHA-256 hash
    /// of the payload persisted in the user KV namespace atomically
    /// with the enqueue, and short-circuits to a cached result when a
    /// prior submission with the same key has already completed.
    ///
    /// Returns `(job_id, newly_submitted)`: `newly_submitted` is `true`
    /// when this call enqueued a new job, and `false` when it either
    /// dedup-hit against a pending submission or short-circuited to a
    /// prior completed submission's persisted outcome. On a duplicate
    /// with a different payload (in-process or across restart) returns
    /// [`Error::InputMismatch`].
    async fn submit_idempotent(
        &self,
        idem_key: String,
        payload: Vec<u8>,
        enqueue_opts: EnqueueOptions,
    ) -> Result<(String, bool)> {
        let kv_key = dedup_kv_key(&idem_key);
        let input_hash = hash_payload(&payload);

        // Pre-check the submission record. Three outcomes:
        //   1. Hash mismatch -> InputMismatch, fast-fail.
        //   2. Hash match + record has job_id + result blob exists ->
        //      short-circuit to the cached outcome. Avoids creating
        //      a new job after the queue's dedup window has closed.
        //   3. Hash match but no cached result -> fall through to the
        //      normal enqueue path (dedups against any pending job).
        if let Some(bytes) = self.queue.kv_get(&kv_key).await? {
            let existing: JobSubmissionRecord = rmp_serde::from_slice(&bytes)?;
            if existing.input_hash != input_hash {
                return Err(Error::InputMismatch(idem_key));
            }
            if self.results.get(&existing.job_id).await?.is_some() {
                tracing::debug!(
                    idem_key = %idem_key,
                    job_id = %existing.job_id,
                    "idempotent submit short-circuited to cached result",
                );
                return Ok((existing.job_id, false));
            }
        }

        // Pre-allocate the id so the submission record can carry it
        // atomically with the enqueue. After the job acks (releasing
        // the queue dedup key), the recorded id remains the pointer
        // back to the result blob.
        let id = ulid::Ulid::new().to_string();
        let record_bytes = rmp_serde::to_vec_named(&JobSubmissionRecord {
            input_hash,
            job_id: id.clone(),
        })?;
        let kv_writes = HashMap::from([(kv_key.clone(), record_bytes)]);
        let enqueue_opts = EnqueueOptions {
            id_override: Some(id.clone()),
            ..enqueue_opts
        };

        let result = self
            .queue
            .enqueue_with_kv(&self.queue_name, payload, enqueue_opts, kv_writes)
            .await?;

        match result {
            EnqueueResult::New(returned) => {
                debug_assert_eq!(returned, id);
                Ok((returned, true))
            }
            EnqueueResult::AlreadyEnqueued(other_id) => {
                // A concurrent submit beat us between our pre-check and
                // the enqueue transaction. Our kv_writes were not
                // applied; re-read the KV record (the winner's) and
                // verify its hash matches ours; if not, the apparent
                // dedup-hit is actually a mismatch.
                if let Some(bytes) = self.queue.kv_get(&kv_key).await? {
                    let existing: JobSubmissionRecord = rmp_serde::from_slice(&bytes)?;
                    if existing.input_hash != input_hash {
                        return Err(Error::InputMismatch(idem_key));
                    }
                }
                Ok((other_id, false))
            }
        }
    }
}

/// Per-submission overrides for [`JobRunner::submit_with`].
///
/// Every field is optional; the defaults inherit the queue's configuration.
/// Construct via [`SubmitOptions::default`] and struct-update syntax so future
/// fields stay non-breaking.
#[derive(Debug, Clone, Default)]
pub struct SubmitOptions {
    /// Override the job type's and queue's `max_attempts` for just this
    /// submission. Takes precedence over [`Job::max_attempts`].
    pub max_attempts: Option<u32>,
    /// Override the queue's default priority. Lower numbers are claimed
    /// first; see [`taquba::PRIORITY_HIGH`] and the other priority constants.
    pub priority: Option<u32>,
    /// Delay the job until this time. The job waits in the scheduled key
    /// space until taquba's scheduler promotes it.
    pub run_at: Option<SystemTime>,
    /// Extra headers to attach to the job record. The runner adds its own
    /// reserved routing header on every submission; setting that key here
    /// fails the submission with [`Error::ReservedHeader`](crate::Error::ReservedHeader).
    pub headers: HashMap<String, String>,
}

type DispatchFuture<'a> =
    Pin<Box<dyn Future<Output = std::result::Result<(), WorkerError>> + Send + 'a>>;

/// Type-erased dispatch from a job-type header to a typed [`Job::run`].
trait ErasedHandler: Send + Sync {
    fn dispatch<'a>(&'a self, job: &'a JobRecord, submitter: &'a Submitter) -> DispatchFuture<'a>;
}

struct TypedHandler<J: Job> {
    _marker: PhantomData<fn() -> J>,
}

impl<J: Job> ErasedHandler for TypedHandler<J> {
    fn dispatch<'a>(&'a self, job: &'a JobRecord, submitter: &'a Submitter) -> DispatchFuture<'a> {
        Box::pin(run_typed::<J>(job, submitter))
    }
}

/// Deserialize, run, and settle a single job of a known type.
async fn run_typed<J: Job>(
    job: &JobRecord,
    submitter: &Submitter,
) -> std::result::Result<(), WorkerError> {
    // A payload that won't deserialize will never deserialize: dead-letter it.
    let input: J = rmp_serde::from_slice(&job.payload).map_err(|err| {
        WorkerError::from(PermanentFailure::new(format!(
            "invalid payload for job type `{}`: {err}",
            J::NAME
        )))
    })?;

    let cancel_token = job.cancel_token.clone().unwrap_or_default();
    let ctx = JobContext::new(submitter, &job.id, job.attempts, cancel_token);

    tracing::info!(
        job_id = %job.id,
        job_type = J::NAME,
        attempt = job.attempts,
        "job started"
    );

    match input.run(ctx).await {
        Ok(output) => {
            // A non-serializable output is a programming error, not a
            // transient one: dead-letter rather than retry forever.
            let bytes = rmp_serde::to_vec_named(&output).map_err(|err| {
                WorkerError::from(PermanentFailure::new(format!(
                    "job type `{}` produced an output that failed to serialize: {err}",
                    J::NAME
                )))
            })?;
            // A result-store write failure is transient: nack and retry. The
            // job already ran, so the retry re-runs it; handlers are required
            // to be idempotent regardless.
            submitter
                .results()
                .put(&job.id, &StoredOutcome::Success { output: bytes })
                .await
                .map_err(WorkerError::from)?;
            submitter.note_terminal(&job.id).await;
            tracing::info!(job_id = %job.id, job_type = J::NAME, "job completed");
            Ok(())
        }
        Err(error) => {
            let kind = input.classify(&error);
            let message = error.to_string();
            // Persist a failure outcome only when this attempt is the last
            // one: a transient error with attempts left is just a retry.
            let exhausted = job.attempts >= job.max_attempts;
            let terminal = matches!(kind, ErrorKind::Permanent) || exhausted;

            if terminal {
                let outcome = StoredOutcome::Failure {
                    kind: kind.into(),
                    message: message.clone(),
                };
                if let Err(err) = submitter.results().put(&job.id, &outcome).await {
                    tracing::warn!(
                        job_id = %job.id,
                        "failed to persist job failure outcome: {err}"
                    );
                }
                submitter.note_terminal(&job.id).await;
            }

            match kind {
                ErrorKind::Permanent => {
                    tracing::warn!(
                        job_id = %job.id,
                        job_type = J::NAME,
                        "job failed permanently: {message}"
                    );
                    Err(WorkerError::from(PermanentFailure::new(message)))
                }
                ErrorKind::Transient => {
                    tracing::warn!(
                        job_id = %job.id,
                        job_type = J::NAME,
                        attempt = job.attempts,
                        "job failed (transient): {message}"
                    );
                    // A plain (non-`PermanentFailure`) error nacks the job.
                    // We rely on taquba's nack behaviour for both branches:
                    // re-queue with backoff if attempts remain, otherwise
                    // dead-letter and notify completion waiters. We *don't*
                    // upgrade exhausted-transient to `PermanentFailure` here:
                    // the error wasn't permanent, just unlucky, and the
                    // truthful classification is preserved in the blob and
                    // in taquba's `last_error` for observability.
                    Err(WorkerError::from(message))
                }
            }
        }
    }
}

/// The taquba [`Worker`] that routes each claimed job to its typed handler.
struct Dispatcher {
    handlers: HashMap<&'static str, Box<dyn ErasedHandler>>,
    submitter: Submitter,
}

impl Worker for Dispatcher {
    async fn process(&self, job: &JobRecord) -> std::result::Result<(), WorkerError> {
        let job_type = job.headers.get(JOB_TYPE_HEADER).ok_or_else(|| {
            WorkerError::from(PermanentFailure::new(format!(
                "job {} is missing the `{JOB_TYPE_HEADER}` header",
                job.id
            )))
        })?;
        let handler = self.handlers.get(job_type.as_str()).ok_or_else(|| {
            WorkerError::from(PermanentFailure::new(format!(
                "no handler registered for job type `{job_type}`"
            )))
        })?;
        handler.dispatch(job, &self.submitter).await
    }
}

/// The orchestration service: registers job types, submits jobs, and spawns
/// the worker tasks that claim and execute them.
///
/// One runner per process: taquba is single-writer. Build it with
/// [`JobRunner::builder`], [`register`](Self::register) every job type, then
/// [`spawn`](Self::spawn) the worker. Jobs can be submitted before or after
/// spawning.
pub struct JobRunner {
    submitter: Submitter,
    handlers: HashMap<&'static str, Box<dyn ErasedHandler>>,
    concurrency: usize,
    poll_interval: Duration,
    spawned: bool,
}

impl JobRunner {
    /// Start configuring a runner.
    pub fn builder() -> JobRunnerBuilder {
        JobRunnerBuilder::new()
    }

    /// Register a job type so the spawned worker can dispatch it.
    ///
    /// Must be called before [`spawn`](Self::spawn).
    ///
    /// # Panics
    ///
    /// Panics if the runner has already been spawned, or if another job type
    /// with the same [`Job::NAME`] is already registered.
    pub fn register<J: Job>(&mut self) -> &mut Self {
        assert!(
            !self.spawned,
            "JobRunner::register must be called before spawn"
        );
        let previous = self.handlers.insert(
            J::NAME,
            Box::new(TypedHandler::<J> {
                _marker: PhantomData,
            }),
        );
        assert!(
            previous.is_none(),
            "job type `{}` is already registered (duplicate Job::NAME)",
            J::NAME
        );
        self
    }

    /// Submit a job with the queue's default options.
    ///
    /// Returns a [`JobHandle`] that can be awaited for the typed result.
    pub async fn submit<J: Job>(&self, job: J) -> Result<JobHandle<J>> {
        self.submitter.submit(job, SubmitOptions::default()).await
    }

    /// Submit a job with per-submission overrides (priority, schedule, etc.).
    pub async fn submit_with<J: Job>(&self, job: J, opts: SubmitOptions) -> Result<JobHandle<J>> {
        self.submitter.submit(job, opts).await
    }

    /// Spawn the worker task and return a handle for graceful shutdown.
    ///
    /// The worker claims and dispatches jobs concurrently (up to the
    /// configured limit) until either `shutdown` resolves or
    /// [`RunnerHandle::shutdown`] is called. In-flight jobs are always allowed
    /// to finish.
    ///
    /// # Panics
    ///
    /// Panics if called more than once.
    pub fn spawn<F>(&mut self, shutdown: F) -> RunnerHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        assert!(!self.spawned, "JobRunner::spawn may only be called once");
        self.spawned = true;

        let dispatcher = Arc::new(Dispatcher {
            handlers: std::mem::take(&mut self.handlers),
            submitter: self.submitter.clone(),
        });
        let queue = self.submitter.queue().clone();
        let queue_name = self.submitter.queue_name().to_string();
        let concurrency = self.concurrency;
        let poll_interval = self.poll_interval;

        // One token fans the "stop now" signal out to the worker and
        // to the sweeper. It's raised either when the caller's
        // `shutdown` future fires, when [`RunnerHandle`] cancels it,
        // or when the worker returns on its own (claim error, etc.).
        let token = CancellationToken::new();
        let outer = token.clone();
        let submitter = self.submitter.clone();
        let needs_sweeper = submitter.result_retention.is_some();

        let join = tokio::spawn(async move {
            let sweep_handle = if needs_sweeper {
                let submitter = submitter.clone();
                let sweep_token = token.clone();
                Some(tokio::spawn(async move {
                    submitter.run_sweep(sweep_token).await;
                }))
            } else {
                None
            };

            let worker_token = token.clone();
            let combined_shutdown = async move {
                tokio::select! {
                    _ = shutdown => {}
                    _ = worker_token.cancelled() => {}
                }
            };
            let result = run_worker_concurrent(
                &queue,
                &queue_name,
                dispatcher,
                concurrency,
                poll_interval,
                combined_shutdown,
            )
            .await;
            // Always cancel so the sweeper exits even when the worker
            // returned on its own rather than via external shutdown.
            token.cancel();
            if let Some(h) = sweep_handle {
                let _ = h.await;
            }
            result
        });

        RunnerHandle { token: outer, join }
    }
}

/// A handle to a spawned [`JobRunner`]'s worker task.
///
/// Dropping a `RunnerHandle` does **not** stop the worker: the spawned
/// task continues to run until the `shutdown` future passed to
/// [`JobRunner::spawn`] resolves on its own. Call [`shutdown`](Self::shutdown)
/// or [`wait`](Self::wait) to terminate or join the worker explicitly.
pub struct RunnerHandle {
    token: CancellationToken,
    join: JoinHandle<taquba::Result<()>>,
}

impl RunnerHandle {
    /// Signal the worker to stop and wait for it to drain.
    ///
    /// Stops claiming new jobs, lets in-flight jobs finish, then returns once
    /// the worker task has exited.
    pub async fn shutdown(self) -> Result<()> {
        self.token.cancel();
        self.wait().await
    }

    /// Wait for the worker task to exit on its own (because the `shutdown`
    /// future passed to [`JobRunner::spawn`] resolved, or a claim error
    /// terminated the loop).
    pub async fn wait(self) -> Result<()> {
        match self.join.await {
            Ok(result) => result.map_err(Error::from),
            Err(join_error) => std::panic::resume_unwind(join_error.into_panic()),
        }
    }
}

/// Builder for a [`JobRunner`]. Created via [`JobRunner::builder`].
pub struct JobRunnerBuilder {
    queue: Option<Arc<Queue>>,
    object_store: Option<Arc<dyn ObjectStore>>,
    queue_name: String,
    result_prefix: Option<String>,
    state: State,
    concurrency: usize,
    poll_interval: Duration,
    result_retention: Option<Duration>,
    clock: Option<Arc<dyn Clock>>,
}

impl JobRunnerBuilder {
    fn new() -> Self {
        Self {
            queue: None,
            object_store: None,
            queue_name: DEFAULT_QUEUE_NAME.to_string(),
            result_prefix: None,
            state: State::default(),
            concurrency: DEFAULT_CONCURRENCY,
            poll_interval: DEFAULT_POLL_INTERVAL,
            result_retention: None,
            clock: None,
        }
    }

    /// The taquba queue to run jobs on. Required.
    ///
    /// Accepts a `Queue` or an `Arc<Queue>`.
    pub fn queue(mut self, queue: impl Into<Arc<Queue>>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// The object store job result blobs are persisted to. Required.
    ///
    /// Typically the same `Arc<dyn ObjectStore>` passed to
    /// [`Queue::open`](taquba::Queue::open), but it does not have to be:
    /// pointing result blobs at a different store (a different bucket, a
    /// different backend) is supported. The blobs land under
    /// [`result_prefix`](Self::result_prefix); that prefix must not overlap
    /// the `path` the queue's SlateDB store was opened at if the two share a
    /// store.
    pub fn object_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.object_store = Some(store);
        self
    }

    /// The logical queue name jobs are enqueued under. Defaults to `"jobs"`.
    pub fn queue_name(mut self, name: impl Into<String>) -> Self {
        self.queue_name = name.into();
        self
    }

    /// Override the object-store prefix job result blobs are written under.
    ///
    /// Defaults to `"{queue_name}-results"`. If the result store and the
    /// queue's SlateDB store share an object store, this prefix must not
    /// overlap the `path` the queue was opened at.
    pub fn result_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.result_prefix = Some(prefix.into());
        self
    }

    /// Register a piece of application state, retrievable from handlers via
    /// [`JobContext::state`]. At most one value per type.
    pub fn state<T: Any + Send + Sync>(mut self, value: T) -> Self {
        self.state.insert(value);
        self
    }

    /// The maximum number of jobs the worker runs concurrently. Defaults to
    /// 16.
    ///
    /// # Panics
    ///
    /// Panics if `max` is zero.
    pub fn max_concurrent_jobs(mut self, max: usize) -> Self {
        assert!(max > 0, "max_concurrent_jobs must be at least 1");
        self.concurrency = max;
        self
    }

    /// How long the worker waits on an idle queue before re-checking.
    /// In-process submissions wake it immediately regardless. Defaults to
    /// 100ms.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Enable result-blob retention with the given window. When set,
    /// the runner writes a terminal marker every time a job reaches a
    /// terminal state (success or terminal failure) and the in-process
    /// sweeper deletes that job's result blob `retention` after
    /// termination. When unset (default), no marker is written and
    /// result blobs are retained indefinitely; plan your own
    /// object-store lifecycle policy in that case.
    ///
    /// Once a blob is swept, any subsequent
    /// [`JobHandle::fetch_result`] for that job returns `Ok(None)`,
    /// and an idempotent re-submission of the same payload falls
    /// through to re-running the job rather than short-circuiting.
    /// Set the window long enough to cover the longest gap your
    /// callers need between the original submission and an
    /// idempotent re-submit.
    ///
    /// # Panics
    ///
    /// Panics if `retention < 1ms`: the sweep interval equals the
    /// retention window, and a sub-millisecond interval would issue
    /// list+delete calls against the object store far faster than
    /// the store can usefully serve them.
    ///
    /// [`JobHandle::fetch_result`]: crate::JobHandle::fetch_result
    pub fn result_retention(mut self, retention: Duration) -> Self {
        assert!(
            retention >= Duration::from_millis(1),
            "result_retention must be at least 1ms",
        );
        self.result_retention = Some(retention);
        self
    }

    /// Override the [`Clock`] the runner reads its timestamps from
    /// (for terminal-marker timestamps and the retention sweep's
    /// cutoff). Defaults to the same clock the [`Queue`] was opened
    /// with (via [`Queue::clock`]).
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Build the runner.
    ///
    /// # Errors
    ///
    /// Returns [`Error::MissingQueue`] if no queue was configured, or
    /// [`Error::MissingObjectStore`] if no object store was configured.
    pub fn build(self) -> Result<JobRunner> {
        let queue = self.queue.ok_or(Error::MissingQueue)?;
        let object_store = self.object_store.ok_or(Error::MissingObjectStore)?;
        let prefix = self
            .result_prefix
            .unwrap_or_else(|| format!("{}-results", self.queue_name));
        let results = ResultStore::new(object_store, prefix);
        let clock = self.clock.unwrap_or_else(|| queue.clock());

        let submitter = Submitter {
            queue,
            queue_name: Arc::from(self.queue_name),
            results,
            state: Arc::new(self.state),
            clock,
            result_retention: self.result_retention,
        };

        Ok(JobRunner {
            submitter,
            handlers: HashMap::new(),
            concurrency: self.concurrency,
            poll_interval: self.poll_interval,
            spawned: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use serde::{Deserialize, Serialize};
    use taquba::object_store::{ObjectStore, memory::InMemory};
    use taquba::{JobStatus, MockClock, OpenOptions, Queue, QueueConfig};

    use crate::handle::JoinError;
    use crate::job::{ErrorKind, payload_idempotency_key};

    #[derive(Debug, thiserror::Error)]
    #[error("{0}")]
    struct TestError(String);

    #[derive(Serialize, Deserialize)]
    struct Adder {
        a: i64,
        b: i64,
    }

    impl Job for Adder {
        const NAME: &'static str = "test.adder";
        type Output = i64;
        type Error = TestError;

        async fn run(&self, ctx: JobContext<'_>) -> std::result::Result<i64, TestError> {
            // Exercise application state access.
            let label = ctx.state::<&'static str>();
            assert_eq!(*label, "ok");
            Ok(self.a + self.b)
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AlwaysFails;

    impl Job for AlwaysFails {
        const NAME: &'static str = "test.always-fails";
        type Output = ();
        type Error = TestError;

        async fn run(&self, _ctx: JobContext<'_>) -> std::result::Result<(), TestError> {
            Err(TestError("nope".to_string()))
        }

        fn classify(&self, _error: &TestError) -> ErrorKind {
            ErrorKind::Permanent
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AlwaysFailsTransient;

    impl Job for AlwaysFailsTransient {
        const NAME: &'static str = "test.always-fails-transient";
        type Output = ();
        type Error = TestError;

        async fn run(&self, _ctx: JobContext<'_>) -> std::result::Result<(), TestError> {
            Err(TestError("flaky".to_string()))
        }
        // Classification stays at the `Transient` default.
    }

    /// A child job that bumps a shared counter so the fan-out test can
    /// observe each invocation independently.
    #[derive(Serialize, Deserialize)]
    struct Increment {
        n: i64,
    }

    impl Job for Increment {
        const NAME: &'static str = "test.increment";
        type Output = ();
        type Error = TestError;

        async fn run(&self, ctx: JobContext<'_>) -> std::result::Result<(), TestError> {
            ctx.state::<Arc<AtomicU32>>().fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        // Distinct dedup keys per `n` so identical children aren't collapsed.
        fn idempotency_key(&self) -> Option<String> {
            Some(format!("increment:{}", self.n))
        }
    }

    /// A parent job that submits N children from inside its handler. The
    /// parent itself doesn't touch the counter; observing the counter reach
    /// N proves the children all ran.
    #[derive(Serialize, Deserialize)]
    struct Coordinator {
        children: i64,
    }

    impl Job for Coordinator {
        const NAME: &'static str = "test.coordinator";
        type Output = ();
        type Error = TestError;

        async fn run(&self, ctx: JobContext<'_>) -> std::result::Result<(), TestError> {
            for n in 0..self.children {
                ctx.submit(Increment { n })
                    .await
                    .map_err(|e| TestError(e.to_string()))?;
            }
            Ok(())
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Keyed {
        n: i64,
    }

    impl Job for Keyed {
        const NAME: &'static str = "test.keyed";
        type Output = i64;
        type Error = TestError;

        async fn run(&self, _ctx: JobContext<'_>) -> std::result::Result<i64, TestError> {
            Ok(self.n)
        }

        fn idempotency_key(&self) -> Option<String> {
            Some(format!("keyed:{}", self.n))
        }
    }

    /// First claim sleeps past the lease so the reaper requeues it;
    /// subsequent claims succeed. The shared counter records every
    /// claim so the test can observe the requeue.
    #[derive(Serialize, Deserialize)]
    struct Reclaimable;

    impl Job for Reclaimable {
        const NAME: &'static str = "test.reclaimable";
        type Output = u32;
        type Error = TestError;

        async fn run(&self, ctx: JobContext<'_>) -> std::result::Result<u32, TestError> {
            ctx.state::<Arc<AtomicU32>>().fetch_add(1, Ordering::SeqCst);
            let attempt = ctx.attempt();
            if attempt == 1 {
                // Wait long enough for the lease to expire under
                // virtual time. Subsequent attempts return immediately.
                tokio::time::sleep(Duration::from_secs(300)).await;
            }
            Ok(attempt)
        }
    }

    /// Like [`Keyed`] but always fails permanently. Used to test that a
    /// cached *failure* outcome is also short-circuited on re-submission.
    #[derive(Serialize, Deserialize)]
    struct KeyedFailure {
        n: i64,
    }

    impl Job for KeyedFailure {
        const NAME: &'static str = "test.keyed-failure";
        type Output = ();
        type Error = TestError;

        async fn run(&self, _ctx: JobContext<'_>) -> std::result::Result<(), TestError> {
            Err(TestError(format!("permanent failure for n={}", self.n)))
        }

        fn idempotency_key(&self) -> Option<String> {
            Some(format!("keyed-failure:{}", self.n))
        }

        fn classify(&self, _error: &TestError) -> ErrorKind {
            ErrorKind::Permanent
        }
    }

    /// A job whose `idempotency_key` is fixed regardless of payload, so
    /// two submissions can share a key but disagree on input.
    #[derive(Debug, Serialize, Deserialize)]
    struct FixedKey {
        content: String,
    }

    impl Job for FixedKey {
        const NAME: &'static str = "test.fixed-key";
        type Output = ();
        type Error = TestError;

        async fn run(&self, _ctx: JobContext<'_>) -> std::result::Result<(), TestError> {
            Ok(())
        }

        fn idempotency_key(&self) -> Option<String> {
            Some("fixed".to_string())
        }
    }

    async fn open_queue(name: &str) -> (Arc<Queue>, Arc<dyn ObjectStore>) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let queue = Arc::new(Queue::open(store.clone(), name).await.unwrap());
        (queue, store)
    }

    async fn open_queue_with_config(
        name: &str,
        cfg: QueueConfig,
    ) -> (Arc<Queue>, Arc<dyn ObjectStore>) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let opts = OpenOptions {
            default_queue_config: cfg,
            ..OpenOptions::default()
        };
        let queue = Arc::new(
            Queue::open_with_options(store.clone(), name, opts)
                .await
                .unwrap(),
        );
        (queue, store)
    }

    /// Open a queue whose internal `now_ms` reads from `clock`, with
    /// tight scheduler / reaper intervals so background sweeps observe
    /// clock advances promptly under `tokio::test(start_paused = true)`.
    async fn open_queue_with_clock(
        name: &str,
        clock: MockClock,
        cfg: QueueConfig,
    ) -> (Arc<Queue>, Arc<dyn ObjectStore>) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let opts = OpenOptions {
            clock: Arc::new(clock),
            scheduler_interval: Duration::from_millis(10),
            reaper_interval: Duration::from_millis(10),
            default_queue_config: cfg,
            ..OpenOptions::default()
        };
        let queue = Arc::new(
            Queue::open_with_options(store.clone(), name, opts)
                .await
                .unwrap(),
        );
        (queue, store)
    }

    #[tokio::test(start_paused = true)]
    async fn submit_without_idempotency_key_is_always_newly_submitted() {
        let (queue, store) = open_queue("test-no-idem").await;
        let runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .state("ok")
            .build()
            .unwrap();

        let first = runner.submit(Adder { a: 1, b: 2 }).await.unwrap();
        let second = runner.submit(Adder { a: 1, b: 2 }).await.unwrap();
        assert!(first.newly_submitted());
        assert!(second.newly_submitted());
        assert_ne!(first.id(), second.id());
    }

    #[tokio::test(start_paused = true)]
    async fn submit_run_and_join_success() {
        let (queue, store) = open_queue("test-success").await;
        let mut runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .state("ok")
            .build()
            .unwrap();
        runner.register::<Adder>();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(Adder { a: 2, b: 3 }).await.unwrap();
        let outcome = job.join().await.unwrap();
        assert_eq!(outcome.unwrap(), 5);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn await_handle_directly_yields_output() {
        let (queue, store) = open_queue("test-await").await;
        let mut runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .state("ok")
            .build()
            .unwrap();
        runner.register::<Adder>();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(Adder { a: 10, b: 7 }).await.unwrap();
        let sum = job.await.unwrap();
        assert_eq!(sum, 17);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_failure_is_dead_lettered_with_recorded_outcome() {
        let (queue, store) = open_queue("test-failure").await;
        let mut runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .build()
            .unwrap();
        runner.register::<AlwaysFails>();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(AlwaysFails).await.unwrap();
        let outcome = job.join().await.unwrap();
        let error = outcome.unwrap_err();
        assert_eq!(error.kind, ErrorKind::Permanent);
        assert!(error.message.contains("nope"));
        assert_eq!(job.status().await.unwrap(), Some(JobStatus::Dead));

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn idempotency_key_collapses_duplicate_submissions() {
        let (queue, store) = open_queue("test-idempotency").await;
        // No spawn: jobs stay pending so the dedup key is still held.
        let runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .build()
            .unwrap();

        let first = runner.submit(Keyed { n: 1 }).await.unwrap();
        assert!(first.newly_submitted());
        let second = runner.submit(Keyed { n: 1 }).await.unwrap();
        assert_eq!(first.id(), second.id());
        assert!(!second.newly_submitted());

        let different = runner.submit(Keyed { n: 2 }).await.unwrap();
        assert_ne!(first.id(), different.id());
        assert!(different.newly_submitted());
    }

    #[tokio::test(start_paused = true)]
    async fn input_mismatch_on_same_key_different_payload() {
        let (queue, store) = open_queue("test-mismatch").await;
        let runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .build()
            .unwrap();

        runner
            .submit(FixedKey {
                content: "alpha".into(),
            })
            .await
            .unwrap();
        let result = runner
            .submit(FixedKey {
                content: "beta".into(),
            })
            .await;
        match result {
            Err(Error::InputMismatch(key)) => assert_eq!(key, "fixed"),
            Err(other) => panic!("expected InputMismatch, got Err({other:?})"),
            Ok(_) => panic!("expected InputMismatch, got Ok(_)"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn input_mismatch_survives_restart() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let queue_name = "test-mismatch-restart";

        // Round 1: register the submission record. The runner / queue
        // are dropped when this scope exits, releasing their
        // background tasks before the next round opens the same store.
        {
            let queue = Arc::new(Queue::open(store.clone(), queue_name).await.unwrap());
            let runner = JobRunner::builder()
                .queue(queue.clone())
                .object_store(store.clone())
                .build()
                .unwrap();
            runner
                .submit(FixedKey {
                    content: "alpha".into(),
                })
                .await
                .unwrap();
        }

        // Round 2: fresh queue against the same store, differing payload.
        let queue = Arc::new(Queue::open(store.clone(), queue_name).await.unwrap());
        let runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .build()
            .unwrap();
        let result = runner
            .submit(FixedKey {
                content: "beta".into(),
            })
            .await;
        match result {
            Err(Error::InputMismatch(_)) => {}
            Err(other) => panic!("expected InputMismatch across restart, got Err({other:?})"),
            Ok(_) => panic!("expected InputMismatch across restart, got Ok(_)"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn idempotency_key_short_circuits_to_cached_success_after_completion() {
        let (queue, store) = open_queue("test-cached-success").await;
        let mut runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .build()
            .unwrap();
        runner.register::<Keyed>();
        let handle = runner.spawn(std::future::pending::<()>());

        // First submission runs to completion and writes a result blob.
        // Awaiting the job under default retention also acks it, releasing
        // the queue dedup key.
        let first = runner.submit(Keyed { n: 42 }).await.unwrap();
        assert!(first.newly_submitted());
        let first_id = first.id().to_string();
        let first_value = first.await.unwrap();
        assert_eq!(first_value, 42);

        // Second submission with the same payload short-circuits: no new
        // job is created, the returned handle points at the original id,
        // and awaiting it yields the cached outcome.
        let second = runner.submit(Keyed { n: 42 }).await.unwrap();
        assert!(!second.newly_submitted());
        assert_eq!(second.id(), first_id);
        assert_eq!(second.await.unwrap(), 42);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn idempotency_key_short_circuits_to_cached_failure_after_completion() {
        // `max_attempts = 1` keeps the test deterministic: the Permanent
        // failure writes its outcome blob on the first (and only) attempt.
        let (queue, store) = open_queue_with_config(
            "test-cached-failure",
            QueueConfig {
                max_attempts: 1,
                ..QueueConfig::default()
            },
        )
        .await;
        let mut runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .build()
            .unwrap();
        runner.register::<KeyedFailure>();
        let handle = runner.spawn(std::future::pending::<()>());

        let first = runner.submit(KeyedFailure { n: 7 }).await.unwrap();
        assert!(first.newly_submitted());
        let first_id = first.id().to_string();
        match first.await {
            Err(JoinError::Job(job_err)) => assert_eq!(job_err.kind, ErrorKind::Permanent),
            other => panic!("expected Permanent JobError, got {other:?}"),
        }

        let second = runner.submit(KeyedFailure { n: 7 }).await.unwrap();
        assert!(!second.newly_submitted());
        assert_eq!(second.id(), first_id);
        match second.await {
            Err(JoinError::Job(job_err)) => assert_eq!(job_err.kind, ErrorKind::Permanent),
            other => panic!("expected cached Permanent JobError, got {other:?}"),
        }

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn idempotency_key_short_circuits_after_restart() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let queue_name = "test-cached-restart";

        // Round 1: run a keyed job to completion against this store.
        let first_id = {
            let queue = Arc::new(Queue::open(store.clone(), queue_name).await.unwrap());
            let mut runner = JobRunner::builder()
                .queue(queue.clone())
                .object_store(store.clone())
                .build()
                .unwrap();
            runner.register::<Keyed>();
            let handle = runner.spawn(std::future::pending::<()>());

            let job = runner.submit(Keyed { n: 99 }).await.unwrap();
            let id = job.id().to_string();
            assert_eq!(job.await.unwrap(), 99);

            handle.shutdown().await.unwrap();
            id
        };

        // Round 2: fresh runner against the same store. The submission
        // record from round 1 + its result blob are still on disk, so
        // re-submitting the same payload should short-circuit.
        let queue = Arc::new(Queue::open(store.clone(), queue_name).await.unwrap());
        let runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .build()
            .unwrap();
        let second = runner.submit(Keyed { n: 99 }).await.unwrap();
        assert!(!second.newly_submitted());
        assert_eq!(second.id(), first_id);
        // fetch_result reads the persisted blob directly.
        let outcome = second
            .fetch_result()
            .await
            .unwrap()
            .expect("cached result should be reachable across restart");
        assert_eq!(outcome.unwrap(), 99);
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_job_type_is_dead_lettered() {
        let (queue, store) = open_queue("test-unknown").await;
        // Register nothing; submit a job whose type has no handler.
        let mut runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .build()
            .unwrap();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(Keyed { n: 9 }).await.unwrap();
        let outcome = job.join().await.unwrap();
        // No handler ran, so no result blob: the outcome is synthesized from
        // the dead-lettered record.
        let error = outcome.unwrap_err();
        assert!(error.message.contains("no handler registered"));
        assert_eq!(job.status().await.unwrap(), Some(JobStatus::Dead));

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn reserved_header_in_submit_options_is_rejected() {
        let (queue, store) = open_queue("test-reserved-header").await;
        let runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .build()
            .unwrap();

        let mut opts = SubmitOptions::default();
        opts.headers
            .insert(JOB_TYPE_HEADER.to_string(), "evil".to_string());
        match runner.submit_with(Keyed { n: 1 }, opts).await {
            Err(Error::ReservedHeader(key)) => assert_eq!(key, JOB_TYPE_HEADER),
            Err(other) => panic!("expected ReservedHeader, got {other:?}"),
            Ok(_) => panic!("expected ReservedHeader, got Ok"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn transient_failure_exhausts_retries_and_dead_letters() {
        let cfg = QueueConfig {
            max_attempts: 2,
            retry_backoff_base: Duration::ZERO,
            ..QueueConfig::default()
        };
        let (queue, store) = open_queue_with_config("test-transient-exhaust", cfg).await;
        let mut runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .build()
            .unwrap();
        runner.register::<AlwaysFailsTransient>();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(AlwaysFailsTransient).await.unwrap();
        let outcome = job.join().await.unwrap();
        let error = outcome.unwrap_err();

        // The classification stays Transient even on the terminal blob: the
        // failure wasn't permanent, just out of retries.
        assert_eq!(error.kind, ErrorKind::Transient);
        assert!(error.message.contains("flaky"));
        assert_eq!(job.status().await.unwrap(), Some(JobStatus::Dead));

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn awaiting_failed_handle_returns_join_error_job() {
        let (queue, store) = open_queue("test-join-error-job").await;
        let mut runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .build()
            .unwrap();
        runner.register::<AlwaysFails>();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(AlwaysFails).await.unwrap();
        match job.await {
            Err(JoinError::Job(job_error)) => {
                assert_eq!(job_error.kind, ErrorKind::Permanent);
                assert!(job_error.message.contains("nope"));
            }
            Err(JoinError::Infra(e)) => panic!("expected JoinError::Job, got Infra: {e}"),
            Ok(()) => panic!("expected JoinError::Job, got Ok"),
        }

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn await_after_queue_record_reaped_falls_back_to_result_blob() {
        // Test the WaitOutcome::NotFound -> fetch_result
        // fallback in JobHandle::join_timeout. Under default retention
        // (`keep_done_jobs: None`), the worker's ack deletes the queue
        // record. If the await starts after that point, `wait_for_completion`
        // sees no record on its first poll and returns NotFound; the
        // durable result blob must be consulted instead.
        let (queue, store) = open_queue("test-notfound-fallback").await;
        let mut runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .state("ok")
            .build()
            .unwrap();
        runner.register::<Adder>();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(Adder { a: 11, b: 31 }).await.unwrap();
        // Wait long enough for the worker to claim, run, and ack the job,
        // *then* start the await. With backoff disabled and the in-memory
        // store, 200ms is plenty.
        tokio::time::sleep(Duration::from_millis(200)).await;
        // Sanity check: the queue record really is gone before we await.
        assert_eq!(job.status().await.unwrap(), None);

        let sum = job.await.unwrap();
        assert_eq!(sum, 42);

        handle.shutdown().await.unwrap();
    }

    #[test]
    fn payload_idempotency_key_is_stable_and_distinguishes_payloads() {
        let same_a = payload_idempotency_key(&Keyed { n: 7 }).unwrap();
        let same_b = payload_idempotency_key(&Keyed { n: 7 }).unwrap();
        assert_eq!(same_a, same_b, "identical payloads must hash identically");

        let different = payload_idempotency_key(&Keyed { n: 8 }).unwrap();
        assert_ne!(
            same_a, different,
            "different payloads must hash differently"
        );

        // The key is prefixed with the job type name for namespace isolation
        // across job types that happen to serialize identically.
        assert!(
            same_a.starts_with(&format!("{}:", Keyed::NAME)),
            "key `{same_a}` must start with `{}:`",
            Keyed::NAME
        );
        // SHA-256 hex is 64 chars after the `name:` prefix.
        let hex_part = same_a.split_once(':').unwrap().1;
        assert_eq!(hex_part.len(), 64, "expected sha-256 hex, got {hex_part:?}");
        assert!(hex_part.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[tokio::test(start_paused = true)]
    async fn fan_out_from_handler_runs_children() {
        // Long lease + single attempt.
        let cfg = QueueConfig {
            lease_duration: Duration::from_secs(300),
            max_attempts: 1,
            retry_backoff_base: Duration::ZERO,
            ..QueueConfig::default()
        };
        let (queue, store) = open_queue_with_config("test-fanout", cfg).await;
        let counter = Arc::new(AtomicU32::new(0));
        let mut runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .state(counter.clone())
            .build()
            .unwrap();
        runner.register::<Coordinator>();
        runner.register::<Increment>();
        let handle = runner.spawn(std::future::pending::<()>());

        // The parent submits 3 children and returns. The children run
        // independently and each bump the counter.
        runner
            .submit(Coordinator { children: 3 })
            .await
            .unwrap()
            .await
            .unwrap();

        // Poll for all 3 children to complete (they're not awaited by the
        // parent, so they can lag its terminal state).
        tokio::time::timeout(Duration::from_secs(5), async {
            while counter.load(Ordering::SeqCst) < 3 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "expected counter to reach 3, got {}",
                counter.load(Ordering::SeqCst)
            )
        });
        assert_eq!(counter.load(Ordering::SeqCst), 3);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn scheduled_job_runs_when_clock_passes_run_at() {
        let t0_ms = 1_700_000_000_000_u64;
        let clock = MockClock::new(t0_ms);
        let (queue, store) =
            open_queue_with_clock("test-scheduled", clock.clone(), QueueConfig::default()).await;
        let mut runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .state("ok")
            .build()
            .unwrap();
        runner.register::<Adder>();
        let handle = runner.spawn(std::future::pending::<()>());

        // Schedule the job 60s past the clock's current value.
        let run_at = SystemTime::UNIX_EPOCH + Duration::from_millis(t0_ms + 60_000);
        let job = runner
            .submit_with(
                Adder { a: 5, b: 7 },
                SubmitOptions {
                    run_at: Some(run_at),
                    ..SubmitOptions::default()
                },
            )
            .await
            .unwrap();

        // Initially scheduled: the scheduler hasn't promoted it yet
        // because the clock is still at T0 < run_at.
        assert_eq!(job.status().await.unwrap(), Some(JobStatus::Scheduled));

        // Advance past run_at. The scheduler observes it on its next
        // tick, promotes the job to Pending, and the worker claims +
        // runs it.
        clock.advance(Duration::from_secs(120));

        let sum = job.await.unwrap();
        assert_eq!(sum, 12);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn lease_expiry_triggers_reaper_requeue() {
        let t0_ms = 1_700_000_000_000_u64;
        let clock = MockClock::new(t0_ms);
        let cfg = QueueConfig {
            lease_duration: Duration::from_secs(10),
            max_attempts: 5,
            retry_backoff_base: Duration::ZERO,
            ..QueueConfig::default()
        };
        let (queue, store) = open_queue_with_clock("test-lease", clock.clone(), cfg).await;
        let attempts = Arc::new(AtomicU32::new(0));
        let mut runner = JobRunner::builder()
            .queue(queue)
            .object_store(store)
            .state(attempts.clone())
            .build()
            .unwrap();
        runner.register::<Reclaimable>();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(Reclaimable).await.unwrap();

        // Let the first claim land (worker increments `attempts` and
        // enters its long sleep). The 200ms sleep just has to exceed
        // the worker's `poll_interval` so the worker's
        // `wait_for_jobs` returns.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        // Advance past the lease (10s). On its next tick the reaper
        // observes the expired claim and requeues the job; the worker
        // reclaims it in a fresh handler invocation (the first is
        // still in its 300s virtual sleep).
        clock.advance(Duration::from_secs(30));

        // The second attempt returns immediately with its attempt
        // number, which is taquba's `attempts` after re-claim.
        let attempt = job.await.unwrap();
        assert_eq!(attempt, 2);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        // Don't shutdown: the first handler is still in its 300s
        // virtual sleep, and a graceful shutdown would drain the
        // JoinSet (waiting for that task to drop). Tokio's test
        // runtime will abort the spawned worker when the test
        // function returns.
        drop(handle);
    }

    /// Build a [`ResultStore`] pointing at the prefix
    /// [`JobRunnerBuilder::build`] derives by default
    /// (`"{queue_name}-results"`), so tests can inspect markers and
    /// blobs the runner wrote.
    fn inspect_results(store: Arc<dyn ObjectStore>, queue_name: &str) -> ResultStore {
        ResultStore::new(store, format!("{queue_name}-results"))
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_marker_written_when_retention_is_set() {
        let queue_name = "test-retention-marker";
        let (queue, store) = open_queue(queue_name).await;
        let mut runner = JobRunner::builder()
            .queue(queue)
            .object_store(store.clone())
            .queue_name(queue_name)
            .state("ok")
            .result_retention(Duration::from_secs(60))
            .build()
            .unwrap();
        runner.register::<Adder>();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(Adder { a: 1, b: 2 }).await.unwrap();
        let job_id = job.id().to_string();
        assert_eq!(job.await.unwrap(), 3);

        let markers = inspect_results(store, queue_name)
            .list_terminal_markers()
            .await
            .unwrap();
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].job_id, job_id);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn no_terminal_marker_when_retention_is_unset() {
        let queue_name = "test-retention-off";
        let (queue, store) = open_queue(queue_name).await;
        let mut runner = JobRunner::builder()
            .queue(queue)
            .object_store(store.clone())
            .queue_name(queue_name)
            .state("ok")
            .build()
            .unwrap();
        runner.register::<Adder>();
        let handle = runner.spawn(std::future::pending::<()>());

        runner
            .submit(Adder { a: 1, b: 2 })
            .await
            .unwrap()
            .await
            .unwrap();

        assert!(
            inspect_results(store, queue_name)
                .list_terminal_markers()
                .await
                .unwrap()
                .is_empty()
        );

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_marker_written_for_terminal_failures_too() {
        let queue_name = "test-retention-failure";
        let (queue, store) = open_queue_with_config(
            queue_name,
            QueueConfig {
                max_attempts: 1,
                ..QueueConfig::default()
            },
        )
        .await;
        let mut runner = JobRunner::builder()
            .queue(queue)
            .object_store(store.clone())
            .queue_name(queue_name)
            .result_retention(Duration::from_secs(60))
            .build()
            .unwrap();
        runner.register::<AlwaysFails>();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(AlwaysFails).await.unwrap();
        let job_id = job.id().to_string();
        let _ = job.join().await.unwrap();

        let markers = inspect_results(store, queue_name)
            .list_terminal_markers()
            .await
            .unwrap();
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].job_id, job_id);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn sweeper_keeps_markers_younger_than_retention() {
        // Retention 200ms (sweep interval also 200ms). Advancing 200ms
        // after the marker is written fires the next sweep tick at the
        // exact retention boundary; strict `<` means the marker isn't
        // yet expired, so the sweep must skip it.
        let queue_name = "test-retention-young";
        let t0_ms = 1_700_000_000_000_u64;
        let clock = MockClock::new(t0_ms);
        let (queue, store) =
            open_queue_with_clock(queue_name, clock.clone(), QueueConfig::default()).await;
        let mut runner = JobRunner::builder()
            .queue(queue)
            .object_store(store.clone())
            .queue_name(queue_name)
            .state("ok")
            .result_retention(Duration::from_millis(200))
            .build()
            .unwrap();
        runner.register::<Adder>();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(Adder { a: 1, b: 2 }).await.unwrap();
        let job_id = job.id().to_string();
        assert_eq!(job.await.unwrap(), 3);

        // Advance to the boundary; the sweep at this point must NOT
        // drop the marker (strict `<` in the cutoff comparison).
        clock.advance(Duration::from_millis(200));
        tokio::time::sleep(Duration::from_millis(200)).await;

        let markers = inspect_results(store, queue_name)
            .list_terminal_markers()
            .await
            .unwrap();
        assert_eq!(markers.len(), 1, "marker at boundary must be retained");
        assert_eq!(markers[0].job_id, job_id);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn sweeper_removes_expired_markers_and_result_blobs() {
        let queue_name = "test-retention-expired";
        let t0_ms = 1_700_000_000_000_u64;
        let clock = MockClock::new(t0_ms);
        let (queue, store) =
            open_queue_with_clock(queue_name, clock.clone(), QueueConfig::default()).await;
        let mut runner = JobRunner::builder()
            .queue(queue)
            .object_store(store.clone())
            .queue_name(queue_name)
            .state("ok")
            .result_retention(Duration::from_millis(100))
            .build()
            .unwrap();
        runner.register::<Adder>();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(Adder { a: 1, b: 2 }).await.unwrap();
        let job_id = job.id().to_string();
        assert_eq!(job.await.unwrap(), 3);

        // Both the result blob and the marker exist before the sweep.
        let results = inspect_results(store.clone(), queue_name);
        assert!(results.get(&job_id).await.unwrap().is_some());
        assert_eq!(results.list_terminal_markers().await.unwrap().len(), 1);

        // Advance well past retention so the next sweep tick clears
        // both the result blob and the marker.
        clock.advance(Duration::from_millis(300));
        tokio::time::sleep(Duration::from_millis(300)).await;

        let cleared = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let markers = results.list_terminal_markers().await.unwrap();
                let blob = results.get(&job_id).await.unwrap();
                if markers.is_empty() && blob.is_none() {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or(false);
        assert!(cleared, "sweeper did not clear the expired marker + blob");

        handle.shutdown().await.unwrap();
    }
}
