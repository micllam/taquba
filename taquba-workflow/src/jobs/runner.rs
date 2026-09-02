use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime};

use crate::{
    Memo, MemoStore, NoopTerminalHook, RunSpec, Step, StepError, StepOutcome, StepRunner,
    WorkflowRuntime,
};
use taquba::object_store::ObjectStore;
use taquba::{Clock, Queue};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::jobs::context::{JobContext, State};
use crate::jobs::error::{Error, Result};
use crate::jobs::handle::JobHandle;
use crate::jobs::job::{ErrorKind, Job};
use crate::keys::hex_sha256;
use crate::outcome::{hash_input, read_outcome, run_recorded};

/// Reserved header key holding a job's [`Job::NAME`], read by the step
/// runner to route the run to the registered handler.
pub(crate) const JOB_TYPE_HEADER: &str = "jobs.type";

const DEFAULT_QUEUE_NAME: &str = "jobs";
const DEFAULT_CONCURRENCY: usize = 16;
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The run id of a job with an idempotency key: the hex SHA-256 digest of
/// the key, so any key maps onto the character set a run id accepts.
fn run_id_for_key(key: &str) -> String {
    hex_sha256(&[key.as_bytes()])
}

/// Per-submission overrides for [`JobRunner::submit_with`].
///
/// Every field is optional; the defaults inherit the queue's configuration.
/// Construct via [`SubmitOptions::default`] and struct-update syntax.
#[derive(Debug, Clone, Default)]
pub struct SubmitOptions {
    /// Override the job type's and queue's `max_attempts` for this
    /// submission. Takes precedence over [`Job::max_attempts`].
    pub max_attempts: Option<u32>,
    /// Override the queue's default priority. Lower numbers are claimed
    /// first; see [`taquba::PRIORITY_HIGH`] and the other priority constants.
    pub priority: Option<u32>,
    /// Delay the job until this time. The job waits in the scheduled key
    /// space until taquba's scheduler promotes it.
    pub run_at: Option<SystemTime>,
    /// Extra headers to attach to the job. The runner adds its own reserved
    /// routing header on every submission; setting that key here fails the
    /// submission with [`Error::ReservedHeader`](crate::jobs::Error::ReservedHeader).
    pub headers: HashMap<String, String>,
}

/// The state shared by the runner, every [`JobHandle`] and every
/// [`JobContext`]: the workflow runtime a job runs as one step of, the
/// memo store its outcome record is read from, and the registered
/// application state.
pub(crate) struct Inner {
    runtime: WorkflowRuntime<Dispatch, NoopTerminalHook>,
    queue: Arc<Queue>,
    memo_store: MemoStore,
    state: State,
    poll_interval: Duration,
}

impl Inner {
    pub(crate) fn runtime(&self) -> &WorkflowRuntime<Dispatch, NoopTerminalHook> {
        &self.runtime
    }

    pub(crate) fn queue(&self) -> &Arc<Queue> {
        &self.queue
    }

    pub(crate) fn state(&self) -> &State {
        &self.state
    }

    pub(crate) fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    pub(crate) fn run_memo(&self, id: &str) -> Memo {
        self.memo_store.new_run_memo(id)
    }

    pub(crate) async fn submit<J: Job>(
        self: &Arc<Self>,
        job: J,
        opts: SubmitOptions,
    ) -> Result<JobHandle<J>> {
        let mut headers = opts.headers;
        if headers.contains_key(JOB_TYPE_HEADER) {
            return Err(Error::ReservedHeader(JOB_TYPE_HEADER.to_string()));
        }
        headers.insert(JOB_TYPE_HEADER.to_string(), J::NAME.to_string());
        let payload = rmp_serde::to_vec_named(&job)?;
        let key = job.idempotency_key();
        let run_id = key.as_deref().map(run_id_for_key);

        // A completed job with this key answers from its outcome record,
        // which outlives the run record the workflow deletes at
        // termination.
        if let Some(run_id) = &run_id
            && let Some(record) = read_outcome(&self.run_memo(run_id)).await?
        {
            if record.input_hash != hash_input(&payload) {
                return Err(Error::InputMismatch(key.unwrap_or_default()));
            }
            tracing::debug!(job_id = %run_id, job_type = J::NAME, "submit matched a completed job");
            return Ok(JobHandle::new(run_id.clone(), None, self.clone(), false));
        }

        let outcome = self
            .runtime
            .submit(RunSpec {
                run_id,
                input: payload,
                headers,
                priority: opts.priority,
                max_attempts_per_step: opts.max_attempts.or_else(|| job.max_attempts()),
                run_at: opts.run_at,
                kv_writes: HashMap::new(),
            })
            .await
            .map_err(|err| match err {
                crate::Error::InputMismatch(_) => Error::InputMismatch(key.unwrap_or_default()),
                other => Error::Workflow(other),
            })?;
        tracing::debug!(
            job_id = %outcome.run_id,
            job_type = J::NAME,
            newly_submitted = outcome.newly_submitted,
            "job submitted"
        );
        Ok(JobHandle::new(
            outcome.run_id,
            outcome.job_id,
            self.clone(),
            outcome.newly_submitted,
        ))
    }
}

type DispatchFuture<'a> =
    Pin<Box<dyn Future<Output = std::result::Result<StepOutcome, StepError>> + Send + 'a>>;

/// Type-erased dispatch from a job-type header to a typed [`Job::run`].
trait ErasedHandler: Send + Sync {
    fn dispatch<'a>(&'a self, inner: Arc<Inner>, step: &'a Step) -> DispatchFuture<'a>;
}

struct TypedHandler<J: Job> {
    _marker: PhantomData<fn() -> J>,
}

impl<J: Job> ErasedHandler for TypedHandler<J> {
    fn dispatch<'a>(&'a self, inner: Arc<Inner>, step: &'a Step) -> DispatchFuture<'a> {
        Box::pin(run_typed::<J>(inner, step))
    }
}

/// Deserialize and run a single job of a known type, and record its
/// outcome.
async fn run_typed<J: Job>(
    inner: Arc<Inner>,
    step: &Step,
) -> std::result::Result<StepOutcome, StepError> {
    run_recorded(step, async {
        // A payload that does not deserialize never will: dead-letter it.
        let input: J = rmp_serde::from_slice(&step.payload).map_err(|err| {
            StepError::permanent(format!("invalid payload for job type `{}`: {err}", J::NAME))
        })?;
        let ctx = JobContext::new(inner, step);
        tracing::info!(
            job_id = %step.run_id,
            job_type = J::NAME,
            attempt = step.attempts,
            "job started"
        );
        match input.run(ctx).await {
            Ok(output) => {
                // A non-serializable output is a programming error, so a
                // retry cannot succeed: dead-letter.
                let bytes = rmp_serde::to_vec_named(&output).map_err(|err| {
                    StepError::permanent(format!(
                        "job type `{}` produced an output that failed to serialize: {err}",
                        J::NAME
                    ))
                })?;
                tracing::info!(job_id = %step.run_id, job_type = J::NAME, "job completed");
                Ok(bytes)
            }
            Err(error) => {
                let message = error.to_string();
                match input.classify(&error) {
                    ErrorKind::Permanent => {
                        tracing::warn!(
                            job_id = %step.run_id,
                            job_type = J::NAME,
                            "job failed permanently: {message}"
                        );
                        Err(StepError::permanent(message))
                    }
                    ErrorKind::Transient => {
                        tracing::warn!(
                            job_id = %step.run_id,
                            job_type = J::NAME,
                            attempt = step.attempts,
                            "job failed (transient): {message}"
                        );
                        Err(StepError::transient(message))
                    }
                }
            }
        }
    })
    .await
}

/// The step runner of the workflow runtime: routes each run's single step
/// to the handler registered for its job type.
pub(crate) struct Dispatch {
    handlers: HashMap<&'static str, Box<dyn ErasedHandler>>,
    inner: Weak<Inner>,
}

impl StepRunner for Dispatch {
    async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
        let job_type = step.headers.get(JOB_TYPE_HEADER).ok_or_else(|| {
            StepError::permanent(format!(
                "job {} is missing the `{JOB_TYPE_HEADER}` header",
                step.run_id
            ))
        })?;
        let handler = self.handlers.get(job_type.as_str()).ok_or_else(|| {
            StepError::permanent(format!("no handler registered for job type `{job_type}`"))
        })?;
        // The worker task holds the runner's shared state for as long as it
        // runs, so the upgrade fails only for a delivery in flight after
        // shutdown.
        let inner = self
            .inner
            .upgrade()
            .ok_or_else(|| StepError::transient("the job runner has shut down"))?;
        handler.dispatch(inner, step).await
    }
}

/// The orchestration service: submits jobs and spawns the worker that runs
/// them.
///
/// One runner per process: taquba is single-writer. Build it with
/// [`JobRunner::builder`], registering every job type on the builder, then
/// [`spawn`](Self::spawn) the worker. Jobs can be submitted before or after
/// spawning.
pub struct JobRunner {
    inner: Arc<Inner>,
    spawned: bool,
}

impl JobRunner {
    /// Start configuring a runner over `queue`, with job memos and outcome
    /// records persisted to `object_store`.
    ///
    /// `queue` accepts a `Queue` or an `Arc<Queue>`. `object_store` is
    /// typically the same `Arc<dyn ObjectStore>` passed to
    /// [`Queue::open`](taquba::Queue::open); records are written under
    /// [`JobRunnerBuilder::memo_prefix`], which must not overlap the path
    /// the queue's SlateDB store was opened at when the two share a store.
    pub fn builder(
        queue: impl Into<Arc<Queue>>,
        object_store: Arc<dyn ObjectStore>,
    ) -> JobRunnerBuilder {
        JobRunnerBuilder::new(queue.into(), object_store)
    }

    /// Submit a job with the queue's default options.
    ///
    /// Returns a [`JobHandle`] that can be awaited for the typed result.
    pub async fn submit<J: Job>(&self, job: J) -> Result<JobHandle<J>> {
        self.inner.submit(job, SubmitOptions::default()).await
    }

    /// Submit a job with per-submission overrides (priority, schedule, etc.).
    pub async fn submit_with<J: Job>(&self, job: J, opts: SubmitOptions) -> Result<JobHandle<J>> {
        self.inner.submit(job, opts).await
    }

    /// Spawn the worker task and return a handle for graceful shutdown.
    ///
    /// The worker claims and runs jobs concurrently (up to the configured
    /// limit) until either `shutdown` resolves or
    /// [`RunnerHandle::shutdown`] is called. In-flight jobs are allowed to
    /// finish.
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

        let token = CancellationToken::new();
        let worker_token = token.clone();
        let inner = self.inner.clone();
        let join = tokio::spawn(async move {
            let combined_shutdown = async move {
                tokio::select! {
                    _ = shutdown => {}
                    _ = worker_token.cancelled() => {}
                }
            };
            inner.runtime.run(combined_shutdown).await
        });
        RunnerHandle { token, join }
    }
}

/// A handle to a spawned [`JobRunner`]'s worker task.
///
/// Dropping a `RunnerHandle` does not stop the worker: the spawned task
/// continues until the `shutdown` future passed to [`JobRunner::spawn`]
/// resolves. Call [`shutdown`](Self::shutdown) or [`wait`](Self::wait) to
/// stop or join the worker explicitly.
pub struct RunnerHandle {
    token: CancellationToken,
    join: JoinHandle<crate::Result<()>>,
}

impl RunnerHandle {
    /// Signal the worker to stop and wait for it to drain.
    ///
    /// Stops claiming new jobs, lets in-flight jobs finish, then returns
    /// once the worker task has exited.
    pub async fn shutdown(self) -> Result<()> {
        self.token.cancel();
        self.wait().await
    }

    /// Wait for the worker task to exit on its own (because the `shutdown`
    /// future passed to [`JobRunner::spawn`] resolved, or a claim error
    /// ended the loop).
    pub async fn wait(self) -> Result<()> {
        match self.join.await {
            Ok(result) => result.map_err(Error::from),
            Err(join_error) => std::panic::resume_unwind(join_error.into_panic()),
        }
    }
}

/// Builder for a [`JobRunner`]. Created via [`JobRunner::builder`].
pub struct JobRunnerBuilder {
    queue: Arc<Queue>,
    object_store: Arc<dyn ObjectStore>,
    queue_name: String,
    memo_prefix: Option<String>,
    handlers: HashMap<&'static str, Box<dyn ErasedHandler>>,
    state: State,
    concurrency: usize,
    poll_interval: Duration,
    retention: Option<Duration>,
    clock: Option<Arc<dyn Clock>>,
}

impl JobRunnerBuilder {
    fn new(queue: Arc<Queue>, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            queue,
            object_store,
            queue_name: DEFAULT_QUEUE_NAME.to_string(),
            memo_prefix: None,
            handlers: HashMap::new(),
            state: State::default(),
            concurrency: DEFAULT_CONCURRENCY,
            poll_interval: DEFAULT_POLL_INTERVAL,
            retention: None,
            clock: None,
        }
    }

    /// Register a job type so the worker can run it.
    ///
    /// # Panics
    ///
    /// Panics if another job type with the same [`Job::NAME`] is already
    /// registered.
    pub fn register<J: Job>(mut self) -> Self {
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

    /// The logical queue name jobs are enqueued under. Defaults to `"jobs"`.
    pub fn queue_name(mut self, name: impl Into<String>) -> Self {
        self.queue_name = name.into();
        self
    }

    /// The object-store prefix job memos and outcome records are written
    /// under. Defaults to `"{queue_name}-memo"`.
    pub fn memo_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.memo_prefix = Some(prefix.into());
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
    /// 100 ms.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Remove a job's memo and outcome record `retention` after it reaches
    /// a terminal state. When unset (default), records are retained
    /// indefinitely.
    ///
    /// Once a record is removed, [`JobHandle::fetch_result`] for that job
    /// returns `Ok(None)` and an idempotent re-submission of the same
    /// payload runs the job again. Set the window to cover the longest gap
    /// callers need between the original submission and an idempotent
    /// re-submission.
    ///
    /// [`JobHandle::fetch_result`]: crate::jobs::JobHandle::fetch_result
    pub fn retention(mut self, retention: Duration) -> Self {
        self.retention = Some(retention);
        self
    }

    /// Override the [`Clock`] the runner reads timestamps from. Defaults to
    /// the queue's clock ([`Queue::clock`]).
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Build the runner.
    pub fn build(self) -> JobRunner {
        let prefix = self
            .memo_prefix
            .unwrap_or_else(|| format!("{}-memo", self.queue_name));
        let memo_store = MemoStore::new(self.object_store.clone(), prefix.clone());
        let inner = Arc::new_cyclic(|weak: &Weak<Inner>| {
            let dispatch = Dispatch {
                handlers: self.handlers,
                inner: weak.clone(),
            };
            let mut builder = WorkflowRuntime::builder(
                self.queue.clone(),
                self.object_store,
                dispatch,
                NoopTerminalHook,
            )
            .queue_name(self.queue_name)
            .memo_prefix(prefix)
            .max_concurrent_steps(self.concurrency)
            .poll_interval(self.poll_interval);
            if let Some(retention) = self.retention {
                builder = builder.memo_retention(retention);
            }
            if let Some(clock) = self.clock {
                builder = builder.clock(clock);
            }
            Inner {
                runtime: builder.build(),
                queue: self.queue,
                memo_store,
                state: self.state,
                poll_interval: self.poll_interval,
            }
        });
        JobRunner {
            inner,
            spawned: false,
        }
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

    use crate::jobs::handle::JoinError;
    use crate::jobs::job::{ErrorKind, payload_idempotency_key};

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
    }

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

        fn idempotency_key(&self) -> Option<String> {
            Some(format!("increment:{}", self.n))
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Renewing;

    #[derive(Default)]
    struct RenewGate {
        renewed: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl Job for Renewing {
        const NAME: &'static str = "test.renewing";
        type Output = ();
        type Error = TestError;

        async fn run(&self, ctx: JobContext<'_>) -> std::result::Result<(), TestError> {
            ctx.lease()
                .ensure_at_least(Duration::from_secs(600))
                .map_err(|e| TestError(e.to_string()))?;
            let gate = ctx.state::<Arc<RenewGate>>();
            gate.renewed.notify_one();
            gate.release.notified().await;
            Ok(())
        }
    }

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

    #[derive(Serialize, Deserialize)]
    struct CountedKeyed {
        n: i64,
    }

    impl Job for CountedKeyed {
        const NAME: &'static str = "test.counted-keyed";
        type Output = i64;
        type Error = TestError;

        async fn run(&self, ctx: JobContext<'_>) -> std::result::Result<i64, TestError> {
            ctx.state::<Arc<AtomicU32>>().fetch_add(1, Ordering::SeqCst);
            Ok(self.n)
        }

        fn idempotency_key(&self) -> Option<String> {
            Some(format!("counted-keyed:{}", self.n))
        }
    }

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
                // Past the lease under virtual time; later attempts return
                // at once.
                tokio::time::sleep(Duration::from_secs(300)).await;
            }
            Ok(attempt)
        }
    }

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

    #[derive(Serialize, Deserialize)]
    struct Memoizing;

    impl Job for Memoizing {
        const NAME: &'static str = "test.memoizing";
        type Output = u32;
        type Error = TestError;

        async fn run(&self, ctx: JobContext<'_>) -> std::result::Result<u32, TestError> {
            let calls = ctx.state::<Arc<AtomicU32>>().clone();
            let value = ctx
                .memo()
                .memoized("expensive", async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, crate::Error>(7u32)
                })
                .await
                .map_err(|e| TestError(e.to_string()))?;
            ctx.effects()
                .put(b"jobs-test/marker".to_vec(), b"done".to_vec())
                .map_err(|e| TestError(e.to_string()))?;
            if ctx.attempt() == 1 {
                return Err(TestError("retry once".to_string()));
            }
            Ok(value)
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
        let opts = OpenOptions::default().default_queue_config(cfg);
        let queue = Arc::new(
            Queue::open_with_options(store.clone(), name, opts)
                .await
                .unwrap(),
        );
        (queue, store)
    }

    async fn open_queue_with_clock(
        name: &str,
        clock: MockClock,
        cfg: QueueConfig,
    ) -> (Arc<Queue>, Arc<dyn ObjectStore>) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let opts = OpenOptions::default()
            .clock(Arc::new(clock))
            .scheduler_interval(Duration::from_millis(10))
            .reaper_interval(Duration::from_millis(10))
            .default_queue_config(cfg);
        let queue = Arc::new(
            Queue::open_with_options(store.clone(), name, opts)
                .await
                .unwrap(),
        );
        (queue, store)
    }

    async fn count_jobs(queue: &Queue, status: JobStatus) -> usize {
        queue
            .list_jobs(DEFAULT_QUEUE_NAME, status, None, 100)
            .await
            .unwrap()
            .jobs
            .len()
    }

    #[tokio::test(start_paused = true)]
    async fn submit_without_idempotency_key_is_always_newly_submitted() {
        let (queue, store) = open_queue("test-no-idem").await;
        let runner = JobRunner::builder(queue, store).state("ok").build();

        let first = runner.submit(Adder { a: 1, b: 2 }).await.unwrap();
        let second = runner.submit(Adder { a: 1, b: 2 }).await.unwrap();
        assert!(first.newly_submitted());
        assert!(second.newly_submitted());
        assert_ne!(first.id(), second.id());
    }

    #[tokio::test(start_paused = true)]
    async fn submit_run_and_join_success() {
        let (queue, store) = open_queue("test-success").await;
        let mut runner = JobRunner::builder(queue, store)
            .state("ok")
            .register::<Adder>()
            .build();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(Adder { a: 2, b: 3 }).await.unwrap();
        assert_eq!(job.join().await.unwrap().unwrap(), 5);
        let awaited = runner.submit(Adder { a: 10, b: 7 }).await.unwrap();
        assert_eq!(awaited.await.unwrap(), 17);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_handler_uses_its_memo_and_effects_across_a_retry() {
        let cfg = QueueConfig::default()
            .max_attempts(3)
            .retry_backoff_base(Duration::ZERO);
        let (queue, store) = open_queue_with_config("test-memo", cfg).await;
        let calls = Arc::new(AtomicU32::new(0));
        let mut runner = JobRunner::builder(queue.clone(), store)
            .state(calls.clone())
            .register::<Memoizing>()
            .build();
        let handle = runner.spawn(std::future::pending::<()>());

        let value = runner.submit(Memoizing).await.unwrap().await.unwrap();

        assert_eq!(value, 7);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            queue.kv_get(b"jobs-test/marker").await.unwrap().as_deref(),
            Some(&b"done"[..]),
        );

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_handler_extends_its_lease_through_the_context() {
        let base = 1_700_000_000_000;
        let (queue, store) =
            open_queue_with_clock("test-renew", MockClock::new(base), QueueConfig::default()).await;
        let gate = Arc::new(RenewGate::default());
        let mut runner = JobRunner::builder(queue.clone(), store)
            .state(gate.clone())
            .register::<Renewing>()
            .build();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(Renewing).await.unwrap();
        gate.renewed.notified().await;
        let claimed = queue
            .list_jobs(DEFAULT_QUEUE_NAME, JobStatus::Claimed, None, 10)
            .await
            .unwrap()
            .jobs;
        assert_eq!(claimed.len(), 1);
        let expiry = queue
            .lease_expiry(DEFAULT_QUEUE_NAME, &claimed[0].id)
            .expect("a running delivery holds a lease");
        assert!(
            expiry >= base + 600_000,
            "the extension must reach the lease registry",
        );
        gate.release.notify_one();
        job.await.unwrap();

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_failure_is_dead_lettered_with_recorded_outcome() {
        let (queue, store) = open_queue("test-failure").await;
        let mut runner = JobRunner::builder(queue.clone(), store)
            .register::<AlwaysFails>()
            .build();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(AlwaysFails).await.unwrap();
        match job.clone().await {
            Err(JoinError::Job(error)) => {
                assert_eq!(error.kind, ErrorKind::Permanent);
                assert!(error.message.contains("nope"));
            }
            other => panic!("expected JoinError::Job, got {other:?}"),
        }
        assert_eq!(count_jobs(&queue, JobStatus::Dead).await, 1);
        assert!(job.status().await.is_none());

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn idempotency_key_collapses_duplicate_submissions() {
        let (queue, store) = open_queue("test-idempotency").await;
        // No spawn: jobs stay pending so the run is still active.
        let runner = JobRunner::builder(queue, store).build();

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
    async fn a_duplicate_submission_joins_the_in_flight_job() {
        let (queue, store) = open_queue("test-dup-join").await;
        let runs = Arc::new(AtomicU32::new(0));
        let mut runner = JobRunner::builder(queue, store)
            .state(runs.clone())
            .register::<CountedKeyed>()
            .build();

        let first = runner.submit(CountedKeyed { n: 3 }).await.unwrap();
        let second = runner.submit(CountedKeyed { n: 3 }).await.unwrap();
        assert!(!second.newly_submitted());
        let handle = runner.spawn(std::future::pending::<()>());

        assert_eq!(second.await.unwrap(), 3);
        assert_eq!(first.await.unwrap(), 3);
        assert_eq!(runs.load(Ordering::SeqCst), 1);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn input_mismatch_on_same_key_different_payload() {
        let (queue, store) = open_queue("test-mismatch").await;
        let runner = JobRunner::builder(queue, store).build();

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

        {
            let queue = Arc::new(Queue::open(store.clone(), queue_name).await.unwrap());
            let runner = JobRunner::builder(queue.clone(), store.clone()).build();
            runner
                .submit(FixedKey {
                    content: "alpha".into(),
                })
                .await
                .unwrap();
        }

        let queue = Arc::new(Queue::open(store.clone(), queue_name).await.unwrap());
        let runner = JobRunner::builder(queue, store).build();
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
        let mut runner = JobRunner::builder(queue, store).register::<Keyed>().build();
        let handle = runner.spawn(std::future::pending::<()>());

        let first = runner.submit(Keyed { n: 42 }).await.unwrap();
        assert!(first.newly_submitted());
        let first_id = first.id().to_string();
        assert_eq!(first.await.unwrap(), 42);

        let second = runner.submit(Keyed { n: 42 }).await.unwrap();
        assert!(!second.newly_submitted());
        assert_eq!(second.id(), first_id);
        assert_eq!(second.await.unwrap(), 42);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn idempotency_key_short_circuits_to_cached_failure_after_completion() {
        let (queue, store) = open_queue_with_config(
            "test-cached-failure",
            QueueConfig::default().max_attempts(1),
        )
        .await;
        let mut runner = JobRunner::builder(queue, store)
            .register::<KeyedFailure>()
            .build();
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

        let first_id = {
            let queue = Arc::new(Queue::open(store.clone(), queue_name).await.unwrap());
            let mut runner = JobRunner::builder(queue.clone(), store.clone())
                .register::<Keyed>()
                .build();
            let handle = runner.spawn(std::future::pending::<()>());

            let job = runner.submit(Keyed { n: 99 }).await.unwrap();
            let id = job.id().to_string();
            assert_eq!(job.await.unwrap(), 99);

            handle.shutdown().await.unwrap();
            id
        };

        let queue = Arc::new(Queue::open(store.clone(), queue_name).await.unwrap());
        let runner = JobRunner::builder(queue, store).build();
        let second = runner.submit(Keyed { n: 99 }).await.unwrap();
        assert!(!second.newly_submitted());
        assert_eq!(second.id(), first_id);
        let outcome = second
            .fetch_result()
            .await
            .unwrap()
            .expect("cached result should be reachable across restart");
        assert_eq!(outcome.unwrap(), 99);
        assert_eq!(second.await.unwrap(), 99);
    }

    #[tokio::test(start_paused = true)]
    async fn idempotent_resubmit_after_result_swept_reruns() {
        let queue_name = "test-resubmit-after-sweep";
        let (queue, store) = open_queue(queue_name).await;
        let runs = Arc::new(AtomicU32::new(0));
        let mut runner = JobRunner::builder(queue, store.clone())
            .queue_name(queue_name)
            .state(runs.clone())
            .register::<CountedKeyed>()
            .build();
        let handle = runner.spawn(std::future::pending::<()>());

        let first = runner.submit(CountedKeyed { n: 5 }).await.unwrap();
        assert!(first.newly_submitted());
        let first_id = first.id().to_string();
        assert_eq!(first.await.unwrap(), 5);
        assert_eq!(runs.load(Ordering::SeqCst), 1);

        // The retention sweep removing the outcome record.
        MemoStore::new(store, format!("{queue_name}-memo"))
            .clear_memos_for_run(&first_id)
            .await
            .unwrap();

        // The re-submission finds no outcome record and runs the job
        // again under the same id.
        let second = runner.submit(CountedKeyed { n: 5 }).await.unwrap();
        assert!(second.newly_submitted());
        assert_eq!(second.id(), first_id);
        assert_eq!(second.await.unwrap(), 5);
        assert_eq!(runs.load(Ordering::SeqCst), 2);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn retention_removes_the_outcome_record() {
        let t0 = 1_700_000_000_000;
        let clock = MockClock::new(t0);
        let (queue, store) =
            open_queue_with_clock("test-retention", clock.clone(), QueueConfig::default()).await;
        let mut runner = JobRunner::builder(queue, store)
            .state("ok")
            .retention(Duration::from_secs(60))
            .register::<Adder>()
            .build();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(Adder { a: 1, b: 1 }).await.unwrap();
        assert_eq!(job.clone().await.unwrap(), 2);
        assert!(job.fetch_result().await.unwrap().is_some());

        clock.advance(Duration::from_secs(61));
        tokio::time::advance(Duration::from_secs(61)).await;
        for _ in 0..50 {
            if job.fetch_result().await.unwrap().is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(job.fetch_result().await.unwrap().is_none());

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_job_type_is_dead_lettered() {
        let (queue, store) = open_queue("test-unknown").await;
        let mut runner = JobRunner::builder(queue.clone(), store).build();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(Keyed { n: 9 }).await.unwrap();
        let outcome = job.join().await.unwrap();
        let error = outcome.unwrap_err();
        assert!(error.message.contains("no handler registered"));
        assert_eq!(count_jobs(&queue, JobStatus::Dead).await, 1);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn reserved_header_in_submit_options_is_rejected() {
        let (queue, store) = open_queue("test-reserved-header").await;
        let runner = JobRunner::builder(queue, store).build();

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
        let cfg = QueueConfig::default()
            .max_attempts(2)
            .retry_backoff_base(Duration::ZERO);
        let (queue, store) = open_queue_with_config("test-transient-exhaust", cfg).await;
        let mut runner = JobRunner::builder(queue.clone(), store)
            .register::<AlwaysFailsTransient>()
            .build();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(AlwaysFailsTransient).await.unwrap();
        let error = job.join().await.unwrap().unwrap_err();

        assert_eq!(error.kind, ErrorKind::Transient);
        assert!(error.message.contains("flaky"));
        assert_eq!(count_jobs(&queue, JobStatus::Dead).await, 1);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn await_after_the_run_terminated_reads_the_outcome_record() {
        let (queue, store) = open_queue("test-notfound-fallback").await;
        let mut runner = JobRunner::builder(queue, store)
            .state("ok")
            .register::<Adder>()
            .build();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(Adder { a: 11, b: 31 }).await.unwrap();
        // Long enough for the worker to claim, run and ack the job before
        // the wait starts.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(job.status().await.is_none());

        assert_eq!(job.await.unwrap(), 42);

        handle.shutdown().await.unwrap();
    }

    #[test]
    fn payload_idempotency_key_is_stable_and_distinguishes_payloads() {
        let same_a = payload_idempotency_key(&Keyed { n: 7 }).unwrap();
        let same_b = payload_idempotency_key(&Keyed { n: 7 }).unwrap();
        assert_eq!(same_a, same_b);

        let different = payload_idempotency_key(&Keyed { n: 8 }).unwrap();
        assert_ne!(same_a, different);

        assert!(same_a.starts_with(&format!("{}:", Keyed::NAME)));
        let hex_part = same_a.split_once(':').unwrap().1;
        assert_eq!(hex_part.len(), 64);
        assert!(hex_part.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[tokio::test(start_paused = true)]
    async fn fan_out_from_handler_runs_children() {
        let cfg = QueueConfig::default()
            .lease_duration(Duration::from_secs(300))
            .max_attempts(1)
            .retry_backoff_base(Duration::ZERO);
        let (queue, store) = open_queue_with_config("test-fanout", cfg).await;
        let counter = Arc::new(AtomicU32::new(0));
        let mut runner = JobRunner::builder(queue, store)
            .state(counter.clone())
            .register::<Coordinator>()
            .register::<Increment>()
            .build();
        let handle = runner.spawn(std::future::pending::<()>());

        runner
            .submit(Coordinator { children: 3 })
            .await
            .unwrap()
            .await
            .unwrap();

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
        let mut runner = JobRunner::builder(queue.clone(), store)
            .state("ok")
            .register::<Adder>()
            .build();
        let handle = runner.spawn(std::future::pending::<()>());

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
        assert_eq!(count_jobs(&queue, JobStatus::Scheduled).await, 1);

        clock.advance(Duration::from_secs(120));

        assert_eq!(job.await.unwrap(), 12);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn lease_expiry_triggers_reaper_requeue() {
        let t0_ms = 1_700_000_000_000_u64;
        let clock = MockClock::new(t0_ms);
        let cfg = QueueConfig::default()
            .lease_duration(Duration::from_secs(10))
            .max_attempts(5)
            .retry_backoff_base(Duration::ZERO);
        let (queue, store) = open_queue_with_clock("test-lease", clock.clone(), cfg).await;
        let attempts = Arc::new(AtomicU32::new(0));
        let mut runner = JobRunner::builder(queue, store)
            .state(attempts.clone())
            .register::<Reclaimable>()
            .build();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(Reclaimable).await.unwrap();

        // Past the worker's poll interval, so the first claim has happened.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        clock.advance(Duration::from_secs(30));

        let attempt = job.await.unwrap();
        assert_eq!(attempt, 2);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        // The first handler is still in its virtual sleep; a graceful
        // shutdown would wait for it.
        drop(handle);
    }
}
