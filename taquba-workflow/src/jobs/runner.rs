use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::{RunSpec, RunnerHandle, Step, StepError, StepOutcome, StepRunner, WorkflowRuntime};
use std::sync::atomic::{AtomicBool, Ordering};
use taquba::WaitOutcome;
use taquba::object_store::ObjectStore;
use taquba::{Clock, Queue};

use crate::Result;
use crate::group::RunGroup;
use crate::jobs::context::{JobContext, State};
use crate::jobs::group::JobGroup;
use crate::jobs::handle::JobHandle;
use crate::jobs::job::Job;
use crate::keys::{hash_input, hex_sha256};
use crate::runtime::RunResult;
use crate::terminal::NoopTerminalHook;

/// The payload of a job's run: the job's [`Job::NAME`], by which the step
/// runner routes the run to the registered handler, and the serialized
/// job.
#[derive(Serialize, Deserialize)]
struct JobPayload {
    name: String,
    input: Vec<u8>,
}

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
    /// Extra headers to attach to the job. Keys must not start with the
    /// runtime's reserved `workflow.` prefix.
    pub headers: HashMap<String, String>,
}

/// The state shared by the runner and every [`JobHandle`]: the runtime
/// a job runs as one step of, whose terminal hook is
/// [`NoopTerminalHook`], so a job enqueues no notification.
pub(crate) struct Inner {
    pub(crate) runtime: WorkflowRuntime<Dispatch, NoopTerminalHook>,
    spawned: AtomicBool,
}

/// How a job ended, as observed by an in-process waiter: with its run
/// result record, or without one.
pub(crate) enum Terminal {
    Recorded(RunResult),
    /// The run ended without the worker recording a result: it was
    /// cancelled while pending, dead-lettered outside the worker or its
    /// records are gone.
    Unrecorded(Unrecorded),
}

pub(crate) enum Unrecorded {
    /// The queue job was acknowledged.
    Done,
    /// The queue job was dead-lettered outside the worker, with the
    /// queue record's last error.
    Dead(Option<String>),
    /// The queue job was removed by a cancellation before it was claimed.
    Cancelled,
    /// Neither a queue record nor a result record exists.
    NotFound,
}

impl Inner {
    /// The run result record of `run_id`, if one exists.
    pub(crate) async fn run_result(&self, run_id: &str) -> Result<Option<RunResult>> {
        self.runtime.inner.core.run_result(run_id).await
    }

    /// The group named `id`; see [`WorkflowRuntime::group`].
    pub(crate) fn group(
        &self,
        id: impl Into<String>,
    ) -> Result<RunGroup<'_, Dispatch, NoopTerminalHook>> {
        self.runtime.group(id)
    }

    /// A group with a generated id; see [`WorkflowRuntime::new_group`].
    pub(crate) fn new_group(&self) -> RunGroup<'_, Dispatch, NoopTerminalHook> {
        self.runtime.new_group()
    }

    /// Spawn the worker. Panics on a second call: the runtime is
    /// single-writer and its runner spawns one worker.
    pub(crate) fn spawn_once<F>(&self, shutdown: F) -> RunnerHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        assert!(
            !self.spawned.swap(true, Ordering::SeqCst),
            "spawn may only be called once"
        );
        self.runtime.spawn(shutdown)
    }

    /// Wait up to `timeout` for the run `run_id`, whose step job is
    /// `job_id`, to reach a terminal state, then read its result record.
    /// Returns `Ok(None)` when the timeout elapses first.
    pub(crate) async fn wait_terminal_within(
        &self,
        run_id: &str,
        job_id: &str,
        timeout: Duration,
    ) -> Result<Option<Terminal>> {
        let queue = &self.runtime.inner.core.queue;
        match queue.wait_for_completion_timeout(job_id, timeout).await? {
            Some(outcome) => Ok(Some(self.terminal(run_id, outcome).await?)),
            None => Ok(None),
        }
    }

    /// [`Self::wait_terminal_within`] without a bound.
    pub(crate) async fn wait_terminal(&self, run_id: &str, job_id: &str) -> Result<Terminal> {
        let outcome = self
            .runtime
            .inner
            .core
            .queue
            .wait_for_completion(job_id)
            .await?;
        self.terminal(run_id, outcome).await
    }

    async fn terminal(&self, run_id: &str, outcome: WaitOutcome) -> Result<Terminal> {
        let unrecorded = match outcome {
            WaitOutcome::Done(_) => Unrecorded::Done,
            WaitOutcome::Dead(record) => Unrecorded::Dead(record.last_error),
            WaitOutcome::Cancelled => Unrecorded::Cancelled,
            WaitOutcome::NotFound => Unrecorded::NotFound,
        };
        Ok(match self.run_result(run_id).await? {
            Some(result) => Terminal::Recorded(result),
            None => Terminal::Unrecorded(unrecorded),
        })
    }
}

/// The run payload of `job`: its [`Job::NAME`] and its serialized
/// fields.
pub(crate) fn job_payload<J: Job>(job: &J) -> Result<Vec<u8>> {
    Ok(rmp_serde::to_vec_named(&JobPayload {
        name: J::NAME.to_string(),
        input: rmp_serde::to_vec_named(job)?,
    })?)
}

impl Inner {
    pub(crate) async fn submit<J: Job>(
        self: &Arc<Self>,
        job: J,
        opts: SubmitOptions,
    ) -> Result<JobHandle<J>> {
        let payload = job_payload(&job)?;
        let key = job.idempotency_key();
        let run_id = key.as_deref().map(run_id_for_key);

        // A completed job with this key answers from its result record,
        // which outlives the run record the workflow deletes at
        // termination.
        if let Some(run_id) = &run_id
            && let Some(result) = self.run_result(run_id).await?
        {
            if result.input_hash != hash_input(&payload) {
                return Err(crate::Error::InputMismatch(run_id.clone()));
            }
            tracing::debug!(job_id = %run_id, job_type = J::NAME, "submit matched a completed job");
            return Ok(JobHandle::new(run_id.clone(), None, self.clone(), false));
        }

        let outcome = self
            .runtime
            .submit(RunSpec {
                run_id,
                input: payload,
                headers: opts.headers,
                priority: opts.priority,
                max_attempts_per_step: opts.max_attempts.or_else(|| job.max_attempts()),
                run_at: opts.run_at,
                kv_writes: HashMap::new(),
            })
            .await?;
        tracing::debug!(
            job_id = %outcome.run_id,
            job_type = J::NAME,
            newly_submitted = outcome.newly_submitted,
            "job submitted"
        );
        Ok(JobHandle::new(
            outcome.run_id,
            Some(outcome.job_id),
            self.clone(),
            outcome.newly_submitted,
        ))
    }
}

type DispatchFuture<'a> =
    Pin<Box<dyn Future<Output = std::result::Result<StepOutcome, StepError>> + Send + 'a>>;

/// Type-erased dispatch from a job name to a typed [`Job::run`] over the
/// serialized job.
trait ErasedHandler: Send + Sync {
    fn dispatch<'a>(
        &'a self,
        state: &'a State,
        step: &'a Step,
        input: Vec<u8>,
    ) -> DispatchFuture<'a>;
}

struct TypedHandler<J: Job> {
    _marker: PhantomData<fn() -> J>,
}

impl<J: Job> ErasedHandler for TypedHandler<J> {
    fn dispatch<'a>(
        &'a self,
        state: &'a State,
        step: &'a Step,
        input: Vec<u8>,
    ) -> DispatchFuture<'a> {
        Box::pin(run_typed::<J>(state, step, input))
    }
}

/// Run a single job of a known type: decode `input`, the serialized
/// job carried in the step's payload, run it and encode its output as
/// the step's result. An input that does not decode and an output that
/// does not encode are permanent errors, since a retry cannot change
/// either.
async fn run_typed<J: Job>(
    state: &State,
    step: &Step,
    input: Vec<u8>,
) -> std::result::Result<StepOutcome, StepError> {
    let input: J = rmp_serde::from_slice(&input)
        .map_err(|err| StepError::permanent(format!("invalid input for `{}`: {err}", J::NAME)))?;
    let output = {
        let ctx = JobContext::new(state, &step.delivery);
        tracing::info!(
            job_id = %step.run_id,
            job_type = J::NAME,
            attempt = step.attempts,
            "job started"
        );
        match input.run(ctx).await {
            Ok(output) => {
                tracing::info!(job_id = %step.run_id, job_type = J::NAME, "job completed");
                output
            }
            Err(error) => {
                let message = error.to_string();
                let kind = input.classify(&error);
                tracing::warn!(
                    job_id = %step.run_id,
                    job_type = J::NAME,
                    attempt = step.attempts,
                    "job failed ({kind:?}): {message}"
                );
                return Err(StepError { message, kind });
            }
        }
    };
    let result = rmp_serde::to_vec_named(&output).map_err(|err| {
        StepError::permanent(format!(
            "`{}` produced an output that failed to serialize: {err}",
            J::NAME
        ))
    })?;
    Ok(StepOutcome::Succeed { result })
}

/// The step runner of the workflow runtime: routes each run's single step
/// to the handler registered for its job type, with the registered
/// application state.
pub(crate) struct Dispatch {
    handlers: HashMap<&'static str, Box<dyn ErasedHandler>>,
    state: State,
}

impl StepRunner for Dispatch {
    async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
        let JobPayload { name, input } = rmp_serde::from_slice(&step.payload).map_err(|err| {
            StepError::permanent(format!(
                "job {} has a malformed payload: {err}",
                step.run_id
            ))
        })?;
        let handler = self.handlers.get(name.as_str()).ok_or_else(|| {
            StepError::permanent(format!("no handler registered for job type `{name}`"))
        })?;
        handler.dispatch(&self.state, step, input).await
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

    /// The group of `J` jobs named `id`, which must be 1 to 128 bytes of
    /// `[A-Za-z0-9_-]`; [`Error::InvalidGroupId`](crate::Error::InvalidGroupId)
    /// otherwise.
    pub fn group<J: Job>(&self, id: impl Into<String>) -> Result<JobGroup<J>> {
        let id = self.inner.group(id)?.id().to_string();
        Ok(JobGroup::new(self.inner.clone(), id))
    }

    /// A group of `J` jobs with a generated id.
    pub fn new_group<J: Job>(&self) -> JobGroup<J> {
        let id = self.inner.new_group().id().to_string();
        JobGroup::new(self.inner.clone(), id)
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
        self.inner.spawn_once(shutdown)
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
    group_retention: Option<Duration>,
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
            group_retention: None,
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

    /// The object-store prefix job memos and run result records are written
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

    /// Remove a job's memo and run result record `retention` after it reaches
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

    /// Remove a job group's state (its manifest, member records and the
    /// memo entries and run result records of its members) `retention`
    /// after a [`JobGroup::results`] or [`JobGroup::join`] consumer
    /// observed the last member's termination; see
    /// [`WorkflowRuntimeBuilder::group_retention`](crate::WorkflowRuntimeBuilder::group_retention).
    /// When unset (default), a group is retained until
    /// [`JobGroup::forget`].
    ///
    /// # Panics
    ///
    /// [`build`](Self::build) panics if `retention < 1ms`.
    pub fn group_retention(mut self, retention: Duration) -> Self {
        self.group_retention = Some(retention);
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
        let memo_prefix = self
            .memo_prefix
            .unwrap_or_else(|| format!("{}-memo", self.queue_name));
        let dispatch = Dispatch {
            handlers: self.handlers,
            state: self.state,
        };
        let mut builder =
            WorkflowRuntime::builder(self.queue, self.object_store, dispatch, NoopTerminalHook)
                .queue_name(self.queue_name)
                .memo_prefix(memo_prefix)
                .max_concurrent_steps(self.concurrency)
                .poll_interval(self.poll_interval);
        if let Some(clock) = self.clock {
            builder = builder.clock(clock);
        }
        if let Some(retention) = self.retention {
            builder = builder.memo_retention(retention);
        }
        if let Some(retention) = self.group_retention {
            builder = builder.group_retention(retention);
        }
        JobRunner {
            inner: Arc::new(Inner {
                runtime: builder.build(),
                spawned: AtomicBool::new(false),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{Error, MemoStore};

    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use serde::{Deserialize, Serialize};
    use taquba::object_store::{ObjectStore, memory::InMemory};
    use taquba::{JobStatus, OpenOptions, Queue, QueueConfig};

    use crate::StepErrorKind;
    use crate::jobs::handle::JoinError;
    use crate::jobs::job::payload_idempotency_key;
    use crate::test_util::{fast_options, open_queue, open_queue_at_with, open_queue_with};

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

        fn classify(&self, _error: &TestError) -> StepErrorKind {
            StepErrorKind::Permanent
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
            ctx.lease
                .ensure_at_least(Duration::from_secs(600))
                .map_err(|e| TestError(e.to_string()))?;
            let gate = ctx.state::<Arc<RenewGate>>();
            gate.renewed.notify_one();
            gate.release.notified().await;
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
            if ctx.attempts == 1 {
                // Past the lease under virtual time; later attempts return
                // at once.
                tokio::time::sleep(Duration::from_secs(300)).await;
            }
            Ok(ctx.attempts)
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

        fn classify(&self, _error: &TestError) -> StepErrorKind {
            StepErrorKind::Permanent
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
                .memo
                .memoized("expensive", async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, crate::Error>(7u32)
                })
                .await
                .map_err(|e| TestError(e.to_string()))?;
            ctx.effects
                .put(b"jobs-test/marker".to_vec(), b"done".to_vec())
                .map_err(|e| TestError(e.to_string()))?;
            if ctx.attempts == 1 {
                return Err(TestError("retry once".to_string()));
            }
            Ok(value)
        }
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
        let (queue, store) = open_queue().await;
        let runner = JobRunner::builder(queue, store).state("ok").build();

        let first = runner.submit(Adder { a: 1, b: 2 }).await.unwrap();
        let second = runner.submit(Adder { a: 1, b: 2 }).await.unwrap();
        assert!(first.newly_submitted());
        assert!(second.newly_submitted());
        assert_ne!(first.id(), second.id());
    }

    #[tokio::test(start_paused = true)]
    async fn a_handler_uses_its_memo_and_effects_across_a_retry() {
        let cfg = QueueConfig::default()
            .max_attempts(3)
            .retry_backoff_base(Duration::ZERO);
        let (queue, store) =
            open_queue_with(OpenOptions::default().default_queue_config(cfg)).await;
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
        let (queue, store, _clock) = open_queue_at_with(base, fast_options()).await;
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
        let (queue, store) = open_queue().await;
        let mut runner = JobRunner::builder(queue.clone(), store)
            .register::<AlwaysFails>()
            .build();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(AlwaysFails).await.unwrap();
        match job.clone().await {
            Err(JoinError::Job(error)) => {
                assert_eq!(error.kind, StepErrorKind::Permanent);
                assert!(error.message.contains("nope"));
            }
            other => panic!("expected JoinError::Job, got {other:?}"),
        }
        assert_eq!(count_jobs(&queue, JobStatus::Dead).await, 1);
        assert!(job.status().await.unwrap().is_none());

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_duplicate_submission_joins_the_in_flight_job() {
        let (queue, store) = open_queue().await;
        let runs = Arc::new(AtomicU32::new(0));
        let mut runner = JobRunner::builder(queue, store)
            .state(runs.clone())
            .register::<CountedKeyed>()
            .build();

        // No worker yet: the jobs stay pending, so the runs are active.
        let first = runner.submit(CountedKeyed { n: 3 }).await.unwrap();
        assert!(first.newly_submitted());
        let second = runner.submit(CountedKeyed { n: 3 }).await.unwrap();
        assert!(!second.newly_submitted());
        assert_eq!(first.id(), second.id());
        let different = runner.submit(CountedKeyed { n: 4 }).await.unwrap();
        assert!(different.newly_submitted());
        assert_ne!(first.id(), different.id());
        let handle = runner.spawn(std::future::pending::<()>());

        assert_eq!(second.await.unwrap(), 3);
        assert_eq!(first.await.unwrap(), 3);
        assert_eq!(different.await.unwrap(), 4);
        assert_eq!(runs.load(Ordering::SeqCst), 2);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn input_mismatch_names_the_run_id_and_survives_restart() {
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
            Err(Error::InputMismatch(id)) => assert_eq!(id, run_id_for_key("fixed")),
            Err(other) => panic!("expected InputMismatch across restart, got Err({other:?})"),
            Ok(_) => panic!("expected InputMismatch across restart, got Ok(_)"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn idempotency_key_short_circuits_to_cached_failure_after_completion() {
        let (queue, store) = open_queue_with(
            OpenOptions::default().default_queue_config(QueueConfig::default().max_attempts(1)),
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
            Err(JoinError::Job(job_err)) => assert_eq!(job_err.kind, StepErrorKind::Permanent),
            other => panic!("expected Permanent JobError, got {other:?}"),
        }

        let second = runner.submit(KeyedFailure { n: 7 }).await.unwrap();
        assert!(!second.newly_submitted());
        assert_eq!(second.id(), first_id);
        match second.await {
            Err(JoinError::Job(job_err)) => assert_eq!(job_err.kind, StepErrorKind::Permanent),
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
        let (queue, store) = open_queue().await;
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

        // The retention sweep removing the run result record.
        MemoStore::new(store, format!("{queue_name}-memo"))
            .clear_memos_for_run(&first_id)
            .await
            .unwrap();

        // The re-submission finds no run result record and runs the job
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
        let (queue, store, clock) = open_queue_at_with(t0, fast_options()).await;
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
        let (queue, store) = open_queue().await;
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
    async fn transient_failure_exhausts_retries_and_dead_letters() {
        let cfg = QueueConfig::default()
            .max_attempts(2)
            .retry_backoff_base(Duration::ZERO);
        let (queue, store) =
            open_queue_with(OpenOptions::default().default_queue_config(cfg)).await;
        let mut runner = JobRunner::builder(queue.clone(), store)
            .register::<AlwaysFailsTransient>()
            .build();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(AlwaysFailsTransient).await.unwrap();
        let error = job.join().await.unwrap().unwrap_err();

        assert_eq!(error.kind, StepErrorKind::Transient);
        assert!(error.message.contains("flaky"));
        assert_eq!(count_jobs(&queue, JobStatus::Dead).await, 1);

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn await_after_the_run_terminated_reads_the_outcome_record() {
        let (queue, store) = open_queue().await;
        let mut runner = JobRunner::builder(queue, store)
            .state("ok")
            .register::<Adder>()
            .build();
        let handle = runner.spawn(std::future::pending::<()>());

        let job = runner.submit(Adder { a: 11, b: 31 }).await.unwrap();
        // Long enough for the worker to claim, run and ack the job before
        // the wait starts.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(job.status().await.unwrap().is_none());

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
    async fn scheduled_job_runs_when_clock_passes_run_at() {
        let t0_ms = 1_700_000_000_000_u64;
        let (queue, store, clock) = open_queue_at_with(t0_ms, fast_options()).await;
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
        let cfg = QueueConfig::default()
            .lease_duration(Duration::from_secs(10))
            .max_attempts(5)
            .retry_backoff_base(Duration::ZERO);
        let (queue, store, clock) =
            open_queue_at_with(t0_ms, fast_options().default_queue_config(cfg)).await;
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
