use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use taquba::object_store::ObjectStore;
use taquba::{
    Clock, EnqueueOptions, EnqueueRequest, EnqueueResult, FailWith, JobRecord, LeaseHandle,
    PermanentFailure, Queue, SettlementEffects, Worker, WorkerError,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, warn};

use crate::durable::{
    DurableCurrentStep, DurableRunOutcome, DurableRunRecord, DurableStepOutcome,
    DurableStepOutcomeRecord,
};
use crate::effects::{EffectsHandle, StagedEffects, TerminalEffects};
use crate::error::{Error, Result};
use crate::keys::{
    DEDUP_PREFIX, HEADER_RUN_ID, HEADER_STEP, HEADER_TERMINAL, RESERVED_HEADER_PREFIX,
    RESERVED_KV_PREFIX, TERMINAL_KV_PREFIX, hash_input, run_kv_key, step_kv_key, terminal_kv_key,
    validate_run_id,
};
use crate::kv::KvReadHandle;
use crate::memo::MemoStore;
use crate::registry::RunRegistry;
use crate::runner::{Step, StepError, StepErrorKind, StepOutcome, StepRunner, Trigger};
use crate::sweep::Sweep;
use crate::terminal::{RunOutcome, TerminalHook};

/// The encoded current-step pointer for `job_id` at `step_number`.
fn current_step_bytes(step_number: u32, job_id: &str) -> Result<Vec<u8>> {
    Ok(rmp_serde::to_vec_named(&DurableCurrentStep {
        step_number,
        job_id: job_id.to_string(),
    })?)
}

/// The portion of `delay` still ahead of `now_ms`, measured from
/// `stored_at_ms`. Saturates to the full delay if the clock reads
/// earlier than the stored timestamp.
fn remaining_delay(stored_at_ms: u64, now_ms: u64, delay: Duration) -> Duration {
    let elapsed = Duration::from_millis(now_ms.saturating_sub(stored_at_ms));
    delay.saturating_sub(elapsed)
}

/// Per-step enqueue options the runtime forwards through to Taquba. The
/// runtime always owns `headers` (it injects [`HEADER_RUN_ID`] and
/// [`HEADER_STEP`]) and `dedup_key` (it derives one from
/// `(run_id, step_number)`), so callers only pick the three fields below.
#[derive(Debug, Default)]
pub(crate) struct StepEnqueueOpts {
    /// Earliest claimable time for the step. `None` means immediate.
    pub(crate) run_at: Option<SystemTime>,
    /// Per-step priority override.
    pub(crate) priority: Option<u32>,
    /// Per-step `max_attempts` override.
    pub(crate) max_attempts: Option<u32>,
    /// Additional runtime-owned reserved headers for the step job.
    pub(crate) reserved_headers: Vec<(&'static str, String)>,
}

/// Spec passed to [`WorkflowRuntime::submit`].
#[derive(Debug, Clone, Default)]
pub struct RunSpec {
    /// Caller-supplied run identifier of 1 to [`MAX_RUN_ID_LEN`](crate::MAX_RUN_ID_LEN) bytes of
    /// `[A-Za-z0-9_-]`; anything else is rejected with
    /// [`Error::InvalidRunId`]. If `None`, the runtime generates a ULID.
    /// The dedup key for the first step job is `run:{run_id}:0`, so
    /// re-submitting the same `run_id` while the run is active returns the
    /// existing job rather than creating a duplicate.
    ///
    /// A terminated run releases its id for re-submission. The second
    /// run shares the first run's memo and step-output entries, which is
    /// what makes a re-submission resume from them, and under
    /// [`WorkflowRuntimeBuilder::memo_retention`] the first run's marker
    /// expires against those shared entries even while the second run is
    /// executing. The second run then re-executes the affected steps.
    pub run_id: Option<String>,
    /// Bytes handed to the runner as the first step's payload.
    pub input: Vec<u8>,
    /// Submitter-supplied metadata, threaded through every step of the run
    /// and surfaced to the terminal hook. Reserved `workflow.*` keys are
    /// rejected at submission with [`Error::ReservedHeaderInSubmit`].
    pub headers: HashMap<String, String>,
    /// Override the queue's default priority for every step of this run.
    pub priority: Option<u32>,
    /// Override the queue's `max_attempts` for every step of this run.
    pub max_attempts_per_step: Option<u32>,
    /// Earliest time the first step may run. The step-0 job waits in the
    /// queue's scheduled state until the queue's clock passes this time;
    /// `None` makes it claimable at once.
    pub run_at: Option<SystemTime>,
    /// Writes applied to the caller KV namespace in the same transaction
    /// as the step-0 enqueue. Applied only when the submission is new: a
    /// duplicate submission's writes are dropped, and the writes do not
    /// participate in the duplicate-submission input check. Keys must
    /// not start with the reserved `workflow/` prefix
    /// ([`RESERVED_KV_PREFIX`]); values are capped at
    /// [`taquba::MAX_KV_VALUE_SIZE`].
    pub kv_writes: HashMap<Vec<u8>, Vec<u8>>,
}

/// Outcome of [`WorkflowRuntime::submit`].
///
/// `submit` is idempotent on `run_id`: re-submitting an active run is a
/// no-op and the returned `SubmitOutcome` carries `newly_submitted = false`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SubmitOutcome {
    /// The run's identifier (generated if the spec didn't carry one).
    pub run_id: String,
    /// `true` if this call enqueued a new run; `false` if a run with this
    /// id was already active (in this runtime's registry or in the
    /// durable cross-restart record) and this call was a no-op. Call
    /// [`WorkflowRuntime::status`] for the run's current state when
    /// needed.
    pub newly_submitted: bool,
    /// The id of the queue job currently representing the run: its
    /// first step for a new submission, and the step the run has reached
    /// for a duplicate, whether the run is tracked in process or known
    /// only from its durable record.
    pub job_id: String,
}

/// In-memory status snapshot for an active run. Returned by
/// [`WorkflowRuntime::status`]. Terminal runs are not retained; the
/// registry entry is removed when the run terminates.
#[derive(Debug, Clone)]
pub struct RunStatus {
    /// The run's identifier.
    pub run_id: String,
    /// Lifecycle state of the run within this runtime process.
    pub state: RunState,
    /// Step number of the most recently observed step.
    pub current_step: u32,
}

/// Lifecycle state tracked in [`RunStatus::state`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RunState {
    /// A step job exists in the queue but has not yet been claimed.
    Pending,
    /// A step is currently being processed by a worker.
    Running,
    /// [`WorkflowRuntime::cancel`] was called for this run and the run
    /// has not yet terminated. Reported until the in-flight step
    /// returns and the runtime settles the run as
    /// [`crate::TerminalStatus::Cancelled`] (entry removed); after
    /// that, [`WorkflowRuntime::status`] returns `None`.
    ///
    /// Only set by external cancellation. A pure runner-issued
    /// [`crate::StepOutcome::Cancel`] (with no external `cancel()`
    /// call) terminates as `Cancelled` without ever transitioning
    /// through `Cancelling`: a runner-issued cancel is observed when
    /// `run_step` returns, and the run terminates at that point.
    Cancelling,
}

/// Builder for [`WorkflowRuntime`].
///
/// Construct via [`WorkflowRuntime::builder`].
pub struct WorkflowRuntimeBuilder<R, H> {
    queue: Arc<Queue>,
    object_store: Arc<dyn ObjectStore>,
    queue_name: String,
    memo_prefix: String,
    runner: R,
    terminal_hook: H,
    max_concurrent_steps: usize,
    poll_interval: Duration,
    memo_retention: Option<Duration>,
    step_output_replay: bool,
    clock: Arc<dyn Clock>,
    sweeps: Vec<Sweep>,
}

impl<R: StepRunner, H: TerminalHook> WorkflowRuntimeBuilder<R, H> {
    /// The Taquba queue name that step jobs are enqueued onto. Defaults to
    /// `"workflow-steps"`. Multiple runtimes can share a `Queue` handle by
    /// using distinct queue names.
    pub fn queue_name(mut self, name: impl Into<String>) -> Self {
        self.queue_name = name.into();
        self
    }

    /// The object-store path prefix [`Step::memo`] entries live under.
    /// Defaults to `"workflow-memo"`. Pick a distinct value when multiple
    /// runtimes share an object store, so their memo namespaces don't
    /// collide.
    pub fn memo_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.memo_prefix = prefix.into();
        self
    }

    /// Maximum number of steps processed concurrently in [`WorkflowRuntime::run`].
    /// Defaults to 16.
    pub fn max_concurrent_steps(mut self, n: usize) -> Self {
        assert!(n > 0, "max_concurrent_steps must be at least 1");
        self.max_concurrent_steps = n;
        self
    }

    /// Maximum time the worker loop waits on an empty queue before re-checking.
    /// Defaults to 250ms.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Enable memo retention with the given window. When set, the
    /// runtime writes a terminal marker for every run that reaches a
    /// terminal state, and the in-process sweeper will clear that run's
    /// memo entries `retention` after termination. When unset (default),
    /// no marker is written and memo entries are retained indefinitely.
    ///
    /// Panics if `retention < 1ms`: smaller values would turn the sweep
    /// loop into a hot spin.
    pub fn memo_retention(mut self, retention: Duration) -> Self {
        assert!(
            retention >= Duration::from_millis(1),
            "memo_retention must be at least 1ms",
        );
        self.memo_retention = Some(retention);
        self
    }

    /// Enable content-addressed replay of runner-returned step outcomes.
    ///
    /// When enabled, the runtime writes every [`StepOutcome`] the runner
    /// returns, including `Fail` and `Cancel`, to object storage before
    /// applying it. Step errors ([`StepError`](crate::StepError)) are not
    /// recorded, so retries still invoke the runner. The replay key is
    /// scoped to `(run_id, step_number, SHA-256(step payload))`. If the
    /// same step is delivered again after a crash before ack, the stored
    /// outcome is replayed without invoking the runner again. The record
    /// includes the effects staged through [`Step::effects`], so a
    /// replayed outcome applies them as well. A replayed
    /// [`StepOutcome::Continue`] with a [`Trigger::After`] delay reduces
    /// the delay by the time already elapsed since the outcome was
    /// stored, preserving the original schedule.
    ///
    /// This is disabled by default because it adds one object-store read
    /// per step delivery (the replay lookup) plus one write per recorded
    /// outcome, and makes that write part of step settlement.
    pub fn step_output_replay(mut self) -> Self {
        self.step_output_replay = true;
        self
    }

    /// Override the [`Clock`] the runtime reads its timestamps from.
    /// Defaults to the same clock the [`Queue`] was opened with (via
    /// [`Queue::clock`]).
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Add a retention sweep that [`WorkflowRuntime::run`] drives beside
    /// the memo sweep, for a layer whose terminal markers live under its
    /// own prefix.
    pub(crate) fn sweep(mut self, sweep: Sweep) -> Self {
        self.sweeps.push(sweep);
        self
    }

    /// Finalize the builder.
    pub fn build(self) -> WorkflowRuntime<R, H> {
        let memo_store = MemoStore::new(self.object_store, self.memo_prefix);
        let mut sweeps = self.sweeps;
        if let Some(retention) = self.memo_retention {
            let memos = memo_store.clone();
            sweeps.push(Sweep::new(TERMINAL_KV_PREFIX, retention, move |run_id| {
                let memos = memos.clone();
                async move { memos.clear_memos_for_run(&run_id).await.map(|_| ()) }
            }));
        }
        let core = RuntimeCore {
            queue: self.queue,
            queue_name: self.queue_name,
            max_concurrent_steps: self.max_concurrent_steps,
            poll_interval: self.poll_interval,
            registry: RunRegistry::default(),
            submit_locks: std::sync::Mutex::new(HashMap::new()),
            memo_store,
            memo_retention: self.memo_retention,
            sweeps,
            step_output_replay: self.step_output_replay,
            clock: self.clock,
        };
        let inner = RuntimeInner {
            runner: self.runner,
            terminal_hook: self.terminal_hook,
            core,
        };
        WorkflowRuntime {
            inner: Arc::new(inner),
        }
    }
}

/// Durable runtime for workflow runs. Cheap to clone (internally `Arc`).
pub struct WorkflowRuntime<R, H> {
    pub(crate) inner: Arc<RuntimeInner<R, H>>,
}

impl<R, H> Clone for WorkflowRuntime<R, H> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// A runtime's [`StepRunner`] and [`TerminalHook`], with the shared
/// [`RuntimeCore`] they operate on.
pub(crate) struct RuntimeInner<R, H> {
    runner: R,
    terminal_hook: H,
    pub(crate) core: RuntimeCore,
}

/// The state every component of a runtime operates on: the queue
/// handle, the run registry, the memo store and the clock. Methods
/// that invoke the runner or the hook are on [`RuntimeInner`].
pub(crate) struct RuntimeCore {
    pub(crate) queue: Arc<Queue>,
    queue_name: String,
    max_concurrent_steps: usize,
    poll_interval: Duration,
    pub(crate) registry: RunRegistry,
    /// Per-run-id submission locks. Each lock is held across the
    /// duplicate checks and the step-0 enqueue of its run, so two
    /// concurrent submits of the same run cannot both pass the checks
    /// before either commits, while submits of distinct runs proceed
    /// concurrently. Run ids are unbounded, so entries are removed
    /// once no submit references them.
    submit_locks: std::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>,
    pub(crate) memo_store: MemoStore,
    /// Window after a run reaches a terminal state during which its
    /// memo entries are retained for replay. `None` disables retention
    /// entirely (no terminal marker is written and no memo sweep runs).
    pub(crate) memo_retention: Option<Duration>,
    /// The retention sweeps [`WorkflowRuntime::run`] drives: the memo
    /// sweep when `memo_retention` is set, and every sweep a layer
    /// registered through [`WorkflowRuntimeBuilder::sweep`].
    sweeps: Vec<Sweep>,
    /// Whether runner-returned step outcomes are persisted and replayed
    /// by `(run_id, step_number, SHA-256(step payload))`.
    step_output_replay: bool,
    /// Time source. Defaults to the queue's clock; tests can substitute
    /// a [`MockClock`](taquba::MockClock) to virtualise time.
    pub(crate) clock: Arc<dyn Clock>,
}

impl<R: StepRunner, H: TerminalHook> WorkflowRuntime<R, H> {
    /// Start configuring a runtime. Takes the four required dependencies
    /// (Taquba queue, object store, [`StepRunner`], [`TerminalHook`]); optional
    /// fields are set via [`WorkflowRuntimeBuilder`] methods before [`build`].
    ///
    /// The object store backs [`Step::memo`]; it does **not** need to be the
    /// same store the [`Queue`] was opened with, though sharing one store is
    /// the common case (just clone the `Arc`). Use a distinct
    /// [`WorkflowRuntimeBuilder::memo_prefix`] when multiple runtimes share
    /// one store.
    ///
    /// Use [`crate::NoopTerminalHook`] if you don't need terminal callbacks.
    ///
    /// [`Step::memo`]: crate::Step::memo
    /// [`build`]: WorkflowRuntimeBuilder::build
    pub fn builder(
        queue: Arc<Queue>,
        object_store: Arc<dyn ObjectStore>,
        runner: R,
        terminal_hook: H,
    ) -> WorkflowRuntimeBuilder<R, H> {
        let clock = queue.clock();
        WorkflowRuntimeBuilder {
            queue,
            object_store,
            queue_name: "workflow-steps".to_string(),
            memo_prefix: "workflow-memo".to_string(),
            runner,
            terminal_hook,
            max_concurrent_steps: 16,
            poll_interval: Duration::from_millis(250),
            memo_retention: None,
            step_output_replay: false,
            clock,
            sweeps: Vec::new(),
        }
    }

    /// Submit a new run. Enqueues step 0 with payload `spec.input`.
    ///
    /// Idempotent on `(run_id, spec.input)`: if a run with the same id is
    /// already active (either in this runtime's in-memory registry or in
    /// the durable cross-restart record written to Taquba's user KV
    /// namespace) and `spec.input` matches the original submission, this
    /// call is a no-op and the returned [`SubmitOutcome`] has
    /// `newly_submitted = false`. A re-submission of an active `run_id`
    /// with a *different* input is rejected with [`Error::InputMismatch`];
    /// pick a fresh `run_id` for a new run.
    #[instrument(skip(self, spec), fields(run_id))]
    pub async fn submit(&self, spec: RunSpec) -> Result<SubmitOutcome> {
        if let Some(supplied) = spec.run_id.as_deref() {
            validate_run_id(supplied)?;
        }
        let run_id = spec
            .run_id
            .clone()
            .unwrap_or_else(|| ulid::Ulid::new().to_string());
        tracing::Span::current().record("run_id", run_id.as_str());

        for k in spec.headers.keys() {
            if k.starts_with(RESERVED_HEADER_PREFIX) {
                return Err(Error::ReservedHeaderInSubmit(k.clone()));
            }
        }
        for key in spec.kv_writes.keys() {
            if key.starts_with(RESERVED_KV_PREFIX.as_bytes()) {
                return Err(Error::ReservedKvKey(
                    String::from_utf8_lossy(key).into_owned(),
                ));
            }
        }

        // Hold this run's submit lock across the duplicate checks and
        // the enqueue, so two concurrent submits with the same `run_id`
        // cannot both pass the checks before either commits. The lock
        // is per run id: submits of distinct runs (e.g. a bulk batch)
        // proceed concurrently and share WAL group commits instead of
        // serialising at one durable enqueue per flush interval.
        let lock = self.inner.core.submit_lock_for(&run_id);
        let result = {
            let _guard = lock.lock().await;
            self.submit_under_lock(&run_id, spec).await
        };
        drop(lock);
        self.inner.core.release_submit_lock(&run_id);
        result
    }

    async fn submit_under_lock(&self, run_id: &str, spec: RunSpec) -> Result<SubmitOutcome> {
        let input_hash = hash_input(&spec.input);

        // A worker-resumed entry stores no hash and reports `None`; fall
        // through to the durable-record check below, which always
        // includes it.
        if let Some(existing) = self.inner.core.registry.known_input_hash(run_id) {
            if existing != input_hash {
                return Err(Error::InputMismatch(run_id.to_string()));
            }
            let job_id = self
                .inner
                .core
                .registry
                .current_job_id(run_id)
                .ok_or_else(|| Error::InconsistentRunState(run_id.to_string()))?;
            return Ok(SubmitOutcome {
                run_id: run_id.to_string(),
                newly_submitted: false,
                job_id,
            });
        }

        // Cross-restart duplicate check. The submit lock above closes
        // the in-process race window; this read closes the across-restart
        // one (same queue, fresh runtime).
        if let Some(bytes) = self.inner.core.queue.kv_get(&run_kv_key(run_id)).await? {
            let existing: DurableRunRecord = rmp_serde::from_slice(&bytes)?;
            if existing.input_hash != input_hash {
                return Err(Error::InputMismatch(run_id.to_string()));
            }
            let current = self.inner.core.current_step(run_id).await?;
            return Ok(SubmitOutcome {
                run_id: run_id.to_string(),
                newly_submitted: false,
                job_id: current.job_id,
            });
        }

        let job_id = self.inner.core.queue.next_job_id();
        let mut headers = spec.headers.clone();
        headers.insert(HEADER_RUN_ID.to_string(), run_id.to_string());
        headers.insert(HEADER_STEP.to_string(), "0".to_string());
        let enqueue_opts = EnqueueOptions::default()
            .headers(headers)
            .run_at(spec.run_at)
            .priority(spec.priority)
            .max_attempts(spec.max_attempts_per_step)
            .dedup_key(Some(format!("{DEDUP_PREFIX}{run_id}:0")))
            .id_override(Some(job_id.clone()));

        let record_bytes = rmp_serde::to_vec_named(&DurableRunRecord {
            run_id: run_id.to_string(),
            submitted_at_ms: self.inner.core.clock.now_ms(),
            input_hash,
        })?;
        let mut kv = spec.kv_writes;
        kv.insert(run_kv_key(run_id), record_bytes);
        kv.insert(step_kv_key(run_id), current_step_bytes(0, &job_id)?);

        let job_id = match self
            .inner
            .core
            .queue
            .enqueue_with_kv(&self.inner.core.queue_name, spec.input, enqueue_opts, kv)
            .await?
        {
            EnqueueResult::New(id) => id,
            // A dedup_key hit without our durable record means either
            // another writer beat us, or a prior run on `(run_id, step 0)`
            // released its dedup key (job claimed) but the durable record
            // is missing, which only happens if the run terminated
            // without going through `terminate`. Either way the safe
            // verdict is duplicate.
            EnqueueResult::AlreadyEnqueued(existing) => {
                return Ok(SubmitOutcome {
                    run_id: run_id.to_string(),
                    newly_submitted: false,
                    job_id: existing,
                });
            }
        };

        self.inner.core.registry.insert_submitted(
            run_id,
            &job_id,
            spec.headers.clone(),
            input_hash,
        );

        debug!(run_id = %run_id, job_id = %job_id, "run submitted");
        Ok(SubmitOutcome {
            run_id: run_id.to_string(),
            newly_submitted: true,
            job_id,
        })
    }

    /// Look up the in-process status of a run. Returns `None` for unknown or
    /// already-terminated runs (the registry only retains active runs).
    ///
    /// Returns [`RunState::Cancelling`] for any run with a pending
    /// cancellation request, regardless of its underlying step lifecycle
    /// position; the cancellation overlay wins over `Pending`/`Running`
    /// until the run terminates.
    pub async fn status(&self, run_id: &str) -> Option<RunStatus> {
        self.inner.core.registry.status(run_id)
    }

    /// Request cancellation of an active run.
    ///
    /// Returns `Ok(true)` if a cancellation was initiated for `run_id`, or
    /// `Ok(false)` if the run is not active in this runtime (already
    /// terminal, never submitted here, or owned by a different runtime
    /// instance).
    ///
    /// The run terminates as [`TerminalStatus::Cancelled`](crate::TerminalStatus::Cancelled) and its
    /// notification job is enqueued for the terminal hook:
    ///
    /// - **Pending / scheduled step**: the queued step job is removed
    ///   and the notification enqueued in one transaction before this
    ///   call returns; the hook runs from a worker afterwards. A run
    ///   whose step is already claimed keeps its durable state until
    ///   the worker settles it.
    /// - **Running step**: cancellation is delivered to the runner via
    ///   [`Step::cancel_token`]; runners that watch the token short-circuit
    ///   immediately. Runners that ignore the token are allowed to run to
    ///   completion (futures cannot be safely aborted mid-step). In both
    ///   cases the runner's [`StepOutcome`] / [`StepError`] is discarded
    ///   and the worker settles the run once the step returns, with
    ///   any pending transient retry suppressed and the step acked rather
    ///   than nacked.
    ///
    /// Cancellation is best-effort: if the run is already terminal by the
    /// time `cancel` is called (either because the runner returned a
    /// terminating [`StepOutcome`] or a prior `cancel` already settled
    /// it), `cancel` returns `Ok(false)` and the run keeps whatever
    /// terminal outcome it already delivered.
    pub async fn cancel(&self, run_id: &str) -> Result<bool> {
        // `cancel_with` below fires the claim's cancellation token, the
        // parent of `Step::cancel_token`. A runner that does not watch
        // it runs to completion and the worker terminates the run once
        // `run_step` returns.
        let Some((job_id, headers, current_step)) = self.inner.core.registry.request_cancel(run_id)
        else {
            return Ok(false);
        };

        // `error` is `None`: external cancellation supplies no reason at
        // the API level. The effects are built before the outcome is
        // known; the queue applies them only on `Removed`.
        let effects = self.inner.terminate_collecting_effects(
            &RunOutcome::cancelled(run_id.to_string(), None, headers, current_step),
            None,
            None,
        );
        match self.inner.core.queue.cancel_with(&job_id, effects).await?.0 {
            taquba::CancelOutcome::Removed => {
                // Job was Pending/Scheduled and is now removed; no worker
                // will ever see it. The marker, the record delete and the
                // notification committed with the removal, leaving only
                // process state to remove.
                self.inner.core.forget_run(run_id);
            }
            taquba::CancelOutcome::Requested => {
                // Worker is processing the step. The worker reads our own
                // registry `cancel_requested` flag after `run_step` returns
                // and terminates the run.
            }
            taquba::CancelOutcome::NotFound => {
                // Job already gone from Taquba (e.g. just acked between our
                // registry read and the queue call). The worker path still
                // honours our `cancel_requested` flag if it hasn't terminated
                // the run yet; if it has, this cancel is a no-op past the
                // registry update.
            }
        }
        Ok(true)
    }

    /// Spawn [`Self::run`] as a Tokio task and return a handle for
    /// graceful shutdown. The worker runs until `shutdown` resolves or
    /// [`RunnerHandle::shutdown`] is called; in-flight steps finish
    /// either way.
    pub fn spawn<F>(&self, shutdown: F) -> RunnerHandle
    where
        F: Future<Output = ()> + Send + 'static,
        R: 'static,
        H: 'static,
    {
        let token = CancellationToken::new();
        let worker_token = token.clone();
        let runtime = self.clone();
        let join = tokio::spawn(async move {
            let combined_shutdown = async move {
                tokio::select! {
                    _ = shutdown => {}
                    _ = worker_token.cancelled() => {}
                }
            };
            runtime.run(combined_shutdown).await
        });
        RunnerHandle::new(token, join)
    }

    /// Drive the step worker loop until `shutdown` resolves. Spawns up
    /// to `max_concurrent_steps` step processors and, when
    /// [`WorkflowRuntimeBuilder::memo_retention`] is set, a
    /// memo-retention sweeper running in parallel. Both halt cleanly
    /// when `shutdown` resolves or the worker errors.
    pub async fn run<F>(&self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()>,
        R: 'static,
        H: 'static,
    {
        // One cancellation token fans the "stop now" signal out to the
        // worker (via `cancelled_owned`) and to the sweeper (which
        // selects on its own clone). The signal is raised either when
        // the caller's `shutdown` future fires or when the worker
        // returns on its own (typically with an error).
        let stop = CancellationToken::new();

        let sweep_handles: Vec<_> = (0..self.inner.core.sweeps.len())
            .map(|i| {
                let inner = self.inner.clone();
                let token = stop.clone();
                tokio::spawn(async move {
                    let core = &inner.core;
                    core.sweeps[i].run(&core.queue, &*core.clock, token).await;
                })
            })
            .collect();

        let worker = Arc::new(StepWorker {
            inner: self.inner.clone(),
        });
        let worker_fut = taquba::run_worker_concurrent(
            &self.inner.core.queue,
            &self.inner.core.queue_name,
            worker,
            self.inner.core.max_concurrent_steps,
            self.inner.core.poll_interval,
            stop.clone().cancelled_owned(),
        );

        let mut shutdown = std::pin::pin!(shutdown);
        let mut worker_fut = std::pin::pin!(worker_fut);
        let result = tokio::select! {
            _ = shutdown.as_mut() => {
                stop.cancel();
                worker_fut.await
            }
            res = worker_fut.as_mut() => {
                stop.cancel();
                res
            }
        };

        for handle in sweep_handles {
            let _ = handle.await;
        }

        result?;
        Ok(())
    }
}

/// A handle to a worker task spawned by [`WorkflowRuntime::spawn`].
///
/// Dropping a `RunnerHandle` does not stop the worker: the task
/// continues until the `shutdown` future passed to `spawn` resolves.
/// Call [`shutdown`](Self::shutdown) or [`wait`](Self::wait) to stop or
/// join the worker explicitly.
pub struct RunnerHandle {
    token: CancellationToken,
    join: tokio::task::JoinHandle<Result<()>>,
}

impl RunnerHandle {
    pub(crate) fn new(token: CancellationToken, join: tokio::task::JoinHandle<Result<()>>) -> Self {
        Self { token, join }
    }

    /// Signal the worker to stop and wait for it to drain: it stops
    /// claiming, lets in-flight steps finish and returns once the task
    /// has exited.
    pub async fn shutdown(self) -> Result<()> {
        self.token.cancel();
        self.wait().await
    }

    /// Wait for the worker task to exit on its own, because the
    /// `shutdown` future passed to [`WorkflowRuntime::spawn`] resolved
    /// or a claim error ended the loop.
    pub async fn wait(self) -> Result<()> {
        match self.join.await {
            Ok(result) => result,
            Err(join_error) => std::panic::resume_unwind(join_error.into_panic()),
        }
    }
}

struct StepWorker<R, H> {
    inner: Arc<RuntimeInner<R, H>>,
}

impl<R: StepRunner + 'static, H: TerminalHook + 'static> Worker for StepWorker<R, H> {
    async fn process_with_effects(
        &self,
        job: &JobRecord,
        lease: &LeaseHandle,
    ) -> std::result::Result<SettlementEffects, WorkerError> {
        self.inner.process_step(job, lease).await
    }
}

fn split_headers(headers: &HashMap<String, String>) -> HashMap<String, String> {
    headers
        .iter()
        .filter(|(k, _)| !k.starts_with(RESERVED_HEADER_PREFIX))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn parse_step_headers(job: &JobRecord) -> std::result::Result<(String, u32), Error> {
    let run_id = job
        .headers
        .get(HEADER_RUN_ID)
        .ok_or(Error::MissingHeader(HEADER_RUN_ID))?
        .to_string();
    let step_str = job
        .headers
        .get(HEADER_STEP)
        .ok_or(Error::MissingHeader(HEADER_STEP))?;
    let step_number: u32 = step_str.parse().map_err(|_| Error::InvalidStepHeader {
        header: HEADER_STEP,
        value: step_str.clone(),
    })?;
    Ok((run_id, step_number))
}

impl RuntimeCore {
    /// One pass of every retention sweep; the number of entities cleared.
    #[cfg(test)]
    pub(crate) async fn sweep_once(&self) -> Result<usize> {
        let mut cleared = 0;
        for sweep in &self.sweeps {
            cleared += sweep.pass(&self.queue, &*self.clock).await?;
        }
        Ok(cleared)
    }

    /// The current-step pointer of a run whose durable record exists.
    pub(crate) async fn current_step(&self, run_id: &str) -> Result<DurableCurrentStep> {
        let bytes = self
            .queue
            .kv_get(&step_kv_key(run_id))
            .await?
            .ok_or_else(|| Error::InconsistentRunState(run_id.to_string()))?;
        Ok(rmp_serde::from_slice(&bytes)?)
    }

    /// Returns the per-run-id submit lock for `run_id`, creating it on
    /// first access.
    fn submit_lock_for(&self, run_id: &str) -> Arc<Mutex<()>> {
        let mut map = self.submit_locks.lock().unwrap();
        map.entry(run_id.to_string()).or_default().clone()
    }

    /// Drop `run_id`'s submit lock entry once no submit references it.
    /// Callers drop their clone of the lock first; a concurrent submit
    /// that has already cloned the entry keeps it alive and removes it
    /// when it finishes.
    fn release_submit_lock(&self, run_id: &str) {
        let mut map = self.submit_locks.lock().unwrap();
        if let Some(lock) = map.get(run_id)
            && Arc::strong_count(lock) == 1
        {
            map.remove(run_id);
        }
    }

    /// Build the enqueue request for one step of a run, with a
    /// pre-assigned job id so the registry can reference the step
    /// before the enqueue commits. Returns the request and the
    /// assigned id.
    fn step_enqueue_request(
        &self,
        run_id: &str,
        step_number: u32,
        payload: Vec<u8>,
        user_headers: &HashMap<String, String>,
        opts: StepEnqueueOpts,
    ) -> (EnqueueRequest, String) {
        let job_id = self.queue.next_job_id();
        let mut headers = user_headers.clone();
        headers.insert(HEADER_RUN_ID.to_string(), run_id.to_string());
        headers.insert(HEADER_STEP.to_string(), step_number.to_string());
        for (key, value) in &opts.reserved_headers {
            headers.insert((*key).to_string(), value.clone());
        }

        let request = EnqueueRequest {
            queue: self.queue_name.clone(),
            payload,
            options: EnqueueOptions::default()
                .headers(headers)
                .run_at(opts.run_at)
                .priority(opts.priority)
                .max_attempts(opts.max_attempts)
                .dedup_key(Some(format!("{DEDUP_PREFIX}{run_id}:{step_number}")))
                .id_override(Some(job_id.clone())),
        };
        (request, job_id)
    }

    /// Build the enqueue request for a run's terminal-notification job.
    /// `priority` and `max_attempts` are inherited from the terminal
    /// step when one exists.
    fn notification_enqueue_request(
        &self,
        outcome: &RunOutcome,
        priority: Option<u32>,
        max_attempts: Option<u32>,
    ) -> Result<EnqueueRequest> {
        let payload = rmp_serde::to_vec_named(&DurableRunOutcome::from(outcome))?;
        let mut headers = HashMap::new();
        headers.insert(HEADER_RUN_ID.to_string(), outcome.run_id.clone());
        headers.insert(HEADER_TERMINAL.to_string(), "1".to_string());
        Ok(EnqueueRequest {
            queue: self.queue_name.clone(),
            payload,
            options: EnqueueOptions::default()
                .headers(headers)
                .priority(priority)
                .max_attempts(max_attempts)
                .dedup_key(Some(format!("{DEDUP_PREFIX}{}:terminal", outcome.run_id))),
        })
    }

    /// Remove the run's registry entry as part of terminating it.
    /// Process state only: a missed removal reports an already-terminal
    /// run as active until the process restarts, while an early removal
    /// strands a run that is still retrying. Worker paths necessarily
    /// call this before the settlement commits, and a settlement that
    /// then fails redelivers the step, whose
    /// [`Self::registry_mark_running`] rebuilds the entry without its
    /// previous `cancel_requested` flag.
    pub(crate) fn forget_run(&self, run_id: &str) {
        self.registry.forget(run_id);
    }

    async fn load_step_output(
        &self,
        run_id: &str,
        step_number: u32,
        step_payload: &[u8],
    ) -> Result<Option<(StepOutcome, StagedEffects)>> {
        let Some(bytes) = self
            .memo_store
            .get_step_output(run_id, step_number, step_payload)
            .await?
        else {
            return Ok(None);
        };
        match rmp_serde::from_slice::<DurableStepOutcomeRecord>(&bytes) {
            Ok(record) => {
                let mut outcome = StepOutcome::from(record.outcome);
                match &mut outcome {
                    StepOutcome::Continue {
                        when: Trigger::After(delay),
                        ..
                    } => {
                        *delay = remaining_delay(record.stored_at_ms, self.clock.now_ms(), *delay);
                    }
                    StepOutcome::Continue {
                        when: Trigger::OnSignal { timeout, .. },
                        ..
                    } => {
                        *timeout =
                            remaining_delay(record.stored_at_ms, self.clock.now_ms(), *timeout);
                    }
                    _ => {}
                }
                Ok(Some((outcome, record.effects)))
            }
            Err(err) => {
                warn!(
                    run_id = %run_id,
                    step_number,
                    error = %err,
                    "step-output replay entry failed to deserialize; recomputing",
                );
                Ok(None)
            }
        }
    }

    async fn store_step_output(
        &self,
        run_id: &str,
        step_number: u32,
        step_payload: &[u8],
        outcome: &StepOutcome,
        effects: &StagedEffects,
    ) -> Result<()> {
        let record = DurableStepOutcomeRecord {
            stored_at_ms: self.clock.now_ms(),
            outcome: DurableStepOutcome::from(outcome),
            effects: effects.clone(),
        };
        let bytes = rmp_serde::to_vec_named(&record)?;
        self.memo_store
            .put_step_output(run_id, step_number, step_payload, &bytes)
            .await
    }

    /// Build the effects that advance the run to `next_step`: the next
    /// step's enqueue joins the current step's acknowledgement
    /// transaction, so the transition is atomic. The pre-assigned job
    /// id is recorded on the registry before the settlement commits;
    /// if the settlement fails because the claim was lost to the
    /// reaper, the redelivered step's [`Self::registry_mark_running`]
    /// rewinds the entry, and a cancel issued in the window falls back
    /// to the registry's `cancel_requested` flag.
    async fn advance(
        &self,
        run_id: &str,
        next_step: u32,
        payload: Vec<u8>,
        user_headers: &HashMap<String, String>,
        opts: StepEnqueueOpts,
    ) -> SettlementEffects {
        self.advance_with_kv(run_id, next_step, payload, user_headers, opts, |_| {
            HashMap::new()
        })
        .await
    }

    /// [`Self::advance`] with caller KV writes joined to the same
    /// acknowledgement transaction. `kv_writes` receives the next step's
    /// pre-assigned job id so the writes can reference it.
    pub(crate) async fn advance_with_kv(
        &self,
        run_id: &str,
        next_step: u32,
        payload: Vec<u8>,
        user_headers: &HashMap<String, String>,
        opts: StepEnqueueOpts,
        kv_writes: impl FnOnce(&str) -> HashMap<Vec<u8>, Vec<u8>>,
    ) -> SettlementEffects {
        let (request, next_job_id) =
            self.step_enqueue_request(run_id, next_step, payload, user_headers, opts);
        let mut kv_writes = kv_writes(&next_job_id);
        // The pointer's encoding cannot fail for a step number and an id.
        if let Ok(pointer) = current_step_bytes(next_step, &next_job_id) {
            kv_writes.insert(step_kv_key(run_id), pointer);
        }
        self.registry.mark_pending(run_id, next_step, next_job_id);
        SettlementEffects::default()
            .enqueues(vec![request])
            .kv_writes(kv_writes)
    }
}

impl<R: StepRunner, H: TerminalHook> RuntimeInner<R, H> {
    /// Settle a run into its terminal state: return the deletes of the
    /// durable run record and the current-step pointer, the terminal
    /// marker's write (when memo
    /// retention is enabled) and the terminal-notification enqueue
    /// (when the hook observes this outcome) as [`SettlementEffects`]
    /// for the settlement transaction. The notification job's payload
    /// is the committed outcome and the configured [`TerminalHook`]
    /// runs as its worker.
    ///
    /// The effects are pure: nothing is written and no state is
    /// mutated here, so a caller that builds them and then commits a
    /// non-terminal outcome leaves no trace. Dropping the run's
    /// registry entry is the caller's own [`RuntimeCore::forget_run`] call. A
    /// settlement that fails redelivers the step, which re-terminates
    /// and rebuilds the same effects.
    pub(crate) fn terminate_collecting_effects(
        &self,
        outcome: &RunOutcome,
        priority: Option<u32>,
        max_attempts: Option<u32>,
    ) -> SettlementEffects {
        let kv_deletes = vec![run_kv_key(&outcome.run_id), step_kv_key(&outcome.run_id)];
        let kv_writes = if self.core.memo_retention.is_some() {
            HashMap::from([(
                terminal_kv_key(&outcome.run_id, self.core.clock.now_ms()),
                Vec::new(),
            )])
        } else {
            HashMap::new()
        };
        let enqueues = if self.terminal_hook.observes(outcome) {
            match self
                .core
                .notification_enqueue_request(outcome, priority, max_attempts)
            {
                Ok(request) => vec![request],
                Err(err) => {
                    warn!(
                        run_id = %outcome.run_id,
                        "failed to build the terminal notification: {err}"
                    );
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };
        SettlementEffects::default()
            .enqueues(enqueues)
            .kv_writes(kv_writes)
            .kv_deletes(kv_deletes)
    }

    /// [`Self::terminate_collecting_effects`] plus
    /// [`RuntimeCore::forget_run`]: the pairing every worker-path
    /// termination site performs before its settlement commits.
    pub(crate) fn worker_terminate(
        &self,
        outcome: RunOutcome,
        priority: Option<u32>,
        max_attempts: Option<u32>,
    ) -> SettlementEffects {
        let effects = self.terminate_collecting_effects(&outcome, priority, max_attempts);
        self.core.forget_run(&outcome.run_id);
        effects
    }

    /// Process a terminal-notification job: decode the committed
    /// outcome and run the configured [`TerminalHook`] as the job's
    /// worker. Effects the hook stages join this job's acknowledgement.
    /// A transient hook error retries the job per the queue's backoff;
    /// a permanent one dead-letters it.
    async fn process_notification(
        &self,
        job: &JobRecord,
    ) -> std::result::Result<SettlementEffects, WorkerError> {
        let outcome: RunOutcome = match rmp_serde::from_slice::<DurableRunOutcome>(&job.payload) {
            Ok(durable) => durable.into(),
            Err(err) => {
                warn!(job_id = %job.id, error = %err, "terminal notification has a malformed payload");
                return Err(PermanentFailure::new(err.to_string()).into());
            }
        };
        let effects = TerminalEffects::for_delivery();
        let result = self.terminal_hook.on_termination(&outcome, &effects).await;
        let (staged, enqueues) = effects.seal_and_take();
        match result {
            Ok(()) => Ok(SettlementEffects::default()
                .enqueues(enqueues)
                .kv_writes(staged.writes)
                .kv_deletes(staged.deletes.into_iter().collect())),
            Err(StepError {
                message,
                kind: StepErrorKind::Permanent,
            }) => Err(PermanentFailure::new(message).into()),
            Err(StepError {
                message,
                kind: StepErrorKind::Transient,
            }) => Err(message.into()),
        }
    }

    async fn process_step(
        &self,
        job: &JobRecord,
        lease: &LeaseHandle,
    ) -> std::result::Result<SettlementEffects, WorkerError> {
        if job.headers.contains_key(HEADER_TERMINAL) {
            return self.process_notification(job).await;
        }

        let (run_id, step_number) = match parse_step_headers(job) {
            Ok(v) => v,
            Err(e) => {
                warn!(job_id = %job.id, error = %e, "workflow step has malformed headers");
                if e.is_permanent() {
                    return Err(PermanentFailure::new(e.to_string()).into());
                }
                return Err(e.to_string().into());
            }
        };

        let user_headers = split_headers(&job.headers);

        self.core
            .registry
            .mark_running(&run_id, step_number, &job.id, &user_headers);

        // `Queue::cancel` fires the claim's token, and a re-claim fires it
        // again from the job's persisted `cancel_requested`. The runner
        // receives a child, so a runner firing its own token is not
        // treated as an external cancellation below.
        let claim_cancel = lease.cancel_token().clone();
        let cancel_token = claim_cancel.child_token();

        let (step_signal, signal_kv_deletes) = match self
            .core
            .resolve_step_signal(job, &run_id, step_number)
            .await
        {
            Ok(v) => v,
            // Transient: the step retries and resolves again.
            Err(e) => return Err(e.to_string().into()),
        };

        let effects_handle = EffectsHandle::for_delivery();
        let step = Step {
            run_id: run_id.clone(),
            step_number,
            payload: job.payload.clone(),
            headers: user_headers.clone(),
            job_id: job.id.clone(),
            attempts: job.attempts,
            max_attempts: job.max_attempts,
            cancel_token,
            lease: lease.clone(),
            memo: self.core.memo_store.new_memo(&run_id, step_number),
            run_memo: self.core.memo_store.new_run_memo(&run_id),
            effects: effects_handle.clone(),
            kv: KvReadHandle::for_delivery(self.core.queue.clone()),
            signal: step_signal,
        };

        // Preserve the run's per-step priority and max_attempts across the
        // boundary by re-using the values from the just-processed job.
        let inherit_opts = || StepEnqueueOpts {
            run_at: None,
            priority: Some(job.priority),
            max_attempts: Some(job.max_attempts),
            reserved_headers: Vec::new(),
        };

        let mut replayed_step_output = false;
        let mut replayed_effects = StagedEffects::default();
        let outcome = if self.core.step_output_replay {
            match self
                .core
                .load_step_output(&run_id, step_number, &job.payload)
                .await
            {
                Ok(Some((outcome, effects))) => {
                    replayed_step_output = true;
                    replayed_effects = effects;
                    debug!(
                        run_id = %run_id,
                        step_number,
                        "replaying stored step outcome",
                    );
                    Ok(outcome)
                }
                Ok(None) => self.runner.run_step(&step).await,
                Err(err) => return Err(err.to_string().into()),
            }
        } else {
            self.runner.run_step(&step).await
        };

        // Sealed as soon as the runner has returned: an effect staged
        // through a retained handle clone after this point could not
        // join the settlement, so staging it errors.
        let sealed = effects_handle.seal_and_take();
        let failure_writes = sealed.on_failure;
        let caller_effects = if replayed_step_output {
            replayed_effects
        } else {
            sealed.outcome
        };

        // Both sources are required: the claim's token reports a
        // cancellation after a restart, and the registry flag reports one
        // after a step advance, which the job-scoped persisted flag
        // cannot.
        let external_cancel =
            claim_cancel.is_cancelled() || self.core.registry.cancel_requested(&run_id);

        if self.core.step_output_replay
            && !replayed_step_output
            && !external_cancel
            && let Ok(ref outcome) = outcome
            && let Err(err) = self
                .core
                .store_step_output(&run_id, step_number, &job.payload, outcome, &caller_effects)
                .await
        {
            if err.is_permanent() {
                return Err(PermanentFailure::new(err.to_string()).into());
            }
            return Err(err.to_string().into());
        }

        let runner_cancelled = matches!(outcome, Ok(StepOutcome::Cancel { .. }));

        // Cancellation precedence:
        // 1. A runner-issued `StepOutcome::Cancel` wins (it carries an
        //    in-step reason that we surface on `RunOutcome::error`).
        // 2. Otherwise an external `WorkflowRuntime::cancel` overrides
        //    whatever outcome the runner returned (including transient
        //    retries and permanent dead-letters), with `error: None` so
        //    consumers can distinguish external vs. runner-issued cancel.
        let settled = match outcome {
            Ok(StepOutcome::Cancel { reason }) => Ok(self.worker_terminate(
                RunOutcome::cancelled(run_id.clone(), Some(reason), user_headers, step_number),
                Some(job.priority),
                Some(job.max_attempts),
            )),
            _ if external_cancel => Ok(self.worker_terminate(
                RunOutcome::cancelled(run_id.clone(), None, user_headers, step_number),
                Some(job.priority),
                Some(job.max_attempts),
            )),
            Ok(StepOutcome::Continue { payload, when }) => match when {
                Trigger::Immediate => Ok(self
                    .core
                    .advance(
                        &run_id,
                        step_number + 1,
                        payload,
                        &user_headers,
                        inherit_opts(),
                    )
                    .await),
                Trigger::After(delay) => {
                    let now = UNIX_EPOCH + Duration::from_millis(self.core.clock.now_ms());
                    let opts = StepEnqueueOpts {
                        run_at: Some(now + delay),
                        ..inherit_opts()
                    };
                    Ok(self
                        .core
                        .advance(&run_id, step_number + 1, payload, &user_headers, opts)
                        .await)
                }
                Trigger::OnSignal {
                    correlation_key,
                    timeout,
                } => {
                    self.advance_on_signal(
                        &run_id,
                        step_number + 1,
                        payload,
                        &user_headers,
                        inherit_opts(),
                        &correlation_key,
                        timeout,
                    )
                    .await
                }
            },
            Ok(StepOutcome::Succeed { result }) => Ok(self.worker_terminate(
                RunOutcome::succeeded(run_id.clone(), result, user_headers, step_number),
                Some(job.priority),
                Some(job.max_attempts),
            )),
            Ok(StepOutcome::Fail { reason }) => {
                // Runner verdict: workflow failed but the step itself ran
                // cleanly. Ack the step (no dead-letter); the run
                // terminates as `Failed`.
                Ok(self.worker_terminate(
                    RunOutcome::failed(run_id.clone(), reason, user_headers, step_number),
                    Some(job.priority),
                    Some(job.max_attempts),
                ))
            }
            // The two terminating failures carry the runner's failure
            // writes beside the termination effects; the core applies
            // them only with the dead-lettering settlement.
            Err(StepError {
                message,
                kind: StepErrorKind::Permanent,
            }) => {
                let mut effects = self.worker_terminate(
                    RunOutcome::failed(run_id.clone(), message.clone(), user_headers, step_number),
                    Some(job.priority),
                    None,
                );
                effects.kv_writes.extend(failure_writes);
                Err(FailWith::new(PermanentFailure::new(message), effects).into())
            }
            Err(StepError {
                message,
                kind: StepErrorKind::Transient,
            }) => {
                // Last attempt: this nack will dead-letter. Build the
                // effects while the registry entry and headers are in
                // hand; `nack_with` decides whether they apply. The
                // attempts test applies only to the registry removal.
                if job.attempts >= job.max_attempts {
                    let mut effects = self.worker_terminate(
                        RunOutcome::failed(
                            run_id.clone(),
                            message.clone(),
                            user_headers,
                            step_number,
                        ),
                        Some(job.priority),
                        None,
                    );
                    effects.kv_writes.extend(failure_writes);
                    return Err(FailWith::new(WorkerError::from(message), effects).into());
                }
                Err(message.into())
            }
        };
        // Durable signal entries scheduled for cleanup are deleted with the
        // step's settlement; on retry paths they survive for redelivery.
        settled.map(|mut effects| {
            effects.kv_deletes.extend(signal_kv_deletes);
            // The external-cancel override commits Cancelled in place of
            // the runner's outcome; the staged effects describe that
            // outcome and are discarded with it. A runner-issued Cancel
            // keeps its effects.
            if runner_cancelled || !external_cancel {
                effects.kv_writes.extend(caller_effects.writes);
                effects.kv_deletes.extend(caller_effects.deletes);
            }
            effects
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::MAX_RUN_ID_LEN;
    use crate::keys::{
        TERMINAL_KV_PREFIX, parse_timestamped_kv_key, signal_buf_kv_key, signal_wait_kv_key,
    };
    use crate::signal::SignalOutcome;
    use crate::terminal::NoopTerminalHook;
    use crate::terminal::TerminalStatus;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicU32, Ordering};
    use taquba::object_store::memory::InMemory;
    use taquba::{MockClock, OpenOptions, QueueConfig};
    use tokio::sync::oneshot;

    /// Recording terminal hook backed by an mpsc channel.
    struct ChannelHook {
        tx: tokio::sync::mpsc::UnboundedSender<RunOutcome>,
    }

    impl TerminalHook for ChannelHook {
        async fn on_termination(
            &self,
            outcome: &RunOutcome,
            _effects: &TerminalEffects,
        ) -> std::result::Result<(), StepError> {
            let _ = self.tx.send(outcome.clone());
            Ok(())
        }
    }

    /// Runner that executes a fixed list of step outcomes in order.
    struct ScriptedRunner {
        script: Arc<StdMutex<Vec<StepOutcome>>>,
    }

    impl ScriptedRunner {
        fn new(steps: Vec<StepOutcome>) -> Self {
            Self {
                script: Arc::new(StdMutex::new(steps)),
            }
        }
    }

    impl StepRunner for ScriptedRunner {
        async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
            let next = self.script.lock().unwrap().remove(0);
            Ok(next)
        }
    }

    /// Every terminal marker in the queue's KV namespace, as
    /// `(run_id, terminal_at_ms)` pairs in key order (oldest first).
    async fn terminal_markers(queue: &Queue) -> Vec<(String, u64)> {
        let page = queue
            .kv_scan(TERMINAL_KV_PREFIX, None, 1_000)
            .await
            .unwrap();
        page.entries
            .iter()
            .map(|(key, _)| {
                parse_timestamped_kv_key(TERMINAL_KV_PREFIX, key).expect("well-formed marker key")
            })
            .collect()
    }

    async fn fresh_queue() -> (Arc<Queue>, Arc<dyn taquba::object_store::ObjectStore>) {
        let store: Arc<dyn taquba::object_store::ObjectStore> = Arc::new(InMemory::new());
        let queue = Arc::new(Queue::open(store.clone(), "test").await.unwrap());
        (queue, store)
    }

    /// Queue + object store + a [`MockClock`] wired into the queue. Use
    /// [`advance`] to move both the mock clock and tokio's paused time
    /// in lockstep.
    async fn fresh_queue_with_mock_clock(
        initial_ms: u64,
    ) -> (
        Arc<Queue>,
        Arc<dyn taquba::object_store::ObjectStore>,
        MockClock,
    ) {
        let clock = MockClock::new(initial_ms);
        let opts = OpenOptions::default().clock(Arc::new(clock.clone()));
        let store: Arc<dyn taquba::object_store::ObjectStore> = Arc::new(InMemory::new());
        let queue = Arc::new(
            Queue::open_with_options(store.clone(), "test", opts)
                .await
                .unwrap(),
        );
        (queue, store, clock)
    }

    /// Advance both a [`MockClock`] and tokio's paused time by `by`, so
    /// timestamp reads and `tokio::time::sleep` / `interval` move
    /// together in tests.
    async fn advance(clock: &MockClock, by: Duration) {
        clock.advance(by);
        tokio::time::advance(by).await;
    }

    /// [`fresh_queue_fast_retry`] with a [`MockClock`] wired in, for
    /// multi-attempt tests that read the clock's value as well as
    /// depending on retries being prompt.
    async fn fresh_queue_fast_retry_with_mock_clock(
        initial_ms: u64,
    ) -> (
        Arc<Queue>,
        Arc<dyn taquba::object_store::ObjectStore>,
        MockClock,
    ) {
        let clock = MockClock::new(initial_ms);
        let opts = OpenOptions::default()
            .clock(Arc::new(clock.clone()))
            .default_queue_config(QueueConfig::default().retry_backoff_base(Duration::ZERO))
            .reaper_interval(Duration::from_millis(50))
            .scheduler_interval(Duration::from_millis(50));
        let store: Arc<dyn taquba::object_store::ObjectStore> = Arc::new(InMemory::new());
        let queue = Arc::new(
            Queue::open_with_options(store.clone(), "test", opts)
                .await
                .unwrap(),
        );
        (queue, store, clock)
    }

    /// Queue with zero retry backoff and a tight reaper, so multi-attempt
    /// tests run in well under a second.
    async fn fresh_queue_fast_retry() -> (Arc<Queue>, Arc<dyn taquba::object_store::ObjectStore>) {
        let opts = OpenOptions::default()
            .default_queue_config(QueueConfig::default().retry_backoff_base(Duration::ZERO))
            .reaper_interval(Duration::from_millis(50))
            .scheduler_interval(Duration::from_millis(50));
        let store: Arc<dyn taquba::object_store::ObjectStore> = Arc::new(InMemory::new());
        let queue = Arc::new(
            Queue::open_with_options(store.clone(), "test", opts)
                .await
                .unwrap(),
        );
        (queue, store)
    }

    fn spawn_runtime<R, H>(runtime: WorkflowRuntime<R, H>) -> oneshot::Sender<()>
    where
        R: StepRunner + 'static,
        H: TerminalHook + 'static,
    {
        let (tx, rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = runtime
                .run(async move {
                    let _ = rx.await;
                })
                .await;
        });
        tx
    }

    /// A runner that extends its lease through the step and reports the
    /// resulting expiry.
    struct RenewingRunner {
        queue: Arc<Queue>,
        tx: tokio::sync::mpsc::UnboundedSender<u64>,
    }

    impl StepRunner for RenewingRunner {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            step.lease
                .ensure_at_least(Duration::from_secs(600))
                .map_err(|e| StepError::transient(e.to_string()))?;
            let expiry = self
                .queue
                .lease_expiry("workflow-steps", &step.job_id)
                .expect("a running step holds a lease");
            let _ = self.tx.send(expiry);
            Ok(StepOutcome::Succeed { result: Vec::new() })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_step_runner_extends_its_lease_through_the_step() {
        let base = 1_700_000_000_000;
        let (queue, store, _clock) = fresh_queue_with_mock_clock(base).await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (hook_tx, mut hook_rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            RenewingRunner { queue, tx },
            ChannelHook { tx: hook_tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        let expiry = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            expiry >= base + 600_000,
            "the extension must reach the lease registry",
        );
        let outcome = tokio::time::timeout(Duration::from_secs(2), hook_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.status, TerminalStatus::Succeeded);

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn single_step_succeeds_and_fires_hook() {
        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store.clone(),
            ScriptedRunner::new(vec![StepOutcome::Succeed {
                result: b"done".to_vec(),
            }]),
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"in".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.status, TerminalStatus::Succeeded);
        assert_eq!(outcome.result.as_deref(), Some(b"done".as_slice()));
        assert_eq!(outcome.final_step, 0);
        assert!(runtime.status(&handle.run_id).await.is_none());

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn multi_step_run_advances_through_continue() {
        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store.clone(),
            ScriptedRunner::new(vec![
                StepOutcome::continue_now(b"step1".to_vec()),
                StepOutcome::continue_now(b"step2".to_vec()),
                StepOutcome::Succeed {
                    result: b"final".to_vec(),
                },
            ]),
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"start".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.final_step, 2);
        assert_eq!(outcome.status, TerminalStatus::Succeeded);
        assert_eq!(outcome.result.as_deref(), Some(b"final".as_slice()));

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn continue_after_delays_next_step_until_promotion() {
        let initial = 1_700_000_000_000u64;
        let (queue, store, clock) = fresh_queue_with_mock_clock(initial).await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            ScriptedRunner::new(vec![
                StepOutcome::continue_after(b"step1".to_vec(), Duration::from_secs(60)),
                StepOutcome::Succeed {
                    result: b"final".to_vec(),
                },
            ]),
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"start".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        // The delayed step is held in `scheduled` and the run must not
        // terminate while the delay is pending.
        assert!(
            tokio::time::timeout(Duration::from_millis(500), rx.recv())
                .await
                .is_err()
        );
        let stats = queue.stats("workflow-steps").await.unwrap();
        assert_eq!(stats.scheduled, 1);

        advance(&clock, Duration::from_secs(61)).await;
        queue.promote_scheduled_now().await.unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.final_step, 1);
        assert_eq!(outcome.status, TerminalStatus::Succeeded);

        let _ = shutdown.send(());
    }

    type ObservedSignals = Arc<StdMutex<Vec<Option<Vec<u8>>>>>;

    /// Two-step runner: step 0 continues with a signal wait on
    /// `correlation_key`, step 1 records [`Step::signal`] and succeeds.
    struct SignalProbe {
        correlation_key: String,
        timeout: Duration,
        observed: ObservedSignals,
    }

    impl StepRunner for SignalProbe {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            if step.step_number == 0 {
                Ok(StepOutcome::continue_on_signal(
                    Vec::new(),
                    self.correlation_key.clone(),
                    self.timeout,
                ))
            } else {
                self.observed.lock().unwrap().push(step.signal.clone());
                Ok(StepOutcome::Succeed { result: Vec::new() })
            }
        }
    }

    async fn wait_for_scheduled(queue: &Queue, count: i64) {
        for _ in 0..200 {
            if queue.stats("workflow-steps").await.unwrap().scheduled == count {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("scheduled count never reached {count}");
    }

    fn signal_probe_runtime(
        queue: Arc<Queue>,
        store: Arc<dyn taquba::object_store::ObjectStore>,
        correlation_key: &str,
        timeout: Duration,
    ) -> (
        WorkflowRuntime<SignalProbe, ChannelHook>,
        ObservedSignals,
        tokio::sync::mpsc::UnboundedReceiver<RunOutcome>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let observed = Arc::new(StdMutex::new(Vec::new()));
        let runtime = WorkflowRuntime::builder(
            queue,
            store,
            SignalProbe {
                correlation_key: correlation_key.to_string(),
                timeout,
                observed: observed.clone(),
            },
            ChannelHook { tx },
        )
        .build();
        (runtime, observed, rx)
    }

    #[tokio::test(start_paused = true)]
    async fn signal_wakes_waiting_run_early_with_payload() {
        let (queue, store, _clock) = fresh_queue_with_mock_clock(1_700_000_000_000).await;
        let (runtime, observed, mut rx) =
            signal_probe_runtime(queue.clone(), store, "order-1", Duration::from_secs(3600));
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        wait_for_scheduled(&queue, 1).await;

        let outcome = runtime.signal("order-1", b"paid".to_vec()).await.unwrap();
        assert_eq!(outcome, SignalOutcome::Delivered);

        // The run completes without the timeout elapsing on the mock clock.
        let terminal = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal.status, TerminalStatus::Succeeded);
        assert_eq!(
            observed.lock().unwrap().as_slice(),
            &[Some(b"paid".to_vec())]
        );

        // Both durable signal entries are consumed.
        assert!(
            queue
                .kv_get(&signal_wait_kv_key("order-1"))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            queue
                .kv_get(&signal_buf_kv_key("order-1"))
                .await
                .unwrap()
                .is_none()
        );

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_run_waiting_on_a_signal_survives_a_close_and_reopen() {
        let store: Arc<dyn taquba::object_store::ObjectStore> = Arc::new(InMemory::new());
        let open = |store: Arc<dyn taquba::object_store::ObjectStore>| async move {
            Arc::new(
                Queue::open_with_options(
                    store,
                    "test",
                    OpenOptions::default().clock(Arc::new(MockClock::new(1_700_000_000_000))),
                )
                .await
                .unwrap(),
            )
        };

        let queue = open(store.clone()).await;
        let (runtime, _observed, _rx) = signal_probe_runtime(
            queue.clone(),
            store.clone(),
            "approval",
            Duration::from_secs(3600),
        );
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let worker = tokio::spawn({
            let runtime = runtime.clone();
            async move {
                runtime
                    .run(async move {
                        let _ = stop_rx.await;
                    })
                    .await
            }
        });
        runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        wait_for_scheduled(&queue, 1).await;
        let _ = stop_tx.send(());
        worker.await.unwrap().unwrap();
        drop(runtime);
        Arc::into_inner(queue)
            .expect("no other queue references at close")
            .close()
            .await
            .unwrap();

        let queue = open(store.clone()).await;
        assert_eq!(queue.stats("workflow-steps").await.unwrap().scheduled, 1);

        let (runtime, observed, mut rx) =
            signal_probe_runtime(queue.clone(), store, "approval", Duration::from_secs(3600));
        let shutdown = spawn_runtime(runtime.clone());

        let delivery = runtime
            .signal("approval", b"approved".to_vec())
            .await
            .unwrap();
        assert_eq!(delivery, SignalOutcome::Delivered);

        let terminal = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal.status, TerminalStatus::Succeeded);
        assert_eq!(
            observed.lock().unwrap().as_slice(),
            &[Some(b"approved".to_vec())]
        );
        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn signal_timeout_delivers_none() {
        let (queue, store, clock) = fresh_queue_with_mock_clock(1_700_000_000_000).await;
        let (runtime, observed, mut rx) =
            signal_probe_runtime(queue.clone(), store, "order-2", Duration::from_secs(60));
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        wait_for_scheduled(&queue, 1).await;

        advance(&clock, Duration::from_secs(61)).await;
        queue.promote_scheduled_now().await.unwrap();

        let terminal = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal.status, TerminalStatus::Succeeded);
        assert_eq!(observed.lock().unwrap().as_slice(), &[None]);

        assert!(
            queue
                .kv_get(&signal_wait_kv_key("order-2"))
                .await
                .unwrap()
                .is_none()
        );

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn signal_before_waiter_is_buffered_and_consumed_at_registration() {
        let (queue, store, _clock) = fresh_queue_with_mock_clock(1_700_000_000_000).await;
        let (runtime, observed, mut rx) =
            signal_probe_runtime(queue.clone(), store, "order-3", Duration::from_secs(3600));
        let shutdown = spawn_runtime(runtime.clone());

        let outcome = runtime.signal("order-3", b"early".to_vec()).await.unwrap();
        assert_eq!(outcome, SignalOutcome::Buffered);

        runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();

        // The buffered signal is consumed at registration: the run
        // completes without any waiting and without the timeout elapsing.
        let terminal = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal.status, TerminalStatus::Succeeded);
        assert_eq!(
            observed.lock().unwrap().as_slice(),
            &[Some(b"early".to_vec())]
        );

        assert!(
            queue
                .kv_get(&signal_buf_kv_key("order-3"))
                .await
                .unwrap()
                .is_none()
        );

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn second_signal_before_consumption_replaces_first() {
        let (queue, store, _clock) = fresh_queue_with_mock_clock(1_700_000_000_000).await;
        let (runtime, observed, mut rx) =
            signal_probe_runtime(queue.clone(), store, "order-4", Duration::from_secs(3600));
        let shutdown = spawn_runtime(runtime.clone());

        assert_eq!(
            runtime.signal("order-4", b"first".to_vec()).await.unwrap(),
            SignalOutcome::Buffered
        );
        assert_eq!(
            runtime.signal("order-4", b"second".to_vec()).await.unwrap(),
            SignalOutcome::Buffered
        );

        runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();

        let terminal = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal.status, TerminalStatus::Succeeded);
        assert_eq!(
            observed.lock().unwrap().as_slice(),
            &[Some(b"second".to_vec())]
        );

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn clear_signal_discards_buffered_signal() {
        let (queue, store, _clock) = fresh_queue_with_mock_clock(1_700_000_000_000).await;
        let (runtime, _observed, _rx) =
            signal_probe_runtime(queue.clone(), store, "order-5", Duration::from_secs(60));

        assert_eq!(
            runtime.signal("order-5", b"stale".to_vec()).await.unwrap(),
            SignalOutcome::Buffered
        );
        assert!(runtime.clear_signal("order-5").await.unwrap());
        assert!(!runtime.clear_signal("order-5").await.unwrap());
        assert!(
            queue
                .kv_get(&signal_buf_kv_key("order-5"))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn duplicate_waiter_registration_fails_the_run() {
        let (queue, store, _clock) = fresh_queue_with_mock_clock(1_700_000_000_000).await;
        let (runtime, _observed, mut rx) =
            signal_probe_runtime(queue.clone(), store, "order-6", Duration::from_secs(3600));
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                run_id: Some("run-a".to_string()),
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        wait_for_scheduled(&queue, 1).await;

        runtime
            .submit(RunSpec {
                run_id: Some("run-b".to_string()),
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();

        let terminal = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal.run_id, "run-b");
        assert_eq!(terminal.status, TerminalStatus::Failed);
        assert!(
            terminal
                .error
                .as_deref()
                .is_some_and(|e| e.contains("already registered"))
        );
        assert_eq!(
            queue.stats("workflow-steps").await.unwrap().dead,
            1,
            "the rejected registration dead-letters run-b's step",
        );
        assert!(
            queue.kv_get(&run_kv_key("run-b")).await.unwrap().is_none(),
            "the run record delete rides the dead-letter",
        );
        assert!(
            queue.kv_get(&run_kv_key("run-a")).await.unwrap().is_some(),
            "the waiting run keeps its record",
        );

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn buffered_signal_missed_by_the_wake_is_delivered_at_timeout() {
        let (queue, store, clock) = fresh_queue_with_mock_clock(1_700_000_000_000).await;
        let (runtime, observed, mut rx) =
            signal_probe_runtime(queue.clone(), store, "order-7", Duration::from_secs(60));
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        wait_for_scheduled(&queue, 1).await;

        // A signal that was buffered without winning the wake (written
        // directly to simulate the settling-registration window).
        queue
            .kv_put(&signal_buf_kv_key("order-7"), b"late")
            .await
            .unwrap();

        advance(&clock, Duration::from_secs(61)).await;
        queue.promote_scheduled_now().await.unwrap();

        let terminal = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal.status, TerminalStatus::Succeeded);
        assert_eq!(
            observed.lock().unwrap().as_slice(),
            &[Some(b"late".to_vec())]
        );

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_waiter_leaves_no_live_index_for_the_next_signal() {
        let (queue, store, _clock) = fresh_queue_with_mock_clock(1_700_000_000_000).await;
        let (runtime, _observed, mut rx) =
            signal_probe_runtime(queue.clone(), store, "order-8", Duration::from_secs(3600));
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        wait_for_scheduled(&queue, 1).await;

        assert!(runtime.cancel(&handle.run_id).await.unwrap());
        let terminal = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal.status, TerminalStatus::Cancelled);

        // The signal finds only a stale index entry, cleans it and buffers.
        assert_eq!(
            runtime.signal("order-8", b"orphan".to_vec()).await.unwrap(),
            SignalOutcome::Buffered
        );
        assert!(
            queue
                .kv_get(&signal_wait_kv_key("order-8"))
                .await
                .unwrap()
                .is_none()
        );

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_failure_dead_letters_and_fires_hook() {
        struct FailingRunner;
        impl StepRunner for FailingRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                Err(StepError::permanent("nope"))
            }
        }

        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            FailingRunner,
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.status, TerminalStatus::Failed);
        assert_eq!(outcome.error.as_deref(), Some("nope"));
        assert!(runtime.status(&handle.run_id).await.is_none());

        // Permanent runner errors *do* dead-letter the step, and the
        // notification the hook ran from was enqueued by that same
        // dead-letter transaction, so the record is already visible.
        let stats = queue.stats("workflow-steps").await.unwrap();
        assert_eq!(stats.dead, 1, "permanent error should dead-letter");

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn fail_outcome_terminates_run_without_dead_letter() {
        // StepOutcome::Fail is the runner's *verdict* path, not an
        // infrastructure error: the hook fires with Failed, the registry
        // entry is cleaned up, but the step is acked normally so no dead
        // job is left behind for operators to inspect.
        struct VerdictRunner;
        impl StepRunner for VerdictRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                Ok(StepOutcome::Fail {
                    reason: "agent declined the task".to_string(),
                })
            }
        }

        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            VerdictRunner,
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("hook fired in time")
            .expect("hook channel open");

        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.status, TerminalStatus::Failed);
        assert_eq!(outcome.error.as_deref(), Some("agent declined the task"));
        assert!(runtime.status(&handle.run_id).await.is_none());

        // Crucially: no dead-letter, distinguishing runner verdict from
        // infrastructure failure at the queue level.
        let stats = queue.stats("workflow-steps").await.unwrap();
        assert_eq!(stats.dead, 0, "Fail verdict must not dead-letter");

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn duplicate_submit_in_process_is_idempotent_and_rejects_a_changed_input() {
        // Pause forever on the first step so the run stays active in the
        // registry while we attempt the duplicate submit.
        struct PauseRunner;
        impl StepRunner for PauseRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                std::future::pending().await
            }
        }

        let (queue, store) = fresh_queue().await;
        let runtime =
            WorkflowRuntime::builder(queue, store.clone(), PauseRunner, NoopTerminalHook).build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                run_id: Some("fixed-id".to_string()),
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        // Wait for the worker to start the step so the registry observes the
        // run as Running (or at least Pending).
        for _ in 0..40 {
            if runtime.status(&handle.run_id).await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(runtime.status(&handle.run_id).await.is_some());

        let outcome = runtime
            .submit(RunSpec {
                run_id: Some("fixed-id".to_string()),
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(outcome.run_id, "fixed-id");
        assert!(!outcome.newly_submitted);
        assert_eq!(outcome.job_id, handle.job_id);

        let err = runtime
            .submit(RunSpec {
                run_id: Some("fixed-id".to_string()),
                input: b"y".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(matches!(&err, Error::InputMismatch(id) if id == "fixed-id"));
        assert!(err.is_permanent());

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_duplicate_known_only_from_the_durable_record_reports_the_current_job() {
        let (queue, store) = fresh_queue().await;
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        // Step 0 continues into a step scheduled an hour out, so the run
        // rests at step 1 with a job the second runtime never saw.
        let first = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            ScriptedRunner::new(vec![StepOutcome::continue_after(
                b"next".to_vec(),
                Duration::from_secs(3600),
            )]),
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(first.clone());
        let submitted = first
            .submit(RunSpec {
                run_id: Some("durable".into()),
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        for _ in 0..200 {
            if queue.stats("workflow-steps").await.unwrap().scheduled == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = shutdown.send(());

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let second = WorkflowRuntime::builder(
            queue.clone(),
            store,
            ScriptedRunner::new(vec![]),
            ChannelHook { tx },
        )
        .build();
        let duplicate = second
            .submit(RunSpec {
                run_id: Some("durable".into()),
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(!duplicate.newly_submitted);
        assert_ne!(
            duplicate.job_id, submitted.job_id,
            "the pointer moved to step 1"
        );
        let step_1 = queue.get_job(&duplicate.job_id).await.unwrap().unwrap();
        assert_eq!(step_1.status, taquba::JobStatus::Scheduled);
        assert_eq!(
            step_1.headers.get(HEADER_STEP).map(String::as_str),
            Some("1")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_run_submitted_with_run_at_stays_scheduled_until_then() {
        struct Echo;
        impl StepRunner for Echo {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                Ok(StepOutcome::Succeed {
                    result: step.payload.clone(),
                })
            }
        }

        let t0 = 1_700_000_000_000;
        let (queue, store, clock) = fresh_queue_with_mock_clock(t0).await;
        let runtime =
            WorkflowRuntime::builder(queue.clone(), store, Echo, NoopTerminalHook).build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                run_at: Some(UNIX_EPOCH + Duration::from_millis(t0 + 60_000)),
                ..Default::default()
            })
            .await
            .unwrap();
        let scheduled = queue
            .list_jobs("workflow-steps", taquba::JobStatus::Scheduled, None, 10)
            .await
            .unwrap()
            .jobs;
        assert_eq!(scheduled.len(), 1);
        let job_id = scheduled[0].id.clone();

        let waiter = tokio::spawn({
            let queue = queue.clone();
            async move {
                queue
                    .wait_for_completion(&job_id, Duration::from_secs(600))
                    .await
            }
        });
        advance(&clock, Duration::from_secs(120)).await;
        assert!(matches!(
            waiter.await.unwrap().unwrap(),
            taquba::WaitOutcome::Done(_),
        ));

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_step_reports_its_attempt_limit() {
        struct Recording(Arc<std::sync::Mutex<Option<u32>>>);
        impl StepRunner for Recording {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                *self.0.lock().unwrap() = Some(step.max_attempts);
                Ok(StepOutcome::Succeed { result: Vec::new() })
            }
        }

        let seen = Arc::new(std::sync::Mutex::new(None));
        let (queue, store) = fresh_queue().await;
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            Recording(seen.clone()),
            NoopTerminalHook,
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let outcome = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                max_attempts_per_step: Some(7),
                ..Default::default()
            })
            .await
            .unwrap();
        let job_id = outcome.job_id;
        queue
            .wait_for_completion(&job_id, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(*seen.lock().unwrap(), Some(7));

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn submit_releases_its_per_run_lock_entry() {
        struct PauseRunner;
        impl StepRunner for PauseRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                std::future::pending().await
            }
        }

        let (queue, store) = fresh_queue().await;
        let runtime =
            WorkflowRuntime::builder(queue, store.clone(), PauseRunner, NoopTerminalHook).build();

        runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        runtime
            .submit(RunSpec {
                input: b"y".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        assert!(runtime.inner.core.submit_locks.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn duplicate_submit_across_restart_is_idempotent_and_rejects_a_changed_input() {
        // Build a runtime, submit a run, then drop the runtime entirely
        // (simulating a process restart of the workflow layer) while
        // keeping the underlying Queue alive. The next runtime instance
        // sees a fresh in-memory registry but must still treat a
        // re-submit as idempotent because the durable run record persists
        // through the enqueue_with_kv path.
        struct PauseRunner;
        impl StepRunner for PauseRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                std::future::pending().await
            }
        }

        let (queue, store) = fresh_queue().await;

        // Submit via the first runtime, drop it without starting its
        // worker loop or going terminal.
        {
            let runtime = WorkflowRuntime::builder(
                queue.clone(),
                store.clone(),
                PauseRunner,
                NoopTerminalHook,
            )
            .build();
            runtime
                .submit(RunSpec {
                    run_id: Some("durable-id".to_string()),
                    input: b"x".to_vec(),
                    ..Default::default()
                })
                .await
                .unwrap();
        }

        // The durable record is queryable independently of any runtime.
        assert!(
            queue
                .kv_get(&run_kv_key("durable-id"))
                .await
                .unwrap()
                .is_some(),
            "durable run record must persist past runtime drop"
        );

        // Fresh runtime, same queue. The registry is empty here, so the
        // duplicate verdict can only come from the durable KV record.
        let runtime2 =
            WorkflowRuntime::builder(queue.clone(), store.clone(), PauseRunner, NoopTerminalHook)
                .build();
        let outcome = runtime2
            .submit(RunSpec {
                run_id: Some("durable-id".to_string()),
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(outcome.run_id, "durable-id");
        assert!(!outcome.newly_submitted);

        let err = runtime2
            .submit(RunSpec {
                run_id: Some("durable-id".to_string()),
                input: b"y".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(matches!(&err, Error::InputMismatch(id) if id == "durable-id"));
    }

    #[tokio::test(start_paused = true)]
    async fn reserved_header_on_submit_is_rejected() {
        let (queue, store) = fresh_queue().await;
        let runtime: WorkflowRuntime<ScriptedRunner, NoopTerminalHook> = WorkflowRuntime::builder(
            queue,
            store.clone(),
            ScriptedRunner::new(vec![]),
            NoopTerminalHook,
        )
        .build();
        let mut headers = HashMap::new();
        headers.insert("workflow.run_id".to_string(), "evil".to_string());

        let err = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                headers,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::ReservedHeaderInSubmit(k) if k == "workflow.run_id"),
            "got: {err:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn user_headers_thread_through_to_terminal_hook() {
        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store.clone(),
            ScriptedRunner::new(vec![
                StepOutcome::continue_now(vec![]),
                StepOutcome::Succeed { result: vec![] },
            ]),
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let mut headers = HashMap::new();
        headers.insert("trace_id".to_string(), "abc-123".to_string());
        headers.insert("tenant".to_string(), "acme".to_string());

        runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                headers,
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(outcome.headers.get("trace_id").unwrap(), "abc-123");
        assert_eq!(outcome.headers.get("tenant").unwrap(), "acme");
        // Reserved keys must not leak through.
        assert!(!outcome.headers.contains_key(HEADER_RUN_ID));
        assert!(!outcome.headers.contains_key(HEADER_STEP));

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn restart_resumes_at_next_step() {
        // Headline durability test: after step 0 has acked and step 1 is in
        // the queue, kill runtime A entirely and spawn runtime B on the same
        // Queue handle. B should claim and complete step 1 without re-running
        // step 0.
        //
        // To make this race-free we gate step 0's runner: the test holds the
        // gate while signalling shutdown to A so A enters drain mode without
        // ever claiming step 1. Then the gate is opened, A's spawned step-0
        // task finishes (enqueueing step 1 + acking step 0) and A exits.
        struct GatedRunner {
            gate: tokio::sync::Mutex<Option<oneshot::Receiver<Vec<u8>>>>,
        }

        impl StepRunner for GatedRunner {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                match step.step_number {
                    0 => {
                        let rx = self.gate.lock().await.take().expect("gate consumed twice");
                        let payload = rx.await.expect("gate sender dropped");
                        Ok(StepOutcome::continue_now(payload))
                    }
                    _ => std::future::pending().await,
                }
            }
        }

        struct CompleteOnStep1;
        impl StepRunner for CompleteOnStep1 {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                assert_eq!(step.step_number, 1, "runtime B should only ever see step 1");
                assert_eq!(step.payload.as_slice(), b"step1-payload");
                Ok(StepOutcome::Succeed {
                    result: b"resumed".to_vec(),
                })
            }
        }

        let (queue, store) = fresh_queue().await;

        let (gate_tx, gate_rx) = oneshot::channel::<Vec<u8>>();
        let runtime_a = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            GatedRunner {
                gate: tokio::sync::Mutex::new(Some(gate_rx)),
            },
            NoopTerminalHook,
        )
        .max_concurrent_steps(1)
        .build();

        let (shutdown_a_tx, shutdown_a_rx) = oneshot::channel::<()>();
        let worker_a = {
            let runtime_a = runtime_a.clone();
            tokio::spawn(async move {
                let _ = runtime_a
                    .run(async move {
                        let _ = shutdown_a_rx.await;
                    })
                    .await;
            })
        };

        let handle = runtime_a
            .submit(RunSpec {
                input: b"input".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Wait for runtime A to claim step 0 and reach the gate (registry
        // shows Running for step 0).
        for _ in 0..80 {
            if let Some(s) = runtime_a.status(&handle.run_id).await
                && s.state == RunState::Running
                && s.current_step == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let s = runtime_a.status(&handle.run_id).await.expect("status");
        assert_eq!(s.state, RunState::Running);
        assert_eq!(s.current_step, 0);

        // A's worker is in the at-capacity select-loop. Signal shutdown
        // first, then open the gate so step 0 finishes processing inside
        // drain mode (A will not claim step 1).
        let _ = shutdown_a_tx.send(());
        let _ = gate_tx.send(b"step1-payload".to_vec());

        worker_a.await.expect("runtime A drained cleanly");

        // Bring up runtime B on the same Queue handle. It should pick up
        // step 1 from where A left off.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime_b =
            WorkflowRuntime::builder(queue, store.clone(), CompleteOnStep1, ChannelHook { tx })
                .build();
        let shutdown_b = spawn_runtime(runtime_b.clone());

        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("hook fired in time")
            .expect("hook channel open");

        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.status, TerminalStatus::Succeeded);
        assert_eq!(outcome.result.as_deref(), Some(b"resumed".as_slice()));
        assert_eq!(outcome.final_step, 1);

        let _ = shutdown_b.send(());
    }

    #[test]
    fn remaining_delay_measures_from_stored_timestamp() {
        let delay = Duration::from_secs(10);
        assert_eq!(remaining_delay(1_000, 4_000, delay), Duration::from_secs(7));
        assert_eq!(remaining_delay(1_000, 20_000, delay), Duration::ZERO);
        assert_eq!(remaining_delay(5_000, 1_000, delay), delay);
    }

    #[tokio::test(start_paused = true)]
    async fn step_output_replay_skips_runner_after_crash_before_ack() {
        struct ContinueRunner {
            calls: Arc<AtomicU32>,
        }

        impl StepRunner for ContinueRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(StepOutcome::continue_now(b"step1-payload".to_vec()))
            }
        }

        let (queue, store) = fresh_queue().await;
        let calls = Arc::new(AtomicU32::new(0));
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            ContinueRunner {
                calls: calls.clone(),
            },
            NoopTerminalHook,
        )
        .step_output_replay()
        .build();

        runtime
            .submit(RunSpec {
                run_id: Some("replay-run".to_string()),
                input: b"input".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let job = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        // Simulate a crash after the step output was stored but before
        // the settlement committed: discard the effects of the first
        // delivery so nothing is enqueued.
        let _ = runtime
            .inner
            .process_step(&job, &LeaseHandle::detached())
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Re-processing the same claimed record replays the stored step
        // outcome without invoking the runner a second time.
        let effects = runtime
            .inner
            .process_step(&job, &LeaseHandle::detached())
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        queue.ack_with(&job, effects).await.unwrap();

        let next = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(next.payload.as_slice(), b"step1-payload");
        assert_eq!(next.headers.get(HEADER_RUN_ID).unwrap(), "replay-run");
        assert_eq!(next.headers.get(HEADER_STEP).unwrap(), "1");
        assert!(
            queue
                .claim("workflow-steps", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none(),
            "the replayed continue must enqueue step 1 exactly once",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn corrupt_step_output_replay_entry_falls_back_to_runner() {
        struct ContinueRunner {
            calls: Arc<AtomicU32>,
        }

        impl StepRunner for ContinueRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(StepOutcome::continue_now(b"step1-payload".to_vec()))
            }
        }

        let (queue, store) = fresh_queue().await;
        let calls = Arc::new(AtomicU32::new(0));
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            ContinueRunner {
                calls: calls.clone(),
            },
            NoopTerminalHook,
        )
        .step_output_replay()
        .build();

        runtime
            .submit(RunSpec {
                run_id: Some("corrupt-run".to_string()),
                input: b"input".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let job = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        runtime
            .inner
            .core
            .memo_store
            .put_step_output("corrupt-run", 0, &job.payload, b"not msgpack")
            .await
            .unwrap();

        runtime
            .inner
            .process_step(&job, &LeaseHandle::detached())
            .await
            .unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "corrupt entry is treated as a miss",
        );

        // The recomputed outcome overwrites the corrupt entry, so a
        // second delivery replays it without invoking the runner again.
        runtime
            .inner
            .process_step(&job, &LeaseHandle::detached())
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn step_output_replay_of_terminal_outcome_skips_runner() {
        struct SucceedRunner {
            calls: Arc<AtomicU32>,
        }

        impl StepRunner for SucceedRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(StepOutcome::Succeed {
                    result: b"final".to_vec(),
                })
            }
        }

        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let calls = Arc::new(AtomicU32::new(0));
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            SucceedRunner {
                calls: calls.clone(),
            },
            ChannelHook { tx },
        )
        .step_output_replay()
        .build();

        runtime
            .submit(RunSpec {
                run_id: Some("terminal-replay".to_string()),
                input: b"input".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let job = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        runtime
            .inner
            .process_step(&job, &LeaseHandle::detached())
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Redelivery after a crash before ack: the stored outcome
        // settles the run without invoking the runner again.
        let effects = runtime
            .inner
            .process_step(&job, &LeaseHandle::detached())
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        queue.ack_with(&job, effects).await.unwrap();

        // The committed settlement enqueued one notification; the hook
        // observes the replayed outcome when it is processed.
        let notification = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let effects = runtime
            .inner
            .process_step(&notification, &LeaseHandle::detached())
            .await
            .unwrap();
        queue.ack_with(&notification, effects).await.unwrap();
        let outcome = rx.recv().await.unwrap();
        assert_eq!(outcome.status, TerminalStatus::Succeeded);
        assert_eq!(outcome.result.as_deref(), Some(b"final".as_slice()));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// Submits a run whose runner always returns
    /// [`StepError::transient`], capped at `max_attempts`. Asserts the
    /// runner is invoked exactly `max_attempts` times (per-step max-attempts
    /// propagation) and that the terminal hook fires Failed exactly once on
    /// the final attempt (fire-once-on-last-attempt logic).
    async fn assert_transient_retries_until_max(max_attempts: u32) {
        struct AlwaysTransient {
            calls: Arc<AtomicU32>,
        }
        impl StepRunner for AlwaysTransient {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Err(StepError::transient("flaky"))
            }
        }

        let (queue, store) = fresh_queue_fast_retry().await;
        let calls = Arc::new(AtomicU32::new(0));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            AlwaysTransient {
                calls: calls.clone(),
            },
            ChannelHook { tx },
        )
        .memo_retention(Duration::from_secs(60))
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                max_attempts_per_step: Some(max_attempts),
                ..Default::default()
            })
            .await
            .unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("hook fired in time")
            .expect("hook channel open");

        assert_eq!(outcome.status, TerminalStatus::Failed);
        assert_eq!(outcome.error.as_deref(), Some("flaky"));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            max_attempts,
            "runner called once per attempt up to max_attempts"
        );

        // Settle window: assert no duplicate hook fires after the terminal one.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err(), "hook fired more than once");

        // The notification job was enqueued with the exhausted nack, so
        // its effects are committed once the hook fires.
        assert_eq!(queue.stats("workflow-steps").await.unwrap().dead, 1);
        assert!(
            queue
                .kv_get(&run_kv_key(&handle.run_id))
                .await
                .unwrap()
                .is_none(),
            "the run record delete rides the exhausted nack",
        );
        assert_eq!(
            terminal_markers(&queue)
                .await
                .iter()
                .filter(|(run_id, _)| *run_id == handle.run_id)
                .count(),
            1,
            "the terminal marker rides the exhausted nack",
        );

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_outcome_terminates_run_without_dead_letter() {
        // `StepOutcome::Cancel` is the runner's cancellation verdict path:
        // the hook fires with Cancelled, the registry is cleaned up, the
        // step is acked, and no dead job is left behind.
        struct CancellingRunner;
        impl StepRunner for CancellingRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                Ok(StepOutcome::Cancel {
                    reason: "upstream aborted".to_string(),
                })
            }
        }

        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            CancellingRunner,
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("hook fired in time")
            .expect("hook channel open");

        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        assert_eq!(outcome.error.as_deref(), Some("upstream aborted"));
        assert!(runtime.status(&handle.run_id).await.is_none());

        let stats = queue.stats("workflow-steps").await.unwrap();
        assert_eq!(stats.dead, 0, "Cancel verdict must not dead-letter");

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_persisted_cancellation_survives_a_rebuilt_registry() {
        // Models a restart: the queue and the job's persisted
        // `cancel_requested` survive while a fresh runtime starts with an
        // empty registry. The runner returns Succeed, so a Cancelled
        // outcome shows the cancellation was read from the claim.
        let (queue, store, _clock) = fresh_queue_with_mock_clock(10_000).await;
        let (tx_a, _rx_a) = tokio::sync::mpsc::unbounded_channel();
        let before = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            ScriptedRunner::new(vec![StepOutcome::Succeed {
                result: b"done".to_vec(),
            }]),
            ChannelHook { tx: tx_a },
        )
        .build();

        let handle = before
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let claim = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("step 0 is claimable");
        assert!(before.cancel(&handle.run_id).await.unwrap());

        let (tx_b, mut rx_b) = tokio::sync::mpsc::unbounded_channel();
        let after = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            ScriptedRunner::new(vec![StepOutcome::Succeed {
                result: b"done".to_vec(),
            }]),
            ChannelHook { tx: tx_b },
        )
        .build();
        assert!(
            after.status(&handle.run_id).await.is_none(),
            "the fresh runtime holds no registry entry for the run",
        );

        let effects = after
            .inner
            .process_step(&claim, &queue.lease_handle(&claim))
            .await
            .unwrap();
        queue.ack_with(&claim, effects).await.unwrap();

        let notification = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("the terminal notification is claimable");
        let effects = after
            .inner
            .process_step(&notification, &LeaseHandle::detached())
            .await
            .unwrap();
        queue.ack_with(&notification, effects).await.unwrap();

        let outcome = rx_b.recv().await.unwrap();
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        assert!(outcome.result.is_none(), "the succeed payload is discarded");
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_a_pending_run_commits_its_marker_and_fires_the_hook() {
        // Pending case: a run sits in the queue, we call `cancel()` before
        // any worker claims it. `cancel` removes the step job and enqueues
        // the notification before returning.
        struct UnreachableRunner;
        impl StepRunner for UnreachableRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                unreachable!("worker must not claim the cancelled step");
            }
        }

        let (queue, store, _clock) = fresh_queue_with_mock_clock(10_000).await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            UnreachableRunner,
            ChannelHook { tx },
        )
        .memo_retention(Duration::from_secs(60))
        .build();
        // Note: deliberately do NOT spawn the worker loop, so the submitted
        // step stays Pending in the queue while we cancel it.

        let mut headers = HashMap::new();
        headers.insert("tenant".to_string(), "acme".to_string());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                headers,
                ..Default::default()
            })
            .await
            .unwrap();
        let status = runtime.status(&handle.run_id).await.expect("active");
        assert_eq!(status.state, RunState::Pending);

        let was_cancelled = runtime.cancel(&handle.run_id).await.unwrap();
        assert!(was_cancelled);
        assert!(runtime.status(&handle.run_id).await.is_none());

        // The marker and the run record's delete commit with the removal,
        // before the notification is processed.
        let markers = terminal_markers(&queue).await;
        assert_eq!(markers, vec![(handle.run_id.clone(), 10_000)]);
        assert_eq!(
            queue.kv_get(&run_kv_key(&handle.run_id)).await.unwrap(),
            None,
        );

        // The step job is gone; the one claimable job is the notification.
        let notification = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let effects = runtime
            .inner
            .process_step(&notification, &LeaseHandle::detached())
            .await
            .unwrap();
        queue.ack_with(&notification, effects).await.unwrap();
        let outcome = rx.recv().await.unwrap();
        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        // External cancellation carries no reason: `error` is `None`.
        assert!(outcome.error.is_none());
        assert_eq!(outcome.headers.get("tenant").unwrap(), "acme");

        let stats = queue.stats("workflow-steps").await.unwrap();
        assert_eq!(stats.dead, 0, "cancel must not dead-letter");
        assert_eq!(stats.pending, 0, "cancelled job must be removed");
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_during_running_step_overrides_outcome() {
        // Running case: the step is in-flight when cancel is called. The
        // runner's eventual outcome is discarded; the worker fires Cancelled.
        struct GatedRunner {
            claimed: Arc<tokio::sync::Notify>,
            gate: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
        }
        impl StepRunner for GatedRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                self.claimed.notify_one();
                let rx = self.gate.lock().await.take().expect("gate consumed twice");
                let _ = rx.await;
                // The runner "successfully completes" the step, but cancel
                // was requested mid-flight so the outcome should be ignored
                // and the hook should fire Cancelled instead.
                Ok(StepOutcome::Succeed {
                    result: b"would-have-succeeded".to_vec(),
                })
            }
        }

        let (queue, store) = fresh_queue().await;
        let claimed = Arc::new(tokio::sync::Notify::new());
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        let (hook_tx, mut hook_rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            GatedRunner {
                claimed: claimed.clone(),
                gate: tokio::sync::Mutex::new(Some(gate_rx)),
            },
            ChannelHook { tx: hook_tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), claimed.notified())
            .await
            .expect("runner reached gate");

        let was_cancelled = runtime.cancel(&handle.run_id).await.unwrap();
        assert!(was_cancelled);

        // Let the runner finish. The worker should observe `cancel_requested`
        // and fire Cancelled rather than advancing or firing Succeeded.
        let _ = gate_tx.send(());

        let outcome = tokio::time::timeout(Duration::from_secs(2), hook_rx.recv())
            .await
            .expect("hook fired")
            .expect("hook channel open");
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        assert!(
            outcome.result.is_none(),
            "succeed payload must be discarded"
        );
        assert!(runtime.status(&handle.run_id).await.is_none());

        let stats = queue.stats("workflow-steps").await.unwrap();
        assert_eq!(stats.dead, 0);

        let _ = shutdown.send(());
    }

    /// Drive a single step that blocks on a gate, calls `cancel(run_id)`
    /// while the step is in-flight, and then has the runner return the
    /// supplied error. Asserts that external cancellation suppresses the
    /// error path entirely: the hook fires `Cancelled` (not `Failed`),
    /// no dead-letter is produced regardless of `permanent`/`transient`,
    /// and the worker returns `Ok` (no retry, no PermanentFailure
    /// propagation).
    async fn assert_cancel_suppresses_runner_error(error: StepError) {
        struct GatedErrRunner {
            claimed: Arc<tokio::sync::Notify>,
            gate: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
            calls: Arc<AtomicU32>,
            error: StdMutex<Option<StepError>>,
        }
        impl StepRunner for GatedErrRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.claimed.notify_one();
                let rx = self.gate.lock().await.take().expect("gate consumed twice");
                let _ = rx.await;
                Err(self
                    .error
                    .lock()
                    .unwrap()
                    .take()
                    .expect("error consumed twice"))
            }
        }

        let (queue, store) = fresh_queue_fast_retry().await;
        let claimed = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicU32::new(0));
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        let (hook_tx, mut hook_rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            GatedErrRunner {
                claimed: claimed.clone(),
                gate: tokio::sync::Mutex::new(Some(gate_rx)),
                calls: calls.clone(),
                error: StdMutex::new(Some(error)),
            },
            ChannelHook { tx: hook_tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), claimed.notified())
            .await
            .expect("runner reached gate");

        let was_cancelled = runtime.cancel(&handle.run_id).await.unwrap();
        assert!(was_cancelled);

        // Release the runner. It returns Err; without cancellation this
        // would either dead-letter (permanent) or nack for retry
        // (transient). Cancellation must suppress both.
        let _ = gate_tx.send(());

        let outcome = tokio::time::timeout(Duration::from_secs(2), hook_rx.recv())
            .await
            .expect("hook fired")
            .expect("hook channel open");
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        assert!(
            outcome.error.is_none(),
            "external cancel must carry no reason (Some(_) would imply runner-issued StepOutcome::Cancel)",
        );
        assert!(runtime.status(&handle.run_id).await.is_none());

        // Settle window: assert no retry attempt and no dead-letter or
        // duplicate hook fires after the terminal one.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "cancellation must suppress retries",
        );
        let stats = queue.stats("workflow-steps").await.unwrap();
        assert_eq!(stats.dead, 0, "cancellation must suppress dead-letter");
        assert!(
            hook_rx.try_recv().is_err(),
            "hook must fire exactly once for the cancelled run",
        );

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_suppresses_a_runner_error() {
        // Without cancellation, `StepError::permanent` dead-letters the
        // step and `StepError::transient` nacks for retry. With an
        // external cancel in flight, the worker must ack and fire
        // `Cancelled` instead, without re-invoking the runner.
        assert_cancel_suppresses_runner_error(StepError::permanent("would-dead-letter")).await;
        assert_cancel_suppresses_runner_error(StepError::transient("would-retry")).await;
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_signals_step_token_for_cooperative_short_circuit() {
        // A runner that watches `step.cancel_token` should short-circuit
        // long after-claim work as soon as `WorkflowRuntime::cancel` is
        // called. Without the token, cancellation latency is bounded by
        // step duration; with it, the runner returns essentially
        // immediately. The test pins this by using a step that would
        // otherwise sleep for 30 seconds; if the token didn't fire, the
        // test would time out.
        struct CooperativeRunner {
            claimed: Arc<tokio::sync::Notify>,
        }
        impl StepRunner for CooperativeRunner {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                self.claimed.notify_one();
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {
                        Ok(StepOutcome::Succeed { result: b"slow".to_vec() })
                    }
                    _ = step.cancel_token.cancelled() => {
                        Ok(StepOutcome::Cancel { reason: "cooperative".to_string() })
                    }
                }
            }
        }

        let (queue, store) = fresh_queue().await;
        let claimed = Arc::new(tokio::sync::Notify::new());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            CooperativeRunner {
                claimed: claimed.clone(),
            },
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), claimed.notified())
            .await
            .expect("runner observed token");

        let start = std::time::Instant::now();
        let was_cancelled = runtime.cancel(&handle.run_id).await.unwrap();
        assert!(was_cancelled);

        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("hook fired well before the 30s sleep would have")
            .expect("hook channel open");
        let elapsed = start.elapsed();

        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        // Runner-issued Cancel wins precedence over external cancel, so
        // the runner's reason surfaces.
        assert_eq!(outcome.error.as_deref(), Some("cooperative"));
        assert!(
            elapsed < Duration::from_secs(2),
            "cooperative cancel must short-circuit the 30s sleep (took {elapsed:?})",
        );
        assert!(runtime.status(&handle.run_id).await.is_none());

        let stats = queue.stats("workflow-steps").await.unwrap();
        assert_eq!(stats.dead, 0);

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn double_cancel_fires_hook_once_and_second_call_returns_false() {
        // Submit a run and cancel twice while it sits pending. The first
        // call removes the queued step, fires the hook, and drops the
        // registry entry. The second call must see no entry and report
        // `Ok(false)`; crucially, the hook must NOT fire a second
        // time.
        struct UnreachableRunner;
        impl StepRunner for UnreachableRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                unreachable!("worker must not claim the cancelled step");
            }
        }

        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            UnreachableRunner,
            ChannelHook { tx },
        )
        .build();
        // Deliberately do not spawn the worker loop, so step 0 stays
        // Pending while both cancels race.

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let first = runtime.cancel(&handle.run_id).await.unwrap();
        assert!(first, "first cancel initiates termination");

        let second = runtime.cancel(&handle.run_id).await.unwrap();
        assert!(
            !second,
            "second cancel must report Ok(false): the first removed the registry entry",
        );

        // Exactly one notification job exists for the double-cancelled run.
        let notification = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("the first cancel enqueued the notification");
        let effects = runtime
            .inner
            .process_step(&notification, &LeaseHandle::detached())
            .await
            .unwrap();
        queue.ack_with(&notification, effects).await.unwrap();
        let _ = rx.recv().await.unwrap();
        assert!(
            queue
                .claim("workflow-steps", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none(),
            "a double-cancelled run must enqueue one notification",
        );
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_returns_false_for_a_terminated_or_unknown_run() {
        // Submit a run that succeeds normally, wait for the terminal
        // hook, then call `cancel`. The registry entry was removed when
        // the success hook fired, so `cancel` must report `Ok(false)`
        // and must not fire a second hook.
        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store.clone(),
            ScriptedRunner::new(vec![StepOutcome::Succeed {
                result: b"done".to_vec(),
            }]),
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("Succeeded hook fired")
            .expect("hook channel open");
        assert_eq!(outcome.status, TerminalStatus::Succeeded);
        assert!(runtime.status(&handle.run_id).await.is_none());

        let was_cancelled = runtime.cancel(&handle.run_id).await.unwrap();
        assert!(
            !was_cancelled,
            "cancel on an already-terminated run must report Ok(false)",
        );

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            rx.try_recv().is_err(),
            "no Cancelled hook may fire after the run already terminated as Succeeded",
        );

        assert!(!runtime.cancel("never-submitted").await.unwrap());

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn status_reports_cancelling_while_termination_in_flight() {
        // Once `cancel()` has been called but the terminal hook hasn't
        // fired yet, `status()` should report `RunState::Cancelling` so
        // external observers can see termination is in progress. A gated
        // runner holds the cancellation window open long enough to
        // observe it deterministically.
        struct GatedRunner {
            claimed: Arc<tokio::sync::Notify>,
            gate: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
        }
        impl StepRunner for GatedRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                self.claimed.notify_one();
                let rx = self.gate.lock().await.take().expect("gate consumed twice");
                let _ = rx.await;
                Ok(StepOutcome::Succeed {
                    result: b"would-have-succeeded".to_vec(),
                })
            }
        }

        let (queue, store) = fresh_queue().await;
        let claimed = Arc::new(tokio::sync::Notify::new());
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store.clone(),
            GatedRunner {
                claimed: claimed.clone(),
                gate: tokio::sync::Mutex::new(Some(gate_rx)),
            },
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), claimed.notified())
            .await
            .expect("runner reached gate");

        // Before cancel: runner is in flight, state is Running.
        let before = runtime.status(&handle.run_id).await.expect("active");
        assert_eq!(before.state, RunState::Running);

        runtime.cancel(&handle.run_id).await.unwrap();

        // After cancel but before the gate is released: the step is still
        // in flight, but the cancellation overlay must dominate the
        // reported state.
        let during = runtime
            .status(&handle.run_id)
            .await
            .expect("entry retained while termination is in flight");
        assert_eq!(during.state, RunState::Cancelling);

        // Release the runner; the worker observes cancel_requested and
        // settles the run as Cancelled, removing the entry.
        let _ = gate_tx.send(());

        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("hook fired")
            .expect("hook channel open");
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        assert!(runtime.status(&handle.run_id).await.is_none());

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn transient_retries_until_max_attempts() {
        assert_transient_retries_until_max(1).await;
        assert_transient_retries_until_max(3).await;
    }

    #[tokio::test(start_paused = true)]
    async fn step_memo_survives_across_attempts_of_the_same_step() {
        // First attempt writes the memo entry and returns a transient
        // error so the runtime retries the step. The second attempt
        // reads the same key back and succeeds. This exercises the
        // central use case: at-least-once retries of one step should
        // short-circuit work the prior attempt already did.
        struct MemoRetryRunner;
        impl StepRunner for MemoRetryRunner {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                if step.attempts == 1 {
                    step.memo
                        .put("cached", b"first-attempt-value")
                        .await
                        .map_err(|e| StepError::transient(e.to_string()))?;
                    return Err(StepError::transient("force a retry"));
                }
                let got = step
                    .memo
                    .get("cached")
                    .await
                    .map_err(|e| StepError::transient(e.to_string()))?;
                assert_eq!(got, Some(b"first-attempt-value".to_vec()));
                Ok(StepOutcome::Succeed {
                    result: got.unwrap_or_default(),
                })
            }
        }

        let (queue, store) = fresh_queue_fast_retry().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime =
            WorkflowRuntime::builder(queue, store, MemoRetryRunner, ChannelHook { tx }).build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: b"start".to_vec(),
                max_attempts_per_step: Some(3),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("hook fired in time")
            .expect("hook channel open");
        assert_eq!(outcome.status, TerminalStatus::Succeeded);
        assert_eq!(
            outcome.result.as_deref(),
            Some(b"first-attempt-value".as_slice())
        );

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn no_terminal_marker_when_memo_retention_is_unset() {
        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store.clone(),
            ScriptedRunner::new(vec![StepOutcome::Succeed {
                result: b"done".to_vec(),
            }]),
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: b"in".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert!(terminal_markers(&runtime.inner.core.queue).await.is_empty());

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_marker_is_written_for_failed_runs_too() {
        // Retention isn't just for successful runs: replay-from-cached-state
        // is precisely the failed-run use case.
        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store.clone(),
            ScriptedRunner::new(vec![StepOutcome::Fail {
                reason: "boom".into(),
            }]),
            ChannelHook { tx },
        )
        .memo_retention(Duration::from_secs(60))
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"in".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.status, TerminalStatus::Failed);

        let markers = terminal_markers(&runtime.inner.core.queue).await;
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].0, handle.run_id);

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_marker_is_written_at_the_runtime_clock() {
        // The queue's MockClock is shared into the runtime by default
        // (via Queue::clock()), so a `clock.advance` between submit and
        // terminate is visible in the marker's terminal_at_ms.
        let (queue, store, clock) = fresh_queue_with_mock_clock(10_000).await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store.clone(),
            ScriptedRunner::new(vec![StepOutcome::Succeed {
                result: b"done".to_vec(),
            }]),
            ChannelHook { tx },
        )
        .memo_retention(Duration::from_secs(60))
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"in".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        advance(&clock, Duration::from_secs(30)).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();

        let markers = terminal_markers(&runtime.inner.core.queue).await;
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].0, handle.run_id);
        // MockClock only moves on explicit advance/set, so the value the
        // effects builder reads is exactly the post-advance clock.
        assert_eq!(markers[0].1, 10_000 + 30_000);

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn submit_rejects_an_unusable_run_id() {
        let (queue, store, _clock) = fresh_queue_with_mock_clock(10_000).await;
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store,
            ScriptedRunner::new(vec![]),
            ChannelHook { tx },
        )
        .build();

        for bad in [
            "",
            "run/1",
            "run 1",
            "run:1",
            &"a".repeat(MAX_RUN_ID_LEN + 1),
        ] {
            let err = runtime
                .submit(RunSpec {
                    run_id: Some(bad.to_string()),
                    input: b"x".to_vec(),
                    ..Default::default()
                })
                .await
                .expect_err("run id must be rejected");
            assert!(
                matches!(err, Error::InvalidRunId { .. }),
                "unexpected error for `{bad}`: {err}",
            );
        }

        let ok = runtime
            .submit(RunSpec {
                run_id: Some("a".repeat(MAX_RUN_ID_LEN)),
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await;
        assert!(ok.is_ok(), "a run id at the limit must be accepted");
    }

    #[tokio::test(start_paused = true)]
    async fn an_empty_run_id_cannot_reach_the_retention_sweep() {
        // An empty run id would resolve to the memo prefix itself, and the
        // sweep would then remove every run's entries.
        let (queue, store, clock) = fresh_queue_with_mock_clock(10_000).await;
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            ScriptedRunner::new(vec![]),
            ChannelHook { tx },
        )
        .memo_retention(Duration::from_secs(60))
        .build();

        let memos = MemoStore::new(store, "workflow-memo");
        memos
            .new_memo("bystander", 0)
            .put("k", b"expensive")
            .await
            .unwrap();

        assert!(
            runtime
                .submit(RunSpec {
                    run_id: Some(String::new()),
                    input: b"x".to_vec(),
                    ..Default::default()
                })
                .await
                .is_err()
        );
        assert!(terminal_markers(&queue).await.is_empty());

        advance(&clock, Duration::from_secs(3_600)).await;
        runtime.inner.core.sweep_once().await.unwrap();
        assert_eq!(
            memos.new_memo("bystander", 0).get("k").await.unwrap(),
            Some(b"expensive".to_vec()),
            "an unrelated run's memo entries must survive",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_malformed_terminal_marker_is_deleted_without_clearing_memos() {
        let (queue, store, clock) = fresh_queue_with_mock_clock(10_000).await;
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            ScriptedRunner::new(vec![]),
            ChannelHook { tx },
        )
        .memo_retention(Duration::from_secs(60))
        .build();

        let memos = MemoStore::new(store, "workflow-memo");
        memos
            .new_memo("bystander", 0)
            .put("k", b"expensive")
            .await
            .unwrap();
        let marker = terminal_kv_key("", 0);
        queue.kv_put(&marker, b"").await.unwrap();
        let mut unparseable = Vec::from(TERMINAL_KV_PREFIX);
        unparseable.extend_from_slice(b"not-a-timestamp");
        queue.kv_put(&unparseable, b"").await.unwrap();

        advance(&clock, Duration::from_secs(3_600)).await;
        runtime.inner.core.sweep_once().await.unwrap();
        assert_eq!(
            memos.new_memo("bystander", 0).get("k").await.unwrap(),
            Some(b"expensive".to_vec()),
            "an unrelated run's memo entries must survive",
        );
        assert!(
            queue.kv_get(&marker).await.unwrap().is_none(),
            "the marker is removed and not retried on every sweep",
        );
        assert!(queue.kv_get(&unparseable).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn clear_memos_for_run_rejects_an_empty_run_id() {
        let store: Arc<dyn taquba::object_store::ObjectStore> = Arc::new(InMemory::new());
        let memos = MemoStore::new(store, "workflow-memo");
        memos
            .new_memo("bystander", 0)
            .put("k", b"expensive")
            .await
            .unwrap();
        assert!(matches!(
            memos.clear_memos_for_run("").await,
            Err(Error::InvalidRunId { .. })
        ));
        assert_eq!(
            memos.new_memo("bystander", 0).get("k").await.unwrap(),
            Some(b"expensive".to_vec()),
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_notification_job_creates_no_registry_entry() {
        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store,
            ScriptedRunner::new(vec![StepOutcome::Succeed {
                result: b"done".to_vec(),
            }]),
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        // The hook runs as the notification job's worker, so an entry
        // created by that job's dispatch is visible once the hook fires.
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("hook fired in time")
            .expect("hook channel open");
        assert!(
            runtime.inner.core.registry.is_empty(),
            "a notification job must not touch the run registry",
        );

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_a_running_step_writes_no_terminal_marker_until_it_settles() {
        // The `Requested` arm: the queue must discard the effects, since
        // a marker written here would mark a still-executing run.
        struct GatedRunner {
            claimed: Arc<tokio::sync::Notify>,
            gate: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
        }
        impl StepRunner for GatedRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                self.claimed.notify_one();
                let rx = self.gate.lock().await.take().expect("gate consumed twice");
                let _ = rx.await;
                Ok(StepOutcome::Succeed {
                    result: b"done".to_vec(),
                })
            }
        }

        let (queue, store, _clock) = fresh_queue_with_mock_clock(10_000).await;
        let claimed = Arc::new(tokio::sync::Notify::new());
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            GatedRunner {
                claimed: claimed.clone(),
                gate: tokio::sync::Mutex::new(Some(gate_rx)),
            },
            ChannelHook { tx },
        )
        .memo_retention(Duration::from_secs(60))
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), claimed.notified())
            .await
            .expect("runner reached gate");

        assert!(runtime.cancel(&handle.run_id).await.unwrap());
        assert!(
            terminal_markers(&queue).await.is_empty(),
            "a run still executing its step must have no terminal marker",
        );
        assert!(
            queue
                .kv_get(&run_kv_key(&handle.run_id))
                .await
                .unwrap()
                .is_some(),
            "the run record must survive a cancel the worker has to finish",
        );

        let _ = gate_tx.send(());
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("hook fired")
            .expect("hook channel open");
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        assert_eq!(
            terminal_markers(&queue).await,
            vec![(handle.run_id.clone(), 10_000)],
            "the worker's settlement writes it",
        );

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_permanent_step_error_commits_its_terminal_marker() {
        struct FailingRunner;
        impl StepRunner for FailingRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                Err(StepError::permanent("nope"))
            }
        }

        let (queue, store, _clock) = fresh_queue_with_mock_clock(10_000).await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            FailingRunner,
            ChannelHook { tx },
        )
        .memo_retention(Duration::from_secs(60))
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.status, TerminalStatus::Failed);

        // The notification exists only because the dead-letter committed,
        // so the marker and the dead job are already observable.
        assert_eq!(
            terminal_markers(&queue).await,
            vec![(handle.run_id.clone(), 10_000)],
        );
        assert_eq!(queue.stats("workflow-steps").await.unwrap().dead, 1);

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_retrying_step_error_commits_no_terminal_marker() {
        // A transient failure with attempts left nacks for retry, and
        // `nack_with` discards the effects on that branch.
        struct FlakyRunner {
            attempts: Arc<std::sync::atomic::AtomicUsize>,
            clock: MockClock,
        }
        impl StepRunner for FlakyRunner {
            async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
                if self
                    .attempts
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    == 0
                {
                    return Err(StepError::transient("flaky"));
                }
                // Separate the two settlements in clock time before the
                // succeeding one: a marker wrongly written for the retry
                // would otherwise share this one's key and go unobserved.
                self.clock.advance(Duration::from_secs(1));
                Ok(StepOutcome::Succeed {
                    result: b"done".to_vec(),
                })
            }
        }

        let (queue, store, clock) = fresh_queue_fast_retry_with_mock_clock(10_000).await;
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            FlakyRunner {
                attempts: attempts.clone(),
                clock: clock.clone(),
            },
            ChannelHook { tx },
        )
        .memo_retention(Duration::from_secs(60))
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                max_attempts_per_step: Some(3),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.status, TerminalStatus::Succeeded);
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);

        // Exactly one marker, stamped after the retry's clock advance:
        // the retry produced none, the success did.
        let markers = terminal_markers(&queue).await;
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].0, handle.run_id);
        assert_eq!(markers[0].1, 11_000);

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn the_sweep_clears_only_markers_older_than_the_cutoff() {
        // Markers sort by timestamp, so the sweep scans from the start
        // of the range and returns at the first unexpired marker.
        let (queue, store, _clock) = fresh_queue_with_mock_clock(10_000).await;
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            ScriptedRunner::new(vec![]),
            ChannelHook { tx },
        )
        .memo_retention(Duration::from_secs(1))
        .build();

        let memos = MemoStore::new(store, "workflow-memo");
        for (run_id, at_ms) in [("old", 1_000u64), ("young", 9_500u64)] {
            memos.new_memo(run_id, 0).put("k", b"v").await.unwrap();
            queue
                .kv_put(&terminal_kv_key(run_id, at_ms), b"")
                .await
                .unwrap();
        }

        // Clock at 10_000 with 1s retention: cutoff 9_000.
        let cleared = runtime.inner.core.sweep_once().await.unwrap();
        assert_eq!(cleared, 1);

        let remaining = terminal_markers(&queue).await;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0, "young");
        assert_eq!(memos.new_memo("old", 0).get("k").await.unwrap(), None);
        assert_eq!(
            memos.new_memo("young", 0).get("k").await.unwrap(),
            Some(b"v".to_vec()),
        );
    }

    #[test]
    fn terminal_marker_keys_sort_oldest_first_and_round_trip() {
        let old = terminal_kv_key("run-b", 1_000);
        let young = terminal_kv_key("run-a", 2_000);
        assert!(
            old < young,
            "ordering must follow the timestamp ahead of the id"
        );
        assert_eq!(
            parse_timestamped_kv_key(TERMINAL_KV_PREFIX, &young),
            Some(("run-a".to_string(), 2_000)),
        );
        assert_eq!(
            parse_timestamped_kv_key(TERMINAL_KV_PREFIX, b"workflow/runs/run-a"),
            None
        );
    }

    /// Yield up to `iters` times waiting for `cond` to become true.
    /// Used in sweeper tests to let the spawned sweep task make
    /// progress between `tokio::time::advance` and the assertion;
    /// returns true if the condition held within the budget.
    async fn yield_until<F, Fut>(iters: usize, mut cond: F) -> bool
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = bool>,
    {
        for _ in 0..iters {
            if cond().await {
                return true;
            }
            tokio::task::yield_now().await;
        }
        false
    }

    #[tokio::test(start_paused = true)]
    async fn the_sweeper_clears_a_marker_only_after_retention_elapses() {
        // Retention 200ms (sweep interval also 200ms). Advancing 200ms
        // after the marker is written fires the next sweep tick at the
        // exact retention boundary; strict `<` means the marker is not
        // yet expired, so the sweep must skip it. Advancing past the
        // boundary must then clear the marker and the run's memo
        // entries.
        let (queue, store, clock) = fresh_queue_with_mock_clock(10_000).await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store.clone(),
            ScriptedRunner::new(vec![StepOutcome::Succeed {
                result: b"done".to_vec(),
            }]),
            ChannelHook { tx },
        )
        .memo_retention(Duration::from_millis(200))
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: b"in".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let memos = MemoStore::new(store.clone(), "workflow-memo");
        memos
            .new_memo(&handle.run_id, 0)
            .put("k", b"cached")
            .await
            .unwrap();

        advance(&clock, Duration::from_millis(200)).await;
        // Let the sweeper finish its boundary tick.
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        let markers = terminal_markers(&runtime.inner.core.queue).await;
        assert_eq!(markers.len(), 1, "boundary marker must not be swept");

        advance(&clock, Duration::from_millis(300)).await;
        let cleared = yield_until(50, || async {
            terminal_markers(&runtime.inner.core.queue).await.is_empty()
        })
        .await;
        assert!(cleared, "sweeper did not clear the expired marker");
        assert_eq!(
            memos.new_memo(&handle.run_id, 0).get("k").await.unwrap(),
            None,
            "sweeper did not clear the run's memo entries",
        );

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn sweeper_keeps_memos_of_runs_without_a_terminal_marker() {
        // A run gets a terminal marker only once it terminates, and a
        // terminated run never resumes. The sweep is keyed on those
        // markers, so an in-flight run's memo entries are never deleted
        // out from under a resume, even past the retention window. Here
        // a memo entry exists for a run with no terminal marker;
        // advancing well past retention must leave it in place.
        let (queue, store, clock) = fresh_queue_with_mock_clock(10_000).await;
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue,
            store.clone(),
            ScriptedRunner::new(vec![]),
            ChannelHook { tx },
        )
        .memo_retention(Duration::from_millis(100))
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let memos = MemoStore::new(store.clone(), "workflow-memo");
        memos
            .new_memo("in-flight-run", 0)
            .put("k", b"cached")
            .await
            .unwrap();

        advance(&clock, Duration::from_millis(500)).await;
        // Give the sweeper several ticks to run against the advanced clock.
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }

        assert_eq!(
            memos.new_memo("in-flight-run", 0).get("k").await.unwrap(),
            Some(b"cached".to_vec()),
            "sweep must not remove memos of a run with no terminal marker",
        );

        let _ = shutdown.send(());
    }

    async fn wait_for_kv(queue: &Queue, key: &[u8]) -> Vec<u8> {
        for _ in 0..200 {
            if let Some(v) = queue.kv_get(key).await.unwrap() {
                return v.to_vec();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "kv key `{}` was never written",
            String::from_utf8_lossy(key)
        );
    }

    async fn wait_for_drained(queue: &Queue) {
        for _ in 0..200 {
            let stats = queue.stats("workflow-steps").await.unwrap();
            if stats.pending == 0 && stats.claimed == 0 && stats.scheduled == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the queue never drained");
    }

    /// Runner that stages one `app/step-{n}` write per step and returns
    /// the next scripted result.
    struct EffectStagingRunner {
        script: Arc<StdMutex<Vec<std::result::Result<StepOutcome, StepError>>>>,
    }

    impl EffectStagingRunner {
        fn new(script: Vec<std::result::Result<StepOutcome, StepError>>) -> Self {
            Self {
                script: Arc::new(StdMutex::new(script)),
            }
        }
    }

    impl StepRunner for EffectStagingRunner {
        async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
            step.effects
                .put(format!("app/step-{}", step.step_number), b"done".to_vec())
                .map_err(|e| StepError::permanent(e.to_string()))?;
            self.script.lock().unwrap().remove(0)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn submit_kv_writes_apply_only_to_a_new_submission() {
        let (queue, store) = fresh_queue().await;
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            ScriptedRunner::new(vec![]),
            NoopTerminalHook,
        )
        .build();
        // No worker loop runs, so the step stays queued and the second
        // submit is a duplicate of an active run.
        let first = runtime
            .submit(RunSpec {
                run_id: Some("kv-run".to_string()),
                input: b"in".to_vec(),
                kv_writes: HashMap::from([(b"app/first".to_vec(), b"1".to_vec())]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(first.newly_submitted);
        assert_eq!(
            queue.kv_get(b"app/first").await.unwrap().as_deref(),
            Some(b"1".as_slice())
        );

        let duplicate = runtime
            .submit(RunSpec {
                run_id: Some("kv-run".to_string()),
                input: b"in".to_vec(),
                kv_writes: HashMap::from([(b"app/second".to_vec(), b"2".to_vec())]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(!duplicate.newly_submitted);
        assert!(queue.kv_get(b"app/second").await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_reserved_kv_key_in_submit_is_rejected() {
        let (queue, store) = fresh_queue().await;
        let runtime =
            WorkflowRuntime::builder(queue, store, ScriptedRunner::new(vec![]), NoopTerminalHook)
                .build();
        let err = runtime
            .submit(RunSpec {
                input: Vec::new(),
                kv_writes: HashMap::from([(b"workflow/x".to_vec(), b"v".to_vec())]),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ReservedKvKey(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn step_effects_commit_with_the_acking_settlement() {
        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            EffectStagingRunner::new(vec![
                Ok(StepOutcome::continue_now(b"next".to_vec())),
                Ok(StepOutcome::Succeed { result: Vec::new() }),
            ]),
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.status, TerminalStatus::Succeeded);
        assert_eq!(wait_for_kv(&queue, b"app/step-0").await, b"done");
        assert_eq!(wait_for_kv(&queue, b"app/step-1").await, b"done");

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_step_effect_is_readable_by_the_next_step() {
        struct ReadingRunner {
            read_under_staging: Arc<StdMutex<Option<Option<Vec<u8>>>>>,
        }
        impl StepRunner for ReadingRunner {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                let read = step
                    .kv
                    .get(b"app/marker")
                    .await
                    .map_err(|e| StepError::permanent(e.to_string()))?
                    .map(|b| b.to_vec());
                if step.step_number == 0 {
                    step.effects
                        .put("app/marker", b"v".to_vec())
                        .map_err(|e| StepError::permanent(e.to_string()))?;
                    let staged_read = step
                        .kv
                        .get(b"app/marker")
                        .await
                        .map_err(|e| StepError::permanent(e.to_string()))?
                        .map(|b| b.to_vec());
                    *self.read_under_staging.lock().unwrap() = Some(staged_read);
                    return Ok(StepOutcome::continue_now(Vec::new()));
                }
                Ok(StepOutcome::Succeed {
                    result: read.unwrap_or_default(),
                })
            }
        }

        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let read_under_staging = Arc::new(StdMutex::new(None));
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            ReadingRunner {
                read_under_staging: read_under_staging.clone(),
            },
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.status, TerminalStatus::Succeeded);
        assert_eq!(outcome.result.as_deref(), Some(b"v".as_slice()));
        assert_eq!(*read_under_staging.lock().unwrap(), Some(None));

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_run_memo_written_in_one_step_is_readable_in_the_next() {
        struct JournalRunner;
        impl StepRunner for JournalRunner {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                if step.step_number == 0 {
                    step.run_memo.put("journal", b"entry").await?;
                    return Ok(StepOutcome::continue_now(Vec::new()));
                }
                let value = step.run_memo.get("journal").await?;
                Ok(StepOutcome::Succeed {
                    result: value.unwrap_or_default(),
                })
            }
        }

        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime =
            WorkflowRuntime::builder(queue, store, JournalRunner, ChannelHook { tx }).build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.status, TerminalStatus::Succeeded);
        assert_eq!(outcome.result.as_deref(), Some(b"entry".as_slice()));

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn step_effects_commit_when_the_runner_fails_the_run() {
        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            EffectStagingRunner::new(vec![Ok(StepOutcome::Fail {
                reason: "denied".to_string(),
            })]),
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.status, TerminalStatus::Failed);
        assert_eq!(outcome.error.as_deref(), Some("denied"));
        assert_eq!(wait_for_kv(&queue, b"app/step-0").await, b"done");

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_runner_cancelling_its_own_token_is_not_an_external_cancel() {
        // The runner receives a child of the claim's token, so firing it
        // leaves the parent uncancelled and the step's staged effects are
        // applied.
        struct SelfCancellingRunner;
        impl StepRunner for SelfCancellingRunner {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                step.cancel_token.cancel();
                step.effects
                    .put("app/step-0", b"done")
                    .map_err(|e| StepError::permanent(e.to_string()))?;
                Ok(StepOutcome::Succeed {
                    result: b"finished".to_vec(),
                })
            }
        }

        let (queue, store, _clock) = fresh_queue_with_mock_clock(10_000).await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            SelfCancellingRunner,
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.status, TerminalStatus::Succeeded);
        assert_eq!(outcome.result.as_deref(), Some(b"finished".as_slice()));
        assert_eq!(wait_for_kv(&queue, b"app/step-0").await, b"done");

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_runner_issued_cancel_keeps_its_staged_effects() {
        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            EffectStagingRunner::new(vec![Ok(StepOutcome::Cancel {
                reason: "obsolete".to_string(),
            })]),
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        assert_eq!(outcome.error.as_deref(), Some("obsolete"));
        assert_eq!(wait_for_kv(&queue, b"app/step-0").await, b"done");

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn an_external_cancel_discards_staged_effects() {
        struct StageThenAwaitCancel {
            started: tokio::sync::mpsc::UnboundedSender<()>,
        }

        impl StepRunner for StageThenAwaitCancel {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                step.effects
                    .put(b"app/override".to_vec(), b"staged".to_vec())
                    .map_err(|e| StepError::permanent(e.to_string()))?;
                let _ = self.started.send(());
                step.cancel_token.cancelled().await;
                Ok(StepOutcome::continue_now(Vec::new()))
            }
        }

        let (queue, store) = fresh_queue().await;
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            StageThenAwaitCancel {
                started: started_tx,
            },
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let handle = runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(runtime.cancel(&handle.run_id).await.unwrap());

        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        assert_eq!(outcome.error, None);

        wait_for_drained(&queue).await;
        assert!(queue.kv_get(b"app/override").await.unwrap().is_none());

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_step_error_applies_no_staged_effects() {
        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            EffectStagingRunner::new(vec![Err(StepError::permanent("permanent failure"))]),
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.status, TerminalStatus::Failed);

        for _ in 0..200 {
            if queue.stats("workflow-steps").await.unwrap().dead == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(queue.stats("workflow-steps").await.unwrap().dead, 1);
        assert!(queue.kv_get(b"app/step-0").await.unwrap().is_none());

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn failure_writes_apply_only_with_the_terminating_failure() {
        struct FailureStagingRunner;

        impl StepRunner for FailureStagingRunner {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                step.effects
                    .put(b"app/outcome".to_vec(), b"staged".to_vec())
                    .map_err(|e| StepError::permanent(e.to_string()))?;
                step.effects
                    .put_reserved_on_failure(
                        b"workflow/test/failed".to_vec(),
                        step.attempts.to_string().into_bytes(),
                    )
                    .map_err(|e| StepError::permanent(e.to_string()))?;
                Err(StepError::transient("still failing"))
            }
        }

        let (queue, store) = fresh_queue_fast_retry().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            FailureStagingRunner,
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: Vec::new(),
                max_attempts_per_step: Some(2),
                ..Default::default()
            })
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.status, TerminalStatus::Failed);
        for _ in 0..200 {
            if queue.stats("workflow-steps").await.unwrap().dead == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(queue.stats("workflow-steps").await.unwrap().dead, 1);

        // The retried first attempt applied nothing; the exhausted second
        // attempt applied its failure write and not its outcome write.
        assert_eq!(
            queue
                .kv_get(b"workflow/test/failed")
                .await
                .unwrap()
                .as_deref(),
            Some(b"2".as_slice()),
        );
        assert!(queue.kv_get(b"app/outcome").await.unwrap().is_none());

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_replayed_step_outcome_restores_its_staged_effects() {
        struct StagingContinueRunner {
            calls: Arc<AtomicU32>,
        }

        impl StepRunner for StagingContinueRunner {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                step.effects
                    .put(b"app/replayed".to_vec(), b"v".to_vec())
                    .map_err(|e| StepError::transient(e.to_string()))?;
                step.effects
                    .delete(b"app/stale".to_vec())
                    .map_err(|e| StepError::transient(e.to_string()))?;
                Ok(StepOutcome::continue_now(b"step1".to_vec()))
            }
        }

        let (queue, store) = fresh_queue().await;
        let calls = Arc::new(AtomicU32::new(0));
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            StagingContinueRunner {
                calls: calls.clone(),
            },
            NoopTerminalHook,
        )
        .step_output_replay()
        .build();

        queue.kv_put(b"app/stale", b"old").await.unwrap();
        runtime
            .submit(RunSpec {
                run_id: Some("replay-effects".to_string()),
                input: b"input".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let job = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        // First delivery: the returned effects are dropped, simulating a
        // crash between the replay-record write and the settlement.
        let _ = runtime
            .inner
            .process_step(&job, &LeaseHandle::detached())
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(queue.kv_get(b"app/replayed").await.unwrap().is_none());

        // Redelivery replays the stored outcome and restores the staged
        // effects into the settlement without invoking the runner.
        let effects = runtime
            .inner
            .process_step(&job, &LeaseHandle::detached())
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            effects.kv_writes.get(b"app/replayed".as_slice()),
            Some(&b"v".to_vec())
        );
        assert!(effects.kv_deletes.contains(&b"app/stale".to_vec()));
        queue.ack_with(&job, effects).await.unwrap();
        assert_eq!(
            queue.kv_get(b"app/replayed").await.unwrap().as_deref(),
            Some(b"v".as_slice())
        );
        assert!(queue.kv_get(b"app/stale").await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn only_the_committed_outcome_produces_a_notification() {
        struct GatedSecondAttempt {
            calls: Arc<AtomicU32>,
            running: tokio::sync::mpsc::UnboundedSender<()>,
        }

        impl StepRunner for GatedSecondAttempt {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Ok(StepOutcome::Succeed {
                        result: b"done".to_vec(),
                    });
                }
                let _ = self.running.send(());
                step.cancel_token.cancelled().await;
                Ok(StepOutcome::Succeed {
                    result: b"done".to_vec(),
                })
            }
        }

        let (queue, store) = fresh_queue().await;
        let (running_tx, mut running_rx) = tokio::sync::mpsc::unbounded_channel();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            GatedSecondAttempt {
                calls: Arc::new(AtomicU32::new(0)),
                running: running_tx,
            },
            ChannelHook { tx },
        )
        .build();

        runtime
            .submit(RunSpec {
                run_id: Some("phantom".to_string()),
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        let job = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        // First settlement attempt: the runner succeeds, but the effects
        // are dropped, as when the settlement loses the claim. The
        // Succeeded notification is dropped with them.
        let _ = runtime
            .inner
            .process_step(&job, &queue.lease_handle(&job))
            .await
            .unwrap();

        // The redelivered attempt observes an external cancel and
        // commits Cancelled.
        let worker = {
            let inner = runtime.inner.clone();
            let queue = queue.clone();
            tokio::spawn(async move {
                let effects = inner
                    .process_step(&job, &queue.lease_handle(&job))
                    .await
                    .unwrap();
                queue.ack_with(&job, effects).await.unwrap();
            })
        };
        running_rx.recv().await.unwrap();
        assert!(runtime.cancel("phantom").await.unwrap());
        worker.await.unwrap();

        let notification = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("the committed settlement enqueued its notification");
        let effects = runtime
            .inner
            .process_step(&notification, &LeaseHandle::detached())
            .await
            .unwrap();
        queue.ack_with(&notification, effects).await.unwrap();
        let outcome = rx.recv().await.unwrap();
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        assert!(
            queue
                .claim("workflow-steps", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none(),
            "the outcome that never committed must produce no notification",
        );
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn hook_effects_commit_with_the_notification_ack() {
        struct EffectHook;

        impl TerminalHook for EffectHook {
            async fn on_termination(
                &self,
                outcome: &RunOutcome,
                effects: &TerminalEffects,
            ) -> std::result::Result<(), StepError> {
                effects
                    .put(
                        format!("app/outcomes/{}", outcome.run_id),
                        outcome.status.as_str(),
                    )
                    .map_err(|e| StepError::permanent(e.to_string()))?;
                effects
                    .enqueue(EnqueueRequest {
                        queue: "side-effects".to_string(),
                        payload: outcome.run_id.clone().into_bytes(),
                        options: EnqueueOptions::default(),
                    })
                    .map_err(|e| StepError::permanent(e.to_string()))?;
                Ok(())
            }
        }

        let (queue, store) = fresh_queue().await;
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            ScriptedRunner::new(vec![StepOutcome::Succeed { result: Vec::new() }]),
            EffectHook,
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                run_id: Some("hooked".to_string()),
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(
            wait_for_kv(&queue, b"app/outcomes/hooked").await,
            b"succeeded"
        );
        let side = queue
            .claim("side-effects", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("the staged enqueue committed with the notification ack");
        assert_eq!(side.payload.as_slice(), b"hooked");

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_transiently_failing_hook_retries_the_notification() {
        struct FlakyHook {
            calls: Arc<AtomicU32>,
        }

        impl TerminalHook for FlakyHook {
            async fn on_termination(
                &self,
                outcome: &RunOutcome,
                effects: &TerminalEffects,
            ) -> std::result::Result<(), StepError> {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(StepError::transient("first attempt fails"));
                }
                effects
                    .put(format!("app/notified/{}", outcome.run_id), b"1".to_vec())
                    .map_err(|e| StepError::permanent(e.to_string()))?;
                Ok(())
            }
        }

        let (queue, store) = fresh_queue_fast_retry().await;
        let calls = Arc::new(AtomicU32::new(0));
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            ScriptedRunner::new(vec![StepOutcome::Succeed { result: Vec::new() }]),
            FlakyHook {
                calls: calls.clone(),
            },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                run_id: Some("flaky".to_string()),
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(wait_for_kv(&queue, b"app/notified/flaky").await, b"1");
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_noop_hook_enqueues_no_notification() {
        let (queue, store) = fresh_queue().await;
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            ScriptedRunner::new(vec![StepOutcome::Succeed { result: Vec::new() }]),
            NoopTerminalHook,
        )
        .build();
        runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        let job = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let effects = runtime
            .inner
            .process_step(&job, &LeaseHandle::detached())
            .await
            .unwrap();
        assert!(effects.enqueues.is_empty());
        queue.ack_with(&job, effects).await.unwrap();
        assert!(
            queue
                .claim("workflow-steps", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[cfg(feature = "webhooks")]
    #[tokio::test(start_paused = true)]
    async fn the_webhook_hook_stages_its_delivery_as_a_notification_effect() {
        use crate::terminal::WebhookTerminalHook;

        let (queue, store) = fresh_queue().await;
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            ScriptedRunner::new(vec![
                StepOutcome::Succeed {
                    result: b"payload".to_vec(),
                },
                StepOutcome::Succeed { result: Vec::new() },
            ]),
            WebhookTerminalHook::new("callbacks"),
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                run_id: Some("with-callback".to_string()),
                input: Vec::new(),
                headers: HashMap::from([(
                    "callback_url".to_string(),
                    "https://example.com/done".to_string(),
                )]),
                ..Default::default()
            })
            .await
            .unwrap();

        let webhook = loop {
            if let Some(job) = queue
                .claim("callbacks", Duration::from_secs(30))
                .await
                .unwrap()
            {
                break job;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert_eq!(webhook.payload.as_slice(), b"payload");
        assert_eq!(
            webhook.headers.get("webhook.url").unwrap(),
            "https://example.com/done"
        );
        assert_eq!(
            webhook.headers.get("http.Workflow-Run-Status").unwrap(),
            "succeeded"
        );

        // A run without a callback header enqueues no notification.
        runtime
            .submit(RunSpec {
                run_id: Some("without-callback".to_string()),
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        wait_for_drained(&queue).await;
        assert!(
            queue
                .claim("callbacks", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_late_write_through_an_escaped_handle_is_refused() {
        struct EscapingRunner {
            escaped: Arc<StdMutex<Option<EffectsHandle>>>,
        }

        impl StepRunner for EscapingRunner {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                *self.escaped.lock().unwrap() = Some(step.effects.clone());
                Ok(StepOutcome::Succeed { result: Vec::new() })
            }
        }

        let (queue, store) = fresh_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let escaped = Arc::new(StdMutex::new(None));
        let runtime = WorkflowRuntime::builder(
            queue,
            store,
            EscapingRunner {
                escaped: escaped.clone(),
            },
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        runtime
            .submit(RunSpec {
                input: Vec::new(),
                ..Default::default()
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();

        let handle = escaped.lock().unwrap().take().unwrap();
        assert!(matches!(
            handle.put("app/late", "v"),
            Err(Error::EffectsSealed)
        ));

        let _ = shutdown.send(());
    }
}
