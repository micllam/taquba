use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::TryStreamExt;
use taquba::object_store::ObjectStore;
use taquba::{
    Clock, EnqueueOptions, EnqueueRequest, EnqueueResult, JobRecord, JobStatus, Queue,
    SettlementEffects, WorkerHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, warn};

use crate::durable::{
    self, DurableCurrentStep, DurableRunOutcome, DurableRunRecord, DurableStepOutcome,
    DurableStepOutcomeRecord, DurableTermination,
};
use crate::effects::StagedEffects;
use crate::error::{Error, Result};
use crate::group::{GroupStore, Membership, RunGroup, pending_member, terminated_member};
use crate::keys::{
    DEDUP_PREFIX, GROUP_TERMINAL_KV_PREFIX, HEADER_RUN_ID, HEADER_STEP, HEADER_TERMINAL,
    RESERVED_HEADER_PREFIX, RESERVED_KV_PREFIX, TERMINAL_KV_PREFIX, hash_input, outcome_kv_key,
    run_kv_key, step_kv_key, terminal_kv_key, validate_run_id,
};
use crate::memo::MemoStore;
use crate::runner::{StepOutcome, StepRunner, Trigger};
use crate::sweep::{Clearable, Sweep, run_periodically};
use crate::terminal::{RunOutcome, TerminalHook, TerminalStatus};
use crate::worker::{ClaimedStep, StepWorker};

/// The encoded current-step pointer for `job_id` at `step_number`.
fn current_step_bytes(step_number: u32, job_id: &str) -> Vec<u8> {
    durable::encode(&DurableCurrentStep {
        step_number,
        job_id: job_id.to_string(),
    })
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
    /// id was already active (its durable run record exists) and this
    /// call was a no-op. Call
    /// [`WorkflowRuntime::status`] for the run's current state when
    /// needed.
    pub newly_submitted: bool,
    /// The id of the queue job currently representing the run: its
    /// first step for a new submission, and the step the run has reached
    /// for a duplicate, read from the run's durable current-step pointer.
    pub job_id: String,
}

/// Status snapshot of a run, read from its durable state by
/// [`WorkflowRuntime::status`].
#[derive(Debug, Clone)]
pub struct RunStatus {
    /// The run's identifier.
    pub run_id: String,
    /// Lifecycle state of the run's current step, or its termination.
    pub state: RunState,
    /// Step number of the run's current step; the final step of a
    /// terminated run.
    pub current_step: u32,
}

/// Lifecycle state tracked in [`RunStatus::state`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RunState {
    /// A step job exists in the queue but has not yet been claimed.
    Pending,
    /// A step is currently being processed by a worker.
    Running,
    /// [`WorkflowRuntime::cancel`] was called for this run and the run
    /// has not yet terminated. Reported until the in-flight step
    /// returns and the runtime settles the run as
    /// [`crate::TerminalStatus::Cancelled`]; after that,
    /// [`WorkflowRuntime::status`] returns `None`.
    ///
    /// Only set by external cancellation. A pure runner-issued
    /// [`crate::StepOutcome::Cancel`] (with no external `cancel()`
    /// call) terminates as `Cancelled` without ever transitioning
    /// through `Cancelling`: a runner-issued cancel is observed when
    /// `run_step` returns, and the run terminates at that point.
    Cancelling,
    /// The run reached a terminal state. Reported from the run's terminal
    /// record, which exists only under
    /// [`WorkflowRuntimeBuilder::memo_retention`] and is removed with the
    /// run's memo entries when the window elapses.
    Terminated(RunTermination),
}

/// The committed terminal outcome of a run, as
/// [`RunState::Terminated`] reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunTermination {
    /// How the run ended.
    pub status: TerminalStatus,
    /// The failure reason, or the runner's reason for a
    /// [`StepOutcome::Cancel`]; `None` for a success and for an external
    /// cancellation.
    pub error: Option<String>,
    /// The runtime clock's time at the terminating settlement, in
    /// milliseconds since the Unix epoch.
    pub terminated_at_ms: u64,
}

impl From<DurableTermination> for RunTermination {
    fn from(record: DurableTermination) -> Self {
        Self {
            status: record.status.into(),
            error: record.error,
            terminated_at_ms: record.terminated_at_ms,
        }
    }
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
    group_retention: Option<Duration>,
    step_output_replay: bool,
    clock: Arc<dyn Clock>,
}

impl<R: StepRunner, H: TerminalHook> WorkflowRuntimeBuilder<R, H> {
    /// The Taquba queue name that step jobs are enqueued onto. Defaults to
    /// `"workflow-steps"`. Multiple runtimes can share a `Queue` handle by
    /// using distinct queue names.
    pub fn queue_name(mut self, name: impl Into<String>) -> Self {
        self.queue_name = name.into();
        self
    }

    /// The object-store path prefix [`Delivery::memo`](crate::Delivery::memo)
    /// entries live under. Defaults to `"workflow-memo"`. Pick a distinct value
    /// when multiple runtimes share an object store, so their memo namespaces
    /// don't collide.
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
    /// runtime writes a terminal marker and a terminal record for every
    /// run that reaches a terminal state, and the in-process sweeper
    /// clears that run's memo entries and terminal record `retention`
    /// after termination. When unset (default), neither is written and
    /// memo entries are retained indefinitely.
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
    /// includes the effects staged through
    /// [`Delivery::effects`](crate::Delivery::effects), so a
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

    /// Remove a group's state (its manifest, member records and the
    /// memo entries of its members) `retention` after every member of
    /// the group terminated. When unset (default), no group terminal
    /// marker is written and a group is retained until it is forgotten.
    ///
    /// Panics if `retention < 1ms`.
    pub(crate) fn group_retention(mut self, retention: Duration) -> Self {
        assert!(
            retention >= Duration::from_millis(1),
            "group_retention must be at least 1ms",
        );
        self.group_retention = Some(retention);
        self
    }

    /// Finalize the builder.
    pub fn build(self) -> WorkflowRuntime<R, H> {
        let memo_store = MemoStore::new(self.object_store.clone(), self.memo_prefix.clone());
        let group_store = GroupStore::new(
            self.object_store,
            self.memo_prefix,
            memo_store.clone(),
            self.queue.clone(),
        );
        let mut sweeps = Vec::new();
        if let Some(retention) = self.memo_retention {
            sweeps.push(Sweep::new(
                TERMINAL_KV_PREFIX,
                retention,
                RunStore {
                    memo_store: memo_store.clone(),
                },
            ));
        }
        if let Some(retention) = self.group_retention {
            sweeps.push(Sweep::new(
                GROUP_TERMINAL_KV_PREFIX,
                retention,
                group_store.clone(),
            ));
        }
        let core = RuntimeCore {
            queue: self.queue,
            queue_name: self.queue_name,
            max_concurrent_steps: self.max_concurrent_steps,
            poll_interval: self.poll_interval,
            memo_store,
            group_store,
            memo_retention: self.memo_retention,
            group_retention: self.group_retention,
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

/// The retained state of a terminated run: its memo and step-output
/// entries in the object store and its terminal record in the queue's
/// KV namespace, removed together by the memo sweep.
struct RunStore {
    memo_store: MemoStore,
}

impl Clearable for RunStore {
    type Error = Error;

    async fn clear(&self, run_id: &str) -> Result<Vec<Vec<u8>>> {
        self.memo_store.clear_memos_for_run(run_id).await?;
        Ok(vec![outcome_kv_key(run_id)])
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
    pub(crate) runner: R,
    pub(crate) terminal_hook: H,
    pub(crate) core: RuntimeCore,
}

/// The state every component of a runtime operates on: the queue
/// handle, the memo store and the clock. Methods
/// that invoke the runner or the hook are on [`RuntimeInner`].
pub(crate) struct RuntimeCore {
    pub(crate) queue: Arc<Queue>,
    queue_name: String,
    max_concurrent_steps: usize,
    poll_interval: Duration,
    pub(crate) memo_store: MemoStore,
    pub(crate) group_store: GroupStore,
    /// Window after a run reaches a terminal state during which its
    /// memo entries are retained for replay. `None` disables retention
    /// entirely (no terminal marker is written and no memo sweep runs).
    pub(crate) memo_retention: Option<Duration>,
    /// Window after every member of a group terminated during which
    /// the group's state is retained; `None` writes no group marker.
    pub(crate) group_retention: Option<Duration>,
    /// The retention sweeps [`WorkflowRuntime::run`] drives: the memo
    /// sweep when `memo_retention` is set and the group sweep when
    /// `group_retention` is set.
    sweeps: Vec<Sweep>,
    /// Whether runner-returned step outcomes are persisted and replayed
    /// by `(run_id, step_number, SHA-256(step payload))`.
    pub(crate) step_output_replay: bool,
    /// Time source. Defaults to the queue's clock; tests can substitute
    /// a [`MockClock`](taquba::MockClock) to virtualise time.
    pub(crate) clock: Arc<dyn Clock>,
}

impl<R: StepRunner, H: TerminalHook> WorkflowRuntime<R, H> {
    /// Start configuring a runtime. Takes the four required dependencies
    /// (Taquba queue, object store, [`StepRunner`], [`TerminalHook`]); optional
    /// fields are set via [`WorkflowRuntimeBuilder`] methods before [`build`].
    ///
    /// The object store backs [`Delivery::memo`]; it does **not** need to be
    /// the same store the [`Queue`] was opened with, though sharing one store
    /// is the common case (just clone the `Arc`). Use a distinct
    /// [`WorkflowRuntimeBuilder::memo_prefix`] when multiple runtimes share one
    /// store.
    ///
    /// Use [`crate::NoopTerminalHook`] if you don't need terminal callbacks.
    ///
    /// [`Delivery::memo`]: crate::Delivery::memo
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
            group_retention: None,
            step_output_replay: false,
            clock,
        }
    }

    /// Submit a new run. Enqueues step 0 with payload `spec.input`.
    ///
    /// Idempotent on `(run_id, spec.input)`: if a run with the same id is
    /// already active (its durable run record in Taquba's user KV
    /// namespace exists, whichever process submitted it) and
    /// `spec.input` matches the original submission, this
    /// call is a no-op and the returned [`SubmitOutcome`] has
    /// `newly_submitted = false`. A re-submission of an active `run_id`
    /// with a *different* input is rejected with [`Error::InputMismatch`];
    /// pick a fresh `run_id` for a new run.
    #[instrument(skip(self, spec), fields(run_id))]
    pub async fn submit(&self, spec: RunSpec) -> Result<SubmitOutcome> {
        let run_id = Self::validate_spec(&spec)?;
        tracing::Span::current().record("run_id", run_id.as_str());
        self.enqueue_run(&run_id, spec, None).await
    }

    /// [`Self::submit`] of a member of a group: the run's step jobs
    /// hold the membership and its member record is written with the
    /// enqueue.
    pub(crate) async fn submit_member(
        &self,
        membership: &Membership,
        spec: RunSpec,
    ) -> Result<SubmitOutcome> {
        let run_id = Self::validate_spec(&spec)?;
        self.enqueue_run(&run_id, spec, Some(membership)).await
    }

    /// The group named `id`; [`Error::InvalidGroupId`] for an id outside
    /// the run id rules.
    pub(crate) fn group(&self, id: impl Into<String>) -> Result<RunGroup<'_, R, H>> {
        let id = id.into();
        validate_run_id(&id).map_err(|_| Error::InvalidGroupId(id.clone()))?;
        Ok(RunGroup::new(self, id))
    }

    /// A group with a generated id.
    pub(crate) fn new_group(&self) -> RunGroup<'_, R, H> {
        RunGroup::new(self, ulid::Ulid::new().to_string())
    }

    /// Check `spec`'s run id, headers and KV keys; the run id, generated
    /// when the spec names none.
    fn validate_spec(spec: &RunSpec) -> Result<String> {
        if let Some(supplied) = spec.run_id.as_deref() {
            validate_run_id(supplied)?;
        }
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
        Ok(spec
            .run_id
            .clone()
            .unwrap_or_else(|| ulid::Ulid::new().to_string()))
    }

    /// Enqueue step 0 of `run_id` unless the run is active. The run
    /// record is written with the step-0 enqueue and deleted with the
    /// termination, so it identifies an active run whichever process
    /// submitted it, and the step's dedup key serialises two
    /// submissions that both find no record. A `membership` is set on
    /// the step job and its pending member record is written with the
    /// enqueue.
    async fn enqueue_run(
        &self,
        run_id: &str,
        spec: RunSpec,
        membership: Option<&Membership>,
    ) -> Result<SubmitOutcome> {
        let input_hash = hash_input(&spec.input);
        let duplicate = |job_id: String| SubmitOutcome {
            run_id: run_id.to_string(),
            newly_submitted: false,
            job_id,
        };
        let check_input = |existing: DurableRunRecord| {
            if existing.input_hash == input_hash {
                Ok(())
            } else {
                Err(Error::InputMismatch(run_id.to_string()))
            }
        };

        if let Some(existing) = self.inner.core.run_record(run_id).await? {
            check_input(existing)?;
            let current = self.inner.core.current_step(run_id).await?;
            return Ok(duplicate(current.job_id));
        }

        let opts = StepEnqueueOpts {
            run_at: spec.run_at,
            priority: spec.priority,
            max_attempts: spec.max_attempts_per_step,
            reserved_headers: membership
                .map(Membership::reserved_headers)
                .unwrap_or_default(),
        };
        let (request, job_id) =
            self.inner
                .core
                .step_enqueue_request(run_id, 0, spec.input, &spec.headers, opts);

        let record_bytes = durable::encode(&DurableRunRecord {
            run_id: run_id.to_string(),
            submitted_at_ms: self.inner.core.clock.now_ms(),
            input_hash,
            cancel_requested: false,
        });
        let mut kv = spec.kv_writes;
        kv.insert(run_kv_key(run_id), record_bytes);
        kv.insert(step_kv_key(run_id), current_step_bytes(0, &job_id));
        if let Some(membership) = membership {
            kv.insert(
                membership.kv_key(),
                durable::encode(&pending_member(run_id)),
            );
        }

        let job_id = match self
            .inner
            .core
            .queue
            .enqueue_with_kv(&request.queue, request.payload, request.options, kv)
            .await?
        {
            EnqueueResult::New(id) => id,
            // A concurrent submission committed first; its record holds
            // the input to check against. A dedup hit without a record
            // is a store this runtime did not write, reported as a
            // duplicate.
            EnqueueResult::AlreadyEnqueued(existing) => {
                if let Some(record) = self.inner.core.run_record(run_id).await? {
                    check_input(record)?;
                }
                return Ok(duplicate(existing));
            }
        };

        debug!(run_id = %run_id, job_id = %job_id, "run submitted");
        Ok(SubmitOutcome {
            run_id: run_id.to_string(),
            newly_submitted: true,
            job_id,
        })
    }

    /// The status of a run, read from its durable state, so it answers
    /// after a restart and from any runtime over the same queue. A
    /// terminated run reports [`RunState::Terminated`] while its terminal
    /// record is retained, which requires
    /// [`WorkflowRuntimeBuilder::memo_retention`]; `None` for a run that
    /// is unknown or whose termination was not recorded.
    ///
    /// A run with a pending cancellation request reports
    /// [`RunState::Cancelling`] whatever its step's lifecycle position,
    /// until the run terminates.
    pub async fn status(&self, run_id: &str) -> Result<Option<RunStatus>> {
        let core = &self.inner.core;
        let Some(record) = core.run_record(run_id).await? else {
            return core.terminated_status(run_id).await;
        };
        // The pointer is deleted with the record; its absence here means
        // the run terminated between the two reads.
        let Some(current) = core.current_step_if_active(run_id).await? else {
            return core.terminated_status(run_id).await;
        };
        let state = if record.cancel_requested {
            RunState::Cancelling
        } else {
            match core.queue.get_job(&current.job_id).await? {
                Some(job) if job.status == JobStatus::Claimed => RunState::Running,
                _ => RunState::Pending,
            }
        };
        Ok(Some(RunStatus {
            run_id: run_id.to_string(),
            state,
            current_step: current.step_number,
        }))
    }

    /// Request cancellation of an active run.
    ///
    /// Returns `Ok(true)` once the request is recorded on the run's
    /// durable record, or `Ok(false)` if the run is unknown or already
    /// terminal. The request reaches a run after a restart and from any
    /// runtime over the same queue.
    ///
    /// The run terminates as [`TerminalStatus::Cancelled`](crate::TerminalStatus::Cancelled) and its
    /// notification job is enqueued for the terminal hook:
    ///
    /// - **Pending / scheduled step**: the queued step job is removed
    ///   and the notification enqueued in one transaction before this
    ///   call returns; the hook runs from a worker afterwards.
    /// - **Running step**: cancellation is delivered to the runner via
    ///   [`Delivery::cancel_token`](crate::Delivery::cancel_token); runners
    ///   that watch the token short-circuit immediately. Runners that ignore
    ///   the token are allowed to run to completion (futures cannot be safely
    ///   aborted mid-step). In both cases the runner's [`StepOutcome`] /
    ///   [`StepError`](crate::StepError) is discarded and the worker settles
    ///   the run once the step returns, with any pending transient retry
    ///   suppressed and the step acked rather than nacked.
    /// - A step claimed after the request is settled as cancelled
    ///   without running.
    ///
    /// Cancellation is best-effort: a run whose terminal step settles
    /// while the request is being recorded keeps the outcome it
    /// committed.
    pub async fn cancel(&self, run_id: &str) -> Result<bool> {
        let core = &self.inner.core;
        if !core.request_cancel(run_id).await? {
            return Ok(false);
        }
        // Settle the current step now: remove it while it is queued, or
        // fire the claim's cancellation token, the parent of
        // `Delivery::cancel_token`, while it runs. A step that settles in
        // between is followed to its successor; a step claimed after the
        // request terminates the run on its own.
        let mut absent: Option<String> = None;
        loop {
            let Some(current) = core.current_step_if_active(run_id).await? else {
                // Terminated on its own after the request was recorded.
                return Ok(false);
            };
            let Some(job) = core.queue.get_job(&current.job_id).await? else {
                if absent.as_deref() == Some(current.job_id.as_str()) {
                    return Err(Error::InconsistentRunState(run_id.to_string()));
                }
                absent = Some(current.job_id);
                continue;
            };
            let claimed = ClaimedStep::parse(&job)?;
            // `error` is `None`: external cancellation supplies no reason
            // at the API level. The effects are built before the outcome
            // is known; the queue applies them only on `Removed`.
            let effects = self
                .inner
                .terminate_collecting_effects(&claimed.cancelled(None), &claimed);
            match core.queue.cancel_with(&job.id, effects).await?.0 {
                taquba::CancelOutcome::Removed | taquba::CancelOutcome::Requested => {
                    return Ok(true);
                }
                taquba::CancelOutcome::NotFound => continue,
            }
        }
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
        let runtime = self.clone();
        WorkerHandle::spawn(shutdown, |stop| async move {
            runtime.run(stop.cancelled_owned()).await
        })
    }

    /// Drive the step worker loop until `shutdown` resolves. Spawns up
    /// to `max_concurrent_steps` step processors, the dead-step
    /// reconciliation that terminates runs whose step the queue
    /// dead-lettered outside the worker and, when
    /// [`WorkflowRuntimeBuilder::memo_retention`] is set, a
    /// memo-retention sweeper, all running in parallel. All halt cleanly
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

        let mut background: Vec<_> = (0..self.inner.core.sweeps.len())
            .map(|i| {
                let inner = self.inner.clone();
                let token = stop.clone();
                tokio::spawn(async move {
                    let core = &inner.core;
                    core.sweeps[i].run(&core.queue, &*core.clock, token).await;
                })
            })
            .collect();
        background.push({
            let inner = self.inner.clone();
            let token = stop.clone();
            tokio::spawn(async move { inner.run_dead_step_reconciliation(token).await })
        });

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

        for handle in background {
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
/// Call [`shutdown`](WorkerHandle::shutdown) or
/// [`wait`](WorkerHandle::wait) to stop or join the worker explicitly.
pub type RunnerHandle = WorkerHandle<Result<()>>;

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

    /// The current-step pointer of `run_id`, `None` when the run is not
    /// active.
    pub(crate) async fn current_step_if_active(
        &self,
        run_id: &str,
    ) -> Result<Option<DurableCurrentStep>> {
        match self.queue.kv_get(&step_kv_key(run_id)).await? {
            Some(bytes) => Ok(Some(rmp_serde::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    /// The status of a terminated run from its terminal record; `None`
    /// when no record exists.
    async fn terminated_status(&self, run_id: &str) -> Result<Option<RunStatus>> {
        let Some(bytes) = self.queue.kv_get(&outcome_kv_key(run_id)).await? else {
            return Ok(None);
        };
        let record: DurableTermination = rmp_serde::from_slice(&bytes)?;
        Ok(Some(RunStatus {
            run_id: run_id.to_string(),
            current_step: record.final_step,
            state: RunState::Terminated(record.into()),
        }))
    }

    /// The durable record of `run_id`, when the run is active.
    pub(crate) async fn run_record(&self, run_id: &str) -> Result<Option<DurableRunRecord>> {
        match self.queue.kv_get(&run_kv_key(run_id)).await? {
            Some(bytes) => Ok(Some(rmp_serde::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Record a cancellation request on the run record of `run_id`.
    /// Returns whether the run is active; a request already recorded
    /// counts as recorded again.
    async fn request_cancel(&self, run_id: &str) -> Result<bool> {
        let key = run_kv_key(run_id);
        loop {
            let Some(current) = self.queue.kv_get(&key).await? else {
                return Ok(false);
            };
            let mut record: DurableRunRecord = rmp_serde::from_slice(&current)?;
            if record.cancel_requested {
                return Ok(true);
            }
            record.cancel_requested = true;
            if self
                .queue
                .kv_compare_put(&key, Some(&current), &durable::encode(&record))
                .await?
            {
                return Ok(true);
            }
        }
    }

    /// Build the enqueue request for one step of a run, with a
    /// pre-assigned job id so the current-step pointer written with
    /// the enqueue can name it. Returns the request and the assigned
    /// id.
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
    /// The priority and attempt limit are inherited from `terminal_step`,
    /// the job of the step that produced the outcome, when one exists.
    fn notification_enqueue_request(
        &self,
        outcome: &RunOutcome,
        terminal_step: Option<&JobRecord>,
    ) -> EnqueueRequest {
        let payload = durable::encode(&DurableRunOutcome::from(outcome));
        let mut headers = HashMap::new();
        headers.insert(HEADER_RUN_ID.to_string(), outcome.run_id.clone());
        headers.insert(HEADER_TERMINAL.to_string(), "1".to_string());
        EnqueueRequest {
            queue: self.queue_name.clone(),
            payload,
            options: EnqueueOptions::default()
                .headers(headers)
                .priority(terminal_step.map(|job| job.priority))
                .max_attempts(terminal_step.map(|job| job.max_attempts))
                .dedup_key(Some(format!("{DEDUP_PREFIX}{}:terminal", outcome.run_id))),
        }
    }

    /// The instant `delay` after the runtime's clock now, as a
    /// [`SystemTime`] for an enqueue's `run_at`.
    pub(crate) fn run_at_after(&self, delay: Duration) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.clock.now_ms()) + delay
    }

    pub(crate) async fn load_step_output(
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

    pub(crate) async fn store_step_output(
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

    /// Build the effects that advance the run of `claimed` to its next
    /// step: the next step's enqueue joins the current step's
    /// acknowledgement transaction, so the transition is atomic.
    pub(crate) async fn advance(
        &self,
        claimed: &ClaimedStep<'_>,
        payload: Vec<u8>,
        opts: StepEnqueueOpts,
    ) -> SettlementEffects {
        self.advance_with_kv(claimed, payload, opts, |_| HashMap::new())
            .await
    }

    /// [`Self::advance`] with caller KV writes joined to the same
    /// acknowledgement transaction. `kv_writes` receives the next step's
    /// pre-assigned job id so the writes can reference it.
    pub(crate) async fn advance_with_kv(
        &self,
        claimed: &ClaimedStep<'_>,
        payload: Vec<u8>,
        opts: StepEnqueueOpts,
        kv_writes: impl FnOnce(&str) -> HashMap<Vec<u8>, Vec<u8>>,
    ) -> SettlementEffects {
        let run_id = claimed.run_id.as_str();
        let next_step = claimed.step_number + 1;
        let (request, next_job_id) =
            self.step_enqueue_request(run_id, next_step, payload, &claimed.headers, opts);
        let mut kv_writes = kv_writes(&next_job_id);
        kv_writes.insert(
            step_kv_key(run_id),
            current_step_bytes(next_step, &next_job_id),
        );
        SettlementEffects::default()
            .enqueues(vec![request])
            .kv_writes(kv_writes)
    }
}

impl<R: StepRunner, H: TerminalHook> RuntimeInner<R, H> {
    /// Settle a run into its terminal state: return the deletes of the
    /// durable run record and the current-step pointer, the writes of
    /// the terminal marker and the terminal record (when memo retention
    /// is enabled), the write of the member record (when the run is a
    /// group member) and the terminal-notification enqueue
    /// (when the hook observes this outcome) as [`SettlementEffects`]
    /// for the settlement transaction. The notification job's payload
    /// is the committed outcome and the configured [`TerminalHook`]
    /// runs as its worker; `terminal_step` is the step that produced
    /// the outcome, or the pending step a cancellation removes.
    ///
    /// The effects are pure: nothing is written and no state is
    /// mutated here, so a caller that builds them and then commits a
    /// non-terminal outcome leaves no trace. A settlement that fails
    /// redelivers the step, which re-terminates and rebuilds the same
    /// effects.
    pub(crate) fn terminate_collecting_effects(
        &self,
        outcome: &RunOutcome,
        terminal_step: &ClaimedStep<'_>,
    ) -> SettlementEffects {
        let kv_deletes = vec![run_kv_key(&outcome.run_id), step_kv_key(&outcome.run_id)];
        let terminated_at_ms = self.core.clock.now_ms();
        let termination = DurableTermination {
            status: outcome.status.into(),
            error: outcome.error.clone(),
            final_step: outcome.final_step,
            terminated_at_ms,
        };
        let mut kv_writes = HashMap::new();
        if self.core.memo_retention.is_some() {
            kv_writes.insert(
                terminal_kv_key(&outcome.run_id, terminated_at_ms),
                Vec::new(),
            );
            kv_writes.insert(
                outcome_kv_key(&outcome.run_id),
                durable::encode(&termination),
            );
        }
        if let Some(membership) = &terminal_step.membership {
            kv_writes.insert(
                membership.kv_key(),
                durable::encode(&terminated_member(&outcome.run_id, termination)),
            );
        }
        let enqueues = if self.terminal_hook.observes(outcome) {
            vec![
                self.core
                    .notification_enqueue_request(outcome, Some(terminal_step.job)),
            ]
        } else {
            Vec::new()
        };
        SettlementEffects::default()
            .enqueues(enqueues)
            .kv_writes(kv_writes)
            .kv_deletes(kv_deletes)
    }

    /// Terminate every run whose step job the queue dead-lettered
    /// outside the worker path: a lease that expired past the attempt
    /// limit, or a claim dead-lettered by crash recovery when the queue
    /// was opened. Such a settlement runs no workflow code, so the run
    /// record and the current-step pointer survive it and no
    /// notification is enqueued. A dead step job whose run record still
    /// exists identifies the case exactly, because every worker-path
    /// dead-letter deletes the record in its own transaction. The run
    /// terminates as [`TerminalStatus::Failed`] with the queue record's
    /// last error, through the same effects as a worker-path
    /// termination committed as one transaction with no transition of
    /// their own; the runner's failure writes cannot apply, since no
    /// runner returned. Returns the number of runs terminated.
    pub(crate) async fn reconcile_dead_steps(&self) -> Result<usize> {
        const PAGE: usize = 256;
        let core = &self.core;
        let mut terminated = 0usize;
        let mut dead = std::pin::pin!(core.queue.jobs(&core.queue_name, JobStatus::Dead, PAGE));
        while let Some(job) = dead.try_next().await? {
            if job.headers.contains_key(HEADER_TERMINAL) {
                continue;
            }
            let Ok(claimed) = ClaimedStep::parse(&job) else {
                continue;
            };
            let run_id = claimed.run_id.as_str();
            if core.queue.kv_get(&run_kv_key(run_id)).await?.is_none() {
                continue;
            }
            let error = job
                .last_error
                .clone()
                .unwrap_or_else(|| "step dead-lettered outside the worker".to_string());
            let effects = self.terminate_collecting_effects(&claimed.failed(error), &claimed);
            core.queue.commit_effects(effects).await?;
            warn!(run_id = %run_id, step_number = claimed.step_number, job_id = %job.id, "terminated a run whose step was dead-lettered outside the worker");
            terminated += 1;
        }
        Ok(terminated)
    }

    /// The reconciliation loop: a pass when the worker starts, then a
    /// pass whenever the queue's dead count has changed since the last
    /// successful pass, checked every poll interval, until `stop` is
    /// cancelled.
    async fn run_dead_step_reconciliation(&self, stop: CancellationToken) {
        let core = &self.core;
        run_periodically(
            core.poll_interval,
            &stop,
            None,
            |reconciled_at: Option<i64>| async move {
                match core.queue.stats(&core.queue_name).await {
                    Ok(stats) if reconciled_at != Some(stats.dead) => {
                        match self.reconcile_dead_steps().await {
                            Ok(_) => Some(stats.dead),
                            Err(err) => {
                                warn!("dead-step reconciliation failed: {err}");
                                reconciled_at
                            }
                        }
                    }
                    Ok(_) => reconciled_at,
                    Err(err) => {
                        warn!("dead-step reconciliation could not read queue stats: {err}");
                        reconciled_at
                    }
                }
            },
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::DurableMember;
    use crate::effects::{EffectsHandle, TerminalEffects};
    use crate::group::{ManifestMember, MemberSpec};
    use crate::keys::{MAX_RUN_ID_LEN, group_member_kv_key};
    use crate::keys::{
        TERMINAL_KV_PREFIX, parse_timestamped_kv_key, signal_buf_kv_key, signal_wait_kv_key,
    };
    use crate::runner::{Step, StepError};
    use crate::signal::SignalOutcome;
    use crate::terminal::NoopTerminalHook;
    use crate::terminal::TerminalStatus;
    use crate::test_util::{
        advance, fast_options, open_queue, open_queue_at, open_queue_at_with, open_queue_with,
    };
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicU32, Ordering};
    use taquba::object_store::memory::InMemory;
    use taquba::{LeaseHandle, MockClock, OpenOptions, QueueConfig};
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

    /// Runner that returns a clone of `result` on every step and counts
    /// its calls.
    struct FixedRunner {
        result: std::result::Result<StepOutcome, StepError>,
        calls: Arc<AtomicU32>,
    }

    impl FixedRunner {
        fn new(result: std::result::Result<StepOutcome, StepError>) -> Self {
            Self {
                result,
                calls: Arc::new(AtomicU32::new(0)),
            }
        }
    }

    impl StepRunner for FixedRunner {
        async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.result.clone()
        }
    }

    /// Runner whose step never returns.
    struct PauseRunner;

    impl StepRunner for PauseRunner {
        async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
            std::future::pending().await
        }
    }

    /// Runner whose step must not be claimed.
    struct UnreachableRunner;

    impl StepRunner for UnreachableRunner {
        async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
            unreachable!("worker must not claim the step");
        }
    }

    /// Runner that reports the claim of its step, holds the step until
    /// its [`Gate`] is released and then returns a clone of `result`.
    struct GatedRunner {
        claimed: Arc<tokio::sync::Notify>,
        release: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
        result: std::result::Result<StepOutcome, StepError>,
        calls: Arc<AtomicU32>,
    }

    /// The test's side of a [`GatedRunner`].
    struct Gate {
        claimed: Arc<tokio::sync::Notify>,
        release: StdMutex<Option<oneshot::Sender<()>>>,
        calls: Arc<AtomicU32>,
    }

    impl GatedRunner {
        fn new(result: std::result::Result<StepOutcome, StepError>) -> (Self, Gate) {
            let claimed = Arc::new(tokio::sync::Notify::new());
            let calls = Arc::new(AtomicU32::new(0));
            let (release_tx, release_rx) = oneshot::channel();
            let runner = Self {
                claimed: claimed.clone(),
                release: tokio::sync::Mutex::new(Some(release_rx)),
                result,
                calls: calls.clone(),
            };
            let gate = Gate {
                claimed,
                release: StdMutex::new(Some(release_tx)),
                calls,
            };
            (runner, gate)
        }
    }

    impl StepRunner for GatedRunner {
        async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.claimed.notify_one();
            let rx = self
                .release
                .lock()
                .await
                .take()
                .expect("gate consumed twice");
            let _ = rx.await;
            self.result.clone()
        }
    }

    impl Gate {
        /// Wait until the runner holds the step.
        async fn claimed(&self) {
            tokio::time::timeout(Duration::from_secs(2), self.claimed.notified())
                .await
                .expect("runner reached gate");
        }

        fn release(&self) {
            if let Some(tx) = self.release.lock().unwrap().take() {
                let _ = tx.send(());
            }
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
        let (queue, store, _clock) = open_queue_at(base).await;
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
    async fn single_step_succeeds_and_writes_no_marker_without_retention() {
        let (queue, store) = open_queue().await;
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
        assert!(runtime.status(&handle.run_id).await.unwrap().is_none());
        assert!(terminal_markers(&runtime.inner.core.queue).await.is_empty());

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn multi_step_run_advances_through_continue_with_its_headers() {
        let (queue, store) = open_queue().await;
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
                headers: HashMap::from([
                    ("trace_id".to_string(), "abc-123".to_string()),
                    ("tenant".to_string(), "acme".to_string()),
                ]),
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
        assert_eq!(outcome.headers.get("trace_id").unwrap(), "abc-123");
        assert_eq!(outcome.headers.get("tenant").unwrap(), "acme");
        assert!(!outcome.headers.contains_key(HEADER_RUN_ID));
        assert!(!outcome.headers.contains_key(HEADER_STEP));

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn continue_after_delays_next_step_until_promotion() {
        let initial = 1_700_000_000_000u64;
        let (queue, store, clock) = open_queue_at(initial).await;
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
        let (queue, store, _clock) = open_queue_at(1_700_000_000_000).await;
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
        let (queue, store, clock) = open_queue_at(1_700_000_000_000).await;
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
    async fn a_buffered_signal_is_consumed_at_registration_and_a_later_one_replaces_it() {
        let (queue, store, _clock) = open_queue_at(1_700_000_000_000).await;
        let (runtime, observed, mut rx) =
            signal_probe_runtime(queue.clone(), store, "order-3", Duration::from_secs(3600));
        let shutdown = spawn_runtime(runtime.clone());

        assert_eq!(
            runtime.signal("order-3", b"first".to_vec()).await.unwrap(),
            SignalOutcome::Buffered
        );
        assert_eq!(
            runtime.signal("order-3", b"second".to_vec()).await.unwrap(),
            SignalOutcome::Buffered
        );

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
            &[Some(b"second".to_vec())]
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
    async fn clear_signal_discards_buffered_signal() {
        let (queue, store, _clock) = open_queue_at(1_700_000_000_000).await;
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
        let (queue, store, _clock) = open_queue_at(1_700_000_000_000).await;
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
        let (queue, store, clock) = open_queue_at(1_700_000_000_000).await;
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
        let (queue, store, _clock) = open_queue_at(1_700_000_000_000).await;
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
    async fn a_failure_notification_inherits_the_step_limits() {
        let (queue, store) = open_queue().await;
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            FixedRunner::new(Err(StepError::permanent("nope"))),
            ChannelHook { tx },
        )
        .build();
        runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                priority: Some(3),
                max_attempts_per_step: Some(5),
                ..Default::default()
            })
            .await
            .unwrap();

        let job = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let err = runtime
            .inner
            .process_step(&job, &LeaseHandle::detached())
            .await
            .unwrap_err();
        let failure = err
            .downcast_ref::<taquba::FailWith>()
            .expect("a terminating failure carries its effects");
        let notification = &failure.effects.enqueues[0];
        assert_eq!(notification.options.priority, Some(3));
        assert_eq!(notification.options.max_attempts, Some(5));
    }

    #[tokio::test(start_paused = true)]
    async fn a_duplicate_submit_is_idempotent_drops_its_kv_writes_and_rejects_a_changed_input() {
        let (queue, store) = open_queue().await;
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            ScriptedRunner::new(vec![]),
            NoopTerminalHook,
        )
        .build();
        // No worker loop runs, so the step stays queued and the run is
        // active for every later submit.
        let spec = |input: &[u8], key: &[u8]| RunSpec {
            run_id: Some("fixed-id".to_string()),
            input: input.to_vec(),
            kv_writes: HashMap::from([(key.to_vec(), b"1".to_vec())]),
            ..Default::default()
        };

        let first = runtime.submit(spec(b"x", b"app/first")).await.unwrap();
        assert!(first.newly_submitted);
        assert!(runtime.status("fixed-id").await.unwrap().is_some());
        assert_eq!(
            queue.kv_get(b"app/first").await.unwrap().as_deref(),
            Some(b"1".as_slice())
        );

        let duplicate = runtime.submit(spec(b"x", b"app/second")).await.unwrap();
        assert_eq!(duplicate.run_id, "fixed-id");
        assert!(!duplicate.newly_submitted);
        assert_eq!(duplicate.job_id, first.job_id);
        assert!(queue.kv_get(b"app/second").await.unwrap().is_none());

        let err = runtime.submit(spec(b"y", b"app/third")).await.unwrap_err();
        assert!(matches!(&err, Error::InputMismatch(id) if id == "fixed-id"));
        assert!(err.is_permanent());
        assert!(queue.kv_get(b"app/third").await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_duplicate_known_only_from_the_durable_record_reports_the_current_job() {
        let (queue, store) = open_queue().await;
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
        let (queue, store, clock) = open_queue_at(t0).await;
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
            async move { queue.wait_for_completion(&job_id).await }
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
        let (queue, store) = open_queue().await;
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
        queue.wait_for_completion(&job_id).await.unwrap();
        assert_eq!(*seen.lock().unwrap(), Some(7));

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_submits_of_one_run_admit_one_and_reject_a_changed_input() {
        let (queue, store) = open_queue().await;
        let runtime =
            WorkflowRuntime::builder(queue, store.clone(), PauseRunner, NoopTerminalHook).build();
        let spec = |input: &[u8]| RunSpec {
            run_id: Some("raced".to_string()),
            input: input.to_vec(),
            ..Default::default()
        };

        let (first, same, changed) = tokio::join!(
            runtime.submit(spec(b"x")),
            runtime.submit(spec(b"x")),
            runtime.submit(spec(b"y")),
        );
        let first = first.unwrap();
        let same = same.unwrap();
        assert!(first.newly_submitted);
        assert!(!same.newly_submitted);
        assert_eq!(same.job_id, first.job_id);
        assert!(matches!(changed, Err(Error::InputMismatch(id)) if id == "raced"));
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

        let (queue, store) = open_queue().await;

        let (runner, gate) =
            GatedRunner::new(Ok(StepOutcome::continue_now(b"step1-payload".to_vec())));
        let runtime_a =
            WorkflowRuntime::builder(queue.clone(), store.clone(), runner, NoopTerminalHook)
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

        gate.claimed().await;
        let s = runtime_a
            .status(&handle.run_id)
            .await
            .unwrap()
            .expect("status");
        assert_eq!(s.state, RunState::Running);
        assert_eq!(s.current_step, 0);

        // A's worker is in the at-capacity select-loop. Signal shutdown
        // first, then open the gate so step 0 finishes processing inside
        // drain mode (A will not claim step 1).
        let _ = shutdown_a_tx.send(());
        gate.release();

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
        let (queue, store) = open_queue().await;
        let calls = Arc::new(AtomicU32::new(0));
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            FixedRunner {
                result: Ok(StepOutcome::continue_now(b"step1-payload".to_vec())),
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
        let (queue, store) = open_queue().await;
        let calls = Arc::new(AtomicU32::new(0));
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            FixedRunner {
                result: Ok(StepOutcome::continue_now(b"step1-payload".to_vec())),
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
        let (queue, store) = open_queue().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let calls = Arc::new(AtomicU32::new(0));
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            FixedRunner {
                result: Ok(StepOutcome::Succeed {
                    result: b"final".to_vec(),
                }),
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
        let (queue, store) = open_queue_with(fast_options()).await;
        let calls = Arc::new(AtomicU32::new(0));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            FixedRunner {
                result: Err(StepError::transient("flaky")),
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
    async fn a_cancellation_survives_a_restart() {
        // Models a restart: the request recorded on the run record and
        // the job's persisted `cancel_requested` survive while a fresh
        // runtime starts with no process state. The runner returns
        // Succeed, so a Cancelled outcome shows the request was read.
        let (queue, store, _clock) = open_queue_at(10_000).await;
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
        assert_eq!(
            after.status(&handle.run_id).await.unwrap().map(|s| s.state),
            Some(RunState::Cancelling),
            "the fresh runtime reads the request from the run record",
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
    async fn a_cancellation_after_the_settlement_read_reaches_the_next_step() {
        // The worker reads the claim's token once the runner has returned.
        // A request recorded after that read does not affect the
        // advancing settlement; it is read from the run record when the
        // next step is claimed, which is then settled without running.
        let (queue, store, _clock) = open_queue_at(10_000).await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            ScriptedRunner::new(vec![
                StepOutcome::Continue {
                    payload: b"next".to_vec(),
                    when: Trigger::Immediate,
                },
                StepOutcome::Succeed {
                    result: b"done".to_vec(),
                },
            ]),
            ChannelHook { tx },
        )
        .build();

        let handle = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let step0 = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("step 0 is claimable");
        let effects = runtime
            .inner
            .process_step(&step0, &queue.lease_handle(&step0))
            .await
            .unwrap();
        assert!(runtime.cancel(&handle.run_id).await.unwrap());
        queue.ack_with(&step0, effects).await.unwrap();

        let step1 = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("step 1 is claimable");
        let effects = runtime
            .inner
            .process_step(&step1, &queue.lease_handle(&step1))
            .await
            .unwrap();
        queue.ack_with(&step1, effects).await.unwrap();

        let notification = queue
            .claim("workflow-steps", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("the terminal notification is claimable");
        let effects = runtime
            .inner
            .process_step(&notification, &LeaseHandle::detached())
            .await
            .unwrap();
        queue.ack_with(&notification, effects).await.unwrap();

        let outcome = rx.recv().await.unwrap();
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        assert_eq!(outcome.final_step, 1);
        assert!(runtime.status(&handle.run_id).await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_a_pending_run_commits_its_marker_and_fires_the_hook_once() {
        // Pending case: a run sits in the queue, we call `cancel()` before
        // any worker claims it. `cancel` removes the step job and enqueues
        // the notification before returning.

        let (queue, store, _clock) = open_queue_at(10_000).await;
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
        let status = runtime
            .status(&handle.run_id)
            .await
            .unwrap()
            .expect("active");
        assert_eq!(status.state, RunState::Pending);

        let was_cancelled = runtime.cancel(&handle.run_id).await.unwrap();
        assert!(was_cancelled);
        let status = runtime.status(&handle.run_id).await.unwrap().unwrap();
        assert_eq!(
            status.state,
            RunState::Terminated(RunTermination {
                status: TerminalStatus::Cancelled,
                error: None,
                terminated_at_ms: 10_000,
            }),
            "the terminal record commits with the removal",
        );
        assert_eq!(status.current_step, 0);
        assert!(
            !runtime.cancel(&handle.run_id).await.unwrap(),
            "a second cancel finds no run record",
        );

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
        assert!(
            queue
                .claim("workflow-steps", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none(),
            "the cancel enqueues one notification",
        );
        assert!(rx.try_recv().is_err());

        let stats = queue.stats("workflow-steps").await.unwrap();
        assert_eq!(stats.dead, 0, "cancel must not dead-letter");
        assert_eq!(stats.pending, 0, "cancelled job must be removed");
    }

    /// Drive a single step that blocks on a gate, calls `cancel(run_id)`
    /// while the step is in-flight, and then has the runner return the
    /// supplied error. Asserts that external cancellation suppresses the
    /// error path entirely: the hook fires `Cancelled` (not `Failed`),
    /// no dead-letter is produced regardless of `permanent`/`transient`,
    /// and the worker returns `Ok` (no retry, no PermanentFailure
    /// propagation).
    async fn assert_cancel_suppresses_runner_error(error: StepError) {
        let (queue, store) = open_queue_with(fast_options()).await;
        let (runner, gate) = GatedRunner::new(Err(error));
        let (hook_tx, mut hook_rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store.clone(),
            runner,
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
        gate.claimed().await;

        let was_cancelled = runtime.cancel(&handle.run_id).await.unwrap();
        assert!(was_cancelled);

        // Release the runner. It returns Err; without cancellation this
        // would either dead-letter (permanent) or nack for retry
        // (transient). Cancellation must suppress both.
        gate.release();

        let outcome = tokio::time::timeout(Duration::from_secs(2), hook_rx.recv())
            .await
            .expect("hook fired")
            .expect("hook channel open");
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        assert!(
            outcome.error.is_none(),
            "external cancel must carry no reason (Some(_) would imply runner-issued StepOutcome::Cancel)",
        );
        assert!(runtime.status(&handle.run_id).await.unwrap().is_none());

        // Settle window: assert no retry attempt and no dead-letter or
        // duplicate hook fires after the terminal one.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            gate.calls.load(Ordering::SeqCst),
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

        let (queue, store) = open_queue().await;
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

        let was_cancelled = runtime.cancel(&handle.run_id).await.unwrap();
        assert!(was_cancelled);

        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("hook fired well before the 30s sleep would have")
            .expect("hook channel open");

        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        // Runner-issued Cancel wins precedence over external cancel, so
        // the runner's reason surfaces.
        assert_eq!(outcome.error.as_deref(), Some("cooperative"));
        assert!(runtime.status(&handle.run_id).await.unwrap().is_none());

        let stats = queue.stats("workflow-steps").await.unwrap();
        assert_eq!(stats.dead, 0);

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_returns_false_for_a_terminated_or_unknown_run() {
        // Submit a run that succeeds normally, wait for the terminal
        // hook, then call `cancel`. The run record was deleted with the
        // success, so `cancel` must report `Ok(false)` and must not fire
        // a second hook.
        let (queue, store) = open_queue().await;
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
        assert!(runtime.status(&handle.run_id).await.unwrap().is_none());

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

        let (queue, store) = open_queue_with(fast_options()).await;
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
    async fn terminal_marker_is_written_at_the_runtime_clock() {
        // The queue's MockClock is shared into the runtime by default
        // (via Queue::clock()), so a `clock.advance` between submit and
        // terminate is visible in the marker's terminal_at_ms.
        let (queue, store, clock) = open_queue_at(10_000).await;
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
    async fn submit_rejects_reserved_headers_reserved_kv_keys_and_unusable_run_ids() {
        let (queue, store, _clock) = open_queue_at(10_000).await;
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

        let err = runtime
            .submit(RunSpec {
                input: b"x".to_vec(),
                headers: HashMap::from([("workflow.run_id".to_string(), "evil".to_string())]),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::ReservedHeaderInSubmit(k) if k == "workflow.run_id"),
            "got: {err:?}"
        );

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
    async fn a_malformed_terminal_marker_is_deleted_without_clearing_memos() {
        let (queue, store, clock) = open_queue_at(10_000).await;
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

    #[tokio::test(start_paused = true)]
    async fn cancelling_a_running_step_overrides_its_outcome_and_writes_its_marker_at_settlement() {
        // Between the request and the settlement the run reports
        // Cancelling and holds its record with no marker (the queue
        // discards the effects on the `Requested` arm); the settlement
        // then commits Cancelled in place of the runner's outcome.
        let (queue, store, _clock) = open_queue_at(10_000).await;
        let (runner, gate) = GatedRunner::new(Ok(StepOutcome::Succeed {
            result: b"would-have-succeeded".to_vec(),
        }));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime =
            WorkflowRuntime::builder(queue.clone(), store.clone(), runner, ChannelHook { tx })
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
        gate.claimed().await;
        assert_eq!(
            runtime
                .status(&handle.run_id)
                .await
                .unwrap()
                .expect("active")
                .state,
            RunState::Running
        );

        assert!(runtime.cancel(&handle.run_id).await.unwrap());
        assert_eq!(
            runtime
                .status(&handle.run_id)
                .await
                .unwrap()
                .expect("entry retained while termination is in flight")
                .state,
            RunState::Cancelling
        );
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

        gate.release();
        let outcome = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("hook fired")
            .expect("hook channel open");
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        assert!(
            outcome.result.is_none(),
            "succeed payload must be discarded"
        );
        let status = runtime.status(&handle.run_id).await.unwrap().unwrap();
        assert!(matches!(
            status.state,
            RunState::Terminated(RunTermination {
                status: TerminalStatus::Cancelled,
                error: None,
                ..
            })
        ));
        assert_eq!(
            terminal_markers(&queue).await,
            vec![(handle.run_id.clone(), 10_000)],
            "the worker's settlement writes it",
        );
        assert_eq!(queue.stats("workflow-steps").await.unwrap().dead, 0);

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_permanent_step_error_dead_letters_with_its_marker_and_no_staged_effects() {
        let (queue, store, _clock) = open_queue_at(10_000).await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            EffectStagingRunner::new(vec![Err(StepError::permanent("nope"))]),
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
        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.status, TerminalStatus::Failed);
        assert_eq!(outcome.error.as_deref(), Some("nope"));
        let status = runtime.status(&handle.run_id).await.unwrap().unwrap();
        assert!(
            matches!(
                status.state,
                RunState::Terminated(RunTermination {
                    status: TerminalStatus::Failed,
                    error: Some(ref error),
                    ..
                }) if error == "nope"
            ),
            "the terminal record commits with the dead-letter",
        );

        // The notification was enqueued by the dead-letter transaction, so
        // the dead job and the marker are already visible, and the staged
        // effect was discarded with the failure.
        assert_eq!(queue.stats("workflow-steps").await.unwrap().dead, 1);
        assert_eq!(
            terminal_markers(&queue).await,
            vec![(handle.run_id.clone(), 10_000)],
        );
        assert!(queue.kv_get(b"app/step-0").await.unwrap().is_none());

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

        let (queue, store, clock) = open_queue_at_with(10_000, fast_options()).await;
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
        let (queue, store, _clock) = open_queue_at(10_000).await;
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
        let (queue, store, clock) = open_queue_at(10_000).await;
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
        assert!(
            runtime.status(&handle.run_id).await.unwrap().is_none(),
            "sweeper did not clear the run's terminal record",
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
        let (queue, store, clock) = open_queue_at(10_000).await;
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
    async fn step_effects_commit_with_the_acking_settlement() {
        let (queue, store) = open_queue().await;
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

        let (queue, store) = open_queue().await;
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

        let (queue, store) = open_queue().await;
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
    async fn a_fail_verdict_acks_with_its_effects_and_no_dead_letter() {
        let (queue, store) = open_queue().await;
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

        let handle = runtime
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
        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.status, TerminalStatus::Failed);
        assert_eq!(outcome.error.as_deref(), Some("denied"));
        assert!(runtime.status(&handle.run_id).await.unwrap().is_none());
        assert_eq!(wait_for_kv(&queue, b"app/step-0").await, b"done");
        assert_eq!(
            queue.stats("workflow-steps").await.unwrap().dead,
            0,
            "a Fail verdict must not dead-letter"
        );

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

        let (queue, store, _clock) = open_queue_at(10_000).await;
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
    async fn a_cancel_verdict_acks_with_its_effects_and_no_dead_letter() {
        let (queue, store) = open_queue().await;
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

        let handle = runtime
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
        assert_eq!(outcome.run_id, handle.run_id);
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
        assert_eq!(outcome.error.as_deref(), Some("obsolete"));
        assert!(runtime.status(&handle.run_id).await.unwrap().is_none());
        assert_eq!(wait_for_kv(&queue, b"app/step-0").await, b"done");
        assert_eq!(
            queue.stats("workflow-steps").await.unwrap().dead,
            0,
            "a Cancel verdict must not dead-letter"
        );

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

        let (queue, store) = open_queue().await;
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
    async fn a_run_dead_lettered_by_the_reaper_is_terminated_by_reconciliation() {
        let (queue, store, clock) = open_queue_at_with(
            1_700_000_000_000,
            fast_options().default_queue_config(
                QueueConfig::default()
                    .retry_backoff_base(Duration::ZERO)
                    .lease_duration(Duration::from_secs(1)),
            ),
        )
        .await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime =
            WorkflowRuntime::builder(queue.clone(), store, PauseRunner, ChannelHook { tx })
                .poll_interval(Duration::from_millis(10))
                .build();
        let shutdown = spawn_runtime(runtime.clone());

        let submitted = runtime
            .submit(RunSpec {
                run_id: Some("hung".into()),
                input: Vec::new(),
                max_attempts_per_step: Some(1),
                ..Default::default()
            })
            .await
            .unwrap();
        for _ in 0..200 {
            if queue.stats("workflow-steps").await.unwrap().claimed == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            runtime.status("hung").await.unwrap().map(|s| s.state),
            Some(RunState::Running)
        );

        // The lease expires past the attempt limit: the reaper dead-letters
        // the step inside the core, with no worker in the loop.
        advance(&clock, Duration::from_secs(2)).await;
        let outcome = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.run_id, "hung");
        assert_eq!(outcome.status, TerminalStatus::Failed);
        assert_eq!(outcome.final_step, 0);
        assert_eq!(
            queue
                .get_job(&submitted.job_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            JobStatus::Dead
        );
        assert!(queue.kv_get(&run_kv_key("hung")).await.unwrap().is_none());
        assert!(queue.kv_get(&step_kv_key("hung")).await.unwrap().is_none());
        assert!(runtime.status("hung").await.unwrap().is_none());

        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_member_record_is_rewritten_only_by_the_terminating_settlement() {
        struct RecordReadingRunner {
            pending_seen: Arc<StdMutex<Vec<bool>>>,
        }

        impl StepRunner for RecordReadingRunner {
            async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
                let record = step
                    .kv
                    .get(&group_member_kv_key("g", "m"))
                    .await?
                    .expect("the member record is written with the submission");
                let member: DurableMember = rmp_serde::from_slice(&record).unwrap();
                self.pending_seen
                    .lock()
                    .unwrap()
                    .push(member.terminated.is_none());
                Err(StepError::transient("still failing"))
            }
        }

        let (queue, store) = open_queue_with(fast_options()).await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let pending_seen = Arc::new(StdMutex::new(Vec::new()));
        let runtime = WorkflowRuntime::builder(
            queue.clone(),
            store,
            RecordReadingRunner {
                pending_seen: pending_seen.clone(),
            },
            ChannelHook { tx },
        )
        .build();
        let shutdown = spawn_runtime(runtime.clone());

        let group = runtime.group("g").unwrap();
        group
            .submit(
                vec![ManifestMember {
                    key: "m".to_string(),
                    input: Vec::new(),
                }],
                &MemberSpec {
                    max_attempts_per_step: Some(2),
                    ..MemberSpec::default()
                },
            )
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

        // The retried first attempt left the record pending; the
        // exhausted second attempt's termination commits with the
        // dead-letter.
        assert_eq!(*pending_seen.lock().unwrap(), vec![true, true]);
        let members = group.members().await.unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].key, "m");
        assert_eq!(members[0].status(), Some(TerminalStatus::Failed));
        assert_eq!(
            members[0]
                .record
                .terminated
                .as_ref()
                .unwrap()
                .error
                .as_deref(),
            Some("still failing")
        );

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

        let (queue, store) = open_queue().await;
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

        let (queue, store) = open_queue().await;
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

        let (queue, store) = open_queue().await;
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

        let (queue, store) = open_queue_with(fast_options()).await;
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
        let (queue, store) = open_queue().await;
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

        let (queue, store) = open_queue().await;
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

        let (queue, store) = open_queue().await;
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
