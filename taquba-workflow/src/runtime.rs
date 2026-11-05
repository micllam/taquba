use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use taquba::{Clock, Queue, SettlementEffects, WaitOutcome, WorkerHandle};
use tokio_util::sync::CancellationToken;

use crate::command::CommandWorker;
use crate::durable::DurableStepRef;
use crate::error::{Error, Result};
use crate::group::{GroupStore, RunGroup};
use crate::keys::{RunId, run_kv_key};
use crate::kv::run_kv_entry_keys;
use crate::memo::MemoStore;
use crate::runner::StepRunner;
use crate::sweep::{Clearable, Cleared, Sweep};
use crate::terminal::{TerminalHook, TerminatedRun};
use crate::view::{RunStatus, WorkflowView};
use crate::worker::StepWorker;

mod builder;
mod cancel;
mod enqueue;
mod terminate;
#[cfg(test)]
mod tests;

pub use builder::WorkflowRuntimeBuilder;
pub(crate) use enqueue::{StepEnqueueOpts, StepWait};

/// The queue name of a runtime built without
/// [`WorkflowRuntimeBuilder::queue_name`].
pub const DEFAULT_QUEUE_NAME: &str = "workflow-steps";

/// The settings of a run's steps, applied by [`WorkflowRuntime::submit`]
/// through [`RunSpec::options`], by [`RunGroup::submit`] and
/// [`RunGroup::resume`] to every member of a group and by
/// [`jobs::JobRunner::submit_with`](crate::jobs::JobRunner::submit_with) to a
/// job.
#[derive(Debug, Clone, Default)]
pub struct RunOptions {
    /// Submitter-supplied metadata, threaded through every step of the run and
    /// surfaced to the terminal hook. Reserved `workflow.*` keys are rejected
    /// at submission with [`Error::ReservedHeader`].
    pub headers: HashMap<String, String>,
    /// Priority of every step, or the queue's default when `None`.
    pub priority: Option<u32>,
    /// Attempt limit of every step, or the queue's `max_attempts` when `None`.
    pub max_attempts_per_step: Option<u32>,
    /// Earliest time the first step can run. The step-0 job waits in the
    /// queue's scheduled state until the queue's clock passes this time. With
    /// `None`, the step-0 job is claimable at once.
    pub run_at: Option<SystemTime>,
}

/// Spec passed to [`WorkflowRuntime::submit`].
#[derive(Debug, Clone, Default)]
pub struct RunSpec {
    /// Caller-supplied run identifier. If `None`, the runtime generates a ULID.
    /// The dedup key for the first step job is `run:{run_id}:0`, so
    /// re-submitting the same `run_id` while the run is active returns the
    /// existing job and does not create a duplicate.
    ///
    /// A run id identifies one run until
    /// [`WorkflowRuntimeBuilder::memo_retention`] removes the terminal record
    /// of the run, so a re-submission of a terminated run reports that run.
    /// Without memo retention a run id identifies one run permanently.
    pub run_id: Option<RunId>,
    /// Bytes that the runner receives as the first step's payload.
    pub input: Vec<u8>,
    /// The settings of the run's steps.
    pub options: RunOptions,
    /// Effects applied in the same transaction as the step-0 enqueue, as
    /// [`taquba::Queue::enqueue_with_effects`] applies them: the enqueues, the
    /// KV writes, the KV deletes, the expiry entries and the count changes.
    /// Applied only when the submission is new: a duplicate submission's
    /// effects are dropped, and the effects do not participate in the
    /// duplicate-submission input check. A KV key written or deleted must not
    /// start with the reserved `workflow/` prefix
    /// ([`RESERVED_KV_PREFIX`](crate::RESERVED_KV_PREFIX)), and neither must
    /// the name of a count
    /// ([`RESERVED_COUNT_PREFIX`](crate::RESERVED_COUNT_PREFIX)). A header of
    /// an enqueue must not start with the reserved `workflow.*` prefix
    /// ([`RESERVED_HEADER_PREFIX`](crate::RESERVED_HEADER_PREFIX)). An enqueue
    /// must not target the queue of the runtime. Values are capped at
    /// [`taquba::MAX_KV_VALUE_SIZE`].
    pub effects: SettlementEffects,
}

/// Outcome of [`WorkflowRuntime::submit`].
///
/// `submit` is idempotent on `run_id`: re-submitting an active or terminated
/// run is a no-op and the returned `SubmitOutcome` has
/// `newly_submitted = false`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitOutcome {
    /// The run's identifier (generated if the spec did not include one).
    pub run_id: RunId,
    /// `true` if this call enqueued a new run. `false` if a run with this id
    /// exists (its durable run record or its terminal record) and this call was
    /// a no-op. Call [`WorkflowRuntime::status`] for the run's current state
    /// when needed.
    pub newly_submitted: bool,
    /// The id of the queue job currently representing the run: its first step
    /// for a new submission, the step an active run reached, read from its
    /// durable current-step pointer, and the final step of a terminated run.
    pub job_id: String,
}

/// Outcome of [`WorkflowRuntime::cancel`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelRunOutcome {
    /// The queued step was removed, and the run terminated as cancelled.
    Removed,
    /// The request reached a running step, which terminates the run when it
    /// returns.
    Requested,
    /// The run is unknown, terminated or dead-lettered outside the worker.
    NotActive,
}

impl<R> WorkflowRuntime<R> {
    /// The runtime over `core` with `runner`.
    fn from_parts(runner: R, core: Arc<RuntimeCore>) -> Self {
        Self {
            inner: Arc::new(RuntimeInner { runner, core }),
        }
    }
}

/// The retained state of a terminated run: its memo and step-output entries in
/// the object store, and its terminal record and run KV entries in the queue's
/// KV namespace, removed together by the memo sweep.
struct RunStore {
    memo_store: MemoStore,
    queue: Arc<Queue>,
    view: WorkflowView,
}

impl Clearable for RunStore {
    type Error = Error;

    /// A run id without the terminal record of the marker belongs to a later
    /// submission, after [`RunGroup::forget`] removed the record of the run
    /// that the marker names.
    async fn clear(&self, run_id: &RunId, marked_at_ms: u64) -> Result<Cleared> {
        let marked = self
            .view
            .terminal_record(run_id)
            .await?
            .is_some_and(|termination| termination.terminated_at_ms == marked_at_ms);
        if !marked {
            return Ok(Cleared::Superseded);
        }
        let mut keys = run_kv_entry_keys(self.queue.view(), run_id).await?;
        self.memo_store.clear_memos_for_run(run_id).await?;
        keys.push(run_kv_key(run_id));
        Ok(Cleared::Removed(keys))
    }
}

/// Durable runtime for workflow runs. A clone copies one `Arc`.
pub struct WorkflowRuntime<R> {
    pub(crate) inner: Arc<RuntimeInner<R>>,
}

impl<R> Clone for WorkflowRuntime<R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// A runtime's [`StepRunner`], with the shared [`RuntimeCore`] it operates on:
/// the executing half of a runtime, held by the worker.
pub(crate) struct RuntimeInner<R> {
    pub(crate) runner: R,
    pub(crate) core: Arc<RuntimeCore>,
}

/// The control half of a runtime: the queue, the stores and the settings, with
/// every operation that reads or settles run state without invoking the runner.
/// It is not generic over the runner type, so the worker, [`WorkflowRuntime`]
/// and [`RunGroup`] share it.
pub(crate) struct RuntimeCore {
    pub(crate) queue: Arc<Queue>,
    pub(crate) view: WorkflowView,
    pub(crate) queue_name: String,
    max_concurrent_steps: usize,
    poll_interval: Duration,
    pub(crate) memo_store: MemoStore,
    pub(crate) group_store: GroupStore,
    /// The sweep that removes the memo entries and the terminal record of a run
    /// a window after its termination, when
    /// [`WorkflowRuntimeBuilder::memo_retention`] is set. Without it no
    /// terminal marker is written.
    pub(crate) memo_sweep: Option<Arc<Sweep>>,
    /// The sweep that removes the state of a group a window after its last
    /// member terminated, when [`WorkflowRuntimeBuilder::group_retention`] is
    /// set. Without it no group marker is written.
    pub(crate) group_sweep: Option<Arc<Sweep>>,
    /// Whether runner-returned step outcomes are persisted and replayed by
    /// `(run_id, step_number, SHA-256(step payload))`.
    pub(crate) step_output_replay: bool,
    /// Time source. Defaults to the queue's clock. A test can substitute a
    /// [`MockClock`](taquba::MockClock) to virtualise time.
    pub(crate) clock: Arc<dyn Clock>,
    /// [`WorkflowRuntimeBuilder::terminal_hook`].
    terminal_hook: Option<Arc<dyn TerminalHook>>,
    /// [`WorkflowRuntimeBuilder::control_queue`].
    control_queue: Option<String>,
}

impl<R: StepRunner> WorkflowRuntime<R> {
    /// Submit a new run. Enqueues step 0 with payload `spec.input`.
    ///
    /// Idempotent on `(run_id, spec.input)`: if a run with the same id is
    /// active or terminated (its durable run record or its terminal record in
    /// Taquba's user KV namespace exists, whichever process submitted it) and
    /// `spec.input` matches the original submission, this call is a no-op and
    /// the returned [`SubmitOutcome`] has `newly_submitted = false`. A
    /// re-submission of such a `run_id` with a *different* input is rejected
    /// with [`Error::InputMismatch`]. A new run requires a fresh `run_id`.
    pub async fn submit(&self, spec: RunSpec) -> Result<SubmitOutcome> {
        self.inner.core.submit(spec).await
    }

    /// The runtime's view of its store: the read-only queries over the queue
    /// and the memo store the runtime writes to.
    pub fn view(&self) -> &WorkflowView {
        &self.inner.core.view
    }

    /// [`WorkflowView::status`] through the runtime's view, so the status is
    /// available after a restart and from any runtime over the same queue.
    pub async fn status(&self, run_id: &RunId) -> Result<Option<RunStatus>> {
        self.inner.core.view.status(run_id).await
    }

    /// Wait until the run `run_id` terminates and report its end. The wait
    /// follows the run's current step across its steps, so it reports a run of
    /// any length, after a restart and from any runtime over the same queue.
    /// For a run that has already terminated, the wait reads its records
    /// without waiting. A step dead-lettered outside the worker is terminated
    /// by the worker's dead-step reconciliation, which the wait polls for at
    /// the poll interval.
    ///
    /// Returns [`Error::RunNotFound`] for a run without a record in the
    /// runtime: never submitted, or terminated and swept. Returns
    /// [`Error::InconsistentRunState`] for a succeeded run whose terminal
    /// record does not have the reference to its result.
    pub async fn wait(&self, run_id: &RunId) -> Result<TerminatedRun> {
        self.inner.core.wait(run_id).await
    }

    /// [`Self::wait`] bounded by `timeout`. Returns `Ok(None)` when the timeout
    /// elapses first.
    pub async fn wait_timeout(
        &self,
        run_id: &RunId,
        timeout: Duration,
    ) -> Result<Option<TerminatedRun>> {
        self.inner.core.wait_timeout(run_id, timeout).await
    }

    /// Request cancellation of an active run for `reason`.
    ///
    /// The run terminates with `reason` on [`RunTermination::error`] and
    /// [`CancelOrigin::Request`] on [`RunTermination::cancelled_by`]. A run
    /// with a recorded request keeps the reason of the first request.
    ///
    /// Returns [`CancelRunOutcome::Removed`] or [`CancelRunOutcome::Requested`]
    /// once the request is recorded on the run's durable record, and
    /// [`CancelRunOutcome::NotActive`] if the run is unknown or already
    /// terminal. A run whose current step the queue dead-lettered outside the
    /// worker is not active, and the worker's dead-step reconciliation
    /// terminates it as failed. The request reaches a run after a restart and
    /// from any runtime over the same queue.
    ///
    /// The run terminates as
    /// [`TerminalStatus::Cancelled`](crate::TerminalStatus::Cancelled):
    ///
    /// - **Pending / scheduled step**: the queued step job is removed
    ///   and the run terminated in one transaction before this call
    ///   returns.
    /// - **Running step**: cancellation is delivered to the runner via
    ///   [`Delivery::cancel_token`](crate::Delivery::cancel_token). Runners
    ///   that watch the token short-circuit immediately. Runners that ignore
    ///   the token are allowed to run to completion (futures cannot be safely
    ///   aborted mid-step). In both cases the runner's [`StepOutcome`] /
    ///   [`StepError`](crate::StepError) is discarded and the worker settles
    ///   the run once the step returns, with any pending transient retry
    ///   suppressed and the step acked, never nacked.
    /// - A step claimed after the request is settled as cancelled
    ///   without running.
    ///
    /// Cancellation is best-effort: a run whose terminal step settles while the
    /// request is recorded keeps the outcome it committed. A request applies
    /// only to the run it is recorded on, and a later submission of the same
    /// run id starts without it.
    ///
    /// [`RunTermination::error`]: crate::RunTermination::error
    /// [`RunTermination::cancelled_by`]: crate::RunTermination::cancelled_by
    /// [`CancelOrigin::Request`]: crate::CancelOrigin::Request
    /// [`StepOutcome`]: crate::StepOutcome
    pub async fn cancel(&self, run_id: &RunId, reason: Option<String>) -> Result<CancelRunOutcome> {
        self.inner.core.cancel_run(run_id, reason).await
    }

    /// The group named `id`.
    pub fn group(&self, id: RunId) -> RunGroup {
        RunGroup::new(self.inner.core.clone(), id)
    }

    /// A group with a generated id.
    pub fn new_group(&self) -> RunGroup {
        RunGroup::new(
            self.inner.core.clone(),
            RunId::generate(&self.inner.core.queue),
        )
    }

    /// Spawn [`Self::run`] as a Tokio task and return a handle for graceful
    /// shutdown. The worker runs until `shutdown` resolves or
    /// [`RunnerHandle::shutdown`] is called. In-flight steps finish either way.
    pub fn spawn<F>(&self, shutdown: F) -> RunnerHandle
    where
        F: Future<Output = ()> + Send + 'static,
        R: 'static,
    {
        let runtime = self.clone();
        WorkerHandle::spawn(shutdown, |stop| async move { runtime.run_with(stop).await })
    }

    /// Run the step worker loop until `shutdown` resolves. Spawns up to
    /// `max_concurrent_steps` step processors, the dead-step reconciliation
    /// that terminates runs whose step the queue dead-lettered outside the
    /// worker, a memo-retention sweeper when
    /// [`WorkflowRuntimeBuilder::memo_retention`] is set and a command worker
    /// when [`WorkflowRuntimeBuilder::control_queue`] is set, all running in
    /// parallel. All halt cleanly when `shutdown` resolves or a worker errors.
    pub async fn run<F>(&self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()>,
        R: 'static,
    {
        let stop = CancellationToken::new();
        let mut worker = std::pin::pin!(self.run_with(stop.clone()));
        tokio::select! {
            res = &mut worker => res,
            () = shutdown => {
                stop.cancel();
                worker.await
            }
        }
    }

    /// [`Self::run`] over a token: the worker and the background loops stop
    /// when `stop` is cancelled, and `stop` is cancelled when the worker
    /// returns on its own, so the background loops halt with it.
    async fn run_with(&self, stop: CancellationToken) -> Result<()>
    where
        R: 'static,
    {
        let mut background: Vec<_> = self
            .inner
            .core
            .sweeps()
            .map(|sweep| {
                let sweep = sweep.clone();
                let core = self.inner.core.clone();
                let token = stop.clone();
                tokio::spawn(async move {
                    sweep
                        .run(&core.queue, &*core.clock, core.poll_interval, token)
                        .await;
                })
            })
            .collect();
        background.push({
            let core = self.inner.core.clone();
            let token = stop.clone();
            tokio::spawn(async move { core.run_dead_step_reconciliation(token).await })
        });
        let commands = self.inner.core.control_queue.clone().map(|control| {
            let core = self.inner.core.clone();
            let token = stop.clone();
            tokio::spawn(async move {
                let worker = CommandWorker { core: core.clone() };
                let shutdown = token.clone().cancelled_owned();
                let result = taquba::run_worker(
                    &core.queue,
                    &control,
                    &worker,
                    core.poll_interval,
                    shutdown,
                )
                .await;
                token.cancel();
                result
            })
        });

        let worker = Arc::new(StepWorker {
            inner: self.inner.clone(),
        });
        let result = taquba::run_worker_concurrent(
            &self.inner.core.queue,
            &self.inner.core.queue_name,
            worker,
            self.inner.core.max_concurrent_steps,
            self.inner.core.poll_interval,
            stop.clone().cancelled_owned(),
        )
        .await;
        stop.cancel();

        for handle in background {
            let _ = handle.await;
        }
        let commanded = match commands {
            Some(handle) => handle.await.unwrap_or(Ok(())),
            None => Ok(()),
        };

        result?;
        commanded?;
        Ok(())
    }
}

/// A handle to a worker task spawned by [`WorkflowRuntime::spawn`].
///
/// Dropping a `RunnerHandle` does not stop the worker: the task continues until
/// the `shutdown` future passed to `spawn` resolves. Call
/// [`shutdown`](WorkerHandle::shutdown) or [`wait`](WorkerHandle::wait) to stop
/// or join the worker explicitly.
pub type RunnerHandle = WorkerHandle<Result<()>>;

impl RuntimeCore {
    /// [`WorkflowRuntime::wait`].
    pub(crate) async fn wait(&self, run_id: &RunId) -> Result<TerminatedRun> {
        self.wait_run(run_id)
            .await?
            .ok_or_else(|| Error::RunNotFound(run_id.clone()))
    }

    /// [`WorkflowRuntime::wait_timeout`].
    pub(crate) async fn wait_timeout(
        &self,
        run_id: &RunId,
        timeout: Duration,
    ) -> Result<Option<TerminatedRun>> {
        match tokio::time::timeout(timeout, self.wait(run_id)).await {
            Ok(end) => end.map(Some),
            Err(_) => Ok(None),
        }
    }

    /// The retention sweeps [`WorkflowRuntime::run`] runs.
    fn sweeps(&self) -> impl Iterator<Item = &Arc<Sweep>> {
        self.memo_sweep.iter().chain(self.group_sweep.iter())
    }

    /// One pass of every retention sweep. Returns the number of markers
    /// removed.
    #[cfg(test)]
    pub(crate) async fn sweep_once(&self) -> Result<usize> {
        let mut removed = 0;
        for sweep in self.sweeps() {
            removed += sweep.pass(&self.queue, &*self.clock).await?;
        }
        Ok(removed)
    }

    /// The current-step pointer of a run whose durable record exists.
    pub(crate) async fn current_step(&self, run_id: &RunId) -> Result<DurableStepRef> {
        self.view
            .current_step_if_active(run_id)
            .await?
            .ok_or_else(|| Error::InconsistentRunState(run_id.clone()))
    }

    /// Wait until `run_id` terminates, following its current step across steps,
    /// and report its end. Returns `None` for a run without a current step or a
    /// record. A current step the queue dead-lettered outside the worker is
    /// polled at the poll interval until reconciliation terminates the run.
    pub(crate) async fn wait_run(&self, run_id: &RunId) -> Result<Option<TerminatedRun>> {
        loop {
            let Some((current, _)) = self.view.current_job(run_id).await? else {
                return self.view.terminated_run(run_id).await;
            };
            match self.queue.wait_for_completion(&current.job_id).await? {
                // `NotFound`: the job settled between the two reads, and the
                // next read follows the pointer.
                WaitOutcome::Done(_) | WaitOutcome::Cancelled | WaitOutcome::NotFound => {}
                WaitOutcome::Dead(_) => {
                    // A worker-path dead-letter deletes the pointer with its
                    // settlement. A pointer that still refers to the dead job
                    // identifies a dead-letter outside the worker, which
                    // reconciliation terminates.
                    let unreconciled = self
                        .view
                        .current_step_if_active(run_id)
                        .await?
                        .is_some_and(|step| step.job_id == current.job_id);
                    if unreconciled {
                        tokio::time::sleep(self.poll_interval).await;
                    }
                }
            }
        }
    }
}
