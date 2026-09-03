//! The typed single-step run of the [`jobs`](crate::jobs) module: the
//! runtime it runs over
//! ([`TypedRuntime`]) and the settings it forwards to the workflow
//! runtime ([`TypedRuntimeOptions`]), the adapter that decodes a typed
//! input, runs a typed handler and encodes its output
//! ([`run_typed_step`]), the durable outcome record it writes before
//! its settlement, stored in the run-scoped memo under a key reserved
//! by this crate, and the in-process wait for a run's terminal state
//! ([`TypedRuntime::wait_terminal`]).

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use taquba::object_store::ObjectStore;
use taquba::{Clock, Queue, WaitOutcome};

use crate::group::RunGroup;
use crate::keys::hash_input;
use crate::memo::Memo;
use crate::runtime::{RunnerHandle, RuntimeCore, WorkflowRuntime, WorkflowRuntimeBuilder};
use crate::terminal::NoopTerminalHook;
use crate::{Result, Step, StepError, StepErrorKind, StepOutcome, StepRunner};

/// The settings a typed single-step layer forwards to the workflow
/// runtime it runs over. Each layer's builder collects them under its
/// own vocabulary and defaults.
pub(crate) struct TypedRuntimeOptions {
    pub(crate) queue_name: String,
    pub(crate) memo_prefix: String,
    pub(crate) max_concurrent: usize,
    pub(crate) poll_interval: Duration,
    /// The clock the runtime reads; the queue's when `None`.
    pub(crate) clock: Option<Arc<dyn Clock>>,
}

impl TypedRuntimeOptions {
    /// Build the runtime over `runner`; `configure` applies the
    /// layer's own builder settings before the build.
    pub(crate) fn build<R: StepRunner>(
        self,
        queue: Arc<Queue>,
        object_store: Arc<dyn ObjectStore>,
        runner: R,
        configure: impl FnOnce(
            WorkflowRuntimeBuilder<R, NoopTerminalHook>,
        ) -> WorkflowRuntimeBuilder<R, NoopTerminalHook>,
    ) -> TypedRuntime<R> {
        let mut builder = WorkflowRuntime::builder(queue, object_store, runner, NoopTerminalHook)
            .queue_name(self.queue_name)
            .memo_prefix(self.memo_prefix)
            .max_concurrent_steps(self.max_concurrent)
            .poll_interval(self.poll_interval);
        if let Some(clock) = self.clock {
            builder = builder.clock(clock);
        }
        TypedRuntime {
            runtime: configure(builder).build(),
            spawned: AtomicBool::new(false),
        }
    }
}

/// The workflow runtime a typed single-step layer runs over. The
/// terminal hook is [`NoopTerminalHook`], so a run enqueues no
/// notification.
pub(crate) struct TypedRuntime<R: StepRunner> {
    pub(crate) runtime: WorkflowRuntime<R, NoopTerminalHook>,
    spawned: AtomicBool,
}

impl<R: StepRunner + 'static> TypedRuntime<R> {
    fn core(&self) -> &RuntimeCore {
        &self.runtime.inner.core
    }

    /// The group named `id`; see [`WorkflowRuntime::group`].
    pub(crate) fn group(&self, id: impl Into<String>) -> Result<RunGroup<'_, R, NoopTerminalHook>> {
        self.runtime.group(id)
    }

    /// A group with a generated id; see [`WorkflowRuntime::new_group`].
    pub(crate) fn new_group(&self) -> RunGroup<'_, R, NoopTerminalHook> {
        self.runtime.new_group()
    }

    /// The run-scoped memo of `run_id`, which holds its outcome record.
    pub(crate) fn run_memo(&self, run_id: &str) -> Memo {
        self.core().memo_store.new_run_memo(run_id)
    }

    /// The outcome record of `run_id`, if one exists.
    pub(crate) async fn outcome(&self, run_id: &str) -> Result<Option<OutcomeRecord>> {
        read_outcome(&self.run_memo(run_id)).await
    }

    /// Spawn the worker. Panics on a second call: the runtime is
    /// single-writer and its layer spawns one worker.
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
    /// `job_id`, to reach a terminal state, then read its outcome
    /// record. Returns `Ok(None)` when the timeout elapses first.
    pub(crate) async fn wait_terminal_within(
        &self,
        run_id: &str,
        job_id: &str,
        timeout: Duration,
    ) -> Result<Option<Terminal>> {
        match self
            .core()
            .queue
            .wait_for_completion_timeout(job_id, timeout)
            .await?
        {
            Some(outcome) => Ok(Some(self.terminal(run_id, outcome).await?)),
            None => Ok(None),
        }
    }

    /// [`Self::wait_terminal_within`] without a bound.
    pub(crate) async fn wait_terminal(&self, run_id: &str, job_id: &str) -> Result<Terminal> {
        let outcome = self.core().queue.wait_for_completion(job_id).await?;
        self.terminal(run_id, outcome).await
    }

    /// The terminal state of the run `run_id` whose step job settled
    /// with `outcome`: the outcome record when the step wrote one.
    async fn terminal(&self, run_id: &str, outcome: WaitOutcome) -> Result<Terminal> {
        let unrecorded = match outcome {
            WaitOutcome::Done(_) => Unrecorded::Done,
            WaitOutcome::Dead(record) => Unrecorded::Dead(record.last_error),
            WaitOutcome::Cancelled => Unrecorded::Cancelled,
            WaitOutcome::NotFound => Unrecorded::NotFound,
        };
        Ok(match self.outcome(run_id).await? {
            Some(record) => Terminal::Recorded(record),
            None => Terminal::Unrecorded(unrecorded),
        })
    }
}

/// Run-memo key of the outcome record. Runners receive the step-scoped
/// memo only, so no caller key can collide with it.
pub(crate) const OUTCOME_KEY: &str = "workflow.outcome";

/// The persisted terminal outcome of one typed single-step run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OutcomeRecord {
    /// SHA-256 of the serialized input, checked by an idempotent
    /// re-submission after completion.
    pub(crate) input_hash: [u8; 32],
    pub(crate) outcome: StoredOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum StoredOutcome {
    Success {
        output: Vec<u8>,
    },
    Failure {
        kind: StoredErrorKind,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum StoredErrorKind {
    Transient,
    Permanent,
}

impl From<StepErrorKind> for StoredErrorKind {
    fn from(kind: StepErrorKind) -> Self {
        match kind {
            StepErrorKind::Transient => Self::Transient,
            StepErrorKind::Permanent => Self::Permanent,
        }
    }
}

impl From<StoredErrorKind> for StepErrorKind {
    fn from(kind: StoredErrorKind) -> Self {
        match kind {
            StoredErrorKind::Transient => Self::Transient,
            StoredErrorKind::Permanent => Self::Permanent,
        }
    }
}

/// Run one typed single-step delivery: decode `input`, the serialized
/// typed input carried in the step's payload, as `I`, run `handler` on it
/// and encode its `O` as the step's result, recording the outcome through
/// [`run_recorded`]. An input that does not decode and an output that
/// does not encode are permanent errors, since a retry cannot change
/// either; `name` identifies the typed handler in those messages.
pub(crate) async fn run_typed_step<I, O, F, Fut>(
    step: &Step,
    name: &str,
    input: &[u8],
    handler: F,
) -> std::result::Result<StepOutcome, StepError>
where
    I: DeserializeOwned,
    O: Serialize,
    F: FnOnce(I) -> Fut,
    Fut: Future<Output = std::result::Result<O, StepError>>,
{
    run_recorded(step, async {
        let input: I = rmp_serde::from_slice(input)
            .map_err(|err| StepError::permanent(format!("invalid input for `{name}`: {err}")))?;
        let output = handler(input).await?;
        rmp_serde::to_vec_named(&output).map_err(|err| {
            StepError::permanent(format!(
                "`{name}` produced an output that failed to serialize: {err}"
            ))
        })
    })
    .await
}

/// Run the typed part of a single-step run and record its outcome: on
/// `Ok(bytes)` the success record is written before the step succeeds with
/// `bytes`; on `Err` a failure record is written when the error is
/// permanent or the attempt is the step's last, and the error is returned.
/// A failed success-record write is a transient error, so the step
/// retries.
async fn run_recorded<F>(step: &Step, produce: F) -> std::result::Result<StepOutcome, StepError>
where
    F: Future<Output = std::result::Result<Vec<u8>, StepError>>,
{
    let input_hash = hash_input(&step.payload);
    match produce.await {
        Ok(bytes) => {
            let record = OutcomeRecord {
                input_hash,
                outcome: StoredOutcome::Success {
                    output: bytes.clone(),
                },
            };
            write_outcome(&step.run_memo, &record)
                .await
                .map_err(|err| StepError::transient(err.to_string()))?;
            Ok(StepOutcome::Succeed { result: bytes })
        }
        Err(err) => {
            if matches!(err.kind, StepErrorKind::Permanent) || step.is_last_attempt() {
                let record = OutcomeRecord {
                    input_hash,
                    outcome: StoredOutcome::Failure {
                        kind: err.kind.into(),
                        message: err.message.clone(),
                    },
                };
                if let Err(write_err) = write_outcome(&step.run_memo, &record).await {
                    tracing::warn!(
                        run_id = %step.run_id,
                        "failed to persist the run's failure outcome: {write_err}"
                    );
                }
            }
            Err(err)
        }
    }
}

/// Read the outcome record of `run_memo`. A record that fails to decode is
/// treated as absent.
pub(crate) async fn read_outcome(run_memo: &Memo) -> Result<Option<OutcomeRecord>> {
    match run_memo.get(OUTCOME_KEY).await? {
        None => Ok(None),
        Some(bytes) => match rmp_serde::from_slice(&bytes) {
            Ok(record) => Ok(Some(record)),
            Err(err) => {
                tracing::warn!(
                    run_id = %run_memo.run_id(),
                    error = %err,
                    "outcome record failed to decode; treated as absent",
                );
                Ok(None)
            }
        },
    }
}

pub(crate) async fn write_outcome(run_memo: &Memo, record: &OutcomeRecord) -> Result<()> {
    let bytes = rmp_serde::to_vec_named(record)?;
    run_memo.put(OUTCOME_KEY, &bytes).await
}

/// How a typed single-step run ended, as observed by an in-process
/// waiter: with its outcome record, or without one.
pub(crate) enum Terminal {
    Recorded(OutcomeRecord),
    /// The run ended without the step recording an outcome: it was
    /// cancelled, dead-lettered by the reaper or its records are gone.
    Unrecorded(Unrecorded),
}

pub(crate) enum Unrecorded {
    /// The queue job was acknowledged; only an external cancellation
    /// acknowledges a typed step without a record.
    Done,
    /// The queue job was dead-lettered outside the step, with the queue
    /// record's last error.
    Dead(Option<String>),
    /// The queue job was removed by a cancellation before it was claimed.
    Cancelled,
    /// Neither a queue record nor an outcome record exists.
    NotFound,
}
