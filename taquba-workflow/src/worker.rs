//! The worker path: how a claimed job is identified, run and settled.
//! [`StepWorker`] is the [`Worker`] the runtime drives; a step job is
//! parsed into a [`StepDelivery`], run through the [`StepRunner`] and
//! settled into the [`SettlementEffects`] of its outcome, and a
//! terminal-notification job runs the [`TerminalHook`].

use std::collections::HashMap;
use std::sync::Arc;

use taquba::{
    FailWith, JobRecord, LeaseHandle, PermanentFailure, SettlementEffects, Worker, WorkerError,
};
use tracing::{debug, warn};

use crate::durable::DurableRunOutcome;
use crate::effects::{EffectsHandle, TerminalEffects};
use crate::error::Error;
use crate::keys::{HEADER_RUN_ID, HEADER_STEP, HEADER_TERMINAL, RESERVED_HEADER_PREFIX};
use crate::kv::KvReadHandle;
use crate::runner::{Step, StepError, StepErrorKind, StepOutcome, StepRunner, Trigger};
use crate::runtime::{RuntimeInner, StepEnqueueOpts};
use crate::terminal::{RunOutcome, TerminalHook};

/// The [`Worker`] of a runtime: every claimed job of the runtime's
/// queue is processed by [`RuntimeInner::process_step`].
pub(crate) struct StepWorker<R, H> {
    pub(crate) inner: Arc<RuntimeInner<R, H>>,
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

/// One claimed step job as the worker path identifies it: the run and
/// step named by the job's reserved headers, the submitter's headers
/// with the reserved ones removed and the queue record itself.
pub(crate) struct StepDelivery<'a> {
    pub(crate) run_id: String,
    pub(crate) step_number: u32,
    /// Submitter-supplied headers, without the reserved `workflow.` keys.
    pub(crate) headers: HashMap<String, String>,
    pub(crate) job: &'a JobRecord,
}

impl<'a> StepDelivery<'a> {
    /// Identify the delivery from `job`'s headers. Fails, permanently,
    /// for a job without the run id header or with a step header that
    /// is not a `u32`.
    pub(crate) fn parse(job: &'a JobRecord) -> std::result::Result<Self, Error> {
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
        let headers = job
            .headers
            .iter()
            .filter(|(k, _)| !k.starts_with(RESERVED_HEADER_PREFIX))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Ok(Self {
            run_id,
            step_number,
            headers,
            job,
        })
    }

    /// The enqueue options of the run's next step: this step's priority
    /// and attempt limit, so a run's per-step settings carry across the
    /// step boundary.
    pub(crate) fn next_step_opts(&self) -> StepEnqueueOpts {
        StepEnqueueOpts {
            run_at: None,
            priority: Some(self.job.priority),
            max_attempts: Some(self.job.max_attempts),
            reserved_headers: Vec::new(),
        }
    }

    /// Whether a transient failure of this attempt dead-letters the
    /// step.
    pub(crate) fn is_last_attempt(&self) -> bool {
        self.job.attempts >= self.job.max_attempts
    }

    /// A `Succeeded` outcome of the run at this step.
    pub(crate) fn succeeded(&self, result: Vec<u8>) -> RunOutcome {
        RunOutcome::succeeded(
            self.run_id.clone(),
            result,
            self.headers.clone(),
            self.step_number,
        )
    }

    /// A `Failed` outcome of the run at this step.
    pub(crate) fn failed(&self, error: String) -> RunOutcome {
        RunOutcome::failed(
            self.run_id.clone(),
            error,
            self.headers.clone(),
            self.step_number,
        )
    }

    /// A `Cancelled` outcome of the run at this step; `reason` is
    /// `None` for an external cancellation.
    pub(crate) fn cancelled(&self, reason: Option<String>) -> RunOutcome {
        RunOutcome::cancelled(
            self.run_id.clone(),
            reason,
            self.headers.clone(),
            self.step_number,
        )
    }
}

/// The worker error reporting `err`: a [`PermanentFailure`] when a
/// retry cannot change the outcome, a retrying error otherwise.
fn worker_error(err: &Error) -> WorkerError {
    if err.is_permanent() {
        PermanentFailure::new(err.to_string()).into()
    } else {
        err.to_string().into()
    }
}

/// The worker error reporting a failure of `kind` with `message`.
fn failure_error(message: String, kind: StepErrorKind) -> WorkerError {
    match kind {
        StepErrorKind::Permanent => PermanentFailure::new(message).into(),
        StepErrorKind::Transient => message.into(),
    }
}

impl<R: StepRunner, H: TerminalHook> RuntimeInner<R, H> {
    /// [`Self::terminate_collecting_effects`] for an outcome reached by
    /// `delivery`, plus [`RuntimeCore::forget_run`](crate::runtime::RuntimeCore::forget_run):
    /// the pairing every worker-path termination site performs before
    /// its settlement commits.
    pub(crate) fn worker_terminate(
        &self,
        delivery: &StepDelivery<'_>,
        outcome: RunOutcome,
    ) -> SettlementEffects {
        let effects = self.terminate_collecting_effects(&outcome, Some(delivery.job));
        self.core.forget_run(&outcome.run_id);
        effects
    }

    /// The error a step returns for a failure that terminates its run:
    /// the effects of the `Failed` termination plus `failure_writes`,
    /// carried on a [`FailWith`] so the core applies them only with the
    /// dead-lettering settlement.
    pub(crate) fn terminating_failure(
        &self,
        delivery: &StepDelivery<'_>,
        message: String,
        kind: StepErrorKind,
        failure_writes: HashMap<Vec<u8>, Vec<u8>>,
    ) -> WorkerError {
        let mut effects = self.worker_terminate(delivery, delivery.failed(message.clone()));
        effects.kv_writes.extend(failure_writes);
        FailWith::new(failure_error(message, kind), effects).into()
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
            Err(StepError { message, kind }) => Err(failure_error(message, kind)),
        }
    }

    /// Process one claimed job of the runtime's queue: a notification
    /// job runs the terminal hook; a step job is run through the
    /// runner, or its stored outcome replayed, and settled with the
    /// effects of the outcome.
    pub(crate) async fn process_step(
        &self,
        job: &JobRecord,
        lease: &LeaseHandle,
    ) -> std::result::Result<SettlementEffects, WorkerError> {
        if job.headers.contains_key(HEADER_TERMINAL) {
            return self.process_notification(job).await;
        }

        let delivery = match StepDelivery::parse(job) {
            Ok(delivery) => delivery,
            Err(err) => {
                warn!(job_id = %job.id, error = %err, "workflow step has malformed headers");
                return Err(worker_error(&err));
            }
        };
        let run_id = delivery.run_id.as_str();
        let step_number = delivery.step_number;

        self.core
            .registry
            .mark_running(run_id, step_number, &job.id, &delivery.headers);

        // `Queue::cancel` fires the claim's token, and a re-claim fires it
        // again from the job's persisted `cancel_requested`. The runner
        // receives a child, so a runner firing its own token is not
        // treated as an external cancellation below.
        let claim_cancel = lease.cancel_token().clone();

        let (step_signal, signal_kv_deletes) = self
            .core
            .resolve_step_signal(job, run_id, step_number)
            .await
            // Transient: the step retries and resolves again.
            .map_err(|err| WorkerError::from(err.to_string()))?;

        let effects_handle = EffectsHandle::for_delivery();
        let step = Step {
            run_id: run_id.to_string(),
            step_number,
            payload: job.payload.clone(),
            headers: delivery.headers.clone(),
            job_id: job.id.clone(),
            attempts: job.attempts,
            max_attempts: job.max_attempts,
            cancel_token: claim_cancel.child_token(),
            lease: lease.clone(),
            memo: self.core.memo_store.new_memo(run_id, step_number),
            run_memo: self.core.memo_store.new_run_memo(run_id),
            effects: effects_handle.clone(),
            kv: KvReadHandle::for_delivery(self.core.queue.clone()),
            signal: step_signal,
        };

        let replayed = if self.core.step_output_replay {
            self.core
                .load_step_output(run_id, step_number, &job.payload)
                .await
                .map_err(|err| WorkerError::from(err.to_string()))?
        } else {
            None
        };
        let (outcome, replayed_effects) = match replayed {
            Some((outcome, effects)) => {
                debug!(run_id = %run_id, step_number, "replaying stored step outcome");
                (Ok(outcome), Some(effects))
            }
            None => (self.runner.run_step(&step).await, None),
        };

        // Sealed as soon as the runner has returned: an effect staged
        // through a retained handle clone after this point could not
        // join the settlement, so staging it errors.
        let sealed = effects_handle.seal_and_take();
        let replayed_step_output = replayed_effects.is_some();
        let caller_effects = replayed_effects.unwrap_or(sealed.outcome);

        // Both sources are required: the claim's token reports a
        // cancellation after a restart, and the registry flag reports one
        // after a step advance, which the job-scoped persisted flag
        // cannot.
        let external_cancel =
            claim_cancel.is_cancelled() || self.core.registry.cancel_requested(run_id);

        if self.core.step_output_replay
            && !replayed_step_output
            && !external_cancel
            && let Ok(ref outcome) = outcome
        {
            self.core
                .store_step_output(run_id, step_number, &job.payload, outcome, &caller_effects)
                .await
                .map_err(|err| worker_error(&err))?;
        }

        let runner_cancelled = matches!(outcome, Ok(StepOutcome::Cancel { .. }));
        let settled = self
            .settle_outcome(&delivery, outcome, external_cancel, sealed.on_failure)
            .await;

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

    /// The settlement of `outcome` for `delivery`: the effects of the
    /// run's transition on an acknowledging outcome, or the error that
    /// retries or dead-letters the step. `failure_writes` are the
    /// runner's reserved writes for a terminating failure.
    ///
    /// Cancellation precedence: a runner-issued [`StepOutcome::Cancel`]
    /// wins and carries its reason on [`RunOutcome::error`]; otherwise
    /// an external cancellation overrides whatever the runner returned,
    /// including a transient retry and a permanent failure, with
    /// `error: None` so consumers can distinguish the two.
    async fn settle_outcome(
        &self,
        delivery: &StepDelivery<'_>,
        outcome: std::result::Result<StepOutcome, StepError>,
        external_cancel: bool,
        failure_writes: HashMap<Vec<u8>, Vec<u8>>,
    ) -> std::result::Result<SettlementEffects, WorkerError> {
        match outcome {
            Ok(StepOutcome::Cancel { reason }) => {
                Ok(self.worker_terminate(delivery, delivery.cancelled(Some(reason))))
            }
            _ if external_cancel => Ok(self.worker_terminate(delivery, delivery.cancelled(None))),
            Ok(StepOutcome::Continue { payload, when }) => match when {
                Trigger::Immediate => Ok(self
                    .core
                    .advance(delivery, payload, delivery.next_step_opts())
                    .await),
                Trigger::After(delay) => {
                    let opts = StepEnqueueOpts {
                        run_at: Some(self.core.run_at_after(delay)),
                        ..delivery.next_step_opts()
                    };
                    Ok(self.core.advance(delivery, payload, opts).await)
                }
                Trigger::OnSignal {
                    correlation_key,
                    timeout,
                } => {
                    self.advance_on_signal(delivery, payload, &correlation_key, timeout)
                        .await
                }
            },
            Ok(StepOutcome::Succeed { result }) => {
                Ok(self.worker_terminate(delivery, delivery.succeeded(result)))
            }
            // A runner verdict: the step ran cleanly and is acknowledged;
            // the run terminates as `Failed` without a dead-letter.
            Ok(StepOutcome::Fail { reason }) => {
                Ok(self.worker_terminate(delivery, delivery.failed(reason)))
            }
            // A permanent error, or a transient one on the last attempt,
            // dead-letters the step and terminates the run; the runner's
            // failure writes apply only with that settlement.
            Err(StepError {
                message,
                kind: kind @ StepErrorKind::Permanent,
            }) => Err(self.terminating_failure(delivery, message, kind, failure_writes)),
            Err(StepError {
                message,
                kind: kind @ StepErrorKind::Transient,
            }) if delivery.is_last_attempt() => {
                Err(self.terminating_failure(delivery, message, kind, failure_writes))
            }
            Err(StepError { message, .. }) => Err(message.into()),
        }
    }
}
