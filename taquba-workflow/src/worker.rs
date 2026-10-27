//! The worker path: how a claimed job is identified, run and settled.
//! [`StepWorker`] is the runtime's [`Worker`]. A step job is parsed into a
//! [`ClaimedStep`], run through the [`StepRunner`] and settled into the
//! [`SettlementEffects`] of its outcome, and a terminal-notification job runs
//! the [`TerminalHook`].

use std::collections::HashMap;
use std::sync::Arc;

use taquba::{
    FailWith, JobRecord, LeaseHandle, PermanentFailure, SettlementEffects, Worker, WorkerError,
};
use tracing::{debug, warn};

use crate::durable::{self, DurableRunOutcome, Submission};
use crate::effects::{EffectsHandle, TerminalEffects};
use crate::error::{Error, worker_error};
use crate::group::Membership;
use crate::keys::{
    HEADER_RUN_ID, HEADER_STEP, HEADER_TERMINAL, RunId, next_step_job_id, submitter_headers,
};
use crate::kv::KvReadHandle;
use crate::runner::{Delivery, Step, StepError, StepErrorKind, StepOutcome, StepRunner, Trigger};
use crate::runtime::StepRef;
use crate::runtime::{RuntimeCore, RuntimeInner, StepEnqueueOpts};
use crate::terminal::{CancelOrigin, RunOutcome, TerminalHook, TerminalStatus};

/// The [`Worker`] of a runtime: every claimed job of the runtime's queue is
/// processed by [`RuntimeInner::process_step`].
pub(crate) struct StepWorker<R, H> {
    pub(crate) inner: Arc<RuntimeInner<R, H>>,
}

impl<R: StepRunner + 'static, H: TerminalHook + 'static> Worker for StepWorker<R, H> {
    async fn process_with_effects(
        &self,
        job: &JobRecord,
        lease: &LeaseHandle,
    ) -> Result<SettlementEffects, WorkerError> {
        self.inner.process_step(job, lease).await
    }
}

/// One claimed step job as the worker path identifies it: the run and step
/// named by the job's reserved headers, the run's group membership when the run
/// is in a group, the submitter's headers with the reserved ones removed and
/// the queue record itself.
pub(crate) struct ClaimedStep<'a> {
    pub(crate) run_id: RunId,
    pub(crate) step_number: u32,
    pub(crate) membership: Option<Membership>,
    /// Submitter-supplied headers, without the reserved `workflow.` keys.
    pub(crate) headers: HashMap<String, String>,
    pub(crate) job: &'a JobRecord,
}

impl<'a> ClaimedStep<'a> {
    /// Identify the claimed step from `job`'s headers. Fails, permanently, for
    /// a job without the run id header, with a run id or group id that is not
    /// valid or with a step header that is not a `u32`.
    pub(crate) fn parse(job: &'a JobRecord) -> Result<Self, Error> {
        let run_id = RunId::new(
            job.headers
                .get(HEADER_RUN_ID)
                .ok_or(Error::MissingHeader(HEADER_RUN_ID))?
                .as_str(),
        )?;
        let step_str = job
            .headers
            .get(HEADER_STEP)
            .ok_or(Error::MissingHeader(HEADER_STEP))?;
        let step_number: u32 = step_str.parse().map_err(|_| Error::InvalidStepHeader {
            header: HEADER_STEP,
            value: step_str.clone(),
        })?;
        let headers = submitter_headers(&job.headers);
        Ok(Self {
            run_id,
            step_number,
            membership: Membership::from_headers(&job.headers)?,
            headers,
            job,
        })
    }

    /// The enqueue options of the run's next step: this step's priority,
    /// attempt limit and group membership, so a run's per-step settings and its
    /// membership hold across the step boundary.
    pub(crate) fn next_step_opts(&self) -> StepEnqueueOpts {
        StepEnqueueOpts {
            run_at: None,
            priority: Some(self.job.priority),
            max_attempts: Some(self.job.max_attempts),
            signal_wait: false,
            reserved_headers: self.reserved_headers_with_none(),
        }
    }

    fn reserved_headers_with_none(&self) -> Vec<(&'static str, String)> {
        self.membership
            .as_ref()
            .map(Membership::reserved_headers)
            .unwrap_or_default()
    }

    /// The outcome of the run terminating at this step with `status`.
    fn outcome(
        &self,
        status: TerminalStatus,
        result: Option<Vec<u8>>,
        error: Option<String>,
    ) -> RunOutcome {
        RunOutcome {
            run_id: self.run_id.clone(),
            status,
            result,
            error,
            cancelled_by: None,
            headers: self.headers.clone(),
            final_step: StepRef {
                number: self.step_number,
                job_id: self.job.id.clone(),
            },
        }
    }

    /// A `Succeeded` outcome of the run at this step.
    pub(crate) fn succeeded(&self, result: Vec<u8>) -> RunOutcome {
        self.outcome(TerminalStatus::Succeeded, Some(result), None)
    }

    /// A `Failed` outcome of the run at this step.
    pub(crate) fn failed(&self, error: String) -> RunOutcome {
        self.outcome(TerminalStatus::Failed, None, Some(error))
    }

    /// A `Cancelled` outcome of the run at this step, cancelled by `origin` for
    /// `reason`.
    pub(crate) fn cancelled(&self, origin: CancelOrigin, reason: Option<String>) -> RunOutcome {
        RunOutcome {
            cancelled_by: Some(origin),
            ..self.outcome(TerminalStatus::Cancelled, None, reason)
        }
    }
}

impl RuntimeCore {
    /// The error a step returns for a failure that terminates its run: the
    /// effects of the `Failed` termination on a [`FailWith`], so the core
    /// applies them only with the dead-lettering settlement. The run result
    /// record is written first, and a failed write is logged.
    pub(crate) async fn terminating_failure(
        &self,
        claimed: &ClaimedStep<'_>,
        error: StepError,
        submission: Submission,
    ) -> WorkerError {
        let outcome = claimed.failed(error.message.clone());
        let termination = self.termination(&outcome, Some(error.kind), submission);
        if let Err(err) = self.store_run_result(&outcome, &termination).await {
            warn!(run_id = %claimed.run_id, "failed to write the run result record: {err}");
        }
        let effects = self.terminate_collecting_effects(&outcome, claimed, termination);
        FailWith::new(error.into_worker_error(), effects).into()
    }

    /// Terminate the run of `claimed` with `outcome` from an acking settlement:
    /// write the run result record, then build the settlement effects. A failed
    /// write is a transient error, so the step is delivered again. `error_kind`
    /// classifies a `Failed` outcome.
    async fn terminate_recorded(
        &self,
        claimed: &ClaimedStep<'_>,
        outcome: RunOutcome,
        submission: Submission,
        error_kind: Option<StepErrorKind>,
    ) -> Result<SettlementEffects, WorkerError> {
        let termination = self.termination(&outcome, error_kind, submission);
        self.store_run_result(&outcome, &termination)
            .await
            .map_err(worker_error)?;
        Ok(self.terminate_collecting_effects(&outcome, claimed, termination))
    }

    /// The settlement of `outcome` for `claimed`: the effects of the run's
    /// transition on an acknowledging outcome, or the error that retries or
    /// dead-letters the step.
    ///
    /// Cancellation precedence: an external cancellation overrides any outcome
    /// that the runner returned, including a [`StepOutcome::Cancel`] that the
    /// request's token led to, a transient retry and a permanent failure.
    async fn settle_outcome(
        &self,
        claimed: &ClaimedStep<'_>,
        outcome: Result<StepOutcome, StepError>,
        external_cancel: Option<ExternalCancel>,
        submission: Submission,
    ) -> Result<SettlementEffects, WorkerError> {
        match (outcome, external_cancel) {
            (_, Some(ExternalCancel { reason })) => {
                let outcome = claimed.cancelled(CancelOrigin::Request, reason);
                self.terminate_recorded(claimed, outcome, submission, None)
                    .await
            }
            (Ok(StepOutcome::Cancel { reason }), None) => {
                let outcome = claimed.cancelled(CancelOrigin::Runner, Some(reason));
                self.terminate_recorded(claimed, outcome, submission, None)
                    .await
            }
            (Ok(StepOutcome::Continue { payload, when }), None) => match when {
                Trigger::Immediate => Ok(self
                    .advance(claimed, payload, claimed.next_step_opts())
                    .await),
                Trigger::After(delay) => {
                    let opts = StepEnqueueOpts {
                        run_at: Some(self.run_at_after(delay)),
                        ..claimed.next_step_opts()
                    };
                    Ok(self.advance(claimed, payload, opts).await)
                }
                Trigger::OnSignal { timeout } => {
                    Ok(self.advance_on_signal(claimed, payload, timeout).await)
                }
            },
            (Ok(StepOutcome::Succeed { result }), None) => {
                self.terminate_recorded(claimed, claimed.succeeded(result), submission, None)
                    .await
            }
            // A runner decision: the step ran cleanly and is acknowledged, and
            // the run terminates as `Failed` without a dead-letter.
            (Ok(StepOutcome::Fail { reason }), None) => {
                self.terminate_recorded(
                    claimed,
                    claimed.failed(reason),
                    submission,
                    Some(StepErrorKind::Permanent),
                )
                .await
            }
            // A permanent error, or a transient one on the last attempt,
            // dead-letters the step and terminates the run.
            (Err(err), None)
                if err.kind == StepErrorKind::Permanent || claimed.job.is_last_attempt() =>
            {
                Err(self.terminating_failure(claimed, err, submission).await)
            }
            (Err(err), None) => Err(err.into_worker_error()),
        }
    }
}

/// An external cancellation that a settlement observed, with the reason of the
/// request on the run record.
pub(crate) struct ExternalCancel {
    pub(crate) reason: Option<String>,
}

impl<R: StepRunner, H: TerminalHook> RuntimeInner<R, H> {
    /// Process a terminal-notification job: decode the committed outcome and
    /// run the configured [`TerminalHook`] as the job's worker. Effects the
    /// hook stages join this job's acknowledgement. A transient hook error
    /// retries the job per the queue's backoff, and a permanent error
    /// dead-letters it.
    async fn process_notification(
        &self,
        job: &JobRecord,
    ) -> Result<SettlementEffects, WorkerError> {
        let outcome: RunOutcome = durable::decode::<DurableRunOutcome>(&job.payload)
            .map_err(|err| {
                warn!(job_id = %job.id, error = %err, "terminal notification has a malformed payload");
                PermanentFailure::new(err.to_string())
            })?
            .into();
        let effects = TerminalEffects::for_delivery(&self.core.queue_name);
        let result = self.terminal_hook.on_termination(&outcome, &effects).await;
        let staged = effects.seal_into_settlement();
        match result {
            Ok(()) => Ok(staged),
            Err(err) => Err(err.into_worker_error()),
        }
    }

    /// Process one claimed job of the runtime's queue. A notification job runs
    /// the terminal hook. A step job is run through the runner, or its stored
    /// outcome is replayed, and the job is settled with the effects of the
    /// outcome.
    pub(crate) async fn process_step(
        &self,
        job: &JobRecord,
        lease: &LeaseHandle,
    ) -> Result<SettlementEffects, WorkerError> {
        if job.headers.contains_key(HEADER_TERMINAL) {
            return self.process_notification(job).await;
        }

        let claimed = match ClaimedStep::parse(job) {
            Ok(claimed) => claimed,
            Err(err) => {
                warn!(job_id = %job.id, error = %err, "workflow step has malformed headers");
                return Err(worker_error(err));
            }
        };
        let run_id = &claimed.run_id;
        let step_number = claimed.step_number;

        // A cancellation requested before this claim is recorded on the run
        // record, and the step is settled as cancelled without running.
        let record = self
            .core
            .view
            .run_record(run_id)
            .await
            .map_err(worker_error)?
            .ok_or_else(|| worker_error(Error::InconsistentRunState(run_id.clone())))?;
        if let Some(request) = &record.cancel_request {
            let outcome = claimed.cancelled(CancelOrigin::Request, request.reason.clone());
            return self
                .core
                .terminate_recorded(&claimed, outcome, Submission::from(&record), None)
                .await;
        }

        // A cancellation requested during the delivery fires the claim's token
        // (`Queue::cancel`, and a re-claim from the job's persisted
        // `cancel_requested`). The runner receives a child, so a runner firing
        // its own token is not treated as an external cancellation below.
        let claim_cancel = lease.cancel_token().clone();

        let effects_handle = EffectsHandle::for_delivery(&self.core.queue_name, run_id);
        let step = Step {
            delivery: Delivery {
                run_id: run_id.clone(),
                headers: claimed.headers.clone(),
                job_id: job.id.clone(),
                next_job_id: next_step_job_id(&job.id),
                attempts: job.attempts,
                max_attempts: job.max_attempts,
                cancel_token: claim_cancel.child_token(),
                lease: lease.clone(),
                memo: self.core.memo_store.new_memo(run_id, step_number),
                run_memo: self.core.memo_store.new_run_memo(run_id),
                effects: effects_handle.clone(),
                kv: KvReadHandle::for_delivery(self.core.queue.clone(), run_id.clone()),
            },
            step_number,
            payload: job.payload.clone(),
            signal: job.signal.clone(),
        };

        let replayed = if self.core.step_output_replay {
            self.core
                .load_step_output(run_id, step_number, &job.payload)
                .await
                .map_err(worker_error)?
        } else {
            None
        };
        let (outcome, replayed) = match replayed {
            Some((outcome, effects)) => {
                debug!(run_id = %run_id, step_number, "replaying stored step outcome");
                (Ok(outcome), Some(effects))
            }
            None => (self.runner.run_step(&step).await, None),
        };

        // Sealed as soon as the runner returns. An effect staged through a
        // retained handle clone after this point cannot join the settlement, so
        // staging it errors.
        let staged = effects_handle.seal_and_take();
        let replayed_step_output = replayed.is_some();
        let caller_effects = replayed.unwrap_or(staged);

        // A request that lands after this read is recorded on the run record
        // and settles the next step before it runs. A token fired by a request
        // reads the request's reason from the run record.
        let external_cancel = if claim_cancel.is_cancelled() {
            let request = self
                .core
                .view
                .run_record(run_id)
                .await
                .map_err(worker_error)?
                .and_then(|record| record.cancel_request);
            Some(ExternalCancel {
                reason: request.and_then(|request| request.reason),
            })
        } else {
            None
        };
        let cancelled_externally = external_cancel.is_some();

        if self.core.step_output_replay
            && !replayed_step_output
            && !cancelled_externally
            && let Ok(ref outcome) = outcome
        {
            self.core
                .store_step_output(run_id, step_number, &job.payload, outcome, &caller_effects)
                .await
                .map_err(worker_error)?;
        }

        let runner_cancelled = matches!(outcome, Ok(StepOutcome::Cancel { .. }));
        let settled = self
            .core
            .settle_outcome(
                &claimed,
                outcome,
                external_cancel,
                Submission::from(&record),
            )
            .await;

        settled.map(|mut effects| {
            // The external-cancel override commits Cancelled in place of the
            // runner's outcome. The staged effects describe that outcome and
            // are discarded with it. A runner-issued Cancel keeps its effects.
            if runner_cancelled || !cancelled_externally {
                caller_effects.add_to(&mut effects);
            }
            effects
        })
    }
}
