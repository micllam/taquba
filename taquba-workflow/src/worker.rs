//! The worker path: how a claimed job is identified, run and settled.
//! [`StepWorker`] is the runtime's [`Worker`]. A step job is parsed into a
//! [`ClaimedStep`], run through the [`StepRunner`] and settled into the
//! [`SettlementEffects`] of its outcome.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use taquba::{FailWith, JobRecord, LeaseHandle, SettlementEffects, Worker, WorkerError};
use tracing::{debug, warn};

use crate::durable::{self, DurableStepOutcome, DurableStepOutcomeRecord, Submission};
use crate::effects::{EffectsHandle, StagedEffects};
use crate::error::{self, Error, worker_error};
use crate::group::Membership;
use crate::keys::{HEADER_RUN_ID, HEADER_STEP, RunId, next_step_job_id, submitter_headers};
use crate::kv::KvReadHandle;
use crate::owner::{HEADER_OWNED, HEADER_OWNER, OwnedQueues, Owner, OwnerRef, OwnerStep};
use crate::runner::{Delivery, Step, StepError, StepErrorKind, StepOutcome, StepRunner, Trigger};
use crate::runtime::{RuntimeCore, RuntimeInner, StepEnqueueOpts};
use crate::terminal::{CancelOrigin, TerminalStatus};

/// The [`Worker`] of a runtime: every claimed job of the runtime's queue is
/// processed by [`RuntimeInner::process_step`].
pub(crate) struct StepWorker<R> {
    pub(crate) inner: Arc<RuntimeInner<R>>,
}

impl<R: StepRunner + 'static> Worker for StepWorker<R> {
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
/// is in a group, its owner when the run is owned, the submitter's headers with
/// the reserved ones removed and the queue record itself.
pub(crate) struct ClaimedStep<'a> {
    pub(crate) run_id: RunId,
    pub(crate) step_number: u32,
    pub(crate) membership: Option<Membership>,
    pub(crate) owner: Option<OwnerRef>,
    /// Submitter-supplied headers, without the reserved `workflow.` keys.
    pub(crate) headers: HashMap<String, String>,
    pub(crate) job: &'a JobRecord,
}

impl<'a> ClaimedStep<'a> {
    /// Identify the claimed step from `job`'s headers. Fails, permanently, for
    /// a job without the run id header, with a run id, group id or owner that
    /// is not valid or with a step header that is not a `u32`.
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
            owner: job
                .headers
                .get(HEADER_OWNER)
                .map(|value| OwnerRef::from_header(value))
                .transpose()?,
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
            wait: None,
            reserved_headers: self.reserved_headers_with_none(),
        }
    }

    fn reserved_headers_with_none(&self) -> Vec<(&'static str, String)> {
        let mut headers = self
            .membership
            .as_ref()
            .map(Membership::reserved_headers)
            .unwrap_or_default();
        if let Some(owner) = &self.owner {
            headers.push((HEADER_OWNER, owner.header_value()));
        }
        headers
    }
}

/// How a run ends at a step: its status, the error with its kind and the origin
/// of a cancellation. The runtime adds the time, the submission and the result
/// to form the terminal record.
pub(crate) struct Ending {
    pub(crate) status: TerminalStatus,
    pub(crate) error: Option<String>,
    pub(crate) cancelled_by: Option<CancelOrigin>,
    pub(crate) error_kind: Option<StepErrorKind>,
}

impl Ending {
    /// A `Succeeded` ending.
    pub(crate) fn succeeded() -> Self {
        Self {
            status: TerminalStatus::Succeeded,
            error: None,
            cancelled_by: None,
            error_kind: None,
        }
    }

    /// A `Failed` ending with `error`, classified as `error_kind`.
    pub(crate) fn failed(error: String, error_kind: Option<StepErrorKind>) -> Self {
        Self {
            status: TerminalStatus::Failed,
            error: Some(error),
            cancelled_by: None,
            error_kind,
        }
    }

    /// A `Cancelled` ending by `origin` for `reason`.
    pub(crate) fn cancelled(origin: CancelOrigin, reason: Option<String>) -> Self {
        Self {
            status: TerminalStatus::Cancelled,
            error: reason,
            cancelled_by: Some(origin),
            error_kind: None,
        }
    }
}

impl RuntimeCore {
    /// The error that settles a failed step: a permanent error, or a transient
    /// one on the last attempt, dead-letters the step and terminates the run,
    /// and another transient error retries the step.
    async fn step_failure(
        &self,
        claimed: &ClaimedStep<'_>,
        err: StepError,
        submission: Submission,
    ) -> WorkerError {
        if err.kind == StepErrorKind::Permanent || claimed.job.is_last_attempt() {
            match self.owned_queues(submission.owner(&claimed.run_id)).await {
                Ok(owned) => self.terminating_failure(claimed, err, submission, &owned),
                Err(read) => worker_error(read),
            }
        } else {
            err.into_worker_error()
        }
    }

    /// The error a step returns for a failure that terminates its run: the
    /// effects of the `Failed` termination on a [`FailWith`], so the core
    /// applies them only with the dead-lettering settlement.
    fn terminating_failure(
        &self,
        claimed: &ClaimedStep<'_>,
        error: StepError,
        submission: Submission,
        owned: &OwnedQueues,
    ) -> WorkerError {
        let ending = Ending::failed(error.message.clone(), Some(error.kind));
        let effects = self.termination_effects(claimed, ending, None, submission, owned);
        FailWith::new(error.into_worker_error(), effects).into()
    }

    /// Terminate the run of `claimed` with `ending` from an acking settlement.
    /// The result of a succeeded run is written before the settlement effects
    /// are built, and a failed write is a transient error, so the step is
    /// delivered again.
    async fn terminate_recorded(
        &self,
        claimed: &ClaimedStep<'_>,
        ending: Ending,
        result: Option<Vec<u8>>,
        submission: Submission,
    ) -> Result<SettlementEffects, WorkerError> {
        let result = match result {
            Some(result) => {
                let reference = self
                    .store_result(claimed, &result)
                    .await
                    .map_err(worker_error)?;
                Some((reference, result))
            }
            None => None,
        };
        let owned = self
            .owned_queues(submission.owner(&claimed.run_id))
            .await
            .map_err(worker_error)?;
        Ok(self.termination_effects(claimed, ending, result, submission, &owned))
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
            (_, Some(ExternalCancel { reason, origin })) => {
                let ending = Ending::cancelled(origin, reason);
                self.terminate_recorded(claimed, ending, None, submission)
                    .await
            }
            (Ok(StepOutcome::Cancel { reason }), None) => {
                let ending = Ending::cancelled(CancelOrigin::Runner, Some(reason));
                self.terminate_recorded(claimed, ending, None, submission)
                    .await
            }
            (Ok(StepOutcome::Continue { payload, when }), None) => match when {
                Trigger::Immediate => Ok(self.advance(claimed, payload, claimed.next_step_opts())),
                Trigger::After(delay) => {
                    let opts = StepEnqueueOpts {
                        run_at: Some(self.run_at_after(delay)),
                        ..claimed.next_step_opts()
                    };
                    Ok(self.advance(claimed, payload, opts))
                }
                Trigger::OnSignal { timeout } => {
                    Ok(self.advance_on_signal(claimed, payload, timeout))
                }
                Trigger::OnGroup { group_id, timeout } => {
                    match self
                        .advance_on_group(claimed, payload, &group_id, timeout)
                        .await
                    {
                        Ok(effects) => Ok(effects),
                        Err(err) => Err(self.step_failure(claimed, err.into(), submission).await),
                    }
                }
            },
            (Ok(StepOutcome::Succeed { result }), None) => {
                self.terminate_recorded(claimed, Ending::succeeded(), Some(result), submission)
                    .await
            }
            // A runner decision: the step ran cleanly and is acknowledged, and
            // the run terminates as `Failed` without a dead-letter.
            (Ok(StepOutcome::Fail { reason }), None) => {
                let ending = Ending::failed(reason, Some(StepErrorKind::Permanent));
                self.terminate_recorded(claimed, ending, None, submission)
                    .await
            }
            (Err(err), None) => Err(self.step_failure(claimed, err, submission).await),
        }
    }
}

/// An external cancellation that a settlement observed, with the reason of the
/// request on the run record.
pub(crate) struct ExternalCancel {
    pub(crate) reason: Option<String>,
    pub(crate) origin: CancelOrigin,
}

impl<R: StepRunner> RuntimeInner<R> {
    /// Process one claimed step job of the runtime's queue. The step runs
    /// through the runner, or its stored outcome is replayed, and the job is
    /// settled with the effects of the outcome.
    pub(crate) async fn process_step(
        &self,
        job: &JobRecord,
        lease: &LeaseHandle,
    ) -> Result<SettlementEffects, WorkerError> {
        if let Some(owner) = job.headers.get(HEADER_OWNED) {
            return self.core.cancel_owned(job, owner).await;
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
            let ending = Ending::cancelled(request.origin.into(), request.reason.clone());
            return self
                .core
                .terminate_recorded(&claimed, ending, None, Submission::from(&record))
                .await;
        }

        // A cancellation requested during the delivery fires the claim's token
        // (`Queue::cancel`, and a re-claim from the job's persisted
        // `cancel_requested`). The runner receives a child, so a runner firing
        // its own token is not treated as an external cancellation below.
        let claim_cancel = lease.cancel_token().clone();

        let effects_handle = EffectsHandle::for_delivery(&self.core.queue_name, run_id);
        let owner_step = lease.fence().map(OwnerStep::new);
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
                owner: Owner {
                    run: Submission::from(&record).owner(run_id),
                    queue: Some(self.core.queue.clone()),
                    step: owner_step.clone(),
                },
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
        // Every owned submission of the step has committed or fails from here,
        // so a termination below reads all of the owner's queues.
        if let Some(owner_step) = &owner_step {
            owner_step.seal().await;
        }
        let replayed_step_output = replayed.is_some();
        let caller_effects = replayed.unwrap_or(staged);

        // A request that lands after this read is recorded on the run record
        // and settles the next step before it runs. A token fired by a request
        // reads the request's reason and origin from the run record.
        let external_cancel = if claim_cancel.is_cancelled() {
            let request = self
                .core
                .view
                .run_record(run_id)
                .await
                .map_err(worker_error)?
                .and_then(|record| record.cancel_request);
            Some(match request {
                Some(request) => ExternalCancel {
                    reason: request.reason,
                    origin: request.origin.into(),
                },
                None => ExternalCancel {
                    reason: None,
                    origin: CancelOrigin::Request,
                },
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

/// The portion of `delay` that remains after `now_ms`, measured from
/// `stored_at_ms`. Saturates to the full delay if the clock reads earlier than
/// the stored timestamp.
fn remaining_delay(stored_at_ms: u64, now_ms: u64, delay: Duration) -> Duration {
    let elapsed = Duration::from_millis(now_ms.saturating_sub(stored_at_ms));
    delay.saturating_sub(elapsed)
}

impl RuntimeCore {
    pub(crate) async fn load_step_output(
        &self,
        run_id: &RunId,
        step_number: u32,
        step_payload: &[u8],
    ) -> error::Result<Option<(StepOutcome, StagedEffects)>> {
        let Some(bytes) = self
            .memo_store
            .get_step_output(run_id, step_number, step_payload)
            .await?
        else {
            return Ok(None);
        };
        let Some(record) = durable::decode_or_absent::<DurableStepOutcomeRecord>(
            &bytes,
            "step-output replay record",
            &format_args!("{run_id}/{step_number}"),
        ) else {
            return Ok(None);
        };
        let mut outcome = StepOutcome::from(record.outcome);
        match &mut outcome {
            StepOutcome::Continue {
                when: Trigger::After(delay),
                ..
            } => {
                *delay = remaining_delay(record.stored_at_ms, self.clock.now_ms(), *delay);
            }
            StepOutcome::Continue {
                when: Trigger::OnSignal { timeout } | Trigger::OnGroup { timeout, .. },
                ..
            } => {
                *timeout = remaining_delay(record.stored_at_ms, self.clock.now_ms(), *timeout);
            }
            _ => {}
        }
        Ok(Some((outcome, record.effects)))
    }

    pub(crate) async fn store_step_output(
        &self,
        run_id: &RunId,
        step_number: u32,
        step_payload: &[u8],
        outcome: &StepOutcome,
        effects: &StagedEffects,
    ) -> error::Result<()> {
        let record = DurableStepOutcomeRecord {
            stored_at_ms: self.clock.now_ms(),
            outcome: DurableStepOutcome::from(outcome),
            effects: effects.clone(),
        };
        self.memo_store
            .put_step_output(run_id, step_number, step_payload, &durable::encode(&record))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remaining_delay_measures_from_stored_timestamp() {
        let delay = Duration::from_secs(10);
        assert_eq!(remaining_delay(1_000, 4_000, delay), Duration::from_secs(7));
        assert_eq!(remaining_delay(1_000, 20_000, delay), Duration::ZERO);
        assert_eq!(remaining_delay(5_000, 1_000, delay), delay);
    }
}
