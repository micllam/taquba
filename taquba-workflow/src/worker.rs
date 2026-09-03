//! The worker path: how a claimed job is identified, run and settled.
//! [`StepWorker`] is the [`Worker`] the runtime drives; a step job is
//! parsed into a [`ClaimedStep`], run through the [`StepRunner`] and
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
use crate::group::Membership;
use crate::keys::{HEADER_RUN_ID, HEADER_STEP, HEADER_TERMINAL, RESERVED_HEADER_PREFIX};
use crate::kv::KvReadHandle;
use crate::runner::{Delivery, Step, StepError, StepErrorKind, StepOutcome, StepRunner, Trigger};
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
/// step named by the job's reserved headers, the run's group membership
/// when it has one, the submitter's headers with the reserved ones
/// removed and the queue record itself.
pub(crate) struct ClaimedStep<'a> {
    pub(crate) run_id: String,
    pub(crate) step_number: u32,
    pub(crate) membership: Option<Membership>,
    /// Submitter-supplied headers, without the reserved `workflow.` keys.
    pub(crate) headers: HashMap<String, String>,
    pub(crate) job: &'a JobRecord,
}

impl<'a> ClaimedStep<'a> {
    /// Identify the claimed step from `job`'s headers. Fails, permanently,
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
            membership: Membership::from_headers(&job.headers),
            headers,
            job,
        })
    }

    /// The enqueue options of the run's next step: this step's priority,
    /// attempt limit and group membership, so a run's per-step settings
    /// and its membership hold across the step boundary.
    pub(crate) fn next_step_opts(&self) -> StepEnqueueOpts {
        StepEnqueueOpts {
            run_at: None,
            priority: Some(self.job.priority),
            max_attempts: Some(self.job.max_attempts),
            reserved_headers: self.reserved_headers_with_none(),
        }
    }

    /// The reserved headers of the next step: the membership's, plus
    /// `header`.
    pub(crate) fn reserved_headers_with(
        &self,
        header: (&'static str, String),
    ) -> Vec<(&'static str, String)> {
        let mut headers = self.reserved_headers_with_none();
        headers.push(header);
        headers
    }

    fn reserved_headers_with_none(&self) -> Vec<(&'static str, String)> {
        self.membership
            .as_ref()
            .map(Membership::reserved_headers)
            .unwrap_or_default()
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

impl<R: StepRunner, H: TerminalHook> RuntimeInner<R, H> {
    /// The error a step returns for a failure that terminates its run:
    /// the effects of the `Failed` termination, with the step's `note`,
    /// on a [`FailWith`] so the core applies them only with the
    /// dead-lettering settlement.
    pub(crate) fn terminating_failure(
        &self,
        claimed: &ClaimedStep<'_>,
        error: StepError,
        note: Option<Vec<u8>>,
    ) -> WorkerError {
        let effects = self.terminate_collecting_effects(
            &claimed.failed(error.message.clone()),
            claimed,
            note,
        );
        FailWith::new(error.into_worker_error(), effects).into()
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
            Err(err) => Err(err.into_worker_error()),
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

        let claimed = match ClaimedStep::parse(job) {
            Ok(claimed) => claimed,
            Err(err) => {
                warn!(job_id = %job.id, error = %err, "workflow step has malformed headers");
                return Err(StepError::from(err).into_worker_error());
            }
        };
        let run_id = claimed.run_id.as_str();
        let step_number = claimed.step_number;

        let (step_signal, signal_kv_deletes) = self
            .core
            .resolve_step_signal(job, run_id, step_number)
            .await
            .map_err(|err| StepError::from(err).into_worker_error())?;

        // A cancellation requested before this claim is recorded on the
        // run record; the step is settled as cancelled without running.
        let record = self
            .core
            .run_record(run_id)
            .await
            .map_err(|err| StepError::from(err).into_worker_error())?
            .ok_or_else(|| {
                StepError::from(Error::InconsistentRunState(run_id.to_string())).into_worker_error()
            })?;
        if record.cancel_requested {
            let mut effects =
                self.terminate_collecting_effects(&claimed.cancelled(None), &claimed, None);
            effects.kv_deletes.extend(signal_kv_deletes);
            return Ok(effects);
        }

        // A cancellation requested during the delivery fires the claim's
        // token (`Queue::cancel`, and a re-claim from the job's persisted
        // `cancel_requested`). The runner receives a child, so a runner
        // firing its own token is not treated as an external
        // cancellation below.
        let claim_cancel = lease.cancel_token().clone();

        let effects_handle = EffectsHandle::for_delivery();
        let step = Step {
            delivery: Delivery {
                run_id: run_id.to_string(),
                headers: claimed.headers.clone(),
                job_id: job.id.clone(),
                attempts: job.attempts,
                max_attempts: job.max_attempts,
                cancel_token: claim_cancel.child_token(),
                lease: lease.clone(),
                memo: self.core.memo_store.new_memo(run_id, step_number),
                run_memo: self.core.memo_store.new_run_memo(run_id),
                effects: effects_handle.clone(),
                kv: KvReadHandle::for_delivery(self.core.queue.clone()),
            },
            step_number,
            payload: job.payload.clone(),
            signal: step_signal,
        };

        let replayed = if self.core.step_output_replay {
            self.core
                .load_step_output(run_id, step_number, &job.payload)
                .await
                .map_err(|err| StepError::from(err).into_worker_error())?
        } else {
            None
        };
        let (outcome, replayed) = match replayed {
            Some((outcome, effects, note)) => {
                debug!(run_id = %run_id, step_number, "replaying stored step outcome");
                (Ok(outcome), Some((effects, note)))
            }
            None => (self.runner.run_step(&step).await, None),
        };

        // Sealed as soon as the runner has returned: an effect staged
        // through a retained handle clone after this point could not
        // join the settlement, so staging it errors.
        let sealed = effects_handle.seal_and_take();
        let replayed_step_output = replayed.is_some();
        let (caller_effects, note) = replayed.unwrap_or((sealed.outcome, sealed.note));

        // A request that lands after this read is recorded on the run
        // record and settles the next step before it runs.
        let external_cancel = claim_cancel.is_cancelled();

        if self.core.step_output_replay
            && !replayed_step_output
            && !external_cancel
            && let Ok(ref outcome) = outcome
        {
            self.core
                .store_step_output(
                    run_id,
                    step_number,
                    &job.payload,
                    outcome,
                    &caller_effects,
                    note.clone(),
                )
                .await
                .map_err(|err| StepError::from(err).into_worker_error())?;
        }

        let runner_cancelled = matches!(outcome, Ok(StepOutcome::Cancel { .. }));
        let settled = self
            .settle_outcome(&claimed, outcome, external_cancel, note)
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

    /// The settlement of `outcome` for `claimed`: the effects of the
    /// run's transition on an acknowledging outcome, or the error that
    /// retries or dead-letters the step. `note` is the step's staged
    /// note, recorded on the run's member record by a terminating
    /// settlement that commits the runner's outcome.
    ///
    /// Cancellation precedence: a runner-issued [`StepOutcome::Cancel`]
    /// wins and carries its reason on [`RunOutcome::error`]; otherwise
    /// an external cancellation overrides whatever the runner returned,
    /// including a transient retry and a permanent failure, with
    /// `error: None` so consumers can distinguish the two.
    async fn settle_outcome(
        &self,
        claimed: &ClaimedStep<'_>,
        outcome: std::result::Result<StepOutcome, StepError>,
        external_cancel: bool,
        note: Option<Vec<u8>>,
    ) -> std::result::Result<SettlementEffects, WorkerError> {
        match outcome {
            Ok(StepOutcome::Cancel { reason }) => Ok(self.terminate_collecting_effects(
                &claimed.cancelled(Some(reason)),
                claimed,
                note,
            )),
            _ if external_cancel => {
                Ok(self.terminate_collecting_effects(&claimed.cancelled(None), claimed, None))
            }
            Ok(StepOutcome::Continue { payload, when }) => match when {
                Trigger::Immediate => Ok(self
                    .core
                    .advance(claimed, payload, claimed.next_step_opts())
                    .await),
                Trigger::After(delay) => {
                    let opts = StepEnqueueOpts {
                        run_at: Some(self.core.run_at_after(delay)),
                        ..claimed.next_step_opts()
                    };
                    Ok(self.core.advance(claimed, payload, opts).await)
                }
                Trigger::OnSignal {
                    correlation_key,
                    timeout,
                } => {
                    self.advance_on_signal(claimed, payload, &correlation_key, timeout)
                        .await
                }
            },
            Ok(StepOutcome::Succeed { result }) => {
                Ok(self.terminate_collecting_effects(&claimed.succeeded(result), claimed, note))
            }
            // A runner verdict: the step ran cleanly and is acknowledged;
            // the run terminates as `Failed` without a dead-letter.
            Ok(StepOutcome::Fail { reason }) => {
                Ok(self.terminate_collecting_effects(&claimed.failed(reason), claimed, note))
            }
            // A permanent error, or a transient one on the last attempt,
            // dead-letters the step and terminates the run; the note is
            // recorded only with that settlement.
            Err(err) if err.kind == StepErrorKind::Permanent || claimed.job.is_last_attempt() => {
                Err(self.terminating_failure(claimed, err, note))
            }
            Err(err) => Err(err.into_worker_error()),
        }
    }
}
