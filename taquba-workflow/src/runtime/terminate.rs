use futures_util::TryStreamExt;
use taquba::{JobStatus, SettlementEffects};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::RuntimeCore;
use crate::durable::{
    self, DurableErrorKind, DurableResult, DurableRun, DurableStepRef, DurableTermination,
    Submission,
};
use crate::effects::TerminalEffects;
use crate::error::Result;
use crate::group::terminated_member;
use crate::keys::{run_kv_key, step_kv_key};
use crate::sweep::run_periodically;
use crate::terminal::{RunTermination, TerminatedRun};
use crate::worker::{ClaimedStep, Ending};

impl RuntimeCore {
    /// The terminal record of the run of `claimed` ending at that step with
    /// `ending` at the clock's current time, with the reference `result` to the
    /// result of a succeeded run and the `submission` of the run record.
    fn termination(
        &self,
        claimed: &ClaimedStep<'_>,
        ending: Ending,
        result: Option<DurableResult>,
        submission: Submission,
    ) -> DurableTermination {
        DurableTermination {
            status: ending.status.into(),
            error: ending.error,
            cancelled_by: ending.cancelled_by.map(Into::into),
            error_kind: ending.error_kind.map(DurableErrorKind::from),
            final_step: DurableStepRef {
                step_number: claimed.step_number,
                job_id: claimed.job.id.clone(),
            },
            terminated_at_ms: self.clock.now_ms(),
            input_hash: submission.input_hash,
            submitted_at_ms: submission.submitted_at_ms,
            headers: claimed.headers.clone(),
            result,
        }
    }

    /// Write `result` as the result object of the run of `claimed` for the
    /// claim's attempt, and return the reference the terminal record stores. An
    /// empty result is not written.
    pub(crate) async fn store_result(
        &self,
        claimed: &ClaimedStep<'_>,
        result: &[u8],
    ) -> Result<DurableResult> {
        if result.is_empty() {
            return Ok(DurableResult::Empty);
        }
        let attempt = claimed.job.attempts;
        self.memo_store
            .put_result(&claimed.run_id, &claimed.job.id, attempt, result)
            .await?;
        Ok(DurableResult::Object { attempt })
    }

    /// Settle a run into its terminal state: return the delete of the
    /// current-step pointer, the write of the terminal record over the run
    /// record, the write of the terminal marker (when memo retention is
    /// enabled), the write of the member record and the decrement of the count
    /// of its group (when the run is a group member) and the effects that the
    /// hook stages through [`TerminalHook::stage_effects`] as
    /// [`SettlementEffects`] for the settlement transaction. `terminal_step` is
    /// the step that produced the outcome, or the pending step a cancellation
    /// removes, and the run ends with `ending`. `result` contains the stored
    /// reference and the bytes of the result of a succeeded run, and
    /// `submission` comes from the run record.
    ///
    /// The effects are pure: this function does not write or mutate state, so a
    /// caller that builds them and then commits a non-terminal outcome does not
    /// leave a trace. A settlement that fails redelivers the step, which
    /// re-terminates and rebuilds the same effects.
    ///
    /// [`TerminalHook::stage_effects`]: crate::TerminalHook::stage_effects
    pub(crate) fn termination_effects(
        &self,
        terminal_step: &ClaimedStep<'_>,
        ending: Ending,
        result: Option<(DurableResult, Vec<u8>)>,
        submission: Submission,
    ) -> SettlementEffects {
        let (reference, result) = result.unzip();
        let termination = self.termination(terminal_step, ending, reference, submission);
        let run_id = &terminal_step.run_id;
        let terminated_at_ms = termination.terminated_at_ms;
        let staging = TerminalEffects::for_delivery(&self.queue_name);
        if let Some(hook) = &self.terminal_hook {
            let outcome = TerminatedRun {
                run_id: run_id.clone(),
                termination: RunTermination::from(&termination),
                result,
                headers: termination.headers.clone(),
            };
            hook.stage_effects(&outcome, &staging);
        }
        let mut effects = staging.seal_into_settlement();
        effects.kv_deletes.push(step_kv_key(run_id));
        effects.kv_writes.insert(
            run_kv_key(run_id),
            durable::encode(&DurableRun::Terminated(termination.clone())),
        );
        if let Some(membership) = &terminal_step.membership {
            effects.kv_writes.insert(
                membership.kv_key(),
                durable::encode(&terminated_member(run_id, termination)),
            );
            effects = effects.count_down(membership.count_name());
        }
        match &self.memo_sweep {
            Some(sweep) => sweep.mark(effects, run_id, terminated_at_ms),
            None => effects,
        }
    }

    /// Terminate every run whose step job the queue dead-lettered outside the
    /// worker path: a lease that expired past the attempt limit, or a claim
    /// dead-lettered by crash recovery when the queue was opened. Such a
    /// settlement does not run workflow code, so the run record and the
    /// current-step pointer remain after it. A dead step job that the run's
    /// current-step pointer refers to identifies the case exactly, because
    /// every worker-path dead-letter deletes the pointer in its own transaction
    /// and a re-submission of the run id refers to a new job. The run
    /// terminates as [`TerminalStatus::Failed`](crate::TerminalStatus::Failed)
    /// with the queue record's last error, through the same effects as a
    /// worker-path termination committed as one transaction without a
    /// transition of their own. The runner's failure writes cannot apply,
    /// because no runner returned. Returns the number of runs terminated.
    pub(crate) async fn reconcile_dead_steps(&self) -> Result<usize> {
        const PAGE: usize = 256;
        let mut terminated = 0usize;
        let mut dead = std::pin::pin!(self.queue.view().jobs(
            &self.queue_name,
            JobStatus::Dead,
            PAGE
        ));
        while let Some(job) = dead.try_next().await? {
            let Ok(claimed) = ClaimedStep::parse(&job) else {
                continue;
            };
            let run_id = &claimed.run_id;
            let current = self.view.current_step_if_active(run_id).await?;
            if current.is_none_or(|current| current.job_id != job.id) {
                continue;
            }
            // The run record is written with the pointer and replaced by the
            // terminal record when the pointer is deleted. Only another writer
            // of the store leaves a pointer without a record, and the worker
            // reports that pointer.
            let Some(record) = self.view.run_record(run_id).await? else {
                warn!(run_id = %run_id, job_id = %job.id, "dead step has a current-step pointer but no run record");
                continue;
            };
            let error = job
                .last_error
                .clone()
                .unwrap_or_else(|| "step dead-lettered outside the worker".to_string());
            let ending = Ending::failed(error, None);
            let effects =
                self.termination_effects(&claimed, ending, None, Submission::from(&record));
            self.queue.commit_effects(effects).await?;
            warn!(run_id = %run_id, step_number = claimed.step_number, job_id = %job.id, "terminated a run whose step was dead-lettered outside the worker");
            terminated += 1;
        }
        Ok(terminated)
    }

    /// The reconciliation loop: a pass when the worker starts, then a pass
    /// whenever the queue's dead count differs from its value at the last
    /// successful pass, checked every poll interval, until `stop` is cancelled.
    pub(super) async fn run_dead_step_reconciliation(&self, stop: CancellationToken) {
        run_periodically(
            self.poll_interval,
            &stop,
            None,
            |reconciled_at: Option<i64>| async move {
                match self.queue.view().stats(&self.queue_name).await {
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
