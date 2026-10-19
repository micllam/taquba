//! The settlement transitions of [`QueueCore`]: the end of a claim and the
//! revival of a dead job.

use tracing::debug;

use crate::effects::SettlementEffects;
use crate::error::{Error, Result};
use crate::history::{AttemptOutcome, JobAttempt, append_attempt};
use crate::job::{Claim, JobRecord, JobStatus};
use crate::queue::EnqueueResult;
use crate::queue_core::QueueCore;
use crate::txn::{Attempt, ClaimEnd, Durability, stage_claim_end, stage_to_pending, take_claim};

impl QueueCore {
    /// The settlement of a claim. The effects are prepared once before the
    /// retry loop. Every iteration fences the claim, chooses the transition
    /// from the stored record and the current time with `end_for`, stages it
    /// and, when the transition is terminal, stages the effects. Returns the
    /// written record and the effects' results, `None` when the transition
    /// discarded them.
    pub(crate) async fn settle_claim<'e>(
        &self,
        claim: &Claim,
        effects: SettlementEffects,
        end_for: impl Fn(&JobRecord, u64) -> ClaimEnd<'e>,
    ) -> Result<(JobRecord, Option<Vec<EnqueueResult>>)> {
        let prepared = self.prepare_effects(effects).await?;
        let claim_id = claim.claim_id();
        let (queue, id) = (&claim.queue, claim.id.as_str());

        let mut settled = {
            let (prepared, end_for) = (&prepared, &end_for);
            self.retry(Durability::Awaited, |mut txn| async move {
                // The returned record is the base for the written record. The
                // claim's copy predates a cancel committed during the delivery.
                let mut job = take_claim(&txn, &self.lease_registry, queue, id, claim_id).await?;
                let now = self.now_ms();
                let end = end_for(&job, now);
                stage_claim_end(&mut txn, &mut job, &end, now)?;
                let staged = if end.is_terminal() {
                    Some(self.stage_effects(&mut txn, prepared).await?)
                } else {
                    None
                };
                Ok((txn, Attempt::Commit((job, end, staged))))
            })
            .await
        };

        let staged = settled.as_mut().ok().and_then(|(.., staged)| staged.take());
        let results = self.finish_effects(prepared, staged).await;
        let (job, end, _) = settled?;
        self.finish_claim_end(&job, &end, claim_id, Some(claim))
            .await;
        Ok((job, results))
    }

    /// Move the dead-letter job `id` back to pending with a reset attempt
    /// count, as [`Queue::requeue_dead_job`](crate::Queue::requeue_dead_job)
    /// documents.
    pub(crate) async fn requeue_dead_job(&self, id: &str) -> Result<()> {
        let job: JobRecord = self
            .transition_by_id(
                id,
                Durability::Awaited,
                || Err(Error::JobNotFound(id.to_string())),
                |mut txn, current_key, mut job| async move {
                    if job.status != JobStatus::Dead {
                        return Err(Error::InvalidState);
                    }
                    txn.delete(&current_key)?;
                    job.attempts = 0;
                    job.last_error = None;
                    job.claimed_at = None;
                    job.failed_at = None;
                    // Revival clears any prior cancel request: the operator
                    // chose to start this job afresh.
                    job.cancel_requested = false;
                    stage_to_pending(&mut txn, &mut job, JobStatus::Dead)?;
                    // The history is kept across the revival. The marker
                    // separates entries recorded before it from the reset
                    // attempt counter.
                    append_attempt(
                        &txn,
                        &job.id,
                        &JobAttempt {
                            attempt: 0,
                            claimed_at: None,
                            recorded_at: self.now_ms(),
                            outcome: AttemptOutcome::Requeued,
                            error: None,
                        },
                    )?;
                    Ok((txn, Attempt::Commit(job)))
                },
            )
            .await?;
        debug!(queue = %job.queue, job_id = %job.id, "dead job re-queued");
        Ok(())
    }
}
