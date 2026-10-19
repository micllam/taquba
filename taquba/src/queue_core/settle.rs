//! The settlement transitions of [`QueueCore`]: the end of a claim and the
//! revival of a dead job.

use tracing::debug;

use crate::effects::SettlementEffects;
use crate::error::{Error, Result};
use crate::history::{AttemptOutcome, JobAttempt, append_attempt};
use crate::job::{Claim, JobRecord, JobStatus};
use crate::queue::EnqueueResult;
use crate::queue_core::QueueCore;
use crate::txn::{
    Attempt, ClaimEnd, Durability, retry, stage_claim_end, stage_to_pending, take_claim,
};

/// The committed outcome of [`QueueCore::settle_claim`]'s transaction.
struct SettledClaim<'e> {
    /// The record as written.
    job: JobRecord,
    end: ClaimEnd<'e>,
    /// The pending key when the job returned to pending.
    pending_key: Option<Vec<u8>>,
    /// The effects' results, or `None` when the transition discarded them.
    results: Option<Vec<EnqueueResult>>,
}

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

        let settled: Result<SettledClaim<'e>> = {
            let (prepared, end_for) = (&prepared, &end_for);
            retry(&self.db, Durability::Awaited, |txn| async move {
                // The returned record is the base for the written record. The
                // claim's copy predates a cancel committed during the delivery.
                let mut job = take_claim(&txn, &self.lease_registry, queue, id, claim_id).await?;
                let now = self.now_ms();
                let end = end_for(&job, now);
                let pending_key = stage_claim_end(&txn, &mut job, &end, now)?;
                let staged = if end.is_terminal() {
                    Some(self.stage_effects(&txn, prepared).await?)
                } else {
                    None
                };
                Ok(Attempt::Commit(txn, (job, end, pending_key, staged)))
            })
            .await
        }
        .map(|(job, end, pending_key, staged)| SettledClaim {
            job,
            end,
            pending_key,
            results: staged.map(|s| self.note_staged_effects(s)),
        });

        self.finish_effects(
            prepared,
            settled.as_ref().ok().and_then(|s| s.results.as_deref()),
        )
        .await;
        let settled = settled?;
        self.finish_claim_end(
            &settled.job,
            &settled.end,
            claim_id,
            settled.pending_key.as_deref(),
            Some(claim),
        )
        .await;
        Ok((settled.job, settled.results))
    }

    /// Move the dead-letter job `id` back to pending with a reset attempt
    /// count, as [`Queue::requeue_dead_job`](crate::Queue::requeue_dead_job)
    /// documents.
    pub(crate) async fn requeue_dead_job(&self, id: &str) -> Result<()> {
        let (job, pending): (JobRecord, Vec<u8>) = self
            .transition_by_id(
                id,
                Durability::Awaited,
                || Err(Error::JobNotFound(id.to_string())),
                |txn, current_key, mut job| async move {
                    if job.status != JobStatus::Dead {
                        txn.rollback();
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
                    let pending = stage_to_pending(&txn, &mut job, JobStatus::Dead)?;
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
                    Ok(Attempt::Commit(txn, (job, pending)))
                },
            )
            .await?;
        self.claim_cursor.note_pending_insert(&job.queue, &pending);
        debug!(queue = %job.queue, job_id = %job.id, "dead job re-queued");
        Ok(())
    }
}
