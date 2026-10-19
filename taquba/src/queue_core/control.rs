//! The control transitions of [`QueueCore`]: the cancel of a job and the early
//! wake of a scheduled job.

use tracing::debug;

use crate::effects::{PreparedEffects, SettlementEffects};
use crate::error::Result;
use crate::job::{JobRecord, JobStatus};
use crate::queue::{CancelOutcome, EnqueueResult, WaitOutcome, WakeOutcome};
use crate::queue_core::QueueCore;
use crate::txn::{Attempt, Durability, stage_remove, stage_to_pending};

impl QueueCore {
    /// Cancel the job `id` and apply `effects` with its removal, as
    /// [`Queue::cancel_with`](crate::Queue::cancel_with) documents.
    pub(crate) async fn cancel(
        &self,
        id: &str,
        effects: SettlementEffects,
    ) -> Result<(CancelOutcome, Vec<EnqueueResult>)> {
        let prepared = self.prepare_effects(effects).await?;
        let outcome = self.cancel_txn(id, &prepared).await;

        self.finish_effects(
            prepared,
            match &outcome {
                Ok((CancelOutcome::Removed, results)) => Some(results.as_slice()),
                _ => None,
            },
        )
        .await;
        outcome
    }

    /// The transaction loop of [`Self::cancel`]: resolve the job, apply the
    /// transition its state allows and commit, including the post-commit
    /// notifications of the committed outcome.
    async fn cancel_txn(
        &self,
        id: &str,
        prepared: &PreparedEffects,
    ) -> Result<(CancelOutcome, Vec<EnqueueResult>)> {
        // `Err` is an outcome reached without a commit.
        let committed = self
            .transition_by_id(
                id,
                Durability::Awaited,
                || Ok(Err(CancelOutcome::NotFound)),
                |txn, current_key, mut job| async move {
                    let (msg, outcome, staged) = match job.status {
                        JobStatus::Pending | JobStatus::Scheduled => {
                            stage_remove(&txn, &current_key, &job)?;
                            let staged = self.stage_effects(&txn, prepared).await?;
                            (
                                "pending/scheduled job cancelled",
                                CancelOutcome::Removed,
                                Some(staged),
                            )
                        }
                        JobStatus::Claimed => {
                            if job.cancel_requested {
                                // The flag is already persisted. The token is
                                // fired again, because a re-claim after the
                                // first request has a fresh token.
                                txn.rollback();
                                self.lease_registry.cancel(&job.queue, id);
                                debug!(job_id = %id, "cancel re-requested on claimed job");
                                return Ok(Attempt::Abort(Err(CancelOutcome::Requested)));
                            }
                            job.cancel_requested = true;
                            let value = job.stored_bytes()?;
                            txn.put(&current_key, &value)?;
                            (
                                "claimed job cancellation requested",
                                CancelOutcome::Requested,
                                None,
                            )
                        }
                        JobStatus::Done | JobStatus::Dead => {
                            txn.rollback();
                            return Ok(Attempt::Abort(Err(CancelOutcome::NotFound)));
                        }
                    };
                    Ok(Attempt::Commit(txn, Ok((job, msg, outcome, staged))))
                },
            )
            .await?;
        let (job, msg, outcome, staged) = match committed {
            Ok(committed) => committed,
            Err(outcome) => return Ok((outcome, Vec::new())),
        };
        // Fired on the Removed path as well: the worker of a claim the reaper
        // requeued just before this call can still observe the token. That
        // claim's end removes the entry.
        self.lease_registry.cancel(&job.queue, id);
        let results = staged
            .map(|s| self.note_staged_effects(s))
            .unwrap_or_default();
        // Removed = terminal (job is gone). Requested = not yet terminal, and
        // the worker's settlement delivers the outcome when it acks / nacks /
        // dead-letters.
        if matches!(outcome, CancelOutcome::Removed) {
            // The record is deleted, so its payload object (if any) is removed
            // here, after the commit.
            self.payload_store.delete_for(&job).await;
            self.completion_waiters
                .settle(id, || WaitOutcome::Cancelled);
        }
        debug!(job_id = %id, "{msg}");
        Ok((outcome, results))
    }

    /// Move the scheduled job `id` to pending before its `run_at`, as
    /// [`Queue::wake_scheduled`](crate::Queue::wake_scheduled) documents.
    pub(crate) async fn wake_scheduled(
        &self,
        id: &str,
        wake_payload: Option<Vec<u8>>,
    ) -> Result<WakeOutcome> {
        // `Err` is an outcome reached without a commit.
        let wake_payload = &wake_payload;
        let woken: std::result::Result<(JobRecord, Vec<u8>), WakeOutcome> = self
            .transition_by_id(
                id,
                Durability::Awaited,
                || Ok(Err(WakeOutcome::NotFound)),
                |txn, current_key, mut job| async move {
                    if job.status != JobStatus::Scheduled {
                        txn.rollback();
                        return Ok(Attempt::Abort(Err(WakeOutcome::NotScheduled)));
                    }
                    txn.delete(&current_key)?;
                    job.woken_at = Some(self.now_ms());
                    job.wake_payload = wake_payload.clone();
                    let pending = stage_to_pending(&txn, &mut job, JobStatus::Scheduled)?;
                    Ok(Attempt::Commit(txn, Ok((job, pending))))
                },
            )
            .await?;
        match woken {
            Ok((job, pending)) => {
                self.claim_cursor.note_pending_insert(&job.queue, &pending);
                debug!(job_id = %id, queue = %job.queue, "scheduled job woken");
                Ok(WakeOutcome::Woken)
            }
            Err(outcome) => Ok(outcome),
        }
    }
}
