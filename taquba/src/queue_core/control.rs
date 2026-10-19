//! The control transitions of [`QueueCore`]: the cancel of a job and the early
//! wake of a scheduled job.

use tracing::debug;

use crate::effects::{PreparedEffects, SettlementEffects, StagedEffects};
use crate::error::Result;
use crate::job::{JobRecord, JobStatus};
use crate::queue::{CancelOutcome, EnqueueResult, WaitOutcome, WakeOutcome};
use crate::queue_core::QueueCore;
use crate::txn::{Attempt, Durability, stage_remove, stage_to_pending};

/// What the cancel transaction did.
enum Cancelled {
    /// The pending or scheduled job was removed, and the effects were staged.
    Removed {
        job: JobRecord,
        staged: StagedEffects,
    },
    /// The cancellation of the claimed job was recorded on its record.
    Requested { job: JobRecord },
    /// The transaction did not commit.
    Uncommitted(CancelOutcome),
}

impl QueueCore {
    /// Cancel the job `id` and apply `effects` with its removal, as
    /// [`Queue::cancel_with`](crate::Queue::cancel_with) documents.
    pub(crate) async fn cancel(
        &self,
        id: &str,
        effects: SettlementEffects,
    ) -> Result<(CancelOutcome, Vec<EnqueueResult>)> {
        let prepared = self.prepare_effects(effects).await?;
        match self.cancel_txn(id, &prepared).await {
            Ok(Cancelled::Removed { job, staged }) => {
                // The effects are recorded before the waiters of the job are
                // woken.
                let results = self.finish_effects(prepared, Some(staged)).await;
                // Fired on the Removed path as well: the worker of a claim the
                // reaper requeued just before this call can still observe the
                // token. That claim's end removes the entry.
                self.lease_registry.cancel(&job.queue, id);
                // The record is deleted, so its payload object (if any) is
                // removed here, after the commit.
                self.payload_store.delete_for(&job).await;
                self.completion_waiters
                    .settle(id, || WaitOutcome::Cancelled);
                debug!(job_id = %id, "pending/scheduled job cancelled");
                Ok((CancelOutcome::Removed, results.unwrap_or_default()))
            }
            // Not yet terminal: the worker's settlement delivers the outcome
            // when it acks, nacks or dead-letters.
            Ok(Cancelled::Requested { job }) => {
                self.finish_effects(prepared, None).await;
                self.lease_registry.cancel(&job.queue, id);
                debug!(job_id = %id, "claimed job cancellation requested");
                Ok((CancelOutcome::Requested, Vec::new()))
            }
            Ok(Cancelled::Uncommitted(outcome)) => {
                self.finish_effects(prepared, None).await;
                Ok((outcome, Vec::new()))
            }
            Err(err) => {
                self.finish_effects(prepared, None).await;
                Err(err)
            }
        }
    }

    /// The transaction loop of [`Self::cancel`]: resolve the job and commit the
    /// transition its state allows.
    async fn cancel_txn(&self, id: &str, prepared: &PreparedEffects) -> Result<Cancelled> {
        self.transition_by_id(
            id,
            Durability::Awaited,
            || Ok(Cancelled::Uncommitted(CancelOutcome::NotFound)),
            |txn, current_key, mut job| async move {
                let cancelled = match job.status {
                    JobStatus::Pending | JobStatus::Scheduled => {
                        stage_remove(&txn, &current_key, &job)?;
                        let staged = self.stage_effects(&txn, prepared).await?;
                        Cancelled::Removed { job, staged }
                    }
                    JobStatus::Claimed => {
                        if job.cancel_requested {
                            // The flag is already persisted. The token is fired
                            // again, because a re-claim after the first request
                            // has a fresh token.
                            txn.rollback();
                            self.lease_registry.cancel(&job.queue, id);
                            debug!(job_id = %id, "cancel re-requested on claimed job");
                            return Ok(Attempt::Abort(Cancelled::Uncommitted(
                                CancelOutcome::Requested,
                            )));
                        }
                        job.cancel_requested = true;
                        let value = job.stored_bytes()?;
                        txn.put(&current_key, &value)?;
                        Cancelled::Requested { job }
                    }
                    JobStatus::Done | JobStatus::Dead => {
                        txn.rollback();
                        return Ok(Attempt::Abort(Cancelled::Uncommitted(
                            CancelOutcome::NotFound,
                        )));
                    }
                };
                Ok(Attempt::Commit(txn, cancelled))
            },
        )
        .await
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
