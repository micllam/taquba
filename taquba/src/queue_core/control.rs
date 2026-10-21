//! The control transitions of [`QueueCore`]: the cancel of a job and the early
//! wake of a scheduled job.

use tracing::debug;

use crate::effects::{PreparedEffects, SettlementEffects};
use crate::error::Result;
use crate::job::{JobRecord, JobStatus};
use crate::queue::{CancelOutcome, EnqueueResult, WaitOutcome, WakeOutcome};
use crate::queue_core::QueueCore;
use crate::txn::{Attempt, Durability};

/// What the cancel transaction did.
enum Cancelled {
    /// The pending or scheduled job was removed, and the effects were staged
    /// with these enqueue results.
    Removed {
        job: JobRecord,
        results: Vec<EnqueueResult>,
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
            Ok(Cancelled::Removed { job, results }) => {
                let results = self.finish_effects(prepared, Some(results)).await;
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
            |mut txn, current_key, mut job| async move {
                let cancelled = match job.status {
                    JobStatus::Pending | JobStatus::Scheduled => {
                        txn.stage_remove(&current_key, &job)?;
                        let results = self.stage_effects(&mut txn, prepared).await?;
                        Cancelled::Removed { job, results }
                    }
                    JobStatus::Claimed => {
                        if job.cancel_requested {
                            // The flag is already persisted. The token is fired
                            // again, because a re-claim after the first request
                            // has a fresh token.
                            self.lease_registry.cancel(&job.queue, id);
                            debug!(job_id = %id, "cancel re-requested on claimed job");
                            let requested = Cancelled::Uncommitted(CancelOutcome::Requested);
                            return Ok((txn, Attempt::Abort(requested)));
                        }
                        job.cancel_requested = true;
                        txn.move_job(&current_key, &mut job)?;
                        Cancelled::Requested { job }
                    }
                    JobStatus::Done | JobStatus::Dead => {
                        let not_found = Cancelled::Uncommitted(CancelOutcome::NotFound);
                        return Ok((txn, Attempt::Abort(not_found)));
                    }
                };
                Ok((txn, Attempt::Commit(cancelled)))
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
        let woken: std::result::Result<JobRecord, WakeOutcome> = self
            .transition_by_id(
                id,
                Durability::Awaited,
                || Ok(Err(WakeOutcome::NotFound)),
                |mut txn, current_key, mut job| async move {
                    if job.status != JobStatus::Scheduled {
                        return Ok((txn, Attempt::Abort(Err(WakeOutcome::NotScheduled))));
                    }
                    job.woken_at = Some(self.now_ms());
                    job.wake_payload = wake_payload.clone();
                    txn.stage_to_pending(&current_key, &mut job)?;
                    Ok((txn, Attempt::Commit(Ok(job))))
                },
            )
            .await?;
        match woken {
            Ok(job) => {
                debug!(job_id = %id, queue = %job.queue, "scheduled job woken");
                Ok(WakeOutcome::Woken)
            }
            Err(outcome) => Ok(outcome),
        }
    }
}
