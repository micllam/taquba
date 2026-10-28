//! The control transitions of [`QueueCore`]: the cancel of a job and the early
//! wake of a scheduled job.

use tracing::debug;

use crate::effects::SettlementEffects;
use crate::error::Result;
use crate::job::{JobRecord, JobStatus};
use crate::keys::QueueName;
use crate::queue::{CancelOutcome, EnqueueResult, WaitOutcome, WakeOutcome};
use crate::queue_core::QueueCore;
use crate::txn::{Attempt, Durability, Txn, get_indexed_job};

/// What moves a scheduled job to pending in [`stage_wake`].
pub(crate) enum Wake {
    /// A signal with its payload, which wakes only a job waiting for a signal
    /// and is stored on [`JobRecord::signal`].
    Signal(Vec<u8>),
    /// An early wake, which wakes any scheduled job and leaves its record
    /// unchanged otherwise.
    Early,
}

/// Why [`stage_wake`] left a job unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NotWoken {
    /// No job has the id.
    Missing,
    /// The job is not scheduled.
    Unscheduled,
    /// The wake is a signal, and the job is not waiting for a signal.
    NoWait,
}

/// Stage the `wake` of the scheduled job `job_id` in `txn`, and return its
/// queue. The transition reads the record, so a wake and a concurrent
/// transition of the job conflict and one of the two retries against the
/// other's commit.
pub(crate) async fn stage_wake(
    txn: &mut Txn,
    job_id: &str,
    wake: Wake,
) -> Result<std::result::Result<QueueName, NotWoken>> {
    let Some((_, current_key, mut job)) = get_indexed_job(txn, job_id).await? else {
        return Ok(Err(NotWoken::Missing));
    };
    if job.status != JobStatus::Scheduled {
        return Ok(Err(NotWoken::Unscheduled));
    }
    if let Wake::Signal(payload) = wake {
        if !job.signal_wait {
            return Ok(Err(NotWoken::NoWait));
        }
        job.signal = Some(payload);
    }
    txn.stage_to_pending(&current_key, &mut job)?;
    Ok(Ok(job.queue))
}

/// What [`stage_cancel`] staged for a job.
pub(crate) enum StagedCancel {
    /// The pending or scheduled job is removed.
    Removed(JobRecord),
    /// The cancellation of the claimed job is recorded on its record.
    Requested(JobRecord),
    /// The record of the claimed job contains the cancellation already. Nothing
    /// is staged.
    AlreadyRequested(JobRecord),
    /// The job is missing, done or dead. Nothing is staged.
    NotFound,
}

impl StagedCancel {
    /// The outcome of the cancel for its caller.
    pub(crate) fn outcome(&self) -> CancelOutcome {
        match self {
            Self::Removed(_) => CancelOutcome::Removed,
            Self::Requested(_) | Self::AlreadyRequested(_) => CancelOutcome::Requested,
            Self::NotFound => CancelOutcome::NotFound,
        }
    }
}

/// Stage the cancel of the job `job_id` in `txn`: the removal of a pending or
/// scheduled job, or the cancellation flag on the record of a claimed job.
pub(crate) async fn stage_cancel(txn: &mut Txn, job_id: &str) -> Result<StagedCancel> {
    let Some((_, current_key, mut job)) = get_indexed_job(txn, job_id).await? else {
        return Ok(StagedCancel::NotFound);
    };
    Ok(match job.status {
        JobStatus::Pending | JobStatus::Scheduled => {
            txn.stage_remove(&current_key, &job)?;
            StagedCancel::Removed(job)
        }
        JobStatus::Claimed if job.cancel_requested => StagedCancel::AlreadyRequested(job),
        JobStatus::Claimed => {
            job.cancel_requested = true;
            txn.move_job(&current_key, &mut job)?;
            StagedCancel::Requested(job)
        }
        JobStatus::Done | JobStatus::Dead => StagedCancel::NotFound,
    })
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
        let mut committed = {
            let prepared = &prepared;
            self.retry(Durability::Awaited, |mut txn| async move {
                let staged = stage_cancel(&mut txn, id).await?;
                Ok(match staged {
                    StagedCancel::Removed(_) => {
                        let results = self.stage_effects(&mut txn, prepared).await?;
                        (txn, Attempt::Commit((staged, Some(results))))
                    }
                    StagedCancel::Requested(_) => (txn, Attempt::Commit((staged, None))),
                    StagedCancel::AlreadyRequested(_) | StagedCancel::NotFound => {
                        (txn, Attempt::Abort((staged, None)))
                    }
                })
            })
            .await
        };
        let results = committed
            .as_mut()
            .ok()
            .and_then(|(_, results)| results.take());
        let results = self.finish_effects(prepared, results).await;
        let (staged, _) = committed?;
        let outcome = staged.outcome();
        if let Some(removed) = self.finish_cancel(staged) {
            self.payload_store.delete_for(&removed).await;
        }
        Ok((outcome, results.unwrap_or_default()))
    }

    /// Complete a cancel after its transaction. It fires the lease token of the
    /// job and settles the completion waiters of a removed job, and returns the
    /// removed job, whose payload object the caller deletes. The token of a
    /// removed job fires as well, for the worker of a claim that the reaper
    /// requeued before the cancel. A claimed job reaches its waiters at its
    /// settlement.
    pub(crate) fn finish_cancel(&self, staged: StagedCancel) -> Option<JobRecord> {
        match staged {
            StagedCancel::Removed(job) => {
                self.lease_registry.cancel(&job.queue, &job.id);
                self.completion_waiters
                    .settle(&job.id, || WaitOutcome::Cancelled);
                debug!(job_id = %job.id, "pending/scheduled job cancelled");
                Some(job)
            }
            StagedCancel::Requested(job) | StagedCancel::AlreadyRequested(job) => {
                self.lease_registry.cancel(&job.queue, &job.id);
                debug!(job_id = %job.id, "claimed job cancellation requested");
                None
            }
            StagedCancel::NotFound => None,
        }
    }

    /// Move the scheduled job `id` to pending before its `run_at`, as
    /// [`Queue::wake_scheduled`](crate::Queue::wake_scheduled) documents.
    pub(crate) async fn wake_scheduled(&self, id: &str) -> Result<WakeOutcome> {
        let outcome = self
            .retry(Durability::Awaited, |mut txn| async move {
                let staged = stage_wake(&mut txn, id, Wake::Early).await?;
                Ok(match staged {
                    Ok(queue) => (txn, Attempt::Commit(Ok(queue))),
                    Err(NotWoken::Missing) => (txn, Attempt::Abort(Err(WakeOutcome::NotFound))),
                    Err(_) => (txn, Attempt::Abort(Err(WakeOutcome::NotScheduled))),
                })
            })
            .await?;
        match outcome {
            Ok(queue) => {
                debug!(job_id = %id, queue = %queue, "scheduled job woken");
                Ok(WakeOutcome::Woken)
            }
            Err(outcome) => Ok(outcome),
        }
    }
}
