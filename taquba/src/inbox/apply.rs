//! The application of the kinds `enqueue`, `cancel` and `signal`.

use crate::error::Result;
use crate::inbox::entry::{CancelEntry, EnqueueEntry, InboxEntry, SignalEntry};
use crate::inbox::record::{InboxOutcome, InboxRecordHandle, Settled};
use crate::queue::{CancelOutcome, EnqueueResult};
use crate::queue_core::QueueCore;
use crate::queue_core::signal::stage_signal;
use crate::txn::{Attempt, Durability};

impl QueueCore {
    /// Apply `entry` to `queue` with `record`. A permanent error of the queue
    /// rejects the entry, and any other error ends the pass.
    pub(crate) async fn apply_entry(
        &self,
        queue: &str,
        record: &InboxRecordHandle,
        entry: InboxEntry,
    ) -> Result<Settled> {
        let settled = match entry {
            InboxEntry::Enqueue(entry) => self.apply_enqueue(queue, entry, record).await,
            InboxEntry::Cancel(entry) => self.apply_cancel(queue, entry, record).await,
            InboxEntry::Signal(entry) => self.apply_signal(queue, entry, record).await,
        };
        match settled {
            Err(err) if err.is_permanent() => Ok(Settled::Rejected(err.to_string())),
            settled => settled,
        }
    }

    async fn apply_enqueue(
        &self,
        queue: &str,
        entry: EnqueueEntry,
        record: &InboxRecordHandle,
    ) -> Result<Settled> {
        let request = entry.into_request(queue.to_string());
        let prepared = self.prepare_job_record(&request.queue, request.payload, request.options)?;
        let outcome = InboxOutcome::Enqueue(EnqueueResult::New(prepared.job.id.clone()));
        let effects = self.prepare_effects(record.effects(outcome)?).await?;
        match self.write_job(prepared, effects).await?.0 {
            EnqueueResult::New(_) => Ok(Settled::Committed),
            existing @ EnqueueResult::AlreadyEnqueued(_) => {
                record.applied(InboxOutcome::Enqueue(existing))
            }
        }
    }

    async fn apply_cancel(
        &self,
        queue: &str,
        entry: CancelEntry,
        record: &InboxRecordHandle,
    ) -> Result<Settled> {
        let Some(job) = self.view.job_record(&entry.job_id).await? else {
            return record.applied(InboxOutcome::Cancel(CancelOutcome::NotFound));
        };
        if *job.queue != *queue {
            return Ok(on_another_queue(&entry.job_id, &job.queue, queue));
        }
        let effects = record.effects(InboxOutcome::Cancel(CancelOutcome::Removed))?;
        match self.cancel(&entry.job_id, effects).await?.0 {
            CancelOutcome::Removed => Ok(Settled::Committed),
            outcome @ (CancelOutcome::Requested | CancelOutcome::NotFound) => {
                record.applied(InboxOutcome::Cancel(outcome))
            }
        }
    }

    /// Commit the signal and the record with its outcome in one transaction.
    async fn apply_signal(
        &self,
        queue: &str,
        entry: SignalEntry,
        record: &InboxRecordHandle,
    ) -> Result<Settled> {
        if let Some(job) = self.view.job_record(&entry.job_id).await?
            && *job.queue != *queue
        {
            return Ok(on_another_queue(&entry.job_id, &job.queue, queue));
        }
        let (job_id, payload) = (&entry.job_id, &entry.payload);
        self.retry(Durability::Awaited, |mut txn| async move {
            let outcome = stage_signal(&mut txn, job_id, payload.clone(), self.now_ms()).await?;
            let stored = record.staged(InboxOutcome::Signal(outcome))?;
            txn.stage_inbox_record(&stored).await?;
            Ok((txn, Attempt::Commit(())))
        })
        .await?;
        Ok(Settled::Committed)
    }
}

/// The rejection of an entry for `queue` about the job `job_id` of `job_queue`.
fn on_another_queue(job_id: &str, job_queue: &str, queue: &str) -> Settled {
    Settled::Rejected(format!(
        "job `{job_id}` is on queue `{job_queue}`, not `{queue}`"
    ))
}
