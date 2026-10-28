//! The application of the kinds `enqueue`, `cancel` and `signal`.

use crate::error::Result;
use crate::inbox::entry::{CancelEntry, EnqueueEntry, InboxEntry, SignalEntry};
use crate::inbox::record::{InboxOutcome, InboxRecordHandle, Rejection};
use crate::queue::EnqueueResult;
use crate::queue_core::QueueCore;
use crate::queue_core::control::stage_cancel;
use crate::queue_core::signal::stage_signal;
use crate::txn::{Attempt, Durability, Txn, get_indexed_job};

impl QueueCore {
    /// Apply `entry` to `queue` and return its outcome. One transaction stages
    /// the effect of the kind and the record with the outcome. A cancel or a
    /// signal for a job of another queue commits a rejection in that
    /// transaction. A permanent error of the operation commits a rejection in a
    /// transaction of its own, and any other error returns.
    pub(crate) async fn apply_entry(
        &self,
        queue: &str,
        record: &InboxRecordHandle,
        entry: InboxEntry,
    ) -> Result<InboxOutcome> {
        let applied = match entry {
            InboxEntry::Enqueue(entry) => self.apply_enqueue(queue, entry, record).await,
            InboxEntry::Cancel(entry) => self.apply_cancel(queue, entry, record).await,
            InboxEntry::Signal(entry) => self.apply_signal(queue, entry, record).await,
        };
        match applied {
            Err(err) if err.is_permanent() => {
                let rejection = Rejection::of_permanent(err);
                self.commit_rejection(record, rejection).await
            }
            applied => applied,
        }
    }

    async fn apply_enqueue(
        &self,
        queue: &str,
        entry: EnqueueEntry,
        record: &InboxRecordHandle,
    ) -> Result<InboxOutcome> {
        let mut prepared = self.prepare_job_record(queue, entry.payload, entry.options)?;
        let offloaded = self.payload_store.offload(&mut prepared.job).await;
        let timer = crate::obs::start();
        let committed = match offloaded {
            Ok(()) => {
                let prepared = &prepared;
                self.retry(Durability::Awaited, |mut txn| async move {
                    // After a dedup hit, the record commits alone.
                    let result = match self.stage_job_writes(&mut txn, prepared).await? {
                        Ok(id) => EnqueueResult::New(id),
                        Err(existing) => EnqueueResult::AlreadyEnqueued(existing),
                    };
                    let outcome = InboxOutcome::Enqueue(result);
                    txn.stage_inbox_record(&record.staged(outcome.clone()))?;
                    Ok((txn, Attempt::Commit(outcome)))
                })
                .await
            }
            Err(err) => Err(err),
        };
        let new = matches!(committed, Ok(InboxOutcome::Enqueue(EnqueueResult::New(_))));
        self.finish_job_write(&prepared, new, timer).await;
        committed
    }

    async fn apply_cancel(
        &self,
        queue: &str,
        entry: CancelEntry,
        record: &InboxRecordHandle,
    ) -> Result<InboxOutcome> {
        let job_id = entry.job_id.as_str();
        let (outcome, staged) = self
            .retry(Durability::Awaited, |mut txn| async move {
                let (outcome, staged) = match on_another_queue(&txn, job_id, queue).await? {
                    Some(rejection) => (InboxOutcome::Rejected(rejection), None),
                    None => {
                        let staged = stage_cancel(&mut txn, job_id).await?;
                        (InboxOutcome::Cancel(staged.outcome()), Some(staged))
                    }
                };
                txn.stage_inbox_record(&record.staged(outcome.clone()))?;
                Ok((txn, Attempt::Commit((outcome, staged))))
            })
            .await?;
        if let Some(removed) = staged.and_then(|staged| self.finish_cancel(staged)) {
            self.payload_store.delete_for(&removed).await;
        }
        Ok(outcome)
    }

    async fn apply_signal(
        &self,
        queue: &str,
        entry: SignalEntry,
        record: &InboxRecordHandle,
    ) -> Result<InboxOutcome> {
        let (job_id, payload) = (entry.job_id.as_str(), &entry.payload);
        self.retry(Durability::Awaited, |mut txn| async move {
            let outcome = match on_another_queue(&txn, job_id, queue).await? {
                Some(rejection) => InboxOutcome::Rejected(rejection),
                None => {
                    InboxOutcome::Signal(stage_signal(&mut txn, job_id, payload.clone()).await?)
                }
            };
            txn.stage_inbox_record(&record.staged(outcome.clone()))?;
            Ok((txn, Attempt::Commit(outcome)))
        })
        .await
    }

    /// Commit the record of the entry of `record` with `rejection`, in a
    /// transaction of its own.
    pub(crate) async fn commit_rejection(
        &self,
        record: &InboxRecordHandle,
        rejection: Rejection,
    ) -> Result<InboxOutcome> {
        let outcome = InboxOutcome::Rejected(rejection);
        let staged = &record.staged(outcome.clone());
        self.retry(Durability::Awaited, |mut txn| async move {
            txn.stage_inbox_record(staged)?;
            Ok((txn, Attempt::Commit(())))
        })
        .await?;
        Ok(outcome)
    }
}

/// The rejection of an entry for `queue` about the job `job_id`, read in `txn`,
/// when the job is on another queue.
async fn on_another_queue(txn: &Txn, job_id: &str, queue: &str) -> Result<Option<Rejection>> {
    Ok(match get_indexed_job(txn, job_id).await? {
        Some((_, _, job)) if *job.queue != *queue => Some(Rejection::OtherQueue {
            job_queue: job.queue.to_string(),
        }),
        _ => None,
    })
}
