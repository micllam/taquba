//! The application of the core kinds `enqueue`, `cancel` and `signal`.

use serde::de::DeserializeOwned;

use crate::error::Result;
use crate::inbox::entry::{CancelEntry, EnqueueEntry, InboxKind, SignalEntry};
use crate::inbox::record::{InboxOutcome, InboxRecordHandle, Settled};
use crate::queue::{CancelOutcome, EnqueueResult};
use crate::queue_core::QueueCore;
use crate::queue_core::signal::stage_signal;
use crate::txn::{Attempt, Durability};

/// A kind of the inbox.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CoreKind {
    Enqueue,
    Cancel,
    Signal,
}

impl CoreKind {
    /// The kind of the name `kind`, or `None` for another name.
    pub(crate) fn parse(kind: &str) -> Option<Self> {
        match kind {
            EnqueueEntry::KIND => Some(Self::Enqueue),
            CancelEntry::KIND => Some(Self::Cancel),
            SignalEntry::KIND => Some(Self::Signal),
            _ => None,
        }
    }
}

impl QueueCore {
    /// Decode `body` as an entry of `kind` and apply it to `queue` with
    /// `record`.
    pub(crate) async fn apply_entry(
        &self,
        kind: CoreKind,
        queue: &str,
        record: &InboxRecordHandle,
        body: &[u8],
    ) -> Result<Settled> {
        match kind {
            CoreKind::Enqueue => {
                apply_decoded(body, async |entry| {
                    self.apply_enqueue(queue, entry, record).await
                })
                .await
            }
            CoreKind::Cancel => {
                apply_decoded(body, async |entry| {
                    self.apply_cancel(queue, entry, record).await
                })
                .await
            }
            CoreKind::Signal => {
                apply_decoded(body, async |entry| {
                    self.apply_signal(queue, entry, record).await
                })
                .await
            }
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
        let outcome = EnqueueResult::New(prepared.job.id.clone());
        let effects = self.prepare_effects(record.effects(&outcome)?).await?;
        match self.write_job(prepared, effects).await?.0 {
            EnqueueResult::New(_) => Ok(Settled::Committed),
            existing @ EnqueueResult::AlreadyEnqueued(_) => record.applied(&existing),
        }
    }

    async fn apply_cancel(
        &self,
        queue: &str,
        entry: CancelEntry,
        record: &InboxRecordHandle,
    ) -> Result<Settled> {
        let Some(job) = self.view.job_record(&entry.job_id).await? else {
            return record.applied(&CancelOutcome::NotFound);
        };
        if *job.queue != *queue {
            return Ok(on_another_queue(&entry.job_id, &job.queue, queue));
        }
        let effects = record.effects(&CancelOutcome::Removed)?;
        match self.cancel(&entry.job_id, effects).await?.0 {
            CancelOutcome::Removed => Ok(Settled::Committed),
            outcome @ (CancelOutcome::Requested | CancelOutcome::NotFound) => {
                record.applied(&outcome)
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
            let stored = record.staged(InboxOutcome::Applied(&outcome))?;
            txn.stage_inbox_record(&stored).await?;
            Ok((txn, Attempt::Commit(())))
        })
        .await?;
        Ok(Settled::Committed)
    }
}

/// Decode `body` as `E` and apply it through `apply`. A body that does not
/// decode and a permanent error of the queue reject the entry, and any other
/// error ends the pass.
async fn apply_decoded<E: DeserializeOwned>(
    body: &[u8],
    apply: impl AsyncFnOnce(E) -> Result<Settled>,
) -> Result<Settled> {
    let entry = match serde_json::from_slice::<E>(body) {
        Ok(entry) => entry,
        Err(err) => return Ok(Settled::Rejected(format!("malformed body: {err}"))),
    };
    match apply(entry).await {
        Err(err) if err.is_permanent() => Ok(Settled::Rejected(err.to_string())),
        settled => settled,
    }
}

/// The rejection of an entry for `queue` about the job `job_id` of `job_queue`.
fn on_another_queue(job_id: &str, job_queue: &str, queue: &str) -> Settled {
    Settled::Rejected(format!(
        "job `{job_id}` is on queue `{job_queue}`, not `{queue}`"
    ))
}
