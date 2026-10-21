//! The application of the core kinds `enqueue`, `cancel` and `signal`.

use crate::effects::EnqueueRequest;
use crate::error::{Error, Result};
use crate::inbox::InboxState;
use crate::inbox::entry::{CancelEntry, EnqueueEntry, InboxKind, SignalEntry};
use crate::inbox::record::{Applied, InboxOutcome, InboxRecordHandle, Settled, malformed};
use crate::queue::{CancelOutcome, EnqueueResult, SignalOutcome};
use crate::queue_core::QueueCore;
use crate::queue_core::signal::stage_signal;
use crate::txn::{Attempt, Durability};

/// A kind that the core applies.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CoreKind {
    Enqueue,
    Cancel,
    Signal,
}

impl CoreKind {
    pub(crate) fn parse(kind: &str) -> Option<Self> {
        match kind {
            EnqueueEntry::KIND => Some(Self::Enqueue),
            CancelEntry::KIND => Some(Self::Cancel),
            SignalEntry::KIND => Some(Self::Signal),
            _ => None,
        }
    }
}

/// A permanent error of the queue rejects the entry, and any other error ends
/// the pass.
fn rejected_or_failed<T, O>(result: Result<T>) -> Result<std::result::Result<T, Applied<O>>> {
    match result {
        Ok(value) => Ok(Ok(value)),
        Err(err) if err.is_permanent() => Ok(Err(Applied::from(err))),
        Err(err) => Err(err),
    }
}

impl QueueCore {
    /// Apply the entry `id` of the core kind `kind` with `body`, recorded at
    /// `applied_at_ms`.
    pub(crate) async fn apply_core_kind(
        &self,
        inbox: &InboxState,
        kind: CoreKind,
        id: &str,
        applied_at_ms: u64,
        body: serde_json::Value,
    ) -> Result<Settled> {
        let id = id.to_string();
        match kind {
            CoreKind::Enqueue => {
                let record = InboxRecordHandle::new(id, applied_at_ms);
                let applied = match serde_json::from_value(body) {
                    Ok(enqueue) => self.apply_enqueue(inbox, enqueue, &record).await?,
                    Err(err) => malformed(err),
                };
                record.settle(applied)
            }
            CoreKind::Cancel => {
                let record = InboxRecordHandle::new(id, applied_at_ms);
                let applied = match serde_json::from_value(body) {
                    Ok(cancel) => self.apply_cancel(inbox, cancel, &record).await?,
                    Err(err) => malformed(err),
                };
                record.settle(applied)
            }
            CoreKind::Signal => {
                let record = InboxRecordHandle::new(id, applied_at_ms);
                let applied = match serde_json::from_value(body) {
                    Ok(signal) => self.apply_signal(signal, &record).await?,
                    Err(err) => malformed(err),
                };
                record.settle(applied)
            }
        }
    }

    async fn apply_enqueue(
        &self,
        inbox: &InboxState,
        entry: EnqueueEntry,
        record: &InboxRecordHandle<EnqueueResult>,
    ) -> Result<Applied<EnqueueResult>> {
        if !inbox.allows(&entry.queue) {
            return Ok(outside_allow_list(&entry.queue));
        }
        let request = EnqueueRequest::from(entry);
        let prepared = match rejected_or_failed(self.prepare_job_record(
            &request.queue,
            request.payload,
            request.options,
        ))? {
            Ok(prepared) => prepared,
            Err(rejected) => return Ok(rejected),
        };
        let outcome = EnqueueResult::New(prepared.job.id.clone());
        let effects = self.prepare_effects(record.effects(&outcome)?).await?;
        match rejected_or_failed(self.write_job(prepared, effects).await)? {
            Ok((EnqueueResult::New(_), _)) => Ok(Applied::Committed),
            Ok((existing @ EnqueueResult::AlreadyEnqueued(_), _)) => {
                Ok(Applied::Separately(InboxOutcome::Applied(existing)))
            }
            Err(rejected) => Ok(rejected),
        }
    }

    async fn apply_cancel(
        &self,
        inbox: &InboxState,
        entry: CancelEntry,
        record: &InboxRecordHandle<CancelOutcome>,
    ) -> Result<Applied<CancelOutcome>> {
        let not_found = || Applied::Separately(InboxOutcome::Applied(CancelOutcome::NotFound));
        let Some(job) = self.view.job_record(&entry.job_id).await? else {
            return Ok(not_found());
        };
        if !inbox.allows(&job.queue) {
            return Ok(outside_allow_list(&job.queue));
        }
        let effects = record.effects(&CancelOutcome::Removed)?;
        Ok(match self.cancel(&entry.job_id, effects).await?.0 {
            CancelOutcome::Removed => Applied::Committed,
            CancelOutcome::Requested => {
                Applied::Separately(InboxOutcome::Applied(CancelOutcome::Requested))
            }
            CancelOutcome::NotFound => not_found(),
        })
    }

    /// Commit the signal and the record with its outcome in one transaction.
    async fn apply_signal(
        &self,
        entry: SignalEntry,
        record: &InboxRecordHandle<SignalOutcome>,
    ) -> Result<Applied<SignalOutcome>> {
        let (job_id, payload) = (&entry.job_id, &entry.payload);
        self.retry(Durability::Awaited, |mut txn| async move {
            let outcome = stage_signal(&mut txn, job_id, payload.clone(), self.now_ms()).await?;
            let stored = record.staged(&InboxOutcome::Applied(&outcome))?;
            txn.stage_inbox_record(&stored).await?;
            Ok((txn, Attempt::Commit(())))
        })
        .await?;
        Ok(Applied::Committed)
    }
}

/// The rejection of an entry for a queue outside the allow-list.
fn outside_allow_list<O>(queue: &str) -> Applied<O> {
    Applied::Separately(InboxOutcome::Rejected(format!(
        "queue `{queue}` is outside the inbox allow-list"
    )))
}

impl<O> From<Error> for Applied<O> {
    fn from(err: Error) -> Self {
        Applied::Separately(InboxOutcome::Rejected(err.to_string()))
    }
}
