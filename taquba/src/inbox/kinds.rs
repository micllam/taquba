//! The application of the core kinds `enqueue`, `cancel` and `signal`.

use serde_json::json;

use crate::error::{Error, Result};
use crate::inbox::entry::{CancelEntry, EnqueueEntry, InboxKind, SignalEntry};
use crate::inbox::record::{Applied, InboxOutcome, InboxRecordHandle};
use crate::inbox::{InboxEntry, InboxState};
use crate::options::EnqueueOptions;
use crate::queue::{CancelOutcome, EnqueueResult};
use crate::queue_core::QueueCore;
use crate::queue_core::signal::stage_signal;
use crate::signal::SignalOutcome;
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

/// The rejection of an entry whose body does not decode as its kind.
fn malformed(err: serde_json::Error) -> Applied {
    Applied::Separately(InboxOutcome::Rejected(format!("malformed body: {err}")))
}

/// A permanent error of the queue rejects the entry, and any other error ends
/// the pass.
fn rejected_or_failed<T>(result: Result<T>) -> Result<std::result::Result<T, Applied>> {
    match result {
        Ok(value) => Ok(Ok(value)),
        Err(err) if err.is_permanent() => Ok(Err(Applied::Separately(InboxOutcome::Rejected(
            err.to_string(),
        )))),
        Err(err) => Err(err),
    }
}

impl QueueCore {
    /// Apply `entry` of the core kind `kind` with the record `record`.
    pub(crate) async fn apply_core_kind(
        &self,
        inbox: &InboxState,
        kind: CoreKind,
        entry: &InboxEntry,
        record: &InboxRecordHandle,
    ) -> Result<Applied> {
        match kind {
            CoreKind::Enqueue => match entry.decode::<EnqueueEntry>() {
                Ok(enqueue) => self.apply_enqueue(inbox, enqueue, record).await,
                Err(err) => Ok(malformed(err)),
            },
            CoreKind::Cancel => match entry.decode::<CancelEntry>() {
                Ok(cancel) => self.apply_cancel(inbox, cancel, record).await,
                Err(err) => Ok(malformed(err)),
            },
            CoreKind::Signal => match entry.decode::<SignalEntry>() {
                Ok(signal) => self.apply_signal(signal, record).await,
                Err(err) => Ok(malformed(err)),
            },
        }
    }

    async fn apply_enqueue(
        &self,
        inbox: &InboxState,
        entry: EnqueueEntry,
        record: &InboxRecordHandle,
    ) -> Result<Applied> {
        if !inbox.allows(&entry.queue) {
            return Ok(outside_allow_list(&entry.queue));
        }
        let opts = EnqueueOptions {
            max_attempts: entry.max_attempts,
            priority: entry.priority,
            run_at: entry
                .run_at
                .map(|ms| std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms)),
            dedup_key: entry.dedup_key,
            headers: entry.headers,
            id_override: entry.job_id,
            signal_key: None,
        };
        let prepared =
            match rejected_or_failed(self.prepare_job_record(&entry.queue, entry.payload, opts))? {
                Ok(prepared) => prepared,
                Err(rejected) => return Ok(rejected),
            };
        let outcome = json!({ "job_id": prepared.job.id, "new": true });
        let effects = self.prepare_effects(record.effects(outcome)).await?;
        match rejected_or_failed(self.write_job(prepared, effects).await)? {
            Ok((EnqueueResult::New(_), _)) => Ok(Applied::Committed),
            Ok((EnqueueResult::AlreadyEnqueued(existing), _)) => Ok(Applied::Separately(
                InboxOutcome::Applied(json!({ "job_id": existing, "new": false })),
            )),
            Err(rejected) => Ok(rejected),
        }
    }

    async fn apply_cancel(
        &self,
        inbox: &InboxState,
        entry: CancelEntry,
        record: &InboxRecordHandle,
    ) -> Result<Applied> {
        let not_found =
            || Applied::Separately(InboxOutcome::Applied(json!({ "outcome": "not_found" })));
        let Some(job) = self.view.job_record(&entry.job_id).await? else {
            return Ok(not_found());
        };
        if !inbox.allows(&job.queue) {
            return Ok(outside_allow_list(&job.queue));
        }
        let effects = record.effects(json!({ "outcome": "removed" }));
        Ok(match self.cancel(&entry.job_id, effects).await?.0 {
            CancelOutcome::Removed => Applied::Committed,
            CancelOutcome::Requested => {
                Applied::Separately(InboxOutcome::Applied(json!({ "outcome": "requested" })))
            }
            CancelOutcome::NotFound => not_found(),
        })
    }

    /// Commit the signal and the record with its outcome in one transaction.
    async fn apply_signal(
        &self,
        entry: SignalEntry,
        record: &InboxRecordHandle,
    ) -> Result<Applied> {
        let (key, payload) = (&entry.key, &entry.payload);
        self.retry(Durability::Awaited, |mut txn| async move {
            let outcome = stage_signal(&mut txn, key, payload.clone(), self.now_ms()).await?;
            let outcome = match outcome {
                SignalOutcome::Delivered => "delivered",
                SignalOutcome::Buffered => "buffered",
            };
            let stored = record.record(InboxOutcome::Applied(json!({ "outcome": outcome })));
            txn.stage_inbox_record(record.id(), &stored).await?;
            Ok((txn, Attempt::Commit(())))
        })
        .await?;
        Ok(Applied::Committed)
    }
}

/// The rejection of an entry for a queue outside the allow-list.
fn outside_allow_list(queue: &str) -> Applied {
    Applied::Separately(InboxOutcome::Rejected(format!(
        "queue `{queue}` is outside the inbox allow-list"
    )))
}

impl From<Error> for Applied {
    fn from(err: Error) -> Self {
        Applied::Separately(InboxOutcome::Rejected(err.to_string()))
    }
}
