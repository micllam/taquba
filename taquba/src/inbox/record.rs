//! The record of an applied inbox entry, and the handle through which the
//! application of a kind commits the record in the transaction of the entry's
//! effect.

use serde::{Deserialize, Serialize};

use crate::codec::encode;
use crate::effects::SettlementEffects;
use crate::keys::QueueName;
use crate::queue::{CancelOutcome, EnqueueResult, SignalOutcome};

/// The outcome of an applied inbox entry, stored in its [`InboxRecord`]: the
/// outcome of the operation of the entry's kind, or a rejection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InboxOutcome {
    /// An [`EnqueueEntry`](crate::EnqueueEntry) was applied, with the result of
    /// the enqueue.
    Enqueue(EnqueueResult),
    /// A [`CancelEntry`](crate::CancelEntry) was applied, with the outcome of
    /// the cancel.
    Cancel(CancelOutcome),
    /// A [`SignalEntry`](crate::SignalEntry) was applied, with the outcome of
    /// the signal.
    Signal(SignalOutcome),
    /// The entry was rejected for the reason, without an effect.
    Rejected(String),
}

/// The record of an applied inbox entry, read through
/// [`QueueView::inbox_record`](crate::QueueView::inbox_record). It is written
/// in the transaction of the entry's effect and deleted
/// [`InboxOptions::record_retention`](crate::InboxOptions::record_retention)
/// after its application. Within that retention, an entry of its queue and id
/// does not apply again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxRecord {
    /// When the writer applied the entry, as milliseconds from the Unix epoch
    /// on the queue's clock.
    pub applied_at_ms: u64,
    /// The outcome of the entry.
    pub outcome: InboxOutcome,
}

/// The record of an entry during its application: the queue and the id of the
/// entry and the time of the pass that applies it.
pub(crate) struct InboxRecordHandle {
    queue: QueueName,
    pub(crate) id: String,
    applied_at_ms: u64,
}

impl InboxRecordHandle {
    pub(crate) fn new(queue: QueueName, id: String, applied_at_ms: u64) -> Self {
        Self {
            queue,
            id,
            applied_at_ms,
        }
    }

    /// Effects that contain the record of the entry with `outcome`.
    pub(crate) fn effects(&self, outcome: InboxOutcome) -> SettlementEffects {
        SettlementEffects {
            inbox_record: Some(self.staged(outcome)),
            ..SettlementEffects::default()
        }
    }

    /// The record of the entry with `outcome`, for a commit of its own.
    pub(crate) fn applied(&self, outcome: InboxOutcome) -> Settled {
        Settled::Applied(self.staged(outcome))
    }

    /// The encoded record of the entry with `outcome`.
    pub(crate) fn staged(&self, outcome: InboxOutcome) -> StagedRecord {
        StagedRecord::encode(
            self.queue.clone(),
            self.id.clone(),
            self.applied_at_ms,
            outcome,
        )
    }
}

/// What the pass commits for an applied entry.
pub(crate) enum Settled {
    /// The record was committed with the entry's effect.
    Committed,
    /// The record of an applied outcome awaits a commit of its own.
    Applied(StagedRecord),
    /// The entry was rejected for the reason, and its record awaits a commit of
    /// its own.
    Rejected(String),
}

/// The encoded record of an entry, as a transaction stages it.
#[derive(Debug, Clone)]
pub(crate) struct StagedRecord {
    pub(crate) queue: QueueName,
    pub(crate) id: String,
    pub(crate) applied_at_ms: u64,
    pub(crate) bytes: Vec<u8>,
}

impl StagedRecord {
    /// The record of the entry `id` of `queue`, applied at `applied_at_ms` with
    /// `outcome`.
    fn encode(queue: QueueName, id: String, applied_at_ms: u64, outcome: InboxOutcome) -> Self {
        let record = InboxRecord {
            applied_at_ms,
            outcome,
        };
        Self {
            queue,
            id,
            applied_at_ms,
            bytes: encode(&record),
        }
    }
}
