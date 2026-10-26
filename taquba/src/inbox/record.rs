//! The record of an applied inbox entry, and the handle through which the
//! application of a kind commits the record in the transaction of the entry's
//! effect.

use serde::{Deserialize, Serialize};

use crate::effects::SettlementEffects;
use crate::error::Result;

/// The outcome of an applied inbox entry, stored in its [`InboxRecord`]. `O` is
/// the [`InboxKind::Outcome`](crate::InboxKind::Outcome) of the entry's kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InboxOutcome<O> {
    /// The entry was applied, with the outcome of its kind.
    Applied(O),
    /// The entry was rejected for the reason, without an effect.
    Rejected(String),
}

/// The record of an applied inbox entry, read through
/// [`QueueView::inbox_record`](crate::QueueView::inbox_record). It is written
/// in the transaction of the entry's effect and deleted
/// [`InboxOptions::record_retention`](crate::InboxOptions::record_retention)
/// after its application, which is the window within which the entry's id is
/// not applied again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxRecord<O> {
    /// When the writer applied the entry, as milliseconds from the Unix epoch
    /// on the queue's clock.
    pub applied_at_ms: u64,
    /// The outcome of the entry.
    pub outcome: InboxOutcome<O>,
}

/// The record of an entry during its application: the id of the entry and the
/// time of the pass that applies it.
pub(crate) struct InboxRecordHandle {
    pub(crate) id: String,
    applied_at_ms: u64,
}

impl InboxRecordHandle {
    pub(crate) fn new(id: String, applied_at_ms: u64) -> Self {
        Self { id, applied_at_ms }
    }

    /// Effects that contain the record of the entry with `outcome`. Fails with
    /// the error of the encoding of `outcome`.
    pub(crate) fn effects<O: Serialize>(&self, outcome: &O) -> Result<SettlementEffects> {
        Ok(SettlementEffects {
            inbox_record: Some(self.staged(InboxOutcome::Applied(outcome))?),
            ..SettlementEffects::default()
        })
    }

    /// The record of the entry with `outcome`, for a commit of its own.
    pub(crate) fn applied<O: Serialize>(&self, outcome: &O) -> Result<Settled> {
        Ok(Settled::Applied(
            self.staged(InboxOutcome::Applied(outcome))?,
        ))
    }

    /// The encoded record of the entry with `outcome`.
    pub(crate) fn staged<O: Serialize>(&self, outcome: InboxOutcome<&O>) -> Result<StagedRecord> {
        StagedRecord::encode(self.id.clone(), self.applied_at_ms, outcome)
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
    pub(crate) id: String,
    pub(crate) applied_at_ms: u64,
    pub(crate) bytes: Vec<u8>,
}

impl StagedRecord {
    /// The record of the entry `id`, applied at `applied_at_ms` with `outcome`.
    fn encode<O: Serialize>(
        id: String,
        applied_at_ms: u64,
        outcome: InboxOutcome<&O>,
    ) -> Result<Self> {
        let record = InboxRecord {
            applied_at_ms,
            outcome,
        };
        Ok(Self {
            id,
            applied_at_ms,
            bytes: rmp_serde::to_vec_named(&record)?,
        })
    }
}
