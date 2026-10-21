//! The record of an applied inbox entry, and the handle through which a handler
//! commits the record in the transaction of the entry's effect.

use serde::{Deserialize, Serialize};

use crate::effects::SettlementEffects;

/// The outcome of an applied inbox entry, stored in its [`InboxRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InboxOutcome {
    /// The entry was applied, with the outcome its kind documents.
    Applied(serde_json::Value),
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
pub struct InboxRecord {
    /// When the writer applied the entry, as milliseconds from the Unix epoch
    /// on the queue's clock.
    pub applied_at_ms: u64,
    /// The outcome of the entry.
    pub outcome: InboxOutcome,
}

/// What an [`InboxHandler`](crate::InboxHandler) reports for an entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Applied {
    /// The handler committed the entry's effect with the record from
    /// [`InboxRecordHandle::effects`] or [`InboxRecordHandle::attach`]. The
    /// pass reads the record back and stops with
    /// [`Error::InboxRecordMissing`](crate::Error::InboxRecordMissing) when it
    /// is absent.
    Committed,
    /// The handler applied the entry without the record, because the
    /// application is idempotent, or rejected it. The pass commits the record
    /// with `outcome` on its own.
    Separately(InboxOutcome),
}

/// The record of one entry during its application, passed to the handler of its
/// kind. The handle builds the [`SettlementEffects`] that commit the record
/// with the entry's effect, through
/// [`Queue::enqueue_with_effects`](crate::Queue::enqueue_with_effects),
/// [`Queue::cancel_with`](crate::Queue::cancel_with),
/// [`Queue::commit_effects`](crate::Queue::commit_effects) or any settlement.
#[derive(Debug, Clone)]
pub struct InboxRecordHandle {
    id: String,
    applied_at_ms: u64,
}

impl InboxRecordHandle {
    pub(crate) fn new(id: String, applied_at_ms: u64) -> Self {
        Self { id, applied_at_ms }
    }

    /// The id of the entry.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Effects that contain the record of the entry with `outcome`, as the kind
    /// of the entry documents it.
    pub fn effects(&self, outcome: serde_json::Value) -> SettlementEffects {
        self.attach(SettlementEffects::default(), outcome)
    }

    /// `effects` with the record of the entry with `outcome` added.
    pub fn attach(
        &self,
        effects: SettlementEffects,
        outcome: serde_json::Value,
    ) -> SettlementEffects {
        self.attach_outcome(effects, InboxOutcome::Applied(outcome))
    }

    /// `effects` with the record of the entry with `outcome` added.
    pub(crate) fn attach_outcome(
        &self,
        mut effects: SettlementEffects,
        outcome: InboxOutcome,
    ) -> SettlementEffects {
        effects.inbox_record = Some((self.id.clone(), self.record(outcome)));
        effects
    }

    /// The record of the entry with `outcome`.
    pub(crate) fn record(&self, outcome: InboxOutcome) -> InboxRecord {
        InboxRecord {
            applied_at_ms: self.applied_at_ms,
            outcome,
        }
    }
}
