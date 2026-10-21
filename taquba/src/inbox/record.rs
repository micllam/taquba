//! The record of an applied inbox entry, and the handle through which a handler
//! commits the record in the transaction of the entry's effect.

use std::fmt;
use std::marker::PhantomData;

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

/// What an [`InboxHandler`](crate::InboxHandler) reports for an entry, with the
/// outcome `O` of its kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Applied<O> {
    /// The handler committed the entry's effect with the record from
    /// [`InboxRecordHandle::effects`] or [`InboxRecordHandle::attach`]. The
    /// pass reads the record back and stops with
    /// [`Error::InboxRecordMissing`](crate::Error::InboxRecordMissing) when it
    /// is absent.
    Committed,
    /// The handler applied the entry without the record, because the
    /// application is idempotent, or rejected it. The pass commits the record
    /// with `outcome` on its own.
    Separately(InboxOutcome<O>),
}

/// The record of one entry during its application, passed to the handler of its
/// kind with the outcome `O` of the kind. The handle builds the
/// [`SettlementEffects`] that commit the record with the entry's effect,
/// through [`Queue::enqueue_with_effects`](crate::Queue::enqueue_with_effects),
/// [`Queue::cancel_with`](crate::Queue::cancel_with),
/// [`Queue::commit_effects`](crate::Queue::commit_effects) or any settlement.
pub struct InboxRecordHandle<O> {
    id: String,
    applied_at_ms: u64,
    outcome: PhantomData<fn(&O)>,
}

impl<O> fmt::Debug for InboxRecordHandle<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InboxRecordHandle")
            .field("id", &self.id)
            .field("applied_at_ms", &self.applied_at_ms)
            .finish()
    }
}

impl<O: Serialize> InboxRecordHandle<O> {
    pub(crate) fn new(id: String, applied_at_ms: u64) -> Self {
        Self {
            id,
            applied_at_ms,
            outcome: PhantomData,
        }
    }

    /// The id of the entry.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Effects that contain the record of the entry with `outcome`. Fails with
    /// the error of the encoding of `outcome`.
    pub fn effects(&self, outcome: &O) -> Result<SettlementEffects> {
        self.attach(SettlementEffects::default(), outcome)
    }

    /// `effects` with the record of the entry with `outcome` added. Fails with
    /// the error of the encoding of `outcome`.
    pub fn attach(&self, effects: SettlementEffects, outcome: &O) -> Result<SettlementEffects> {
        self.attach_outcome(effects, &InboxOutcome::Applied(outcome))
    }

    /// `effects` with the record of the entry with `outcome` added.
    pub(crate) fn attach_outcome(
        &self,
        mut effects: SettlementEffects,
        outcome: &InboxOutcome<&O>,
    ) -> Result<SettlementEffects> {
        effects.inbox_record = Some(self.staged(outcome)?);
        Ok(effects)
    }

    /// What the pass commits for the entry that `applied` reports.
    pub(crate) fn settle(&self, applied: Applied<O>) -> Result<Settled> {
        Ok(match applied {
            Applied::Committed => Settled::Committed,
            Applied::Separately(InboxOutcome::Applied(outcome)) => Settled::Separately {
                record: self.staged(&InboxOutcome::Applied(&outcome))?,
                rejection: None,
            },
            Applied::Separately(InboxOutcome::Rejected(reason)) => Settled::Separately {
                record: self.staged(&InboxOutcome::Rejected(reason.clone()))?,
                rejection: Some(reason),
            },
        })
    }

    /// The encoded record of the entry with `outcome`.
    pub(crate) fn staged(&self, outcome: &InboxOutcome<&O>) -> Result<StagedRecord> {
        let record = BorrowedRecord {
            applied_at_ms: self.applied_at_ms,
            outcome,
        };
        Ok(StagedRecord {
            id: self.id.clone(),
            applied_at_ms: self.applied_at_ms,
            bytes: rmp_serde::to_vec_named(&record)?,
        })
    }
}

/// The rejection of an entry whose body does not decode as its kind.
pub(crate) fn malformed<O>(err: serde_json::Error) -> Applied<O> {
    Applied::Separately(InboxOutcome::Rejected(format!("malformed body: {err}")))
}

/// What the pass commits for an applied entry.
pub(crate) enum Settled {
    /// The record was committed with the entry's effect.
    Committed,
    /// The record awaits a commit of its own, with the reason of a rejection.
    Separately {
        record: StagedRecord,
        rejection: Option<String>,
    },
}

/// The encoding of an [`InboxRecord`] over a borrowed outcome.
#[derive(Serialize)]
struct BorrowedRecord<'a, T> {
    applied_at_ms: u64,
    outcome: &'a InboxOutcome<T>,
}

/// The encoded record of an entry, as a transaction stages it.
#[derive(Debug, Clone)]
pub(crate) struct StagedRecord {
    pub(crate) id: String,
    pub(crate) applied_at_ms: u64,
    pub(crate) bytes: Vec<u8>,
}
