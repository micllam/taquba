//! The record of an applied inbox entry, and the handle through which the
//! application of a kind stages the record in the transaction of the entry's
//! effect.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::codec::encode;
use crate::error::{Error, FormError};
use crate::keys::QueueName;
use crate::queue::{CancelOutcome, EnqueueResult, SignalOutcome};

/// The outcome of an applied inbox entry, stored in its [`InboxRecord`]: the
/// outcome of the operation of the entry's kind, or a rejection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InboxOutcome {
    /// An [`EnqueueEntry`](crate::EnqueueEntry) was applied, with the result of
    /// the enqueue. An [`EnqueueResult::AlreadyEnqueued`] identifies the job
    /// that held the dedup key in the snapshot of the application.
    Enqueue(EnqueueResult),
    /// A [`CancelEntry`](crate::CancelEntry) was applied, with the outcome of
    /// the cancel.
    Cancel(CancelOutcome),
    /// A [`SignalEntry`](crate::SignalEntry) was applied, with the outcome of
    /// the signal.
    Signal(SignalOutcome),
    /// The entry was rejected for the reason, without an effect.
    Rejected(Rejection),
}

/// The reason of the writer for the rejection of an inbox entry. The display
/// form is the message of the log.
#[derive(Debug, Clone, PartialEq, Eq, Error, Serialize, Deserialize)]
pub enum Rejection {
    /// The kind in the path of the entry is not `enqueue`, `cancel` or
    /// `signal`.
    #[error("unknown inbox kind")]
    UnknownKind,
    /// The object name does not have a ULID before the id.
    #[error("the object name does not have a ULID before the id")]
    Unordered,
    /// The body does not decode as the entry of its kind.
    #[error("malformed body: {detail}")]
    Malformed {
        /// The error of the decoder.
        detail: String,
    },
    /// The queue of the entry is outside
    /// [`InboxOptions::queues`](crate::InboxOptions::queues).
    #[error("the queue is outside the inbox allow-list")]
    QueueNotAccepted,
    /// The job of a cancel or a signal is on another queue.
    #[error("the job is on queue `{job_queue}`")]
    OtherQueue {
        /// The queue of the job.
        job_queue: String,
    },
    /// The [`EnqueueOptions::id_override`](crate::EnqueueOptions::id_override)
    /// of an enqueue is outside its form.
    #[error("invalid id: {reason}")]
    InvalidId {
        /// The rule that the id breaks.
        reason: FormError,
    },
    /// The [`EnqueueOptions::id_override`](crate::EnqueueOptions::id_override)
    /// of an enqueue matches an existing job.
    #[error("duplicate job id")]
    DuplicateJobId,
    /// An enqueue sets
    /// [`EnqueueOptions::signal_wait`](crate::EnqueueOptions::signal_wait)
    /// without a `run_at`.
    #[error("a signal wait requires run_at")]
    SignalWaitWithoutRunAt,
    /// The operation of the entry failed with another permanent error.
    #[error("{message}")]
    Failed {
        /// The message of the error.
        message: String,
    },
}

impl Rejection {
    /// The rejection of an entry whose operation failed with the permanent
    /// error `err`.
    pub(crate) fn of_permanent(err: Error) -> Self {
        match err {
            Error::InvalidId { reason, .. } => Self::InvalidId { reason },
            Error::DuplicateJobId { .. } => Self::DuplicateJobId,
            Error::SignalWaitWithoutRunAt => Self::SignalWaitWithoutRunAt,
            err => Self::Failed {
                message: err.to_string(),
            },
        }
    }
}

/// The record of an applied inbox entry, read through
/// [`QueueView::inbox_record`](crate::QueueView::inbox_record). It is written
/// with its outcome in the transaction of the entry's effect and deleted
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
    id: String,
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
