//! Per-job attempt history.
//!
//! Each settlement of a claim (ack, nack, dead-letter, lease expiry) and
//! each operator revival appends one [`JobAttempt`] entry to the job's
//! history key ([`crate::keys::KeyTag::AttemptHistory`]) through the
//! merge operator, so each event adds exactly one write to its
//! settlement transaction and the accumulated history is never read or
//! rewritten there. Entries are individually serialized with
//! MessagePack and the merged value is their plain concatenation;
//! MessagePack values are self-delimiting, so decoding reads the
//! buffer entry by entry.
//!
//! The history is retained exactly as long as the job is findable via
//! [`Queue::get_job`](crate::Queue::get_job): the transaction that
//! removes the job's last record (ack without retention, cancel of a
//! pending or scheduled job, the done and dead retention sweeps) also
//! deletes the history key. [`Queue::requeue_dead_job`](crate::Queue::requeue_dead_job)
//! keeps the history and appends a [`AttemptOutcome::Requeued`] marker,
//! so entries recorded before the revival remain distinguishable after
//! the attempt counter resets.

use serde::{Deserialize, Serialize};
use slatedb::DbTransaction;

use crate::error::Result;
use crate::keys::attempt_history_key;

/// One recorded event in a job's delivery history.
///
/// Returned by [`Queue::attempt_history`](crate::Queue::attempt_history)
/// in write order. All timestamps are wall-clock milliseconds since the
/// UNIX epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobAttempt {
    /// The delivery attempt this event settles. `0` for
    /// [`AttemptOutcome::Requeued`], which is a lifecycle marker rather
    /// than an attempt.
    pub attempt: u32,
    /// When the attempt's claim was taken. `None` for
    /// [`AttemptOutcome::Requeued`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_at: Option<u64>,
    /// When the event was recorded.
    pub recorded_at: u64,
    /// How the attempt ended.
    pub outcome: AttemptOutcome,
    /// The error reported for a failed attempt. `None` for
    /// [`AttemptOutcome::Completed`], [`AttemptOutcome::LeaseExpired`]
    /// and [`AttemptOutcome::Requeued`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// How a recorded [`JobAttempt`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttemptOutcome {
    /// The attempt acked the job.
    Completed,
    /// The attempt failed via [`Queue::nack`](crate::Queue::nack) and
    /// the job was re-queued or scheduled for a retry.
    Retried,
    /// The attempt failed terminally: an explicit
    /// [`Queue::dead_letter`](crate::Queue::dead_letter), a
    /// [`Queue::nack`](crate::Queue::nack) at the attempt limit or a
    /// lease expiry at the attempt limit.
    DeadLettered,
    /// The claim's lease expired and the reaper re-queued the job. The
    /// worker's own outcome for this attempt is unknown.
    LeaseExpired,
    /// An operator revived the dead job via
    /// [`Queue::requeue_dead_job`](crate::Queue::requeue_dead_job),
    /// resetting its attempt count.
    Requeued,
}

/// Stage one history entry in `txn` as a merge on the job's history key.
pub(crate) fn append_attempt(txn: &DbTransaction, id: &str, entry: &JobAttempt) -> Result<()> {
    let bytes = rmp_serde::to_vec_named(entry)?;
    txn.merge(attempt_history_key(id), bytes)?;
    Ok(())
}

/// Decode a merged history value into its entries, in write order.
pub(crate) fn decode_history(bytes: &[u8]) -> Result<Vec<JobAttempt>> {
    let mut rest = bytes;
    let mut entries = Vec::new();
    while !rest.is_empty() {
        let mut de = rmp_serde::Deserializer::new(&mut rest);
        entries.push(JobAttempt::deserialize(&mut de)?);
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_walks_concatenated_entries_in_order() {
        let a = JobAttempt {
            attempt: 1,
            claimed_at: Some(10),
            recorded_at: 20,
            outcome: AttemptOutcome::Retried,
            error: Some("timeout".to_string()),
        };
        let b = JobAttempt {
            attempt: 2,
            claimed_at: Some(30),
            recorded_at: 40,
            outcome: AttemptOutcome::Completed,
            error: None,
        };
        let mut buf = rmp_serde::to_vec_named(&a).unwrap();
        buf.extend(rmp_serde::to_vec_named(&b).unwrap());
        assert_eq!(decode_history(&buf).unwrap(), vec![a, b]);
    }

    #[test]
    fn decode_of_empty_value_is_empty() {
        assert!(decode_history(&[]).unwrap().is_empty());
    }
}
