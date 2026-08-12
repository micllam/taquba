use bytes::Bytes;
use serde::{Deserialize, Serialize};
use slatedb::{DbTransaction, MergeOperator, MergeOperatorError};

use crate::error::Result;
use crate::job::JobStatus;
use crate::keys::{KeyTag, stats_key};

/// Map a [`JobStatus`] to the on-disk metric name used for its counter.
pub(crate) fn metric_name(status: JobStatus) -> &'static str {
    match status {
        JobStatus::Pending => "pending",
        JobStatus::Claimed => "claimed",
        JobStatus::Done => "done",
        JobStatus::Dead => "dead",
        JobStatus::Scheduled => "scheduled",
    }
}

/// Merge operator covering every merging key space, dispatching on the
/// key's tag byte: stats counters ([`KeyTag::Stats`]) accumulate i64
/// deltas in little-endian encoding, and attempt history
/// ([`KeyTag::AttemptHistory`]) appends serialized entries by
/// concatenation. Both avoid read-modify-write races on their keys.
pub struct QueueMergeOperator;

impl MergeOperator for QueueMergeOperator {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operand: Bytes,
    ) -> std::result::Result<Bytes, MergeOperatorError> {
        self.merge_batch(key, existing_value, &[operand])
    }

    fn merge_batch(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> std::result::Result<Bytes, MergeOperatorError> {
        match key.first().copied() {
            Some(tag) if tag == KeyTag::Stats.id() => merge_counters(existing_value, operands),
            Some(tag) if tag == KeyTag::AttemptHistory.id() => {
                Ok(merge_append(existing_value, operands))
            }
            _ => Err(MergeOperatorError::Callback {
                message: "merge on a non-merging key space".to_string(),
            }),
        }
    }
}

fn merge_counters(
    existing_value: Option<Bytes>,
    operands: &[Bytes],
) -> std::result::Result<Bytes, MergeOperatorError> {
    let mut total = existing_value
        .map(|v| read_i64_le(&v))
        .transpose()
        .map_err(|_| MergeOperatorError::Callback {
            message: "invalid 8-byte i64 operand".to_string(),
        })?
        .unwrap_or(0i64);
    for op in operands {
        total += read_i64_le(op).map_err(|_| MergeOperatorError::Callback {
            message: "invalid 8-byte i64 operand".to_string(),
        })?;
    }
    Ok(Bytes::copy_from_slice(&total.to_le_bytes()))
}

fn merge_append(existing_value: Option<Bytes>, operands: &[Bytes]) -> Bytes {
    let existing = existing_value.as_deref().unwrap_or_default();
    let total = existing.len() + operands.iter().map(|o| o.len()).sum::<usize>();
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(existing);
    for op in operands {
        out.extend_from_slice(op);
    }
    Bytes::from(out)
}

fn read_i64_le(bytes: &[u8]) -> std::result::Result<i64, ()> {
    bytes.try_into().map(i64::from_le_bytes).map_err(|_| ())
}

/// Apply stat deltas for a single operation within an existing transaction.
pub(crate) fn update_stats(
    txn: &DbTransaction,
    queue: &str,
    deltas: &[(JobStatus, i64)],
) -> Result<()> {
    for (status, delta) in deltas {
        if *delta != 0 {
            let key = stats_key(queue, metric_name(*status));
            txn.merge(&key, (*delta).to_le_bytes())?;
            // Counter merges are commutative, so two transactions
            // merging the same stats key do not actually conflict.
            // Without this, every job-state transition on a queue
            // contends on the same handful of stats keys and
            // transaction-conflict retries dominate claim latency
            // under concurrency.
            txn.unmark_write([key.as_slice()])?;
        }
    }
    Ok(())
}

/// A snapshot of job counts for a single queue.
///
/// Returned by [`Queue::stats`](crate::Queue::stats). Counters are kept
/// transactionally consistent with job-state writes via SlateDB's merge
/// operator. Live-state counters reflect the current size of each key space.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueStats {
    /// Name of the queue this snapshot describes.
    pub queue: String,
    /// Jobs waiting to be claimed right now.
    pub pending: i64,
    /// Jobs currently held by a worker under a lease.
    pub claimed: i64,
    /// Jobs that completed successfully (cumulative throughput, not
    /// decremented by retention sweeps).
    pub done: i64,
    /// Jobs currently in the dead-letter set. Decremented on
    /// [`Queue::requeue_dead_job`](crate::Queue::requeue_dead_job) and on
    /// retention sweeps.
    pub dead: i64,
    /// Jobs waiting for their `run_at` time before becoming pending. Includes
    /// jobs in retry-backoff between a [`Queue::nack`](crate::Queue::nack)
    /// and the scheduler's next promotion sweep.
    pub scheduled: i64,
}
