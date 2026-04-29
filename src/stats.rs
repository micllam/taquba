use bytes::Bytes;
use slatedb::{Db, DbTransaction, MergeOperator, MergeOperatorError};

use crate::error::{Error, Result};
use crate::job::JobStatus;

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

pub(crate) fn stats_key(queue: &str, metric: &str) -> String {
    format!("stats:{}:{}", queue, metric)
}

/// Merge operator that accumulates i64 deltas using little-endian encoding.
///
/// Used to maintain per-queue job counters without read-modify-write races.
pub struct CounterMergeOperator;

impl MergeOperator for CounterMergeOperator {
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
        _key: &Bytes,
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
            txn.merge(
                stats_key(queue, metric_name(*status)).as_bytes(),
                (*delta).to_le_bytes(),
            )?;
        }
    }
    Ok(())
}

/// A snapshot of job counts for a single queue.
///
/// Returned by [`Queue::stats`](crate::Queue::stats). Counters are kept
/// transactionally consistent with job-state writes via SlateDB's merge
/// operator. Live-state counters reflect the current size of each key space.
#[derive(Debug, Clone, PartialEq)]
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

pub(crate) async fn read_stats(db: &Db, queue: &str) -> Result<QueueStats> {
    Ok(QueueStats {
        queue: queue.to_string(),
        pending: count_for(db, queue, JobStatus::Pending).await?,
        claimed: count_for(db, queue, JobStatus::Claimed).await?,
        done: count_for(db, queue, JobStatus::Done).await?,
        dead: count_for(db, queue, JobStatus::Dead).await?,
        scheduled: count_for(db, queue, JobStatus::Scheduled).await?,
    })
}

async fn count_for(db: &Db, queue: &str, status: JobStatus) -> Result<i64> {
    let key = stats_key(queue, metric_name(status));
    match db.get(key.as_bytes()).await? {
        None => Ok(0),
        Some(bytes) => read_i64_le(&bytes).map_err(|_| Error::InvalidState),
    }
}
