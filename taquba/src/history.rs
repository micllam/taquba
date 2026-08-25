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
    /// [`AttemptOutcome::Completed`], [`AttemptOutcome::LeaseExpired`],
    /// [`AttemptOutcome::Interrupted`] and [`AttemptOutcome::Requeued`].
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
    /// [`Queue::nack`](crate::Queue::nack) at the attempt limit or an
    /// expired or interrupted claim at the attempt limit.
    DeadLettered,
    /// The claim's lease expired and the reaper re-queued the job. The
    /// worker's own outcome for this attempt is unknown.
    LeaseExpired,
    /// The process holding the claim exited without settling it; the
    /// job was re-queued when the store was next opened. The worker's
    /// own outcome for this attempt is unknown.
    Interrupted,
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

    use crate::test_util::*;

    #[tokio::test]
    async fn attempt_history_records_retries_then_completion() {
        let opts = OpenOptions {
            default_queue_config: QueueConfig {
                keep_done_jobs: Some(Duration::from_secs(3600)),
                retry_backoff_base: Duration::ZERO,
                retry_backoff_max: Duration::ZERO,
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let id = q.enqueue("work", b"x".to_vec()).await.unwrap();
        let lease = Duration::from_secs(30);

        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.nack(&job, "timeout").await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.nack(&job, "connection reset").await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.ack(&job).await.unwrap();

        let history = q.attempt_history(&id).await.unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].attempt, 1);
        assert_eq!(history[0].outcome, AttemptOutcome::Retried);
        assert_eq!(history[0].error.as_deref(), Some("timeout"));
        assert!(history[0].claimed_at.is_some());
        assert_eq!(history[1].attempt, 2);
        assert_eq!(history[1].error.as_deref(), Some("connection reset"));
        assert_eq!(history[2].attempt, 3);
        assert_eq!(history[2].outcome, AttemptOutcome::Completed);
        assert_eq!(history[2].error, None);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn attempt_history_removed_when_ack_expunges_the_record() {
        let q = Queue::open_with_options(make_store(), "test", no_backoff_opts())
            .await
            .unwrap();
        let id = q.enqueue("work", b"x".to_vec()).await.unwrap();
        let lease = Duration::from_secs(30);

        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.nack(&job, "failed").await.unwrap();
        assert_eq!(q.attempt_history(&id).await.unwrap().len(), 1);

        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.ack(&job).await.unwrap();
        assert!(q.attempt_history(&id).await.unwrap().is_empty());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn attempt_history_records_dead_letter_on_nack_at_attempt_limit() {
        let opts = OpenOptions {
            default_queue_config: QueueConfig {
                max_attempts: 1,
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let id = q.enqueue("work", b"x".to_vec()).await.unwrap();

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.nack(&job, "failed").await.unwrap();
        assert_eq!(
            q.get_job(&id).await.unwrap().unwrap().status,
            JobStatus::Dead
        );

        let history = q.attempt_history(&id).await.unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].attempt, 1);
        assert_eq!(history[0].outcome, AttemptOutcome::DeadLettered);
        assert_eq!(history[0].error.as_deref(), Some("failed"));
        assert!(history[0].claimed_at.is_some());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn attempt_history_survives_requeue_with_marker() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let id = q.enqueue("work", b"x".to_vec()).await.unwrap();
        let lease = Duration::from_secs(30);

        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.dead_letter(&job, "unroutable").await.unwrap();

        let dead = q.get_job(&id).await.unwrap().unwrap();
        q.requeue_dead_job(dead).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.dead_letter(&job, "still unroutable").await.unwrap();

        let history = q.attempt_history(&id).await.unwrap();
        let outcomes: Vec<_> = history.iter().map(|a| a.outcome).collect();
        assert_eq!(
            outcomes,
            vec![
                AttemptOutcome::DeadLettered,
                AttemptOutcome::Requeued,
                AttemptOutcome::DeadLettered,
            ]
        );
        assert_eq!(history[0].error.as_deref(), Some("unroutable"));
        assert_eq!(history[1].attempt, 0);
        assert_eq!(history[1].claimed_at, None);
        assert_eq!(history[2].attempt, 1);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_of_scheduled_job_removes_attempt_history() {
        let opts = OpenOptions {
            clock: Arc::new(MockClock::new(1_700_000_000_000)),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let id = q.enqueue("work", b"x".to_vec()).await.unwrap();

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        // Default backoff is non-zero, so the nacked job waits in
        // `Scheduled` with one history entry.
        q.nack(&job, "failed").await.unwrap();
        assert_eq!(q.attempt_history(&id).await.unwrap().len(), 1);

        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Removed);
        assert!(q.attempt_history(&id).await.unwrap().is_empty());
        q.close().await.unwrap();
    }
}
