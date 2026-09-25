use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use tracing::debug;

use crate::background::Periodic;
use crate::error::Result;
use crate::job::{JobRecord, JobStatus};
use crate::keys::{KeyTag, parse_key_timestamp, tag_prefix};
use crate::queue_core::QueueCore;
use crate::txn::{Attempt, Durability, retry, stage_to_pending};

pub(crate) struct Scheduler {
    core: Arc<QueueCore>,
}

impl Scheduler {
    pub(crate) fn new(core: Arc<QueueCore>) -> Self {
        Self { core }
    }
}

impl Periodic for Scheduler {
    const NAME: &'static str = "scheduled job promoter";

    async fn step(&self) -> Result<()> {
        self.core.promote_due_jobs().await
    }
}

impl QueueCore {
    /// Scan the scheduled key space from its bound and move any job
    /// whose `run_at` has passed into the pending key space so workers
    /// can claim it. Without a job that can be due, the call does not
    /// read.
    pub(crate) async fn promote_due_jobs(&self) -> Result<()> {
        let now = self.now_ms();
        let Some(mut scan) = self.scheduled_bound.begin(now, 0) else {
            return Ok(());
        };
        let mut due_keys = Vec::new();

        // A scheduled key leads with `run_at`, and the range is within
        // the prefix, so the scan starts at the bound and the first key
        // with a `run_at` in the future ends it.
        let start = Bytes::copy_from_slice(&scan.from().to_be_bytes());
        let mut iter = self
            .db
            .scan_prefix(
                tag_prefix(KeyTag::Scheduled),
                (Bound::Included(start), Bound::Unbounded),
            )
            .await?;
        while let Some(kv) = iter.next().await? {
            let Some(run_at) = parse_key_timestamp(&kv.key, KeyTag::Scheduled) else {
                continue;
            };
            if !scan.due(run_at) {
                scan.retain(run_at);
                break;
            }
            due_keys.push(kv.key.clone());
        }
        drop(iter);

        for key_bytes in due_keys {
            self.promote_job(&key_bytes).await?;
        }
        scan.complete();

        Ok(())
    }

    async fn promote_job(&self, scheduled_key_bytes: &[u8]) -> Result<()> {
        // Promotion commits do not await WAL durability. Each due job is
        // promoted in its own transaction, so awaiting the flush
        // serialises the sweep at one job per flush interval. A commit
        // lost in a crash leaves the scheduled key in place with its
        // `run_at` still in the past, and the next tick re-promotes it:
        // the rewrite is idempotent. Any later durable commit flushes
        // preceding WAL entries, so a job's post-promotion history is
        // never durable without the promotion itself.
        let promoted = retry(&self.db, Durability::Deferred, |txn| async move {
            let Some(raw) = txn.get(scheduled_key_bytes).await? else {
                // Already promoted by a concurrent call; nothing to do.
                txn.rollback();
                return Ok(Attempt::Abort(None));
            };
            let mut job = JobRecord::decode(scheduled_key_bytes, &raw)?;
            txn.delete(scheduled_key_bytes)?;
            let pending = stage_to_pending(&txn, &mut job, JobStatus::Scheduled)?;
            Ok(Attempt::Commit(txn, Some((job, pending))))
        })
        .await?;
        if let Some((job, pending)) = promoted {
            self.claim_cursor.note_pending_insert(&job.queue, &pending);
            debug!(
                queue = %job.queue,
                job_id = %job.id,
                "scheduled job promoted to pending"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::*;

    #[tokio::test]
    async fn a_promotion_pass_over_an_empty_key_space_leaves_no_read_for_the_next() {
        let initial = 1_700_000_000_000u64;
        let clock = MockClock::new(initial);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.promote_scheduled_now().await.unwrap();
        clock.advance(Duration::from_secs(3_600));
        assert!(q.core.scheduled_bound.begin(clock.now_ms(), 0).is_none());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn an_enqueue_with_run_at_after_an_empty_pass_is_promoted_when_due() {
        let initial = 1_700_000_000_000u64;
        let clock = MockClock::new(initial);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        q.promote_scheduled_now().await.unwrap();

        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial + 100);
        q.enqueue_with(
            "jobs",
            b"soon".to_vec(),
            EnqueueOptions {
                run_at: Some(run_at),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        clock.advance(Duration::from_millis(100));
        q.promote_scheduled_now().await.unwrap();

        let s = q.view().stats("jobs").await.unwrap();
        assert_eq!((s.scheduled, s.pending), (0, 1));
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_nack_with_backoff_after_an_empty_pass_is_promoted_when_due() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            default_queue_config: QueueConfig {
                retry_backoff_base: Duration::from_millis(10),
                retry_backoff_max: Duration::from_millis(10),
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        q.promote_scheduled_now().await.unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.nack(&job, "boom").await.unwrap();
        clock.advance(Duration::from_millis(10));
        q.promote_scheduled_now().await.unwrap();

        let s = q.view().stats("work").await.unwrap();
        assert_eq!((s.scheduled, s.pending), (0, 1));
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_enqueue_at_past_is_immediately_pending() {
        let initial = 1_700_000_000_000u64;
        let opts = OpenOptions {
            clock: Arc::new(MockClock::new(initial)),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial - 1_000);
        q.enqueue_with(
            "jobs",
            b"past".to_vec(),
            EnqueueOptions {
                run_at: Some(run_at),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // A past run_at goes straight to pending.
        let job = q.claim("jobs", Duration::from_secs(30)).await.unwrap();
        assert!(job.is_some());

        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_promote_scheduled_now() {
        let initial = 1_700_000_000_000u64;
        let clock = MockClock::new(initial);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial + 100);
        let id = q
            .enqueue_with(
                "jobs",
                b"soon".to_vec(),
                EnqueueOptions {
                    run_at: Some(run_at),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Not yet promoted.
        let s = q.view().stats("jobs").await.unwrap();
        assert_eq!(s.scheduled, 1);
        assert_eq!(s.pending, 0);
        assert!(
            q.claim("jobs", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        // Advance past `run_at` and trigger a manual promotion.
        clock.advance(Duration::from_millis(200));
        q.promote_scheduled_now().await.unwrap();

        let s = q.view().stats("jobs").await.unwrap();
        assert_eq!(s.scheduled, 0);
        assert_eq!(s.pending, 1);

        let job = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, id);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wake_scheduled_promotes_before_run_at() {
        let initial = 1_700_000_000_000u64;
        let clock = MockClock::new(initial);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial + 60_000);
        let id = q
            .enqueue_with(
                "jobs",
                b"waiting".to_vec(),
                EnqueueOptions {
                    run_at: Some(run_at),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(
            q.claim("jobs", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        let outcome = q
            .wake_scheduled(&id, Some(b"signal".to_vec()))
            .await
            .unwrap();
        assert_eq!(outcome, WakeOutcome::Woken);

        let s = q.view().stats("jobs").await.unwrap();
        assert_eq!(s.scheduled, 0);
        assert_eq!(s.pending, 1);

        let job = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, id);
        assert_eq!(job.wake_payload.as_deref(), Some(b"signal".as_slice()));
        assert_eq!(job.woken_at, Some(initial));
        assert!(job.run_at.is_none());

        // A wake without a payload leaves `wake_payload` unset.
        let silent = q
            .enqueue_with(
                "jobs",
                b"waiting".to_vec(),
                EnqueueOptions {
                    run_at: Some(run_at),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            q.wake_scheduled(&silent, None).await.unwrap(),
            WakeOutcome::Woken
        );
        let job = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, silent);
        assert!(job.wake_payload.is_none());
        assert_eq!(job.woken_at, Some(initial));

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wake_scheduled_non_scheduled_outcomes() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let id = q.enqueue("jobs", b"p".to_vec()).await.unwrap();
        assert_eq!(
            q.wake_scheduled(&id, None).await.unwrap(),
            WakeOutcome::NotScheduled
        );

        let job = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            q.wake_scheduled(&job.id, None).await.unwrap(),
            WakeOutcome::NotScheduled
        );

        assert_eq!(
            q.wake_scheduled("01ARZ3NDEKTSV4RRFFQ69G5FAV", None)
                .await
                .unwrap(),
            WakeOutcome::NotFound
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wake_scheduled_after_cancel_is_not_found() {
        let initial = 1_700_000_000_000u64;
        let clock = MockClock::new(initial);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial + 60_000);
        let id = q
            .enqueue_with(
                "jobs",
                b"waiting".to_vec(),
                EnqueueOptions {
                    run_at: Some(run_at),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Removed);
        assert_eq!(
            q.wake_scheduled(&id, None).await.unwrap(),
            WakeOutcome::NotFound
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wake_scheduled_after_promotion_is_not_scheduled() {
        let initial = 1_700_000_000_000u64;
        let clock = MockClock::new(initial);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial + 100);
        let id = q
            .enqueue_with(
                "jobs",
                b"soon".to_vec(),
                EnqueueOptions {
                    run_at: Some(run_at),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        clock.advance(Duration::from_millis(200));
        q.promote_scheduled_now().await.unwrap();

        assert_eq!(
            q.wake_scheduled(&id, Some(b"late".to_vec())).await.unwrap(),
            WakeOutcome::NotScheduled
        );

        let job = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert!(job.wake_payload.is_none());
        assert!(job.woken_at.is_none());

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wake_payload_persists_across_redelivery() {
        let initial = 1_700_000_000_000u64;
        let clock = MockClock::new(initial);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial + 60_000);
        let id = q
            .enqueue_with(
                "jobs",
                b"waiting".to_vec(),
                EnqueueOptions {
                    run_at: Some(run_at),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(
            q.wake_scheduled(&id, Some(b"signal".to_vec()))
                .await
                .unwrap(),
            WakeOutcome::Woken
        );

        let job = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.nack(&job, "worker failed").await.unwrap();

        // The retry backoff moves the job back to `scheduled`; promote it
        // and verify the redelivered record still carries the wake payload.
        clock.advance(Duration::from_secs(5));
        q.promote_scheduled_now().await.unwrap();

        let job = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.id, id);
        assert_eq!(job.attempts, 2);
        assert_eq!(job.wake_payload.as_deref(), Some(b"signal".as_slice()));
        assert_eq!(job.woken_at, Some(initial));

        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_scheduled_job_preserves_priority() {
        let initial = 1_700_000_000_000u64;
        let clock = MockClock::new(initial);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial + 1);
        q.enqueue_with(
            "jobs",
            b"normal".to_vec(),
            EnqueueOptions {
                run_at: Some(run_at),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        // Enqueue a high-priority immediate job after the scheduled one.
        q.enqueue_with(
            "jobs",
            b"high".to_vec(),
            EnqueueOptions {
                priority: Some(PRIORITY_HIGH),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        clock.advance(Duration::from_millis(5));
        q.promote_scheduled_now().await.unwrap();

        // High-priority should come first even though scheduled was enqueued first.
        let j1 = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(j1.payload, b"high");

        let j2 = q
            .claim("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(j2.payload, b"normal");

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_nack_backoff_promoted_after_run_at() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            default_queue_config: QueueConfig {
                retry_backoff_base: Duration::from_millis(10),
                retry_backoff_max: Duration::from_millis(10),
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue_with(
            "work",
            b"payload".to_vec(),
            EnqueueOptions {
                max_attempts: Some(5),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let id = job.id.clone();
        q.nack(&job, "boom").await.unwrap();

        // The job waits in the scheduled key space until the backoff
        // elapses.
        let s = q.view().stats("work").await.unwrap();
        assert_eq!(s.pending, 0);
        assert_eq!(s.claimed, 0);
        assert_eq!(s.scheduled, 1);
        assert!(
            q.claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        // Advance past the backoff and trigger promotion.
        clock.advance(Duration::from_millis(20));
        q.promote_scheduled_now().await.unwrap();

        let retried = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retried.id, id);
        assert_eq!(retried.attempts, 2);
        assert_eq!(retried.last_error.as_deref(), Some("boom"));

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn scheduled_job_offloads_and_materializes_after_promotion() {
        let initial = 1_700_000_000_000;
        let clock = MockClock::new(initial);
        let store = make_store();
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            payload_offload_threshold: Some(64),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(store.clone(), "test", opts)
            .await
            .unwrap();

        let payload = vec![7u8; 256];
        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial + 60_000);
        let id = q
            .enqueue_with(
                "work",
                payload.clone(),
                EnqueueOptions {
                    run_at: Some(run_at),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .unwrap();

        // The payload offloads at enqueue even though the record lands
        // in the scheduled key space.
        assert_eq!(object_count(&store, "test-payloads").await, 1);
        let scheduled = q.view().get_job(&id).await.unwrap().unwrap();
        assert_eq!(scheduled.status, JobStatus::Scheduled);
        assert_eq!(scheduled.payload, payload);

        clock.advance(Duration::from_millis(60_001));
        q.promote_scheduled_now().await.unwrap();

        // Promotion moves the record without touching the object; the
        // claim materializes the payload.
        assert_eq!(object_count(&store, "test-payloads").await, 1);
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.payload, payload);
        q.ack(&job).await.unwrap();
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }
}
