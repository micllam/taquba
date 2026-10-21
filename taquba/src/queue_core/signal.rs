//! The signal transition of [`QueueCore`]: a signal to the id of a job that
//! waits for one moves the job to pending with the payload.

use tracing::debug;

use crate::error::Result;
use crate::queue::SignalOutcome;
use crate::queue_core::QueueCore;
use crate::queue_core::control::stage_wake;
use crate::txn::{Attempt, Durability, Txn};

/// Stage a signal to the job `job_id` with `payload` at `now` in `txn`: the
/// staged wake of a job that waits for a signal, and no change to any other
/// job.
pub(crate) async fn stage_signal(
    txn: &mut Txn,
    job_id: &str,
    payload: Vec<u8>,
    now: u64,
) -> Result<SignalOutcome> {
    Ok(
        match stage_wake(txn, job_id, Some(payload), now, true).await? {
            Ok(_) => SignalOutcome::Delivered,
            Err(_) => SignalOutcome::NotWaiting,
        },
    )
}

impl QueueCore {
    /// Deliver a signal, as [`Queue::signal`](crate::Queue::signal) documents.
    pub(crate) async fn signal(&self, job_id: &str, payload: Vec<u8>) -> Result<SignalOutcome> {
        let payload = &payload;
        let outcome = self
            .retry(Durability::Awaited, |mut txn| async move {
                let outcome =
                    stage_signal(&mut txn, job_id, payload.clone(), self.now_ms()).await?;
                Ok((txn, Attempt::Commit(outcome)))
            })
            .await?;
        debug!(job_id, outcome = ?outcome, "signal committed");
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;
    use crate::queue::{ClaimOutcome, SignalOutcome};
    use crate::test_util::*;

    const INITIAL: u64 = 1_700_000_000_000;
    const LEASE: Duration = Duration::from_secs(30);

    async fn open(clock: &MockClock) -> Queue {
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..no_backoff_opts()
        };
        Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap()
    }

    /// The options of a job that waits for a signal for an hour.
    fn waiting() -> EnqueueOptions {
        EnqueueOptions::default()
            .run_at(std::time::UNIX_EPOCH + Duration::from_millis(INITIAL + 3_600_000))
            .signal_wait(true)
    }

    #[tokio::test]
    async fn a_signal_wakes_the_waiting_job_with_its_payload_and_ends_the_wait() {
        let clock = MockClock::new(INITIAL);
        let q = open(&clock).await;
        let id = q
            .enqueue_with("jobs", b"wait".to_vec(), waiting())
            .await
            .unwrap();
        assert!(q.view().get_job(&id).await.unwrap().unwrap().signal_wait);

        clock.advance(Duration::from_secs(5));
        assert_eq!(
            q.signal(&id, b"paid".to_vec()).await.unwrap(),
            SignalOutcome::Delivered
        );

        let job = q.claim("jobs", LEASE).await.unwrap().unwrap();
        assert_eq!(job.id, id);
        assert_eq!(job.wake_payload.as_deref(), Some(b"paid".as_slice()));
        assert_eq!(job.woken_at, Some(INITIAL + 5_000));
        assert_eq!(job.run_at, None);
        assert!(!job.signal_wait);
        assert_eq!(
            q.signal(&id, b"again".to_vec()).await.unwrap(),
            SignalOutcome::NotWaiting
        );
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_signal_to_a_job_without_a_wait_changes_nothing() {
        let q = open(&MockClock::new(INITIAL)).await;
        let delayed = q
            .enqueue_with("jobs", b"later".to_vec(), waiting().signal_wait(false))
            .await
            .unwrap();
        let pending = q.enqueue("jobs", b"now".to_vec()).await.unwrap();
        for id in ["absent", delayed.as_str(), pending.as_str()] {
            assert_eq!(
                q.signal(id, b"stray".to_vec()).await.unwrap(),
                SignalOutcome::NotWaiting,
                "{id}"
            );
        }
        let delayed = q.view().get_job(&delayed).await.unwrap().unwrap();
        assert_eq!(delayed.status, JobStatus::Scheduled);
        assert_eq!(delayed.woken_at, None);
        let pending = q.view().get_job(&pending).await.unwrap().unwrap();
        assert_eq!(pending.wake_payload, None);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_signal_wait_requires_run_at() {
        let q = open(&MockClock::new(INITIAL)).await;
        let err = q
            .enqueue_with(
                "jobs",
                b"x".to_vec(),
                EnqueueOptions::default().signal_wait(true),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::SignalWaitWithoutRunAt));
        assert_eq!(q.view().stats("jobs").await.unwrap().pending, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_waiter_with_a_past_run_at_is_pending_without_a_wait() {
        let q = open(&MockClock::new(INITIAL)).await;
        let past = EnqueueOptions::default()
            .run_at(std::time::UNIX_EPOCH + Duration::from_millis(INITIAL - 1))
            .signal_wait(true);
        let id = q.enqueue_with("jobs", b"x".to_vec(), past).await.unwrap();
        let job = q.view().get_job(&id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Pending);
        assert!(!job.signal_wait);
        assert_eq!(job.woken_at, None);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn every_transition_out_of_the_scheduled_state_ends_the_wait() {
        let clock = MockClock::new(INITIAL);
        let q = open(&clock).await;

        let promoted = q
            .enqueue_with("jobs", b"a".to_vec(), waiting())
            .await
            .unwrap();
        let cancelled = q
            .enqueue_with("jobs", b"b".to_vec(), waiting())
            .await
            .unwrap();
        let claimed = q
            .enqueue_with("jobs", b"c".to_vec(), waiting())
            .await
            .unwrap();
        let woken = q
            .enqueue_with("jobs", b"d".to_vec(), waiting())
            .await
            .unwrap();

        assert_eq!(q.cancel(&cancelled).await.unwrap(), CancelOutcome::Removed);
        assert!(matches!(
            q.claim_by_id(&claimed, LEASE).await.unwrap(),
            ClaimOutcome::Claimed(_)
        ));
        assert_eq!(
            q.wake_scheduled(&woken, None).await.unwrap(),
            WakeOutcome::Woken
        );
        clock.advance(Duration::from_secs(3_601));
        q.promote_scheduled_now().await.unwrap();

        for id in [&promoted, &cancelled, &claimed, &woken] {
            if let Some(job) = q.view().get_job(id).await.unwrap() {
                assert!(!job.signal_wait, "{id}");
            }
            assert_eq!(
                q.signal(id, b"late".to_vec()).await.unwrap(),
                SignalOutcome::NotWaiting,
                "{id}"
            );
        }
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_retried_waiter_rejects_a_signal_in_its_backoff() {
        let clock = MockClock::new(INITIAL);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            default_queue_config: QueueConfig {
                retry_backoff_base: Duration::from_secs(10),
                retry_backoff_max: Duration::from_secs(10),
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let id = q
            .enqueue_with("jobs", b"a".to_vec(), waiting())
            .await
            .unwrap();
        q.signal(&id, b"go".to_vec()).await.unwrap();
        let job = q.claim("jobs", LEASE).await.unwrap().unwrap();
        q.nack(&job, "transient").await.unwrap();

        let retried = q.view().get_job(&id).await.unwrap().unwrap();
        assert_eq!(retried.status, JobStatus::Scheduled);
        assert!(!retried.signal_wait);
        assert_eq!(
            q.signal(&id, b"duplicate".to_vec()).await.unwrap(),
            SignalOutcome::NotWaiting
        );
        clock.advance(Duration::from_secs(11));
        q.promote_scheduled_now().await.unwrap();
        let job = q.claim("jobs", LEASE).await.unwrap().unwrap();
        assert_eq!(job.wake_payload.as_deref(), Some(b"go".as_slice()));
        q.close().await.unwrap();
    }
}
