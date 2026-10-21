//! The signal transitions of [`QueueCore`]: the registration of a waiter at
//! enqueue, the delivery or buffering of a signal and the discard of a buffer.

use tracing::debug;

use crate::error::{Error, Result};
use crate::job::{JobRecord, JobStatus};
use crate::keys::{signal_buffer_key, signal_waiter_key};
use crate::queue_core::QueueCore;
use crate::signal::SignalOutcome;
use crate::txn::{Attempt, Durability, Txn, get_indexed_job};

/// Stage a signal to `key` with `payload` at `now` in `txn`: a scheduled job
/// waiting at the key moves to pending with the payload on its record, and
/// otherwise the payload is buffered at the key. The waiter index entry is
/// written by a registration and deleted here on both paths, so a signal and a
/// concurrent registration at one key conflict and one of the two retries
/// against the other's commit.
pub(crate) async fn stage_signal(
    txn: &mut Txn,
    key: &str,
    payload: Vec<u8>,
    now: u64,
) -> Result<SignalOutcome> {
    let waiter_key = signal_waiter_key(key);
    if let Some(id) = txn.get(&waiter_key).await? {
        let id = String::from_utf8(id.to_vec()).map_err(|_| Error::InvalidState)?;
        if let Some((_, current_key, mut job)) = get_indexed_job(txn, &id).await?
            && job.status == JobStatus::Scheduled
            && job.signal_key.as_deref() == Some(key)
        {
            job.woken_at = Some(now);
            job.wake_payload = Some(payload);
            txn.stage_to_pending(&current_key, &mut job)?;
            return Ok(SignalOutcome::Delivered);
        }
    }
    txn.put(signal_buffer_key(key), payload)?;
    txn.delete(&waiter_key)?;
    Ok(SignalOutcome::Buffered)
}

impl QueueCore {
    /// Register `job`, a new record with the signal key `signal_key`, in `txn`.
    /// A scheduled job joins the waiter index at the key, and a signal buffered
    /// at the key is consumed instead: the job is written to pending with the
    /// payload on its record and the key cleared. A live waiter at the key
    /// fails the registration.
    pub(crate) async fn stage_signal_wait(
        &self,
        txn: &mut Txn,
        job: &mut JobRecord,
        signal_key: &str,
    ) -> Result<()> {
        let waiter_key = signal_waiter_key(signal_key);
        if job.status == JobStatus::Scheduled
            && let Some(existing) = txn.get(&waiter_key).await?
        {
            let existing = String::from_utf8(existing.to_vec()).map_err(|_| Error::InvalidState)?;
            if let Some((_, _, waiter)) = get_indexed_job(txn, &existing).await?
                && waiter.status == JobStatus::Scheduled
            {
                return Err(Error::SignalWaiterExists {
                    key: signal_key.to_string(),
                });
            }
        }
        let buffer_key = signal_buffer_key(signal_key);
        if let Some(buffered) = txn.get(&buffer_key).await? {
            txn.delete(&buffer_key)?;
            job.status = JobStatus::Pending;
            job.run_at = None;
            job.woken_at = Some(self.now_ms());
            job.wake_payload = Some(buffered.to_vec());
            job.signal_key = None;
            return Ok(());
        }
        match job.status {
            JobStatus::Scheduled => txn.put(&waiter_key, job.id.as_bytes())?,
            _ => job.signal_key = None,
        }
        Ok(())
    }

    /// Deliver or buffer a signal, as [`Queue::signal`](crate::Queue::signal)
    /// documents.
    pub(crate) async fn signal(&self, key: &str, payload: Vec<u8>) -> Result<SignalOutcome> {
        let payload = &payload;
        let outcome = self
            .retry(Durability::Awaited, |mut txn| async move {
                let outcome = stage_signal(&mut txn, key, payload.clone(), self.now_ms()).await?;
                Ok((txn, Attempt::Commit(outcome)))
            })
            .await?;
        debug!(signal_key = key, outcome = ?outcome, "signal committed");
        Ok(outcome)
    }

    /// Discard the signal buffered at `key`, as
    /// [`Queue::clear_signal`](crate::Queue::clear_signal) documents.
    pub(crate) async fn clear_signal(&self, key: &str) -> Result<bool> {
        let buffer_key = &signal_buffer_key(key);
        self.retry(Durability::Awaited, |txn| async move {
            if txn.get(buffer_key).await?.is_none() {
                return Ok((txn, Attempt::Abort(false)));
            }
            txn.delete(buffer_key)?;
            Ok((txn, Attempt::Commit(true)))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::effects::EnqueueRequest;
    use crate::error::Error;
    use crate::queue::ClaimOutcome;
    use crate::signal::SignalOutcome;
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

    /// The options of a job that waits at `key` for an hour.
    fn waiting(key: &str) -> EnqueueOptions {
        EnqueueOptions::default()
            .run_at(std::time::UNIX_EPOCH + Duration::from_millis(INITIAL + 3_600_000))
            .signal_key(key.to_string())
    }

    #[tokio::test]
    async fn a_signal_wakes_the_waiter_with_its_payload_and_ends_the_wait() {
        let clock = MockClock::new(INITIAL);
        let q = open(&clock).await;
        let id = q
            .enqueue_with("jobs", b"wait".to_vec(), waiting("order-1"))
            .await
            .unwrap();
        assert_eq!(
            q.view().signal_waiter("order-1").await.unwrap().as_deref(),
            Some(id.as_str())
        );
        let waiter = q.view().get_job(&id).await.unwrap().unwrap();
        assert_eq!(waiter.signal_key.as_deref(), Some("order-1"));

        clock.advance(Duration::from_secs(5));
        assert_eq!(
            q.signal("order-1", b"paid".to_vec()).await.unwrap(),
            SignalOutcome::Delivered
        );

        let job = q.claim("jobs", LEASE).await.unwrap().unwrap();
        assert_eq!(job.id, id);
        assert_eq!(job.wake_payload.as_deref(), Some(b"paid".as_slice()));
        assert_eq!(job.woken_at, Some(INITIAL + 5_000));
        assert_eq!(job.run_at, None);
        assert_eq!(job.signal_key, None);
        assert_eq!(q.view().signal_waiter("order-1").await.unwrap(), None);
        // The wait ended: a second signal is buffered.
        assert_eq!(
            q.signal("order-1", b"again".to_vec()).await.unwrap(),
            SignalOutcome::Buffered
        );
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_signal_without_a_waiter_is_buffered_and_the_next_enqueue_consumes_it() {
        let clock = MockClock::new(INITIAL);
        let q = open(&clock).await;
        assert_eq!(
            q.signal("order-2", b"first".to_vec()).await.unwrap(),
            SignalOutcome::Buffered
        );
        // A second signal before consumption replaces the first.
        assert_eq!(
            q.signal("order-2", b"second".to_vec()).await.unwrap(),
            SignalOutcome::Buffered
        );

        let id = q
            .enqueue_with("jobs", b"wait".to_vec(), waiting("order-2"))
            .await
            .unwrap();
        let job = q.claim("jobs", LEASE).await.unwrap().unwrap();
        assert_eq!(job.id, id);
        assert_eq!(job.wake_payload.as_deref(), Some(b"second".as_slice()));
        assert_eq!(job.woken_at, Some(INITIAL));
        assert_eq!(job.signal_key, None);
        assert_eq!(q.view().stats("jobs").await.unwrap().scheduled, 0);
        // The buffer is consumed, and the job is not a waiter.
        assert!(!q.clear_signal("order-2").await.unwrap());
        assert_eq!(q.view().signal_waiter("order-2").await.unwrap(), None);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn clear_signal_discards_a_buffered_signal_and_reports_its_absence() {
        let q = open(&MockClock::new(INITIAL)).await;
        q.signal("order-3", b"stale".to_vec()).await.unwrap();
        assert!(q.clear_signal("order-3").await.unwrap());
        assert!(!q.clear_signal("order-3").await.unwrap());
        let id = q
            .enqueue_with("jobs", b"wait".to_vec(), waiting("order-3"))
            .await
            .unwrap();
        assert_eq!(
            q.view().get_job(&id).await.unwrap().unwrap().status,
            JobStatus::Scheduled
        );
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn an_enqueue_with_the_key_of_a_live_waiter_fails_without_a_write() {
        let q = open(&MockClock::new(INITIAL)).await;
        q.enqueue_with("jobs", b"first".to_vec(), waiting("order-4"))
            .await
            .unwrap();
        let err = q
            .enqueue_with_effects(
                "jobs",
                b"second".to_vec(),
                waiting("order-4"),
                SettlementEffects::default().kv_put(b"k", b"v"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::SignalWaiterExists { ref key } if key == "order-4"));
        assert_eq!(q.view().stats("jobs").await.unwrap().scheduled, 1);
        assert!(q.view().kv_get(b"k").await.unwrap().is_none());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_signal_key_requires_run_at_and_is_not_empty() {
        let q = open(&MockClock::new(INITIAL)).await;
        let err = q
            .enqueue_with(
                "jobs",
                b"x".to_vec(),
                EnqueueOptions::default().signal_key("order-5".to_string()),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidSignalKey { ref key, .. } if key == "order-5"));
        let err = q
            .enqueue_with("jobs", b"x".to_vec(), waiting(""))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidSignalKey { ref key, .. } if key.is_empty()));
        assert_eq!(q.view().stats("jobs").await.unwrap().scheduled, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_waiter_with_a_past_run_at_is_pending_without_a_wait() {
        let q = open(&MockClock::new(INITIAL)).await;
        let past = EnqueueOptions::default()
            .run_at(std::time::UNIX_EPOCH + Duration::from_millis(INITIAL - 1))
            .signal_key("order-6".to_string());
        let id = q.enqueue_with("jobs", b"x".to_vec(), past).await.unwrap();
        let job = q.view().get_job(&id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Pending);
        assert_eq!(job.signal_key, None);
        assert_eq!(job.woken_at, None);
        assert_eq!(q.view().signal_waiter("order-6").await.unwrap(), None);
        q.close().await.unwrap();
    }

    /// Every transition out of the scheduled state ends the wait: the index
    /// entry is gone, and a signal to the key is buffered.
    #[tokio::test]
    async fn every_transition_out_of_the_scheduled_state_ends_the_wait() {
        let clock = MockClock::new(INITIAL);
        let q = open(&clock).await;

        let promoted = q
            .enqueue_with("jobs", b"a".to_vec(), waiting("promoted"))
            .await
            .unwrap();
        let cancelled = q
            .enqueue_with("jobs", b"b".to_vec(), waiting("cancelled"))
            .await
            .unwrap();
        let claimed = q
            .enqueue_with("jobs", b"c".to_vec(), waiting("claimed"))
            .await
            .unwrap();
        let woken = q
            .enqueue_with("jobs", b"d".to_vec(), waiting("woken"))
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
        assert_eq!(
            q.view()
                .get_job(&promoted)
                .await
                .unwrap()
                .unwrap()
                .signal_key,
            None
        );

        for key in ["promoted", "cancelled", "claimed", "woken"] {
            assert_eq!(q.view().signal_waiter(key).await.unwrap(), None, "{key}");
            assert_eq!(
                q.signal(key, b"late".to_vec()).await.unwrap(),
                SignalOutcome::Buffered,
                "{key}"
            );
        }
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_retried_waiter_does_not_carry_its_key_into_the_backoff() {
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
        let first = q
            .enqueue_with("jobs", b"a".to_vec(), waiting("order-7"))
            .await
            .unwrap();
        q.signal("order-7", b"go".to_vec()).await.unwrap();
        let job = q.claim("jobs", LEASE).await.unwrap().unwrap();
        q.nack(&job, "transient").await.unwrap();
        let retried = q.view().get_job(&first).await.unwrap().unwrap();
        assert_eq!(retried.status, JobStatus::Scheduled);
        assert_eq!(retried.signal_key, None);

        // A second waiter registers at the key while the first backs off, and
        // the promotion of the first leaves the second indexed.
        let second = q
            .enqueue_with("jobs", b"b".to_vec(), waiting("order-7"))
            .await
            .unwrap();
        clock.advance(Duration::from_secs(11));
        q.promote_scheduled_now().await.unwrap();
        assert_eq!(
            q.view().signal_waiter("order-7").await.unwrap().as_deref(),
            Some(second.as_str())
        );
        assert_eq!(
            q.signal("order-7", b"second".to_vec()).await.unwrap(),
            SignalOutcome::Delivered
        );
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_signal_among_the_effects_commits_with_the_settlement() {
        let clock = MockClock::new(INITIAL);
        let q = open(&clock).await;
        let waiter = q
            .enqueue_with("waits", b"w".to_vec(), waiting("order-8"))
            .await
            .unwrap();
        q.enqueue("work", b"x".to_vec()).await.unwrap();
        let job = q.claim("work", LEASE).await.unwrap().unwrap();

        // A retry discards the effects, so the waiter is untouched.
        q.nack_with(
            &job,
            "transient",
            SettlementEffects::default().signal("order-8", b"early"),
        )
        .await
        .unwrap();
        assert_eq!(
            q.view().signal_waiter("order-8").await.unwrap().as_deref(),
            Some(waiter.as_str())
        );

        let job = q.claim("work", LEASE).await.unwrap().unwrap();
        q.ack_with(
            &job,
            SettlementEffects::default()
                .signal("order-8", b"done")
                .signal("order-9", b"nobody"),
        )
        .await
        .unwrap();
        let woken = q.claim("waits", LEASE).await.unwrap().unwrap();
        assert_eq!(woken.id, waiter);
        assert_eq!(woken.wake_payload.as_deref(), Some(b"done".as_slice()));
        // The signal without a waiter is buffered, and an enqueue among the
        // effects of a later commit consumes it.
        q.commit_effects(SettlementEffects::default().enqueue(EnqueueRequest {
            queue: "waits".to_string(),
            payload: b"n".to_vec(),
            options: waiting("order-9"),
        }))
        .await
        .unwrap();
        let consumed = q.claim("waits", LEASE).await.unwrap().unwrap();
        assert_eq!(consumed.wake_payload.as_deref(), Some(b"nobody".as_slice()));
        q.close().await.unwrap();
    }
}
