//! In-process registry of tasks waiting for a job's terminal transition.
//!
//! A waiter registers its job id before it reads the job's state, so a
//! transition that commits after the read reaches it through the
//! registry; one that commits before the read is visible in the
//! read. Every terminal transition settles its job's waiters after its
//! commit, with the outcome the transition wrote, so a waiter learns
//! the outcome without a second read. The outcome is built only when
//! the job has waiters, so a transition without one costs a map lookup.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

use crate::WaitOutcome;

#[derive(Default)]
pub(crate) struct CompletionWaiters {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    next_key: u64,
    waiters: HashMap<String, Vec<(u64, oneshot::Sender<WaitOutcome>)>>,
}

/// One registered waiter. Dropping it withdraws the registration.
pub(crate) struct Registration {
    registry: Arc<CompletionWaiters>,
    id: String,
    key: u64,
    receiver: oneshot::Receiver<WaitOutcome>,
}

impl CompletionWaiters {
    /// Register a waiter for `id`. Synchronous, so a caller can register
    /// before its first await.
    pub(crate) fn register(self: &Arc<Self>, id: &str) -> Registration {
        let (sender, receiver) = oneshot::channel();
        let key = {
            let mut inner = self.inner.lock().expect("completion waiters lock");
            let key = inner.next_key;
            inner.next_key += 1;
            inner
                .waiters
                .entry(id.to_string())
                .or_default()
                .push((key, sender));
            key
        };
        Registration {
            registry: self.clone(),
            id: id.to_string(),
            key,
            receiver,
        }
    }

    /// Whether any task is waiting on `id`.
    pub(crate) fn has_waiters(&self, id: &str) -> bool {
        self.inner
            .lock()
            .expect("completion waiters lock")
            .waiters
            .contains_key(id)
    }

    /// Deliver the terminal outcome of `id` to its waiters, if any.
    pub(crate) fn settle(&self, id: &str, outcome: impl FnOnce() -> WaitOutcome) {
        let waiters = {
            let mut inner = self.inner.lock().expect("completion waiters lock");
            inner.waiters.remove(id)
        };
        let Some(waiters) = waiters else {
            return;
        };
        let outcome = outcome();
        for (_, sender) in waiters {
            let _ = sender.send(outcome.clone());
        }
    }

    #[cfg(test)]
    pub(crate) fn inner_is_empty(&self) -> bool {
        self.inner
            .lock()
            .expect("completion waiters lock")
            .waiters
            .is_empty()
    }

    fn withdraw(&self, id: &str, key: u64) {
        let mut inner = self.inner.lock().expect("completion waiters lock");
        if let Some(waiters) = inner.waiters.get_mut(id) {
            waiters.retain(|(k, _)| *k != key);
            if waiters.is_empty() {
                inner.waiters.remove(id);
            }
        }
    }
}

impl Registration {
    /// The outcome, if it was delivered already.
    pub(crate) fn try_outcome(&mut self) -> Option<WaitOutcome> {
        self.receiver.try_recv().ok()
    }

    /// The channel the outcome arrives on.
    pub(crate) fn receiver(&mut self) -> &mut oneshot::Receiver<WaitOutcome> {
        &mut self.receiver
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.registry.withdraw(&self.id, self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_settled_outcome_reaches_every_waiter_of_that_job_only() {
        let registry = Arc::new(CompletionWaiters::default());
        let mut a = registry.register("a");
        let mut b = registry.register("a");
        let mut other = registry.register("b");
        registry.settle("a", || WaitOutcome::Cancelled);
        assert!(matches!(a.try_outcome(), Some(WaitOutcome::Cancelled)));
        assert!(matches!(b.try_outcome(), Some(WaitOutcome::Cancelled)));
        assert!(other.try_outcome().is_none());
    }

    #[tokio::test]
    async fn a_dropped_registration_is_withdrawn_and_builds_no_outcome() {
        let registry = Arc::new(CompletionWaiters::default());
        drop(registry.register("a"));
        let mut built = false;
        registry.settle("a", || {
            built = true;
            WaitOutcome::Cancelled
        });
        assert!(!built);
        assert!(registry.inner_is_empty());
    }

    use crate::test_util::*;

    /// Poll `fut` once with a no-op waker. A pending result shows the
    /// future reached its first await, which for `wait_for_completion`
    /// is past the waiter registration.
    fn poll_once<F: std::future::Future>(fut: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        fut.poll(&mut cx)
    }

    #[tokio::test]
    async fn test_wait_for_completion_unknown_id_is_not_found() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let outcome = q.wait_for_completion("does-not-exist").await.unwrap();
        assert!(matches!(outcome, WaitOutcome::NotFound), "{outcome:?}");
        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_wait_for_completion_pending_times_out() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let outcome = q
            .wait_for_completion_timeout(&id, Duration::from_secs(60))
            .await
            .unwrap();
        assert!(outcome.is_none(), "{outcome:?}");
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wait_for_completion_wakes_on_ack() {
        // Default retention deletes the record on ack; the waiter
        // receives it from the settlement.
        let q = Queue::open(make_store(), "test").await.unwrap();
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        let waiter = q.wait_for_completion(&id);
        tokio::pin!(waiter);
        assert!(poll_once(waiter.as_mut()).is_pending());

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();

        match waiter.await.unwrap() {
            WaitOutcome::Done(record) => {
                assert_eq!(record.id, id);
                assert_eq!(record.status, JobStatus::Done);
                assert!(record.completed_at.is_some());
                assert_eq!(record.payload, b"payload");
            }
            other => panic!("expected Done(record), got {other:?}"),
        }
        assert!(q.get_job(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_wait_for_completion_wakes_on_exhausted_nack() {
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
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        let waiter = q.wait_for_completion(&id);
        tokio::pin!(waiter);
        assert!(poll_once(waiter.as_mut()).is_pending());

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.nack(&job, "transient").await.unwrap();

        match waiter.await.unwrap() {
            WaitOutcome::Dead(record) => {
                assert_eq!(record.id, id);
                assert_eq!(record.status, JobStatus::Dead);
                assert_eq!(record.last_error.as_deref(), Some("transient"));
            }
            other => panic!("expected Dead(record), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_wait_for_completion_wakes_on_cancel_removed() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        let waiter = q.wait_for_completion(&id);
        tokio::pin!(waiter);
        assert!(poll_once(waiter.as_mut()).is_pending());

        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Removed);

        assert!(matches!(waiter.await.unwrap(), WaitOutcome::Cancelled));
        assert!(q.get_job(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_wait_for_completion_does_not_wake_on_cancel_requested() {
        // A `Claimed` cancel fires the token but the job is still in
        // flight; the wait continues until the worker settles the claim.
        let q = Queue::open(make_store(), "test").await.unwrap();
        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let id = job.id.clone();

        let waiter = q.wait_for_completion(&id);
        tokio::pin!(waiter);
        assert!(poll_once(waiter.as_mut()).is_pending());

        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Requested);
        assert!(poll_once(waiter.as_mut()).is_pending());

        q.ack(&job).await.unwrap();
        assert!(matches!(waiter.await.unwrap(), WaitOutcome::Done(_)));
    }

    #[tokio::test]
    async fn test_wait_for_completion_returns_immediately_when_already_terminal() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let id = job.id.clone();
        q.dead_letter(&job, "permanent").await.unwrap();

        match q.wait_for_completion(&id).await.unwrap() {
            WaitOutcome::Dead(record) => {
                assert_eq!(record.id, id);
                assert_eq!(record.status, JobStatus::Dead);
            }
            other => panic!("expected Dead(record), got {other:?}"),
        }
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wait_for_completion_fan_out_to_multiple_waiters() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();

        let mut waiters = Vec::new();
        for _ in 0..4 {
            let mut waiter = Box::pin(q.wait_for_completion(&id));
            assert!(poll_once(waiter.as_mut()).is_pending());
            waiters.push(waiter);
        }

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.dead_letter(&job, "permanent").await.unwrap();

        for waiter in waiters {
            match waiter.await.unwrap() {
                WaitOutcome::Dead(record) => {
                    assert_eq!(record.id, id);
                    assert_eq!(record.status, JobStatus::Dead);
                    assert_eq!(record.last_error.as_deref(), Some("permanent"));
                }
                other => panic!("waiter saw {other:?}, expected Dead(record)"),
            }
        }
    }

    #[tokio::test]
    async fn test_wait_for_completion_delivers_offloaded_payloads_inline() {
        // Covers the three settlements that hold a stored record: ack,
        // worker dead-letter and reaper dead-letter.
        let clock = Arc::new(MockClock::new(1_000_000));
        let opts = OpenOptions {
            reaper_interval: Duration::from_secs(3600),
            clock: clock.clone(),
            default_queue_config: QueueConfig {
                max_attempts: 1,
                ..offload_opts().default_queue_config
            },
            ..offload_opts()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let payload = vec![7u8; 256];

        let id = q.enqueue("work", payload.clone()).await.unwrap();
        let waiter = q.wait_for_completion(&id);
        tokio::pin!(waiter);
        assert!(poll_once(waiter.as_mut()).is_pending());
        let job = q
            .claim("work", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        assert!(job.payload_ref.is_some());
        q.ack(&job).await.unwrap();
        match waiter.await.unwrap() {
            WaitOutcome::Done(record) => assert_eq!(record.payload, payload),
            other => panic!("expected Done(record), got {other:?}"),
        }

        let id = q.enqueue("work", payload.clone()).await.unwrap();
        let waiter = q.wait_for_completion(&id);
        tokio::pin!(waiter);
        assert!(poll_once(waiter.as_mut()).is_pending());
        let job = q
            .claim("work", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        q.dead_letter(&job, "permanent").await.unwrap();
        match waiter.await.unwrap() {
            WaitOutcome::Dead(record) => assert_eq!(record.payload, payload),
            other => panic!("expected Dead(record), got {other:?}"),
        }

        let id = q.enqueue("work", payload.clone()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        drop(job);
        let waiter = q.wait_for_completion(&id);
        tokio::pin!(waiter);
        assert!(poll_once(waiter.as_mut()).is_pending());
        clock.advance(Duration::from_secs(11));
        q.reap_now().await.unwrap();
        match waiter.await.unwrap() {
            WaitOutcome::Dead(record) => {
                assert_eq!(record.payload, payload);
                assert_eq!(record.last_error.as_deref(), Some("lease expired"));
            }
            other => panic!("expected Dead(record), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_wait_for_completion_reports_a_removal_that_races_the_read() {
        // An outcome delivered after the registration takes precedence
        // over `NotFound` when the record is gone at the read.
        let q = Queue::open(make_store(), "test").await.unwrap();
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let mut registration = q.core.completion_waiters.register(&id);
        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Removed);
        assert!(matches!(
            registration.try_outcome(),
            Some(WaitOutcome::Cancelled)
        ));
        assert!(q.core.completion_waiters.inner_is_empty());
    }
}
