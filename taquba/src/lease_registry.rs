//! In-memory registry of claimed jobs' leases: the authoritative source
//! of every claim's current expiry, claim token and cancellation token.
//!
//! Lease state is process state, not durable state. The queue is
//! single-writer and single-process, so every path that consults or
//! changes a lease (claim, renewal, settlement, the reaper) runs beside
//! this registry, and a lease held by a process that no longer runs is
//! void by definition: the store's only durable record of a claim is
//! the claimed job record, and every claimed record found at open is
//! re-queued before workers start. Renewal is therefore a memory
//! operation with no durable write.
//!
//! Coherence with the store follows two rules. An entry is inserted
//! before the transaction writing its claim commits, so a failed commit
//! leaves a stale entry: it is discarded when it comes due (the reaper
//! finds no claimed record under it), while a missing entry would leave
//! its claim invisible to the reaper until the next open.
//! And an entry is removed only after the transaction ending its claim
//! has committed, fenced on the claim token, so a removal that runs
//! after a re-claim of the same job cannot delete the new claim's
//! entry.
//!
//! The entry also holds the claim's cancellation token, fired by
//! [`Queue::cancel`](crate::Queue::cancel) through the registry. The
//! token is registered before the claim commits, so a cancellation
//! racing the commit finds it, and is removed with the entry, so a
//! settlement's cleanup cannot discard the token of a later claim of
//! the same job.
//!
//! The reaper examines due entries in place: [`LeaseRegistry::take_due`]
//! marks entries and leaves them in the registry, and a marked entry
//! refuses renewal. Renewal and the reaper share no durable key, so no
//! transaction conflict orders them; the mark closes the race between
//! a renewal and a requeue already under way. A failed reap leaves its
//! entry marked and in place for the next tick.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};

/// How [`LeaseRegistry::renew`] applies the requested expiry.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Renewal {
    /// Set the expiry to the requested value, which may shorten the
    /// lease.
    Set,
    /// Raise the expiry to the requested value when the lease ends
    /// sooner; a lease that already lasts longer is unchanged.
    Extend,
}

/// A due lease the reaper has marked and is examining.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) struct DueLease {
    pub(crate) queue: String,
    pub(crate) id: String,
    pub(crate) expires_at: u64,
    pub(crate) token: u64,
}

struct Entry {
    expires_at: u64,
    token: u64,
    /// The claim's cooperative cancellation token, fired by
    /// [`LeaseRegistry::cancel`].
    cancel: CancellationToken,
    /// Set once the reaper has taken the entry as due. A marked entry
    /// refuses renewal and leaves the registry only through removal.
    reaping: bool,
}

#[derive(Default)]
struct Inner {
    by_job: HashMap<(String, String), Entry>,
    /// Expiry-ordered view of `by_job`. Every mutation updates both
    /// under one lock, and the tuple is unique because a job holds at
    /// most one lease.
    by_expiry: BTreeSet<(u64, String, String)>,
}

/// Claimed jobs' leases, keyed by job and ordered by expiry.
///
/// Shared between the queue and the reaper via `Clone`; all clones
/// reference the same registry.
#[derive(Clone, Default)]
pub(crate) struct LeaseRegistry {
    inner: Arc<Mutex<Inner>>,
}

impl LeaseRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record a claim's lease and cancellation token, replacing any
    /// previous entry for the job. Called before the transaction
    /// writing the claim commits, so a failed commit leaves a stale
    /// entry, discarded when it comes due.
    pub(crate) fn insert(
        &self,
        queue: &str,
        id: &str,
        expires_at: u64,
        token: u64,
        cancel: CancellationToken,
    ) {
        let mut inner = self.lock();
        let key = (queue.to_string(), id.to_string());
        if let Some(old) = inner.by_job.get(&key) {
            let old_expires = old.expires_at;
            inner
                .by_expiry
                .remove(&(old_expires, key.0.clone(), key.1.clone()));
        }
        inner
            .by_expiry
            .insert((expires_at, key.0.clone(), key.1.clone()));
        inner.by_job.insert(
            key,
            Entry {
                expires_at,
                token,
                cancel,
                reaping: false,
            },
        );
    }

    /// Apply `expires_at` to the lease per `mode`. Returns whether the
    /// expiry changed. Fails with [`Error::ClaimLost`], leaving the
    /// registry unchanged, when the job holds no lease, the lease
    /// belongs to a different claim or the reaper has already taken the
    /// entry as due.
    pub(crate) fn renew(
        &self,
        queue: &str,
        id: &str,
        token: u64,
        expires_at: u64,
        mode: Renewal,
    ) -> Result<bool> {
        let mut inner = self.lock();
        let key = (queue.to_string(), id.to_string());
        let old_expires = match inner.by_job.get(&key) {
            Some(e) if e.token == token && !e.reaping => e.expires_at,
            _ => return Err(Error::ClaimLost),
        };
        if old_expires == expires_at
            || (matches!(mode, Renewal::Extend) && old_expires > expires_at)
        {
            return Ok(false);
        }
        inner
            .by_expiry
            .remove(&(old_expires, key.0.clone(), key.1.clone()));
        inner
            .by_expiry
            .insert((expires_at, key.0.clone(), key.1.clone()));
        inner
            .by_job
            .get_mut(&key)
            .expect("entry present above")
            .expires_at = expires_at;
        Ok(true)
    }

    /// The job's current expiry and claim token, or `None` when it
    /// holds no lease.
    pub(crate) fn current(&self, queue: &str, id: &str) -> Option<(u64, u64)> {
        self.lock()
            .by_job
            .get(&(queue.to_string(), id.to_string()))
            .map(|e| (e.expires_at, e.token))
    }

    /// Fire the cancellation token of the claim currently holding the
    /// job. Returns `false` when the job holds no lease. The entry
    /// remains; a claim ends through a settlement or the reaper.
    pub(crate) fn cancel(&self, queue: &str, id: &str) -> bool {
        // Fired outside the lock: firing wakes waiters, and a waiter
        // must be free to read the registry.
        let token = self
            .lock()
            .by_job
            .get(&(queue.to_string(), id.to_string()))
            .map(|entry| entry.cancel.clone());
        match token {
            Some(token) => {
                token.cancel();
                true
            }
            None => false,
        }
    }

    /// Return every entry due at or before `now`, soonest first,
    /// marking each as being reaped. Entries stay in the registry: the
    /// reaper examines in place and removes only after its commit, so a
    /// tick that ends early leaves nothing displaced.
    pub(crate) fn take_due(&self, now: u64) -> Vec<DueLease> {
        let mut inner = self.lock();
        let due_keys: Vec<(u64, String, String)> = inner
            .by_expiry
            .iter()
            .take_while(|(expires_at, _, _)| *expires_at <= now)
            .cloned()
            .collect();
        let mut due = Vec::with_capacity(due_keys.len());
        for (expires_at, queue, id) in due_keys {
            let key = (queue, id);
            let entry = inner
                .by_job
                .get_mut(&key)
                .expect("index entry has a job entry");
            entry.reaping = true;
            due.push(DueLease {
                queue: key.0,
                id: key.1,
                expires_at,
                token: entry.token,
            });
        }
        due
    }

    /// Remove the job's entry if it still belongs to the claim `token`
    /// identifies. Called after the transaction ending the claim has
    /// committed; the fence makes a removal that runs after a re-claim
    /// of the same job a no-op, leaving the new claim's entry in
    /// place.
    pub(crate) fn remove(&self, queue: &str, id: &str, token: u64) {
        let mut inner = self.lock();
        let key = (queue.to_string(), id.to_string());
        let Some(entry) = inner.by_job.get(&key) else {
            return;
        };
        if entry.token != token {
            return;
        }
        let expires_at = entry.expires_at;
        inner.by_job.remove(&key);
        inner.by_expiry.remove(&(expires_at, key.0, key.1));
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().by_job.len()
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, queue: &str, id: &str, expires_at: u64) -> bool {
        self.lock()
            .by_job
            .get(&(queue.to_string(), id.to_string()))
            .is_some_and(|e| e.expires_at == expires_at)
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().expect("lease registry mutex poisoned")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_due_returns_expired_entries_soonest_first_and_leaves_them_in_place() {
        let registry = LeaseRegistry::new();
        registry.insert("q", "c", 30, 3, CancellationToken::new());
        registry.insert("q", "a", 10, 1, CancellationToken::new());
        registry.insert("q", "b", 20, 2, CancellationToken::new());

        let due = registry.take_due(20);
        let ids: Vec<_> = due.iter().map(|d| d.id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b"]);
        assert_eq!(registry.len(), 3);
    }

    #[test]
    fn take_due_leaves_future_entries_unmarked() {
        let registry = LeaseRegistry::new();
        registry.insert("q", "a", 100, 1, CancellationToken::new());
        assert!(registry.take_due(99).is_empty());
        assert!(registry.renew("q", "a", 1, 200, Renewal::Set).is_ok());
    }

    #[test]
    fn a_renewal_moves_the_entry_in_expiry_order() {
        let registry = LeaseRegistry::new();
        registry.insert("q", "a", 10, 1, CancellationToken::new());
        assert!(registry.renew("q", "a", 1, 40, Renewal::Set).is_ok());

        assert!(registry.take_due(10).is_empty());
        let due = registry.take_due(40);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].expires_at, 40);
    }

    #[test]
    fn a_renewal_to_an_unchanged_expiry_keeps_the_entry() {
        let registry = LeaseRegistry::new();
        registry.insert("q", "a", 10, 1, CancellationToken::new());
        assert_eq!(
            registry.renew("q", "a", 1, 10, Renewal::Set).ok(),
            Some(false)
        );
        assert!(registry.contains("q", "a", 10));
        assert_eq!(registry.take_due(10).len(), 1);
    }

    #[test]
    fn renewal_is_refused_for_a_stale_token_and_for_a_marked_entry() {
        let registry = LeaseRegistry::new();
        registry.insert("q", "a", 10, 1, CancellationToken::new());
        assert!(registry.renew("q", "a", 2, 40, Renewal::Set).is_err());
        assert!(registry.renew("q", "missing", 1, 40, Renewal::Set).is_err());

        assert_eq!(registry.take_due(10).len(), 1);
        assert!(registry.renew("q", "a", 1, 40, Renewal::Set).is_err());
    }

    #[test]
    fn removal_is_fenced_on_the_token() {
        let registry = LeaseRegistry::new();
        registry.insert("q", "a", 10, 1, CancellationToken::new());
        registry.remove("q", "a", 2);
        assert_eq!(registry.len(), 1);
        registry.remove("q", "a", 1);
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn cancel_fires_the_current_claims_token_and_keeps_the_entry() {
        let registry = LeaseRegistry::new();
        let cancel = CancellationToken::new();
        registry.insert("q", "a", 10, 1, cancel.clone());

        assert!(registry.cancel("q", "a"));
        assert!(cancel.is_cancelled());
        assert_eq!(registry.len(), 1);
        assert!(!registry.cancel("q", "missing"));
    }

    #[test]
    fn a_fenced_removal_after_a_reclaim_leaves_the_new_claim_cancellable() {
        let registry = LeaseRegistry::new();
        let first = CancellationToken::new();
        let second = CancellationToken::new();
        registry.insert("q", "a", 10, 1, first.clone());
        // The re-claim registers before the first claim's settlement
        // reaches its removal.
        registry.insert("q", "a", 40, 2, second.clone());
        registry.remove("q", "a", 1);

        assert!(registry.cancel("q", "a"));
        assert!(second.is_cancelled());
        assert!(!first.is_cancelled());
    }

    #[test]
    fn insert_replaces_a_stale_entry_for_the_same_job() {
        let registry = LeaseRegistry::new();
        registry.insert("q", "a", 10, 1, CancellationToken::new());
        registry.insert("q", "a", 30, 2, CancellationToken::new());
        assert_eq!(registry.len(), 1);
        assert!(registry.take_due(10).is_empty());
        assert_eq!(registry.take_due(30).len(), 1);
    }

    use crate::test_util::*;

    #[tokio::test]
    async fn test_renew_lease() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_millis(1))
            .await
            .unwrap()
            .unwrap();
        let original_expiry = q.lease_expiry("work", &job.id).unwrap();

        let new_expiry = q.renew_lease(&job, Duration::from_secs(30)).unwrap();
        assert!(new_expiry > original_expiry, "renewed expiry must be later");

        // Reaper skips the renewed lease even once the original expiry
        // has passed.
        clock.advance(Duration::from_secs(1));
        q.reap_now().await.unwrap();
        assert!(
            q.claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        let fetched = q.get_job(&job.id).await.unwrap().unwrap();
        assert_eq!(fetched.status, JobStatus::Claimed);
        // The claim still holds, so the original handle settles it.
        q.ack(&job).await.unwrap();

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn renewal_is_refused_once_cancellation_is_requested() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let lease = q.lease_handle(&job);
        lease.ensure_at_least(Duration::from_secs(60)).unwrap();

        assert_eq!(q.cancel(&job.id).await.unwrap(), CancelOutcome::Requested);

        let expiry = q.lease_expiry("work", &job.id);
        assert!(matches!(
            q.renew_lease(&job, Duration::from_secs(600)),
            Err(Error::CancelRequested)
        ));
        assert!(matches!(
            lease.ensure_at_least(Duration::from_secs(600)),
            Err(Error::CancelRequested)
        ));
        assert_eq!(q.lease_expiry("work", &job.id), expiry);

        // The claim is still held, so the delivery settles as usual.
        q.nack(&job, "cancelled").await.unwrap();

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn renewal_leaves_the_claim_settleable() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        clock.advance(Duration::from_secs(10));
        let renewed = q.renew_lease(&job, Duration::from_secs(60)).unwrap();
        assert_eq!(q.lease_expiry("work", &job.id), Some(renewed));

        // The claim taken before the renewal keeps its token, so it
        // still settles the delivery.
        q.ack(&job).await.unwrap();

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.done, 1);
        assert_eq!(stats.claimed, 0);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn stale_settlement_after_a_reclaim_is_rejected() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let stale = q
            .claim("work", Duration::from_millis(1))
            .await
            .unwrap()
            .unwrap();

        clock.advance(Duration::from_millis(2));
        q.reap_now().await.unwrap();

        // The re-claim writes the same claimed key the stale copy names,
        // so only the claim token separates the two deliveries.
        let fresh = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fresh.id, id);

        assert!(matches!(q.ack(&stale).await, Err(Error::ClaimLost)));
        assert!(matches!(
            q.nack(&stale, "late failure").await,
            Err(Error::ClaimLost)
        ));
        assert!(matches!(
            q.dead_letter(&stale, "late permanent failure").await,
            Err(Error::ClaimLost)
        ));

        // The live claim is untouched by the rejected settlements.
        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.claimed, 1);
        assert_eq!(stats.done, 0);
        assert_eq!(stats.dead, 0);
        q.ack(&fresh).await.unwrap();

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn settling_after_renewal_leaves_no_lease_entry() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let renewed = q.renew_lease(&job, Duration::from_secs(60)).unwrap();

        // The ack removes the lease entry the renewal moved.
        q.ack(&job).await.unwrap();
        assert!(q.core.lease_registry.current("work", &job.id).is_none());
        assert!(q.lease_expiry("work", &job.id).is_none());

        // Nothing is left to come due, so the reaper requeues nothing,
        // even past the renewed expiry.
        assert!(renewed > clock.now_ms());
        clock.advance(Duration::from_secs(61));
        q.reap_now().await.unwrap();
        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.done, 1);
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.dead, 0);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn settlement_is_rejected_when_the_registry_entry_outlives_the_claim() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let claim = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&claim).await.unwrap();

        // The registry lags the store: an entry is removed only after
        // the commit that ends its claim, so a settlement transaction
        // begun inside that lag passes the token check and conflicts
        // with nothing. Recreate the lagging entry and require the
        // in-transaction record read to reject the settlement.
        q.core.lease_registry.insert(
            "work",
            &claim.id,
            clock.now_ms() + 30_000,
            claim.token(),
            claim.cancel_token().clone(),
        );
        assert!(matches!(
            q.nack(&claim, "late failure").await,
            Err(Error::ClaimLost)
        ));
        assert!(matches!(q.ack(&claim).await, Err(Error::ClaimLost)));

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.done, 1);
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.dead, 0);
        assert_eq!(stats.claimed, 0);
        q.close().await.unwrap();
    }

    async fn assert_every_claim_has_a_lease_entry(q: &Queue) {
        let mut iter = q
            .core
            .db
            .scan_prefix(tag_prefix(KeyTag::Claimed), ..)
            .await
            .unwrap();
        let mut claims = 0;
        while let Some(kv) = iter.next().await.unwrap() {
            let job = JobRecord::decode(&kv.key, &kv.value).unwrap();
            assert!(
                q.core.lease_registry.current(&job.queue, &job.id).is_some(),
                "no lease entry for {}/{}",
                job.queue,
                job.id
            );
            claims += 1;
        }
        assert!(claims > 0, "no claims to check");
    }

    #[tokio::test]
    async fn every_live_claim_has_a_lease_entry() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        for i in 0..3u8 {
            q.enqueue("work", vec![i]).await.unwrap();
        }
        let claims = q
            .claim_batch("work", 3, Duration::from_secs(30))
            .await
            .unwrap();
        assert_every_claim_has_a_lease_entry(&q).await;

        let renewed = q.renew_lease(&claims[0], Duration::from_secs(90)).unwrap();
        assert_every_claim_has_a_lease_entry(&q).await;
        assert!(
            q.core
                .lease_registry
                .contains("work", &claims[0].id, renewed)
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_reclaim_after_a_nack_is_cancellable() {
        let q = Queue::open_with_options(make_store(), "test", no_backoff_opts())
            .await
            .unwrap();

        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let first = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let first_token = first.cancel_token().clone();
        q.nack(&first, "transient").await.unwrap();

        let second = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert!(!second.cancel_token().is_cancelled());

        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Requested);
        assert!(second.cancel_token().is_cancelled());
        assert!(!first_token.is_cancelled());

        q.ack(&second).await.unwrap();
        q.close().await.unwrap();
    }
}
