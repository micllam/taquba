//! In-memory registry of claimed jobs' leases: the source of a claim's current
//! expiry and cancellation token. The claimed key in the store identifies the
//! live claim of a job.
//!
//! Lease state is process state and is never durable. The queue is
//! single-writer and single-process, so every path that consults or changes a
//! lease (claim, renewal, settlement, the reaper) runs in the same process as
//! this registry, and a lease held by a process that no longer runs is lost by
//! definition: the store's only durable record of a claim is the claimed job
//! record, and every claimed record found at open is re-queued before workers
//! start. Renewal is therefore a memory operation without a durable write.
//!
//! The registry uses the claim id as part of its key, so each claim has its own
//! entry. Two rules keep the registry coherent with the store:
//!
//! **Insert before the commit.** An entry is inserted before the transaction
//! writing its claim commits. A claim that fails to commit, including one that
//! lost to another claim of the job, leaves its entry and the winner's entry
//! intact. The store does not have a record at the loser's key, so the reaper
//! drops the loser's entry once its expiry passes. The store can apply the
//! commit of a claim whose future was dropped, and the reaper finds that claim
//! through the entry inserted before the commit.
//!
//! **Remove after the commit.** An entry is removed only after the transaction
//! ending its claim commits.
//!
//! The entry also contains the claim's cancellation token, fired by
//! [`Queue::cancel`](crate::Queue::cancel) through the registry. The token is
//! registered before the claim commits, so a cancellation racing the commit
//! finds it, and is removed with its entry.
//!
//! The reaper examines due entries in place: [`LeaseRegistry::take_due`] marks
//! entries and leaves them in the registry, and a marked entry refuses renewal.
//! Renewal and the reaper do not share a durable key, so a transaction conflict
//! does not order them. The mark closes the race between a renewal and a
//! requeue already under way. A failed reap leaves its entry marked and in
//! place for the next tick.
//!
//! Every lease is lost once the store closes, because every settlement then
//! fails. The queue closes the registry at the store's first close reason,
//! including a fence by a second writer. Closing fires every entry's token, and
//! a closed registry refuses every renewal with [`Error::ClaimLost`].

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::keys::QueueName;

/// How [`LeaseRegistry::renew`] applies the requested expiry.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Renewal {
    /// Set the expiry to the requested value, which can shorten the lease.
    Set,
    /// Raise the expiry to the requested value when the lease ends sooner. A
    /// lease that already lasts longer is unchanged.
    Extend,
}

/// A due lease that the reaper marked and is examining.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) struct DueLease {
    pub(crate) queue: QueueName,
    pub(crate) id: String,
    pub(crate) expires_at: u64,
    pub(crate) claim_id: u64,
}

/// The key of an entry.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ClaimKey {
    queue: QueueName,
    id: String,
    claim_id: u64,
}

impl ClaimKey {
    fn new(queue: &QueueName, id: &str, claim_id: u64) -> Self {
        Self {
            queue: queue.clone(),
            id: id.to_string(),
            claim_id,
        }
    }
}

struct Entry {
    expires_at: u64,
    /// The claim's cooperative cancellation token, fired by
    /// [`LeaseRegistry::cancel`].
    cancel: CancellationToken,
    /// Set once the reaper takes the entry as due. A marked entry refuses
    /// renewal and leaves the registry only through removal.
    reaping: bool,
}

#[derive(Default)]
struct Inner {
    by_claim: BTreeMap<ClaimKey, Entry>,
    /// Expiry-ordered view of `by_claim`. Every mutation updates the two
    /// collections under one lock.
    by_expiry: BTreeSet<(u64, ClaimKey)>,
    /// Set by [`LeaseRegistry::close`] and never cleared.
    closed: bool,
}

impl Inner {
    /// The entries of the job `id` of `queue`, in claim id order. The range
    /// requires the field order of `ClaimKey`: queue, job id, claim id.
    fn job_entries(
        &self,
        queue: &QueueName,
        id: &str,
    ) -> impl Iterator<Item = (&ClaimKey, &Entry)> {
        let first = ClaimKey::new(queue, id, 0);
        let last = ClaimKey::new(queue, id, u64::MAX);
        self.by_claim.range(first..=last)
    }
}

/// Claimed jobs' leases. The registry uses the claim as its key and orders the
/// leases by expiry.
///
/// Shared between the queue and the reaper via `Clone`, and all clones
/// reference the same registry.
#[derive(Clone, Default)]
pub(crate) struct LeaseRegistry {
    inner: Arc<Mutex<Inner>>,
}

impl LeaseRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record a claim's lease and cancellation token. The entries of other
    /// claims of the job stay in place. Called before the transaction writing
    /// the claim commits, so a failed commit leaves a stale entry, which the
    /// reaper discards once its expiry passes.
    pub(crate) fn insert(
        &self,
        queue: &QueueName,
        id: &str,
        expires_at: u64,
        claim_id: u64,
        cancel: CancellationToken,
    ) {
        let mut inner = self.lock();
        let key = ClaimKey::new(queue, id, claim_id);
        inner.by_expiry.insert((expires_at, key.clone()));
        let entry = Entry {
            expires_at,
            cancel,
            reaping: false,
        };
        let previous = inner.by_claim.insert(key, entry);
        debug_assert!(previous.is_none(), "claim ids are unique");
    }

    /// Apply `expires_at` to the lease of the claim `claim_id` per `mode`.
    /// Returns whether the expiry changed. Fails with [`Error::ClaimLost`] when
    /// the registry is closed, the claim does not have a lease or the reaper
    /// already took the entry as due. Otherwise fails with
    /// [`Error::CancelRequested`] when the claim's token `cancel` fired. A
    /// failure leaves the registry unchanged.
    pub(crate) fn renew(
        &self,
        queue: &QueueName,
        id: &str,
        claim_id: u64,
        cancel: &CancellationToken,
        expires_at: u64,
        mode: Renewal,
    ) -> Result<bool> {
        let mut inner = self.lock();
        // Checked under the lock that `close` takes to set the mark, so a token
        // fired by `close` is never reported as a cancellation.
        if inner.closed {
            return Err(Error::ClaimLost);
        }
        if cancel.is_cancelled() {
            return Err(Error::CancelRequested);
        }
        let key = ClaimKey::new(queue, id, claim_id);
        let old_expires = match inner.by_claim.get(&key) {
            Some(e) if !e.reaping => e.expires_at,
            _ => return Err(Error::ClaimLost),
        };
        if old_expires == expires_at
            || (matches!(mode, Renewal::Extend) && old_expires > expires_at)
        {
            return Ok(false);
        }
        inner.by_expiry.remove(&(old_expires, key.clone()));
        inner.by_expiry.insert((expires_at, key.clone()));
        inner
            .by_claim
            .get_mut(&key)
            .expect("entry present above")
            .expires_at = expires_at;
        Ok(true)
    }

    /// The expiry of the lease of the claim `claim_id`, or `None` when the
    /// claim does not have an entry.
    pub(crate) fn expiry(&self, queue: &QueueName, id: &str, claim_id: u64) -> Option<u64> {
        self.lock()
            .by_claim
            .get(&ClaimKey::new(queue, id, claim_id))
            .map(|entry| entry.expires_at)
    }

    /// Fire the cancellation token of every claim of the job with an entry: the
    /// live claim, and a claim that the reaper is ending or that failed to
    /// commit. Returns `false` when the job does not have an entry. The entries
    /// remain until a settlement or the reaper ends their claims.
    pub(crate) fn cancel(&self, queue: &QueueName, id: &str) -> bool {
        // Fired outside the lock: firing wakes waiters, and a waiter must be
        // free to read the registry.
        let tokens: Vec<CancellationToken> = self
            .lock()
            .job_entries(queue, id)
            .map(|(_, entry)| entry.cancel.clone())
            .collect();
        for token in &tokens {
            token.cancel();
        }
        !tokens.is_empty()
    }

    /// Mark the registry closed and fire every entry's cancellation token.
    /// Entries stay in the registry.
    pub(crate) fn close(&self) {
        let tokens: Vec<CancellationToken> = {
            let mut inner = self.lock();
            inner.closed = true;
            inner.by_claim.values().map(|e| e.cancel.clone()).collect()
        };
        // Fired outside the lock, as in `cancel`.
        for token in tokens {
            token.cancel();
        }
    }

    /// Return every entry due at or before `now`, soonest first, marking each
    /// as being reaped. Entries stay in the registry: the reaper examines in
    /// place and removes only after its commit, so a tick that ends early
    /// leaves every entry in the registry.
    pub(crate) fn take_due(&self, now: u64) -> Vec<DueLease> {
        let mut inner = self.lock();
        let due_keys: Vec<(u64, ClaimKey)> = inner
            .by_expiry
            .iter()
            .take_while(|(expires_at, _)| *expires_at <= now)
            .cloned()
            .collect();
        let mut due = Vec::with_capacity(due_keys.len());
        for (expires_at, key) in due_keys {
            inner
                .by_claim
                .get_mut(&key)
                .expect("index entry has a claim entry")
                .reaping = true;
            let ClaimKey {
                queue,
                id,
                claim_id,
            } = key;
            due.push(DueLease {
                queue,
                id,
                expires_at,
                claim_id,
            });
        }
        due
    }

    /// Remove the entry of the claim `claim_id`. Called after the transaction
    /// ending the claim commits. The entries of other claims of the job stay in
    /// place.
    pub(crate) fn remove(&self, queue: &QueueName, id: &str, claim_id: u64) {
        let mut inner = self.lock();
        let key = ClaimKey::new(queue, id, claim_id);
        if let Some(entry) = inner.by_claim.remove(&key) {
            inner.by_expiry.remove(&(entry.expires_at, key));
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().by_claim.len()
    }

    /// The expiry and the claim id of each entry of the job, in claim id order.
    #[cfg(test)]
    pub(crate) fn leases(&self, queue: &QueueName, id: &str) -> Vec<(u64, u64)> {
        self.lock()
            .job_entries(queue, id)
            .map(|(key, entry)| (entry.expires_at, key.claim_id))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, queue: &QueueName, id: &str, expires_at: u64) -> bool {
        self.leases(queue, id)
            .iter()
            .any(|(expiry, _)| *expiry == expires_at)
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().expect("lease registry mutex poisoned")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn renew_set(
        registry: &LeaseRegistry,
        id: &str,
        claim_id: u64,
        expires_at: u64,
    ) -> Result<bool> {
        let live = CancellationToken::new();
        registry.renew(&qn("q"), id, claim_id, &live, expires_at, Renewal::Set)
    }

    #[test]
    fn take_due_returns_expired_entries_soonest_first_and_leaves_them_in_place() {
        let registry = LeaseRegistry::new();
        registry.insert(&qn("q"), "c", 30, 3, CancellationToken::new());
        registry.insert(&qn("q"), "a", 10, 1, CancellationToken::new());
        registry.insert(&qn("q"), "b", 20, 2, CancellationToken::new());

        let due = registry.take_due(20);
        let ids: Vec<_> = due.iter().map(|d| d.id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b"]);
        assert_eq!(registry.len(), 3);
    }

    #[test]
    fn take_due_leaves_future_entries_unmarked() {
        let registry = LeaseRegistry::new();
        registry.insert(&qn("q"), "a", 100, 1, CancellationToken::new());
        assert!(registry.take_due(99).is_empty());
        assert!(renew_set(&registry, "a", 1, 200).is_ok());
    }

    #[test]
    fn a_renewal_moves_the_entry_in_expiry_order() {
        let registry = LeaseRegistry::new();
        registry.insert(&qn("q"), "a", 10, 1, CancellationToken::new());
        assert!(renew_set(&registry, "a", 1, 40).is_ok());

        assert!(registry.take_due(10).is_empty());
        let due = registry.take_due(40);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].expires_at, 40);
    }

    #[test]
    fn a_renewal_to_an_unchanged_expiry_keeps_the_entry() {
        let registry = LeaseRegistry::new();
        registry.insert(&qn("q"), "a", 10, 1, CancellationToken::new());
        assert_eq!(renew_set(&registry, "a", 1, 10).ok(), Some(false));
        assert!(registry.contains(&qn("q"), "a", 10));
        assert_eq!(registry.take_due(10).len(), 1);
    }

    #[test]
    fn renewal_is_refused_for_a_stale_claim_id_and_for_a_marked_entry() {
        let registry = LeaseRegistry::new();
        registry.insert(&qn("q"), "a", 10, 1, CancellationToken::new());
        assert!(renew_set(&registry, "a", 2, 40).is_err());
        assert!(renew_set(&registry, "missing", 1, 40).is_err());

        assert_eq!(registry.take_due(10).len(), 1);
        assert!(renew_set(&registry, "a", 1, 40).is_err());
    }

    #[test]
    fn removal_is_fenced_on_the_claim_id() {
        let registry = LeaseRegistry::new();
        registry.insert(&qn("q"), "a", 10, 1, CancellationToken::new());
        registry.remove(&qn("q"), "a", 2);
        assert_eq!(registry.len(), 1);
        registry.remove(&qn("q"), "a", 1);
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn cancel_fires_the_token_of_every_claim_of_the_job_and_keeps_the_entries() {
        let registry = LeaseRegistry::new();
        let (first, second) = (CancellationToken::new(), CancellationToken::new());
        registry.insert(&qn("q"), "a", 10, 1, first.clone());
        registry.insert(&qn("q"), "a", 10, 2, second.clone());

        assert!(registry.cancel(&qn("q"), "a"));
        assert!(first.is_cancelled() && second.is_cancelled());
        assert_eq!(registry.len(), 2);
        assert!(!registry.cancel(&qn("q"), "missing"));
    }

    #[test]
    fn a_fenced_removal_after_a_reclaim_leaves_the_new_claim_cancellable() {
        let registry = LeaseRegistry::new();
        let first = CancellationToken::new();
        let second = CancellationToken::new();
        registry.insert(&qn("q"), "a", 10, 1, first.clone());
        // The re-claim registers before the first claim's settlement reaches
        // its removal.
        registry.insert(&qn("q"), "a", 40, 2, second.clone());
        registry.remove(&qn("q"), "a", 1);

        assert!(registry.cancel(&qn("q"), "a"));
        assert!(second.is_cancelled());
        assert!(!first.is_cancelled());
    }

    #[test]
    fn each_claim_of_a_job_has_its_own_entry() {
        let registry = LeaseRegistry::new();
        registry.insert(&qn("q"), "a", 10, 1, CancellationToken::new());
        registry.insert(&qn("q"), "a", 30, 2, CancellationToken::new());
        assert_eq!(registry.leases(&qn("q"), "a"), vec![(10, 1), (30, 2)]);
        let due: Vec<_> = registry.take_due(10).iter().map(|d| d.claim_id).collect();
        assert_eq!(due, vec![1]);
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
        let original_expiry = q.lease_expiry(&job).unwrap();

        let new_expiry = q.renew_lease(&job, Duration::from_secs(30)).unwrap();
        assert!(new_expiry > original_expiry, "renewed expiry must be later");

        // Reaper skips the renewed lease even after the original expiry passes.
        clock.advance(Duration::from_secs(1));
        q.reap_now().await.unwrap();
        assert!(
            q.claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        let fetched = q.view().get_job(&job.id).await.unwrap().unwrap();
        assert_eq!(fetched.status, JobStatus::Claimed);
        // The claim is still live, so the original handle settles it.
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

        let expiry = q.lease_expiry(&job);
        assert!(matches!(
            q.renew_lease(&job, Duration::from_secs(600)),
            Err(Error::CancelRequested)
        ));
        assert!(matches!(
            lease.ensure_at_least(Duration::from_secs(600)),
            Err(Error::CancelRequested)
        ));
        assert_eq!(q.lease_expiry(&job), expiry);

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
        assert_eq!(q.lease_expiry(&job), Some(renewed));

        // The claim taken before the renewal keeps its claim id, so it still
        // settles the delivery.
        q.ack(&job).await.unwrap();

        let stats = q.view().stats("work").await.unwrap();
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

        // The re-claim writes the same claimed key the stale copy names, so
        // only the claim id separates the two deliveries.
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
        let stats = q.view().stats("work").await.unwrap();
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
        assert!(
            q.core
                .lease_registry
                .leases(&qn("work"), &job.id)
                .is_empty()
        );

        // No entry is left to expire, so the reaper does not requeue the job,
        // even past the renewed expiry.
        assert!(renewed > clock.now_ms());
        clock.advance(Duration::from_secs(61));
        q.reap_now().await.unwrap();
        let stats = q.view().stats("work").await.unwrap();
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

        // The registry lags the store: an entry is removed only after the
        // commit that ends its claim, so a settlement transaction begun inside
        // that lag passes the claim id check and conflicts with nothing.
        // Recreate the lagging entry and require the in-transaction record read
        // to reject the settlement.
        q.core.lease_registry.insert(
            &qn("work"),
            &claim.id,
            clock.now_ms() + 30_000,
            claim.claim_id(),
            claim.cancel_token().clone(),
        );
        assert!(matches!(
            q.nack(&claim, "late failure").await,
            Err(Error::ClaimLost)
        ));
        assert!(matches!(q.ack(&claim).await, Err(Error::ClaimLost)));

        let stats = q.view().stats("work").await.unwrap();
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
                q.lease_expiry(&job).is_some(),
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
                .contains(&qn("work"), &claims[0].id, renewed)
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_fence_fires_every_token_and_refuses_renewal_with_claim_lost() {
        let store = make_store();
        let first = Queue::open(store.clone(), "test").await.unwrap();
        first.enqueue("work", b"payload".to_vec()).await.unwrap();
        let claim = first
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let lease = first.lease_handle(&claim);

        let second = Queue::open(store, "test").await.unwrap();
        // A write takes the WAL path to the fence, which does not wait for the
        // manifest poll. The failed write returns after the store reported the
        // close, so the watcher fires the token at the next await.
        assert!(first.enqueue("work", b"late".to_vec()).await.is_err());
        lease.cancel_token().cancelled().await;
        assert!(matches!(
            lease.ensure_at_least(Duration::from_secs(60)),
            Err(Error::ClaimLost)
        ));
        assert!(matches!(
            first.renew_lease(&claim, Duration::from_secs(60)),
            Err(Error::ClaimLost)
        ));

        let _ = first.close().await;
        second.close().await.unwrap();
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
