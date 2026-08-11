//! In-memory registry of claimed jobs' leases: the authoritative source
//! of every claim's current expiry and claim token.
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
//! The reaper examines due entries in place: [`LeaseRegistry::take_due`]
//! marks entries and leaves them in the registry, and a marked entry
//! refuses renewal. Renewal and the reaper share no durable key, so no
//! transaction conflict orders them; the mark closes the race between
//! a renewal and a requeue already under way. A failed reap leaves its
//! entry marked and in place for the next tick.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

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

    /// Record a claim's lease, replacing any previous entry for the
    /// job. Called before the transaction writing the claim commits, so
    /// a failed commit leaves a stale entry, discarded when it comes
    /// due.
    pub(crate) fn insert(&self, queue: &str, id: &str, expires_at: u64, token: u64) {
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
                reaping: false,
            },
        );
    }

    /// Set the lease's expiry to `expires_at`. Returns `false`, leaving
    /// the registry unchanged, when the job holds no lease, the lease
    /// belongs to a different claim or the reaper has already taken the
    /// entry as due.
    pub(crate) fn renew(&self, queue: &str, id: &str, token: u64, expires_at: u64) -> bool {
        let mut inner = self.lock();
        let key = (queue.to_string(), id.to_string());
        let old_expires = match inner.by_job.get(&key) {
            Some(e) if e.token == token && !e.reaping => e.expires_at,
            _ => return false,
        };
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
        true
    }

    /// The job's current expiry and claim token, or `None` when it
    /// holds no lease.
    pub(crate) fn current(&self, queue: &str, id: &str) -> Option<(u64, u64)> {
        self.lock()
            .by_job
            .get(&(queue.to_string(), id.to_string()))
            .map(|e| (e.expires_at, e.token))
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
        registry.insert("q", "c", 30, 3);
        registry.insert("q", "a", 10, 1);
        registry.insert("q", "b", 20, 2);

        let due = registry.take_due(20);
        let ids: Vec<_> = due.iter().map(|d| d.id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b"]);
        assert_eq!(registry.len(), 3);
    }

    #[test]
    fn take_due_leaves_future_entries_unmarked() {
        let registry = LeaseRegistry::new();
        registry.insert("q", "a", 100, 1);
        assert!(registry.take_due(99).is_empty());
        assert!(registry.renew("q", "a", 1, 200));
    }

    #[test]
    fn a_renewal_moves_the_entry_in_expiry_order() {
        let registry = LeaseRegistry::new();
        registry.insert("q", "a", 10, 1);
        assert!(registry.renew("q", "a", 1, 40));

        assert!(registry.take_due(10).is_empty());
        let due = registry.take_due(40);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].expires_at, 40);
    }

    #[test]
    fn a_renewal_to_an_unchanged_expiry_keeps_the_entry() {
        let registry = LeaseRegistry::new();
        registry.insert("q", "a", 10, 1);
        assert!(registry.renew("q", "a", 1, 10));
        assert!(registry.contains("q", "a", 10));
        assert_eq!(registry.take_due(10).len(), 1);
    }

    #[test]
    fn renewal_is_refused_for_a_stale_token_and_for_a_marked_entry() {
        let registry = LeaseRegistry::new();
        registry.insert("q", "a", 10, 1);
        assert!(!registry.renew("q", "a", 2, 40));
        assert!(!registry.renew("q", "missing", 1, 40));

        assert_eq!(registry.take_due(10).len(), 1);
        assert!(!registry.renew("q", "a", 1, 40));
    }

    #[test]
    fn removal_is_fenced_on_the_token() {
        let registry = LeaseRegistry::new();
        registry.insert("q", "a", 10, 1);
        registry.remove("q", "a", 2);
        assert_eq!(registry.len(), 1);
        registry.remove("q", "a", 1);
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn insert_replaces_a_stale_entry_for_the_same_job() {
        let registry = LeaseRegistry::new();
        registry.insert("q", "a", 10, 1);
        registry.insert("q", "a", 30, 2);
        assert_eq!(registry.len(), 1);
        assert!(registry.take_due(10).is_empty());
        assert_eq!(registry.take_due(30).len(), 1);
    }
}
