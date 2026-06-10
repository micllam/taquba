use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::Notify;

/// Upper bound on wakeups issued for one batch of inserts. Beyond the
/// cap, woken workers drain the backlog by looping on claim, and
/// `Notify::notify_one` stores at most one permit when no task is
/// waiting, so extra calls would be wasted work.
const MAX_INSERT_WAKEUPS: usize = 64;

/// Per-queue in-memory claim-scan state: a scan-start bound and a
/// pending-insert epoch.
///
/// The scan-start bound is the position the next claim scans from,
/// skipping the tombstone band left by previously claimed (and
/// deleted) `pending:` entries. After a claim it excludes the claimed
/// key; after an insert that lands at or before it, it moves back to
/// include the inserted key. The invariant is that every live
/// `pending:` key sorts at or after the bound, so the claim path only
/// falls back to a front prefix scan when the bound is unknown (cold
/// start or process restart).
///
/// The epoch counts committed `pending:` inserts. When a claim's full
/// prefix scan finds nothing, it records the epoch it observed before
/// its transaction began; until the next insert bumps the epoch,
/// subsequent claims return `None` without scanning. Without this,
/// every poll of an empty queue re-scans the tombstone band from the
/// front, which grows with every job claimed since the last
/// compaction.
///
/// Shared across the queue, reaper, and scheduler via `Clone`; all
/// clones reference the same in-memory map. Not persisted: on
/// process restart the first claim falls back to a prefix scan and
/// re-warms the state naturally.
#[derive(Clone, Default)]
pub(crate) struct ClaimCursor {
    inner: Arc<Mutex<HashMap<String, QueueClaimState>>>,
}

/// Where the next claim scan starts.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) struct ScanFrom {
    pub(crate) key: Bytes,
    /// `true` when `key` itself may be live (it was inserted at or
    /// before the previous bound); `false` when `key` was claimed and
    /// the scan starts strictly after it.
    pub(crate) inclusive: bool,
}

#[derive(Default)]
struct QueueClaimState {
    scan_from: Option<ScanFrom>,
    /// Bumped after every committed `pending:` insert.
    epoch: u64,
    /// The epoch observed by a claim whose full prefix scan found
    /// nothing. While it equals `epoch`, the queue is known empty.
    empty_as_of: Option<u64>,
    /// Smallest key inserted at or after the bound since the last
    /// [`ClaimCursor::advance`] consumed it. Job ids are generated
    /// before their enqueue transaction commits, so a key can sort
    /// below keys an in-flight claim is about to advance past while
    /// still being ahead of the bound when its insert is recorded.
    /// `advance` clamps to this key so the bound never jumps over an
    /// insert it could not have observed.
    min_insert_ahead: Option<Bytes>,
    /// Queue-scoped wakeup for tasks waiting in `claim_with_wait` or
    /// `wait_for_jobs_on`. Each recorded insert issues one
    /// `notify_one`, waking one waiting worker per job instead of the
    /// whole pool.
    wakeup: Arc<Notify>,
}

/// Snapshot of one queue's claim-scan state, taken at the start of a
/// claim attempt.
pub(crate) struct ClaimScanStart {
    pub(crate) scan_from: Option<ScanFrom>,
    pub(crate) epoch: u64,
    pub(crate) known_empty: bool,
}

impl ClaimCursor {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Snapshot the scan state for one claim attempt. The epoch must
    /// be read before the claim's transaction begins: emptiness
    /// recorded against it is then revoked by any insert the
    /// transaction's snapshot could have missed.
    pub(crate) fn begin_claim(&self, queue: &str) -> ClaimScanStart {
        let map = self.inner.lock().unwrap();
        match map.get(queue) {
            Some(s) => ClaimScanStart {
                scan_from: s.scan_from.clone(),
                epoch: s.epoch,
                known_empty: s.empty_as_of == Some(s.epoch),
            },
            None => ClaimScanStart {
                scan_from: None,
                epoch: 0,
                known_empty: false,
            },
        }
    }

    /// Advance the scan start past `claimed`, without ever moving it
    /// past a key the claim could not have observed. The advance is
    /// dropped entirely if an insert moved the bound back while the
    /// claim was in flight (the next claim scans from the moved bound
    /// instead), and it is clamped to the smallest key inserted ahead
    /// of the bound since the previous advance, because such a key may
    /// have committed after the claim's snapshot yet sort below
    /// `claimed`.
    pub(crate) fn advance(&self, queue: &str, claimed: Bytes, observed: &ClaimScanStart) {
        let mut map = self.inner.lock().unwrap();
        let s = map.entry(queue.to_string()).or_default();
        if s.scan_from != observed.scan_from {
            return;
        }
        s.scan_from = match s.min_insert_ahead.take() {
            Some(min) if min.as_ref() < claimed.as_ref() => Some(ScanFrom {
                key: min,
                inclusive: true,
            }),
            _ => Some(ScanFrom {
                key: claimed,
                inclusive: false,
            }),
        };
    }

    /// Record that a full `pending:` prefix scan found nothing, as
    /// observed at `epoch` (the value returned by
    /// [`Self::begin_claim`] for the same attempt). The scan-start
    /// bound is kept: nothing is live behind it, and inserts landing
    /// behind it move it themselves. Claims short-circuit to `None`
    /// until the next insert bumps the epoch past `epoch`.
    pub(crate) fn mark_empty(&self, queue: &str, epoch: u64) {
        let mut map = self.inner.lock().unwrap();
        let s = map.entry(queue.to_string()).or_default();
        s.empty_as_of = Some(epoch);
    }

    /// Record one committed `pending:` insert. See
    /// [`Self::note_pending_inserts`] for the semantics, including why
    /// this must be called after the insert's transaction commits.
    pub(crate) fn note_pending_insert(&self, queue: &str, new_key: &str) {
        self.note_pending_inserts(queue, new_key, 1);
    }

    /// Record `count` committed `pending:` inserts whose smallest key
    /// is `min_key`: bump the epoch, revoking any emptiness recorded
    /// against an earlier one, move the scan-start bound back to
    /// include `min_key` if it would otherwise be skipped, and issue
    /// one queue-scoped wakeup per insert (capped) so waiting workers
    /// wake one per job. When the queue was known empty at the time
    /// this insert is recorded (a prior claim's scan found nothing and
    /// no insert has been recorded since), no key from before that
    /// scan is live, so the bound moves directly to `min_key` even
    /// when no bound existed; a concurrent insert whose key sorts
    /// below `min_key` moves the bound back when it is itself
    /// recorded. Every site that writes `pending:`
    /// keys (enqueue, batch enqueue, nack-requeue, dead-job requeue,
    /// reaper-requeue, scheduler promotion) calls this *after* its
    /// transaction commits. Calling it before the commit would let a
    /// concurrent claim scan miss the job, record emptiness at the
    /// already-bumped epoch, and strand the job until the next
    /// insert.
    pub(crate) fn note_pending_inserts(&self, queue: &str, min_key: &str, count: usize) {
        let wakeup = {
            let mut map = self.inner.lock().unwrap();
            let s = map.entry(queue.to_string()).or_default();
            let was_known_empty = s.empty_as_of == Some(s.epoch);
            s.epoch += 1;
            let include_min_key = match &s.scan_from {
                _ if was_known_empty => true,
                Some(sf) => {
                    min_key.as_bytes() < sf.key.as_ref()
                        || (min_key.as_bytes() == sf.key.as_ref() && !sf.inclusive)
                }
                // Bound unknown (cold start or restart): pre-existing keys
                // may be live, so the front-scan fallback must stay in
                // charge.
                None => false,
            };
            if include_min_key {
                s.scan_from = Some(ScanFrom {
                    key: Bytes::copy_from_slice(min_key.as_bytes()),
                    inclusive: true,
                });
                s.min_insert_ahead = None;
            } else if s.scan_from.is_some() {
                // The key is ahead of the current bound, but an in-flight
                // claim may be about to advance the bound past it; record
                // it so that advance clamps. Keys behind the bound moved
                // the bound itself above, which subsumes the clamp.
                let key = Bytes::copy_from_slice(min_key.as_bytes());
                let is_new_min = s
                    .min_insert_ahead
                    .as_ref()
                    .is_none_or(|min| key.as_ref() < min.as_ref());
                if is_new_min {
                    s.min_insert_ahead = Some(key);
                }
            }
            s.wakeup.clone()
        };
        for _ in 0..count.min(MAX_INSERT_WAKEUPS) {
            wakeup.notify_one();
        }
    }

    /// The queue-scoped wakeup that [`Self::note_pending_inserts`]
    /// notifies, one `notify_one` per recorded insert. `notify_one`
    /// leaves a permit when no task is waiting, so a waiter that
    /// subscribes after an insert still wakes immediately.
    pub(crate) fn wakeup_for(&self, queue: &str) -> Arc<Notify> {
        self.inner
            .lock()
            .unwrap()
            .entry(queue.to_string())
            .or_default()
            .wakeup
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan_from(key: &'static [u8], inclusive: bool) -> Option<ScanFrom> {
        Some(ScanFrom {
            key: Bytes::from_static(key),
            inclusive,
        })
    }

    #[test]
    fn begin_claim_on_unknown_queue_is_not_known_empty() {
        let state = ClaimCursor::new();
        let scan = state.begin_claim("q");
        assert!(scan.scan_from.is_none());
        assert!(!scan.known_empty);
    }

    #[test]
    fn mark_empty_short_circuits_until_next_insert() {
        let state = ClaimCursor::new();
        let scan = state.begin_claim("q");
        state.mark_empty("q", scan.epoch);

        assert!(state.begin_claim("q").known_empty);

        state.note_pending_insert("q", "pending:q:00000000:job-1");
        assert!(!state.begin_claim("q").known_empty);
    }

    #[test]
    fn insert_between_begin_and_mark_revokes_emptiness() {
        let state = ClaimCursor::new();
        let scan = state.begin_claim("q");
        state.note_pending_insert("q", "pending:q:00000000:job-1");
        state.mark_empty("q", scan.epoch);

        assert!(!state.begin_claim("q").known_empty);
    }

    #[test]
    fn mark_empty_keeps_the_scan_bound() {
        let state = ClaimCursor::new();
        let scan = state.begin_claim("q");
        state.advance("q", Bytes::from_static(b"pending:q:00000000:job-5"), &scan);
        let scan = state.begin_claim("q");
        state.mark_empty("q", scan.epoch);

        assert_eq!(
            state.begin_claim("q").scan_from,
            scan_from(b"pending:q:00000000:job-5", false),
        );
    }

    #[test]
    fn insert_behind_the_bound_moves_it_back_inclusively() {
        let state = ClaimCursor::new();
        let scan = state.begin_claim("q");
        state.advance("q", Bytes::from_static(b"pending:q:00000000:job-5"), &scan);

        state.note_pending_insert("q", "pending:q:00000000:job-9");
        assert_eq!(
            state.begin_claim("q").scan_from,
            scan_from(b"pending:q:00000000:job-5", false),
        );

        state.note_pending_insert("q", "pending:q:00000000:job-3");
        assert_eq!(
            state.begin_claim("q").scan_from,
            scan_from(b"pending:q:00000000:job-3", true),
        );
    }

    #[test]
    fn reinsert_of_the_claimed_key_becomes_inclusive() {
        let state = ClaimCursor::new();
        let scan = state.begin_claim("q");
        state.advance("q", Bytes::from_static(b"pending:q:00000000:job-5"), &scan);

        state.note_pending_insert("q", "pending:q:00000000:job-5");
        assert_eq!(
            state.begin_claim("q").scan_from,
            scan_from(b"pending:q:00000000:job-5", true),
        );
    }

    #[test]
    fn insert_while_known_empty_sets_the_bound_without_a_prior_one() {
        let state = ClaimCursor::new();
        let scan = state.begin_claim("q");
        state.mark_empty("q", scan.epoch);

        state.note_pending_insert("q", "pending:q:00000000:job-1");
        assert_eq!(
            state.begin_claim("q").scan_from,
            scan_from(b"pending:q:00000000:job-1", true),
        );
    }

    #[test]
    fn insert_with_unknown_bound_leaves_front_scan_in_charge() {
        let state = ClaimCursor::new();
        state.note_pending_insert("q", "pending:q:00000000:job-1");
        assert!(state.begin_claim("q").scan_from.is_none());
    }

    #[test]
    fn advance_clamps_to_key_inserted_ahead_during_the_claim() {
        let state = ClaimCursor::new();
        let scan = state.begin_claim("q");
        state.advance("q", Bytes::from_static(b"pending:q:00000000:job-2"), &scan);

        // While a claim that observed the bound at job-2 is in flight,
        // job-3 commits. It is ahead of the bound, so it does not move
        // it, but it sorts below the keys the claim is about to
        // advance past.
        let observed = state.begin_claim("q");
        state.note_pending_insert("q", "pending:q:00000000:job-3");
        state.advance(
            "q",
            Bytes::from_static(b"pending:q:00000000:job-5"),
            &observed,
        );

        assert_eq!(
            state.begin_claim("q").scan_from,
            scan_from(b"pending:q:00000000:job-3", true),
        );
    }

    #[test]
    fn advance_clamp_is_consumed_by_one_advance() {
        let state = ClaimCursor::new();
        let scan = state.begin_claim("q");
        state.advance("q", Bytes::from_static(b"pending:q:00000000:job-2"), &scan);

        let observed = state.begin_claim("q");
        state.note_pending_insert("q", "pending:q:00000000:job-3");
        state.advance(
            "q",
            Bytes::from_static(b"pending:q:00000000:job-5"),
            &observed,
        );

        let observed = state.begin_claim("q");
        state.advance(
            "q",
            Bytes::from_static(b"pending:q:00000000:job-5"),
            &observed,
        );
        assert_eq!(
            state.begin_claim("q").scan_from,
            scan_from(b"pending:q:00000000:job-5", false),
        );
    }

    #[test]
    fn advance_ignores_clamp_keys_at_or_past_the_claimed_key() {
        let state = ClaimCursor::new();
        let scan = state.begin_claim("q");
        state.advance("q", Bytes::from_static(b"pending:q:00000000:job-2"), &scan);

        let observed = state.begin_claim("q");
        state.note_pending_insert("q", "pending:q:00000000:job-9");
        state.advance(
            "q",
            Bytes::from_static(b"pending:q:00000000:job-5"),
            &observed,
        );

        assert_eq!(
            state.begin_claim("q").scan_from,
            scan_from(b"pending:q:00000000:job-5", false),
        );
    }

    #[test]
    fn advance_is_dropped_when_the_bound_moved_during_the_claim() {
        let state = ClaimCursor::new();
        let scan = state.begin_claim("q");
        state.advance("q", Bytes::from_static(b"pending:q:00000000:job-5"), &scan);

        let observed = state.begin_claim("q");
        state.note_pending_insert("q", "pending:q:00000000:job-3");
        state.advance(
            "q",
            Bytes::from_static(b"pending:q:00000000:job-7"),
            &observed,
        );

        assert_eq!(
            state.begin_claim("q").scan_from,
            scan_from(b"pending:q:00000000:job-3", true),
        );
    }
}
