use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use slatedb::IsolationLevel;
use tokio::sync::Notify;

use crate::error::Result;
use crate::keys::{KeyTag, cursor_key, tag_prefix};
use crate::queue_core::QueueCore;

/// Upper bound on wakeups issued for one batch of inserts. Beyond the
/// cap, woken workers drain the backlog by looping on claim, and
/// `Notify::notify_one` stores at most one permit when no task is
/// waiting, so extra calls would be wasted work.
const MAX_INSERT_WAKEUPS: usize = 64;

/// Per-queue in-process claim state: a scan-start bound, a
/// pending-insert epoch, the insert wakeup and the claim lock.
///
/// The scan-start bound is the position the next claim scans from,
/// skipping the tombstone band left by previously claimed (and
/// deleted) pending entries. After a claim it excludes the claimed
/// key; after an insert that lands at or before it, it moves back to
/// include the inserted key. The invariant is that every live
/// pending key sorts at or after the bound, so the claim path only
/// falls back to a front prefix scan when the bound is unknown (cold
/// start or process restart).
///
/// The epoch counts committed pending inserts. When a claim's full
/// prefix scan finds nothing, it records the epoch it observed before
/// its transaction began; until the next insert bumps the epoch,
/// subsequent claims return `None` without scanning. Without this,
/// every poll of an empty queue re-scans the tombstone band from the
/// front, which grows with every job claimed since the last
/// compaction.
///
/// Shared across the queue, reaper, and scheduler via `Clone`; all
/// clones reference the same in-memory map. The bound and emptiness
/// marker survive a clean close ([`Self::export`] / [`Self::restore`]);
/// after a crash the first claim falls back to a prefix scan and
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
    /// Bumped after every committed pending insert.
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
    /// Held across the queue's claim transaction, so same-queue claim
    /// attempts serialise here in place of a transaction-conflict
    /// retry. Per queue, so different queues' claims run in parallel.
    claim_lock: Arc<tokio::sync::Mutex<()>>,
}

/// Snapshot of one queue's claim-scan state, taken at the start of a
/// claim attempt.
pub(crate) struct ClaimScanStart {
    pub(crate) scan_from: Option<ScanFrom>,
    pub(crate) epoch: u64,
    pub(crate) known_empty: bool,
}

/// One queue's persistable claim-scan state: the scan bound and
/// whether a full scan has proven the queue empty. Exported at clean
/// close and restored at the next open.
pub(crate) struct CursorState {
    pub(crate) scan_from: Option<ScanFrom>,
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
    /// have committed after the claim's snapshot yet sort at or below
    /// `claimed` (a key equal to `claimed` is a reinsert of the job
    /// the claim took, requeued after its lease expired within the
    /// claim).
    pub(crate) fn advance(&self, queue: &str, claimed: Bytes, observed: &ClaimScanStart) {
        let mut map = self.inner.lock().unwrap();
        let s = map.entry(queue.to_string()).or_default();
        if s.scan_from != observed.scan_from {
            return;
        }
        s.scan_from = match s.min_insert_ahead.take() {
            Some(min) if min.as_ref() <= claimed.as_ref() => Some(ScanFrom {
                key: min,
                inclusive: true,
            }),
            _ => Some(ScanFrom {
                key: claimed,
                inclusive: false,
            }),
        };
    }

    /// Record that a full pending prefix scan found nothing, as
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

    /// Record one committed pending insert. See
    /// [`Self::note_pending_inserts`] for the semantics, including why
    /// this must be called after the insert's transaction commits.
    pub(crate) fn note_pending_insert(&self, queue: &str, new_key: &[u8]) {
        self.note_pending_inserts(queue, new_key, 1);
    }

    /// Record `count` committed pending inserts whose smallest key
    /// is `min_key`: bump the epoch, revoking any emptiness recorded
    /// against an earlier one, update the scan state so no claim can
    /// miss the key, and issue one queue-scoped wakeup per insert
    /// (capped) so waiting workers wake one per job.
    ///
    /// The scan-start bound moves back to include `min_key` when a
    /// scan from it would otherwise skip the key. When no bound
    /// exists, one is set only if a prior scan proved the queue empty
    /// (no insert recorded since); otherwise keys from before this
    /// process may be live and claims must keep falling back to the
    /// front scan. A key a scan would already yield is recorded for
    /// [`Self::advance`] to clamp to, because an in-flight claim may
    /// otherwise advance the bound past it.
    ///
    /// Every site that writes pending keys (enqueue, batch
    /// enqueue, nack-requeue, dead-job requeue, reaper-requeue,
    /// scheduler promotion) calls this *after* its transaction
    /// commits. Calling it before the commit would let a concurrent
    /// claim scan miss the job, record emptiness at the already-bumped
    /// epoch, and strand the job until the next insert.
    pub(crate) fn note_pending_inserts(&self, queue: &str, min_key: &[u8], count: usize) {
        let wakeup = {
            let mut map = self.inner.lock().unwrap();
            let s = map.entry(queue.to_string()).or_default();
            let was_known_empty = s.empty_as_of == Some(s.epoch);
            s.epoch += 1;
            let include_min_key = match &s.scan_from {
                // Move the bound back when a scan from it would skip
                // the key.
                Some(sf) => {
                    min_key < sf.key.as_ref() || (min_key == sf.key.as_ref() && !sf.inclusive)
                }
                // Bound unknown (cold start or restart): a bound may be
                // set only when a scan has proven the queue empty;
                // otherwise keys from before this process may be live
                // and claims must keep falling back to the front scan.
                None => was_known_empty,
            };
            if include_min_key {
                s.scan_from = Some(ScanFrom {
                    key: Bytes::copy_from_slice(min_key),
                    inclusive: true,
                });
                s.min_insert_ahead = None;
            } else {
                // A scan would already yield the key (or no bound is
                // set yet), but an in-flight claim may be about to
                // advance the bound past it; record it so that advance
                // clamps.
                let key = Bytes::copy_from_slice(min_key);
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

    /// Export every queue's persistable state. Queues with neither a
    /// bound nor recorded emptiness are omitted; their next claim
    /// falls back to the front prefix scan regardless.
    pub(crate) fn export(&self) -> Vec<(String, CursorState)> {
        let map = self.inner.lock().unwrap();
        map.iter()
            .filter_map(|(queue, s)| {
                let known_empty = s.empty_as_of == Some(s.epoch);
                if s.scan_from.is_none() && !known_empty {
                    return None;
                }
                Some((
                    queue.clone(),
                    CursorState {
                        scan_from: s.scan_from.clone(),
                        known_empty,
                    },
                ))
            })
            .collect()
    }

    /// Restore one queue's state from a record persisted at the
    /// previous clean close. Must be called before the queue serves
    /// traffic: the persisted state is valid because nothing mutates
    /// the store while it is closed, and any insert after this call
    /// updates the restored state through the normal paths.
    pub(crate) fn restore(&self, queue: &str, state: CursorState) {
        let mut map = self.inner.lock().unwrap();
        let s = map.entry(queue.to_string()).or_default();
        s.scan_from = state.scan_from;
        if state.known_empty {
            s.empty_as_of = Some(s.epoch);
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

    /// The mutex a claim on `queue` holds across its scan and commit.
    pub(crate) fn claim_lock_for(&self, queue: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.inner
            .lock()
            .unwrap()
            .entry(queue.to_string())
            .or_default()
            .claim_lock
            .clone()
    }
}

/// On-disk form of one queue's claim-scan state, stored under
/// [`cursor_key`]. Written only by a clean
/// [`Queue::close`](crate::Queue::close); the next open deletes the
/// record before it accepts any call, so a record is never observed
/// after the state it describes could have changed.
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedCursor {
    /// Queue the state belongs to; stored in the record so the
    /// reader does not parse it out of the key.
    queue: String,
    /// Scan bound key, when one was established.
    bound_key: Option<Vec<u8>>,
    /// Whether the bound key itself may be live.
    bound_inclusive: bool,
    /// Whether a full scan had proven the queue empty at close.
    known_empty: bool,
}

/// Write each queue's claim-scan state under its cursor key. Runs
/// after the background tasks have stopped; `close` consumes the
/// handle, so the exported state cannot change between the export and
/// the database closing.
pub(crate) async fn persist_cursor_state(core: &QueueCore) -> Result<()> {
    let states = core.claim_cursor.export();
    if states.is_empty() {
        return Ok(());
    }
    let txn = core.db.begin(IsolationLevel::Snapshot).await?;
    for (queue, state) in states {
        let record = PersistedCursor {
            queue: queue.clone(),
            bound_key: state.scan_from.as_ref().map(|sf| sf.key.to_vec()),
            bound_inclusive: state.scan_from.is_some_and(|sf| sf.inclusive),
            known_empty: state.known_empty,
        };
        txn.put(cursor_key(&queue), &rmp_serde::to_vec_named(&record)?)?;
    }
    txn.commit().await?;
    Ok(())
}

/// Restore the claim cursor from cursor records persisted by the
/// previous clean close, then durably delete them before the queue
/// accepts any call. A record is valid only as of the close that wrote
/// it: once inserts resume the live bound can move behind the
/// persisted one, so a crash before the delete is durable would leave
/// a record whose stale bound makes a later open skip the jobs behind
/// it.
pub(crate) async fn restore_cursor_state(core: &QueueCore) -> Result<()> {
    let txn = core.db.begin(IsolationLevel::Snapshot).await?;
    let mut records = Vec::new();
    {
        let mut iter = txn.scan_prefix(tag_prefix(KeyTag::Cursor), ..).await?;
        while let Some(kv) = iter.next().await? {
            let record: PersistedCursor = rmp_serde::from_slice(&kv.value)?;
            records.push((kv.key, record));
        }
    }
    if records.is_empty() {
        return Ok(());
    }
    for (key, record) in records {
        core.claim_cursor.restore(
            &record.queue,
            CursorState {
                scan_from: record.bound_key.map(|key| ScanFrom {
                    key: Bytes::from(key),
                    inclusive: record.bound_inclusive,
                }),
                known_empty: record.known_empty,
            },
        );
        txn.delete(&key)?;
    }
    txn.commit().await?;
    Ok(())
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

        state.note_pending_insert("q", b"pending:q:00000000:job-1");
        assert!(!state.begin_claim("q").known_empty);
    }

    #[test]
    fn insert_between_begin_and_mark_revokes_emptiness() {
        let state = ClaimCursor::new();
        let scan = state.begin_claim("q");
        state.note_pending_insert("q", b"pending:q:00000000:job-1");
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

        state.note_pending_insert("q", b"pending:q:00000000:job-9");
        assert_eq!(
            state.begin_claim("q").scan_from,
            scan_from(b"pending:q:00000000:job-5", false),
        );

        state.note_pending_insert("q", b"pending:q:00000000:job-3");
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

        state.note_pending_insert("q", b"pending:q:00000000:job-5");
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

        state.note_pending_insert("q", b"pending:q:00000000:job-1");
        assert_eq!(
            state.begin_claim("q").scan_from,
            scan_from(b"pending:q:00000000:job-1", true),
        );
    }

    #[test]
    fn insert_with_unknown_bound_keeps_the_front_scan_fallback() {
        let state = ClaimCursor::new();
        state.note_pending_insert("q", b"pending:q:00000000:job-1");
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
        state.note_pending_insert("q", b"pending:q:00000000:job-3");
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
        state.note_pending_insert("q", b"pending:q:00000000:job-3");
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
    fn advance_clamps_to_a_reinsert_of_the_claimed_key() {
        let state = ClaimCursor::new();
        let scan = state.begin_claim("q");
        state.advance("q", Bytes::from_static(b"pending:q:00000000:job-2"), &scan);

        // The claim takes job-5, whose lease expires within the claim,
        // and the reaper requeues it at its original key before the
        // claim's bound update runs.
        let observed = state.begin_claim("q");
        state.note_pending_insert("q", b"pending:q:00000000:job-5");
        state.advance(
            "q",
            Bytes::from_static(b"pending:q:00000000:job-5"),
            &observed,
        );

        assert_eq!(
            state.begin_claim("q").scan_from,
            scan_from(b"pending:q:00000000:job-5", true),
        );
    }

    #[test]
    fn advance_ignores_clamp_keys_past_the_claimed_key() {
        let state = ClaimCursor::new();
        let scan = state.begin_claim("q");
        state.advance("q", Bytes::from_static(b"pending:q:00000000:job-2"), &scan);

        let observed = state.begin_claim("q");
        state.note_pending_insert("q", b"pending:q:00000000:job-9");
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
    fn advance_clamps_to_key_inserted_during_a_cold_start_claim() {
        let state = ClaimCursor::new();

        // A cold-start claim observes no bound and front-scans. While
        // it runs, a reaper requeue inserts a key that sorts below the
        // keys the claim will advance past.
        let observed = state.begin_claim("q");
        state.note_pending_insert("q", b"pending:q:00000000:job-1");
        state.advance(
            "q",
            Bytes::from_static(b"pending:q:00000000:job-5"),
            &observed,
        );

        assert_eq!(
            state.begin_claim("q").scan_from,
            scan_from(b"pending:q:00000000:job-1", true),
        );
    }

    #[test]
    fn restore_sets_bound_and_emptiness() {
        let state = ClaimCursor::new();
        state.restore(
            "q",
            CursorState {
                scan_from: scan_from(b"pending:q:00000000:job-5", false),
                known_empty: true,
            },
        );

        let scan = state.begin_claim("q");
        assert_eq!(
            scan.scan_from,
            scan_from(b"pending:q:00000000:job-5", false)
        );
        assert!(scan.known_empty);
    }

    #[test]
    fn restored_emptiness_is_revoked_by_an_insert() {
        let state = ClaimCursor::new();
        state.restore(
            "q",
            CursorState {
                scan_from: None,
                known_empty: true,
            },
        );

        state.note_pending_insert("q", b"pending:q:00000000:job-1");
        let scan = state.begin_claim("q");
        assert!(!scan.known_empty);
        assert_eq!(scan.scan_from, scan_from(b"pending:q:00000000:job-1", true));
    }

    #[test]
    fn export_skips_queues_without_persistable_state() {
        let state = ClaimCursor::new();
        let scan = state.begin_claim("q1");
        state.advance(
            "q1",
            Bytes::from_static(b"pending:q1:00000000:job-1"),
            &scan,
        );
        let _ = state.wakeup_for("q2");

        let mut exported = state.export();
        assert_eq!(exported.len(), 1);
        let (queue, cursor) = exported.pop().unwrap();
        assert_eq!(queue, "q1");
        assert_eq!(
            cursor.scan_from,
            scan_from(b"pending:q1:00000000:job-1", false),
        );
        assert!(!cursor.known_empty);
    }

    #[test]
    fn advance_is_dropped_when_the_bound_moved_during_the_claim() {
        let state = ClaimCursor::new();
        let scan = state.begin_claim("q");
        state.advance("q", Bytes::from_static(b"pending:q:00000000:job-5"), &scan);

        let observed = state.begin_claim("q");
        state.note_pending_insert("q", b"pending:q:00000000:job-3");
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

    use crate::test_util::*;

    #[tokio::test]
    async fn claim_finds_job_enqueued_after_empty_polls() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        assert!(q.claim("work", lease).await.unwrap().is_none());
        assert!(q.claim("work", lease).await.unwrap().is_none());

        q.enqueue("work", b"job".to_vec()).await.unwrap();

        let job = q.claim("work", lease).await.unwrap().unwrap();
        assert_eq!(job.payload, b"job");
        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn claim_finds_batch_enqueued_after_queue_drained() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        q.enqueue("work", b"first".to_vec()).await.unwrap();
        let first = q.claim("work", lease).await.unwrap().unwrap();
        q.ack(&first).await.unwrap();
        assert!(q.claim("work", lease).await.unwrap().is_none());
        assert!(q.claim("work", lease).await.unwrap().is_none());

        q.enqueue_batch("work", vec![b"second".to_vec()])
            .await
            .unwrap();

        let second = q.claim("work", lease).await.unwrap().unwrap();
        assert_eq!(second.payload, b"second");
        q.ack(&second).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn enqueue_wakes_one_waiting_worker_per_job() {
        let q = Arc::new(Queue::open(make_store(), "test").await.unwrap());
        let lease = Duration::from_secs(5);
        let max_wait = Duration::from_secs(60);

        let mut waiters = Vec::new();
        for _ in 0..3 {
            let q = q.clone();
            waiters.push(tokio::spawn(async move {
                q.claim_with_wait("work", lease, max_wait).await.unwrap()
            }));
        }
        tokio::task::yield_now().await;

        q.enqueue("work", b"job".to_vec()).await.unwrap();

        let mut claimed = 0;
        for handle in waiters {
            if let Some(job) = handle.await.unwrap() {
                claimed += 1;
                q.ack(&job).await.unwrap();
            }
        }
        assert_eq!(claimed, 1, "exactly one waiter wakes with the job");
    }

    #[tokio::test(start_paused = true)]
    async fn batch_enqueue_wakes_one_waiting_worker_per_job() {
        let q = Arc::new(Queue::open(make_store(), "test").await.unwrap());
        let lease = Duration::from_secs(5);
        let max_wait = Duration::from_secs(60);

        let mut waiters = Vec::new();
        for _ in 0..3 {
            let q = q.clone();
            waiters.push(tokio::spawn(async move {
                q.claim_with_wait("work", lease, max_wait).await.unwrap()
            }));
        }
        tokio::task::yield_now().await;

        q.enqueue_batch("work", vec![b"a".to_vec(), b"b".to_vec()])
            .await
            .unwrap();

        let mut claimed = 0;
        for handle in waiters {
            if let Some(job) = handle.await.unwrap() {
                claimed += 1;
                q.ack(&job).await.unwrap();
            }
        }
        assert_eq!(claimed, 2, "one waiter wakes per inserted job");
    }

    #[tokio::test(start_paused = true)]
    async fn claim_with_wait_waits_full_deadline_despite_stale_permit() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);

        // A successful claim_with_wait passes the wakeup on, leaving a
        // stale permit behind when no task is waiting.
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q
            .claim_with_wait("work", lease, Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();

        let start = tokio::time::Instant::now();
        let next = q
            .claim_with_wait("work", lease, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(next.is_none());
        assert!(
            start.elapsed() >= Duration::from_secs(5),
            "stale permit must not end the wait early",
        );
        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn wait_for_jobs_on_consumes_permit_from_earlier_insert() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        q.enqueue("work", b"job".to_vec()).await.unwrap();

        let start = tokio::time::Instant::now();
        q.wait_for_jobs_on("work", Duration::from_secs(60)).await;
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "insert before the wait must wake it via the stored permit",
        );

        let job = q.claim("work", Duration::from_secs(5)).await.unwrap();
        q.ack(&job.unwrap()).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn claim_batch_claims_in_order_up_to_max() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        for payload in [b"a", b"b", b"c", b"d", b"e"] {
            q.enqueue("work", payload.to_vec()).await.unwrap();
        }

        let first = q.claim_batch("work", 3, lease).await.unwrap();
        assert_eq!(
            first
                .iter()
                .map(|j| j.payload.as_slice())
                .collect::<Vec<_>>(),
            [b"a", b"b", b"c"],
        );
        for job in &first {
            assert_eq!(job.status, JobStatus::Claimed);
            assert_eq!(job.attempts, 1);
            assert!(q.lease_expiry("work", &job.id).is_some());
        }

        let rest = q.claim_batch("work", 3, lease).await.unwrap();
        assert_eq!(
            rest.iter()
                .map(|j| j.payload.as_slice())
                .collect::<Vec<_>>(),
            [b"d", b"e"],
        );

        for job in first.iter().chain(rest.iter()) {
            q.ack(job).await.unwrap();
        }
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn claim_batch_zero_max_claims_nothing() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        q.enqueue("work", b"job".to_vec()).await.unwrap();

        assert!(
            q.claim_batch("work", 0, Duration::from_secs(5))
                .await
                .unwrap()
                .is_empty(),
        );

        let job = q
            .claim("work", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn partial_claim_batch_marks_empty_until_next_enqueue() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        q.enqueue("work", b"only".to_vec()).await.unwrap();

        let batch = q.claim_batch("work", 8, lease).await.unwrap();
        assert_eq!(batch.len(), 1);
        assert!(q.claim("work", lease).await.unwrap().is_none());

        q.enqueue("work", b"next".to_vec()).await.unwrap();
        let next = q.claim("work", lease).await.unwrap().unwrap();
        assert_eq!(next.payload, b"next");

        q.ack(&batch[0]).await.unwrap();
        q.ack(&next).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn claim_finds_job_requeued_by_nack_after_empty_poll() {
        let q = Queue::open_with_options(make_store(), "test", no_backoff_opts())
            .await
            .unwrap();
        let lease = Duration::from_secs(5);
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();
        assert!(q.claim("work", lease).await.unwrap().is_none());

        q.nack(&job, "retry").await.unwrap();

        let retried = q.claim("work", lease).await.unwrap().unwrap();
        assert_eq!(retried.payload, b"job");
        assert_eq!(retried.attempts, 2);
        q.ack(&retried).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cursor_bound_persists_across_a_clean_close() {
        let store = make_store();
        let lease = Duration::from_secs(5);
        let q = Queue::open(store.clone(), "test").await.unwrap();
        q.enqueue("work", b"first".to_vec()).await.unwrap();
        q.enqueue("work", b"second".to_vec()).await.unwrap();
        let first = q.claim("work", lease).await.unwrap().unwrap();
        q.ack(&first).await.unwrap();
        q.close().await.unwrap();

        let q = Queue::open(store, "test").await.unwrap();
        let scan = q.core.claim_cursor.begin_claim("work");
        assert!(scan.scan_from.is_some());
        assert!(!scan.known_empty);
        assert!(
            q.core.db.get(cursor_key("work")).await.unwrap().is_none(),
            "the cursor record is consumed at open",
        );

        let second = q.claim("work", lease).await.unwrap().unwrap();
        assert_eq!(second.payload, b"second");
        q.ack(&second).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cursor_emptiness_persists_across_a_clean_close() {
        let store = make_store();
        let lease = Duration::from_secs(5);
        let q = Queue::open(store.clone(), "test").await.unwrap();
        q.enqueue("work", b"only".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.ack(&job).await.unwrap();
        assert!(q.claim("work", lease).await.unwrap().is_none());
        q.close().await.unwrap();

        let q = Queue::open(store, "test").await.unwrap();
        assert!(q.core.claim_cursor.begin_claim("work").known_empty);

        q.enqueue("work", b"revives".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();
        assert_eq!(job.payload, b"revives");
        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn restored_bound_moves_back_for_an_insert_behind_it() {
        let store = make_store();
        let lease = Duration::from_secs(5);
        let q = Queue::open(store.clone(), "test").await.unwrap();
        q.enqueue("work", b"normal-1".to_vec()).await.unwrap();
        q.enqueue("work", b"normal-2".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.ack(&job).await.unwrap();
        q.close().await.unwrap();

        // A high-priority job sorts before the restored bound, which
        // sits in the normal-priority band.
        let q = Queue::open(store, "test").await.unwrap();
        q.enqueue_with(
            "work",
            b"urgent".to_vec(),
            EnqueueOptions {
                priority: Some(PRIORITY_HIGH),
                ..EnqueueOptions::default()
            },
        )
        .await
        .unwrap();

        let job = q.claim("work", lease).await.unwrap().unwrap();
        assert_eq!(job.payload, b"urgent");
        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }
}
