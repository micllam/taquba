//! The claim transitions of [`QueueCore`]: the scan claim of a batch and the
//! claim of one job by id.

use std::ops::Bound;
use std::time::Duration;

use slatedb::IsolationLevel;
use slatedb::config::ScanOptions;
use tracing::{debug, warn};
use ulid::Ulid;

use crate::error::Result;
use crate::job::{Claim, JobRecord, JobStatus};
use crate::keys::{QueueName, dedup_index_key, pending_prefix};
use crate::queue::ClaimOutcome;
use crate::queue_core::QueueCore;
use crate::txn::{Attempt, Commit, Durability, Txn};

/// Generate a claim id. A ULID's low 64 bits fall inside its 80-bit random
/// component, so claim ids are distinct across claims of the same job. The
/// value identifies a claim and is not ordered, so it fences only against this
/// queue's own state and is not a fencing token for anything outside it.
fn new_claim_id() -> u64 {
    Ulid::new().0 as u64
}

impl QueueCore {
    /// Claim up to `max_jobs` pending jobs of `queue` in one transaction, with
    /// their offloaded payloads fetched. `max_jobs` must be at least 1.
    pub(crate) async fn claim_batch(
        &self,
        queue: &QueueName,
        max_jobs: usize,
        lease_duration: Duration,
    ) -> Result<Vec<Claim>> {
        // The empty checks report from in-process state, which outlives a close
        // of the store. A transaction reports the close.
        if self.closed.is_cancelled() {
            drop(self.db.begin(IsolationLevel::Snapshot)?);
        }
        // Empty check before taking the claim lock: a queue known to be empty
        // is reported from in-process state, without contention with claims
        // that have work to do. A stale value is safe in both directions.
        // Emptiness is revoked only by an insert, and a stale "not empty" falls
        // through to the locked scan.
        if self.claim_cursor.known_empty(queue) {
            return Ok(Vec::new());
        }
        let mut jobs = {
            let lock = self.claim_cursor.claim_lock_for(queue);
            let _guard = lock.lock().await;
            self.claim_batch_locked(queue, max_jobs, lease_duration)
                .await?
        };
        // Offloaded payloads are fetched after the claim lock is released, so
        // other claims on the queue proceed during the object-store reads. On a
        // fetch failure the claim has already committed: the affected jobs stay
        // claimed until their leases expire and are then redelivered. Their
        // cancel tokens stay registered, so a cancel during that window still
        // fires the token and persists the request.
        self.materialize_payloads(&mut jobs).await?;
        Ok(jobs)
    }

    /// The scan-and-claim transaction of [`Self::claim_batch`]. The caller must
    /// hold the queue's claim lock for the duration of the call. Offloaded
    /// payloads are not fetched here, so the lock is never held across a
    /// payload read.
    async fn claim_batch_locked(
        &self,
        queue: &QueueName,
        max_jobs: usize,
        lease_duration: Duration,
    ) -> Result<Vec<Claim>> {
        let prefix = pending_prefix(queue);
        let prefix_bytes = prefix.as_slice();
        let timer = crate::obs::start();
        loop {
            // The scan state (and its pending-insert epoch) is read before the
            // transaction begins, so any insert the snapshot can miss bumps the
            // epoch after this read and revokes the emptiness recorded below.
            let scan = self.claim_cursor.begin_claim(queue);
            if scan.known_empty {
                return Ok(Vec::new());
            }
            let mut txn = self.begin()?;

            let mut candidates = Vec::new();
            // Set when the scan ran out of pending keys before filling the
            // batch, proving nothing is live beyond the candidates.
            let mut drained = false;
            // SlateDB leaves block caching off for scans. This scan takes at
            // most `max_jobs` entries from one prefix and the next claim
            // resumes where it stopped, so uncached it re-reads the same block
            // once a compacted sorted run overlaps the prefix.
            let scan_options = ScanOptions::default().with_cache_blocks(true);
            let mut iter = match scan.scan_from.clone() {
                // Resume from the recorded bound (after the last claimed key,
                // or a key inserted before it). The subrange is relative to the
                // prefix, so scan_prefix ends at the prefix upper bound
                // natively and a drained queue is detected without scanning
                // beyond the prefix.
                Some(sf) => {
                    let suffix = sf.key.slice(prefix_bytes.len()..);
                    let start = if sf.inclusive {
                        Bound::Included(suffix)
                    } else {
                        Bound::Excluded(suffix)
                    };
                    txn.scan_prefix_with_options(
                        prefix_bytes,
                        (start, Bound::Unbounded),
                        &scan_options,
                    )
                    .await?
                }
                // Front scan: bound unknown (the first claim after the queue
                // opens), so pre-existing keys can be live anywhere in the
                // prefix.
                None => {
                    txn.scan_prefix_with_options(prefix_bytes, .., &scan_options)
                        .await?
                }
            };
            while candidates.len() < max_jobs {
                match iter.next().await? {
                    Some(c) => candidates.push(c),
                    None => {
                        drained = true;
                        break;
                    }
                }
            }
            if candidates.is_empty() {
                // Every live pending key sorts at or after a known bound (an
                // insert before it moves it back), so a bound scan that ends
                // without a key establishes that the queue is empty, without a
                // read of the tombstone band from the front.
                self.claim_cursor.mark_empty(queue, &scan);
                return Ok(Vec::new());
            }

            let now = self.now_ms();
            let last_pending_key = candidates
                .last()
                .expect("candidates checked non-empty above")
                .key
                .clone();

            let mut jobs = Vec::with_capacity(candidates.len());
            for kv in &candidates {
                let job = JobRecord::decode(&kv.key, &kv.value)?;
                jobs.push(self.stage_claim(&mut txn, &kv.key, job, now, lease_duration)?);
            }

            // Claims commit without awaiting WAL durability. The claimed state
            // only matters across a restart, where either version of it
            // recovers: a claim lost with the unflushed WAL leaves the job
            // pending, and a durable one is requeued at open. The only
            // difference is that the durable claim consumed an attempt.
            match self.commit(txn, Durability::Deferred).await? {
                Commit::Committed => {
                    self.claim_cursor.advance(queue, last_pending_key, &scan);
                    if drained {
                        // The scan ran dry inside this snapshot, so nothing is
                        // left after taking these jobs. Record emptiness so the
                        // next poll short-circuits. Any insert after the epoch
                        // read revokes it.
                        self.claim_cursor.mark_empty(queue, &scan);
                    }
                    // The claim histogram measures the claim transaction.
                    // Offloaded payload fetches happen after the claim lock is
                    // released and are not included.
                    crate::obs::claimed(queue, jobs.len() as u64, timer);
                    debug!(queue = %queue, count = jobs.len(), "jobs claimed");
                    return Ok(jobs);
                }
                Commit::Conflict => {
                    warn!(queue = %queue, "claim transaction conflict, retrying");
                    continue;
                }
            }
        }
    }

    /// Claim one pending or scheduled job by id, with its offloaded payload
    /// fetched.
    pub(crate) async fn claim_by_id(
        &self,
        id: &str,
        lease_duration: Duration,
    ) -> Result<ClaimOutcome> {
        let timer = crate::obs::start();
        // `Err` is an outcome reached without a commit. The claim lock and the
        // claim cursor are not touched, as in `cancel`: removing a pending key
        // keeps every live key at or after the scan bound, and a concurrent
        // scan claim conflicts on the key.
        let claimed: std::result::Result<Claim, ClaimOutcome> = self
            .transition_by_id(
                id,
                Durability::Deferred,
                || Ok(Err(ClaimOutcome::NotFound)),
                |mut txn, current_key, job| async move {
                    if !matches!(job.status, JobStatus::Pending | JobStatus::Scheduled) {
                        return Ok((txn, Attempt::Abort(Err(ClaimOutcome::NotClaimable))));
                    }
                    let claim = self.stage_claim(
                        &mut txn,
                        &current_key,
                        job,
                        self.now_ms(),
                        lease_duration,
                    )?;
                    Ok((txn, Attempt::Commit(Ok(claim))))
                },
            )
            .await?;
        match claimed {
            Ok(claim) => {
                let mut jobs = vec![claim];
                self.materialize_payloads(&mut jobs).await?;
                let claim = jobs.pop().expect("one claim was materialized");
                crate::obs::claimed(&claim.queue, 1, timer);
                debug!(queue = %claim.queue, job_id = %id, "job claimed by id");
                Ok(ClaimOutcome::Claimed(Box::new(claim)))
            }
            Err(outcome) => Ok(outcome),
        }
    }

    /// Stage the claim of `job`, read at `key`, in `txn`: the claimed record
    /// with its attempt counted, the release of its dedup key and a lease that
    /// expires `lease_duration` after `now`.
    fn stage_claim(
        &self,
        txn: &mut Txn,
        key: &[u8],
        mut job: JobRecord,
        now: u64,
        lease_duration: Duration,
    ) -> Result<Claim> {
        job.status = JobStatus::Claimed;
        job.claimed_at = Some(now);
        job.attempts += 1;
        // The claim removes the dedup key from the record before it writes the
        // claimed copy. With the key kept, a later nack would return the record
        // to pending with the key set, and the next claim would delete a dedup
        // index entry that by then belongs to another job.
        let dedup_key_to_release = job.dedup_key.take();
        let claim_id = new_claim_id();
        job.claim_id = Some(claim_id);
        txn.move_job(key, &mut job)?;
        // A cancellation requested during an earlier claim of the job is
        // persisted on the record and fires this claim's token immediately.
        let cancel = tokio_util::sync::CancellationToken::new();
        if job.cancel_requested {
            cancel.cancel();
        }
        // Registered before the commit. A claim that fails to commit leaves its
        // entry, and the reaper drops the entry when due because the claim's
        // record is absent.
        self.lease_registry.insert(
            &job.queue,
            &job.id,
            now + lease_duration.as_millis() as u64,
            claim_id,
            cancel.clone(),
        );
        if let Some(dk) = dedup_key_to_release.as_deref() {
            txn.delete(dedup_index_key(&job.queue, dk))?;
        }
        Ok(Claim::new(job, cancel))
    }

    /// Fetch the offloaded payloads of `jobs` concurrently. The slowest object
    /// bounds the wall time of a batch, and the fetch times do not add up. Jobs
    /// with inline payloads are untouched. On a fetch failure, one error is
    /// returned after every fetch settles.
    async fn materialize_payloads(&self, jobs: &mut [Claim]) -> Result<()> {
        let store = &self.payload_store;
        let fetched =
            futures_util::future::join_all(jobs.iter_mut().map(|c| store.materialize(c.job_mut())))
                .await;
        fetched.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::*;
    use crate::txn::get_indexed_job;

    async fn open_at(clock: &MockClock) -> Queue {
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap()
    }

    /// Stage two claims of the job `id`, the loser after the winner, then
    /// commit the winner and let the loser conflict. Returns the winning claim.
    async fn race(q: &Queue, id: &str, winner_lease: Duration, loser_lease: Duration) -> Claim {
        let core = &q.core;
        let stage = async |lease| {
            let mut txn = core.begin().unwrap();
            let (_, key, job) = get_indexed_job(&txn, id).await.unwrap().unwrap();
            let claim = core
                .stage_claim(&mut txn, &key, job, core.now_ms(), lease)
                .unwrap();
            (txn, claim)
        };
        let (winner_txn, winner) = stage(winner_lease).await;
        let (loser_txn, _) = stage(loser_lease).await;
        assert_eq!(
            core.commit(winner_txn, Durability::Awaited).await.unwrap(),
            Commit::Committed
        );
        assert_eq!(
            core.commit(loser_txn, Durability::Awaited).await.unwrap(),
            Commit::Conflict
        );
        winner
    }

    #[tokio::test]
    async fn the_winner_of_a_claim_race_settles() {
        let q = open_at(&MockClock::new(1_700_000_000_000)).await;
        let id = q.enqueue("work", b"p".to_vec()).await.unwrap();
        let lease = Duration::from_secs(30);

        let claim = race(&q, &id, lease, lease).await;
        q.ack(&claim).await.unwrap();

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn the_lease_of_a_losing_claim_does_not_reap_the_winner() {
        let clock = MockClock::new(1_700_000_000_000);
        let q = open_at(&clock).await;
        let id = q.enqueue("work", b"p".to_vec()).await.unwrap();

        let _claim = race(&q, &id, Duration::from_secs(60), Duration::from_secs(1)).await;
        clock.advance(Duration::from_secs(2));
        q.reap_now().await.unwrap();
        let job = q.view().get_job(&id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Claimed);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_cancel_fires_the_token_of_the_winner_of_a_claim_race() {
        let q = open_at(&MockClock::new(1_700_000_000_000)).await;
        let id = q.enqueue("work", b"p".to_vec()).await.unwrap();
        let lease = Duration::from_secs(30);

        let claim = race(&q, &id, lease, lease).await;
        q.cancel(&id).await.unwrap();
        assert!(claim.cancel_token().is_cancelled());

        q.close().await.unwrap();
    }
}
