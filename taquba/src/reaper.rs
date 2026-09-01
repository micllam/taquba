use std::sync::Arc;
use std::time::Duration;

use slatedb::IsolationLevel;
use tracing::{debug, warn};

use crate::background::Periodic;
use crate::error::{Error, Result};
use crate::history::AttemptOutcome;
use crate::job::{JobRecord, JobStatus};
use crate::keys::{
    KeyTag, attempt_history_key, claimed_key, job_index_key, parse_key_timestamp, tag_prefix,
};
use crate::lease_registry::DueLease;
use crate::queue_core::QueueCore;
use crate::stats::update_stats;
use crate::txn::{ClaimEnd, Commit, Durability, commit, stage_claim_end};

pub(crate) struct Reaper {
    core: Arc<QueueCore>,
}

impl Reaper {
    pub(crate) fn new(core: Arc<QueueCore>) -> Self {
        Self { core }
    }
}

impl Periodic for Reaper {
    const NAME: &'static str = "lease reaper";

    /// Reap expired leases, then run the done and dead retention sweeps
    /// of every queue with a retention configured. A sweep error is
    /// logged here; the reap error is returned.
    async fn step(&self) -> Result<()> {
        let core = &self.core;
        let reaped = core.reap_expired().await;
        // The largest configured `keep_done_jobs`: no done record newer
        // than `now - max_keep_done` is expired on any queue, so the
        // time-ordered done scan stops at the first key past it.
        let max_keep_done = core.configs.iter().filter_map(|c| c.keep_done_jobs).max();
        if max_keep_done.is_some()
            && let Err(e) = core
                .sweep_expired(
                    JobStatus::Done,
                    &|queue| core.configs.get(queue).keep_done_jobs,
                    max_keep_done,
                )
                .await
        {
            warn!("done retention sweep error: {e}");
        }
        if core.configs.iter().any(|c| c.dead_retention.is_some())
            && let Err(e) = core
                .sweep_expired(
                    JobStatus::Dead,
                    &|queue| core.configs.get(queue).dead_retention,
                    None,
                )
                .await
        {
            warn!("dead retention sweep error: {e}");
        }
        reaped
    }
}

/// The transition for a claim that ends without a settlement: the
/// job is dead-lettered once its attempts are exhausted and otherwise
/// returns to pending without a backoff. Shared by the reaper and the
/// open-time recovery of interrupted claims.
fn unsettled_claim_end<'a>(
    job: &JobRecord,
    outcome: AttemptOutcome,
    error: &'a str,
) -> ClaimEnd<'a> {
    if job.attempts >= job.max_attempts {
        ClaimEnd::Dead { error }
    } else {
        ClaimEnd::Retry {
            run_at: None,
            outcome,
            error: None,
        }
    }
}

impl QueueCore {
    /// Requeue or dead-letter every claim whose lease has expired.
    pub(crate) async fn reap_expired(&self) -> Result<()> {
        let due = self.lease_registry.take_due(self.now_ms());

        // Due entries stay in the registry, marked, while they are
        // examined; each is removed only after its reap commits. A tick
        // that ends early therefore leaves nothing displaced, and the
        // next tick retries whatever remains due.
        for lease in &due {
            match self.reap_job(lease).await {
                Ok(()) => {}
                // A storage failure applies to the remaining entries as
                // well; end the tick.
                Err(e @ Error::Storage(_)) => return Err(e),
                // Any other error, an undecodable record for example, is
                // specific to this job. Its entry stays marked for the next
                // tick and must not block the entries behind it.
                Err(e) => {
                    warn!(queue = %lease.queue, job_id = %lease.id, "reaping expired lease failed: {e}");
                }
            }
        }

        Ok(())
    }

    async fn reap_job(&self, lease: &DueLease) -> Result<()> {
        let registry = &self.lease_registry;
        let DueLease {
            queue, id, token, ..
        } = lease;
        let (queue, id, token) = (queue.as_str(), id.as_str(), *token);
        let claimed_key_bytes = claimed_key(queue, id);

        loop {
            let txn = self.db.begin(IsolationLevel::Snapshot).await?;

            // A settlement removes the entry after its commit; an entry
            // that is gone or belongs to a new claim leaves nothing to
            // reap. The check runs after the transaction begins: a claim
            // transition the check does not see must then commit after the
            // snapshot and conflict on the staged delete of the claimed
            // key. Checked before the transaction, a late settlement and
            // re-claim completing in between would leave this transaction
            // reading the new claim's record, indistinguishable in the
            // store from the old claim's.
            match registry.current(queue, id) {
                Some((_, current)) if current == token => {}
                _ => {
                    txn.rollback();
                    return Ok(());
                }
            }

            // An entry with no claimed record is the residue of a claim
            // whose commit failed (the entry is registered first), or of a
            // settlement whose entry removal has not run yet. Neither case
            // leaves a claim to recover; drop the entry.
            let Some(raw) = txn.get(&claimed_key_bytes).await? else {
                txn.rollback();
                registry.remove(queue, id, token);
                debug!(queue = %queue, job_id = %id, "dropped lease entry with no claimed record");
                return Ok(());
            };

            let mut job = JobRecord::decode(&claimed_key_bytes, &raw)?;
            txn.delete(&claimed_key_bytes)?;
            let end = unsettled_claim_end(&job, AttemptOutcome::LeaseExpired, "lease expired");
            let pending_key = stage_claim_end(&txn, &mut job, &end, self.now_ms())?;

            // The commit does not await WAL durability: each expired claim
            // is its own transaction, so awaiting the flush would serialise
            // the sweep at one job per flush interval, and a commit lost in
            // a crash is redone by the requeue of claimed records at the
            // next open.
            match commit(txn, Durability::Deferred).await? {
                Commit::Committed => {
                    self.finish_claim_end(&job, &end, token, pending_key.as_deref(), None)
                        .await;
                    if end.is_terminal() {
                        crate::obs::dead_lettered(&job.queue);
                    } else {
                        crate::obs::reaped(&job.queue, 1);
                    }
                    return Ok(());
                }
                // A settlement committed concurrently; retry against fresh state.
                Commit::Conflict => continue,
            }
        }
    }

    /// Re-queue every claimed record found in the store. Called at open,
    /// before workers or the reaper start: the queue is single-process and
    /// single-writer, so a claim present at open belongs to a process that
    /// no longer runs and is void. The requeue consumes no attempt itself;
    /// attempts count claims, so `max_attempts` still bounds a
    /// crash-looping job.
    ///
    /// Runs after the claim cursor is restored, and notes each re-queued
    /// job's pending key, which sorts behind the restored clean-close
    /// bound.
    pub(crate) async fn requeue_interrupted_claims(&self) -> Result<()> {
        let mut interrupted: Vec<JobRecord> = Vec::new();
        let mut iter = self.db.scan_prefix(tag_prefix(KeyTag::Claimed), ..).await?;
        while let Some(kv) = iter.next().await? {
            match JobRecord::decode(&kv.key, &kv.value) {
                Ok(job) => interrupted.push(job),
                // Re-queueing needs the record; the key is left in place
                // for later inspection.
                Err(e) => warn!(key = ?kv.key, "undecodable claimed record at open: {e}"),
            }
        }
        drop(iter);

        for mut job in interrupted {
            let txn = self.db.begin(IsolationLevel::Snapshot).await?;
            txn.delete(claimed_key(&job.queue, &job.id))?;
            let end = unsettled_claim_end(
                &job,
                AttemptOutcome::Interrupted,
                "claim interrupted by process exit",
            );
            let pending_key = stage_claim_end(&txn, &mut job, &end, self.now_ms())?;
            // The commit does not await WAL durability: awaiting the flush
            // would serialise the open at one job per flush interval, and a
            // requeue lost in a crash is redone by the next open from the
            // claimed record it left in place. Nothing else writes at open,
            // so a commit error surfaces to the caller and fails the open.
            txn.commit().await?;
            if let Some(pending_key) = pending_key {
                self.claim_cursor
                    .note_pending_insert(&job.queue, &pending_key);
            }
        }
        Ok(())
    }

    /// Delete the records of `status` (`Done` or `Dead`) whose retention
    /// window has expired. The window is resolved per record from the
    /// job's queue via `retention_for`; records on queues without a
    /// window are skipped.
    ///
    /// Done keys lead with `completed_at` (see [`crate::keys::done_key`]),
    /// so `max_retention`, the largest window configured on any queue,
    /// ends the done scan at the first key newer than `now - max_retention`:
    /// no later record can be expired for any queue. Dead keys group by
    /// queue, so the dead scan reads the whole key space and ignores
    /// `max_retention`.
    async fn sweep_expired(
        &self,
        status: JobStatus,
        retention_for: &(dyn Fn(&str) -> Option<Duration> + Sync),
        max_retention: Option<Duration>,
    ) -> Result<()> {
        let (tag, min_cutoff) = match status {
            JobStatus::Done => (KeyTag::Done, max_retention),
            JobStatus::Dead => (KeyTag::Dead, None),
            _ => return Err(Error::InvalidState),
        };
        let now = self.now_ms();
        let min_cutoff = min_cutoff.map(|r| now.saturating_sub(r.as_millis() as u64));

        let mut victims: Vec<(Vec<u8>, String, String, Option<String>)> = Vec::new();
        let mut iter = self.db.scan_prefix(tag_prefix(tag), ..).await?;
        while let Some(kv) = iter.next().await? {
            if let Some(min_cutoff) = min_cutoff {
                let Some(terminal_at_in_key) = parse_key_timestamp(&kv.key, tag) else {
                    continue;
                };
                if terminal_at_in_key >= min_cutoff {
                    break;
                }
            }

            let Ok(job) = JobRecord::decode(&kv.key, &kv.value) else {
                continue;
            };
            let terminal_at = match status {
                JobStatus::Done => job.completed_at,
                _ => job.failed_at,
            };
            let Some(terminal_at) = terminal_at else {
                continue;
            };
            let Some(retention) = retention_for(&job.queue) else {
                continue;
            };
            let cutoff = now.saturating_sub(retention.as_millis() as u64);
            if terminal_at < cutoff {
                victims.push((kv.key.to_vec(), job.queue, job.id, job.payload_ref));
            }
        }
        drop(iter);

        for (key, queue, id, payload_ref) in victims {
            // `QueueStats::dead` counts the live dead-letter records; the
            // done counter counts completions and is not decremented.
            let dead_stats_queue = matches!(status, JobStatus::Dead).then_some(queue.as_str());
            self.sweep_victim(&key, &id, payload_ref.as_deref(), dead_stats_queue)
                .await?;
        }
        Ok(())
    }

    /// Delete one expired record in its own transaction: re-check that the
    /// record still exists, then remove it together with its job index
    /// entry and attempt history and, when `dead_stats_queue` is set,
    /// decrement that queue's dead counter so `QueueStats::dead` reflects
    /// the live size of the dead-letter inbox.
    ///
    /// The commit does not await WAL durability: a commit lost in a crash
    /// leaves the record in place for the next sweep and the existence
    /// re-check keeps the rerun idempotent, including the counter
    /// decrement. A conflicting commit leaves the victim to the next
    /// sweep. The payload object is deleted only after the commit, so a
    /// crash in between leaves an orphaned object, never a live record
    /// whose payload is gone.
    async fn sweep_victim(
        &self,
        key: &[u8],
        id: &str,
        payload_ref: Option<&str>,
        dead_stats_queue: Option<&str>,
    ) -> Result<()> {
        let txn = self.db.begin(IsolationLevel::Snapshot).await?;
        let existed = txn.get(key).await?.is_some();
        if existed {
            txn.delete(key)?;
            txn.delete(job_index_key(id))?;
            txn.delete(attempt_history_key(id))?;
            if let Some(queue) = dead_stats_queue {
                update_stats(&txn, queue, &[(JobStatus::Dead, -1)])?;
            }
        }
        match commit(txn, Durability::Deferred).await? {
            Commit::Committed => {}
            Commit::Conflict => return Ok(()),
        }
        if existed && let Some(payload_ref) = payload_ref {
            self.payload_store.delete_best_effort(payload_ref, id).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::*;

    #[tokio::test]
    async fn reaping_leaves_no_claim_state() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        // Two jobs, one with retries left and one out of attempts, so the
        // requeue and dead-letter branches are both covered.
        let retried = q.enqueue("work", b"a".to_vec()).await.unwrap();
        let doomed = q
            .enqueue_with(
                "work",
                b"b".to_vec(),
                EnqueueOptions {
                    max_attempts: Some(1),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .unwrap();
        let claimed = q
            .claim_batch("work", 2, Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(claimed.len(), 2);

        clock.advance(Duration::from_secs(31));
        q.reap_now().await.unwrap();

        for id in [&retried, &doomed] {
            assert!(q.core.lease_registry.current("work", id).is_none());
            assert!(
                q.core
                    .db
                    .get(&claimed_key("work", id))
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.claimed, 0);
        assert_eq!(stats.pending, 1);
        assert_eq!(stats.dead, 1);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn stale_settlement_after_reaper_requeue_is_rejected() {
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

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.pending, 1);
        assert_eq!(stats.claimed, 0);

        assert!(matches!(q.ack(&stale).await, Err(Error::ClaimLost)));
        assert!(matches!(
            q.nack(&stale, "late failure").await,
            Err(Error::ClaimLost)
        ));
        assert!(matches!(
            q.dead_letter(&stale, "late permanent failure").await,
            Err(Error::ClaimLost)
        ));

        assert!(matches!(
            q.renew_lease(&stale, Duration::from_secs(30)),
            Err(Error::ClaimLost)
        ));

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.pending, 1);
        assert_eq!(stats.claimed, 0);
        assert_eq!(stats.done, 0);
        assert_eq!(stats.dead, 0);

        let fresh = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fresh.id, id);
        assert_eq!(fresh.attempts, 2);
        q.ack(&fresh).await.unwrap();

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.claimed, 0);
        assert_eq!(stats.done, 1);
        assert_eq!(stats.dead, 0);

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn stale_settlement_after_reaper_dead_letter_is_rejected() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let id = q
            .enqueue_with(
                "work",
                b"payload".to_vec(),
                EnqueueOptions {
                    max_attempts: Some(1),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let stale = q
            .claim("work", Duration::from_millis(1))
            .await
            .unwrap()
            .unwrap();

        clock.advance(Duration::from_millis(2));
        q.reap_now().await.unwrap();

        let dead = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(dead.status, JobStatus::Dead);
        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.claimed, 0);
        assert_eq!(stats.dead, 1);

        assert!(matches!(q.ack(&stale).await, Err(Error::ClaimLost)));
        assert!(matches!(
            q.nack(&stale, "late failure").await,
            Err(Error::ClaimLost)
        ));
        assert!(matches!(
            q.dead_letter(&stale, "late permanent failure").await,
            Err(Error::ClaimLost)
        ));

        assert!(matches!(
            q.renew_lease(&stale, Duration::from_secs(30)),
            Err(Error::ClaimLost)
        ));

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.claimed, 0);
        assert_eq!(stats.done, 0);
        assert_eq!(stats.dead, 1);
        assert!(
            q.claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn ack_succeeds_on_expired_lease_before_reaper_runs() {
        // Settlement is fenced on the claim token; the claim stays
        // settleable past its lease expiry until the reaper requeues
        // the job.
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

        clock.advance(Duration::from_secs(5));

        q.ack(&job).await.unwrap();

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.claimed, 0);
        assert_eq!(stats.done, 1);
        assert_eq!(stats.dead, 0);
        assert!(
            q.claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );

        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cancel_persists_across_reaper_requeue() {
        // Claim -> cancel -> drop the job back to pending via the reaper
        // (lease elapsed) -> re-claim sees cancel_requested and a pre-fired token.
        //
        // Disable the auto-reaper so the cancel definitely happens while
        // the job is Claimed; trigger the requeue manually with reap_now.
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            reaper_interval: Duration::from_secs(3600),
            ..no_backoff_opts()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let job1 = q
            .claim("work", Duration::from_millis(50))
            .await
            .unwrap()
            .unwrap();
        let first_token = job1.cancel_token().clone();
        assert_eq!(q.cancel(&job1.id).await.unwrap(), CancelOutcome::Requested,);
        assert!(first_token.is_cancelled());
        assert!(
            q.get_job(&job1.id).await.unwrap().unwrap().cancel_requested,
            "cancel_requested must persist on the claimed record",
        );

        // Force lease expiry, then trigger the reaper.
        clock.advance(Duration::from_millis(100));
        q.reap_now().await.unwrap();

        let job2 = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job1.id, job2.id);
        assert!(job2.cancel_requested);
        assert!(
            job2.cancel_token().is_cancelled(),
            "re-claim should surface a pre-cancelled token",
        );

        q.ack(&job2).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_reaped_claim_leaves_no_cancel_token_entry() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            reaper_interval: Duration::from_secs(3600),
            ..no_backoff_opts()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let claim = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let token = claim.cancel_token().clone();

        clock.advance(Duration::from_secs(31));
        q.reap_now().await.unwrap();
        assert!(q.core.lease_registry.current("work", &id).is_none());
        assert!(!q.core.lease_registry.cancel("work", &id));
        assert!(!token.is_cancelled());

        // The requeued job holds no entry to fire.
        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Removed);
        assert_eq!(q.core.lease_registry.len(), 0);
        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_retention_is_per_queue_on_ack_and_sweep() {
        // Two queues sharing one Queue instance, with very different
        // retention policies. The default-config queue ("transient") drops
        // jobs on ack; the per-queue override ("kept") retains them. Then
        // the same background reaper sweep must respect each queue's window.
        let clock = MockClock::new(1_700_000_000_000);
        let reaper_interval = Duration::from_millis(10);
        let kept_retention = Duration::from_millis(50);

        let opts = OpenOptions {
            reaper_interval,
            default_queue_config: QueueConfig {
                keep_done_jobs: None,
                ..QueueConfig::default()
            },
            queue_configs: HashMap::from([(
                "kept".to_string(),
                QueueConfig {
                    keep_done_jobs: Some(kept_retention),
                    ..QueueConfig::default()
                },
            )]),
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let kept_id = q.enqueue("kept", b"a".to_vec()).await.unwrap();
        let transient_id = q.enqueue("transient", b"b".to_vec()).await.unwrap();

        let kept_job = q
            .claim("kept", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let transient_job = q
            .claim("transient", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&kept_job).await.unwrap();
        q.ack(&transient_job).await.unwrap();

        // The "transient" queue has no retention: ack dropped the record.
        assert!(
            q.get_job(&transient_id).await.unwrap().is_none(),
            "queues without keep_done_jobs must drop on ack"
        );
        // The "kept" queue has retention: ack preserved the record.
        assert!(
            q.get_job(&kept_id).await.unwrap().is_some(),
            "queues with keep_done_jobs must retain on ack"
        );

        // Fire a reaper tick before the retention window has elapsed:
        // the kept record must survive.
        tokio::time::sleep(reaper_interval * 2).await;
        assert!(
            q.get_job(&kept_id).await.unwrap().is_some(),
            "reaper sweep before retention elapses must not purge"
        );

        // Advance the test clock past the retention window; the next
        // reaper tick purges the record.
        clock.advance(kept_retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;
        assert!(
            q.get_job(&kept_id).await.unwrap().is_none(),
            "reaper sweep after retention elapses must purge"
        );

        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_dead_retention_is_per_queue() {
        // Two queues with different dead-letter retention windows. The
        // same reaper sweep purges the short-window queue's record while
        // leaving the long-window one intact.
        let clock = MockClock::new(1_700_000_000_000);
        let reaper_interval = Duration::from_millis(10);
        let ephemeral_retention = Duration::from_millis(50);

        let opts = OpenOptions {
            reaper_interval,
            default_queue_config: QueueConfig {
                dead_retention: Some(Duration::from_secs(3600)),
                ..QueueConfig::default()
            },
            queue_configs: HashMap::from([(
                "ephemeral".to_string(),
                QueueConfig {
                    dead_retention: Some(ephemeral_retention),
                    ..QueueConfig::default()
                },
            )]),
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        for queue in ["ephemeral", "durable"] {
            q.enqueue_with(
                queue,
                b"x".to_vec(),
                EnqueueOptions {
                    max_attempts: Some(1),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let job = q
                .claim(queue, Duration::from_secs(30))
                .await
                .unwrap()
                .unwrap();
            q.nack(&job, "fatal").await.unwrap();
        }

        assert_eq!(q.dead_jobs("ephemeral", None, 100).await.unwrap().len(), 1);
        assert_eq!(q.dead_jobs("durable", None, 100).await.unwrap().len(), 1);

        clock.advance(ephemeral_retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;

        assert_eq!(
            q.dead_jobs("ephemeral", None, 100).await.unwrap().len(),
            0,
            "short-retention queue must be purged"
        );
        assert_eq!(
            q.dead_jobs("durable", None, 100).await.unwrap().len(),
            1,
            "long-retention queue must be untouched by the same sweep"
        );

        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_done_retention_uses_completion_time_not_enqueue_time() {
        // Both the scheduler (`run_at < now_ms`) and the retention sweep
        // (`completed_at < now_ms - retention`) compare against the queue's
        // clock, so virtualising it via `MockClock` is enough to drive
        // both deterministically.
        let initial = 1_700_000_000_000_u64;
        let clock = MockClock::new(initial);
        let reaper_interval = Duration::from_millis(10);
        let retention = Duration::from_millis(50);
        let schedule_delay = Duration::from_millis(220);
        let opts = OpenOptions {
            reaper_interval,
            default_queue_config: QueueConfig {
                keep_done_jobs: Some(retention),
                ..QueueConfig::default()
            },
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        // Construct `run_at` from the mock clock so it is comparable to
        // the queue's `now_ms` without relying on the system clock.
        let run_at = std::time::UNIX_EPOCH + Duration::from_millis(initial) + schedule_delay;
        let id = q
            .enqueue_with(
                "work",
                b"weekly".to_vec(),
                EnqueueOptions {
                    run_at: Some(run_at),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Advance past the schedule, promote, claim, ack.
        clock.advance(schedule_delay + Duration::from_millis(20));
        q.promote_scheduled_now().await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        let elapsed_since_enqueue = q.now_ms().saturating_sub(job.enqueued_at);
        assert!(
            elapsed_since_enqueue > schedule_delay.as_millis() as u64,
            "enqueued_at should be well over {}ms old (was {elapsed_since_enqueue}ms)",
            schedule_delay.as_millis(),
        );
        q.ack(&job).await.unwrap();

        // Fire a reaper tick right after ack: completion is fresh
        // relative to the retention window, so the record survives even
        // though `enqueued_at` is now far older than the retention.
        tokio::time::sleep(reaper_interval * 2).await;
        let kept = q.get_job(&id).await.unwrap().expect(
            "fresh completion must survive the sweep regardless of how long ago the job was enqueued",
        );
        assert!(
            kept.completed_at.is_some(),
            "ack must stamp completed_at when keep_done_jobs is set"
        );

        // Advance past the retention window; the next reaper tick purges
        // the record.
        clock.advance(retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;
        assert!(q.get_job(&id).await.unwrap().is_none());

        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_dead_retention_sweep_boundary() {
        // Drive a job to dead-letter, then exercise both sides of the
        // retention cutoff with a single configured window: a reaper tick
        // before the cutoff has elapsed must leave the job alone; one
        // after it elapses must purge it (along with its index pointer
        // and the `dead` counter).
        let clock = MockClock::new(1_700_000_000_000);
        let reaper_interval = Duration::from_millis(10);
        let retention = Duration::from_millis(50);
        let opts = OpenOptions {
            reaper_interval,
            default_queue_config: QueueConfig {
                dead_retention: Some(retention),
                ..QueueConfig::default()
            },
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        q.enqueue_with(
            "work",
            b"payload".to_vec(),
            EnqueueOptions {
                max_attempts: Some(1),
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
        q.nack(&job, "fatal").await.unwrap();

        let dead = q.dead_jobs("work", None, 100).await.unwrap();
        assert_eq!(dead.len(), 1);
        assert!(dead[0].failed_at.is_some(), "failed_at must be stamped");
        assert_eq!(q.stats("work").await.unwrap().dead, 1);

        // Fire a reaper tick before the retention cutoff has elapsed:
        // the dead record must survive.
        tokio::time::sleep(reaper_interval * 2).await;
        assert_eq!(q.dead_jobs("work", None, 100).await.unwrap().len(), 1);

        // Advance the test clock past the cutoff. The next reaper tick
        // purges the record; the counter and index pointer must also be
        // cleaned up.
        clock.advance(retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;
        assert!(q.dead_jobs("work", None, 100).await.unwrap().is_empty());
        assert_eq!(
            q.stats("work").await.unwrap().dead,
            0,
            "dead counter must reflect the sweep"
        );
        assert!(q.get_job(&id).await.unwrap().is_none());

        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_requeue_dead_rejects_stale_record_after_retention_sweep() {
        let clock = MockClock::new(1_700_000_000_000);
        let reaper_interval = Duration::from_millis(10);
        let retention = Duration::from_millis(50);
        let q = Queue::open_with_options(
            make_store(),
            "test",
            OpenOptions {
                reaper_interval,
                default_queue_config: QueueConfig {
                    dead_retention: Some(retention),
                    ..Default::default()
                },
                clock: Arc::new(clock.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        q.enqueue_with(
            "work",
            b"payload".to_vec(),
            EnqueueOptions {
                max_attempts: Some(1),
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
        q.nack(&job, "fatal").await.unwrap();

        let dead = q.dead_jobs("work", None, 100).await.unwrap().pop().unwrap();
        clock.advance(retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;

        assert!(q.dead_jobs("work", None, 100).await.unwrap().is_empty());
        let err = q.requeue_dead_job(dead).await.unwrap_err();
        assert!(matches!(err, Error::JobNotFound(_)));
        assert_eq!(q.stats("work").await.unwrap().pending, 0);
        assert_eq!(q.stats("work").await.unwrap().dead, 0);

        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn done_retention_keeps_the_payload_object_until_the_sweep() {
        let clock = MockClock::new(1_700_000_000_000);
        let reaper_interval = Duration::from_millis(10);
        let retention = Duration::from_millis(20);
        let store = make_store();
        let opts = OpenOptions {
            reaper_interval,
            default_queue_config: QueueConfig {
                keep_done_jobs: Some(retention),
                ..QueueConfig::default()
            },
            clock: Arc::new(clock.clone()),
            payload_offload_threshold: Some(64),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(store.clone(), "test", opts)
            .await
            .unwrap();

        let payload = vec![9u8; 256];
        let id = q.enqueue("work", payload.clone()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();

        // The done record is kept, so the payload object stays and the
        // record read materializes it.
        assert_eq!(object_count(&store, "test-payloads").await, 1);
        let done = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(done.payload, payload);

        clock.advance(retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;

        assert!(q.get_job(&id).await.unwrap().is_none());
        assert_eq!(
            object_count(&store, "test-payloads").await,
            0,
            "the retention sweep must delete the payload object with the record"
        );
        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn dead_retention_sweep_deletes_the_payload_object() {
        let clock = MockClock::new(1_700_000_000_000);
        let reaper_interval = Duration::from_millis(10);
        let retention = Duration::from_millis(20);
        let store = make_store();
        let opts = OpenOptions {
            reaper_interval,
            default_queue_config: QueueConfig {
                dead_retention: Some(retention),
                ..QueueConfig::default()
            },
            clock: Arc::new(clock.clone()),
            payload_offload_threshold: Some(64),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(store.clone(), "test", opts)
            .await
            .unwrap();

        q.enqueue("work", vec![5u8; 256]).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.dead_letter(&job, "permanent").await.unwrap();
        assert_eq!(object_count(&store, "test-payloads").await, 1);

        clock.advance(retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;

        assert!(q.dead_jobs("work", None, 10).await.unwrap().is_empty());
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn reaper_dead_letter_preserves_the_payload_object() {
        let clock = MockClock::new(1_700_000_000_000);
        let store = make_store();
        let opts = OpenOptions {
            default_queue_config: QueueConfig {
                max_attempts: 1,
                ..QueueConfig::default()
            },
            clock: Arc::new(clock.clone()),
            payload_offload_threshold: Some(64),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(store.clone(), "test", opts)
            .await
            .unwrap();

        let payload = vec![2u8; 256];
        let id = q.enqueue("work", payload.clone()).await.unwrap();
        let job = q
            .claim("work", Duration::from_millis(10))
            .await
            .unwrap()
            .unwrap();
        drop(job);
        clock.advance(Duration::from_millis(20));
        q.reap_now().await.unwrap();

        let dead = q.dead_jobs("work", None, 10).await.unwrap();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].id, id);
        assert_eq!(dead[0].payload, payload);
        assert_eq!(
            object_count(&store, "test-payloads").await,
            1,
            "reaper-driven dead-letter must preserve the payload object"
        );
        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn dead_retention_sweep_removes_attempt_history() {
        let clock = MockClock::new(1_700_000_000_000);
        let reaper_interval = Duration::from_millis(10);
        let retention = Duration::from_millis(20);
        let opts = OpenOptions {
            reaper_interval,
            clock: Arc::new(clock.clone()),
            default_queue_config: QueueConfig {
                dead_retention: Some(retention),
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let id = q.enqueue("work", b"x".to_vec()).await.unwrap();

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.dead_letter(&job, "failed").await.unwrap();
        assert_eq!(q.attempt_history(&id).await.unwrap().len(), 1);

        clock.advance(retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;

        assert!(q.get_job(&id).await.unwrap().is_none());
        assert!(q.attempt_history(&id).await.unwrap().is_empty());
        q.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn done_retention_sweep_removes_attempt_history() {
        let clock = MockClock::new(1_700_000_000_000);
        let reaper_interval = Duration::from_millis(10);
        let retention = Duration::from_millis(20);
        let opts = OpenOptions {
            reaper_interval,
            clock: Arc::new(clock.clone()),
            default_queue_config: QueueConfig {
                keep_done_jobs: Some(retention),
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let id = q.enqueue("work", b"x".to_vec()).await.unwrap();

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();
        assert_eq!(q.attempt_history(&id).await.unwrap().len(), 1);

        clock.advance(retention + Duration::from_millis(10));
        tokio::time::sleep(reaper_interval * 2).await;

        assert!(q.get_job(&id).await.unwrap().is_none());
        assert!(q.attempt_history(&id).await.unwrap().is_empty());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn attempt_history_records_lease_expiries() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            default_queue_config: QueueConfig {
                max_attempts: 2,
                ..QueueConfig::default()
            },
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let id = q.enqueue("work", b"x".to_vec()).await.unwrap();
        let lease = Duration::from_secs(10);

        q.claim("work", lease).await.unwrap().unwrap();
        clock.advance(lease + Duration::from_secs(1));
        q.reap_now().await.unwrap();

        q.claim("work", lease).await.unwrap().unwrap();
        clock.advance(lease + Duration::from_secs(1));
        q.reap_now().await.unwrap();

        let history = q.attempt_history(&id).await.unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].outcome, AttemptOutcome::LeaseExpired);
        assert_eq!(history[0].attempt, 1);
        assert_eq!(history[0].error, None);
        assert_eq!(history[1].outcome, AttemptOutcome::DeadLettered);
        assert_eq!(history[1].attempt, 2);
        assert_eq!(history[1].error.as_deref(), Some("lease expired"));
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn an_unreapable_job_does_not_block_the_leases_behind_it() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();

        let poison = q.enqueue("work", b"a".to_vec()).await.unwrap();
        let healthy = q.enqueue("work", b"b".to_vec()).await.unwrap();
        let claims = q
            .claim_batch("work", 2, Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(claims.len(), 2);

        // The poisoned record sorts first, both jobs sharing an expiry.
        q.core
            .db
            .put(claimed_key("work", &poison), b"not messagepack")
            .await
            .unwrap();

        clock.advance(Duration::from_secs(31));
        q.reap_now().await.unwrap();

        assert_eq!(
            q.get_job(&healthy).await.unwrap().unwrap().status,
            JobStatus::Pending
        );
        // The poisoned job's entry is kept for a later tick; the
        // healthy job's was removed by its requeue.
        assert_eq!(q.core.lease_registry.len(), 1);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_claim_held_at_close_is_requeued_at_open() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = || OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", opts())
            .await
            .unwrap();
        let id = q.enqueue("work", b"payload".to_vec()).await.unwrap();
        let claim = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        // Drop the claim without settling it, as a crashed worker would.
        drop(claim);
        q.close().await.unwrap();

        // A claim present at open belongs to a process that no longer
        // holds the store, so it is requeued immediately, before its
        // lease expires.
        let q = Queue::open_with_options(store, "test", opts())
            .await
            .unwrap();
        let job = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Pending);
        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.pending, 1);
        assert_eq!(stats.claimed, 0);
        let history = q.attempt_history(&id).await.unwrap();
        assert_eq!(history.last().unwrap().outcome, AttemptOutcome::Interrupted);

        // The requeued job is claimable at once: its pending insert is
        // recorded against the restored clean-close bound. The next
        // attempt is consumed by the re-claim.
        let reclaim = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reclaim.id, id);
        assert_eq!(reclaim.attempts, 2);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_claim_out_of_attempts_is_dead_lettered_at_open() {
        let clock = MockClock::new(1_700_000_000_000);
        let opts = || OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", opts())
            .await
            .unwrap();
        let id = q
            .enqueue_with(
                "work",
                b"payload".to_vec(),
                EnqueueOptions {
                    max_attempts: Some(1),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .unwrap();
        let claim = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        drop(claim);
        q.close().await.unwrap();

        let q = Queue::open_with_options(store, "test", opts())
            .await
            .unwrap();
        let job = q.get_job(&id).await.unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Dead);
        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.dead, 1);
        assert_eq!(stats.claimed, 0);
        q.close().await.unwrap();
    }
}
