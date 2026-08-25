use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use slatedb::{Db, IsolationLevel};
use tracing::{debug, warn};

use crate::WaitOutcome;
use crate::background::Periodic;
use crate::claim_cursor::ClaimCursor;
use crate::clock::Clock;
use crate::completion::CompletionWaiters;
use crate::error::{Error, Result};
use crate::history::{AttemptOutcome, JobAttempt, append_attempt};
use crate::job::{JobRecord, JobStatus};
use crate::keys::{
    KeyTag, attempt_history_key, claimed_key, job_index_key, parse_key_timestamp, tag_prefix,
};
use crate::lease_registry::{DueLease, LeaseRegistry};
use crate::payload_store::PayloadStore;
use crate::queue::QueueConfig;
use crate::stats::update_stats;
use crate::txn::{Commit, Durability, commit, stage_dead_letter, stage_to_pending, write_options};

pub(crate) struct Reaper {
    pub(crate) db: Arc<Db>,
    pub(crate) default_queue_config: QueueConfig,
    pub(crate) queue_configs: HashMap<String, QueueConfig>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) completion_waiters: Arc<CompletionWaiters>,
    pub(crate) claim_cursor: ClaimCursor,
    pub(crate) payload_store: Arc<PayloadStore>,
    pub(crate) lease_registry: LeaseRegistry,
}

impl Reaper {
    fn config(&self, queue: &str) -> &QueueConfig {
        self.queue_configs
            .get(queue)
            .unwrap_or(&self.default_queue_config)
    }

    fn configs(&self) -> impl Iterator<Item = &QueueConfig> {
        std::iter::once(&self.default_queue_config).chain(self.queue_configs.values())
    }
}

impl Periodic for Reaper {
    const NAME: &'static str = "lease reaper";

    /// Reap expired leases, then run the done and dead retention sweeps
    /// of every queue with a retention configured. A sweep error is
    /// logged here; the reap error is returned.
    async fn step(&self) -> Result<()> {
        let reaped = reap_expired(
            &self.db,
            self.clock.as_ref(),
            &self.completion_waiters,
            &self.claim_cursor,
            &self.lease_registry,
            &self.payload_store,
        )
        .await;
        // The largest configured `keep_done_jobs`: no done record newer
        // than `now - max_keep_done` is expired on any queue, so the
        // time-ordered done scan stops at the first key past it.
        let max_keep_done = self.configs().filter_map(|c| c.keep_done_jobs).max();
        if max_keep_done.is_some()
            && let Err(e) = sweep_expired(
                &self.db,
                self.clock.as_ref(),
                JobStatus::Done,
                &|queue| self.config(queue).keep_done_jobs,
                max_keep_done,
                &self.payload_store,
            )
            .await
        {
            warn!("done retention sweep error: {e}");
        }
        if self.configs().any(|c| c.dead_retention.is_some())
            && let Err(e) = sweep_expired(
                &self.db,
                self.clock.as_ref(),
                JobStatus::Dead,
                &|queue| self.config(queue).dead_retention,
                None,
                &self.payload_store,
            )
            .await
        {
            warn!("dead retention sweep error: {e}");
        }
        reaped
    }
}

/// Requeue or dead-letter every claim whose lease has expired.
pub(crate) async fn reap_expired(
    db: &Db,
    clock: &dyn Clock,
    completion_waiters: &CompletionWaiters,
    claim_cursor: &ClaimCursor,
    lease_registry: &LeaseRegistry,
    payload_store: &PayloadStore,
) -> Result<()> {
    let now = clock.now_ms();
    let due = lease_registry.take_due(now);

    // Due entries stay in the registry, marked, while they are
    // examined; each is removed only after its reap commits. A tick
    // that ends early therefore leaves nothing displaced, and the next
    // tick retries whatever remains due.
    for lease in &due {
        match reap_job(
            db,
            clock,
            lease_registry,
            lease,
            completion_waiters,
            claim_cursor,
            payload_store,
        )
        .await
        {
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

async fn reap_job(
    db: &Db,
    clock: &dyn Clock,
    registry: &LeaseRegistry,
    lease: &DueLease,
    completion_waiters: &CompletionWaiters,
    claim_cursor: &ClaimCursor,
    payload_store: &PayloadStore,
) -> Result<()> {
    let DueLease {
        queue, id, token, ..
    } = lease;
    let (queue, id, token) = (queue.as_str(), id.as_str(), *token);
    let claimed_key_bytes = claimed_key(queue, id);

    loop {
        let txn = db.begin(IsolationLevel::Snapshot).await?;

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
        let end = stage_unsettled_claim_end(
            &txn,
            &mut job,
            clock.now_ms(),
            AttemptOutcome::LeaseExpired,
            "lease expired",
        )?;

        // The commit does not await WAL durability: each expired claim
        // is its own transaction, so awaiting the flush would serialise
        // the sweep at one job per flush interval, and a commit lost in
        // a crash is redone by the requeue of claimed records at the
        // next open.
        match commit(txn, Durability::Deferred).await? {
            Commit::Committed => {
                registry.remove(queue, id, token);
                match end {
                    ClaimEnd::Requeued { pending_key } => {
                        claim_cursor.note_pending_insert(&job.queue, &pending_key);
                        crate::obs::reaped(&job.queue, 1);
                    }
                    ClaimEnd::DeadLettered => {
                        crate::obs::dead_lettered(&job.queue);
                        if completion_waiters.has_waiters(id) {
                            // Delivered in the form `get_job` returns; a
                            // failed payload fetch leaves the stored form.
                            let mut delivered = job.clone();
                            if let Err(e) =
                                crate::read::materialize_payload(payload_store, &mut delivered)
                                    .await
                            {
                                warn!(queue = %queue, job_id = %id, error = %e, "payload of a dead-lettered job could not be fetched for its waiters");
                            }
                            completion_waiters
                                .settle(id, || WaitOutcome::Dead(Box::new(delivered)));
                        }
                    }
                }
                return Ok(());
            }
            // A settlement committed concurrently; retry against fresh state.
            Commit::Conflict => continue,
        }
    }
}

/// How an unsettled claim's job left the claimed state.
enum ClaimEnd {
    Requeued { pending_key: Vec<u8> },
    DeadLettered,
}

/// Stage the transition of a claimed job whose claim ended without a
/// settlement: back to pending, or to the dead-letter set once its
/// attempts are exhausted. Shared by the reaper and the open-time
/// requeue; `error` names the cause in the history and log entries.
fn stage_unsettled_claim_end(
    txn: &slatedb::DbTransaction,
    job: &mut JobRecord,
    now: u64,
    outcome: AttemptOutcome,
    error: &str,
) -> Result<ClaimEnd> {
    txn.delete(claimed_key(&job.queue, &job.id))?;

    if job.attempts >= job.max_attempts {
        stage_dead_letter(txn, job, now, error)?;
        Ok(ClaimEnd::DeadLettered)
    } else {
        let claimed_at = job.claimed_at.take();
        let pending = stage_to_pending(txn, job, JobStatus::Claimed)?;
        append_attempt(
            txn,
            &job.id,
            &JobAttempt {
                attempt: job.attempts,
                claimed_at,
                recorded_at: now,
                outcome,
                error: None,
            },
        )?;
        debug!(
            queue = %job.queue,
            job_id = %job.id,
            attempts = job.attempts,
            "{error}: job re-queued"
        );
        Ok(ClaimEnd::Requeued {
            pending_key: pending,
        })
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
pub(crate) async fn requeue_interrupted_claims(
    db: &Db,
    clock: &dyn Clock,
    claim_cursor: &ClaimCursor,
) -> Result<()> {
    let mut interrupted: Vec<JobRecord> = Vec::new();
    let mut iter = db.scan_prefix(tag_prefix(KeyTag::Claimed), ..).await?;
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
        let txn = db.begin(IsolationLevel::Snapshot).await?;
        let end = stage_unsettled_claim_end(
            &txn,
            &mut job,
            clock.now_ms(),
            AttemptOutcome::Interrupted,
            "claim interrupted by process exit",
        )?;
        // The commit does not await WAL durability: awaiting the flush
        // would serialise the open at one job per flush interval, and a
        // requeue lost in a crash is redone by the next open from the
        // claimed record it left in place. Nothing else writes at open,
        // so a commit error surfaces to the caller and fails the open.
        txn.commit_with_options(&write_options(Durability::Deferred))
            .await?;
        if let ClaimEnd::Requeued { pending_key } = end {
            claim_cursor.note_pending_insert(&job.queue, &pending_key);
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
    db: &Db,
    clock: &dyn Clock,
    status: JobStatus,
    retention_for: &(dyn Fn(&str) -> Option<Duration> + Sync),
    max_retention: Option<Duration>,
    payload_store: &PayloadStore,
) -> Result<()> {
    let (tag, min_cutoff) = match status {
        JobStatus::Done => (KeyTag::Done, max_retention),
        JobStatus::Dead => (KeyTag::Dead, None),
        _ => return Err(Error::InvalidState),
    };
    let now = clock.now_ms();
    let min_cutoff = min_cutoff.map(|r| now.saturating_sub(r.as_millis() as u64));

    let mut victims: Vec<(Vec<u8>, String, String, Option<String>)> = Vec::new();
    let mut iter = db.scan_prefix(tag_prefix(tag), ..).await?;
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
        sweep_victim(
            db,
            payload_store,
            &key,
            &id,
            payload_ref.as_deref(),
            dead_stats_queue,
        )
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
    db: &Db,
    payload_store: &PayloadStore,
    key: &[u8],
    id: &str,
    payload_ref: Option<&str>,
    dead_stats_queue: Option<&str>,
) -> Result<()> {
    let txn = db.begin(IsolationLevel::Snapshot).await?;
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
        payload_store.delete_best_effort(payload_ref, id).await;
    }
    Ok(())
}
