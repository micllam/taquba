use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use slatedb::config::WriteOptions;
use slatedb::{Db, IsolationLevel};
use tokio::sync::{Notify, watch};
use tracing::{debug, warn};

use crate::claim_cursor::ClaimCursor;
use crate::clock::Clock;
use crate::error::Result;
use crate::job::{JobRecord, JobStatus};
use crate::queue::{QueueConfig, dead_key, job_index_key, parse_leading_timestamp, pending_key};
use crate::stats::update_stats;

pub(crate) struct Reaper {
    pub(crate) db: Arc<Db>,
    pub(crate) interval: Duration,
    pub(crate) default_queue_config: QueueConfig,
    pub(crate) queue_configs: HashMap<String, QueueConfig>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) job_available: Arc<Notify>,
    pub(crate) completion_notify: Arc<Notify>,
    pub(crate) claim_cursor: ClaimCursor,
}

impl Reaper {
    pub(crate) async fn run(self, mut shutdown: watch::Receiver<bool>) {
        let Reaper {
            db,
            interval,
            default_queue_config,
            queue_configs,
            clock,
            job_available,
            completion_notify,
            claim_cursor,
        } = self;

        let any_keep_done = default_queue_config.keep_done_jobs.is_some()
            || queue_configs.values().any(|c| c.keep_done_jobs.is_some());
        let any_dead_retention = default_queue_config.dead_retention.is_some()
            || queue_configs.values().any(|c| c.dead_retention.is_some());

        // Largest configured `keep_done_jobs` across every queue (named
        // and default). Any `done:` record whose `completed_at` is
        // newer than `now - max_keep_done` cannot be expired for any
        // queue, so the time-ordered `done:` scan can stop the first
        // time it sees a key past that threshold.
        let max_keep_done: Option<Duration> = default_queue_config
            .keep_done_jobs
            .into_iter()
            .chain(queue_configs.values().filter_map(|c| c.keep_done_jobs))
            .max();

        let keep_done_for = |queue: &str| -> Option<Duration> {
            queue_configs
                .get(queue)
                .unwrap_or(&default_queue_config)
                .keep_done_jobs
        };
        let dead_retention_for = |queue: &str| -> Option<Duration> {
            queue_configs
                .get(queue)
                .unwrap_or(&default_queue_config)
                .dead_retention
        };

        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    match reap_expired(&db, clock.as_ref(), &completion_notify, &claim_cursor).await {
                        Ok(0) => {}
                        Ok(_) => job_available.notify_waiters(),
                        Err(e) => warn!("lease reaper error: {e}"),
                    }
                    if any_keep_done
                        && let Err(e) =
                            sweep_done(&db, clock.as_ref(), &keep_done_for, max_keep_done)
                                .await
                        {
                            warn!("done retention sweep error: {e}");
                        }
                    if any_dead_retention
                        && let Err(e) = sweep_dead(&db, clock.as_ref(), &dead_retention_for).await {
                            warn!("dead retention sweep error: {e}");
                        }
                }
                _ = shutdown.changed() => break,
            }
        }
        debug!("lease reaper stopped");
    }
}

/// Returns the number of expired claims that were processed. Callers can use
/// this to decide whether to wake any waiting workers.
pub(crate) async fn reap_expired(
    db: &Db,
    clock: &dyn Clock,
    completion_notify: &Notify,
    claim_cursor: &ClaimCursor,
) -> Result<usize> {
    let now = clock.now_ms();
    let mut expired_keys = Vec::new();

    let mut iter = db.scan_prefix(b"claimed:").await?;
    while let Some(kv) = iter.next().await? {
        // Key format: "claimed:{ts:020}:{queue}:{ulid}".
        // Sorted globally by `ts`, so the first key whose timestamp is in the
        // future ends the scan; everything after it is also in the future.
        let Some(lease_expiry) = parse_leading_timestamp(&kv.key, "claimed:") else {
            continue;
        };
        if lease_expiry > now {
            break;
        }
        expired_keys.push(kv.key.clone());
    }
    drop(iter);

    let count = expired_keys.len();
    for key_bytes in expired_keys {
        reap_job(db, clock, &key_bytes, completion_notify, claim_cursor).await?;
    }

    Ok(count)
}

async fn reap_job(
    db: &Db,
    clock: &dyn Clock,
    claimed_key_bytes: &[u8],
    completion_notify: &Notify,
    claim_cursor: &ClaimCursor,
) -> Result<()> {
    loop {
        let txn = db.begin(IsolationLevel::Snapshot).await?;

        let raw = match txn.get(claimed_key_bytes).await? {
            // Job was already acked/nacked by the worker: nothing to do.
            None => {
                txn.rollback();
                return Ok(());
            }
            Some(raw) => raw,
        };

        let mut job: JobRecord = rmp_serde::from_slice(&raw)?;
        txn.delete(claimed_key_bytes)?;

        if job.attempts >= job.max_attempts {
            job.status = JobStatus::Dead;
            job.last_error = Some("lease expired".to_string());
            job.failed_at = Some(clock.now_ms());
            let dead = dead_key(&job.queue, &job.id);
            let value = rmp_serde::to_vec_named(&job)?;
            txn.put(dead.as_bytes(), &value)?;
            txn.put(job_index_key(&job.id).as_bytes(), dead.as_bytes())?;
            update_stats(
                &txn,
                &job.queue,
                &[(JobStatus::Claimed, -1), (JobStatus::Dead, 1)],
            )?;
            warn!(
                queue = %job.queue,
                job_id = %job.id,
                attempts = job.attempts,
                "lease expired: job dead-lettered"
            );
        } else {
            job.status = JobStatus::Pending;
            job.claimed_at = None;
            job.lease_expires_at = None;
            let priority = job.priority;
            let pending = pending_key(&job.queue, priority, &job.id);
            let value = rmp_serde::to_vec_named(&job)?;
            txn.put(pending.as_bytes(), &value)?;
            txn.put(job_index_key(&job.id).as_bytes(), pending.as_bytes())?;
            update_stats(
                &txn,
                &job.queue,
                &[(JobStatus::Pending, 1), (JobStatus::Claimed, -1)],
            )?;
            debug!(
                queue = %job.queue,
                job_id = %job.id,
                attempts = job.attempts,
                "lease expired: job re-queued"
            );
        }

        let became_dead = matches!(job.status, JobStatus::Dead);
        let requeued_pending_key =
            (!became_dead).then(|| pending_key(&job.queue, job.priority, &job.id));
        // Reap commits do not await WAL durability. Each expired claim
        // is processed in its own transaction, so awaiting the flush
        // serialises the sweep at one job per flush interval. A commit
        // lost in a crash leaves the expired claimed key in place and
        // the next sweep re-processes it: the rewrite is idempotent and
        // requeues do not consume an attempt. Any later durable commit
        // flushes preceding WAL entries, so a job's post-requeue
        // history is never durable without the requeue itself.
        let write_opts = WriteOptions {
            await_durable: false,
            ..WriteOptions::default()
        };
        match txn.commit_with_options(&write_opts).await {
            Ok(_) => {
                if let Some(key) = requeued_pending_key {
                    claim_cursor.note_pending_insert(&job.queue, &key);
                    crate::obs::reaped(&job.queue, 1);
                }
                if became_dead {
                    crate::obs::dead_lettered(&job.queue);
                    completion_notify.notify_waiters();
                }
                return Ok(());
            }
            // Worker acked/nacked while we were running; retry to re-check.
            Err(e) if e.kind() == slatedb::ErrorKind::Transaction => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

/// Delete done jobs whose retention window has expired. The window is
/// resolved per-record by looking up the job's queue via `keep_done_for`.
/// Records on queues with `keep_done_jobs = None` are skipped.
///
/// `done:` keys are sorted globally by `completed_at` (see
/// [`crate::queue::done_key`]), so once the scan hits a key whose
/// timestamp is newer than `now - max_keep_done`, no remaining record
/// can be expired for any queue and the loop breaks. The per-record
/// queue-specific retention check still runs below the threshold to
/// honour mixed retention values across queues.
async fn sweep_done(
    db: &Db,
    clock: &dyn Clock,
    keep_done_for: &(dyn Fn(&str) -> Option<Duration> + Sync),
    max_keep_done: Option<Duration>,
) -> Result<()> {
    let now = clock.now_ms();
    let min_cutoff = max_keep_done.map(|r| now.saturating_sub(r.as_millis() as u64));

    let mut victims: Vec<(Vec<u8>, String)> = Vec::new();
    let mut iter = db.scan_prefix(b"done:").await?;
    while let Some(kv) = iter.next().await? {
        // Key format: "done:{completed_at:020}:{queue}:{id}".
        if let Some(min_cutoff) = min_cutoff {
            let Some(completed_at_in_key) = parse_leading_timestamp(&kv.key, "done:") else {
                continue;
            };
            if completed_at_in_key >= min_cutoff {
                break;
            }
        }

        let job: JobRecord = match rmp_serde::from_slice(&kv.value) {
            Ok(j) => j,
            Err(_) => continue,
        };
        let Some(completed_at) = job.completed_at else {
            continue;
        };
        let Some(retention) = keep_done_for(&job.queue) else {
            continue;
        };
        let cutoff = now.saturating_sub(retention.as_millis() as u64);
        if completed_at < cutoff {
            victims.push((kv.key.to_vec(), job.id.clone()));
        }
    }
    drop(iter);

    for (key, id) in victims {
        let txn = db.begin(IsolationLevel::Snapshot).await?;
        // Re-check existence; could have been swept by a previous tick.
        if txn.get(&key).await?.is_some() {
            txn.delete(&key)?;
            txn.delete(job_index_key(&id).as_bytes())?;
        }
        // Retention deletes do not await WAL durability: a commit lost
        // in a crash leaves the record in place for the next sweep,
        // and the existence re-check above keeps the rerun idempotent.
        let write_opts = WriteOptions {
            await_durable: false,
            ..WriteOptions::default()
        };
        match txn.commit_with_options(&write_opts).await {
            Ok(_) => {}
            Err(e) if e.kind() == slatedb::ErrorKind::Transaction => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Delete dead-letter jobs whose retention window has expired. The window
/// is resolved per-record by looking up the job's queue via
/// `dead_retention_for`. Records on queues with `dead_retention = None`
/// are skipped.
async fn sweep_dead(
    db: &Db,
    clock: &dyn Clock,
    dead_retention_for: &(dyn Fn(&str) -> Option<Duration> + Sync),
) -> Result<()> {
    let now = clock.now_ms();

    let mut victims: Vec<(Vec<u8>, String, String)> = Vec::new();
    let mut iter = db.scan_prefix(b"dead:").await?;
    while let Some(kv) = iter.next().await? {
        let job: JobRecord = match rmp_serde::from_slice(&kv.value) {
            Ok(j) => j,
            Err(_) => continue,
        };
        // Skip records without a failed_at.
        let Some(failed_at) = job.failed_at else {
            continue;
        };
        let Some(retention) = dead_retention_for(&job.queue) else {
            continue;
        };
        let cutoff = now.saturating_sub(retention.as_millis() as u64);
        if failed_at < cutoff {
            victims.push((kv.key.to_vec(), job.queue.clone(), job.id.clone()));
        }
    }
    drop(iter);

    for (key, queue, id) in victims {
        let txn = db.begin(IsolationLevel::Snapshot).await?;
        if txn.get(&key).await?.is_some() {
            txn.delete(&key)?;
            txn.delete(job_index_key(&id).as_bytes())?;
            // Decrement the dead counter so QueueStats::dead reflects the live
            // size of the dead-letter inbox, consistent with how requeue
            // already adjusts it.
            update_stats(&txn, &queue, &[(JobStatus::Dead, -1)])?;
        }
        // Retention deletes do not await WAL durability: a commit lost
        // in a crash leaves the record in place for the next sweep,
        // and the existence re-check above keeps the rerun idempotent,
        // including the dead counter decrement.
        let write_opts = WriteOptions {
            await_durable: false,
            ..WriteOptions::default()
        };
        match txn.commit_with_options(&write_opts).await {
            Ok(_) => {}
            Err(e) if e.kind() == slatedb::ErrorKind::Transaction => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
