use std::sync::Arc;
use std::time::Duration;

use slatedb::{Db, IsolationLevel};
use tokio::sync::{Notify, watch};
use tracing::{debug, warn};

use crate::error::Result;
use crate::job::{JobRecord, JobStatus};
use crate::queue::{dead_key, job_index_key, now_ms, pending_key};
use crate::stats::update_stats;

pub(crate) async fn reap_loop(
    db: Arc<Db>,
    interval: Duration,
    keep_done_jobs: Option<Duration>,
    dead_retention: Option<Duration>,
    job_available: Arc<Notify>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                match reap_expired(&db).await {
                    Ok(0) => {}
                    Ok(_) => job_available.notify_waiters(),
                    Err(e) => warn!("lease reaper error: {e}"),
                }
                if let Some(retention) = keep_done_jobs {
                    if let Err(e) = sweep_done(&db, retention).await {
                        warn!("done retention sweep error: {e}");
                    }
                }
                if let Some(retention) = dead_retention {
                    if let Err(e) = sweep_dead(&db, retention).await {
                        warn!("dead retention sweep error: {e}");
                    }
                }
            }
            _ = shutdown.changed() => break,
        }
    }
    debug!("lease reaper stopped");
}

/// Returns the number of expired claims that were processed. Callers can use
/// this to decide whether to wake any waiting workers.
pub(crate) async fn reap_expired(db: &Db) -> Result<usize> {
    let now = now_ms();
    let mut expired_keys = Vec::new();

    let mut iter = db.scan_prefix(b"claimed:").await?;
    while let Some(kv) = iter.next().await? {
        let key_str = match std::str::from_utf8(&kv.key) {
            Ok(s) => s,
            Err(_) => continue,
        };
        // Key format: "claimed:{ts:020}:{queue}:{ulid}".
        // Sorted globally by `ts`, so the first key whose timestamp is in the
        // future ends the scan; everything after it is also in the future.
        let after = match key_str.strip_prefix("claimed:") {
            Some(s) => s,
            None => continue,
        };
        let ts_str = match after.split(':').next() {
            Some(s) => s,
            None => continue,
        };
        let lease_expiry = match ts_str.parse::<u64>() {
            Ok(v) => v,
            Err(_) => continue,
        };
        if lease_expiry > now {
            break;
        }
        expired_keys.push(kv.key.clone());
    }
    drop(iter);

    let count = expired_keys.len();
    for key_bytes in expired_keys {
        reap_job(db, &key_bytes).await?;
    }

    Ok(count)
}

async fn reap_job(db: &Db, claimed_key_bytes: &[u8]) -> Result<()> {
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
            job.failed_at = Some(now_ms());
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

        match txn.commit().await {
            Ok(_) => return Ok(()),
            // Worker acked/nacked while we were running; retry to re-check.
            Err(e) if e.kind() == slatedb::ErrorKind::Transaction => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

/// Delete done jobs whose retention window has expired.
pub(crate) async fn sweep_done(db: &Db, retention: Duration) -> Result<()> {
    let cutoff = now_ms().saturating_sub(retention.as_millis() as u64);

    let mut victims: Vec<(Vec<u8>, String, String)> = Vec::new();
    let mut iter = db.scan_prefix(b"done:").await?;
    while let Some(kv) = iter.next().await? {
        let job: JobRecord = match rmp_serde::from_slice(&kv.value) {
            Ok(j) => j,
            Err(_) => continue,
        };
        let Some(completed_at) = job.completed_at else {
            continue;
        };
        if completed_at < cutoff {
            victims.push((kv.key.to_vec(), job.queue.clone(), job.id.clone()));
        }
    }
    drop(iter);

    for (key, _queue, id) in victims {
        let txn = db.begin(IsolationLevel::Snapshot).await?;
        // Re-check existence; could have been swept by a previous tick.
        if txn.get(&key).await?.is_some() {
            txn.delete(&key)?;
            txn.delete(job_index_key(&id).as_bytes())?;
        }
        match txn.commit().await {
            Ok(_) => {}
            Err(e) if e.kind() == slatedb::ErrorKind::Transaction => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Delete dead-letter jobs whose retention window has expired.
pub(crate) async fn sweep_dead(db: &Db, retention: Duration) -> Result<()> {
    let cutoff = now_ms().saturating_sub(retention.as_millis() as u64);

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
        match txn.commit().await {
            Ok(_) => {}
            Err(e) if e.kind() == slatedb::ErrorKind::Transaction => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
