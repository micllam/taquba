use std::sync::Arc;
use std::time::Duration;

use slatedb::{Db, IsolationLevel};
use tokio::sync::{Notify, watch};
use tracing::{debug, warn};

use crate::clock::Clock;
use crate::error::Result;
use crate::job::{JobRecord, JobStatus};
use crate::queue::{job_index_key, parse_leading_timestamp, pending_key};
use crate::stats::update_stats;

pub(crate) struct Scheduler {
    pub(crate) db: Arc<Db>,
    pub(crate) interval: Duration,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) job_available: Arc<Notify>,
}

impl Scheduler {
    pub(crate) async fn run(self, mut shutdown: watch::Receiver<bool>) {
        let Scheduler {
            db,
            interval,
            clock,
            job_available,
        } = self;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    match promote_due_jobs(&db, clock.as_ref()).await {
                        Ok(0) => {}
                        Ok(_) => job_available.notify_waiters(),
                        Err(e) => warn!("scheduled job promoter error: {e}"),
                    }
                }
                _ = shutdown.changed() => break,
            }
        }
        debug!("scheduled job promoter stopped");
    }
}

/// Scan the `scheduled:` key space and move any job whose `run_at` has passed
/// into the `pending:` key space so workers can claim it. Returns the number
/// of jobs that were promoted.
pub(crate) async fn promote_due_jobs(db: &Db, clock: &dyn Clock) -> Result<usize> {
    let now = clock.now_ms();
    let mut due_keys = Vec::new();

    let mut iter = db.scan_prefix(b"scheduled:").await?;
    while let Some(kv) = iter.next().await? {
        // Key format: "scheduled:{run_at:020}:{queue}:{ulid}".
        // Sorted globally by `run_at`, so the first key with a timestamp in the
        // future ends the scan.
        let Some(run_at) = parse_leading_timestamp(&kv.key, "scheduled:") else {
            continue;
        };
        if run_at > now {
            break;
        }
        due_keys.push(kv.key.clone());
    }
    drop(iter);

    let count = due_keys.len();
    for key_bytes in due_keys {
        promote_job(db, &key_bytes).await?;
    }

    Ok(count)
}

async fn promote_job(db: &Db, scheduled_key_bytes: &[u8]) -> Result<()> {
    loop {
        let txn = db.begin(IsolationLevel::Snapshot).await?;

        let raw = match txn.get(scheduled_key_bytes).await? {
            // Already promoted by a concurrent call; nothing to do.
            None => {
                txn.rollback();
                return Ok(());
            }
            Some(raw) => raw,
        };

        let mut job: JobRecord = rmp_serde::from_slice(&raw)?;
        txn.delete(scheduled_key_bytes)?;

        job.status = JobStatus::Pending;
        job.run_at = None;
        let priority = job.priority;
        let pending = pending_key(&job.queue, priority, &job.id);
        let value = rmp_serde::to_vec_named(&job)?;
        txn.put(pending.as_bytes(), &value)?;
        txn.put(job_index_key(&job.id).as_bytes(), pending.as_bytes())?;
        update_stats(
            &txn,
            &job.queue,
            &[(JobStatus::Pending, 1), (JobStatus::Scheduled, -1)],
        )?;

        match txn.commit().await {
            Ok(_) => {
                debug!(
                    queue = %job.queue,
                    job_id = %job.id,
                    "scheduled job promoted to pending"
                );
                return Ok(());
            }
            Err(e) if e.kind() == slatedb::ErrorKind::Transaction => continue,
            Err(e) => return Err(e.into()),
        }
    }
}
