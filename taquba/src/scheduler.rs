use std::sync::Arc;

use slatedb::{Db, IsolationLevel};
use tracing::debug;

use crate::background::Periodic;
use crate::claim_cursor::ClaimCursor;
use crate::clock::Clock;
use crate::error::Result;
use crate::job::{JobRecord, JobStatus};
use crate::keys::{KeyTag, parse_key_timestamp, tag_prefix};
use crate::txn::{Commit, Durability, commit, stage_to_pending};

pub(crate) struct Scheduler {
    pub(crate) db: Arc<Db>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) claim_cursor: ClaimCursor,
}

impl Periodic for Scheduler {
    const NAME: &'static str = "scheduled job promoter";

    async fn step(&self) -> Result<()> {
        self.promote_due_jobs().await
    }
}

impl Scheduler {
    /// Move every job whose `run_at` has passed from the scheduled key
    /// space into the pending key space.
    pub(crate) async fn promote_due_jobs(&self) -> Result<()> {
        promote_due_jobs(&self.db, self.clock.as_ref(), &self.claim_cursor).await
    }
}

/// Scan the scheduled key space and move any job whose `run_at` has passed
/// into the pending key space so workers can claim it.
async fn promote_due_jobs(db: &Db, clock: &dyn Clock, claim_cursor: &ClaimCursor) -> Result<()> {
    let now = clock.now_ms();
    let mut due_keys = Vec::new();

    let mut iter = db.scan_prefix(tag_prefix(KeyTag::Scheduled), ..).await?;
    while let Some(kv) = iter.next().await? {
        // Scheduled keys lead with `run_at`, so the scan is sorted globally
        // by it and the first key with a timestamp in the future ends the
        // scan.
        let Some(run_at) = parse_key_timestamp(&kv.key, KeyTag::Scheduled) else {
            continue;
        };
        if run_at > now {
            break;
        }
        due_keys.push(kv.key.clone());
    }
    drop(iter);

    for key_bytes in due_keys {
        promote_job(db, &key_bytes, claim_cursor).await?;
    }

    Ok(())
}

async fn promote_job(
    db: &Db,
    scheduled_key_bytes: &[u8],
    claim_cursor: &ClaimCursor,
) -> Result<()> {
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

        let mut job = JobRecord::decode(scheduled_key_bytes, &raw)?;
        txn.delete(scheduled_key_bytes)?;

        let pending = stage_to_pending(&txn, &mut job, JobStatus::Scheduled)?;

        // Promotion commits do not await WAL durability. Each due job
        // is promoted in its own transaction, so awaiting the flush
        // serialises the sweep at one job per flush interval. A commit
        // lost in a crash leaves the scheduled key in place with its
        // `run_at` still in the past, and the next tick re-promotes
        // it: the rewrite is idempotent. Any later durable commit
        // flushes preceding WAL entries, so a job's post-promotion
        // history is never durable without the promotion itself.
        match commit(txn, Durability::Deferred).await? {
            Commit::Committed => {
                claim_cursor.note_pending_insert(&job.queue, &pending);
                debug!(
                    queue = %job.queue,
                    job_id = %job.id,
                    "scheduled job promoted to pending"
                );
                return Ok(());
            }
            Commit::Conflict => continue,
        }
    }
}
