//! Transaction helpers shared by the queue, reaper and scheduler.
use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Deref;

use bytes::Bytes;
use slatedb::{DbTransaction, IsolationLevel};

use tracing::{debug, warn};

use crate::error::{Error, Result};
use crate::expiry::ExpiryIndex;
use crate::history::{AttemptOutcome, JobAttempt, append_attempt};
use crate::job::{JobRecord, JobStatus};
use crate::keys::{QueueName, attempt_history_key, claimed_key, dedup_index_key, job_index_key};
use crate::lease_registry::LeaseRegistry;
use crate::queue_core::QueueCore;
use crate::stats::update_stats;

/// A store transaction that notes, for every job record and expiry entry it
/// writes and every job it enqueues, the process state that the commit updates.
/// [`QueueCore::commit`] applies the notes once the transaction commits. A
/// transaction that is dropped, rolled back or in conflict discards them.
pub(crate) struct Txn {
    txn: DbTransaction,
    notes: CommitNotes,
}

/// The process state that the writes of a [`Txn`] update.
#[derive(Default)]
struct CommitNotes {
    /// Per queue, the smallest pending key written and the count of pending
    /// records written, for the claim cursor.
    pending: BTreeMap<QueueName, (Vec<u8>, usize)>,
    /// The earliest `run_at` of a scheduled record written.
    scheduled: Option<u64>,
    /// The earliest `completed_at` of a done record written.
    done: Option<u64>,
    /// The expiry entries written, for the bounds of their indexes.
    expiry: Vec<(ExpiryIndex, u64)>,
    /// The new jobs, as their queue and id, for the enqueue counter.
    enqueued: Vec<(QueueName, String)>,
}

impl Deref for Txn {
    type Target = DbTransaction;

    fn deref(&self) -> &DbTransaction {
        &self.txn
    }
}

impl Txn {
    /// Write `job` at its [`JobRecord::stored_key`], repoint its index entry to
    /// that key and note the record for the commit.
    pub(crate) fn put_job(&mut self, job: &JobRecord) -> Result<()> {
        let key = job.stored_key();
        self.txn.put(&key, job.stored_bytes()?)?;
        self.txn.put(job_index_key(&job.id), &key)?;
        self.note_written(job, &key);
        Ok(())
    }

    /// Note the entry of `index` at `at_ms`, written in this transaction.
    pub(crate) fn note_expiry_entry(&mut self, index: &ExpiryIndex, at_ms: u64) {
        self.notes.expiry.push((index.clone(), at_ms));
    }

    /// Note the new job `job`, written in this transaction.
    pub(crate) fn note_enqueued(&mut self, job: &JobRecord) {
        self.notes
            .enqueued
            .push((job.queue.clone(), job.id.clone()));
    }

    /// Note the record `job`, written at `key`, for the commit.
    fn note_written(&mut self, job: &JobRecord, key: &[u8]) {
        let notes = &mut self.notes;
        let earliest = |bound: &mut Option<u64>, at: Option<u64>| {
            if let Some(at) = at {
                *bound = Some(bound.map_or(at, |b| b.min(at)));
            }
        };
        match job.status {
            JobStatus::Pending => {
                let (min_key, count) = notes
                    .pending
                    .entry(job.queue.clone())
                    .or_insert_with(|| (key.to_vec(), 0));
                if key < min_key.as_slice() {
                    *min_key = key.to_vec();
                }
                *count += 1;
            }
            JobStatus::Scheduled => earliest(&mut notes.scheduled, job.run_at),
            JobStatus::Done => earliest(&mut notes.done, job.completed_at),
            JobStatus::Claimed | JobStatus::Dead => {}
        }
    }
}

/// Resolve a job id through its index entry within `txn`: returns the index
/// key, the record's current key and the decoded record, or `None` when the id
/// is not indexed or the indexed key does not have a record.
pub(crate) async fn get_indexed_job(
    txn: &DbTransaction,
    id: &str,
) -> Result<Option<(Vec<u8>, Bytes, JobRecord)>> {
    let index_key = job_index_key(id);
    let Some(current_key) = txn.get(&index_key).await? else {
        return Ok(None);
    };
    let Some(bytes) = txn.get(&current_key).await? else {
        return Ok(None);
    };
    let job = JobRecord::decode(&current_key, &bytes)?;
    Ok(Some((index_key, current_key, job)))
}

/// Verify that the claim `claim_id` identifies is still the job's live claim,
/// stage the deletion of its record and return the stored record. Returns
/// [`Error::ClaimLost`] when the claim ended.
///
/// This is the fence every settlement passes through, in three parts. The
/// registry claim id check rejects a settlement superseded by a re-claim. The
/// in-transaction record read rejects a settlement whose claim ended while its
/// registry entry, removed only after the ending commit, was still present. The
/// staged delete makes a settlement racing a concurrent requeue or re-claim a
/// transaction conflict. The claimed key does not identify a claim, because a
/// reap and a re-claim rewrite the same key. Call it inside the retry loop so a
/// retry re-runs both checks. A renewal changes neither the claim id nor the
/// record, so a claim held across a renewal still settles.
///
/// A settlement that writes a record must base it on the returned record, which
/// includes changes committed during the claim (a cancel's `cancel_requested`
/// flag). The claim's own copy predates them.
pub(crate) async fn take_claim(
    txn: &DbTransaction,
    registry: &LeaseRegistry,
    queue: &QueueName,
    id: &str,
    claim_id: u64,
) -> Result<JobRecord> {
    match registry.current(queue, id) {
        Some((_, current)) if current == claim_id => {}
        _ => return Err(Error::ClaimLost),
    }
    let key = claimed_key(queue, id);
    let Some(raw) = txn.get(&key).await? else {
        return Err(Error::ClaimLost);
    };
    let job = JobRecord::decode(&key, &raw)?;
    txn.delete(&key)?;
    Ok(job)
}

/// Whether a commit waits for its WAL flush before returning.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Durability {
    /// The commit returns once the write is durable.
    Awaited,
    /// The commit returns once the write is applied in memory. A transition
    /// committed this way must be redone on recovery when the flush is lost.
    Deferred,
}

/// Outcome of a commit without a storage error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Commit {
    Committed,
    /// Another transaction committed a conflicting write, and the caller
    /// retries against fresh state.
    Conflict,
}

/// Commit `txn`, a store transaction without job record writes. A transaction
/// conflict is reported as [`Commit::Conflict`], and a storage failure is
/// returned as an error.
pub(crate) async fn commit(txn: DbTransaction, durability: Durability) -> Result<Commit> {
    match txn.commit().await {
        Ok(handle) => {
            if let (Durability::Awaited, Some(handle)) = (durability, handle) {
                handle.await_durable().await?;
            }
            Ok(Commit::Committed)
        }
        Err(e) if e.kind() == slatedb::ErrorKind::Transaction => Ok(Commit::Conflict),
        Err(e) => Err(e.into()),
    }
}

/// The decision of one attempt of a body run by [`QueueCore::retry`], which the
/// body returns with its transaction.
pub(crate) enum Attempt<T> {
    /// Commit the transaction, then return the value.
    Commit(T),
    /// Return the value without a commit. Dropping the transaction rolls it
    /// back.
    Abort(T),
}

impl QueueCore {
    /// Begin a `Snapshot` transaction of the store.
    pub(crate) async fn begin(&self) -> Result<Txn> {
        Ok(Txn {
            txn: self.db.begin(IsolationLevel::Snapshot).await?,
            notes: CommitNotes::default(),
        })
    }

    /// Commit `txn` as [`commit`] does, then apply its notes when it committed.
    pub(crate) async fn commit(&self, txn: Txn, durability: Durability) -> Result<Commit> {
        let Txn { txn, notes } = txn;
        let outcome = commit(txn, durability).await?;
        if outcome == Commit::Committed {
            for (queue, (min_key, count)) in &notes.pending {
                self.claim_cursor
                    .note_pending_inserts(queue, min_key, *count);
            }
            if let Some(run_at) = notes.scheduled {
                self.scheduled_bound.lower(run_at);
            }
            if let Some(completed_at) = notes.done {
                self.done_bound.lower(completed_at);
            }
            for (index, at_ms) in &notes.expiry {
                index.written(*at_ms);
            }
            let mut enqueued: BTreeMap<&QueueName, u64> = BTreeMap::new();
            for (queue, id) in &notes.enqueued {
                *enqueued.entry(queue).or_default() += 1;
                debug!(queue = %queue, job_id = %id, "job enqueued");
            }
            for (queue, count) in enqueued {
                crate::obs::enqueued(queue, count);
            }
        }
        Ok(outcome)
    }

    /// Run `body` against a fresh transaction until its commit does not
    /// conflict. A [`Commit::Conflict`] begins a new transaction and runs
    /// `body` again against fresh state. An [`Attempt::Abort`] drops the
    /// transaction and returns the value. `body` takes the transaction by value
    /// and returns it with its decision, so its future does not borrow from the
    /// call and is `Send` wherever its captures are.
    pub(crate) async fn retry<T, F, Fut>(&self, durability: Durability, body: F) -> Result<T>
    where
        F: Fn(Txn) -> Fut,
        Fut: Future<Output = Result<(Txn, Attempt<T>)>>,
    {
        loop {
            match body(self.begin().await?).await? {
                (_, Attempt::Abort(value)) => return Ok(value),
                (txn, Attempt::Commit(value)) => match self.commit(txn, durability).await? {
                    Commit::Committed => return Ok(value),
                    Commit::Conflict => {
                        debug!("transaction conflict, retrying against fresh state");
                        continue;
                    }
                },
            }
        }
    }
}

/// Stage the transition of a claimed job to the dead-letter set: the record is
/// rewritten at its dead key with `error` as its last error, a `DeadLettered`
/// attempt is appended and the stats are adjusted. The caller stages the
/// deletion of the claimed key before the call.
fn stage_dead_letter(txn: &mut Txn, job: &mut JobRecord, now: u64, error: &str) -> Result<()> {
    let claimed_at = job.claimed_at.take();
    job.status = JobStatus::Dead;
    job.last_error = Some(error.to_string());
    job.failed_at = Some(now);
    append_attempt(
        txn,
        &job.id,
        &JobAttempt {
            attempt: job.attempts,
            claimed_at,
            recorded_at: now,
            outcome: AttemptOutcome::DeadLettered,
            error: Some(error.to_string()),
        },
    )?;
    txn.put_job(job)?;
    update_stats(
        txn,
        &job.queue,
        &[(JobStatus::Claimed, -1), (JobStatus::Dead, 1)],
    )?;
    warn!(
        queue = %job.queue,
        job_id = %job.id,
        attempts = job.attempts,
        error,
        "job dead-lettered"
    );
    Ok(())
}

/// Stage the move of a job into the pending key space from the state `from`,
/// whose record the caller already staged for deletion.
pub(crate) fn stage_to_pending(txn: &mut Txn, job: &mut JobRecord, from: JobStatus) -> Result<()> {
    job.status = JobStatus::Pending;
    job.run_at = None;
    txn.put_job(job)?;
    update_stats(txn, &job.queue, &[(JobStatus::Pending, 1), (from, -1)])?;
    Ok(())
}

/// The transition that ends a claim, chosen by the settling path from the
/// stored record read inside its transaction.
pub(crate) enum ClaimEnd<'a> {
    /// The job completed. With `keep` a done record is written. Otherwise the
    /// record, its index entry and its attempt history are removed.
    Done { keep: bool },
    /// The job returns to the queue: to pending immediately, or to the
    /// scheduled key space when `run_at` is set. `error`, when present, is
    /// recorded on the job and in its attempt history.
    Retry {
        run_at: Option<u64>,
        outcome: AttemptOutcome,
        error: Option<&'a str>,
    },
    /// The job is dead-lettered.
    Dead { error: &'a str },
}

impl ClaimEnd<'_> {
    /// Whether the transition ends the job, which every transition except a
    /// retry does.
    pub(crate) fn is_terminal(&self) -> bool {
        !matches!(self, Self::Retry { .. })
    }
}

/// Stage the removal of a job's last record, stored at `current_key`: the
/// record, its job index entry, its attempt history and, when the job has a
/// dedup key, the dedup index entry. The counter of the job's status is
/// decremented, except the `Done` counter, which counts completions and is
/// cumulative. Every path that removes a job's last record stages it here, so
/// the history shares the record's lifetime by construction.
pub(crate) fn stage_remove(txn: &DbTransaction, current_key: &[u8], job: &JobRecord) -> Result<()> {
    txn.delete(current_key)?;
    txn.delete(job_index_key(&job.id))?;
    txn.delete(attempt_history_key(&job.id))?;
    if let Some(dedup_key) = &job.dedup_key {
        txn.delete(dedup_index_key(&job.queue, dedup_key))?;
    }
    if job.status != JobStatus::Done {
        update_stats(txn, &job.queue, &[(job.status, -1)])?;
    }
    Ok(())
}

/// Stage the transition that ends a claim. Before the call, the caller reads
/// the stored record and stages the deletion of its claimed key. On return
/// `job` is the record as written.
pub(crate) fn stage_claim_end(
    txn: &mut Txn,
    job: &mut JobRecord,
    end: &ClaimEnd<'_>,
    now: u64,
) -> Result<()> {
    match *end {
        ClaimEnd::Done { keep } => {
            if keep {
                job.status = JobStatus::Done;
                job.completed_at = Some(now);
                append_attempt(
                    txn,
                    &job.id,
                    &JobAttempt {
                        attempt: job.attempts,
                        claimed_at: job.claimed_at,
                        recorded_at: now,
                        outcome: AttemptOutcome::Completed,
                        error: None,
                    },
                )?;
                txn.put_job(job)?;
                update_stats(txn, &job.queue, &[(JobStatus::Claimed, -1)])?;
            } else {
                // The removal stages the claimed key's deletion a second time.
                // The write set records the key once.
                stage_remove(txn, &claimed_key(&job.queue, &job.id), job)?;
                job.status = JobStatus::Done;
                job.completed_at = Some(now);
            }
            update_stats(txn, &job.queue, &[(JobStatus::Done, 1)])?;
            Ok(())
        }
        ClaimEnd::Retry {
            run_at,
            outcome,
            error,
        } => {
            let claimed_at = job.claimed_at.take();
            if let Some(error) = error {
                job.last_error = Some(error.to_string());
            }
            append_attempt(
                txn,
                &job.id,
                &JobAttempt {
                    attempt: job.attempts,
                    claimed_at,
                    recorded_at: now,
                    outcome,
                    error: error.map(str::to_string),
                },
            )?;
            match run_at {
                None => stage_to_pending(txn, job, JobStatus::Claimed)?,
                Some(run_at) => {
                    job.status = JobStatus::Scheduled;
                    job.run_at = Some(run_at);
                    txn.put_job(job)?;
                    update_stats(
                        txn,
                        &job.queue,
                        &[(JobStatus::Claimed, -1), (JobStatus::Scheduled, 1)],
                    )?;
                }
            }
            debug!(
                queue = %job.queue,
                job_id = %job.id,
                attempts = job.attempts,
                outcome = ?outcome,
                run_at,
                "job returned to the queue"
            );
            Ok(())
        }
        ClaimEnd::Dead { error } => stage_dead_letter(txn, job, now, error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::*;

    #[tokio::test]
    async fn a_commit_applies_the_notes_of_its_record_writes_and_a_drop_discards_them() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let core = &q.core;
        // An empty claim records the queue as known empty, until a pending
        // insert is noted.
        assert!(
            q.claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .is_none()
        );
        let job = JobRecord::new_pending("j".into(), qn("work"), b"p".to_vec(), 3, 0, 1);

        let mut txn = core.begin().await.unwrap();
        txn.put_job(&job).unwrap();
        drop(txn);
        assert!(core.claim_cursor.known_empty("work"));

        let mut txn = core.begin().await.unwrap();
        txn.put_job(&job).unwrap();
        let outcome = core.commit(txn, Durability::Awaited).await.unwrap();
        assert_eq!(outcome, Commit::Committed);
        assert!(!core.claim_cursor.known_empty("work"));

        q.close().await.unwrap();
    }
}
