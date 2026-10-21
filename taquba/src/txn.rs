//! Transaction helpers shared by the queue, reaper and scheduler.
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::ops::Deref;

use bytes::Bytes;
use slatedb::{DbTransaction, IsolationLevel};

use tracing::{debug, warn};

use crate::error::{Error, Result};
use crate::expiry::ExpiryIndex;
use crate::history::{AttemptOutcome, JobAttempt, append_attempt};
use crate::inbox::record::InboxRecord;
use crate::job::{JobRecord, JobStatus};
use crate::keys::{
    KeyTag, QueueName, attempt_history_key, claimed_key, dedup_index_key, inbox_expiry_key,
    inbox_record_key, job_index_key,
};
use crate::lease_registry::LeaseRegistry;
use crate::queue_core::QueueCore;
use crate::stats::update_stats;

/// A store transaction that notes, for every job record and expiry entry it
/// writes and every job it enqueues, the process state that the commit updates.
/// [`QueueCore::commit`] applies the notes once the transaction commits. A
/// transaction that is dropped, rolled back or in conflict discards them.
///
/// The transaction counts the state change of every job it inserts, moves or
/// removes. [`QueueCore::commit`] writes the counters in the commit.
pub(crate) struct Txn {
    txn: DbTransaction,
    notes: CommitNotes,
    /// The change of each counter, by queue and state.
    stats: HashMap<(QueueName, JobStatus), i64>,
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
    /// The earliest `applied_at` of an inbox record written.
    inbox: Option<u64>,
}

impl Deref for Txn {
    type Target = DbTransaction;

    fn deref(&self) -> &DbTransaction {
        &self.txn
    }
}

impl Txn {
    /// Write the new job `job` and its index entry, and note it for the enqueue
    /// counter.
    pub(crate) fn insert_job(&mut self, job: &JobRecord) -> Result<()> {
        self.put_job(&job.stored_key(), job)?;
        self.count_move(&job.queue, None, Some(job.status));
        self.notes
            .enqueued
            .push((job.queue.clone(), job.id.clone()));
        Ok(())
    }

    /// Move `job` from its record at `from` to its record at
    /// [`JobRecord::stored_key`], and repoint its index entry to that key. A
    /// job that leaves the scheduled state leaves its signal wait.
    pub(crate) fn move_job(&mut self, from: &[u8], job: &mut JobRecord) -> Result<()> {
        if job.status != JobStatus::Scheduled {
            job.signal_wait = false;
        }
        let key = job.stored_key();
        if from == key.as_slice() {
            // The index entry already points to the key.
            self.txn.put(&key, job.stored_bytes()?)?;
            self.note_written(job, &key);
            return Ok(());
        }
        self.txn.delete(from)?;
        self.put_job(&key, job)?;
        self.count_move(&job.queue, Some(key_status(from)), Some(job.status));
        Ok(())
    }

    /// Stage the removal of a job's last record, stored at `key`: the record,
    /// its job index entry, its attempt history and, when the job has a dedup
    /// key, the dedup index entry. A `job` in the done state counts a
    /// completion. Every path that removes a job's last record stages it here,
    /// so the history shares the record's lifetime by construction.
    pub(crate) fn stage_remove(&mut self, key: &[u8], job: &JobRecord) -> Result<()> {
        self.txn.delete(key)?;
        self.txn.delete(job_index_key(&job.id))?;
        self.txn.delete(attempt_history_key(&job.id))?;
        if let Some(dedup_key) = &job.dedup_key {
            self.txn.delete(dedup_index_key(&job.queue, dedup_key))?;
        }
        let completed = (job.status == JobStatus::Done).then_some(JobStatus::Done);
        self.count_move(&job.queue, Some(key_status(key)), completed);
        Ok(())
    }

    /// Note the entry of `index` at `at_ms`, written in this transaction.
    pub(crate) fn note_expiry_entry(&mut self, index: &ExpiryIndex, at_ms: u64) {
        self.notes.expiry.push((index.clone(), at_ms));
    }

    /// Write the inbox record `record` of the entry `id` and its expiry entry,
    /// and note the record for the bound of the inbox expiry key space. A
    /// record at the id fails the call with [`Error::InboxAlreadyApplied`].
    pub(crate) async fn stage_inbox_record(
        &mut self,
        id: &str,
        record: &InboxRecord,
    ) -> Result<()> {
        let key = inbox_record_key(id);
        if self.txn.get(&key).await?.is_some() {
            return Err(Error::InboxAlreadyApplied { id: id.to_string() });
        }
        self.txn.put(key, rmp_serde::to_vec_named(record)?)?;
        self.txn
            .put(inbox_expiry_key(record.applied_at_ms, id), Vec::new())?;
        let at = record.applied_at_ms;
        self.notes.inbox = Some(self.notes.inbox.map_or(at, |b| b.min(at)));
        Ok(())
    }

    /// Write `job` at `key`, repoint its index entry to that key and note the
    /// record for the commit.
    fn put_job(&mut self, key: &[u8], job: &JobRecord) -> Result<()> {
        self.txn.put(key, job.stored_bytes()?)?;
        self.txn.put(job_index_key(&job.id), key)?;
        self.note_written(job, key);
        Ok(())
    }

    /// Count the move of a job of `queue` from the state `from` to the state
    /// `to`, where `None` is the absence of a record. The `Done` counter counts
    /// completions: a job that enters the done state increments it with or
    /// without a record. A job that leaves the done state does not decrement
    /// it.
    fn count_move(&mut self, queue: &QueueName, from: Option<JobStatus>, to: Option<JobStatus>) {
        if from == to {
            return;
        }
        if let Some(from) = from.filter(|from| *from != JobStatus::Done) {
            *self.stats.entry((queue.clone(), from)).or_default() -= 1;
        }
        if let Some(to) = to {
            *self.stats.entry((queue.clone(), to)).or_default() += 1;
        }
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

/// The state of the job record stored at `key`.
fn key_status(key: &[u8]) -> JobStatus {
    KeyTag::of(key)
        .and_then(KeyTag::job_status)
        .expect("a job record key")
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

/// Commit `txn`. A transaction conflict is reported as [`Commit::Conflict`],
/// and a storage failure is returned as an error.
async fn commit(txn: DbTransaction, durability: Durability) -> Result<Commit> {
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
            stats: HashMap::new(),
        })
    }

    /// Write the counters of `txn` and commit it as [`commit`] does, then apply
    /// its notes when it committed.
    pub(crate) async fn commit(&self, txn: Txn, durability: Durability) -> Result<Commit> {
        let Txn { txn, notes, stats } = txn;
        for ((queue, status), delta) in &stats {
            if *delta != 0 {
                update_stats(&txn, queue, *status, *delta)?;
            }
        }
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
            if let (Some(applied_at), Some(inbox)) = (notes.inbox, &self.inbox) {
                inbox.bound.lower(applied_at);
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

impl Txn {
    /// Move `job` from its record at `from` into the pending key space.
    pub(crate) fn stage_to_pending(&mut self, from: &[u8], job: &mut JobRecord) -> Result<()> {
        job.status = JobStatus::Pending;
        job.run_at = None;
        self.move_job(from, job)
    }

    /// Stage the transition that ends the claim of `job`, the stored claimed
    /// record. On return `job` is the record as written.
    pub(crate) fn stage_claim_end(
        &mut self,
        job: &mut JobRecord,
        end: &ClaimEnd<'_>,
        now: u64,
    ) -> Result<()> {
        let from = claimed_key(&job.queue, &job.id);
        match *end {
            ClaimEnd::Done { keep } => {
                job.status = JobStatus::Done;
                job.completed_at = Some(now);
                if !keep {
                    return self.stage_remove(&from, job);
                }
                append_attempt(
                    self,
                    &job.id,
                    &JobAttempt {
                        attempt: job.attempts,
                        claimed_at: job.claimed_at,
                        recorded_at: now,
                        outcome: AttemptOutcome::Completed,
                        error: None,
                    },
                )?;
                self.move_job(&from, job)
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
                    self,
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
                    None => self.stage_to_pending(&from, job)?,
                    Some(run_at) => {
                        job.status = JobStatus::Scheduled;
                        job.run_at = Some(run_at);
                        self.move_job(&from, job)?;
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
            ClaimEnd::Dead { error } => self.stage_dead_letter(&from, job, now, error),
        }
    }

    /// Move the claimed job `job` from its record at `from` to the dead-letter
    /// set, with `error` as its last error, and append a `DeadLettered`
    /// attempt.
    fn stage_dead_letter(
        &mut self,
        from: &[u8],
        job: &mut JobRecord,
        now: u64,
        error: &str,
    ) -> Result<()> {
        let claimed_at = job.claimed_at.take();
        job.status = JobStatus::Dead;
        job.last_error = Some(error.to_string());
        job.failed_at = Some(now);
        append_attempt(
            self,
            &job.id,
            &JobAttempt {
                attempt: job.attempts,
                claimed_at,
                recorded_at: now,
                outcome: AttemptOutcome::DeadLettered,
                error: Some(error.to_string()),
            },
        )?;
        self.move_job(from, job)?;
        warn!(
            queue = %job.queue,
            job_id = %job.id,
            attempts = job.attempts,
            error,
            "job dead-lettered"
        );
        Ok(())
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
        txn.insert_job(&job).unwrap();
        drop(txn);
        assert!(core.claim_cursor.known_empty("work"));

        let mut txn = core.begin().await.unwrap();
        txn.insert_job(&job).unwrap();
        let outcome = core.commit(txn, Durability::Awaited).await.unwrap();
        assert_eq!(outcome, Commit::Committed);
        assert!(!core.claim_cursor.known_empty("work"));

        q.close().await.unwrap();
    }
}
