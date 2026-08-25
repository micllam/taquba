//! The effects of a settlement and the preparation of new job
//! records: validation, payload offload before the transaction, staging
//! inside it and the work that follows its commit.

use std::collections::HashMap;
use std::time::Duration;

use slatedb::DbTransaction;
use tracing::debug;
use ulid::Ulid;

use crate::error::{Error, Result};
use crate::job::{JobRecord, JobStatus};
use crate::keys::{dedup_index_key, job_index_key, pending_key, scheduled_key, user_scoped_key};
use crate::queue::{
    EnqueueOptions, EnqueueResult, SettlementEffects, validate_id_override, validate_kv_value_size,
    validate_queue_name,
};
use crate::queue_core::QueueCore;
use crate::stats::update_stats;
use crate::txn::put_job_record;

/// A job record prepared by [`Queue::prepare_job_record`], paired with
/// its primary key, awaiting staging into a transaction.
pub(crate) struct PreparedJob {
    pub(crate) job: JobRecord,
    pub(crate) key: Vec<u8>,
    pub(crate) id_override_used: bool,
}

/// [`SettlementEffects`] validated and prepared by [`Queue::prepare_effects`],
/// awaiting staging into a settlement transaction.
#[derive(Default)]
pub(crate) struct PreparedEffects {
    pub(crate) prepared_jobs: Vec<PreparedJob>,
    pub(crate) kv_writes: HashMap<Vec<u8>, Vec<u8>>,
    pub(crate) kv_deletes: Vec<Vec<u8>>,
}

/// Effects staged into a settlement transaction by
/// [`Queue::stage_effects`], retained for the work that follows the
/// commit.
pub(crate) struct StagedEffects {
    /// One result per prepared enqueue, in order.
    pub(crate) results: Vec<EnqueueResult>,
    /// The enqueues that staged a new record.
    pub(crate) jobs: Vec<StagedJob>,
}

/// Identity of a job staged by [`Queue::stage_job_writes`], retained
/// for post-commit bookkeeping.
pub(crate) struct StagedJob {
    pub(crate) id: String,
    pub(crate) queue: String,
    /// `Some` when the job landed in the pending key space, in which
    /// case the commit must be followed by a cursor insert note, which
    /// also wakes a waiting worker.
    pub(crate) pending_key: Option<Vec<u8>>,
}

impl QueueCore {
    /// Generate a job id. Ids increase with call order and take their
    /// timestamp from the queue's clock.
    pub(crate) fn next_job_id(&self) -> String {
        let at = std::time::UNIX_EPOCH + Duration::from_millis(self.now_ms());
        let mut generator = self.id_gen.lock().expect("id generator mutex poisoned");
        match generator.generate_from_datetime(at) {
            Ok(id) => id.to_string(),
            // Unreachable short of 2^80 ids inside one millisecond.
            Err(_) => Ulid::from_datetime(at).to_string(),
        }
    }

    /// Resolve [`EnqueueOptions`] against the queue's defaults and build
    /// the [`JobRecord`] + its primary key. Shared by every path that
    /// writes a new record; the paths diverge only in how they persist
    /// the prepared record.
    pub(crate) fn prepare_job_record(
        &self,
        queue: &str,
        payload: Vec<u8>,
        opts: EnqueueOptions,
    ) -> Result<PreparedJob> {
        validate_queue_name(queue)?;
        let cfg = self.configs.get(queue);
        let max_attempts = opts.max_attempts.unwrap_or(cfg.max_attempts);
        let priority = opts.priority.unwrap_or(cfg.default_priority);

        // A `run_at` that is at or before now is just an immediate enqueue.
        let run_at = opts.run_at.and_then(|when| {
            let ms = when
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            (ms > self.now_ms()).then_some(ms)
        });

        let (id, id_override_used) = match opts.id_override {
            Some(supplied) => {
                validate_id_override(&supplied)?;
                (supplied, true)
            }
            None => (self.next_job_id(), false),
        };

        let (status, key) = match run_at {
            Some(ms) => (JobStatus::Scheduled, scheduled_key(queue, ms, &id)),
            None => (JobStatus::Pending, pending_key(queue, priority, &id)),
        };

        let mut job = JobRecord::new_pending(
            id,
            queue.to_string(),
            payload,
            max_attempts,
            priority,
            self.now_ms(),
        );
        job.headers = opts.headers;
        job.status = status;
        job.run_at = run_at;
        job.dedup_key = opts.dedup_key;

        Ok(PreparedJob {
            job,
            key,
            id_override_used,
        })
    }

    /// Offload the oversized payloads of `prepared`, in order, before
    /// the transaction that writes their records. On a failure no
    /// object written here is left behind.
    pub(crate) async fn offload_prepared(&self, prepared: &mut [PreparedJob]) -> Result<()> {
        self.payload_store
            .offload_all(prepared.iter_mut().map(|p| &mut p.job))
            .await
    }

    /// Delete the payload objects of prepared jobs whose records will
    /// not be written.
    pub(crate) async fn discard_prepared(&self, prepared: &[PreparedJob]) {
        for prepared_job in prepared {
            self.payload_store.delete_for(&prepared_job.job).await;
        }
    }

    /// Release prepared effects once their settlement has ended:
    /// delete the payload objects of follow-up jobs that no committed
    /// record points at. `results` aligns index-wise with the prepared
    /// jobs: an [`EnqueueResult::AlreadyEnqueued`] entry marks a dedup
    /// downgrade whose object is unreferenced. `None` means no
    /// follow-up record committed (the settlement failed or took a
    /// branch that discards the effects), so every offloaded object is
    /// deleted. Every settlement path ends with this call, on every
    /// branch.
    pub(crate) async fn finish_effects(
        &self,
        prepared: PreparedEffects,
        results: Option<&[EnqueueResult]>,
    ) {
        match results {
            Some(results) => {
                for (prepared, result) in prepared.prepared_jobs.iter().zip(results) {
                    if matches!(result, EnqueueResult::AlreadyEnqueued(_)) {
                        self.payload_store.delete_for(&prepared.job).await;
                    }
                }
            }
            None => self.discard_prepared(&prepared.prepared_jobs).await,
        }
    }

    /// Validate `effects` and prepare them for staging: size-check the
    /// KV writes, build the follow-up job records and offload their
    /// payloads. Runs once, before a settlement's transaction loop, so
    /// the follow-up ids stay stable across conflict retries and a
    /// committed record never points at an unwritten object. The
    /// caller passes the result to [`Self::finish_effects`] once the
    /// settlement has ended.
    pub(crate) async fn prepare_effects(
        &self,
        effects: SettlementEffects,
    ) -> Result<PreparedEffects> {
        for value in effects.kv_writes.values() {
            validate_kv_value_size(value)?;
        }
        if let Some(key) = effects
            .kv_deletes
            .iter()
            .find(|k| effects.kv_writes.contains_key(*k))
        {
            return Err(Error::ConflictingKvEffect { key: key.clone() });
        }
        let mut prepared_jobs = effects
            .enqueues
            .into_iter()
            .map(|r| self.prepare_job_record(&r.queue, r.payload, r.options))
            .collect::<Result<Vec<_>>>()?;
        self.offload_prepared(&mut prepared_jobs).await?;
        Ok(PreparedEffects {
            prepared_jobs,
            kv_writes: effects.kv_writes,
            kv_deletes: effects.kv_deletes,
        })
    }

    /// Add prepared effects to a caller-owned settlement transaction.
    /// Called inside every iteration of the settlement's retry loop.
    /// A dedup hit downgrades that enqueue to
    /// [`EnqueueResult::AlreadyEnqueued`] without affecting the rest.
    /// After the transaction commits, the caller passes the result to
    /// [`Self::note_staged_effects`].
    pub(crate) async fn stage_effects(
        &self,
        txn: &DbTransaction,
        prepared: &PreparedEffects,
    ) -> Result<StagedEffects> {
        let mut staged = Vec::with_capacity(prepared.prepared_jobs.len());
        let mut results = Vec::with_capacity(prepared.prepared_jobs.len());
        for prepared_job in &prepared.prepared_jobs {
            match self.stage_job_writes(txn, prepared_job).await? {
                Ok(staged_job) => {
                    results.push(EnqueueResult::New(staged_job.id.clone()));
                    staged.push(staged_job);
                }
                Err(existing) => results.push(EnqueueResult::AlreadyEnqueued(existing)),
            }
        }
        for (k, v) in &prepared.kv_writes {
            txn.put(user_scoped_key(k), v)?;
        }
        for k in &prepared.kv_deletes {
            txn.delete(user_scoped_key(k))?;
        }
        Ok(StagedEffects {
            results,
            jobs: staged,
        })
    }

    /// Record the staged jobs after the commit and return their enqueue
    /// results.
    pub(crate) fn note_staged_effects(&self, staged: StagedEffects) -> Vec<EnqueueResult> {
        for staged_job in &staged.jobs {
            self.note_staged_job(staged_job);
        }
        staged.results
    }

    /// Add one prepared job's writes (record, job index, dedup index,
    /// stats delta) to a caller-owned transaction. Returns
    /// `Ok(Err(existing_id))` on a dedup hit, in which case no writes
    /// were added and the caller decides whether to roll back; the
    /// outer `Err` is reserved for real failures. After the
    /// transaction commits, the caller must pass the staged value to
    /// [`Self::note_staged_job`].
    pub(crate) async fn stage_job_writes(
        &self,
        txn: &DbTransaction,
        prepared: &PreparedJob,
    ) -> Result<std::result::Result<StagedJob, String>> {
        let PreparedJob {
            job,
            key,
            id_override_used,
        } = prepared;
        let dkey = job
            .dedup_key
            .as_ref()
            .map(|dk| dedup_index_key(&job.queue, dk));

        if let Some(ref dkey) = dkey
            && let Some(bytes) = txn.get(&dkey).await?
        {
            let existing = String::from_utf8(bytes.to_vec()).map_err(|_| Error::InvalidState)?;
            return Ok(Err(existing));
        }

        if *id_override_used && txn.get(job_index_key(&job.id)).await?.is_some() {
            return Err(Error::DuplicateJobId { id: job.id.clone() });
        }

        let value = job.stored_bytes()?;
        put_job_record(txn, key, &job_index_key(&job.id), &value)?;
        if let Some(ref dkey) = dkey {
            txn.put(dkey, job.id.as_bytes())?;
        }
        update_stats(txn, &job.queue, &[(job.status, 1)])?;

        Ok(Ok(StagedJob {
            id: job.id.clone(),
            queue: job.queue.clone(),
            pending_key: matches!(job.status, JobStatus::Pending).then(|| key.clone()),
        }))
    }

    /// Post-commit bookkeeping for one staged job: a Pending job is
    /// recorded on the claim cursor, which wakes a waiting worker; a
    /// Scheduled job becomes claimable later via the scheduler loop,
    /// which records its own insert.
    pub(crate) fn note_staged_job(&self, staged: &StagedJob) {
        if let Some(ref pending_key) = staged.pending_key {
            self.claim_cursor
                .note_pending_insert(&staged.queue, pending_key);
        }
        debug!(queue = %staged.queue, job_id = %staged.id, "job enqueued");
    }
}
