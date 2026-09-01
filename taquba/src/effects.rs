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
use crate::kv::validate_kv_value_size;
use crate::options::EnqueueOptions;
use crate::queue::{EnqueueResult, validate_id_override, validate_queue_name};
use crate::queue_core::QueueCore;
use crate::stats::update_stats;
use crate::txn::put_job_record;

/// One enqueue carried by [`SettlementEffects`].
#[derive(Debug, Clone)]
pub struct EnqueueRequest {
    /// Queue the job is enqueued on.
    pub queue: String,
    /// Job payload.
    pub payload: Vec<u8>,
    /// Per-job options; `run_at`, `dedup_key`, `priority`, and
    /// `id_override` are all honoured exactly as in
    /// [`Queue::enqueue_with`](crate::Queue::enqueue_with).
    pub options: EnqueueOptions,
}

/// Effects applied in the same transaction as a settlement: an
/// acknowledgement via [`Queue::ack_with`](crate::Queue::ack_with), a dead-letter via
/// [`Queue::dead_letter_with`](crate::Queue::dead_letter_with) or [`Queue::nack_with`](crate::Queue::nack_with), or a
/// pending-job removal via [`Queue::cancel_with`](crate::Queue::cancel_with). Either the
/// settlement and every effect commit together or nothing does. A
/// branch that applies no effects ([`Queue::nack_with`](crate::Queue::nack_with) while attempts
/// remain, [`Queue::cancel_with`](crate::Queue::cancel_with) other than
/// [`CancelOutcome::Removed`](crate::CancelOutcome::Removed)) commits without them. A key named in
/// both `kv_writes` and `kv_deletes` is rejected with
/// [`Error::ConflictingKvEffect`].
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct SettlementEffects {
    /// Jobs enqueued atomically with the settlement.
    pub enqueues: Vec<EnqueueRequest>,
    /// Writes applied to the caller KV namespace, as in
    /// [`Queue::enqueue_with_kv`](crate::Queue::enqueue_with_kv). Values are size-capped at
    /// [`MAX_KV_VALUE_SIZE`](crate::MAX_KV_VALUE_SIZE).
    pub kv_writes: HashMap<Vec<u8>, Vec<u8>>,
    /// Keys deleted from the caller KV namespace.
    pub kv_deletes: Vec<Vec<u8>>,
}

impl SettlementEffects {
    /// Set [`Self::enqueues`].
    #[must_use]
    pub fn enqueues(mut self, enqueues: Vec<EnqueueRequest>) -> Self {
        self.enqueues = enqueues;
        self
    }

    /// Add one request to [`Self::enqueues`].
    #[must_use]
    pub fn enqueue(mut self, request: EnqueueRequest) -> Self {
        self.enqueues.push(request);
        self
    }

    /// Set [`Self::kv_writes`].
    #[must_use]
    pub fn kv_writes(mut self, kv_writes: HashMap<Vec<u8>, Vec<u8>>) -> Self {
        self.kv_writes = kv_writes;
        self
    }

    /// Add one write to [`Self::kv_writes`].
    #[must_use]
    pub fn kv_put(mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        self.kv_writes.insert(key.into(), value.into());
        self
    }

    /// Set [`Self::kv_deletes`].
    #[must_use]
    pub fn kv_deletes(mut self, kv_deletes: Vec<Vec<u8>>) -> Self {
        self.kv_deletes = kv_deletes;
        self
    }

    /// Add one key to [`Self::kv_deletes`].
    #[must_use]
    pub fn kv_delete(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.kv_deletes.push(key.into());
        self
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::*;

    #[tokio::test(start_paused = true)]
    async fn ack_with_applies_no_effects_when_a_crash_interrupts_a_stalled_settlement() {
        let store = FaultStore::wrap();
        let clock = MockClock::new(1_700_000_000_000);
        let opts = || OpenOptions {
            clock: Arc::new(clock.clone()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(store.clone(), "test", opts())
            .await
            .unwrap();
        let effects = || SettlementEffects {
            enqueues: vec![EnqueueRequest {
                queue: "next".to_string(),
                payload: b"follow".to_vec(),
                options: EnqueueOptions::default(),
            }],
            kv_writes: HashMap::from([(b"runs/1".to_vec(), b"done".to_vec())]),
            kv_deletes: Vec::new(),
        };
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        // A durable barrier write, so the claim record is flushed before
        // faults are enabled and the crash loses only the settlement.
        q.kv_put(b"barrier", b"x").await.unwrap();

        // The settlement stalls on the unavailable store (SlateDB retries
        // transient put errors with backoff, driven virtually by the
        // paused runtime); the elapsed timeout drops the in-flight call
        // and the queue is dropped without a close, simulating a crash
        // mid-outage.
        store.fail_puts(true);
        let stalled =
            tokio::time::timeout(Duration::from_secs(30), q.ack_with(&job, effects())).await;
        assert!(stalled.is_err());
        drop(q);

        store.fail_puts(false);
        let q = Queue::open_with_options(store, "test", opts())
            .await
            .unwrap();
        // None of the settlement's effects survived the crash.
        assert!(q.kv_get(b"runs/1").await.unwrap().is_none());
        assert!(
            q.claim("next", Duration::from_secs(5))
                .await
                .unwrap()
                .is_none()
        );
        // The job is still owned by the crashed claim; expire the lease
        // and redeliver.
        clock.advance(Duration::from_secs(60));
        q.reap_now().await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.payload, b"job");
        let results = q.ack_with(&job, effects()).await.unwrap();
        assert!(matches!(results[0], EnqueueResult::New(_)));
        assert_eq!(
            q.kv_get(b"runs/1").await.unwrap().as_deref(),
            Some(b"done".as_slice())
        );
        let follow = q
            .claim("next", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(follow.payload, b"follow");
        assert!(
            q.claim("next", Duration::from_secs(5))
                .await
                .unwrap()
                .is_none()
        );
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn ack_with_applies_enqueue_and_kv_effects_atomically() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        q.enqueue_with_kv(
            "work",
            b"first".to_vec(),
            EnqueueOptions::default(),
            HashMap::from([(b"runs/1".to_vec(), b"active".to_vec())]),
        )
        .await
        .unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();

        let results = q
            .ack_with(
                &job,
                SettlementEffects {
                    enqueues: vec![EnqueueRequest {
                        queue: "next".to_string(),
                        payload: b"second".to_vec(),
                        options: EnqueueOptions::default(),
                    }],
                    kv_writes: HashMap::from([(b"runs/2".to_vec(), b"done".to_vec())]),
                    kv_deletes: vec![b"runs/1".to_vec()],
                },
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], EnqueueResult::New(_)));

        let follow_up = q.claim("next", lease).await.unwrap().unwrap();
        assert_eq!(follow_up.payload, b"second");
        q.ack(&follow_up).await.unwrap();
        assert!(q.kv_get(b"runs/1").await.unwrap().is_none());
        assert_eq!(
            q.kv_get(b"runs/2").await.unwrap().as_deref(),
            Some(b"done".as_slice()),
        );
        assert_eq!(q.stats("work").await.unwrap().done, 1);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_lost_claim_applies_no_effects() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();
        q.ack(&job).await.unwrap();

        let effects = || SettlementEffects {
            enqueues: vec![EnqueueRequest {
                queue: "next".to_string(),
                payload: b"x".to_vec(),
                options: EnqueueOptions::default(),
            }],
            kv_writes: HashMap::from([(b"k".to_vec(), b"v".to_vec())]),
            kv_deletes: Vec::new(),
        };
        assert!(matches!(
            q.ack_with(&job, effects()).await,
            Err(Error::ClaimLost)
        ));
        assert!(matches!(
            q.nack_with(&job, "late", effects()).await,
            Err(Error::ClaimLost)
        ));
        assert!(matches!(
            q.dead_letter_with(&job, "late", effects()).await,
            Err(Error::ClaimLost)
        ));
        assert_eq!(q.stats("next").await.unwrap().pending, 0);
        assert!(q.kv_get(b"k").await.unwrap().is_none());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn ack_with_dedup_hit_downgrades_one_request() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        let existing_id = q
            .enqueue_with(
                "next",
                b"existing".to_vec(),
                EnqueueOptions {
                    dedup_key: Some("dk".to_string()),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .unwrap();
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();

        let results = q
            .ack_with(
                &job,
                SettlementEffects {
                    enqueues: vec![
                        EnqueueRequest {
                            queue: "next".to_string(),
                            payload: b"dup".to_vec(),
                            options: EnqueueOptions {
                                dedup_key: Some("dk".to_string()),
                                ..EnqueueOptions::default()
                            },
                        },
                        EnqueueRequest {
                            queue: "next".to_string(),
                            payload: b"fresh".to_vec(),
                            options: EnqueueOptions::default(),
                        },
                    ],
                    ..SettlementEffects::default()
                },
            )
            .await
            .unwrap();
        assert!(matches!(&results[0], EnqueueResult::AlreadyEnqueued(id) if *id == existing_id));
        assert!(matches!(&results[1], EnqueueResult::New(_)));
        assert_eq!(q.stats("next").await.unwrap().pending, 2);
        assert_eq!(q.stats("work").await.unwrap().done, 1);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn dead_letter_with_applies_enqueue_and_kv_effects_atomically() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        assert!(q.claim("notify", lease).await.unwrap().is_none());
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();

        let results = q
            .dead_letter_with(
                &job,
                "bad input",
                SettlementEffects {
                    enqueues: vec![EnqueueRequest {
                        queue: "notify".to_string(),
                        payload: b"failed".to_vec(),
                        options: EnqueueOptions::default(),
                    }],
                    kv_writes: HashMap::from([(b"runs/1".to_vec(), b"failed".to_vec())]),
                    kv_deletes: Vec::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], EnqueueResult::New(_)));

        assert_eq!(q.stats("work").await.unwrap().dead, 1);
        assert_eq!(q.stats("notify").await.unwrap().pending, 1);
        assert_eq!(
            q.kv_get(b"runs/1").await.unwrap().as_deref(),
            Some(b"failed".as_slice()),
        );
        let follow_up = q.claim("notify", lease).await.unwrap().unwrap();
        assert_eq!(follow_up.payload, b"failed");
        q.ack(&follow_up).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn nack_with_discards_effects_while_attempts_remain() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();

        let outcome = q
            .nack_with(
                &job,
                "transient",
                SettlementEffects {
                    enqueues: vec![EnqueueRequest {
                        queue: "notify".to_string(),
                        payload: b"x".to_vec(),
                        options: EnqueueOptions::default(),
                    }],
                    kv_writes: HashMap::from([(b"k".to_vec(), b"v".to_vec())]),
                    kv_deletes: Vec::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome, NackOutcome::Retried);

        let stats = q.stats("work").await.unwrap();
        assert_eq!(stats.scheduled, 1);
        assert_eq!(stats.dead, 0);
        assert_eq!(q.stats("notify").await.unwrap().pending, 0);
        assert!(q.kv_get(b"k").await.unwrap().is_none());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn nack_with_applies_effects_when_it_dead_letters() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        assert!(q.claim("notify", lease).await.unwrap().is_none());
        q.enqueue_with(
            "work",
            b"job".to_vec(),
            EnqueueOptions {
                max_attempts: Some(1),
                ..EnqueueOptions::default()
            },
        )
        .await
        .unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();

        let outcome = q
            .nack_with(
                &job,
                "final failure",
                SettlementEffects {
                    enqueues: vec![EnqueueRequest {
                        queue: "notify".to_string(),
                        payload: b"failed".to_vec(),
                        options: EnqueueOptions::default(),
                    }],
                    kv_writes: HashMap::from([(b"runs/1".to_vec(), b"failed".to_vec())]),
                    kv_deletes: Vec::new(),
                },
            )
            .await
            .unwrap();
        let NackOutcome::DeadLettered(results) = outcome else {
            panic!("expected a dead-lettering nack, got {outcome:?}");
        };
        assert!(matches!(results[0], EnqueueResult::New(_)));

        assert_eq!(q.stats("work").await.unwrap().dead, 1);
        assert_eq!(q.stats("notify").await.unwrap().pending, 1);
        assert_eq!(
            q.kv_get(b"runs/1").await.unwrap().as_deref(),
            Some(b"failed".as_slice()),
        );
        let follow_up = q.claim("notify", lease).await.unwrap().unwrap();
        assert_eq!(follow_up.payload, b"failed");
        q.ack(&follow_up).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_with_applies_effects_with_the_removal() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        assert!(q.claim("notify", lease).await.unwrap().is_none());
        let id = q.enqueue("work", b"job".to_vec()).await.unwrap();

        let (outcome, results) = q
            .cancel_with(
                &id,
                SettlementEffects {
                    enqueues: vec![EnqueueRequest {
                        queue: "notify".to_string(),
                        payload: b"cancelled".to_vec(),
                        options: EnqueueOptions::default(),
                    }],
                    kv_writes: HashMap::new(),
                    kv_deletes: vec![b"runs/1".to_vec()],
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome, CancelOutcome::Removed);
        assert!(matches!(results[0], EnqueueResult::New(_)));

        assert_eq!(q.stats("work").await.unwrap().pending, 0);
        assert_eq!(q.stats("notify").await.unwrap().pending, 1);
        assert!(q.get_job(&id).await.unwrap().is_none());
        let follow_up = q.claim("notify", lease).await.unwrap().unwrap();
        assert_eq!(follow_up.payload, b"cancelled");
        q.ack(&follow_up).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_with_discards_effects_on_a_claimed_job() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        let lease = Duration::from_secs(5);
        let id = q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();

        let (outcome, results) = q
            .cancel_with(
                &id,
                SettlementEffects {
                    enqueues: vec![EnqueueRequest {
                        queue: "notify".to_string(),
                        payload: b"x".to_vec(),
                        options: EnqueueOptions::default(),
                    }],
                    kv_writes: HashMap::from([(b"k".to_vec(), b"v".to_vec())]),
                    kv_deletes: Vec::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome, CancelOutcome::Requested);
        assert!(results.is_empty());
        assert_eq!(q.stats("notify").await.unwrap().pending, 0);
        assert!(q.kv_get(b"k").await.unwrap().is_none());

        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_with_discards_effects_on_an_unknown_job() {
        let q = Queue::open(make_store(), "test").await.unwrap();

        let (outcome, results) = q
            .cancel_with(
                "missing",
                SettlementEffects {
                    kv_writes: HashMap::from([(b"k".to_vec(), b"v".to_vec())]),
                    ..SettlementEffects::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome, CancelOutcome::NotFound);
        assert!(results.is_empty());
        assert!(q.kv_get(b"k").await.unwrap().is_none());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_with_deletes_the_payload_object_of_a_discarded_follow_up() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();
        let id = q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        let (outcome, _) = q
            .cancel_with(
                &id,
                SettlementEffects {
                    enqueues: vec![EnqueueRequest {
                        queue: "notify".to_string(),
                        payload: vec![1u8; 256],
                        options: EnqueueOptions::default(),
                    }],
                    ..SettlementEffects::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome, CancelOutcome::Requested);
        assert_eq!(
            object_count(&store, "test-payloads").await,
            0,
            "the discarded follow-up's offloaded object is removed"
        );

        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_key_both_written_and_deleted_is_rejected_before_the_settlement() {
        let q = Queue::open(make_store(), "test").await.unwrap();
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();

        let effects = SettlementEffects {
            kv_writes: HashMap::from([(b"k".to_vec(), b"v".to_vec())]),
            kv_deletes: vec![b"k".to_vec()],
            ..SettlementEffects::default()
        };
        let err = q.ack_with(&job, effects.clone()).await.unwrap_err();
        assert!(matches!(err, Error::ConflictingKvEffect { ref key } if key == b"k"));
        assert!(q.kv_get(b"k").await.unwrap().is_none());
        assert_eq!(
            q.stats("work").await.unwrap().claimed,
            1,
            "the claim is untouched"
        );

        let err = q.nack_with(&job, "e", effects.clone()).await.unwrap_err();
        assert!(matches!(err, Error::ConflictingKvEffect { .. }));
        let err = q.dead_letter_with(&job, "e", effects).await.unwrap_err();
        assert!(matches!(err, Error::ConflictingKvEffect { .. }));

        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    fn offloaded_follow_up() -> SettlementEffects {
        SettlementEffects {
            enqueues: vec![EnqueueRequest {
                queue: "notify".to_string(),
                payload: vec![1u8; 256],
                options: EnqueueOptions::default(),
            }],
            ..SettlementEffects::default()
        }
    }

    #[tokio::test]
    async fn nack_with_deletes_the_payload_object_of_a_discarded_follow_up() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        let outcome = q
            .nack_with(&job, "transient", offloaded_follow_up())
            .await
            .unwrap();
        assert_eq!(outcome, NackOutcome::Retried);
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_lost_claim_deletes_the_payload_object_of_a_prepared_follow_up() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.ack(&job).await.unwrap();

        let err = q
            .dead_letter_with(&job, "late", offloaded_follow_up())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ClaimLost));
        assert_eq!(object_count(&store, "test-payloads").await, 0);

        let err = q
            .nack_with(&job, "late", offloaded_follow_up())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ClaimLost));
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_with_deletes_the_payload_object_when_the_job_is_unknown() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let (outcome, _) = q
            .cancel_with("no-such-job", offloaded_follow_up())
            .await
            .unwrap();
        assert_eq!(outcome, CancelOutcome::NotFound);
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn ack_with_schedules_a_future_effect() {
        let initial = 1_700_000_000_000u64;
        let opts = OpenOptions {
            clock: Arc::new(MockClock::new(initial)),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap();
        let lease = Duration::from_secs(5);
        q.enqueue("work", b"job".to_vec()).await.unwrap();
        let job = q.claim("work", lease).await.unwrap().unwrap();

        q.ack_with(
            &job,
            SettlementEffects {
                enqueues: vec![EnqueueRequest {
                    queue: "next".to_string(),
                    payload: b"later".to_vec(),
                    options: EnqueueOptions {
                        run_at: Some(
                            std::time::UNIX_EPOCH + Duration::from_millis(initial + 300_000),
                        ),
                        ..EnqueueOptions::default()
                    },
                }],
                ..SettlementEffects::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(q.stats("next").await.unwrap().scheduled, 1);
        assert!(q.claim("next", lease).await.unwrap().is_none());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn ack_effect_enqueues_offload_and_clean_up_on_dedup_downgrade() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        // An existing job holds the dedup key the follow-up enqueue will hit.
        q.enqueue_with(
            "next",
            vec![1u8; 16],
            EnqueueOptions {
                dedup_key: Some("once".to_string()),
                ..EnqueueOptions::default()
            },
        )
        .await
        .unwrap();

        q.enqueue("work", vec![2u8; 16]).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        let results = q
            .ack_with(
                &job,
                SettlementEffects {
                    enqueues: vec![
                        EnqueueRequest {
                            queue: "next".to_string(),
                            payload: vec![3u8; 256],
                            options: EnqueueOptions {
                                dedup_key: Some("once".to_string()),
                                ..EnqueueOptions::default()
                            },
                        },
                        EnqueueRequest {
                            queue: "next".to_string(),
                            payload: vec![4u8; 256],
                            options: EnqueueOptions::default(),
                        },
                    ],
                    ..SettlementEffects::default()
                },
            )
            .await
            .unwrap();

        assert!(matches!(results[0], EnqueueResult::AlreadyEnqueued(_)));
        assert!(matches!(results[1], EnqueueResult::New(_)));
        // The dedup-downgraded follow-up job's payload object is removed;
        // the committed follow-up job's object remains.
        assert_eq!(object_count(&store, "test-payloads").await, 1);

        let follow_up_ids = match &results[1] {
            EnqueueResult::New(id) => id.clone(),
            _ => unreachable!(),
        };
        let follow_up = q.get_job(&follow_up_ids).await.unwrap().unwrap();
        assert_eq!(follow_up.payload, vec![4u8; 256]);
        q.close().await.unwrap();
    }
}
