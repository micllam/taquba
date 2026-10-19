//! The transitions of [`QueueCore`] that write a new job, or commit settlement
//! effects without a job transition.

use crate::effects::{PreparedEffects, PreparedJob, SettlementEffects, StagedEffects};
use crate::error::Result;
use crate::keys::user_scoped_key;
use crate::kv::kv_state_matches;
use crate::queue::EnqueueResult;
use crate::queue_core::QueueCore;
use crate::txn::{Attempt, Durability, retry};

impl QueueCore {
    /// Body of `commit_effects` and `kv_compare_commit`: the transaction loop
    /// over the prepared effects, with every compare of `compares` (a key and
    /// its expected state) tested first.
    pub(crate) async fn commit_effects_where(
        &self,
        effects: SettlementEffects,
        compares: &[(&[u8], Option<&[u8]>)],
    ) -> Result<Option<Vec<EnqueueResult>>> {
        let prepared = self.prepare_effects(effects).await?;
        let compares: Vec<_> = compares
            .iter()
            .map(|(key, expected)| (user_scoped_key(key), *expected))
            .collect();
        let mut committed = {
            let (prepared, compares) = (&prepared, &compares);
            retry(&self.db, Durability::Awaited, |txn| async move {
                for (scoped, expected) in compares {
                    if !kv_state_matches(&txn, scoped, *expected).await? {
                        txn.rollback();
                        return Ok(Attempt::Abort(None));
                    }
                }
                let staged = self.stage_effects(&txn, prepared).await?;
                Ok(Attempt::Commit(txn, Some(staged)))
            })
            .await
        };
        let staged = committed.as_mut().ok().and_then(Option::take);
        let results = self.finish_effects(prepared, staged).await;
        committed.map(|_| results)
    }

    /// Persist a prepared [`JobRecord`](crate::JobRecord) with its prepared
    /// `effects` in a single transaction, checking the dedup index and, for a
    /// caller-supplied id, the job index. Retries on transaction conflict.
    ///
    /// Returns [`EnqueueResult::AlreadyEnqueued`] without the effects if
    /// `job.dedup_key` is set and a pending or scheduled job with the same
    /// dedup key already exists. Returns
    /// [`Error::DuplicateJobId`](crate::Error::DuplicateJobId) if `id_override`
    /// was used and the id is already indexed. Otherwise writes the record, the
    /// job index, the dedup index (when set) and the effects, and returns
    /// [`EnqueueResult::New`] with the results of the effects' enqueues. Every
    /// branch ends with [`QueueCore::finish_effects`].
    pub(crate) async fn write_job(
        &self,
        mut prepared: PreparedJob,
        effects: PreparedEffects,
    ) -> Result<(EnqueueResult, Vec<EnqueueResult>)> {
        let written = match self.payload_store.offload(&mut prepared.job).await {
            Ok(()) => self.write_job_txn(&prepared, &effects).await,
            Err(err) => Err(err),
        };
        let (outcome, staged) = match written {
            Ok(Ok((id, staged))) => (Ok(EnqueueResult::New(id)), Some(staged)),
            Ok(Err(existing)) => (Ok(EnqueueResult::AlreadyEnqueued(existing)), None),
            Err(err) => (Err(err), None),
        };
        // A payload object is live only when a new record committed. On a dedup
        // downgrade or an error the record does not exist, so remove the object
        // written above.
        if staged.is_none() {
            self.payload_store.delete_for(&prepared.job).await;
        }
        let results = self.finish_effects(effects, staged).await;
        outcome.map(|outcome| (outcome, results.unwrap_or_default()))
    }

    /// The transaction loop of [`Self::write_job`], after any payload offload.
    /// The inner `Ok` is the id of the new job with the staged effects, and the
    /// inner `Err` is the id of the pending or scheduled job that a dedup key
    /// matched, as `stage_job_writes` reports it.
    async fn write_job_txn(
        &self,
        prepared: &PreparedJob,
        effects: &PreparedEffects,
    ) -> Result<std::result::Result<(String, StagedEffects), String>> {
        let timer = crate::obs::start();
        let written = retry(&self.db, Durability::Awaited, |txn| async move {
            let staged = match self.stage_job_writes(&txn, prepared).await? {
                Ok(staged) => staged,
                Err(already_enqueued) => {
                    txn.rollback();
                    return Ok(Attempt::Abort(Err(already_enqueued)));
                }
            };
            let staged_effects = self.stage_effects(&txn, effects).await?;
            Ok(Attempt::Commit(txn, Ok((staged, staged_effects))))
        })
        .await?;
        Ok(written.map(|(staged, staged_effects)| {
            crate::obs::enqueue_committed(&staged.queue, timer);
            self.note_staged_job(&staged);
            (staged.id, staged_effects)
        }))
    }
}
