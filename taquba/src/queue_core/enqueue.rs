//! The transitions of [`QueueCore`] that write a new job, or commit settlement
//! effects without a job transition.

use crate::effects::{PreparedEffects, PreparedJob, SettlementEffects};
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
        let committed = {
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
        }
        .map(|staged| staged.map(|staged| self.note_staged_effects(staged)));
        self.finish_effects(
            prepared,
            committed
                .as_ref()
                .ok()
                .and_then(|results| results.as_deref()),
        )
        .await;
        committed
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
        // A payload object is live only when a new record committed. On a dedup
        // downgrade or an error the record does not exist, so remove the object
        // written above.
        if !matches!(written, Ok(Ok(_))) {
            self.payload_store.delete_for(&prepared.job).await;
        }
        let results = match &written {
            Ok(Ok((_, results))) => Some(results.as_slice()),
            _ => None,
        };
        self.finish_effects(effects, results).await;
        written.map(|written| match written {
            Ok((id, results)) => (EnqueueResult::New(id), results),
            Err(already_enqueued) => (EnqueueResult::AlreadyEnqueued(already_enqueued), Vec::new()),
        })
    }

    /// The transaction loop of [`Self::write_job`], after any payload offload.
    /// The inner `Ok` is the id of the new job with the results of the effects'
    /// enqueues, and the inner `Err` is the id of the pending or scheduled job
    /// that a dedup key matched, as `stage_job_writes` reports it.
    async fn write_job_txn(
        &self,
        prepared: &PreparedJob,
        effects: &PreparedEffects,
    ) -> Result<std::result::Result<(String, Vec<EnqueueResult>), String>> {
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
            (staged.id, self.note_staged_effects(staged_effects))
        }))
    }
}
