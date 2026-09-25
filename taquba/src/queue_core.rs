//! The state of an open queue that its handle and background tasks operate on.

use std::collections::HashMap;
use std::sync::Arc;

use std::future::Future;

use bytes::Bytes;
use slatedb::{Db, DbTransaction};

use crate::claim_cursor::ClaimCursor;
use crate::clock::Clock;
use crate::completion::CompletionWaiters;
use crate::error::Result;
use crate::job::{Claim, JobRecord, JobStatus};
use crate::lease_registry::LeaseRegistry;
use crate::options::QueueConfig;
use crate::payload_store::PayloadStore;
use crate::queue::WaitOutcome;
use crate::time_bound::TimeBound;
use crate::txn::ClaimEnd;
use crate::txn::{Attempt, Durability, get_indexed_job, retry};

/// The per-queue configurations of an open queue: a default and the
/// overrides keyed by queue name.
pub(crate) struct QueueConfigs {
    default: QueueConfig,
    per_queue: HashMap<String, QueueConfig>,
}

impl QueueConfigs {
    pub(crate) fn new(default: QueueConfig, per_queue: HashMap<String, QueueConfig>) -> Self {
        Self { default, per_queue }
    }

    /// The configuration of `queue`: its override, or the default.
    pub(crate) fn get(&self, queue: &str) -> &QueueConfig {
        self.per_queue.get(queue).unwrap_or(&self.default)
    }

    /// The default and every override.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &QueueConfig> {
        std::iter::once(&self.default).chain(self.per_queue.values())
    }
}

/// The handles every component of an open queue operates on: the
/// store, the clock, the configurations and the in-process registries.
/// Held as one `Arc` by the [`Queue`](crate::Queue) and by each
/// background task.
pub(crate) struct QueueCore {
    pub(crate) db: Arc<Db>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) configs: QueueConfigs,
    pub(crate) claim_cursor: ClaimCursor,
    /// The bound of the scheduled key space: the earliest `run_at` of a
    /// live scheduled key, lowered after every commit that writes one.
    pub(crate) scheduled_bound: TimeBound,
    pub(crate) lease_registry: LeaseRegistry,
    pub(crate) completion_waiters: Arc<CompletionWaiters>,
    pub(crate) payload_store: Arc<PayloadStore>,
    /// Source of job ids. Pending keys sort by id within a priority, so
    /// ids must increase with enqueue order, including inside one
    /// millisecond. One generator per store suffices: a store has a
    /// single writer process.
    pub(crate) id_gen: std::sync::Mutex<ulid::Generator>,
}

impl QueueCore {
    /// Current time in milliseconds since the UNIX epoch, read from
    /// the configured [`Clock`].
    pub(crate) fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    /// Complete a claim-ending transition after its commit. It removes
    /// the lease entry, fenced on `claim_id`, records the pending insert,
    /// deletes the payload object of a done job whose record was not kept
    /// and delivers a terminal outcome to the job's completion waiters.
    /// The delivered record includes its payload inline, taken from
    /// `claim` when one is given and otherwise fetched from the payload
    /// store, only when the job has waiters.
    pub(crate) async fn finish_claim_end(
        &self,
        job: &JobRecord,
        end: &ClaimEnd<'_>,
        claim_id: u64,
        pending_key: Option<&[u8]>,
        claim: Option<&Claim>,
    ) {
        self.lease_registry.remove(&job.queue, &job.id, claim_id);
        if let Some(key) = pending_key {
            self.claim_cursor.note_pending_insert(&job.queue, key);
        }
        if job.status == JobStatus::Scheduled
            && let Some(run_at) = job.run_at
        {
            self.scheduled_bound.lower(run_at);
        }
        if let ClaimEnd::Done { keep: false } = end {
            self.payload_store.delete_for(job).await;
        }
        if !end.is_terminal() || !self.completion_waiters.has_waiters(&job.id) {
            return;
        }
        let mut delivered = job.clone();
        if delivered.payload_ref.is_some() {
            match claim {
                Some(claim) => delivered.payload = claim.job().payload.clone(),
                None => {
                    if let Err(e) = self.payload_store.materialize(&mut delivered).await {
                        tracing::warn!(
                            queue = %job.queue,
                            job_id = %job.id,
                            error = %e,
                            "payload of a terminal job could not be fetched for its waiters"
                        );
                    }
                }
            }
        }
        let outcome = match job.status {
            JobStatus::Dead => WaitOutcome::Dead(Box::new(delivered)),
            _ => WaitOutcome::Done(Box::new(delivered)),
        };
        self.completion_waiters.settle(&job.id, || outcome);
    }
    /// One job transition addressed by id. The job's current record is
    /// read inside a retried transaction and passed to `transition`
    /// with the key it is stored at. The transition stages the next
    /// state and returns the call's value, or aborts with a value. A job
    /// without a record aborts with `missing()`.
    pub(crate) async fn transition_by_id<T, F, Fut>(
        &self,
        id: &str,
        durability: Durability,
        missing: impl Fn() -> Result<T>,
        transition: F,
    ) -> Result<T>
    where
        F: Fn(DbTransaction, Bytes, JobRecord) -> Fut,
        Fut: Future<Output = Result<Attempt<T>>>,
    {
        let (missing, transition) = (&missing, &transition);
        retry(&self.db, durability, |txn| async move {
            let Some((_, current_key, job)) = get_indexed_job(&txn, id).await? else {
                txn.rollback();
                return missing().map(Attempt::Abort);
            };
            transition(txn, current_key, job).await
        })
        .await
    }
}
