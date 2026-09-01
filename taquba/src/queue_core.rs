//! The state of an open queue that its handle and background tasks operate on.

use std::collections::HashMap;
use std::sync::Arc;

use slatedb::Db;

use crate::claim_cursor::ClaimCursor;
use crate::clock::Clock;
use crate::completion::CompletionWaiters;
use crate::job::{Claim, JobRecord, JobStatus};
use crate::lease_registry::LeaseRegistry;
use crate::options::QueueConfig;
use crate::payload_store::PayloadStore;
use crate::queue::WaitOutcome;
use crate::txn::ClaimEnd;

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

    /// Complete a claim-ending transition after its commit: remove the
    /// lease entry, fenced on `token`; record the pending insert; delete
    /// the payload object of a done job whose record was not kept; and
    /// deliver a terminal outcome to the job's completion waiters. The
    /// delivered record carries its payload inline, taken from `claim`
    /// when one is given and otherwise fetched from the payload store,
    /// only when the job has waiters.
    pub(crate) async fn finish_claim_end(
        &self,
        job: &JobRecord,
        end: &ClaimEnd<'_>,
        token: u64,
        pending_key: Option<&[u8]>,
        claim: Option<&Claim>,
    ) {
        self.lease_registry.remove(&job.queue, &job.id, token);
        if let Some(key) = pending_key {
            self.claim_cursor.note_pending_insert(&job.queue, key);
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
}
