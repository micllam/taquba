//! The claim on a job: the value a worker settles a delivery with, and its
//! staged form before the claim's commit is durable.

use tokio_util::sync::CancellationToken;

use crate::job::JobRecord;
use crate::txn::Durable;

/// A held claim on a job: the record as it was delivered, the claim id that
/// identifies this delivery and the delivery's cancellation token.
///
/// Produced only by the `Queue::claim*` calls, and required by
/// [`Queue::ack`](crate::Queue::ack),
/// [`Queue::ack_with`](crate::Queue::ack_with),
/// [`Queue::nack`](crate::Queue::nack),
/// [`Queue::dead_letter`](crate::Queue::dead_letter) and
/// [`Queue::renew_lease`](crate::Queue::renew_lease). A [`JobRecord`] read
/// through [`QueueView::get_job`](crate::QueueView::get_job) or
/// [`QueueView::list_jobs`](crate::QueueView::list_jobs) therefore cannot
/// settle a delivery the caller does not hold.
///
/// Dereferences to the claimed [`JobRecord`], so a caller reads `claim.payload`
/// and `claim.id` directly.
///
/// Not [`Clone`]: a claim is the sole handle able to settle its delivery. The
/// record itself is reachable through [`Claim::job`] and [`Claim::into_job`].
#[derive(Debug)]
pub struct Claim {
    /// The claimed record, with its `claim_id`.
    job: JobRecord,
    cancel: CancellationToken,
}

/// A claim committed in memory and not yet delivered. Only
/// [`StagedClaim::into_claim`] makes it a [`Claim`], with the proof that its
/// commit is durable.
#[derive(Debug)]
pub(crate) struct StagedClaim {
    /// The claimed record, with its `claim_id`.
    job: JobRecord,
    cancel: CancellationToken,
}

impl StagedClaim {
    /// Stage the claimed record `job`, which must have a `claim_id`.
    pub(crate) fn new(job: JobRecord, cancel: CancellationToken) -> Self {
        debug_assert!(job.claim_id.is_some(), "a claimed record has a claim_id");
        StagedClaim { job, cancel }
    }

    pub(crate) fn job(&self) -> &JobRecord {
        &self.job
    }

    pub(crate) fn job_mut(&mut self) -> &mut JobRecord {
        &mut self.job
    }

    /// The claim for delivery to a worker, once `_durable` shows that the
    /// claim's commit is durable.
    pub(crate) fn into_claim(self, _durable: &Durable) -> Claim {
        Claim {
            job: self.job,
            cancel: self.cancel,
        }
    }
}

impl Claim {
    pub(crate) fn claim_id(&self) -> u64 {
        self.job.claim_id.expect("a claimed record has a claim_id")
    }

    /// The delivery's cooperative cancellation token, fired by
    /// [`Queue::cancel`](crate::Queue::cancel) while this claim is live, and
    /// already fired when the job was re-claimed with
    /// [`JobRecord::cancel_requested`] persisted.
    ///
    /// A caller can `select!` on it to stop early and settle the job as usual.
    /// The token is in-process state and is not persisted.
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }

    /// The claimed job.
    pub fn job(&self) -> &JobRecord {
        &self.job
    }

    /// Consume the claim and return its record.
    ///
    /// Settlement requires the claim, so the delivery cannot be settled after
    /// the call. The job returns to the queue when its lease expires.
    pub fn into_job(self) -> JobRecord {
        self.job
    }
}

impl std::ops::Deref for Claim {
    type Target = JobRecord;

    fn deref(&self) -> &JobRecord {
        &self.job
    }
}
