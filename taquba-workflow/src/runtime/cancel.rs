use taquba::JobStatus;

use super::{CancelRunOutcome, RuntimeCore};
use crate::durable::{self, DurableCancelRequest, DurableRun, Submission};
use crate::error::Result;
use crate::keys::{RunId, run_kv_key};
use crate::terminal::CancelOrigin;
use crate::worker::{ClaimedStep, Ending};

impl RuntimeCore {
    /// [`WorkflowRuntime::cancel`].
    ///
    /// [`WorkflowRuntime::cancel`]: super::WorkflowRuntime::cancel
    pub(crate) async fn cancel_run(
        &self,
        run_id: &RunId,
        reason: Option<String>,
    ) -> Result<CancelRunOutcome> {
        let Some((submission, request)) = self.request_cancel(run_id, reason).await? else {
            return Ok(CancelRunOutcome::NotActive);
        };
        // Settle the current step now: remove it while it is queued, or fire
        // the claim's cancellation token, the parent of
        // `Delivery::cancel_token`, while it runs. A step that settles in
        // between is followed to its successor. A step claimed after the
        // request terminates the run on its own.
        loop {
            let Some((_, job)) = self.view.current_job(run_id).await? else {
                // Terminated on its own after the request was recorded.
                return Ok(CancelRunOutcome::NotActive);
            };
            if job.status == JobStatus::Dead {
                // Dead-lettered by the queue outside the worker. Reconciliation
                // terminates the run, and the request does not apply.
                return Ok(CancelRunOutcome::NotActive);
            }
            let claimed = ClaimedStep::parse(&job)?;
            // The effects are built before the outcome is known, and the queue
            // applies them only on `Removed`.
            let ending = Ending::cancelled(CancelOrigin::Request, request.reason.clone());
            let termination = self.termination(&claimed, ending, None, submission);
            let effects = self.terminate_collecting_effects(&claimed, termination, None);
            match self.queue.cancel_with(&job.id, effects).await?.0 {
                taquba::CancelOutcome::Removed => return Ok(CancelRunOutcome::Removed),
                taquba::CancelOutcome::Requested => return Ok(CancelRunOutcome::Requested),
                taquba::CancelOutcome::NotFound => continue,
            }
        }
    }

    /// Record a cancellation request for `reason` on the run record of
    /// `run_id`. Returns the record's submission and the recorded request when
    /// the run is active, `None` otherwise. A run with a recorded request keeps
    /// the first request.
    async fn request_cancel(
        &self,
        run_id: &RunId,
        reason: Option<String>,
    ) -> Result<Option<(Submission, DurableCancelRequest)>> {
        let key = run_kv_key(run_id);
        loop {
            let Some(current) = self.queue.view().kv_get(&key).await? else {
                return Ok(None);
            };
            let DurableRun::Active(mut record) = durable::decode(&current)? else {
                return Ok(None);
            };
            if let Some(request) = record.cancel_request.clone() {
                return Ok(Some((Submission::from(&record), request)));
            }
            let request = DurableCancelRequest {
                reason: reason.clone(),
                requested_at_ms: self.clock.now_ms(),
            };
            record.cancel_request = Some(request.clone());
            let submission = Submission::from(&record);
            if self
                .queue
                .kv_compare_put(
                    &key,
                    Some(&current),
                    &durable::encode(&DurableRun::Active(record)),
                )
                .await?
            {
                return Ok(Some((submission, request)));
            }
        }
    }
}
