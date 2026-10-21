//! The signal wait of a step: the next step of a run is enqueued with a signal
//! key and the timeout as its `run_at`, so the queue indexes it as the waiter
//! at the key, and the step that runs next reads the signal from its job
//! record.

use std::time::Duration;

use taquba::{JobRecord, JobStatus, SettlementEffects, WorkerError};

use crate::error::worker_error;
use crate::runner::StepError;
use crate::runtime::{RuntimeCore, StepEnqueueOpts};
use crate::worker::ClaimedStep;

/// The signal that reached the step job `job`: the payload of the signal when a
/// signal woke the job, and `None` when the job became pending any other way.
pub(crate) fn step_signal(job: &JobRecord) -> Option<Vec<u8>> {
    job.woken_at
        .map(|_| job.wake_payload.clone().unwrap_or_default())
}

impl RuntimeCore {
    /// Build the effects that advance the run of `claimed` to a step that waits
    /// for a signal at `signal_key` until `timeout` from now. The queue
    /// registers the step as the waiter at the key in the acknowledgement
    /// transaction, or writes it to pending with a signal buffered at the key.
    /// A live waiter at the key fails the run as a permanent step error does.
    pub(crate) async fn advance_on_signal(
        &self,
        claimed: &ClaimedStep<'_>,
        payload: Vec<u8>,
        signal_key: &str,
        timeout: Duration,
        input_hash: [u8; 32],
    ) -> std::result::Result<SettlementEffects, WorkerError> {
        // One waiter per key. The check reads current state so the run fails
        // cleanly, and a registration that races it fails at the settlement.
        if let Some(existing) = self
            .queue
            .view()
            .signal_waiter(signal_key)
            .await
            .map_err(worker_error)?
            && let Ok(Some(job)) = self.queue.view().job_record(&existing).await
            && job.status == JobStatus::Scheduled
        {
            let message = format!("a waiter is already registered at signal key `{signal_key}`");
            return Err(self
                .terminating_failure(claimed, StepError::permanent(message), input_hash)
                .await);
        }
        let opts = StepEnqueueOpts {
            run_at: Some(self.run_at_after(timeout)),
            signal_key: Some(signal_key.to_string()),
            ..claimed.next_step_opts()
        };
        Ok(self.advance(claimed, payload, opts).await)
    }
}
