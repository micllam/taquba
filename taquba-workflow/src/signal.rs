//! The signal wait of a step: the next step of a run is enqueued as a signal
//! wait with the timeout as its `run_at`, so a signal to its job id wakes it,
//! and the step that runs next reads the signal from its job record.

use std::time::Duration;

use taquba::{JobRecord, SettlementEffects};

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
    /// for a signal until `timeout` from now.
    pub(crate) async fn advance_on_signal(
        &self,
        claimed: &ClaimedStep<'_>,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> SettlementEffects {
        let opts = StepEnqueueOpts {
            run_at: Some(self.run_at_after(timeout)),
            signal_wait: true,
            ..claimed.next_step_opts()
        };
        self.advance(claimed, payload, opts).await
    }
}
