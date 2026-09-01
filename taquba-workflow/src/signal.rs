//! Durable signal delivery: one buffered signal per correlation key,
//! held in three key spaces of the caller KV namespace. The waiter
//! index maps a correlation key to the step job scheduled to wait on
//! it, the buffer holds a signal that arrived while no waiter was
//! registered and the delivered record stores a consumed payload under
//! `(run id, step)` so a redelivered step observes the same signal.
//! Registration consumes a buffered signal at the previous step's
//! settlement; delivery to a registered waiter wakes its scheduled
//! job; a waiter promoted by its timeout consumes the buffer at claim
//! time.

use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};

use taquba::{FailWith, JobRecord, JobStatus, PermanentFailure, SettlementEffects, WorkerError};
use tracing::warn;

use crate::error::Result;
use crate::keys::{
    HEADER_SIGNAL_DELIVERED, HEADER_SIGNAL_WAIT, signal_buf_kv_key, signal_delivered_kv_key,
    signal_wait_kv_key,
};
use crate::runner::StepRunner;
use crate::runtime::{RuntimeInner, StepEnqueueOpts, WorkflowRuntime};
use crate::terminal::{RunOutcome, TerminalHook, TerminalStatus};

/// Outcome of [`WorkflowRuntime::signal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalOutcome {
    /// A waiter was registered for the correlation key and has been woken;
    /// its step runs next with [`Step::signal`](crate::Step::signal) set to the payload.
    Delivered,
    /// No waiter was woken. The signal is buffered durably under the
    /// correlation key and is consumed by the next waiter registered for
    /// it, or discarded by [`WorkflowRuntime::clear_signal`].
    Buffered,
}

/// Number of waiter-index reads [`WorkflowRuntime::signal`] performs
/// before concluding no waiter exists, and the pause between them. A
/// registration settling concurrently becomes visible within one
/// acknowledgement commit, which these reads cover.
const SIGNAL_WAIT_READ_ATTEMPTS: u32 = 10;
const SIGNAL_WAIT_READ_INTERVAL: Duration = Duration::from_millis(25);

impl<R: StepRunner, H: TerminalHook> WorkflowRuntime<R, H> {
    /// Deliver a signal for `correlation_key`, waking the run waiting on
    /// it via [`Trigger::OnSignal`](crate::Trigger::OnSignal).
    ///
    /// When a waiter is registered and still waiting, its next step is
    /// promoted immediately and observes `payload` on [`Step::signal`](crate::Step::signal);
    /// the call returns [`SignalOutcome::Delivered`]. Otherwise the signal
    /// is buffered durably under the correlation key and the next waiter
    /// registered for it consumes the buffered payload at its
    /// registration; the call returns [`SignalOutcome::Buffered`].
    ///
    /// One buffered signal is held per correlation key: a second signal
    /// before consumption replaces the first. A buffered signal persists
    /// until a waiter consumes it or [`Self::clear_signal`] discards it.
    /// The buffer write is durable before the call returns, so a signal
    /// is never lost once this call returns; delivery to a waiter whose
    /// registration is settling concurrently falls back to the buffer and
    /// reaches it no later than its timeout.
    pub async fn signal(&self, correlation_key: &str, payload: Vec<u8>) -> Result<SignalOutcome> {
        let queue = &self.inner.queue;
        let buf_key = signal_buf_kv_key(correlation_key);
        // Buffer first, durably: a waiter registering concurrently reads
        // the buffer at its settlement, so the signal is never lost even
        // if the waiter index is not yet visible below.
        queue.kv_put(&buf_key, &payload).await?;

        let wait_key = signal_wait_kv_key(correlation_key);
        for attempt in 0..SIGNAL_WAIT_READ_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(SIGNAL_WAIT_READ_INTERVAL).await;
            }
            let Some(waiter) = queue.kv_get(&wait_key).await? else {
                continue;
            };
            let Ok(job_id) = std::str::from_utf8(&waiter).map(str::to_string) else {
                let _ = queue.kv_compare_delete(&wait_key, &waiter).await;
                return Ok(SignalOutcome::Buffered);
            };
            return match queue.wake_scheduled(&job_id, Some(payload.clone())).await? {
                taquba::WakeOutcome::Woken => {
                    let _ = queue.kv_compare_delete(&buf_key, &payload).await;
                    let _ = queue.kv_compare_delete(&wait_key, &waiter).await;
                    Ok(SignalOutcome::Delivered)
                }
                taquba::WakeOutcome::NotScheduled | taquba::WakeOutcome::NotFound => {
                    // Stale index entry: the waiter was already promoted or
                    // its run was cancelled. Remove the entry; the signal
                    // stays buffered.
                    let _ = queue.kv_compare_delete(&wait_key, &waiter).await;
                    Ok(SignalOutcome::Buffered)
                }
            };
        }
        Ok(SignalOutcome::Buffered)
    }

    /// Discard the buffered signal for `correlation_key`, if one exists.
    /// Returns `true` when a buffered signal was removed.
    pub async fn clear_signal(&self, correlation_key: &str) -> Result<bool> {
        let queue = &self.inner.queue;
        let buf_key = signal_buf_kv_key(correlation_key);
        loop {
            let Some(current) = queue.kv_get(&buf_key).await? else {
                return Ok(false);
            };
            if queue.kv_compare_delete(&buf_key, &current).await? {
                return Ok(true);
            }
        }
    }
}

impl<R: StepRunner, H: TerminalHook> RuntimeInner<R, H> {
    /// Build the effects that advance the run to a step that waits for a
    /// signal for `correlation_key`. When a buffered signal already exists it is
    /// consumed: the next step is enqueued immediately, the payload is
    /// recorded under the durable delivered key and the buffer entry is
    /// deleted, all in the acknowledgement transaction. Otherwise the next
    /// step is scheduled `timeout` from now and the waiter index entry
    /// joins the same transaction.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn advance_on_signal(
        &self,
        run_id: &str,
        next_step: u32,
        payload: Vec<u8>,
        user_headers: &HashMap<String, String>,
        base_opts: StepEnqueueOpts,
        correlation_key: &str,
        timeout: Duration,
    ) -> std::result::Result<SettlementEffects, WorkerError> {
        let wait_key = signal_wait_kv_key(correlation_key);

        // One waiter per correlation key: reject a registration while a
        // live waiter holds the key. The check reads current state; a
        // stale index entry (its job no longer scheduled) is overwritten.
        // The rejection terminates the run like any permanent step error.
        match self.queue.kv_get(&wait_key).await {
            Ok(Some(existing)) => {
                if let Ok(existing_id) = std::str::from_utf8(&existing)
                    && let Ok(Some(job)) = self.queue.get_job(existing_id).await
                    && job.status == JobStatus::Scheduled
                {
                    let message = format!(
                        "a waiter is already registered for correlation key `{correlation_key}`"
                    );
                    let effects = self.terminate_collecting_effects(
                        &RunOutcome {
                            run_id: run_id.to_string(),
                            status: TerminalStatus::Failed,
                            result: None,
                            error: Some(message.clone()),
                            headers: user_headers.clone(),
                            final_step: next_step.saturating_sub(1),
                        },
                        base_opts.priority,
                        None,
                    );
                    self.forget_run(run_id);
                    return Err(FailWith::new(PermanentFailure::new(message), effects).into());
                }
            }
            Ok(None) => {}
            Err(e) => return Err(e.to_string().into()),
        }

        let buf_key = signal_buf_kv_key(correlation_key);
        match self.queue.kv_get(&buf_key).await {
            Ok(Some(buffered)) => {
                let opts = StepEnqueueOpts {
                    reserved_headers: vec![(HEADER_SIGNAL_DELIVERED, "1".to_string())],
                    ..base_opts
                };
                let delivered_key = signal_delivered_kv_key(run_id, next_step);
                let buffered = buffered.to_vec();
                let mut effects = self
                    .advance_with_kv(run_id, next_step, payload, user_headers, opts, |_| {
                        HashMap::from([(delivered_key, buffered)])
                    })
                    .await;
                effects.kv_deletes.push(buf_key);
                Ok(effects)
            }
            Ok(None) => {
                let now = UNIX_EPOCH + Duration::from_millis(self.clock.now_ms());
                let opts = StepEnqueueOpts {
                    run_at: Some(now + timeout),
                    reserved_headers: vec![(HEADER_SIGNAL_WAIT, correlation_key.to_string())],
                    ..base_opts
                };
                let effects = self
                    .advance_with_kv(run_id, next_step, payload, user_headers, opts, |job_id| {
                        HashMap::from([(wait_key, job_id.as_bytes().to_vec())])
                    })
                    .await;
                Ok(effects)
            }
            Err(e) => Err(e.to_string().into()),
        }
    }

    /// Resolve the signal delivery for a claimed step job: the payload to
    /// expose on [`Step::signal`](crate::Step::signal) and the durable signal entries to delete
    /// with the step's settlement.
    pub(crate) async fn resolve_step_signal(
        &self,
        job: &JobRecord,
        run_id: &str,
        step_number: u32,
    ) -> Result<(Option<Vec<u8>>, Vec<Vec<u8>>)> {
        if job.headers.contains_key(HEADER_SIGNAL_DELIVERED) {
            let delivered_key = signal_delivered_kv_key(run_id, step_number);
            let payload = self.queue.kv_get(&delivered_key).await?.map(|b| b.to_vec());
            if payload.is_none() {
                warn!(run_id = %run_id, step_number, "delivered signal record is missing");
            }
            return Ok((payload, vec![delivered_key]));
        }

        let Some(correlation_key) = job.headers.get(HEADER_SIGNAL_WAIT) else {
            return Ok((None, Vec::new()));
        };
        let wait_key = signal_wait_kv_key(correlation_key);
        let _ = self
            .queue
            .kv_compare_delete(&wait_key, job.id.as_bytes())
            .await;

        if job.woken_at.is_some() {
            // A signal promoted this job early. The payload is on the job
            // record, so redelivery of this step observes it again.
            return Ok((job.wake_payload.clone(), Vec::new()));
        }

        // The timeout promoted this job. A prior attempt of this step may
        // already have consumed the buffer into the delivered record.
        let delivered_key = signal_delivered_kv_key(run_id, step_number);
        if let Some(prior) = self.queue.kv_get(&delivered_key).await? {
            return Ok((Some(prior.to_vec()), vec![delivered_key]));
        }
        // A signal buffered after this waiter's settlement read of the
        // buffer, without winning the wake, is consumed here so it is
        // delivered rather than dropped. The delivery is recorded before
        // the buffer is consumed, so a retry of this step observes the
        // same signal.
        let buf_key = signal_buf_kv_key(correlation_key);
        if let Some(buffered) = self.queue.kv_get(&buf_key).await? {
            let buffered = buffered.to_vec();
            self.queue.kv_put(&delivered_key, &buffered).await?;
            let _ = self.queue.kv_compare_delete(&buf_key, &buffered).await;
            return Ok((Some(buffered), vec![delivered_key]));
        }
        Ok((None, Vec::new()))
    }
}
