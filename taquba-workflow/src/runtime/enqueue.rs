use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use taquba::{EnqueueOptions, EnqueueRequest, EnqueueResult, SettlementEffects};
use tracing::{debug, instrument};

use super::{RunSpec, RuntimeCore, SubmitOutcome};
use crate::durable::{self, DurableCancelRequest, DurableRun, DurableRunRecord, DurableStepRef};
use crate::error::{Error, Result};
use crate::group::{Membership, pending_member};
use crate::keys::{
    DEDUP_PREFIX, HEADER_RUN_ID, HEADER_STEP, RunId, check_effects, check_headers, hash_input,
    next_step_job_id, run_kv_key, step_kv_key,
};
use crate::worker::ClaimedStep;

/// The encoded current-step pointer for `job_id` at `step_number`.
fn current_step_bytes(step_number: u32, job_id: &str) -> Vec<u8> {
    durable::encode(&DurableStepRef {
        step_number,
        job_id: job_id.to_string(),
    })
}

/// Per-step enqueue options the runtime forwards through to Taquba. The runtime
/// always owns `headers` (it injects [`HEADER_RUN_ID`] and [`HEADER_STEP`]) and
/// `dedup_key` (it derives one from `(run_id, step_number)`), so callers only
/// choose the three fields below.
#[derive(Debug, Default)]
pub(crate) struct StepEnqueueOpts {
    /// Earliest claimable time for the step. `None` means immediate.
    pub(crate) run_at: Option<SystemTime>,
    /// Per-step priority override.
    pub(crate) priority: Option<u32>,
    /// Per-step `max_attempts` override.
    pub(crate) max_attempts: Option<u32>,
    /// Whether the step waits for a signal until `run_at`.
    pub(crate) signal_wait: bool,
    /// The name of the count the step waits for until `run_at`.
    pub(crate) count_wait: Option<String>,
    /// Additional runtime-owned reserved headers for the step job.
    pub(crate) reserved_headers: Vec<(&'static str, String)>,
}

impl RuntimeCore {
    /// [`WorkflowRuntime::submit`].
    ///
    /// [`WorkflowRuntime::submit`]: super::WorkflowRuntime::submit
    #[instrument(skip(self, spec), fields(run_id))]
    pub(crate) async fn submit(&self, spec: RunSpec) -> Result<SubmitOutcome> {
        let run_id = self.validate_spec(&spec)?;
        tracing::Span::current().record("run_id", run_id.as_str());
        self.enqueue_run(&run_id, spec, None, None).await
    }

    /// [`Self::submit`] of a member of a group: the run's step jobs hold the
    /// membership and its member record is written with the enqueue.
    pub(crate) async fn submit_member(
        &self,
        membership: &Membership,
        spec: RunSpec,
    ) -> Result<SubmitOutcome> {
        let run_id = self.validate_spec(&spec)?;
        self.enqueue_run(&run_id, spec, Some(membership), None)
            .await
    }

    /// [`Self::submit`] of a run whose run record contains a cancellation
    /// request for `reason`. A claim of its first step terminates the run
    /// without running the step.
    pub(crate) async fn submit_cancelled(
        &self,
        spec: RunSpec,
        reason: Option<String>,
    ) -> Result<SubmitOutcome> {
        let run_id = self.validate_spec(&spec)?;
        let request = DurableCancelRequest {
            reason,
            requested_at_ms: self.clock.now_ms(),
        };
        self.enqueue_run(&run_id, spec, None, Some(request)).await
    }

    /// Check `spec`'s headers and effects, and return the run id, generated
    /// when the spec does not include one.
    fn validate_spec(&self, spec: &RunSpec) -> Result<RunId> {
        check_headers(&spec.options.headers)?;
        check_effects(&spec.effects, &self.queue_name)?;
        Ok(spec
            .run_id
            .clone()
            .unwrap_or_else(|| RunId::generate(&self.queue)))
    }

    /// Enqueue step 0 of `run_id` unless the run is known. The run record is
    /// written with the step-0 enqueue and replaced by the terminal record at
    /// the termination, so it identifies a run whichever process submitted it,
    /// and the step's dedup key serialises two submissions that both find the
    /// record absent. A `membership` is set on the step job, and its pending
    /// member record and the increment of the count of its group commit with
    /// the enqueue. A `cancel_request` is written on the run record.
    async fn enqueue_run(
        &self,
        run_id: &RunId,
        spec: RunSpec,
        membership: Option<&Membership>,
        cancel_request: Option<DurableCancelRequest>,
    ) -> Result<SubmitOutcome> {
        let input_hash = hash_input(&spec.input);
        let duplicate = |job_id: String| SubmitOutcome {
            run_id: run_id.clone(),
            newly_submitted: false,
            job_id,
        };
        let check_input = |existing: [u8; 32]| {
            if existing == input_hash {
                Ok(())
            } else {
                Err(Error::InputMismatch(run_id.clone()))
            }
        };

        match self.view.run_entry(run_id).await? {
            Some(DurableRun::Active(existing)) => {
                check_input(existing.input_hash)?;
                let current = self.current_step(run_id).await?;
                return Ok(duplicate(current.job_id));
            }
            Some(DurableRun::Terminated(termination)) => {
                check_input(termination.input_hash)?;
                return Ok(duplicate(termination.final_step.job_id));
            }
            None => {}
        }

        let opts = StepEnqueueOpts {
            run_at: spec.options.run_at,
            priority: spec.options.priority,
            max_attempts: spec.options.max_attempts_per_step,
            signal_wait: false,
            count_wait: None,
            reserved_headers: membership
                .map(Membership::reserved_headers)
                .unwrap_or_default(),
        };
        let job_id = self.queue.next_job_id();
        let request = self.step_enqueue_request(
            job_id.clone(),
            run_id,
            0,
            spec.input,
            &spec.options.headers,
            opts,
        );

        let record_bytes = durable::encode(&DurableRun::Active(DurableRunRecord {
            run_id: run_id.clone(),
            submitted_at_ms: self.clock.now_ms(),
            input_hash,
            cancel_request,
        }));
        let mut effects = spec
            .effects
            .kv_put(run_kv_key(run_id), record_bytes)
            .kv_put(step_kv_key(run_id), current_step_bytes(0, &job_id));
        if let Some(membership) = membership {
            effects = effects
                .kv_put(
                    membership.kv_key(),
                    durable::encode(&pending_member(run_id)),
                )
                .count_up(membership.count_name());
        }
        let submitted = SubmitOutcome {
            run_id: run_id.clone(),
            newly_submitted: true,
            job_id,
        };

        match self
            .queue
            .enqueue_with_effects(&request.queue, request.payload, request.options, effects)
            .await?
            .0
        {
            EnqueueResult::New(_) => {}
            // A concurrent submission committed first, and its record contains
            // the input to check against. A dedup hit without a record is a
            // store this runtime did not write, reported as a duplicate.
            EnqueueResult::AlreadyEnqueued(existing) => {
                if let Some(record) = self.view.run_record(run_id).await? {
                    check_input(record.input_hash)?;
                }
                return Ok(duplicate(existing));
            }
        }

        debug!(run_id = %run_id, job_id = %submitted.job_id, "run submitted");
        Ok(submitted)
    }

    /// Build the enqueue request for one step of a run with the pre-assigned
    /// job id `job_id`, which the current-step pointer written with the enqueue
    /// names.
    fn step_enqueue_request(
        &self,
        job_id: String,
        run_id: &RunId,
        step_number: u32,
        payload: Vec<u8>,
        user_headers: &HashMap<String, String>,
        opts: StepEnqueueOpts,
    ) -> EnqueueRequest {
        let mut headers = user_headers.clone();
        headers.insert(HEADER_RUN_ID.to_string(), run_id.to_string());
        headers.insert(HEADER_STEP.to_string(), step_number.to_string());
        for (key, value) in &opts.reserved_headers {
            headers.insert((*key).to_string(), value.clone());
        }

        EnqueueRequest {
            queue: self.queue_name.clone(),
            payload,
            options: EnqueueOptions::default()
                .headers(headers)
                .run_at(opts.run_at)
                .priority(opts.priority)
                .max_attempts(opts.max_attempts)
                .dedup_key(Some(format!("{DEDUP_PREFIX}{run_id}:{step_number}")))
                .id_override(Some(job_id))
                .signal_wait(opts.signal_wait)
                .count_wait(opts.count_wait),
        }
    }

    /// The instant `delay` after the runtime's clock now, as a [`SystemTime`]
    /// for an enqueue's `run_at`.
    pub(crate) fn run_at_after(&self, delay: Duration) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.clock.now_ms()) + delay
    }

    /// Build the effects that advance the run of `claimed` to its next step,
    /// the job [`next_step_job_id`] of the claimed job: the next step's enqueue
    /// joins the current step's acknowledgement transaction, so the transition
    /// is atomic.
    pub(crate) fn advance(
        &self,
        claimed: &ClaimedStep<'_>,
        payload: Vec<u8>,
        opts: StepEnqueueOpts,
    ) -> SettlementEffects {
        let run_id = &claimed.run_id;
        let next_step = claimed.step_number + 1;
        let next_job_id = next_step_job_id(&claimed.job.id);
        let pointer = current_step_bytes(next_step, &next_job_id);
        let request = self.step_enqueue_request(
            next_job_id,
            run_id,
            next_step,
            payload,
            &claimed.headers,
            opts,
        );
        SettlementEffects::default()
            .enqueues(vec![request])
            .kv_writes(HashMap::from([(step_kv_key(run_id), pointer)]))
    }
}
