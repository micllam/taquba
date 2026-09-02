//! The in-process registry of active runs. Every method takes and
//! releases the lock internally, no method awaits and the guard never
//! leaves this module, so the registry lock cannot be held across an
//! await point.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::runtime::{RunState, RunStatus};

/// Per-active-run state retained by the runtime. Combines the publicly
/// observable [`RunStatus`] with the in-process state needed to resolve
/// [`WorkflowRuntime::cancel`](crate::WorkflowRuntime::cancel) races:
/// the Taquba job currently representing the run (so `cancel` can
/// target it), the submitter's headers (so the notification includes
/// the right metadata even when `cancel` terminates a pending step
/// directly) and a flag for any pending cancellation request.
struct RegistryEntry {
    status: RunStatus,
    current_job_id: String,
    user_headers: HashMap<String, String>,
    cancel_requested: bool,
    /// SHA-256 of the original `spec.input`. `Some` for entries created
    /// by [`WorkflowRuntime::submit`](crate::WorkflowRuntime::submit);
    /// `None` for entries created by a worker resuming a step after
    /// restart, which does not have access to the original input. The
    /// duplicate-submit check falls through to the durable record
    /// (which always includes the hash) when this is `None`.
    input_hash: Option<[u8; 32]>,
}

/// The map of active runs, keyed by run id. Terminal runs are removed
/// by [`Self::forget`]; the map holds only runs this process considers
/// live.
#[derive(Default)]
pub(crate) struct RunRegistry {
    runs: Mutex<HashMap<String, RegistryEntry>>,
}

impl RunRegistry {
    /// The stored input hash of `run_id`, when an entry created by
    /// `submit` exists. `None` for an unknown run and for a
    /// worker-resumed entry, which stores no hash.
    pub(crate) fn known_input_hash(&self, run_id: &str) -> Option<[u8; 32]> {
        self.runs
            .lock()
            .unwrap()
            .get(run_id)
            .and_then(|entry| entry.input_hash)
    }

    /// The id of the queue job backing the run's current step, when the
    /// run is tracked.
    pub(crate) fn current_job_id(&self, run_id: &str) -> Option<String> {
        self.runs
            .lock()
            .unwrap()
            .get(run_id)
            .map(|entry| entry.current_job_id.clone())
    }

    /// Record a newly submitted run: step 0, [`RunState::Pending`],
    /// backed by `job_id`.
    pub(crate) fn insert_submitted(
        &self,
        run_id: &str,
        job_id: &str,
        user_headers: HashMap<String, String>,
        input_hash: [u8; 32],
    ) {
        self.runs.lock().unwrap().insert(
            run_id.to_string(),
            RegistryEntry {
                status: RunStatus {
                    run_id: run_id.to_string(),
                    state: RunState::Pending,
                    current_step: 0,
                },
                current_job_id: job_id.to_string(),
                user_headers,
                cancel_requested: false,
                input_hash: Some(input_hash),
            },
        );
    }

    /// The status of `run_id`, with a pending cancellation reported as
    /// [`RunState::Cancelling`] regardless of the underlying step
    /// lifecycle position.
    pub(crate) fn status(&self, run_id: &str) -> Option<RunStatus> {
        self.runs.lock().unwrap().get(run_id).map(|entry| {
            let mut status = entry.status.clone();
            if entry.cancel_requested {
                status.state = RunState::Cancelling;
            }
            status
        })
    }

    /// Whether a cancellation has been requested for `run_id`.
    pub(crate) fn cancel_requested(&self, run_id: &str) -> bool {
        self.runs
            .lock()
            .unwrap()
            .get(run_id)
            .is_some_and(|entry| entry.cancel_requested)
    }

    /// Set the cancellation flag on `run_id` and return the job id,
    /// submitter headers and current step the cancellation targets.
    /// `None` when the run is not active in this registry.
    pub(crate) fn request_cancel(
        &self,
        run_id: &str,
    ) -> Option<(String, HashMap<String, String>, u32)> {
        let mut runs = self.runs.lock().unwrap();
        let entry = runs.get_mut(run_id)?;
        entry.cancel_requested = true;
        Some((
            entry.current_job_id.clone(),
            entry.user_headers.clone(),
            entry.status.current_step,
        ))
    }

    /// Transition the entry for `run_id` into [`RunState::Running`] for
    /// `step_number`, recording the Taquba job id backing the step so a
    /// concurrent cancellation can target it. Creates a fresh entry,
    /// with no input hash and no cancellation flag, when the run is
    /// unknown to this registry (a worker resuming after a restart
    /// first learns of the run by claiming its step).
    pub(crate) fn mark_running(
        &self,
        run_id: &str,
        step_number: u32,
        job_id: &str,
        user_headers: &HashMap<String, String>,
    ) {
        let mut runs = self.runs.lock().unwrap();
        match runs.get_mut(run_id) {
            Some(entry) => {
                entry.status.state = RunState::Running;
                entry.status.current_step = step_number;
                entry.current_job_id = job_id.to_string();
            }
            None => {
                runs.insert(
                    run_id.to_string(),
                    RegistryEntry {
                        status: RunStatus {
                            run_id: run_id.to_string(),
                            state: RunState::Running,
                            current_step: step_number,
                        },
                        current_job_id: job_id.to_string(),
                        user_headers: user_headers.clone(),
                        cancel_requested: false,
                        input_hash: None,
                    },
                );
            }
        }
    }

    /// Transition the entry for `run_id` to [`RunState::Pending`] at
    /// `next_step`, backed by `next_job_id`. The cancellation flag is
    /// deliberately preserved: a cancellation issued during the
    /// just-settled step must survive the advance.
    pub(crate) fn mark_pending(&self, run_id: &str, next_step: u32, next_job_id: String) {
        if let Some(entry) = self.runs.lock().unwrap().get_mut(run_id) {
            entry.status.state = RunState::Pending;
            entry.status.current_step = next_step;
            entry.current_job_id = next_job_id;
        }
    }

    /// Remove the entry for `run_id`.
    pub(crate) fn forget(&self, run_id: &str) {
        self.runs.lock().unwrap().remove(run_id);
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.runs.lock().unwrap().is_empty()
    }
}
