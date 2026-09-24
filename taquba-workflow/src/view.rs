//! The read-only queries of a workflow store: the status and the outcome of a
//! run, from the queue's KV namespace and the memo store the runtime writes to.

use taquba::{JobRecord, JobStatus, QueueView};

use crate::durable::{
    self, DurableCurrentStep, DurableRunRecord, DurableRunResult, DurableTermination,
};
use crate::error::{Error, Result};
use crate::keys::{RunId, outcome_kv_key, run_kv_key, step_kv_key};
use crate::memo::{MemoStore, RUN_RESULT_MEMO_KEY};
use crate::runtime::{RunResult, RunState, RunStatus, RunTermination};
use crate::terminal::RunOutcome;

/// The read-only queries of a workflow store, over a [`QueueView`] and the
/// [`MemoStore`] the runtime writes to.
/// [`WorkflowRuntime::view`](crate::WorkflowRuntime::view) returns the
/// runtime's own view, and a process without a runtime builds a view with
/// [`WorkflowView::new`] from a [`taquba::QueueReader::view`] and a memo store
/// at the runtime's prefix.
#[derive(Clone)]
pub struct WorkflowView {
    queue: QueueView,
    memos: MemoStore,
}

impl WorkflowView {
    /// A view over `queue` and `memos`. Through a reader's view the queries are
    /// of the flushed state the reader last observed.
    pub fn new(queue: QueueView, memos: MemoStore) -> Self {
        Self { queue, memos }
    }

    /// The status of `run_id` from its durable state. An active run is read
    /// from the run record, the current-step pointer and the step's queue job,
    /// and a terminated run from its terminal record. A terminated run reports
    /// [`RunState::Terminated`] until the memo sweep removes its terminal
    /// record, and a run that is unknown or swept is `None`. A run with a
    /// pending cancellation request reports [`RunState::Cancelling`] at every
    /// lifecycle position of its step, until the run terminates.
    ///
    /// When a second read returns the current-step pointer with its job still
    /// absent, the call fails with [`Error::InconsistentRunState`]. The runtime
    /// does not write a pointer without its job.
    pub async fn status(&self, run_id: &RunId) -> Result<Option<RunStatus>> {
        let Some(record) = self.run_record(run_id).await? else {
            return self.terminated_status(run_id).await;
        };
        // The pointer is deleted with the record, so its absence here means
        // that the run terminated between the two reads.
        let Some((current, job)) = self.current_job(run_id).await? else {
            return self.terminated_status(run_id).await;
        };
        let state = if record.cancel_requested {
            RunState::Cancelling
        } else if job.status == JobStatus::Claimed {
            RunState::Running
        } else {
            RunState::Pending
        };
        Ok(Some(RunStatus {
            run_id: run_id.clone(),
            state,
            current_step: current.step_number,
        }))
    }

    /// The committed outcome of a terminated run, read from its run result
    /// record: the result the runner returned or the error that ended the run,
    /// with the submitter's headers and the final step. The record is written
    /// by the worker that terminates the run before the terminating settlement
    /// and is removed with the run's memo entries. It is `None` for a run that
    /// is unknown, still active or was terminated without a worker (a
    /// cancellation of a pending step, a dead-letter outside the worker), whose
    /// status [`Self::status`] reports. A record belongs to the termination its
    /// terminal record describes: a re-submission of a terminated run id leaves
    /// the earlier run's record in place until its own termination overwrites
    /// it, and such a record is not reported.
    pub async fn outcome(&self, run_id: &RunId) -> Result<Option<RunOutcome>> {
        if self.current_step_if_active(run_id).await?.is_some() {
            return Ok(None);
        }
        Ok(self
            .recorded_result(run_id)
            .await?
            .map(|result| result.outcome))
    }

    /// The durable record of `run_id`, when the run is active.
    pub(crate) async fn run_record(&self, run_id: &RunId) -> Result<Option<DurableRunRecord>> {
        durable::kv_record(&self.queue, &run_kv_key(run_id)).await
    }

    /// The current-step pointer of `run_id`, or `None` when the run is not
    /// active.
    pub(crate) async fn current_step_if_active(
        &self,
        run_id: &RunId,
    ) -> Result<Option<DurableCurrentStep>> {
        durable::kv_record(&self.queue, &step_kv_key(run_id)).await
    }

    /// The current step of `run_id` with its queue job in the stored form of
    /// [`QueueView::job_record`], or `None` when the run is not active. The
    /// pointer and the job change in one transaction, so a pointer that moved
    /// between the two reads is followed. When a second read returns the
    /// pointer unchanged and its job is still absent, the call fails with
    /// [`Error::InconsistentRunState`]. The runtime does not write a pointer
    /// without its job.
    pub(crate) async fn current_job(
        &self,
        run_id: &RunId,
    ) -> Result<Option<(DurableCurrentStep, JobRecord)>> {
        let mut absent: Option<String> = None;
        loop {
            let Some(current) = self.current_step_if_active(run_id).await? else {
                return Ok(None);
            };
            if let Some(job) = self.queue.job_record(&current.job_id).await? {
                return Ok(Some((current, job)));
            }
            if absent.as_deref() == Some(current.job_id.as_str()) {
                return Err(Error::InconsistentRunState(run_id.clone()));
            }
            absent = Some(current.job_id);
        }
    }

    /// The terminal record of `run_id`, or `None` when no record exists.
    pub(crate) async fn terminal_record(
        &self,
        run_id: &RunId,
    ) -> Result<Option<DurableTermination>> {
        durable::kv_record(&self.queue, &outcome_kv_key(run_id)).await
    }

    /// The status of a terminated run from its terminal record, or `None` when
    /// no record exists.
    async fn terminated_status(&self, run_id: &RunId) -> Result<Option<RunStatus>> {
        Ok(self.terminal_record(run_id).await?.map(|record| RunStatus {
            run_id: run_id.clone(),
            current_step: record.final_step,
            state: RunState::Terminated(record.into()),
        }))
    }

    /// The run result record of the termination `run_id`'s terminal record
    /// describes, or `None` when no terminal record remains or the worker that
    /// terminated the run wrote no record.
    pub(crate) async fn recorded_result(&self, run_id: &RunId) -> Result<Option<RunResult>> {
        match self.terminal_record(run_id).await? {
            Some(termination) => self.run_result_of(run_id, &termination.into()).await,
            None => Ok(None),
        }
    }

    /// The run result record of `run_id` when it belongs to `termination`. A
    /// record outlives a re-submission of the run id until the next termination
    /// overwrites it, and a record written before a settlement that did not
    /// commit outlives the termination that followed, so a record of another
    /// termination is not reported.
    pub(crate) async fn run_result_of(
        &self,
        run_id: &RunId,
        termination: &RunTermination,
    ) -> Result<Option<RunResult>> {
        Ok(self
            .run_result(run_id)
            .await?
            .filter(|result| result.termination == *termination))
    }

    /// The run result record of `run_id`, whichever termination it belongs to.
    /// A record that fails to decode is treated as absent.
    async fn run_result(&self, run_id: &RunId) -> Result<Option<RunResult>> {
        let Some(bytes) = self
            .memos
            .new_run_memo(run_id)
            .get(RUN_RESULT_MEMO_KEY)
            .await?
        else {
            return Ok(None);
        };
        Ok(
            durable::decode_or_absent::<DurableRunResult>(&bytes, "run result record", run_id).map(
                |record| RunResult {
                    termination: record.termination.into(),
                    outcome: record.outcome.into(),
                },
            ),
        )
    }
}
