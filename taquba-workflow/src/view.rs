//! The read-only queries of a workflow store: the status and the outcome of a
//! run and the status of a run group, from the queue's KV namespace and the
//! memo store the runtime writes to.

use std::collections::HashMap;

use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use taquba::{InboxOutcome, JobRecord, JobStatus, KvOrder, QueueView};

use crate::command::CommandState;
use crate::durable::{
    self, DurableRun, DurableRunRecord, DurableRunResult, DurableStepRef, DurableTermination,
};
use crate::error::{Error, Result};
use crate::group::{self, GroupStatus};
use crate::keys::{
    RUN_KV_ENTRY_PREFIX, RUN_KV_PREFIX, RunId, STEP_KV_PREFIX, run_kv_entries_range,
    run_kv_entry_key, run_kv_key, step_kv_key, submitter_headers,
};
use crate::memo::{MemoStore, RUN_RESULT_MEMO_KEY};
use crate::runtime::{RunEnd, RunState, RunStatus, RunTermination};
use crate::terminal::{RunOutcome, TerminalStatus};

/// The runs that [`WorkflowView::runs`] lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunFilter {
    /// The active and the terminated runs.
    All,
    /// The runs submitted and not terminated.
    Active,
    /// The terminated runs, until the memo sweep removes their terminal record.
    Terminated,
}

/// A run in the listing of [`WorkflowView::runs`], or read by
/// [`WorkflowView::run`].
#[derive(Debug, Clone)]
pub struct ListedRun {
    /// The status of the run, as [`WorkflowView::status`] reports it.
    pub status: RunStatus,
    /// The runtime clock's time at the submission, as a Unix timestamp in
    /// milliseconds.
    pub submitted_at_ms: u64,
    /// The submitter's headers, without the reserved `workflow.` headers.
    pub headers: HashMap<String, String>,
}

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
    /// lifecycle position of its step except a dead-lettered step, until the
    /// run terminates.
    ///
    /// When a second read returns the current-step pointer with its job still
    /// absent, the call fails with [`Error::InconsistentRunState`]. The runtime
    /// does not write a pointer without its job.
    pub async fn status(&self, run_id: &RunId) -> Result<Option<RunStatus>> {
        Ok(self.run(run_id).await?.map(|run| run.status))
    }

    /// The run `run_id` as [`WorkflowView::runs`] lists it: its status, as
    /// [`WorkflowView::status`] reports it, with its submission time and
    /// headers. The reads are those of `status`.
    pub async fn run(&self, run_id: &RunId) -> Result<Option<ListedRun>> {
        match self.run_entry(run_id).await? {
            Some(run) => self.listed(run_id.clone(), run).await,
            None => Ok(None),
        }
    }

    /// The runs that `filter` selects, in the order `order` of their run ids,
    /// with `page_size` records in a page. A run id that the runtime generates
    /// comes from [`Queue::next_job_id`](taquba::Queue::next_job_id), so such
    /// runs list in the order of their submission. A terminated run lists until
    /// the memo sweep removes its terminal record. The listing is not a
    /// snapshot. A run lists once, in the state of its read. A run that is
    /// submitted or removed during the listing can be absent, and with
    /// [`RunFilter::Active`] so can a run that terminates before its read.
    pub fn runs(
        &self,
        filter: RunFilter,
        order: KvOrder,
        page_size: usize,
    ) -> impl Stream<Item = Result<ListedRun>> + Send + '_ {
        // The current-step pointers are the active runs.
        let prefix = match filter {
            RunFilter::Active => STEP_KV_PREFIX,
            RunFilter::All | RunFilter::Terminated => RUN_KV_PREFIX,
        };
        self.queue
            .kv_entries(prefix, .., order, page_size)
            .map_err(Error::from)
            .try_filter_map(move |(key, value)| async move {
                let run_id = RunId::new(String::from_utf8_lossy(&key[prefix.len()..]))?;
                match filter {
                    RunFilter::All => self.listed(run_id, durable::decode(&value)?).await,
                    RunFilter::Active => match self.run_record(&run_id).await? {
                        Some(record) => self.listed_active(&run_id, &record).await,
                        None => Ok(None),
                    },
                    RunFilter::Terminated => Ok(match durable::decode(&value)? {
                        DurableRun::Terminated(termination) => {
                            Some(listed_terminated(run_id, termination))
                        }
                        DurableRun::Active(_) => None,
                    }),
                }
            })
    }

    /// The listed run of the record `run` of `run_id`. The termination deletes
    /// the pointer of an active run, so a run whose pointer is absent has
    /// terminated after the read of `run`, and lists from its terminal record.
    async fn listed(&self, run_id: RunId, run: DurableRun) -> Result<Option<ListedRun>> {
        let record = match run {
            DurableRun::Terminated(termination) => {
                return Ok(Some(listed_terminated(run_id, termination)));
            }
            DurableRun::Active(record) => record,
        };
        if let Some(run) = self.listed_active(&run_id, &record).await? {
            return Ok(Some(run));
        }
        Ok(self
            .terminal_record(&run_id)
            .await?
            .map(|termination| listed_terminated(run_id, termination)))
    }

    /// The listed run of the active run `run_id` with the run record `record`,
    /// or `None` when its current-step pointer is absent.
    async fn listed_active(
        &self,
        run_id: &RunId,
        record: &DurableRunRecord,
    ) -> Result<Option<ListedRun>> {
        let Some((current, job)) = self.current_job(run_id).await? else {
            return Ok(None);
        };
        Ok(Some(ListedRun {
            status: RunStatus {
                run_id: run_id.clone(),
                state: active_state(record, &job),
                step: current.into(),
            },
            submitted_at_ms: record.submitted_at_ms,
            headers: submitter_headers(&job.headers),
        }))
    }

    /// The end of a terminated run, as
    /// [`WorkflowRuntime::wait`](crate::WorkflowRuntime::wait) reports it: the
    /// termination from its terminal record, with the committed outcome from
    /// its run result record. The worker that terminates the run writes the
    /// record before the terminating settlement, so the outcome is `None` for a
    /// run terminated without a worker (a cancellation of a pending step, a
    /// dead-letter outside the worker). The end is `None` for a run that is
    /// unknown, still active or swept.
    pub async fn run_end(&self, run_id: &RunId) -> Result<Option<RunEnd>> {
        let Some(termination) = self.terminal_record(run_id).await? else {
            return Ok(None);
        };
        let termination = RunTermination::from(termination);
        let outcome = self.run_result_of(run_id, &termination).await?;
        Ok(Some(RunEnd {
            termination,
            outcome,
        }))
    }

    /// The state of the [`Command`](crate::Command) of the inbox entry
    /// `entry_id` on `control_queue`, or `None` before a pass applies the entry
    /// and after its record expires. An applied entry of another kind than
    /// `enqueue` reads as `None` as well. A command job without a record counts
    /// as applied, because the queue deletes a done job. [`Self::status`] reads
    /// the state of the run that the command names.
    pub async fn command(
        &self,
        control_queue: &str,
        entry_id: &str,
    ) -> Result<Option<CommandState>> {
        let Some(record) = self.queue.inbox_record(control_queue, entry_id).await? else {
            return Ok(None);
        };
        let job_id = match record.outcome {
            InboxOutcome::Rejected(reason) => return Ok(Some(CommandState::Rejected(reason))),
            InboxOutcome::Enqueue(enqueued) => enqueued.into_id(),
            InboxOutcome::Cancel(_) | InboxOutcome::Signal(_) => return Ok(None),
        };
        Ok(Some(match self.queue.get_job(&job_id).await? {
            None => CommandState::Applied,
            Some(job) => match job.status {
                JobStatus::Done => CommandState::Applied,
                JobStatus::Dead => CommandState::Failed(job.last_error.unwrap_or_default()),
                JobStatus::Pending | JobStatus::Scheduled | JobStatus::Claimed => {
                    CommandState::Queued {
                        last_error: job.last_error,
                    }
                }
            },
        }))
    }

    /// The value of the run KV entry `key` of `run_id`, which a step staged
    /// through [`EffectsHandle::put_run_kv`](crate::EffectsHandle::put_run_kv),
    /// `None` when no value exists.
    pub async fn run_kv(&self, run_id: &RunId, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.queue.kv_get(&run_kv_entry_key(run_id, key)).await?)
    }

    /// The run KV entries of `run_id` in the order `order` of their keys, with
    /// `page_size` entries in a page. A key is the one that the step staged.
    pub fn run_kv_entries(
        &self,
        run_id: &RunId,
        order: KvOrder,
        page_size: usize,
    ) -> impl Stream<Item = Result<(Vec<u8>, Bytes)>> + Send + '_ {
        let range = run_kv_entries_range(run_id);
        let len = range.start.len();
        self.queue
            .kv_entries(RUN_KV_ENTRY_PREFIX, range, order, page_size)
            .map_ok(move |(mut key, value)| (key.split_off(len), value))
            .map_err(Error::from)
    }

    /// The status of the run group `group_id`: the members of its manifest,
    /// counted by the state of their member records. It is `None` when the
    /// manifest is absent, for a group never submitted or removed.
    pub async fn group_status(&self, group_id: &RunId) -> Result<Option<GroupStatus>> {
        let Some(manifest) = group::read_manifest(&self.memos, group_id).await? else {
            return Ok(None);
        };
        let mut status = GroupStatus {
            group_id: group_id.clone(),
            total: manifest.members.len(),
            pending: 0,
            succeeded: 0,
            failed: 0,
            cancelled: 0,
        };
        for member in group::read_members(&self.queue, group_id).await? {
            match member.status() {
                None => status.pending += 1,
                Some(TerminalStatus::Succeeded) => status.succeeded += 1,
                Some(TerminalStatus::Failed) => status.failed += 1,
                Some(TerminalStatus::Cancelled) => status.cancelled += 1,
            }
        }
        Ok(Some(status))
    }

    /// The record of `run_id`, `None` for a run that is unknown or swept.
    pub(crate) async fn run_entry(&self, run_id: &RunId) -> Result<Option<DurableRun>> {
        durable::kv_record(&self.queue, &run_kv_key(run_id)).await
    }

    /// The run record of `run_id`, when the run is active.
    pub(crate) async fn run_record(&self, run_id: &RunId) -> Result<Option<DurableRunRecord>> {
        Ok(match self.run_entry(run_id).await? {
            Some(DurableRun::Active(record)) => Some(record),
            Some(DurableRun::Terminated(_)) | None => None,
        })
    }

    /// The current-step pointer of `run_id`, or `None` when the run is not
    /// active.
    pub(crate) async fn current_step_if_active(
        &self,
        run_id: &RunId,
    ) -> Result<Option<DurableStepRef>> {
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
    ) -> Result<Option<(DurableStepRef, JobRecord)>> {
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

    /// The terminal record of `run_id`, when the run is terminated.
    pub(crate) async fn terminal_record(
        &self,
        run_id: &RunId,
    ) -> Result<Option<DurableTermination>> {
        Ok(match self.run_entry(run_id).await? {
            Some(DurableRun::Terminated(termination)) => Some(termination),
            Some(DurableRun::Active(_)) | None => None,
        })
    }

    /// The outcome in the run result record of `run_id` when the record belongs
    /// to `termination`. A record written before a settlement that did not
    /// commit outlives the termination that followed, so a record of another
    /// termination is not reported. A record that fails to decode is treated as
    /// absent.
    pub(crate) async fn run_result_of(
        &self,
        run_id: &RunId,
        termination: &RunTermination,
    ) -> Result<Option<RunOutcome>> {
        let Some(bytes) = self
            .memos
            .new_run_memo(run_id)
            .get(RUN_RESULT_MEMO_KEY)
            .await?
        else {
            return Ok(None);
        };
        Ok(
            durable::decode_or_absent::<DurableRunResult>(&bytes, "run result record", run_id)
                .and_then(|record| {
                    (RunTermination::from(record.termination) == *termination)
                        .then(|| record.outcome.into())
                }),
        )
    }
}

/// The state of an active run with the run record `record` and the current
/// step's job `job`.
fn active_state(record: &DurableRunRecord, job: &JobRecord) -> RunState {
    match (job.status, &record.cancel_request) {
        (JobStatus::Dead, _) => RunState::DeadLettered,
        (_, Some(request)) => RunState::Cancelling {
            reason: request.reason.clone(),
            requested_at_ms: request.requested_at_ms,
        },
        (JobStatus::Claimed, None) => RunState::Running,
        (JobStatus::Scheduled, None) => scheduled_state(job),
        (JobStatus::Pending | JobStatus::Done, None) => RunState::Pending,
    }
}

/// The listed run of the terminated run `run_id` with the terminal record
/// `termination`.
fn listed_terminated(run_id: RunId, termination: DurableTermination) -> ListedRun {
    ListedRun {
        submitted_at_ms: termination.submitted_at_ms,
        headers: termination.headers.clone(),
        status: RunStatus {
            run_id,
            step: termination.final_step.clone().into(),
            state: RunState::Terminated(termination.into()),
        },
    }
}

/// The state of a run whose current step's job is scheduled: a wait for a
/// signal when the job waits for one, a delay otherwise.
fn scheduled_state(job: &JobRecord) -> RunState {
    let run_at_ms = job.run_at.unwrap_or_default();
    if job.signal_wait {
        RunState::Waiting {
            timeout_at_ms: run_at_ms,
        }
    } else {
        RunState::Scheduled { run_at_ms }
    }
}
