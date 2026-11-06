//! The read-only queries of a workflow store: the status and the outcome of a
//! run and the status of a run group, from the queue's KV namespace and the
//! memo store the runtime writes to.

use std::collections::HashMap;

use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use taquba::{InboxOutcome, JobRecord, JobStatus, KvOrder, QueueView};

use crate::command::CommandState;
use crate::durable::{
    self, DurableResult, DurableRun, DurableRunRecord, DurableStepRef, DurableTermination,
};
use crate::error::{Error, Result};
use crate::group::{self, GroupStatus};
use crate::keys::{
    RUN_KV_ENTRY_PREFIX, RUN_KV_PREFIX, RunId, STEP_KV_PREFIX, group_of_count_name,
    run_kv_entries_range, run_kv_entry_key, run_kv_key, step_kv_key, submitter_headers,
};
use crate::memo::MemoStore;
use crate::terminal::{RunTermination, TerminalStatus, TerminatedRun};

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

    /// The outcome of a terminated run, as
    /// [`WorkflowRuntime::wait`](crate::WorkflowRuntime::wait) reports it: the
    /// termination from its terminal record, with the result of a succeeded
    /// run. The outcome is `None` for a run that is unknown, still active or
    /// swept. Returns [`Error::InconsistentRunState`] for a succeeded run whose
    /// terminal record does not have the reference to its result.
    pub async fn terminated_run(&self, run_id: &RunId) -> Result<Option<TerminatedRun>> {
        match self.terminal_record(run_id).await? {
            Some(termination) => self.outcome_of(run_id, termination).await,
            None => Ok(None),
        }
    }

    /// The state of the [`Command`](crate::Command) of the inbox entry
    /// `entry_id` on `control_queue`, read from the record of the entry and
    /// from the command job. [`Self::status`] reads the state of the run that
    /// the command names.
    ///
    /// The state is `None` before a pass applies the entry, after its record
    /// expires and for an applied entry of another kind than `enqueue`. It is
    /// `None` as well when the command job is absent: a cancel through the
    /// inbox removed it, or a sweep of the control queue removed it after the
    /// record retention.
    ///
    /// An entry whose [`dedup_key`](taquba::EnqueueOptions::dedup_key) matched
    /// a command job reads the state of that job. An entry whose `id_override`
    /// is the id of an existing job reads as [`CommandState::Rejected`] with
    /// [`taquba::Rejection::DuplicateJobId`].
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
        let Some(job) = self.queue.get_job(&job_id).await? else {
            return Ok(None);
        };
        Ok(Some(match job.status {
            JobStatus::Done => CommandState::Applied,
            JobStatus::Dead => CommandState::Failed(job.last_error.unwrap_or_default()),
            JobStatus::Pending | JobStatus::Scheduled | JobStatus::Claimed => {
                CommandState::Queued {
                    last_error: job.last_error,
                }
            }
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

    /// The outcome of `run_id` with the terminal record `termination`, with the
    /// result that the record refers to. A result object that the memo sweep
    /// has removed before the terminal record reports the run as swept, `None`.
    pub(crate) async fn outcome_of(
        &self,
        run_id: &RunId,
        termination: DurableTermination,
    ) -> Result<Option<TerminatedRun>> {
        let result = match (termination.status.into(), termination.result) {
            (TerminalStatus::Succeeded, Some(DurableResult::Empty)) => Some(Vec::new()),
            (TerminalStatus::Succeeded, Some(DurableResult::Object { attempt })) => {
                let job_id = &termination.final_step.job_id;
                match self.memos.get_result(run_id, job_id, attempt).await? {
                    Some(result) => Some(result),
                    None => return Ok(None),
                }
            }
            (TerminalStatus::Succeeded, None) => {
                return Err(Error::InconsistentRunState(run_id.clone()));
            }
            (TerminalStatus::Failed | TerminalStatus::Cancelled, _) => None,
        };
        Ok(Some(TerminatedRun {
            run_id: run_id.clone(),
            termination: RunTermination::from(&termination),
            result,
            headers: termination.headers,
        }))
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
            state: RunState::Terminated(RunTermination::from(&termination)),
        },
    }
}

/// The state of a run whose current step's job is scheduled: a wait for a
/// signal or for a group when the job waits for one, a delay otherwise.
fn scheduled_state(job: &JobRecord) -> RunState {
    let run_at_ms = job.run_at.unwrap_or_default();
    let group_id = job.count_wait.as_deref().and_then(group_of_count_name);
    match (job.signal_wait, group_id) {
        (true, _) => RunState::Waiting {
            timeout_at_ms: run_at_ms,
        },
        (false, Some(group_id)) => RunState::WaitingForGroup {
            group_id,
            timeout_at_ms: run_at_ms,
        },
        (false, None) => RunState::Scheduled { run_at_ms },
    }
}

/// A step of a run, with the queue job of the step.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StepRef {
    /// The zero-based step number.
    pub number: u32,
    /// The id of the step's queue job, which [`taquba::QueueView::job_record`]
    /// reads while the queue retains the job.
    pub job_id: String,
}

/// Status snapshot of a run, read from its durable state by
/// [`WorkflowView::status`].
#[derive(Debug, Clone)]
pub struct RunStatus {
    /// The run's identifier.
    pub run_id: RunId,
    /// Lifecycle state of the run's current step, or its termination.
    pub state: RunState,
    /// The run's current step, or the final step of a terminated run.
    pub step: StepRef,
}

/// Lifecycle state tracked in [`RunStatus::state`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunState {
    /// The current step's job is ready for a worker to claim.
    Pending,
    /// The current step's job becomes claimable at `run_at_ms`, a Unix
    /// timestamp in milliseconds: a step after a [`Trigger::After`] delay, or a
    /// retry after a transient error.
    ///
    /// [`Trigger::After`]: crate::Trigger::After
    Scheduled {
        /// The time from which a worker can claim the job.
        run_at_ms: u64,
    },
    /// The current step is waiting for a signal ([`Trigger::OnSignal`]) to its
    /// job id, the `job_id` of [`RunStatus::step`], and becomes claimable at
    /// `timeout_at_ms` without one.
    ///
    /// [`Trigger::OnSignal`]: crate::Trigger::OnSignal
    Waiting {
        /// The end of the wait without a signal, as a Unix timestamp in
        /// milliseconds.
        timeout_at_ms: u64,
    },
    /// The current step is waiting for the last active member of the group
    /// `group_id` to terminate ([`Trigger::OnGroup`]), and becomes claimable at
    /// `timeout_at_ms` at the latest.
    ///
    /// [`Trigger::OnGroup`]: crate::Trigger::OnGroup
    WaitingForGroup {
        /// The group the step is waiting for.
        group_id: RunId,
        /// The end of the wait, as a Unix timestamp in milliseconds.
        timeout_at_ms: u64,
    },
    /// A step is currently being processed by a worker.
    Running,
    /// [`WorkflowRuntime::cancel`] was called for this run and the run has not
    /// yet terminated. Reported until the in-flight step returns and the
    /// runtime settles the run as [`crate::TerminalStatus::Cancelled`], which
    /// [`Self::Terminated`] reports.
    ///
    /// Only set by external cancellation. A pure runner-issued
    /// [`crate::StepOutcome::Cancel`] (without an external `cancel()` call)
    /// terminates as `Cancelled` without ever transitioning through
    /// `Cancelling`: a runner-issued cancel is observed when `run_step`
    /// returns, and the run terminates at that point.
    ///
    /// [`WorkflowRuntime::cancel`]: crate::WorkflowRuntime::cancel
    Cancelling {
        /// The reason of the request.
        reason: Option<String>,
        /// The runtime clock's time at the request, as a Unix timestamp in
        /// milliseconds.
        requested_at_ms: u64,
    },
    /// The queue dead-lettered the current step's job outside the worker: a
    /// lease that expired past the attempt limit, or a claim dead-lettered by
    /// crash recovery at open. The reconciliation of the worker terminates the
    /// run as [`crate::TerminalStatus::Failed`], and
    /// [`WorkflowRuntime::cancel`] returns
    /// [`CancelRunOutcome::NotActive`](crate::CancelRunOutcome::NotActive) for
    /// the run.
    ///
    /// [`WorkflowRuntime::cancel`]: crate::WorkflowRuntime::cancel
    DeadLettered,
    /// The run reached a terminal state. Reported from the run's terminal
    /// record, written with the terminating settlement and removed with the
    /// run's memo entries by the memo sweep under
    /// [`WorkflowRuntimeBuilder::memo_retention`].
    ///
    /// [`WorkflowRuntimeBuilder::memo_retention`]:
    ///     crate::WorkflowRuntimeBuilder::memo_retention
    Terminated(RunTermination),
}
