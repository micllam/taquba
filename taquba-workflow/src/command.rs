//! The commands of a runtime: a submit or a cancel that another process sends
//! as a job on the runtime's control queue, through the queue's inbox.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use taquba::{EnqueueEntry, JobRecord, JobStatus, LeaseHandle, PermanentFailure, WorkerError};

use crate::error::worker_error;
use crate::keys::RunId;
use crate::runtime::{CancelRunOutcome, RunOptions, RunSpec, RuntimeCore};

/// A request to a runtime from another process: the JSON payload of a job on
/// the runtime's control queue,
/// [`WorkflowRuntimeBuilder::control_queue`](crate::WorkflowRuntimeBuilder::control_queue).
/// A sender enqueues the [`EnqueueEntry`] of the command through a
/// [`taquba::InboxSender`] for that queue. The command job applies at least
/// once, and an error of the runtime retries or dead-letters it with the error
/// in [`JobRecord::last_error`].
///
/// The payload is a JSON object that contains the variant in its `command`
/// field and the fields of the variant. The input of a submission is base64:
///
/// ```json
/// {"command": "submit", "run_id": "run-1", "input": "aGVsbG8=",
///  "headers": {"tenant": "a"}, "run_at": 1700000000000, "priority": 5,
///  "max_attempts_per_step": 3}
/// {"command": "cancel", "run_id": "run-1", "reason": "operator"}
/// ```
///
/// Every field of a submission other than `run_id` and `input`, and the reason
/// of a cancellation, may be absent. A sender in another language writes the
/// inbox entry of the core kind `enqueue` for the control queue, with the
/// payload base64 in its `payload` field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum Command {
    /// A submission of a run.
    Submit(SubmitCommand),
    /// A cancellation of a run.
    Cancel(CancelCommand),
}

/// The state of a command, as [`WorkflowView::command`](crate::WorkflowView::command)
/// reads it from the record of its inbox entry and from its job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandState {
    /// The command job waits for the control worker, or for a retry after the
    /// error of its last attempt.
    Queued {
        /// The error of the last attempt.
        last_error: Option<String>,
    },
    /// The control worker applied the command.
    Applied,
    /// The inbox rejected the entry, or the command job was dead-lettered, for
    /// the reason.
    Failed(String),
}

impl From<Command> for EnqueueEntry {
    fn from(command: Command) -> Self {
        let payload = serde_json::to_vec(&command).expect("a command encodes as JSON");
        EnqueueEntry::new(payload)
    }
}

/// [`WorkflowRuntime::submit`](crate::WorkflowRuntime::submit) of a [`RunSpec`]
/// without effects. The run id identifies the run across the attempts of the
/// command, and a submission of an existing run id with another input fails the
/// command.
///
/// A sender delays the run with [`Self::run_at`]. A command job delayed with
/// the `run_at` of its [`EnqueueEntry`] finds a cancelled or finished run of
/// its id while the terminal record of the run is retained, and after the memo
/// sweep removes the record it starts a new run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitCommand {
    /// [`RunSpec::run_id`].
    pub run_id: RunId,
    /// [`RunSpec::input`], base64 in the payload.
    #[serde(with = "taquba::inbox_bytes")]
    pub input: Vec<u8>,
    /// [`RunOptions::headers`].
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
    /// [`RunOptions::run_at`], as milliseconds from the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_at: Option<u64>,
    /// [`RunOptions::priority`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u32>,
    /// [`RunOptions::max_attempts_per_step`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts_per_step: Option<u32>,
}

impl SubmitCommand {
    /// A submission of `run_id` with `input` and every option unset.
    pub fn new(run_id: RunId, input: impl Into<Vec<u8>>) -> Self {
        Self {
            run_id,
            input: input.into(),
            headers: HashMap::new(),
            run_at: None,
            priority: None,
            max_attempts_per_step: None,
        }
    }

    fn into_spec(self) -> RunSpec {
        RunSpec {
            run_id: Some(self.run_id),
            input: self.input,
            options: RunOptions {
                headers: self.headers,
                priority: self.priority,
                max_attempts_per_step: self.max_attempts_per_step,
                run_at: self.run_at.map(|ms| UNIX_EPOCH + Duration::from_millis(ms)),
            },
            effects: Default::default(),
        }
    }
}

/// [`WorkflowRuntime::cancel`](crate::WorkflowRuntime::cancel) of a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelCommand {
    /// The run.
    pub run_id: RunId,
    /// The reason of the request: the `reason` of [`WorkflowRuntime::cancel`].
    ///
    /// [`WorkflowRuntime::cancel`]: crate::WorkflowRuntime::cancel
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl CancelCommand {
    /// A cancellation of `run_id` without a reason.
    pub fn new(run_id: RunId) -> Self {
        Self {
            run_id,
            reason: None,
        }
    }
}

/// The worker of the control queue: it applies each command job. A payload that
/// does not decode as a [`Command`] dead-letters the job. A cancellation of a
/// run that is not active applies the first submission of the run that waits on
/// the control queue, with its first step held for [`HELD_FIRST_STEP`], and
/// then cancels the run. The submission job applies later as a duplicate. A
/// cancellation that fails at its every attempt leaves the run held.
pub(crate) struct CommandWorker {
    pub(crate) core: Arc<RuntimeCore>,
}

/// How long a submission applied for a cancellation delays its first step: a
/// hundred years, so that the step waits for the cancellation to remove it.
const HELD_FIRST_STEP: Duration = Duration::from_secs(100 * 365 * 24 * 3600);

impl taquba::Worker for CommandWorker {
    async fn process(&self, job: &JobRecord, _lease: &LeaseHandle) -> Result<(), WorkerError> {
        let command = decode(&job.payload)
            .map_err(|err| PermanentFailure::new(format!("malformed command: {err}")))?;
        match command {
            Command::Submit(submit) => {
                self.core
                    .submit(submit.into_spec())
                    .await
                    .map_err(worker_error)?;
            }
            Command::Cancel(cancel) => {
                let outcome = self
                    .core
                    .cancel_run(&cancel.run_id, cancel.reason.clone())
                    .await
                    .map_err(worker_error)?;
                if outcome != CancelRunOutcome::NotActive {
                    return Ok(());
                }
                let waiting = self
                    .waiting_submission(&job.queue, &cancel.run_id)
                    .await
                    .map_err(worker_error)?;
                let Some(submit) = waiting else {
                    return Ok(());
                };
                let mut spec = submit.into_spec();
                let held = Duration::from_millis(self.core.clock.now_ms()) + HELD_FIRST_STEP;
                spec.options.run_at = Some(UNIX_EPOCH + held);
                match self.core.submit(spec).await {
                    Ok(_) => {}
                    // The submission job fails on its own when it applies.
                    Err(err) if err.is_permanent() => return Ok(()),
                    Err(err) => return Err(worker_error(err)),
                }
                self.core
                    .cancel_run(&cancel.run_id, cancel.reason)
                    .await
                    .map_err(worker_error)?;
            }
        }
        Ok(())
    }
}

impl CommandWorker {
    /// The first submission of `run_id` that is pending or scheduled on
    /// `queue`.
    async fn waiting_submission(
        &self,
        queue: &str,
        run_id: &RunId,
    ) -> taquba::Result<Option<SubmitCommand>> {
        let view = self.core.queue.view();
        for status in [JobStatus::Pending, JobStatus::Scheduled] {
            let mut jobs = std::pin::pin!(view.jobs(queue, status, SCAN_PAGE));
            while let Some(job) = jobs.try_next().await? {
                if let Ok(Command::Submit(submit)) = decode(&job.payload)
                    && submit.run_id == *run_id
                {
                    return Ok(Some(submit));
                }
            }
        }
        Ok(None)
    }
}

/// The page size of a scan of the control queue.
const SCAN_PAGE: usize = 256;

/// The command of the JSON `payload`.
fn decode(payload: &[u8]) -> serde_json::Result<Command> {
    serde_json::from_slice(payload)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use taquba::{
        EnqueueResult, InboxOptions, InboxOutcome, InboxSender, JobStatus, MockClock, OpenOptions,
        Queue,
    };
    use tokio::sync::oneshot;

    use super::*;
    use crate::error::Error;
    use crate::runner::{Step, StepError, StepOutcome, StepRunner};
    use crate::runtime::{DEFAULT_QUEUE_NAME, WorkflowRuntime};
    use crate::terminal::{NoopTerminalHook, TerminalStatus};
    use crate::test_util::{advance, open_queue_at_with, rid};

    const CONTROL: &str = "workflow-steps.control";
    const INITIAL: u64 = 1_700_000_000_000;

    /// A runner whose run with the input `wait` waits at step 0 for a minute,
    /// and whose other runs succeed with their payload.
    struct Echo;

    impl StepRunner for Echo {
        async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
            match (step.step_number, step.payload.as_slice()) {
                (0, b"wait") => Ok(StepOutcome::continue_after(
                    step.payload.clone(),
                    Duration::from_secs(60),
                )),
                _ => Ok(StepOutcome::Succeed {
                    result: step.payload.clone(),
                }),
            }
        }
    }

    /// A queue under `opts` with a mock clock and an inbox for [`CONTROL`], and
    /// a runtime with that control queue.
    async fn open(
        opts: OpenOptions,
    ) -> (
        Arc<Queue>,
        InboxSender,
        WorkflowRuntime<Echo, NoopTerminalHook>,
        MockClock,
    ) {
        let inbox = InboxOptions::new([CONTROL]).touch_poll_interval(None);
        let (queue, store, clock) = open_queue_at_with(INITIAL, opts.inbox(inbox)).await;
        let runtime =
            WorkflowRuntime::builder(queue.clone(), store.clone(), Echo, NoopTerminalHook)
                .poll_interval(Duration::from_millis(20))
                .control_queue(CONTROL)
                .build()
                .unwrap();
        (queue, InboxSender::new(store, "test-inbox"), runtime, clock)
    }

    /// Run `runtime` until the returned sender fires.
    fn spawn(runtime: &WorkflowRuntime<Echo, NoopTerminalHook>) -> oneshot::Sender<()> {
        let (tx, rx) = oneshot::channel::<()>();
        let running = runtime.clone();
        tokio::spawn(async move {
            let _ = running
                .run(async move {
                    let _ = rx.await;
                })
                .await;
        });
        tx
    }

    /// Send `entry` to the control queue, apply the inbox and return the job of
    /// the command once the worker settled it: `None` once acked, because the
    /// queue deletes a done job, and the record of a dead job.
    async fn send(queue: &Queue, sender: &InboxSender, entry: EnqueueEntry) -> Option<JobRecord> {
        let id = sender.send(CONTROL, entry).await.unwrap();
        queue.apply_inbox_now().await.unwrap();
        let record = queue.view().inbox_record(&id).await.unwrap().unwrap();
        let InboxOutcome::Enqueue(EnqueueResult::New(job_id)) = record.outcome else {
            panic!("the command was not enqueued: {:?}", record.outcome);
        };
        for _ in 0..500 {
            match queue.view().get_job(&job_id).await.unwrap() {
                None => return None,
                Some(job) if job.status == JobStatus::Dead => return Some(job),
                Some(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
        panic!("the command job {job_id} did not settle");
    }

    #[tokio::test(start_paused = true)]
    async fn a_submit_command_runs_the_run_and_a_command_that_cannot_apply_dead_letters() {
        let (queue, sender, runtime, _clock) = open(OpenOptions::default()).await;
        let shutdown = spawn(&runtime);
        let submit = |input: &[u8]| Command::Submit(SubmitCommand::new(rid("run-1"), input));
        assert!(
            send(&queue, &sender, submit(b"hello").into())
                .await
                .is_none()
        );
        let end = runtime.wait(&rid("run-1")).await.unwrap();
        assert_eq!(end.termination.status, TerminalStatus::Succeeded);
        assert_eq!(
            end.outcome.unwrap().result.as_deref(),
            Some(b"hello".as_slice())
        );

        // A second submission of the run is a no-op, one with another input
        // fails permanently, and a payload that does not decode dead-letters.
        assert!(
            send(&queue, &sender, submit(b"hello").into())
                .await
                .is_none()
        );
        let other = send(&queue, &sender, submit(b"other").into())
            .await
            .unwrap();
        assert!(other.last_error.unwrap().contains("different input"));
        let malformed = send(&queue, &sender, EnqueueEntry::new(b"{}"))
            .await
            .unwrap();
        assert!(malformed.last_error.unwrap().contains("malformed command"));
        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_view_reads_the_state_of_a_command_from_its_record_and_its_job() {
        let (queue, sender, runtime, _clock) = open(OpenOptions::default()).await;
        let view = runtime.view();
        let submit = Command::Submit(SubmitCommand::new(rid("run-1"), b"x"));
        let applied = sender
            .send(CONTROL, EnqueueEntry::from(submit))
            .await
            .unwrap();
        let malformed = sender
            .send(CONTROL, EnqueueEntry::new(b"{}"))
            .await
            .unwrap();
        let cancel = Command::Cancel(CancelCommand::new(rid("run-1")));
        let outside = sender
            .send("other", EnqueueEntry::from(cancel))
            .await
            .unwrap();
        assert_eq!(view.command(&applied).await.unwrap(), None);

        // Without a worker, the command job waits on the control queue.
        queue.apply_inbox_now().await.unwrap();
        assert_eq!(
            view.command(&applied).await.unwrap(),
            Some(CommandState::Queued { last_error: None })
        );
        assert!(matches!(
            view.command(&outside).await.unwrap(),
            Some(CommandState::Failed(ref reason)) if reason.contains("allow-list")
        ));

        let tx = spawn(&runtime);
        for _ in 0..500 {
            let states = (
                view.command(&applied).await.unwrap(),
                view.command(&malformed).await.unwrap(),
            );
            if let (Some(CommandState::Applied), Some(CommandState::Failed(reason))) = states {
                assert!(reason.contains("malformed command"));
                let _ = tx.send(());
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the commands did not settle");
    }

    #[test]
    fn a_command_is_a_json_object_tagged_by_its_variant() {
        let submit = Command::Submit(SubmitCommand {
            run_at: Some(1),
            ..SubmitCommand::new(rid("run-1"), b"hello")
        });
        assert_eq!(
            serde_json::to_value(&submit).unwrap(),
            serde_json::json!({
                "command": "submit", "run_id": "run-1", "input": "aGVsbG8=", "run_at": 1
            })
        );
        let cancel: Command =
            serde_json::from_value(serde_json::json!({"command": "cancel", "run_id": "run-1"}))
                .unwrap();
        assert_eq!(cancel, Command::Cancel(CancelCommand::new(rid("run-1"))));
    }

    #[tokio::test]
    async fn a_control_queue_differs_from_the_queue_name_and_is_in_the_allow_list() {
        let inbox = InboxOptions::new([CONTROL, DEFAULT_QUEUE_NAME]);
        let (queue, store, _clock) =
            open_queue_at_with(INITIAL, OpenOptions::default().inbox(inbox)).await;
        for control in [DEFAULT_QUEUE_NAME, "unlisted"] {
            let built =
                WorkflowRuntime::builder(queue.clone(), store.clone(), Echo, NoopTerminalHook)
                    .control_queue(control)
                    .build();
            assert!(
                matches!(built, Err(Error::InvalidControlQueue(ref name)) if name == control),
                "{control}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancel_command_cancels_the_run_and_ignores_an_inactive_one() {
        let (queue, sender, runtime, _clock) = open(OpenOptions::default()).await;
        let shutdown = spawn(&runtime);
        runtime
            .submit(crate::RunSpec {
                run_id: Some(rid("run-2")),
                input: b"wait".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let cancel = Command::Cancel(CancelCommand {
            reason: Some("operator".to_string()),
            ..CancelCommand::new(rid("run-2"))
        });
        assert!(send(&queue, &sender, cancel.into()).await.is_none());
        let end = runtime.wait(&rid("run-2")).await.unwrap();
        assert_eq!(end.termination.status, TerminalStatus::Cancelled);
        assert_eq!(end.termination.error.as_deref(), Some("operator"));

        let unknown = Command::Cancel(CancelCommand::new(rid("run-9")));
        assert!(send(&queue, &sender, unknown.into()).await.is_none());
        let _ = shutdown.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancel_applies_a_waiting_submission_of_its_run_and_cancels_the_run() {
        let (queue, sender, runtime, clock) = open(OpenOptions::default()).await;
        let shutdown = spawn(&runtime);
        // The submission waits an hour on the control queue for its `run_at`,
        // so the cancellation sent after it reaches the worker first.
        let submit = EnqueueEntry {
            run_at: Some(INITIAL + 3_600_000),
            ..Command::Submit(SubmitCommand::new(rid("run-3"), b"x")).into()
        };
        let cancel = Command::Cancel(CancelCommand {
            reason: Some("operator".to_string()),
            ..CancelCommand::new(rid("run-3"))
        });
        let submit_id = sender.send(CONTROL, submit).await.unwrap();
        sender
            .send(CONTROL, EnqueueEntry::from(cancel))
            .await
            .unwrap();
        queue.apply_inbox_now().await.unwrap();
        let mut ended = None;
        for _ in 0..500 {
            ended = runtime.view().run_end(&rid("run-3")).await.unwrap();
            if ended.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let end = ended.unwrap();
        assert_eq!(end.termination.status, TerminalStatus::Cancelled);
        assert_eq!(end.termination.error.as_deref(), Some("operator"));
        // The cancellation removed the first step while it waited, so no worker
        // recorded an outcome.
        assert!(end.outcome.is_none());

        // At its `run_at`, the submission applies as a duplicate of the run.
        advance(&clock, Duration::from_secs(3_600)).await;
        queue.promote_scheduled_now().await.unwrap();
        let mut state = None;
        for _ in 0..500 {
            state = runtime.view().command(&submit_id).await.unwrap();
            if state == Some(CommandState::Applied) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(state, Some(CommandState::Applied));
        assert!(matches!(
            runtime.status(&rid("run-3")).await.unwrap().unwrap().state,
            crate::RunState::Terminated(_)
        ));
        let _ = shutdown.send(());
    }
}
