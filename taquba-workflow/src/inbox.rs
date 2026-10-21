//! The inbox kinds of the runtime: `workflow.submit` and `workflow.cancel`,
//! registered on the queue's inbox by
//! [`WorkflowRuntimeBuilder::inbox_kinds`](crate::WorkflowRuntimeBuilder::inbox_kinds),
//! so another process submits and cancels runs through a
//! [`taquba::InboxSender`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use taquba::{
    Applied, InboxHandler, InboxHandlerError, InboxKind, InboxOutcome, InboxRecordHandle,
};

use crate::error::Error;
use crate::keys::RunId;
use crate::runtime::{
    CancelRunOutcome, DEFAULT_QUEUE_NAME, RunOptions, RunSpec, RuntimeCore, SubmitOutcome,
};

/// The entry of the kind `workflow.submit`: a [`RunSpec`] without effects. Its
/// outcome is the [`SubmitOutcome`] of the submission. A submission of an
/// existing run id with another input is rejected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitEntry {
    /// The queue name of the runtime that runs the entry, as
    /// [`WorkflowRuntimeBuilder::queue_name`](crate::WorkflowRuntimeBuilder::queue_name)
    /// sets it. Defaults to [`DEFAULT_QUEUE_NAME`].
    #[serde(default = "default_queue_name")]
    pub queue: String,
    /// [`RunSpec::run_id`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    /// [`RunSpec::input`], base64 in the body.
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

impl SubmitEntry {
    /// An entry with `input` for the runtime with the default queue name and
    /// every option unset.
    pub fn new(input: impl Into<Vec<u8>>) -> Self {
        Self {
            queue: default_queue_name(),
            run_id: None,
            input: input.into(),
            headers: HashMap::new(),
            run_at: None,
            priority: None,
            max_attempts_per_step: None,
        }
    }
}

/// The field of the two entries that selects the runtime by its queue name.
pub(crate) const INBOX_QUEUE_FIELD: &str = "queue";

fn default_queue_name() -> String {
    DEFAULT_QUEUE_NAME.to_string()
}

impl InboxKind for SubmitEntry {
    const KIND: &'static str = "workflow.submit";
    type Outcome = SubmitOutcome;
}

/// The entry of the kind `workflow.cancel`:
/// [`WorkflowRuntime::cancel`](crate::WorkflowRuntime::cancel) of a run. Its
/// outcome is the [`CancelRunOutcome`] of the cancel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelRunEntry {
    /// The queue name of the runtime of the run, as [`SubmitEntry::queue`].
    #[serde(default = "default_queue_name")]
    pub queue: String,
    /// The run.
    pub run_id: RunId,
    /// The reason of the request: the `reason` of [`WorkflowRuntime::cancel`].
    ///
    /// [`WorkflowRuntime::cancel`]: crate::WorkflowRuntime::cancel
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl CancelRunEntry {
    /// An entry for `run_id` on the runtime with the default queue name,
    /// without a reason.
    pub fn new(run_id: RunId) -> Self {
        Self {
            queue: default_queue_name(),
            run_id,
            reason: None,
        }
    }
}

impl InboxKind for CancelRunEntry {
    const KIND: &'static str = "workflow.cancel";
    type Outcome = CancelRunOutcome;
}

/// A permanent error of the runtime rejects the entry, and any other error ends
/// the pass.
fn outcome_of<O>(err: Error) -> Result<Applied<O>, InboxHandlerError> {
    if err.is_permanent() {
        Ok(Applied::Separately(InboxOutcome::Rejected(err.to_string())))
    } else {
        Err(Box::new(err))
    }
}

/// The handler of `workflow.submit`.
pub(crate) struct SubmitHandler {
    pub(crate) core: Arc<RuntimeCore>,
}

impl InboxHandler for SubmitHandler {
    type Kind = SubmitEntry;

    async fn apply(
        &self,
        submit: SubmitEntry,
        record: &InboxRecordHandle<SubmitOutcome>,
    ) -> Result<Applied<SubmitOutcome>, InboxHandlerError> {
        let spec = RunSpec {
            run_id: Some(submit.run_id.unwrap_or_else(RunId::generate)),
            input: submit.input,
            options: RunOptions {
                headers: submit.headers,
                priority: submit.priority,
                max_attempts_per_step: submit.max_attempts_per_step,
                run_at: submit
                    .run_at
                    .map(|ms| UNIX_EPOCH + Duration::from_millis(ms)),
            },
            effects: Default::default(),
        };
        let submitted = self
            .core
            .submit_recorded(spec, |outcome, effects| record.attach(effects, outcome))
            .await;
        match submitted {
            Ok(outcome) if outcome.newly_submitted => Ok(Applied::Committed),
            Ok(outcome) => Ok(Applied::Separately(InboxOutcome::Applied(outcome))),
            Err(err) => outcome_of(err),
        }
    }
}

/// The handler of `workflow.cancel`.
pub(crate) struct CancelRunHandler {
    pub(crate) core: Arc<RuntimeCore>,
}

impl InboxHandler for CancelRunHandler {
    type Kind = CancelRunEntry;

    async fn apply(
        &self,
        cancel: CancelRunEntry,
        record: &InboxRecordHandle<CancelRunOutcome>,
    ) -> Result<Applied<CancelRunOutcome>, InboxHandlerError> {
        let cancelled = self
            .core
            .cancel_run(&cancel.run_id, cancel.reason, |effects| {
                record.attach(effects, &CancelRunOutcome::Removed)
            })
            .await;
        match cancelled {
            Ok(CancelRunOutcome::Removed) => Ok(Applied::Committed),
            Ok(outcome) => Ok(Applied::Separately(InboxOutcome::Applied(outcome))),
            Err(err) => outcome_of(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use taquba::{InboxOptions, InboxOutcome, InboxSender, OpenOptions, Queue};
    use tokio::sync::oneshot;

    use super::*;
    use crate::runner::{Step, StepError, StepOutcome, StepRunner};
    use crate::runtime::{RunState, WorkflowRuntime};
    use crate::terminal::{NoopTerminalHook, TerminalStatus};
    use crate::test_util::{open_queue_with, rid};

    /// A runner whose step 0 waits a second, then succeeds with its payload.
    struct Echo;

    impl StepRunner for Echo {
        async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
            match step.step_number {
                0 => Ok(StepOutcome::continue_after(
                    step.payload.clone(),
                    Duration::from_secs(1),
                )),
                _ => Ok(StepOutcome::Succeed {
                    result: step.payload.clone(),
                }),
            }
        }
    }

    async fn open() -> (Arc<Queue>, Arc<dyn taquba::object_store::ObjectStore>) {
        open_queue_with(OpenOptions::default().inbox(InboxOptions::default())).await
    }

    fn runtime(
        queue: &Arc<Queue>,
        store: &Arc<dyn taquba::object_store::ObjectStore>,
    ) -> WorkflowRuntime<Echo, NoopTerminalHook> {
        WorkflowRuntime::builder(queue.clone(), store.clone(), Echo, NoopTerminalHook)
            .poll_interval(Duration::from_millis(20))
            .inbox_kinds()
            .build()
            .unwrap()
    }

    fn spawn(runtime: &WorkflowRuntime<Echo, NoopTerminalHook>) -> oneshot::Sender<()> {
        let (tx, rx) = oneshot::channel::<()>();
        let runtime = runtime.clone();
        tokio::spawn(async move {
            let _ = runtime
                .run(async move {
                    let _ = rx.await;
                })
                .await;
        });
        tx
    }

    /// Close `queue` once `runtime` is dropped, which ends its registrations.
    async fn close(queue: Arc<Queue>, runtime: WorkflowRuntime<Echo, NoopTerminalHook>) {
        drop(runtime);
        for _ in 0..100 {
            if Arc::strong_count(&queue) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Arc::into_inner(queue)
            .expect("the runtime's registrations are dropped with it")
            .close()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_submit_entry_runs_the_run_and_records_its_duplicates() {
        let (queue, store) = open().await;
        let runtime = runtime(&queue, &store);
        let sender = InboxSender::new(store.clone(), "test-inbox");
        sender
            .send_with_id(
                "first",
                &SubmitEntry {
                    run_id: Some(rid("run-1")),
                    ..SubmitEntry::new(b"hello")
                },
            )
            .await
            .unwrap();
        queue.apply_inbox_now().await.unwrap();
        let first = queue
            .view()
            .inbox_record::<SubmitEntry>("first")
            .await
            .unwrap()
            .unwrap();
        let status = runtime.status(&rid("run-1")).await.unwrap().unwrap();
        assert_eq!(
            first.outcome,
            InboxOutcome::Applied(SubmitOutcome {
                run_id: rid("run-1"),
                newly_submitted: true,
                job_id: status.step.job_id,
            })
        );

        let shutdown = spawn(&runtime);
        let end = runtime.wait(&rid("run-1")).await.unwrap();
        assert_eq!(end.termination.status, TerminalStatus::Succeeded);
        assert_eq!(
            end.outcome.unwrap().result.as_deref(),
            Some(b"hello".as_slice())
        );
        let _ = shutdown.send(());

        // A second submission of the run is recorded as a duplicate, and one
        // with another input is rejected.
        sender
            .send_with_id(
                "again",
                &SubmitEntry {
                    run_id: Some(rid("run-1")),
                    ..SubmitEntry::new(b"hello")
                },
            )
            .await
            .unwrap();
        sender
            .send_with_id(
                "other",
                &SubmitEntry {
                    run_id: Some(rid("run-1")),
                    ..SubmitEntry::new(b"other")
                },
            )
            .await
            .unwrap();
        queue.apply_inbox_now().await.unwrap();
        assert_eq!(
            queue
                .view()
                .inbox_record::<SubmitEntry>("again")
                .await
                .unwrap()
                .unwrap()
                .outcome,
            InboxOutcome::Applied(SubmitOutcome {
                run_id: rid("run-1"),
                newly_submitted: false,
                job_id: end.termination.final_step.job_id,
            })
        );
        assert!(matches!(
            queue
                .view()
                .inbox_record::<SubmitEntry>("other")
                .await
                .unwrap()
                .unwrap()
                .outcome,
            InboxOutcome::Rejected(_)
        ));
        close(queue, runtime).await;
    }

    #[tokio::test]
    async fn a_cancel_entry_removes_the_queued_step_and_records_the_other_outcomes() {
        let (queue, store) = open().await;
        let runtime = runtime(&queue, &store);
        runtime
            .submit(RunSpec {
                run_id: Some(rid("run-2")),
                input: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let sender = InboxSender::new(store.clone(), "test-inbox");
        sender
            .send_with_id(
                "cancel",
                &CancelRunEntry {
                    reason: Some("operator".to_string()),
                    ..CancelRunEntry::new(rid("run-2"))
                },
            )
            .await
            .unwrap();
        sender
            .send_with_id("unknown", &CancelRunEntry::new(rid("run-9")))
            .await
            .unwrap();
        queue.apply_inbox_now().await.unwrap();

        assert_eq!(
            queue
                .view()
                .inbox_record::<CancelRunEntry>("cancel")
                .await
                .unwrap()
                .unwrap()
                .outcome,
            InboxOutcome::Applied(CancelRunOutcome::Removed)
        );
        assert!(matches!(
            runtime.status(&rid("run-2")).await.unwrap().unwrap().state,
            RunState::Terminated(ref termination)
                if termination.status == TerminalStatus::Cancelled
                    && termination.error.as_deref() == Some("operator")
        ));
        assert_eq!(
            queue
                .view()
                .inbox_record::<CancelRunEntry>("unknown")
                .await
                .unwrap()
                .unwrap()
                .outcome,
            InboxOutcome::Applied(CancelRunOutcome::NotActive)
        );
        close(queue, runtime).await;
    }

    #[tokio::test]
    async fn each_runtime_registers_the_kinds_for_its_queue_name() {
        let (queue, store) = open().await;
        let runtime = runtime(&queue, &store);
        let err = WorkflowRuntime::builder(queue.clone(), store.clone(), Echo, NoopTerminalHook)
            .inbox_kinds()
            .build()
            .err()
            .expect("a second runtime with the queue name fails");
        assert!(matches!(
            err,
            Error::Queue(taquba::Error::InboxKindTaken { ref kind, ref value })
                if kind == "workflow.submit" && value.as_deref() == Some("workflow-steps")
        ));

        // A runtime with another queue name registers the kinds for it, and an
        // entry reaches the runtime its `queue` field names.
        let billing =
            WorkflowRuntime::builder(queue.clone(), store.clone(), Echo, NoopTerminalHook)
                .queue_name("billing")
                .inbox_kinds()
                .build()
                .unwrap();
        let sender = InboxSender::new(store.clone(), "test-inbox");
        sender
            .send_with_id(
                "to-billing",
                &SubmitEntry {
                    queue: "billing".to_string(),
                    run_id: Some(rid("run-b")),
                    ..SubmitEntry::new(b"b")
                },
            )
            .await
            .unwrap();
        sender
            .send_with_id(
                "to-default",
                &SubmitEntry {
                    run_id: Some(rid("run-d")),
                    ..SubmitEntry::new(b"d")
                },
            )
            .await
            .unwrap();
        sender
            .send_with_id(
                "to-nobody",
                &SubmitEntry {
                    queue: "other".to_string(),
                    ..SubmitEntry::new(b"o")
                },
            )
            .await
            .unwrap();
        queue.apply_inbox_now().await.unwrap();
        for (id, queue_name) in [("to-billing", "billing"), ("to-default", "workflow-steps")] {
            assert!(
                queue
                    .view()
                    .inbox_record::<SubmitEntry>(id)
                    .await
                    .unwrap()
                    .is_some(),
                "{id}"
            );
            assert_eq!(queue.view().stats(queue_name).await.unwrap().pending, 1);
        }
        assert_eq!(
            queue
                .view()
                .inbox_record::<SubmitEntry>("to-nobody")
                .await
                .unwrap(),
            None
        );
        drop(billing);
        close(queue, runtime).await;

        let (queue, store) = open_queue_with(OpenOptions::default()).await;
        let err = WorkflowRuntime::builder(queue.clone(), store, Echo, NoopTerminalHook)
            .inbox_kinds()
            .build()
            .err()
            .expect("a queue without an inbox refuses the kinds");
        assert!(matches!(
            err,
            Error::Queue(taquba::Error::InboxNotConfigured)
        ));
        Arc::into_inner(queue).unwrap().close().await.unwrap();
    }
}
