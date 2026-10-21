//! The inbox kinds of the runtime: `workflow.submit` and `workflow.cancel`,
//! registered on the queue's inbox by
//! [`WorkflowRuntimeBuilder::inbox_kinds`](crate::WorkflowRuntimeBuilder::inbox_kinds),
//! so another process submits and cancels runs through a
//! [`taquba::InboxSender`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::json;
use taquba::{
    Applied, InboxEntry, InboxHandler, InboxHandlerError, InboxKind, InboxOutcome,
    InboxRecordHandle,
};

use crate::error::Error;
use crate::keys::RunId;
use crate::runtime::{CancelRun, DEFAULT_QUEUE_NAME, RunOptions, RunSpec, RuntimeCore};

/// The entry of the kind `workflow.submit`: a [`RunSpec`] without effects. The
/// outcome recorded for the entry is `{"run_id": id, "newly_submitted": true}`,
/// or `{"run_id": id, "job_id": id, "newly_submitted": false}` for a run that
/// exists, as [`SubmitOutcome`](crate::SubmitOutcome) reports it. A submission
/// of an existing run id with another input is rejected.
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
}

/// The entry of the kind `workflow.cancel`:
/// [`WorkflowRuntime::cancel`](crate::WorkflowRuntime::cancel) of a run. The
/// outcome recorded for the entry is `{"cancelled": true, "removed": true}`
/// when the queued step was removed, `{"cancelled": true, "removed": false}`
/// when the request reached a running step and `{"cancelled": false}` for a run
/// that is unknown or terminated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelRunEntry {
    /// The queue name of the runtime of the run, as [`SubmitEntry::queue`].
    #[serde(default = "default_queue_name")]
    pub queue: String,
    /// The run.
    pub run_id: RunId,
}

impl CancelRunEntry {
    /// An entry for `run_id` on the runtime with the default queue name.
    pub fn new(run_id: RunId) -> Self {
        Self {
            queue: default_queue_name(),
            run_id,
        }
    }
}

impl InboxKind for CancelRunEntry {
    const KIND: &'static str = "workflow.cancel";
}

/// The rejection of an entry for `reason`.
fn rejected(reason: String) -> Applied {
    Applied::Separately(InboxOutcome::Rejected(reason))
}

/// A permanent error of the runtime rejects the entry, and any other error ends
/// the pass.
fn outcome_of(err: Error) -> Result<Applied, InboxHandlerError> {
    if err.is_permanent() {
        Ok(rejected(err.to_string()))
    } else {
        Err(Box::new(err))
    }
}

/// The handler of `workflow.submit`.
pub(crate) struct SubmitHandler {
    pub(crate) core: Arc<RuntimeCore>,
}

impl InboxHandler for SubmitHandler {
    async fn apply(
        &self,
        entry: &InboxEntry,
        record: &InboxRecordHandle,
    ) -> Result<Applied, InboxHandlerError> {
        let submit: SubmitEntry = match entry.decode() {
            Ok(submit) => submit,
            Err(err) => return Ok(rejected(format!("malformed body: {err}"))),
        };
        let run_id = submit.run_id.unwrap_or_else(RunId::generate);
        let spec = RunSpec {
            run_id: Some(run_id.clone()),
            input: submit.input,
            options: RunOptions {
                headers: submit.headers,
                priority: submit.priority,
                max_attempts_per_step: submit.max_attempts_per_step,
                run_at: submit
                    .run_at
                    .map(|ms| UNIX_EPOCH + Duration::from_millis(ms)),
            },
            effects: record.effects(json!({ "run_id": run_id, "newly_submitted": true })),
        };
        match self.core.submit(spec).await {
            Ok(outcome) if outcome.newly_submitted => Ok(Applied::Committed),
            Ok(outcome) => Ok(Applied::Separately(InboxOutcome::Applied(json!({
                "run_id": outcome.run_id,
                "job_id": outcome.job_id,
                "newly_submitted": false,
            })))),
            Err(err) => outcome_of(err),
        }
    }
}

/// The handler of `workflow.cancel`.
pub(crate) struct CancelRunHandler {
    pub(crate) core: Arc<RuntimeCore>,
}

impl InboxHandler for CancelRunHandler {
    async fn apply(
        &self,
        entry: &InboxEntry,
        record: &InboxRecordHandle,
    ) -> Result<Applied, InboxHandlerError> {
        let cancel: CancelRunEntry = match entry.decode() {
            Ok(cancel) => cancel,
            Err(err) => return Ok(rejected(format!("malformed body: {err}"))),
        };
        let removed = json!({ "cancelled": true, "removed": true });
        let cancelled = self
            .core
            .cancel_run(&cancel.run_id, |effects| {
                record.attach(effects, removed.clone())
            })
            .await;
        match cancelled {
            Ok(CancelRun::Removed) => Ok(Applied::Committed),
            Ok(CancelRun::Requested) => Ok(Applied::Separately(InboxOutcome::Applied(
                json!({ "cancelled": true, "removed": false }),
            ))),
            Ok(CancelRun::NotActive) => Ok(Applied::Separately(InboxOutcome::Applied(
                json!({ "cancelled": false }),
            ))),
            Err(err) => outcome_of(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use serde_json::json;
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
        assert_eq!(
            queue
                .view()
                .inbox_record("first")
                .await
                .unwrap()
                .unwrap()
                .outcome,
            InboxOutcome::Applied(json!({ "run_id": "run-1", "newly_submitted": true }))
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
        let again = queue.view().inbox_record("again").await.unwrap().unwrap();
        let InboxOutcome::Applied(outcome) = again.outcome else {
            panic!("{:?}", again.outcome);
        };
        assert_eq!(outcome["run_id"], "run-1");
        assert_eq!(outcome["newly_submitted"], false);
        assert!(matches!(
            queue
                .view()
                .inbox_record("other")
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
            .send_with_id("cancel", &CancelRunEntry::new(rid("run-2")))
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
                .inbox_record("cancel")
                .await
                .unwrap()
                .unwrap()
                .outcome,
            InboxOutcome::Applied(json!({ "cancelled": true, "removed": true }))
        );
        assert!(matches!(
            runtime.status(&rid("run-2")).await.unwrap().unwrap().state,
            RunState::Terminated(ref termination) if termination.status == TerminalStatus::Cancelled
        ));
        assert_eq!(
            queue
                .view()
                .inbox_record("unknown")
                .await
                .unwrap()
                .unwrap()
                .outcome,
            InboxOutcome::Applied(json!({ "cancelled": false }))
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
                queue.view().inbox_record(id).await.unwrap().is_some(),
                "{id}"
            );
            assert_eq!(queue.view().stats(queue_name).await.unwrap().pending, 1);
        }
        assert_eq!(queue.view().inbox_record("to-nobody").await.unwrap(), None);
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
