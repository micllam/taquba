//! Another process submits and cancels runs through the inbox.
//!
//! The runtime applies each job on its control queue as a `Command`. An
//! operator process uses an `InboxSender` and a `QueueReader`, without a queue
//! handle. It sends a `SubmitCommand` for each of two runs as an `EnqueueEntry`
//! for the control queue. Once the reader reports a step of the longer run in
//! progress, the operator sends a `CancelCommand` for that run. The writer
//! applies each entry at its next pass of the inbox, and the runtime's worker
//! applies the command job.
//!
//! ```text
//! cargo run -p taquba-workflow --example remote_submit
//! ```
//!
//! Through the reader, the operator reads the state of each command with
//! `WorkflowView::command` and the end of each run with
//! `WorkflowView::run_end`. The reader polls the store every half second, so
//! the operator observes each change with up to that lag. The control queue
//! keeps its done jobs for the record retention of the inbox, as
//! `WorkflowRuntimeBuilder::control_queue` requires.
//!
//! The operator process is simulated by a task that shares the in-memory object
//! store of the queue.

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use taquba::object_store::{ObjectStore, memory::InMemory};
use taquba::{
    DEFAULT_INBOX_RECORD_RETENTION, EnqueueEntry, InboxOptions, InboxSender, OpenOptions, Queue,
    QueueConfig, QueueReader, ReaderOptions, default_inbox_path,
};
use taquba_workflow::{
    CancelCommand, Command, CommandState, MemoStore, NoopTerminalHook, RunEnd, RunId, RunState,
    Step, StepError, StepOutcome, StepRunner, SubmitCommand, WorkflowRuntime, WorkflowView,
};
use tokio::sync::oneshot;

const QUEUE_PATH: &str = "workflow";
const CONTROL_QUEUE: &str = "workflow-steps.control";
const MEMO_PREFIX: &str = "workflow-memo";
const REPORT_RUN: &str = "report-q3";
const EXPORT_RUN: &str = "export-archive";
const TOUCH_POLL_INTERVAL: Duration = Duration::from_millis(500);
const READER_POLL_INTERVAL: Duration = Duration::from_millis(500);
const READ_INTERVAL: Duration = Duration::from_millis(200);
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const STAGE_WORK: Duration = Duration::from_secs(1);

/// The step runner. The input of a run states its count of stages, and each
/// step performs one stage.
struct Stages;

impl StepRunner for Stages {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        let stages: u32 = std::str::from_utf8(&step.payload)
            .ok()
            .and_then(|input| input.parse().ok())
            .ok_or_else(|| StepError::permanent("the input is not a count of stages"))?;
        tokio::time::sleep(STAGE_WORK).await;
        let done = step.step_number + 1;
        println!(
            "[runtime] {} finished stage {done} of {stages}",
            step.run_id
        );
        if done < stages {
            Ok(StepOutcome::continue_now(step.payload.clone()))
        } else {
            Ok(StepOutcome::Succeed {
                result: format!("{stages} stages").into_bytes(),
            })
        }
    }
}

/// The process on the other side: it writes commands to the inbox and reads
/// their effects from the store, and it does not open the queue.
async fn operator(store: Arc<dyn ObjectStore>) -> Result<(), Box<dyn Error>> {
    let sender = InboxSender::new(store.clone(), default_inbox_path(QUEUE_PATH));
    let reader_opts = ReaderOptions::default().manifest_poll_interval(READER_POLL_INTERVAL);
    let reader = QueueReader::open_with_options(store.clone(), QUEUE_PATH, reader_opts).await?;
    let view = WorkflowView::new(reader.view().clone(), MemoStore::new(store, MEMO_PREFIX));
    let report = RunId::new(REPORT_RUN)?;
    let export = RunId::new(EXPORT_RUN)?;

    let mut submissions = Vec::new();
    for (run_id, stages) in [(&report, 2), (&export, 10)] {
        let command = Command::Submit(SubmitCommand::new(run_id.clone(), stages.to_string()));
        let entry_id = sender
            .send(CONTROL_QUEUE, EnqueueEntry::from(command))
            .await?;
        println!("[operator] sent the submission of {run_id} as inbox entry {entry_id}");
        submissions.push(entry_id);
    }
    for entry_id in &submissions {
        let state = settled_command(&view, entry_id).await?;
        println!("[operator] inbox entry {entry_id}: {state:?}");
    }

    tokio::time::timeout(READ_TIMEOUT, async {
        loop {
            if let Some(status) = view.status(&export).await?
                && status.state == RunState::Running
            {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(READ_INTERVAL).await;
        }
    })
    .await??;
    let cancel = CancelCommand {
        run_id: export.clone(),
        reason: Some("superseded by a newer export".to_string()),
    };
    let entry_id = sender
        .send(CONTROL_QUEUE, EnqueueEntry::from(Command::Cancel(cancel)))
        .await?;
    println!("[operator] sent the cancellation of {export} as inbox entry {entry_id}");
    let state = settled_command(&view, &entry_id).await?;
    println!("[operator] inbox entry {entry_id}: {state:?}");

    for run_id in [&report, &export] {
        let end = run_end(&view, run_id).await?;
        let result = end.outcome.and_then(|outcome| outcome.result);
        println!(
            "[operator] run {run_id} ended as {:?}, cancelled by {:?}, with the result {:?}",
            end.termination.status,
            end.termination.cancelled_by,
            result.as_deref().map(String::from_utf8_lossy),
        );
    }
    reader.close().await?;
    Ok(())
}

/// The state of the command of `entry_id` once the inbox records the entry and
/// the command leaves the control queue.
async fn settled_command(
    view: &WorkflowView,
    entry_id: &str,
) -> Result<CommandState, Box<dyn Error>> {
    tokio::time::timeout(READ_TIMEOUT, async {
        loop {
            match view.command(CONTROL_QUEUE, entry_id).await? {
                None | Some(CommandState::Queued { .. }) => {}
                Some(state) => return Ok(state),
            }
            tokio::time::sleep(READ_INTERVAL).await;
        }
    })
    .await?
}

/// The end of `run_id` once the run terminates.
async fn run_end(view: &WorkflowView, run_id: &RunId) -> Result<RunEnd, Box<dyn Error>> {
    tokio::time::timeout(READ_TIMEOUT, async {
        loop {
            if let Some(end) = view.run_end(run_id).await? {
                return Ok(end);
            }
            tokio::time::sleep(READ_INTERVAL).await;
        }
    })
    .await?
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let control_config = QueueConfig::default().keep_done_jobs(DEFAULT_INBOX_RECORD_RETENTION);
    let opts = OpenOptions::default()
        .inbox(InboxOptions::new([CONTROL_QUEUE]).touch_poll_interval(TOUCH_POLL_INTERVAL))
        .queue_config(CONTROL_QUEUE, control_config);
    let queue = Arc::new(Queue::open_with_options(store.clone(), QUEUE_PATH, opts).await?);
    let runtime = WorkflowRuntime::builder(queue, store.clone(), Stages, NoopTerminalHook)
        .poll_interval(Duration::from_millis(200))
        .memo_prefix(MEMO_PREFIX)
        .control_queue(CONTROL_QUEUE)
        .build()?;

    let (stop, stopped) = oneshot::channel::<()>();
    let running = tokio::spawn(async move {
        runtime
            .run(async move {
                let _ = stopped.await;
            })
            .await
    });
    let result = operator(store).await;
    let _ = stop.send(());
    running.await??;
    result
}
