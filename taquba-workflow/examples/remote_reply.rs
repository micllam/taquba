//! Remote work that replies through the inbox, without an inbound endpoint.
//!
//! A step waits for the reply with `StepOutcome::continue_on_signal` and stages
//! its request as an enqueue to a request queue, with `Delivery::next_job_id`,
//! the id of the waiting step, in the request. The enqueue commits in the
//! settlement that registers the wait, so the request leaves after the wait
//! exists. A worker of the request queue forwards the request to the machine
//! that performs the work, and that machine sends its reply as a `SignalEntry`
//! to the id through an `InboxSender`. The writer applies the entry at its next
//! pass of the inbox, and the signal wakes the run with the reply as its
//! payload. The remote machine needs write access to the inbox prefix alone,
//! and this process does not need a reachable port.
//!
//! ```text
//! cargo run -p taquba-workflow --example remote_reply
//! ```
//!
//! No lease is held during the wait: the requesting step settles, and the
//! waiting run is a scheduled job in the store, so the remote work can outlast
//! this process. The queue rejects a reply that arrives after the timeout, and
//! the inbox records the outcome `SignalOutcome::NotWaiting` for it.
//!
//! The remote machine is simulated by a task that receives the request over a
//! channel, works for two seconds and sends its reply to the inbox in the same
//! in-memory object store that the queue uses.

use std::sync::Arc;
use std::time::Duration;

use taquba::object_store::{ObjectStore, memory::InMemory};
use taquba::{
    EnqueueOptions, EnqueueRequest, InboxOptions, InboxSender, JobRecord, LeaseHandle, OpenOptions,
    Queue, SignalEntry, Worker, WorkerError, default_inbox_path, run_worker,
};
use taquba_workflow::{
    RunId, RunOutcome, RunSpec, Step, StepError, StepOutcome, StepRunner, TerminalEffects,
    TerminalHook, TerminalStatus, WorkflowRuntime,
};
use tokio::sync::{mpsc, oneshot};

const QUEUE_PATH: &str = "workflow";
const REQUEST_QUEUE: &str = "remote-requests";
const RUN_ID: &str = "translate-invoice-118";
const REPLY_TIMEOUT: Duration = Duration::from_secs(30);
const TOUCH_POLL_INTERVAL: Duration = Duration::from_millis(500);
const REMOTE_WORK: Duration = Duration::from_secs(2);

/// A request for the remote worker: the work and the id of the waiting step.
struct Request {
    text: String,
    wait_id: String,
}

/// The step runner. Step 0 stages the request and waits, step 1 reads the
/// reply.
struct Dispatcher;

impl StepRunner for Dispatcher {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        match step.step_number {
            0 => dispatch(step),
            _ => collect(step),
        }
    }
}

fn dispatch(step: &Step) -> Result<StepOutcome, StepError> {
    let text = String::from_utf8_lossy(&step.payload).into_owned();
    let wait_id = step.next_job_id.clone();
    println!(
        "[step {}] requesting {text:?}; the reply wakes {wait_id}",
        step.step_number
    );
    // The request commits with the settlement that registers the wait.
    step.effects
        .enqueue(EnqueueRequest {
            queue: REQUEST_QUEUE.to_string(),
            payload: format!("{wait_id}\n{text}").into_bytes(),
            options: EnqueueOptions::default(),
        })
        .map_err(|e| StepError::permanent(e.to_string()))?;
    Ok(StepOutcome::continue_on_signal(
        step.payload.clone(),
        REPLY_TIMEOUT,
    ))
}

fn collect(step: &Step) -> Result<StepOutcome, StepError> {
    let Some(reply) = step.signal.as_deref() else {
        let reason = format!("no reply within {}s", REPLY_TIMEOUT.as_secs());
        println!("[step {}] {reason}", step.step_number);
        return Ok(StepOutcome::Fail { reason });
    };
    println!(
        "[step {}] reply: {:?}",
        step.step_number,
        String::from_utf8_lossy(reply)
    );
    Ok(StepOutcome::Succeed {
        result: reply.to_vec(),
    })
}

/// The worker of the request queue: it forwards each request to the remote
/// machine.
struct RequestForwarder {
    remote: mpsc::Sender<Request>,
}

impl Worker for RequestForwarder {
    async fn process(&self, job: &JobRecord, _lease: &LeaseHandle) -> Result<(), WorkerError> {
        let payload = String::from_utf8_lossy(&job.payload);
        let (wait_id, text) = payload.split_once('\n').ok_or("malformed request")?;
        self.remote
            .send(Request {
                text: text.to_string(),
                wait_id: wait_id.to_string(),
            })
            .await
            .map_err(|_| "remote worker is gone")?;
        Ok(())
    }
}

/// The machine on the other side: it receives a request, performs the work and
/// sends the reply as a signal entry. In production this is any process with
/// write access to the inbox prefix.
async fn remote_worker(mut requests: mpsc::Receiver<Request>, sender: InboxSender) {
    while let Some(request) = requests.recv().await {
        tokio::time::sleep(REMOTE_WORK).await;
        let entry = SignalEntry {
            job_id: request.wait_id,
            payload: request.text.to_uppercase().into_bytes(),
        };
        match sender.send(&entry).await {
            Ok(id) => println!("[remote] sent the reply as inbox entry {id}"),
            Err(e) => println!("[remote] failed to send the reply: {e}"),
        }
    }
}

struct ShutdownOnTermination {
    shutdown: tokio::sync::Mutex<Option<oneshot::Sender<()>>>,
}

impl TerminalHook for ShutdownOnTermination {
    async fn on_termination(
        &self,
        outcome: &RunOutcome,
        _effects: &TerminalEffects,
    ) -> std::result::Result<(), StepError> {
        match outcome.status {
            TerminalStatus::Succeeded => println!(
                "run {} succeeded: {:?}",
                outcome.run_id,
                String::from_utf8_lossy(outcome.result.as_deref().unwrap_or(&[]))
            ),
            _ => println!(
                "run {} terminated as {:?}: {}",
                outcome.run_id,
                outcome.status,
                outcome.error.as_deref().unwrap_or("(no error)")
            ),
        }
        if let Some(tx) = self.shutdown.lock().await.take() {
            let _ = tx.send(());
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let opts = OpenOptions {
        inbox: Some(InboxOptions::default().touch_poll_interval(TOUCH_POLL_INTERVAL)),
        ..OpenOptions::default()
    };
    let queue = Arc::new(Queue::open_with_options(store.clone(), QUEUE_PATH, opts).await?);
    let sender = InboxSender::new(store.clone(), default_inbox_path(QUEUE_PATH));

    let (requests, request_rx) = mpsc::channel::<Request>(8);
    let (tx, rx) = oneshot::channel::<()>();
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store,
        Dispatcher,
        ShutdownOnTermination {
            shutdown: tokio::sync::Mutex::new(Some(tx)),
        },
    )
    .poll_interval(Duration::from_millis(200))
    .build()?;

    tokio::spawn(remote_worker(request_rx, sender));
    let (stop_forwarder, forwarder_stopped) = oneshot::channel::<()>();
    let forwarder = tokio::spawn({
        let queue = queue.clone();
        async move {
            let worker = RequestForwarder { remote: requests };
            run_worker(
                &queue,
                REQUEST_QUEUE,
                &worker,
                Duration::from_millis(200),
                async move {
                    let _ = forwarder_stopped.await;
                },
            )
            .await
        }
    });

    let outcome = runtime
        .submit(RunSpec {
            run_id: Some(RunId::new(RUN_ID)?),
            input: b"invoice 118: three chairs, one table".to_vec(),
            ..Default::default()
        })
        .await?;
    println!("submitted run {}", outcome.run_id);

    runtime
        .run(async move {
            let _ = rx.await;
        })
        .await?;
    let _ = stop_forwarder.send(());
    forwarder.await??;
    Ok(())
}
