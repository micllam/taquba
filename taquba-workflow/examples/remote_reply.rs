//! Remote work that replies through the bucket, with no inbound endpoint.
//!
//! A step sends a request to a worker on another machine and waits for
//! the reply with `StepOutcome::continue_on_signal`. The worker writes
//! its reply as an object at a key the step chose, and a watcher task in
//! this process turns the object's arrival into the signal that wakes
//! the run. The remote worker therefore needs only a way to PUT one
//! object (a presigned URL in production), and this process does not
//! need a reachable port.
//!
//! ```text
//! cargo run -p taquba-workflow --example remote_reply
//! ```
//!
//! The pieces:
//!
//! - The reply key is `replies/{run_id}/{step}`, so a step delivered
//!   twice issues two requests whose replies overwrite each other.
//! - The step stages a pending marker, `remote/pending/{correlation_key}`
//!   with the reply key as its value, through `Delivery::effects`. The
//!   marker commits in the settlement that registers the waiter, so the
//!   watcher never sees a marker without a waiter, and a step that fails
//!   to settle does not leave a marker.
//! - The watcher reads the pending markers, HEADs each reply key, and on
//!   a hit delivers the key as the signal payload and removes the
//!   marker. It issues one HEAD per waiting run per poll and no LIST.
//! - The next step reads the reply object at the key in `Step::signal`,
//!   or escalates when the timeout elapsed first.
//!
//! No lease is held during the wait: the requesting step settles, and
//! the waiting run is a scheduled job in the store, so the remote work
//! can outlast this process. There is no waiter for a reply that arrives
//! after the timeout, and its object stays in the bucket for a retention
//! sweep.
//!
//! The remote machine is simulated by a task that receives the request
//! over a channel, works for two seconds and PUTs the reply into the same
//! in-memory object store that the queue uses.

use std::sync::Arc;
use std::time::Duration;

use futures_util::TryStreamExt;
use taquba::Queue;
use taquba::object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path};
use taquba_workflow::{
    RunOutcome, RunSpec, SignalOutcome, Step, StepError, StepOutcome, StepRunner, TerminalEffects,
    TerminalHook, TerminalStatus, WorkflowRuntime,
};
use tokio::sync::{mpsc, oneshot};

const RUN_ID: &str = "translate-invoice-118";
const REPLY_TIMEOUT: Duration = Duration::from_secs(30);
const WATCH_INTERVAL: Duration = Duration::from_millis(500);
const REMOTE_WORK: Duration = Duration::from_secs(2);
const PENDING_PREFIX: &[u8] = b"remote/pending/";

/// Correlation key of the wait a step registers for its reply.
fn correlation_key(run_id: &str, step_number: u32) -> String {
    format!("remote:{run_id}:{step_number}")
}

/// KV key of the pending marker for a correlation key.
fn pending_marker(correlation_key: &str) -> Vec<u8> {
    [PENDING_PREFIX, correlation_key.as_bytes()].concat()
}

/// Object key of the reply for a step, deterministic per run and step.
fn reply_key(run_id: &str, step_number: u32) -> String {
    format!("replies/{run_id}/{step_number}")
}

/// A request for the remote worker: the work and where to put the reply.
struct Request {
    text: String,
    reply_key: String,
}

/// The step runner. Step 0 dispatches, step 1 reads the reply.
struct Dispatcher {
    remote: mpsc::Sender<Request>,
    store: Arc<dyn ObjectStore>,
}

impl StepRunner for Dispatcher {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        match step.step_number {
            0 => self.dispatch(step).await,
            _ => self.collect(step).await,
        }
    }
}

impl Dispatcher {
    async fn dispatch(&self, step: &Step) -> Result<StepOutcome, StepError> {
        let text = String::from_utf8_lossy(&step.payload).into_owned();
        let reply_key = reply_key(&step.run_id, step.step_number);
        let key = correlation_key(&step.run_id, step.step_number);
        println!(
            "[step {}] sending {text:?}; reply expected at {reply_key}",
            step.step_number
        );

        // The marker commits with this step's settlement, in the same
        // transaction that registers the waiter.
        step.effects
            .put(pending_marker(&key), reply_key.as_bytes())?;
        self.remote
            .send(Request { text, reply_key })
            .await
            .map_err(|_| StepError::transient("remote worker is gone"))?;
        Ok(StepOutcome::continue_on_signal(
            step.payload.clone(),
            key,
            REPLY_TIMEOUT,
        ))
    }

    async fn collect(&self, step: &Step) -> Result<StepOutcome, StepError> {
        let Some(reply_key) = step.signal.as_deref() else {
            let reason = format!("no reply within {}s", REPLY_TIMEOUT.as_secs());
            println!("[step {}] {reason}", step.step_number);
            return Ok(StepOutcome::Fail { reason });
        };
        let path = Path::from(String::from_utf8_lossy(reply_key).into_owned());
        let reply = self
            .store
            .get(&path)
            .await
            .map_err(|e| StepError::transient(format!("reply read: {e}")))?
            .bytes()
            .await
            .map_err(|e| StepError::transient(format!("reply read: {e}")))?;
        println!(
            "[step {}] reply at {path}: {:?}",
            step.step_number,
            String::from_utf8_lossy(&reply)
        );
        Ok(StepOutcome::Succeed {
            result: reply.to_vec(),
        })
    }
}

/// The machine on the other side: it receives a request, works, and
/// PUTs the reply at the key it was given. In production this is any
/// process with a presigned PUT URL for that key.
async fn remote_worker(mut requests: mpsc::Receiver<Request>, store: Arc<dyn ObjectStore>) {
    while let Some(request) = requests.recv().await {
        tokio::time::sleep(REMOTE_WORK).await;
        let reply = request.text.to_uppercase();
        let path = Path::from(request.reply_key);
        match store.put(&path, PutPayload::from(reply.into_bytes())).await {
            Ok(_) => println!("[remote] wrote the reply to {path}"),
            Err(e) => println!("[remote] failed to write {path}: {e}"),
        }
    }
}

/// Turns reply objects into signals. Each poll reads the pending markers
/// and issues one HEAD per marker.
async fn watch_replies(
    queue: Arc<Queue>,
    store: Arc<dyn ObjectStore>,
    runtime: WorkflowRuntime<Dispatcher, ShutdownOnTermination>,
) {
    loop {
        tokio::time::sleep(WATCH_INTERVAL).await;
        if let Err(e) = watch_once(&queue, &store, &runtime).await {
            println!("[watcher] pass failed: {e}");
        }
    }
}

async fn watch_once(
    queue: &Queue,
    store: &Arc<dyn ObjectStore>,
    runtime: &WorkflowRuntime<Dispatcher, ShutdownOnTermination>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut markers = std::pin::pin!(queue.kv_entries(PENDING_PREFIX, 64));
    while let Some((marker, reply_key)) = markers.try_next().await? {
        let path = Path::from(String::from_utf8_lossy(&reply_key).into_owned());
        match store.head(&path).await {
            Ok(_) => {}
            Err(taquba::object_store::Error::NotFound { .. }) => continue,
            Err(e) => return Err(e.into()),
        }
        let key = String::from_utf8_lossy(&marker[PENDING_PREFIX.len()..]).into_owned();
        match runtime.signal(&key, reply_key.to_vec()).await? {
            SignalOutcome::Delivered => println!("[watcher] {path} arrived; woke {key}"),
            SignalOutcome::Buffered => {
                println!("[watcher] {path} arrived after the wait ended; discarding the signal");
                runtime.clear_signal(&key).await?;
            }
        }
        queue.kv_delete(&marker).await?;
    }
    Ok(())
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
    let queue = Arc::new(Queue::open(store.clone(), "workflow").await?);

    let (requests, inbox) = mpsc::channel::<Request>(8);
    let (tx, rx) = oneshot::channel::<()>();
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store.clone(),
        Dispatcher {
            remote: requests,
            store: store.clone(),
        },
        ShutdownOnTermination {
            shutdown: tokio::sync::Mutex::new(Some(tx)),
        },
    )
    .poll_interval(Duration::from_millis(200))
    .build();

    tokio::spawn(remote_worker(inbox, store.clone()));
    tokio::spawn(watch_replies(queue.clone(), store, runtime.clone()));

    let outcome = runtime
        .submit(RunSpec {
            run_id: Some(RUN_ID.to_string()),
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
    Ok(())
}
