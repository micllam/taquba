use super::*;
use crate::durable::{self, DurableMember, DurableResult, DurableRun, DurableTermination};
use crate::effects::{EffectsHandle, TerminalEffects};
use crate::group::GroupMember;
use crate::keys::TERMINAL_KV_PREFIX;
use crate::keys::group_member_kv_key;
use crate::keys::run_kv_entry_key;
use crate::keys::{HEADER_RUN_ID, HEADER_STEP, step_kv_key};
use crate::runner::{Step, StepError, StepErrorKind, StepOutcome, Trigger};
use crate::terminal::{CancelOrigin, RunTermination, TerminalStatus};
use crate::test_util::{
    advance, fast_options, open_queue, open_queue_at, open_queue_at_with, open_queue_with, rid,
};
use crate::view::{RunFilter, WorkflowView};
use crate::view::{RunState, StepRef};
use futures_util::TryStreamExt;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::UNIX_EPOCH;
use taquba::KvOrder;
use taquba::object_store::ObjectStore;
use taquba::object_store::ObjectStoreExt;
use taquba::object_store::memory::InMemory;
use taquba::{EnqueueOptions, EnqueueRequest, JobStatus};
use taquba::{Expired, ExpiryIndex};
use taquba::{
    LeaseHandle, MockClock, OpenOptions, QueueConfig, QueueReader, SignalOutcome, WakeOutcome,
};
use tokio::sync::oneshot;

mod cancel;
mod effects;
mod reconcile;
mod replay;
mod retention;
mod signals;
mod steps;
mod submit;
mod termination;
mod view;

/// Runner that executes a fixed list of step outcomes in order.
struct ScriptedRunner {
    script: Arc<StdMutex<Vec<StepOutcome>>>,
}

impl ScriptedRunner {
    fn new(steps: Vec<StepOutcome>) -> Self {
        Self {
            script: Arc::new(StdMutex::new(steps)),
        }
    }
}

impl StepRunner for ScriptedRunner {
    async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
        let next = self.script.lock().unwrap().remove(0);
        Ok(next)
    }
}

/// Runner that returns a clone of `result` on every step and counts its calls.
struct FixedRunner {
    result: std::result::Result<StepOutcome, StepError>,
    calls: Arc<AtomicU32>,
}

impl FixedRunner {
    fn new(result: std::result::Result<StepOutcome, StepError>) -> Self {
        Self {
            result,
            calls: Arc::new(AtomicU32::new(0)),
        }
    }
}

impl StepRunner for FixedRunner {
    async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.result.clone()
    }
}

/// Runner whose step never returns.
struct PauseRunner;

impl StepRunner for PauseRunner {
    async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
        std::future::pending().await
    }
}

/// Runner whose step must not be claimed.
struct UnreachableRunner;

impl StepRunner for UnreachableRunner {
    async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
        unreachable!("worker must not claim the step");
    }
}

/// Runner that reports the claim of its step, keeps the step until its [`Gate`]
/// is released and then returns a clone of `result`.
struct GatedRunner {
    claimed: Arc<tokio::sync::Notify>,
    release: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
    result: std::result::Result<StepOutcome, StepError>,
    calls: Arc<AtomicU32>,
}

/// The test's side of a [`GatedRunner`].
struct Gate {
    claimed: Arc<tokio::sync::Notify>,
    release: StdMutex<Option<oneshot::Sender<()>>>,
    calls: Arc<AtomicU32>,
}

impl GatedRunner {
    fn new(result: std::result::Result<StepOutcome, StepError>) -> (Self, Gate) {
        let claimed = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicU32::new(0));
        let (release_tx, release_rx) = oneshot::channel();
        let runner = Self {
            claimed: claimed.clone(),
            release: tokio::sync::Mutex::new(Some(release_rx)),
            result,
            calls: calls.clone(),
        };
        let gate = Gate {
            claimed,
            release: StdMutex::new(Some(release_tx)),
            calls,
        };
        (runner, gate)
    }
}

impl StepRunner for GatedRunner {
    async fn run_step(&self, _step: &Step) -> std::result::Result<StepOutcome, StepError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.claimed.notify_one();
        let rx = self
            .release
            .lock()
            .await
            .take()
            .expect("gate consumed twice");
        let _ = rx.await;
        self.result.clone()
    }
}

impl Gate {
    /// Wait until the runner reports the claim of the step.
    async fn claimed(&self) {
        tokio::time::timeout(Duration::from_secs(2), self.claimed.notified())
            .await
            .expect("runner reached gate");
    }

    fn release(&self) {
        if let Some(tx) = self.release.lock().unwrap().take() {
            let _ = tx.send(());
        }
    }
}

/// Every terminal marker in the queue's KV namespace, as
/// `(run_id, terminal_at_ms)` pairs in key order (oldest first).
async fn terminal_markers(queue: &Queue) -> Vec<(RunId, u64)> {
    let page = queue
        .view()
        .kv_scan(TERMINAL_KV_PREFIX, .., taquba::KvOrder::Ascending, 1_000)
        .await
        .unwrap();
    let index = ExpiryIndex::new(TERMINAL_KV_PREFIX);
    page.entries
        .iter()
        .map(|(key, _)| {
            let (at_ms, suffix) = index.parse(key).expect("well-formed marker key");
            let id = std::str::from_utf8(suffix).expect("run id");
            (RunId::new(id).expect("run id"), at_ms)
        })
        .collect()
}

/// The terminal status `status` reports for `run_id`, or `None` while the run
/// is active or unknown.
async fn terminal_status_of<R: StepRunner>(
    runtime: &WorkflowRuntime<R>,
    run_id: &RunId,
) -> Option<TerminalStatus> {
    match runtime.status(run_id).await.unwrap().map(|s| s.state) {
        Some(RunState::Terminated(termination)) => Some(termination.status),
        _ => None,
    }
}

/// The outcome of `run_id` once it terminates, within two seconds.
async fn wait_for<R: StepRunner>(runtime: &WorkflowRuntime<R>, run_id: &RunId) -> TerminatedRun {
    tokio::time::timeout(Duration::from_secs(2), runtime.wait(run_id))
        .await
        .expect("the run terminates")
        .unwrap()
}

fn spawn_runtime<R>(runtime: WorkflowRuntime<R>) -> oneshot::Sender<()>
where
    R: StepRunner + 'static,
{
    let (tx, rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let _ = runtime
            .run(async move {
                let _ = rx.await;
            })
            .await;
    });
    tx
}

async fn wait_for_kv(queue: &Queue, key: &[u8]) -> Vec<u8> {
    for _ in 0..200 {
        if let Some(v) = queue.view().kv_get(key).await.unwrap() {
            return v.to_vec();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "kv key `{}` was never written",
        String::from_utf8_lossy(key)
    );
}

/// Runner that stages one `app/step-{n}` write per step and returns the next
/// scripted result.
struct EffectStagingRunner {
    script: Arc<StdMutex<Vec<std::result::Result<StepOutcome, StepError>>>>,
}

impl EffectStagingRunner {
    fn new(script: Vec<std::result::Result<StepOutcome, StepError>>) -> Self {
        Self {
            script: Arc::new(StdMutex::new(script)),
        }
    }
}

impl StepRunner for EffectStagingRunner {
    async fn run_step(&self, step: &Step) -> std::result::Result<StepOutcome, StepError> {
        step.effects
            .put(format!("app/step-{}", step.step_number), b"done".to_vec())
            .map_err(|e| StepError::permanent(e.to_string()))?;
        step.effects
            .enqueue(EnqueueRequest {
                queue: "app-requests".to_string(),
                payload: step.step_number.to_string().into_bytes(),
                options: EnqueueOptions::default(),
            })
            .map_err(|e| StepError::permanent(e.to_string()))?;
        self.script.lock().unwrap().remove(0)
    }
}
