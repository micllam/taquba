//! Submit a run that loops through several steps before completing.
//!
//! Each step parses its payload as `"<current>/<target>"`, increments the
//! current value, and either returns `Continue` with the new state or
//! `Complete` with the final value. The completion hook prints the result
//! and the runtime shuts down.
//!
//! Run with:
//!
//! ```text
//! cargo run -p taquba-workflow --example multi_step
//! ```

use std::sync::Arc;
use std::time::Duration;

use taquba::Queue;
use taquba::object_store::memory::InMemory;
use taquba_workflow::{
    RunOutcome, RunSpec, Step, StepError, StepOutcome, StepRunner, TerminalHook, TerminalStatus,
    WorkflowRuntime,
};
use tokio::sync::oneshot;

struct Counter;

impl StepRunner for Counter {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        let text = std::str::from_utf8(&step.payload)
            .map_err(|e| StepError::permanent(format!("non-utf8 payload: {e}")))?;
        let (current, target) = text
            .split_once('/')
            .ok_or_else(|| StepError::permanent("expected `<current>/<target>`"))?;
        let current: u32 = current
            .parse()
            .map_err(|e| StepError::permanent(format!("current: {e}")))?;
        let target: u32 = target
            .parse()
            .map_err(|e| StepError::permanent(format!("target: {e}")))?;

        let next = current + 1;
        println!(
            "  step {} for run {}: {} -> {} (target {})",
            step.step_number, step.run_id, current, next, target
        );

        if next >= target {
            Ok(StepOutcome::Succeed {
                result: format!("reached {next}").into_bytes(),
            })
        } else {
            Ok(StepOutcome::Continue {
                payload: format!("{next}/{target}").into_bytes(),
            })
        }
    }
}

struct ShutdownOnComplete {
    shutdown: tokio::sync::Mutex<Option<oneshot::Sender<()>>>,
}

impl TerminalHook for ShutdownOnComplete {
    async fn on_termination(&self, outcome: &RunOutcome) {
        let result = match outcome.status {
            TerminalStatus::Succeeded => {
                String::from_utf8_lossy(outcome.result.as_deref().unwrap_or(&[])).into_owned()
            }
            TerminalStatus::Failed => format!(
                "failed: {}",
                outcome.error.as_deref().unwrap_or("(no error)")
            ),
            _ => "(unknown terminal status)".to_string(),
        };
        println!(
            "completion: run={} status={:?} final_step={} result={:?} trace={:?}",
            outcome.run_id,
            outcome.status,
            outcome.final_step,
            result,
            outcome.headers.get("trace_id"),
        );
        if let Some(tx) = self.shutdown.lock().await.take() {
            let _ = tx.send(());
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "agent-demo").await?);

    let (tx, rx) = oneshot::channel::<()>();
    let runtime = WorkflowRuntime::builder(
        queue,
        store,
        Counter,
        ShutdownOnComplete {
            shutdown: tokio::sync::Mutex::new(Some(tx)),
        },
    )
    .max_concurrent_steps(4)
    .poll_interval(Duration::from_millis(50))
    .build();

    // Spawn the worker loop. It exits when `rx` resolves (fired by the hook
    // on terminal status).
    let worker_runtime = runtime.clone();
    let worker_task = tokio::spawn(async move {
        worker_runtime
            .run(async move {
                let _ = rx.await;
            })
            .await
    });

    // Submit a run that loops from 0 to 5 across six steps.
    let mut headers = std::collections::HashMap::new();
    headers.insert("trace_id".to_string(), "demo-trace".to_string());
    let handle = runtime
        .submit(RunSpec {
            input: b"0/5".to_vec(),
            headers,
            ..Default::default()
        })
        .await?;
    println!("submitted run {}", handle.run_id);

    worker_task.await??;
    Ok(())
}
