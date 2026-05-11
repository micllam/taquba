//! Minimal one-step run: submit a payload, the runner echoes it back, the
//! completion hook prints the result and shuts the runtime down.
//!
//! Run with:
//!
//! ```text
//! cargo run -p taquba-workflow --example single_step
//! ```

use std::sync::Arc;

use taquba::Queue;
use taquba::object_store::memory::InMemory;
use taquba_workflow::{
    RunOutcome, RunSpec, Step, StepError, StepOutcome, StepRunner, TerminalHook, WorkflowRuntime,
};
use tokio::sync::oneshot;

struct Echo;

impl StepRunner for Echo {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        Ok(StepOutcome::Succeed {
            result: step.payload.clone(),
        })
    }
}

struct PrintAndExit {
    shutdown: tokio::sync::Mutex<Option<oneshot::Sender<()>>>,
}

impl TerminalHook for PrintAndExit {
    async fn on_termination(&self, outcome: &RunOutcome) {
        println!(
            "run {} -> {:?}: {}",
            outcome.run_id,
            outcome.status,
            String::from_utf8_lossy(outcome.result.as_deref().unwrap_or(b""))
        );
        if let Some(tx) = self.shutdown.lock().await.take() {
            let _ = tx.send(());
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = Arc::new(Queue::open(Arc::new(InMemory::new()), "agent-demo").await?);

    let (tx, rx) = oneshot::channel::<()>();
    let runtime = WorkflowRuntime::builder(
        queue,
        Echo,
        PrintAndExit {
            shutdown: tokio::sync::Mutex::new(Some(tx)),
        },
    )
    .build();

    let worker_runtime = runtime.clone();
    let worker_task = tokio::spawn(async move {
        worker_runtime
            .run(async move {
                let _ = rx.await;
            })
            .await
    });

    runtime
        .submit(RunSpec {
            input: b"hello, world".to_vec(),
            ..Default::default()
        })
        .await?;

    worker_task.await??;
    Ok(())
}
