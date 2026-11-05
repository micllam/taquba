//! Minimal one-step run. The runner returns its payload unchanged as the
//! result. The submitter waits for the run, prints the result and shuts the
//! runtime down.
//!
//! Run with:
//!
//! ```text
//! cargo run -p taquba-workflow --example single_step
//! ```

use std::sync::Arc;

use taquba::Queue;
use taquba::object_store::memory::InMemory;
use taquba_workflow::{RunSpec, Step, StepError, StepOutcome, StepRunner, WorkflowRuntime};

struct Echo;

impl StepRunner for Echo {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        Ok(StepOutcome::Succeed {
            result: step.payload.clone(),
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "agent-demo").await?);

    let runtime = WorkflowRuntime::builder(queue, store, Echo).build()?;
    let worker = runtime.spawn(std::future::pending());

    let handle = runtime
        .submit(RunSpec {
            input: b"hello, world".to_vec(),
            ..Default::default()
        })
        .await?;

    let run = runtime.wait(&handle.run_id).await?;
    println!(
        "run {} -> {}: {}",
        run.run_id,
        run.termination.status,
        String::from_utf8_lossy(run.result.as_deref().unwrap_or(b""))
    );

    worker.shutdown().await?;
    Ok(())
}
