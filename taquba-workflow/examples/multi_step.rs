//! Submit a run that loops through several steps before completing.
//!
//! Each step parses its payload as `"<current>/<target>"`, increments the
//! current value and either returns `Continue` with the new state or `Succeed`
//! with the final value. The submitter waits for the run, prints the result and
//! shuts the runtime down.
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
    RunOptions, RunSpec, Step, StepError, StepOutcome, StepRunner, TerminalStatus, WorkflowRuntime,
};

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
            Ok(StepOutcome::continue_now(
                format!("{next}/{target}").into_bytes(),
            ))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "agent-demo").await?);

    let runtime = WorkflowRuntime::builder(queue, store, Counter)
        .max_concurrent_steps(4)
        .poll_interval(Duration::from_millis(50))
        .build()?;
    let worker = runtime.spawn(std::future::pending());

    // Submit a run that loops from 0 to 5 across six steps.
    let mut headers = std::collections::HashMap::new();
    headers.insert("trace_id".to_string(), "demo-trace".to_string());
    let handle = runtime
        .submit(RunSpec {
            input: b"0/5".to_vec(),
            options: RunOptions {
                headers,
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;
    println!("submitted run {}", handle.run_id);

    let run = runtime.wait(&handle.run_id).await?;
    let termination = &run.termination;
    let result = match termination.status {
        TerminalStatus::Succeeded => {
            String::from_utf8_lossy(run.result.as_deref().unwrap_or(&[])).into_owned()
        }
        TerminalStatus::Failed | TerminalStatus::Cancelled => format!(
            "{}: {}",
            termination.status,
            termination.error.as_deref().unwrap_or("(no error)")
        ),
    };
    println!(
        "completion: run={} status={} final_step={} result={:?} trace={:?}",
        run.run_id,
        termination.status,
        termination.final_step.number,
        result,
        run.headers.get("trace_id"),
    );

    worker.shutdown().await?;
    Ok(())
}
