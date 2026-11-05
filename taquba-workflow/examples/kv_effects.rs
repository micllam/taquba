//! Maintain application state in Taquba's caller KV namespace, updated
//! atomically with a run's own transitions.
//!
//! An order-processing run keeps a status row under
//! `app/orders/{run_id}/status`:
//!
//!   - Submission writes `received` in the same transaction as the
//!     step-0 enqueue (`RunSpec::effects`), together with a pending
//!     marker.
//!   - The validation step stages `validated` through `Delivery::effects`,
//!     so the new status commits with the settlement that enqueues the
//!     fulfilment step.
//!   - The fulfilment step stages `fulfilled` and deletes the pending
//!     marker, both applied with the terminal acknowledgement.
//!
//! Every value read back below was written by a settlement transaction, so no
//! crash point can leave the status row disagreeing with the run's actual
//! state.
//!
//! Run with:
//!
//! ```text
//! cargo run -p taquba-workflow --example kv_effects
//! ```

use std::sync::Arc;
use std::time::Duration;

use taquba::object_store::memory::InMemory;
use taquba::{Queue, SettlementEffects};
use taquba_workflow::{RunId, RunSpec, Step, StepError, StepOutcome, StepRunner, WorkflowRuntime};

fn status_key(run_id: &str) -> Vec<u8> {
    format!("app/orders/{run_id}/status").into_bytes()
}

fn pending_key(run_id: &str) -> Vec<u8> {
    format!("app/orders/{run_id}/pending").into_bytes()
}

struct OrderFlow;

impl StepRunner for OrderFlow {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        if step.step_number == 0 {
            // Validation: the status update commits with the same transaction
            // that enqueues the fulfilment step.
            step.effects.put(status_key(&step.run_id), "validated")?;
            Ok(StepOutcome::continue_now(step.payload.clone()))
        } else {
            // Fulfilment: the final status and the marker delete are applied
            // with the terminal acknowledgement.
            step.effects.put(status_key(&step.run_id), "fulfilled")?;
            step.effects.delete(pending_key(&step.run_id))?;
            Ok(StepOutcome::Succeed {
                result: step.payload.clone(),
            })
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "kv-effects-demo").await?);

    let runtime = WorkflowRuntime::builder(queue.clone(), store, OrderFlow)
        .poll_interval(Duration::from_millis(50))
        .build()?;

    // Submit before the worker starts, so the status row still reads `received`
    // when printed below.
    let run_id = "order-1001";
    let handle = runtime
        .submit(RunSpec {
            run_id: Some(RunId::new(run_id)?),
            input: b"2 units of item 7".to_vec(),
            effects: SettlementEffects::default()
                .kv_put(status_key(run_id), b"received")
                .kv_put(pending_key(run_id), b"1"),
            ..Default::default()
        })
        .await?;
    println!("submitted: status = {}", read_status(&queue, run_id).await?);

    let worker = runtime.spawn(std::future::pending());

    // The final effects commit with the terminal acknowledgement, so they are
    // visible once the run has terminated.
    let run = runtime.wait(&handle.run_id).await?;
    println!("run terminated: {}", run.termination.status);

    println!("final: status = {}", read_status(&queue, run_id).await?);
    let pending = queue.view().kv_get(&pending_key(run_id)).await?;
    println!("final: pending marker present = {}", pending.is_some());

    worker.shutdown().await?;
    Ok(())
}

async fn read_status(queue: &Queue, run_id: &str) -> Result<String, taquba::Error> {
    let value = queue.view().kv_get(&status_key(run_id)).await?;
    Ok(value
        .map(|v| String::from_utf8_lossy(&v).into_owned())
        .unwrap_or_else(|| "<absent>".to_string()))
}
