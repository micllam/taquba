//! Maintain application state in Taquba's caller KV namespace, updated
//! atomically with a run's own transitions.
//!
//! An order-processing run keeps a status row under
//! `app/orders/{run_id}/status`:
//!
//!   - submission writes `received` in the same transaction as the
//!     step-0 enqueue (`RunSpec::kv_writes`), together with a pending
//!     marker;
//!   - the validation step stages `validated` through `Step::effects`,
//!     so the new status commits with the settlement that enqueues the
//!     fulfilment step;
//!   - the fulfilment step stages `fulfilled` and deletes the pending
//!     marker, both applied with the terminal acknowledgement.
//!
//! Every value read back below was written by a settlement transaction,
//! so no crash point can leave the status row disagreeing with the
//! run's actual state.
//!
//! Run with:
//!
//! ```text
//! cargo run -p taquba-workflow --example kv_effects
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use taquba::Queue;
use taquba::object_store::memory::InMemory;
use taquba_workflow::{
    RunOutcome, RunSpec, Step, StepError, StepOutcome, StepRunner, TerminalEffects, TerminalHook,
    WorkflowRuntime,
};

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
            // Validation: the status update commits with the same
            // transaction that enqueues the fulfilment step.
            step.effects.put(status_key(&step.run_id), "validated")?;
            Ok(StepOutcome::continue_now(step.payload.clone()))
        } else {
            // Fulfilment: the final status and the marker delete are
            // applied with the terminal acknowledgement.
            step.effects.put(status_key(&step.run_id), "fulfilled")?;
            step.effects.delete(pending_key(&step.run_id))?;
            Ok(StepOutcome::Succeed {
                result: step.payload.clone(),
            })
        }
    }
}

struct CollectOutcomes {
    tx: tokio::sync::mpsc::UnboundedSender<RunOutcome>,
}

impl TerminalHook for CollectOutcomes {
    async fn on_termination(
        &self,
        outcome: &RunOutcome,
        _effects: &TerminalEffects,
    ) -> std::result::Result<(), StepError> {
        let _ = self.tx.send(outcome.clone());
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "kv-effects-demo").await?);

    let (tx, mut outcomes) = tokio::sync::mpsc::unbounded_channel();
    let runtime = WorkflowRuntime::builder(queue.clone(), store, OrderFlow, CollectOutcomes { tx })
        .poll_interval(Duration::from_millis(50))
        .build();

    // Submit before the worker starts, so the status row still reads
    // `received` when printed below.
    let run_id = "order-1001";
    runtime
        .submit(RunSpec {
            run_id: Some(run_id.into()),
            input: b"2 units of item 7".to_vec(),
            kv_writes: HashMap::from([
                (status_key(run_id), b"received".to_vec()),
                (pending_key(run_id), b"1".to_vec()),
            ]),
            ..Default::default()
        })
        .await?;
    println!("submitted: status = {}", read_status(&queue, run_id).await?);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let worker_runtime = runtime.clone();
    let worker_task = tokio::spawn(async move {
        worker_runtime
            .run(async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    // The hook runs as the worker of a notification job enqueued
    // atomically with the terminal acknowledgement, so once the outcome
    // arrives the final effects are already committed.
    let outcome = outcomes.recv().await.expect("terminal outcome");
    println!("run terminated: {}", outcome.status);

    println!("final: status = {}", read_status(&queue, run_id).await?);
    let pending = queue.kv_get(&pending_key(run_id)).await?;
    println!("final: pending marker present = {}", pending.is_some());

    let _ = shutdown_tx.send(());
    worker_task.await??;
    Ok(())
}

async fn read_status(queue: &Queue, run_id: &str) -> Result<String, taquba::Error> {
    let value = queue.kv_get(&status_key(run_id)).await?;
    Ok(value
        .map(|v| String::from_utf8_lossy(&v).into_owned())
        .unwrap_or_else(|| "<absent>".to_string()))
}
