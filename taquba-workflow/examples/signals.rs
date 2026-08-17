//! Pause a run until an external signal arrives, with a timeout.
//!
//! An approval flow: step 0 requests approval and defers the decision step
//! with `StepOutcome::continue_on_signal`; the decision step reads
//! `Step::signal` to distinguish an approval payload from a timeout. Three
//! orders demonstrate the three delivery paths:
//!
//!   - `order-a` waits and a signal wakes it before its timeout.
//!   - `order-b` receives no signal and escalates when its timeout elapses.
//!   - `order-c` is signalled before it registers; the buffered signal is
//!     consumed at registration and the run never waits.
//!
//! Run with:
//!
//! ```text
//! cargo run -p taquba-workflow --example signals
//! ```

use std::sync::Arc;
use std::time::Duration;

use taquba::Queue;
use taquba::object_store::memory::InMemory;
use taquba_workflow::{
    RunOutcome, RunSpec, SignalOutcome, Step, StepError, StepOutcome, StepRunner, TerminalEffects,
    TerminalHook, WorkflowRuntime,
};

/// Correlation key for an order's approval signal.
fn approval_key(run_id: &str) -> String {
    format!("approval:{run_id}")
}

struct ApprovalFlow;

impl StepRunner for ApprovalFlow {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        if step.step_number == 0 {
            // Input format: `<description>|<timeout secs>`.
            let text = std::str::from_utf8(&step.payload)
                .map_err(|e| StepError::permanent(format!("non-utf8 input: {e}")))?;
            let (description, timeout) = text
                .split_once('|')
                .ok_or_else(|| StepError::permanent("expected `<description>|<timeout secs>`"))?;
            let timeout: u64 = timeout
                .parse()
                .map_err(|e| StepError::permanent(format!("timeout: {e}")))?;

            println!("  [{}] awaiting approval: {description}", step.run_id);
            return Ok(StepOutcome::continue_on_signal(
                description.as_bytes().to_vec(),
                approval_key(&step.run_id),
                Duration::from_secs(timeout),
            ));
        }

        // Decision step: `Step::signal` carries the approver's payload, or
        // `None` when the timeout elapsed first.
        let description = String::from_utf8_lossy(&step.payload).into_owned();
        let verdict = match step.signal.as_deref() {
            Some(b"approve") => format!("approved: {description}"),
            Some(other) => format!(
                "rejected ({}): {description}",
                String::from_utf8_lossy(other)
            ),
            None => format!("escalated after timeout: {description}"),
        };
        println!("  [{}] {verdict}", step.run_id);
        Ok(StepOutcome::Succeed {
            result: verdict.into_bytes(),
        })
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
    let queue = Arc::new(Queue::open(store.clone(), "signals-demo").await?);

    let (tx, mut outcomes) = tokio::sync::mpsc::unbounded_channel();
    let runtime =
        WorkflowRuntime::builder(queue.clone(), store, ApprovalFlow, CollectOutcomes { tx })
            .poll_interval(Duration::from_millis(50))
            .build();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let worker_runtime = runtime.clone();
    let worker_task = tokio::spawn(async move {
        worker_runtime
            .run(async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    // Order A: the run waits; a signal wakes it well before its timeout.
    println!("order-a: signal arrives while the run waits");
    runtime
        .submit(RunSpec {
            run_id: Some("order-a".into()),
            input: b"3 licences|300".to_vec(),
            ..Default::default()
        })
        .await?;
    wait_for_waiter(&queue).await?;
    let delivery = runtime
        .signal(&approval_key("order-a"), b"approve".to_vec())
        .await?;
    println!("  signal outcome: {delivery:?}");
    assert_eq!(delivery, SignalOutcome::Delivered);
    print_terminal(outcomes.recv().await.expect("order-a outcome"));

    // Order B: no signal arrives; the timeout promotes the decision step.
    println!("order-b: no signal; the timeout elapses");
    runtime
        .submit(RunSpec {
            run_id: Some("order-b".into()),
            input: b"12 laptops|2".to_vec(),
            ..Default::default()
        })
        .await?;
    print_terminal(outcomes.recv().await.expect("order-b outcome"));

    // Order C: the signal arrives before the run registers its waiter; it
    // is buffered durably and consumed at registration, so the run never
    // waits.
    println!("order-c: signal buffered before the run registers");
    let delivery = runtime
        .signal(&approval_key("order-c"), b"budget exceeded".to_vec())
        .await?;
    println!("  signal outcome: {delivery:?}");
    assert_eq!(delivery, SignalOutcome::Buffered);
    runtime
        .submit(RunSpec {
            run_id: Some("order-c".into()),
            input: b"1 espresso machine|300".to_vec(),
            ..Default::default()
        })
        .await?;
    print_terminal(outcomes.recv().await.expect("order-c outcome"));

    let _ = shutdown_tx.send(());
    worker_task.await??;
    Ok(())
}

/// Wait until the order's decision step is registered as a waiter (a
/// scheduled job on the workflow queue).
async fn wait_for_waiter(queue: &Queue) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..100 {
        if queue.stats("workflow-steps").await?.scheduled >= 1 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err("waiter was never registered".into())
}

fn print_terminal(outcome: RunOutcome) {
    println!(
        "  terminal: run={} status={:?} result={:?}\n",
        outcome.run_id,
        outcome.status,
        String::from_utf8_lossy(outcome.result.as_deref().unwrap_or(&[])),
    );
}
