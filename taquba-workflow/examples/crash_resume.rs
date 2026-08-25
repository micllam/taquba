//! Crash recovery on a persistent store: run it, interrupt it, run it again.
//!
//! The run advances through three stages, each pausing long enough to be
//! interrupted. A stage's output is committed before the next stage starts,
//! and within a stage each unit of work is recorded in the step's memo store.
//!
//! Run it, press Ctrl-C during any stage, then run it again. The second
//! process resumes the same run rather than starting a new one: committed
//! stages do not re-execute, the interrupted stage runs again with its
//! attempt count raised and the units it had already completed are served
//! from the memo store.
//!
//! ```text
//! cargo run -p taquba-workflow --example crash_resume
//! ```
//!
//! State lives under `/tmp/taquba-crash-resume-example`. Remove that
//! directory to discard a run and start over.

use std::sync::Arc;
use std::time::Duration;

use taquba::object_store::local::LocalFileSystem;
use taquba::{OpenOptions, Queue, QueueConfig};
use taquba_workflow::{
    RunOutcome, RunSpec, Step, StepError, StepOutcome, StepRunner, TerminalEffects, TerminalHook,
    TerminalStatus, WorkflowRuntime,
};
use tokio::sync::oneshot;

const QUEUE_DIR: &str = "/tmp/taquba-crash-resume-example";
const RUN_ID: &str = "crash-resume-demo";
const STAGES: [&str; 3] = ["fetch", "transform", "publish"];
const UNITS_PER_STAGE: u32 = 2;
const UNIT_SECONDS: u64 = 3;
// Must exceed a stage's duration, or the reaper requeues a step that is still
// running and a second attempt starts alongside the first.
const LEASE: Duration = Duration::from_secs(12);

struct Stages;

impl StepRunner for Stages {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        let stage = STAGES
            .get(step.step_number as usize)
            .ok_or_else(|| StepError::permanent(format!("no stage {}", step.step_number)))?;

        // The payload carries the stages committed so far.
        let committed = String::from_utf8(step.payload.clone())
            .map_err(|e| StepError::permanent(format!("non-utf8 payload: {e}")))?;

        println!();
        println!(
            "step {} ({stage}), attempt {}: committed stages [{}]",
            step.step_number,
            step.attempts,
            if committed.is_empty() {
                "none"
            } else {
                committed.as_str()
            }
        );

        for unit in 0..UNITS_PER_STAGE {
            let key = format!("unit-{unit}");
            if step.memo.get(&key).await?.is_some() {
                println!("  unit {unit}: recorded by an earlier attempt, not re-executed");
                continue;
            }

            for remaining in (1..=UNIT_SECONDS).rev() {
                println!("  unit {unit}: {remaining}s of work left (Ctrl-C to interrupt)");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            step.memo.put(&key, b"done").await?;
            println!("  unit {unit}: complete and recorded");
        }

        let committed = if committed.is_empty() {
            stage.to_string()
        } else {
            format!("{committed},{stage}")
        };

        if step.step_number as usize + 1 == STAGES.len() {
            Ok(StepOutcome::Succeed {
                result: committed.into_bytes(),
            })
        } else {
            Ok(StepOutcome::continue_now(committed.into_bytes()))
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
        println!();
        match outcome.status {
            TerminalStatus::Succeeded => println!(
                "run {} succeeded after {} steps, result: {}",
                outcome.run_id,
                outcome.final_step + 1,
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
    // The directory must exist before LocalFileSystem can canonicalize it.
    std::fs::create_dir_all(QUEUE_DIR)?;
    let store = Arc::new(LocalFileSystem::new_with_prefix(QUEUE_DIR)?);

    // An interrupted step stays claimed until its lease expires and the
    // reaper returns it to pending, so the lease sets the resume delay.
    // Each interruption spends one delivery attempt, hence the raised
    // max_attempts: the default of three dead-letters the run on a third
    // Ctrl-C.
    let queue = Arc::new(
        Queue::open_with_options(
            store.clone(),
            "workflow",
            OpenOptions::default()
                .reaper_interval(Duration::from_secs(1))
                .default_queue_config(
                    QueueConfig::default()
                        .lease_duration(LEASE)
                        .max_attempts(100),
                ),
        )
        .await?,
    );

    let (tx, rx) = oneshot::channel::<()>();
    let runtime = WorkflowRuntime::builder(
        queue,
        store,
        Stages,
        ShutdownOnTermination {
            shutdown: tokio::sync::Mutex::new(Some(tx)),
        },
    )
    .poll_interval(Duration::from_millis(200))
    .build();

    // Submission is idempotent on the run id across restarts: a later
    // process finds the durable run record and this call is a no-op.
    let outcome = runtime
        .submit(RunSpec {
            run_id: Some(RUN_ID.to_string()),
            input: Vec::new(),
            ..Default::default()
        })
        .await?;

    if outcome.newly_submitted {
        println!("submitted run {}", outcome.run_id);
    } else {
        println!(
            "run {} is already in flight; this process resumes it once the \
             interrupted step's {}s lease expires",
            outcome.run_id,
            LEASE.as_secs()
        );
    }

    // The worker loop claims steps until the terminal hook fires.
    runtime
        .run(async move {
            let _ = rx.await;
        })
        .await?;

    println!("remove {QUEUE_DIR} to discard the run and start over");
    Ok(())
}
