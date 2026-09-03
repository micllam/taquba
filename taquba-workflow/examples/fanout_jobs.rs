//! Inner fan-out composing workflow steps with a job group: a workflow
//! step submits one typed job per URL as the members of a group named
//! after the step, joins their typed results and continues with the
//! aggregate.
//!
//! - **Step 0 (`fetch`)**: submits one `FetchPage` job per URL in the
//!   run input to the group `fetch-{run_id}`, joins every result and
//!   continues with the joined report.
//! - **Step 1 (`report`)**: formats the aggregate into the run's final
//!   result.
//!
//! The group keeps the fan-out safe under at-least-once delivery: a
//! retry of step 0 submits the same group again, which runs only the
//! members that did not succeed, and joins the recorded results of the
//! rest without running them twice.
//!
//! Both layers consume one shared `Arc<Queue>`: the workflow runtime
//! and the job runner are consumers of the same store, in the same
//! process, on different logical queues.
//!
//! ```text
//! cargo run -p taquba-workflow --example fanout_jobs
//! ```

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use taquba::Queue;
use taquba::object_store::memory::InMemory;
use taquba_workflow::jobs::{Job, JobContext, JobRunner};
use taquba_workflow::{
    RunOutcome, RunSpec, Step, StepError, StepOutcome, StepRunner, TerminalEffects, TerminalHook,
    TerminalStatus, WorkflowRuntime,
};
use tokio::sync::oneshot;

/// Mocked page fetch. A real version would issue an HTTP request; the
/// example derives a deterministic byte count from the URL.
#[derive(Serialize, Deserialize)]
struct FetchPage {
    url: String,
}

#[derive(Debug, thiserror::Error)]
#[error("fetch error: {0}")]
struct FetchError(String);

impl Job for FetchPage {
    const NAME: &'static str = "example.fetch_page";
    type Output = u64;
    type Error = FetchError;

    async fn run(&self, _ctx: JobContext<'_>) -> Result<u64, FetchError> {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(self.url.len() as u64 * 100)
    }

    fn idempotency_key(&self) -> Option<String> {
        Some(format!("fetch:{}", self.url))
    }
}

const STEP_FETCH: u32 = 0;
const STEP_REPORT: u32 = 1;

struct FanoutRunner {
    jobs: Arc<JobRunner>,
}

impl StepRunner for FanoutRunner {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        match step.step_number {
            STEP_FETCH => {
                let input = std::str::from_utf8(&step.payload)
                    .map_err(|e| StepError::permanent(format!("non-utf8 input: {e}")))?;
                let urls: Vec<&str> = input.lines().collect();

                // Fan out: one typed job per URL, as one group per step.
                let group = self
                    .jobs
                    .group::<FetchPage>(format!("fetch-{}", step.run_id))?;
                group
                    .submit(urls.iter().map(|url| FetchPage {
                        url: (*url).to_string(),
                    }))
                    .await?;

                // Join: every typed result, in submission order.
                let mut lines = Vec::with_capacity(urls.len());
                let mut total: u64 = 0;
                for member in group.join().await? {
                    let bytes = member
                        .result
                        .map_err(|e| StepError::transient(format!("fetch failed: {e}")))?;
                    println!("[step 0] fetched {}: {bytes} bytes", member.key);
                    lines.push(format!("{}: {bytes} bytes", member.key));
                    total += bytes;
                }
                lines.push(format!("total: {total} bytes"));
                Ok(StepOutcome::continue_now(lines.join("\n").into_bytes()))
            }
            STEP_REPORT => {
                let findings = std::str::from_utf8(&step.payload)
                    .map_err(|e| StepError::permanent(format!("non-utf8 payload: {e}")))?;
                let report = format!("fetch report\n------------\n{findings}");
                println!("[step 1] report ready");
                Ok(StepOutcome::Succeed {
                    result: report.into_bytes(),
                })
            }
            other => Err(StepError::permanent(format!(
                "unexpected step number {other}"
            ))),
        }
    }
}

struct ShutdownOnComplete {
    shutdown: tokio::sync::Mutex<Option<oneshot::Sender<()>>>,
}

impl TerminalHook for ShutdownOnComplete {
    async fn on_termination(
        &self,
        outcome: &RunOutcome,
        _effects: &TerminalEffects,
    ) -> std::result::Result<(), StepError> {
        println!(
            "\n=== run {} {} (final_step={}) ===",
            outcome.run_id, outcome.status, outcome.final_step
        );
        match outcome.status {
            TerminalStatus::Succeeded => {
                if let Some(result) = &outcome.result {
                    println!("{}", String::from_utf8_lossy(result));
                }
            }
            _ => {
                if let Some(err) = &outcome.error {
                    eprintln!("error: {err}");
                }
            }
        }
        if let Some(tx) = self.shutdown.lock().await.take() {
            let _ = tx.send(());
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "fanout-demo").await?);

    // Typed-jobs layer, sharing the queue with the workflow runtime
    // below. The runner's dispatch worker runs until shutdown.
    let mut jobs = JobRunner::builder(queue.clone(), store.clone())
        .register::<FetchPage>()
        .build();
    let jobs_handle = jobs.spawn(std::future::pending::<()>());
    let jobs = Arc::new(jobs);

    let (tx, rx) = oneshot::channel::<()>();
    let runtime = WorkflowRuntime::builder(
        queue,
        store,
        FanoutRunner { jobs },
        ShutdownOnComplete {
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

    let urls = [
        "https://example.com/a",
        "https://example.com/longer/path/b",
        "https://example.com/c",
    ];
    let handle = runtime
        .submit(RunSpec {
            input: urls.join("\n").into_bytes(),
            ..Default::default()
        })
        .await?;
    println!("submitted run {}", handle.run_id);

    worker_task.await??;
    jobs_handle.shutdown().await?;
    Ok(())
}
