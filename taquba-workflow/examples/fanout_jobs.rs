//! Inner fan-out composing workflow steps with a job group: a workflow step
//! submits one typed job per URL as the members of a group named after the run.
//! The run waits for the group without a worker, and the next step joins the
//! typed results into a report.
//!
//! - **Step 0 (`fetch`)**: submits one `FetchPage` job per URL in the run
//!   input to the group `fetch-{run_id}` and continues on the group.
//! - **Step 1 (`report`)**: runs once the last job terminates, joins the
//!   recorded results and formats them into the run's final result.
//!
//! The group keeps the fan-out safe under at-least-once delivery: a retry of
//! step 0 submits the same group again, which runs only the members that did
//! not succeed.
//!
//! Both layers consume one shared `Arc<Queue>`: the workflow runtime and the
//! job runner are consumers of the same store, in the same process, on
//! different logical queues.
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
use taquba_workflow::{RunId, RunSpec, Step, StepError, StepOutcome, StepRunner, WorkflowRuntime};

/// Mocked page fetch. The example derives a deterministic byte count from the
/// URL where a real version issues an HTTP request.
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

/// The longest wait of the run for its fetches.
const FETCH_TIMEOUT: Duration = Duration::from_secs(600);

struct FanoutRunner {
    jobs: JobRunner,
}

impl StepRunner for FanoutRunner {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        let group_id = RunId::new(format!("fetch-{}", step.run_id))?;
        let group = self.jobs.group::<FetchPage>(group_id.clone());
        match step.step_number {
            STEP_FETCH => {
                let input = std::str::from_utf8(&step.payload)
                    .map_err(|e| StepError::permanent(format!("non-utf8 input: {e}")))?;

                // Fan out: one typed job per URL, as one group per run.
                group
                    .submit(input.lines().map(|url| FetchPage {
                        url: url.to_string(),
                    }))
                    .await?;
                println!("[step 0] submitted the fetches, waiting for the group");

                // Wait for the last fetch without holding a worker.
                Ok(StepOutcome::continue_on_group(
                    Vec::new(),
                    group_id,
                    FETCH_TIMEOUT,
                ))
            }
            STEP_REPORT => {
                if group.status().await?.pending > 0 {
                    return Err(StepError::permanent("the fetches outlasted the timeout"));
                }

                // Join: every typed result, recorded at its termination, in
                // submission order.
                let mut lines = vec!["fetch report".to_string(), "------------".to_string()];
                let mut total: u64 = 0;
                for member in group.join().await? {
                    let bytes = member
                        .result
                        .map_err(|e| StepError::permanent(format!("fetch failed: {e}")))?;
                    println!("[step 1] fetched {}: {bytes} bytes", member.key);
                    lines.push(format!("{}: {bytes} bytes", member.key));
                    total += bytes;
                }
                lines.push(format!("total: {total} bytes"));
                Ok(StepOutcome::Succeed {
                    result: lines.join("\n").into_bytes(),
                })
            }
            other => Err(StepError::permanent(format!(
                "unexpected step number {other}"
            ))),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "fanout-demo").await?);

    // Typed-jobs layer, sharing the queue with the workflow runtime below. The
    // runner's dispatch worker runs until shutdown.
    let jobs = JobRunner::builder(queue.clone(), store.clone())
        .register::<FetchPage>()
        .build();
    let jobs_handle = jobs.spawn(std::future::pending::<()>());

    let runtime = WorkflowRuntime::builder(queue, store, FanoutRunner { jobs }).build()?;
    let worker = runtime.spawn(std::future::pending());

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

    let run = runtime.wait(&handle.run_id).await?;
    let termination = &run.termination;
    println!(
        "\n=== run {} {} (final_step={}) ===",
        run.run_id, termination.status, termination.final_step.number
    );
    match (&run.result, &termination.error) {
        (Some(result), _) => println!("{}", String::from_utf8_lossy(result)),
        (None, Some(err)) => eprintln!("error: {err}"),
        (None, None) => {}
    }

    worker.shutdown().await?;
    jobs_handle.shutdown().await?;
    Ok(())
}
