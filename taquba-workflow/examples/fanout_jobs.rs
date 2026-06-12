//! Inner fan-out composing `taquba-workflow` with `taquba-jobs`: a
//! workflow step submits N typed jobs to a shared [`JobRunner`], joins
//! their typed results, and memoizes the aggregate so a step retry
//! does not re-submit the fan-out.
//!
//! - **Step 0 (`fetch`)**: submits one `FetchPage` job per URL in the
//!   run input, awaits every handle, and stores the joined results in
//!   the step's memo before continuing.
//! - **Step 1 (`report`)**: formats the aggregate into the run's final
//!   result.
//!
//! Two mechanisms keep the fan-out safe under at-least-once delivery:
//!
//! - The joined aggregate is memoized under the step's memo key, so a
//!   step retry after the join completed replays the cached aggregate
//!   instead of re-submitting any jobs.
//! - Each job carries an idempotency key, so a step retry that crashed
//!   mid-fan-out (some jobs submitted, memo not yet written) collapses
//!   its re-submissions onto the in-flight or completed jobs instead
//!   of running them twice.
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
use taquba_jobs::{Job, JobContext, JobRunner};
use taquba_workflow::{
    RunOutcome, RunSpec, Step, StepError, StepOutcome, StepRunner, TerminalHook, TerminalStatus,
    WorkflowRuntime,
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

/// Memo key holding the joined fan-out results for step 0.
const FETCH_MEMO_KEY: &str = "fetch-results";

struct FanoutRunner {
    jobs: Arc<JobRunner>,
}

impl StepRunner for FanoutRunner {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        match step.step_number {
            STEP_FETCH => {
                let aggregate = if let Some(cached) = step.memo.get(FETCH_MEMO_KEY).await? {
                    println!("[step 0] memo hit; fan-out not re-submitted");
                    cached
                } else {
                    let input = std::str::from_utf8(&step.payload)
                        .map_err(|e| StepError::permanent(format!("non-utf8 input: {e}")))?;
                    let urls: Vec<&str> = input.lines().collect();

                    // Fan out: one typed job per URL on the shared runner.
                    let mut handles = Vec::with_capacity(urls.len());
                    for url in &urls {
                        let handle = self
                            .jobs
                            .submit(FetchPage {
                                url: (*url).to_string(),
                            })
                            .await
                            .map_err(|e| StepError::transient(format!("submit failed: {e}")))?;
                        handles.push(handle);
                    }

                    // Join: await every typed result.
                    let mut lines = Vec::with_capacity(urls.len());
                    let mut total: u64 = 0;
                    for (url, handle) in urls.iter().zip(handles) {
                        let bytes = handle
                            .await
                            .map_err(|e| StepError::transient(format!("fetch failed: {e}")))?;
                        println!("[step 0] fetched {url}: {bytes} bytes");
                        lines.push(format!("{url}: {bytes} bytes"));
                        total += bytes;
                    }
                    lines.push(format!("total: {total} bytes"));

                    // Memoize the aggregate before continuing, so a retry
                    // of this step replays it instead of re-submitting.
                    let aggregate = lines.join("\n").into_bytes();
                    step.memo.put(FETCH_MEMO_KEY, &aggregate).await?;
                    aggregate
                };
                Ok(StepOutcome::Continue { payload: aggregate })
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
    async fn on_termination(&self, outcome: &RunOutcome) {
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
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let queue = Arc::new(Queue::open(store.clone(), "fanout-demo").await?);

    // Typed-jobs layer, sharing the queue with the workflow runtime
    // below. The runner's dispatch worker runs until shutdown.
    let mut jobs = JobRunner::builder()
        .queue(queue.clone())
        .object_store(store.clone())
        .build()?;
    jobs.register::<FetchPage>();
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
