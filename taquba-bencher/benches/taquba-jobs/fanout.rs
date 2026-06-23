// cargo bench -p taquba-bencher --bench fanout > fanout.csv
//
// Fan-out benchmark for typed jobs: submit N_JOBS concurrently with
// idempotency keys, await every handle, then submit the identical
// batch again so each handle short-circuits to its cached result
// blob. The cold phase measures the full round trip (idempotency
// record plus enqueue, claim, run, result-blob write, completion
// notification, result read); the resubmit phase measures the
// idempotent short-circuit that crash-resume relies on.
//
// Parameters (env vars, all optional).
//   N_JOBS              jobs per phase (default 500)
//   JOB_WORK_MS         simulated work per job execution (default 0)
//   MAX_CONCURRENT      jobs processed in parallel (default 50)
//   FLUSH_INTERVAL_MS   SlateDB WAL flush interval in ms (default 1)
//   STORE_LATENCY_MS    injected object-store latency per call (default 0).
//                       When set, the in-memory store is wrapped in
//                       object_store's ThrottledStore so every get, put,
//                       list, and delete sleeps this long before running,
//                       approximating an S3-class backend. Applies to
//                       result blobs as well as the queue.
//   STORE_JITTER_MS     random tail latency in [0, STORE_JITTER_MS] added to
//                       each write on top of STORE_LATENCY_MS (default 0).
//   STORE_URL           object-store URL (s3://bucket/prefix, gs://...,
//                       az://..., file:///abs/path) to run against
//                       instead of the in-memory store; see
//                       the crate README. Incompatible with
//                       STORE_LATENCY_MS and STORE_JITTER_MS.
//
// Output (stdout): CSV with header `phase,jobs,secs,jobs_per_sec`,
// one row per phase (`cold`, `resubmit`). Status prints go to stderr
// so stdout stays a clean data stream.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use taquba::{OpenOptions, Queue, QueueConfig};
use taquba_bencher::{env_var, init_tracing, store_from_env};
use taquba_jobs::{Job, JobContext, JobRunner};

#[derive(Debug, thiserror::Error)]
#[error("bench job error: {0}")]
struct BenchError(String);

#[derive(serde::Serialize, serde::Deserialize)]
struct BenchJob {
    idx: u32,
    work_ms: u64,
}

static EXECUTIONS: AtomicUsize = AtomicUsize::new(0);

impl Job for BenchJob {
    const NAME: &'static str = "bench.fanout";
    type Output = u32;
    type Error = BenchError;

    async fn run(&self, _ctx: JobContext<'_>) -> Result<u32, BenchError> {
        EXECUTIONS.fetch_add(1, Ordering::SeqCst);
        if self.work_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.work_ms)).await;
        }
        Ok(self.idx)
    }

    fn idempotency_key(&self) -> Option<String> {
        Some(format!("bench:{}", self.idx))
    }
}

/// Submit `n_jobs` concurrently, await every handle, and return the
/// phase duration plus how many of the handles were newly submitted.
async fn run_phase(
    runner: &JobRunner,
    n_jobs: u32,
    work_ms: u64,
) -> Result<(f64, usize), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let handles = futures_util::future::try_join_all(
        (0..n_jobs).map(|idx| runner.submit(BenchJob { idx, work_ms })),
    )
    .await?;
    let newly_submitted = handles.iter().filter(|h| h.newly_submitted()).count();
    futures_util::future::try_join_all(
        handles
            .into_iter()
            .map(|handle| async move { handle.await }),
    )
    .await?;
    Ok((start.elapsed().as_secs_f64(), newly_submitted))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let n_jobs: u32 = env_var("N_JOBS", 500);
    let job_work_ms: u64 = env_var("JOB_WORK_MS", 0);
    let max_concurrent: usize = env_var("MAX_CONCURRENT", 50).max(1);
    let flush_interval_ms: u64 = env_var("FLUSH_INTERVAL_MS", 1);
    let store_latency_ms: u64 = env_var("STORE_LATENCY_MS", 0);

    eprintln!(
        "fanout: jobs={n_jobs}, job_work={job_work_ms}ms, \
         max_concurrent={max_concurrent}, flush_interval={flush_interval_ms}ms, \
         store_latency={store_latency_ms}ms",
    );

    let store = store_from_env(store_latency_ms)?;
    let queue = Arc::new(
        Queue::open_with_options(
            store.clone(),
            "bench-db",
            OpenOptions {
                default_queue_config: QueueConfig {
                    keep_done_jobs: None,
                    ..QueueConfig::default()
                },
                flush_interval: Some(Duration::from_millis(flush_interval_ms)),
                ..OpenOptions::default()
            },
        )
        .await?,
    );

    let mut runner = JobRunner::builder()
        .queue(queue.clone())
        .object_store(store)
        .max_concurrent_jobs(max_concurrent)
        .build()?;
    runner.register::<BenchJob>();
    let worker = runner.spawn(std::future::pending::<()>());

    println!("phase,jobs,secs,jobs_per_sec");

    let (cold_secs, cold_new) = run_phase(&runner, n_jobs, job_work_ms).await?;
    let cold_executed = EXECUTIONS.load(Ordering::SeqCst);
    eprintln!(
        "cold: {n_jobs} jobs in {cold_secs:.2}s, {cold_new} newly submitted, \
         {cold_executed} executed",
    );
    println!(
        "cold,{n_jobs},{cold_secs:.3},{:.0}",
        n_jobs as f64 / cold_secs,
    );

    // The identical batch again: every handle should short-circuit to
    // the cached result blob without re-running the job.
    let (resubmit_secs, resubmit_new) = run_phase(&runner, n_jobs, job_work_ms).await?;
    let total_executed = EXECUTIONS.load(Ordering::SeqCst);
    eprintln!(
        "resubmit: {n_jobs} jobs in {resubmit_secs:.2}s, {resubmit_new} newly \
         submitted, {} re-executed",
        total_executed - cold_executed,
    );
    println!(
        "resubmit,{n_jobs},{resubmit_secs:.3},{:.0}",
        n_jobs as f64 / resubmit_secs,
    );

    worker.shutdown().await?;
    drop(runner);
    let queue =
        Arc::try_unwrap(queue).map_err(|_| "queue still has outstanding references at shutdown")?;
    queue.close().await?;
    Ok(())
}
