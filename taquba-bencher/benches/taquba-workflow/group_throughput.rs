// cargo bench -p taquba-bencher --bench group_throughput > group.csv
//
// Group throughput benchmark for typed jobs. Runs N_ITEMS jobs of a
// group through N_PHASES memoized phases that do no work, so the
// measured cost is the per-item overhead: run submission, the single
// workflow step, one memo write per phase, terminal accounting and the
// result read. Completions are counted per second.
//
// Parameters (env vars, all optional).
//   N_ITEMS             jobs in the group (default 500).
//   N_PHASES            memoized phases per item (default 3).
//   MAX_CONCURRENT      items processed in parallel (default 16).
//   FLUSH_INTERVAL_MS   SlateDB WAL flush interval in ms (default 1).
//   STORE_LATENCY_MS    injected object-store latency per call (default 0).
//                       When set, the in-memory store is wrapped in
//                       object_store's ThrottledStore so every get, put,
//                       list, and delete sleeps this long before running,
//                       approximating an S3-class backend.
//   STORE_JITTER_MS     random tail latency in [0, STORE_JITTER_MS] added to
//                       each write on top of STORE_LATENCY_MS (default 0).
//   STORE_URL           object-store URL (s3://bucket/prefix, gs://...,
//                       az://..., file:///abs/path) to run against
//                       instead of the in-memory store; see
//                       the crate README. Incompatible with
//                       STORE_LATENCY_MS and STORE_JITTER_MS.
//
// Output (stdout): CSV with header `window_sec,completed`, one row per
// second with the cumulative number of terminal items. A summary
// (items/s, succeeded / failed counts) goes to stderr so stdout stays
// a clean data stream.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::TryStreamExt;
use taquba::{OpenOptions, Queue, QueueConfig};
use taquba_bencher::{env_var, init_tracing, store_from_env};
use taquba_workflow::jobs::{Job, JobContext, JobRunner};

#[derive(serde::Serialize, serde::Deserialize)]
struct Item {
    idx: u32,
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
struct BenchError(#[from] taquba_workflow::Error);

/// The phase count, registered as handler state.
struct Phases(usize);

impl Job for Item {
    const NAME: &'static str = "bench.phases";
    type Output = u32;
    type Error = BenchError;

    async fn run(&self, ctx: JobContext<'_>) -> Result<u32, BenchError> {
        let n_phases = ctx.state::<Phases>().0;
        let mut acc = self.idx;
        for phase in 0..n_phases {
            let value = acc;
            acc = ctx
                .memo
                .memoized(&format!("phase-{phase}"), async move {
                    Ok::<_, BenchError>(value.wrapping_add(1))
                })
                .await?;
        }
        Ok(acc)
    }
}

/// Cumulative completions per elapsed second, from the completion
/// instants of a run.
fn progress_rows(started: Instant, completions: &[Instant]) -> Vec<(u64, usize)> {
    let mut rows = Vec::new();
    let mut completed = 0;
    for at in completions {
        completed += 1;
        let sec = at.duration_since(started).as_secs();
        match rows.last_mut() {
            Some((last, count)) if *last == sec => *count = completed,
            _ => rows.push((sec, completed)),
        }
    }
    rows
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let n_items: usize = env_var("N_ITEMS", 500);
    let n_phases: usize = env_var("N_PHASES", 3);
    let max_concurrent: usize = env_var("MAX_CONCURRENT", 16).max(1);
    let flush_interval_ms: u64 = env_var("FLUSH_INTERVAL_MS", 1);
    let store_latency_ms: u64 = env_var("STORE_LATENCY_MS", 0);

    eprintln!(
        "group_throughput: items={n_items}, phases={n_phases}, \
         max_concurrent={max_concurrent}, flush_interval={flush_interval_ms}ms, \
         store_latency={store_latency_ms}ms",
    );

    let store = store_from_env(store_latency_ms)?;
    let queue = Arc::new(
        Queue::open_with_options(
            store.clone(),
            "bench-db",
            OpenOptions::default()
                .default_queue_config(QueueConfig::default().keep_done_jobs(None))
                .flush_interval(Some(Duration::from_millis(flush_interval_ms))),
        )
        .await?,
    );

    let mut runner = JobRunner::builder(queue.clone(), store)
        .register::<Item>()
        .state(Phases(n_phases))
        .max_concurrent_jobs(max_concurrent)
        .build();
    let worker = runner.spawn(std::future::pending::<()>());

    let started = Instant::now();
    let group = runner.new_group::<Item>();
    group
        .submit((0..n_items as u32).map(|idx| Item { idx }))
        .await?;
    let (mut succeeded, mut failed) = (0usize, 0usize);
    let mut completions = Vec::with_capacity(n_items);
    {
        let mut results = std::pin::pin!(group.results().await?);
        while let Some(result) = results.try_next().await? {
            completions.push(Instant::now());
            match result.result {
                Ok(_) => succeeded += 1,
                Err(_) => failed += 1,
            }
            if completions.len() % 100 == 0 {
                eprintln!(
                    "  t={}s completed={}/{n_items}",
                    started.elapsed().as_secs(),
                    completions.len()
                );
            }
        }
    }
    let elapsed = started.elapsed();
    worker.shutdown().await?;

    println!("window_sec,completed");
    for (sec, completed) in progress_rows(started, &completions) {
        println!("{sec},{completed}");
    }

    let secs = elapsed.as_secs_f64();
    eprintln!(
        "summary: {n_items} items ({succeeded} succeeded, {failed} failed) in {secs:.2}s \
         ({:.0} items/s)",
        n_items as f64 / secs,
    );

    drop(group);
    drop(runner);
    let queue =
        Arc::try_unwrap(queue).map_err(|_| "queue still has outstanding references at shutdown")?;
    queue.close().await?;
    Ok(())
}
