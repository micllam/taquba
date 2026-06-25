// cargo bench -p taquba-bencher --bench bulk_throughput > bulk.csv
//
// Batch throughput benchmark for the bulk orchestrator. Runs N_ITEMS
// items through a pipeline of N_PHASES memoized phases that do no
// work, so the measured cost is the per-item overhead: run
// submission, the single workflow step, one memo write per phase, and
// terminal accounting. A watcher samples progress once per second.
//
// Parameters (env vars, all optional).
//   N_ITEMS             input items in the batch (default 500).
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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use taquba::{OpenOptions, Queue, QueueConfig};
use taquba_bencher::{env_var, init_tracing, store_from_env};
use taquba_bulk::{Bulk, BulkCtx, Pipeline};
use taquba_workflow::StepError;

#[derive(serde::Serialize, serde::Deserialize)]
struct Item {
    idx: u32,
}

struct PhasesPipeline {
    n_phases: usize,
}

impl Pipeline for PhasesPipeline {
    type Input = Item;
    type Output = u32;
    type Error = StepError;

    async fn run(&self, ctx: &BulkCtx<Item>) -> Result<u32, StepError> {
        let mut acc = ctx.input.idx;
        for phase in 0..self.n_phases {
            let value = acc;
            acc = ctx
                .memoized(&format!("phase-{phase}"), async move {
                    Ok::<_, StepError>(value.wrapping_add(1))
                })
                .await?;
        }
        Ok(acc)
    }
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
        "bulk_throughput: items={n_items}, phases={n_phases}, \
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

    let bulk = Arc::new(
        Bulk::builder(queue.clone(), store, PhasesPipeline { n_phases })
            .max_concurrent(max_concurrent)
            .build(),
    );

    // Watcher: sample cumulative progress once per second.
    let progress_rows: Arc<Mutex<Vec<(u64, usize)>>> = Arc::new(Mutex::new(Vec::new()));
    let watcher = {
        let bulk = bulk.clone();
        let progress_rows = progress_rows.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.tick().await; // skip immediate first tick
            loop {
                tick.tick().await;
                let p = bulk.progress();
                let sec = p.elapsed.as_secs();
                progress_rows.lock().unwrap().push((sec, p.completed));
                eprintln!(
                    "  t={sec}s completed={}/{} ({:.0}/s)",
                    p.completed, p.total, p.rate_per_sec,
                );
            }
        })
    };

    let inputs = (0..n_items as u32).map(|idx| Item { idx });
    let report = bulk.run(inputs).await?;
    watcher.abort();
    // Await the aborted watcher so its `bulk` clone is dropped before the
    // queue refcount check below. `abort()` only requests cancellation, so
    // without this the task can still hold a `bulk` clone (and through it a
    // `queue` clone) when `Arc::try_unwrap` runs.
    let _ = watcher.await;

    println!("window_sec,completed");
    for (sec, completed) in progress_rows.lock().unwrap().iter() {
        println!("{sec},{completed}");
    }

    let secs = report.elapsed.as_secs_f64();
    eprintln!(
        "summary: {} items ({} succeeded, {} failed) in {secs:.2}s ({:.0} items/s)",
        report.total,
        report.succeeded,
        report.failed,
        report.total as f64 / secs,
    );

    drop(bulk);
    let queue =
        Arc::try_unwrap(queue).map_err(|_| "queue still has outstanding references at shutdown")?;
    queue.close().await?;
    Ok(())
}
