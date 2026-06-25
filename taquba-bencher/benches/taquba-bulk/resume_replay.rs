// cargo bench -p taquba-bencher --bench resume_replay > resume.csv
//
// Resume benchmark for per-item memoization. Every item's pipeline
// runs N_PHASES memoized phases of PHASE_WORK_MS simulated work each;
// when FAIL_AT is above 0, each item fails transiently on its first
// attempt after completing FAIL_AT phases, so the retry re-enters the
// pipeline and resumes through memo hits instead of re-paying the
// completed phases. Setting MEMO=0 runs the identical workload
// without memoization, so the difference between the two runs is the
// work that memoization saves a retried item.
//
// Parameters (env vars, all optional).
//   N_ITEMS             input items in the batch (default 200).
//   N_PHASES            memoized phases per item (default 4).
//   FAIL_AT             phases each item completes before its injected
//                       first-attempt transient failure; 0 disables
//                       the injection (default 2). Must be at most
//                       N_PHASES.
//   PHASE_WORK_MS       simulated work per phase execution (default 20).
//   MEMO                1 wraps phases in BulkCtx::memoized, 0 runs
//                       them bare (default 1).
//   MAX_CONCURRENT      items processed in parallel (default 16).
//   FLUSH_INTERVAL_MS   SlateDB WAL flush interval in ms (default 1).
//   STORE_LATENCY_MS    injected object-store latency per call (default 0).
//                       When set, the in-memory store is wrapped in
//                       object_store's ThrottledStore so every get, put,
//                       list, and delete sleeps this long before running,
//                       approximating an S3-class backend. Applies to
//                       memo reads and writes as well as the queue.
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
// (items/s, phase executions against the no-retry floor) goes to
// stderr so stdout stays a clean data stream.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
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

struct ResumePipeline {
    n_phases: usize,
    fail_at: usize,
    phase_work: Duration,
    memoize: bool,
    /// Number of times a phase body actually ran (memo hits excluded).
    executions: Arc<AtomicUsize>,
    /// Items that have already taken their injected failure.
    failed_once: Mutex<HashSet<u32>>,
}

impl ResumePipeline {
    async fn run_phase(
        &self,
        ctx: &BulkCtx<Item>,
        phase: usize,
        value: u32,
    ) -> Result<u32, StepError> {
        let executions = self.executions.clone();
        let work = self.phase_work;
        let body = async move {
            executions.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(work).await;
            Ok::<_, StepError>(value.wrapping_add(1))
        };
        if self.memoize {
            ctx.memoized(&format!("phase-{phase}"), body).await
        } else {
            body.await
        }
    }
}

impl Pipeline for ResumePipeline {
    type Input = Item;
    type Output = u32;
    type Error = StepError;

    async fn run(&self, ctx: &BulkCtx<Item>) -> Result<u32, StepError> {
        let mut acc = ctx.input.idx;
        for phase in 0..self.n_phases {
            if self.fail_at > 0
                && phase == self.fail_at
                && self.failed_once.lock().unwrap().insert(ctx.input.idx)
            {
                return Err(StepError::transient("injected first-attempt failure"));
            }
            acc = self.run_phase(ctx, phase, acc).await?;
        }
        Ok(acc)
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let n_items: usize = env_var("N_ITEMS", 200);
    let n_phases: usize = env_var("N_PHASES", 4);
    let fail_at: usize = env_var("FAIL_AT", 2);
    let phase_work_ms: u64 = env_var("PHASE_WORK_MS", 20);
    let memoize: bool = env_var::<u8>("MEMO", 1) != 0;
    let max_concurrent: usize = env_var("MAX_CONCURRENT", 16).max(1);
    let flush_interval_ms: u64 = env_var("FLUSH_INTERVAL_MS", 1);
    let store_latency_ms: u64 = env_var("STORE_LATENCY_MS", 0);
    if fail_at > n_phases {
        return Err("FAIL_AT must be at most N_PHASES".into());
    }

    eprintln!(
        "resume_replay: items={n_items}, phases={n_phases}, fail_at={fail_at}, \
         phase_work={phase_work_ms}ms, memo={memoize}, \
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
                    // Zero backoff: a retried item goes straight back to
                    // pending, so the measured resume cost is the replay
                    // itself, not the backoff wait.
                    retry_backoff_base: Duration::ZERO,
                    ..QueueConfig::default()
                },
                flush_interval: Some(Duration::from_millis(flush_interval_ms)),
                ..OpenOptions::default()
            },
        )
        .await?,
    );

    let executions = Arc::new(AtomicUsize::new(0));
    let bulk = Arc::new(
        Bulk::builder(
            queue.clone(),
            store,
            ResumePipeline {
                n_phases,
                fail_at,
                phase_work: Duration::from_millis(phase_work_ms),
                memoize,
                executions: executions.clone(),
                failed_once: Mutex::new(HashSet::new()),
            },
        )
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

    // Each phase execution memoization avoided appears as the
    // difference between the executions a retry-free run needs and
    // the executions this run performed.
    let floor = n_items * n_phases;
    let executed = executions.load(Ordering::SeqCst);
    let secs = report.elapsed.as_secs_f64();
    eprintln!(
        "summary: {} items ({} succeeded, {} failed) in {secs:.2}s ({:.0} items/s); \
         phase executions {executed} against a no-retry floor of {floor} \
         ({} re-executed)",
        report.total,
        report.succeeded,
        report.failed,
        report.total as f64 / secs,
        executed - floor,
    );

    drop(bulk);
    let queue =
        Arc::try_unwrap(queue).map_err(|_| "queue still has outstanding references at shutdown")?;
    queue.close().await?;
    Ok(())
}
