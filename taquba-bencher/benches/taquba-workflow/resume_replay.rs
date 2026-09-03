// cargo bench -p taquba-bencher --bench resume_replay > resume.csv
//
// Resume benchmark for per-item memoization. Every job of a group
// runs N_PHASES memoized phases of PHASE_WORK_MS simulated work each;
// when FAIL_AT is above 0, each job fails transiently on its first
// attempt after completing FAIL_AT phases, so the retry re-enters the
// handler and resumes through memo hits without re-paying the
// completed phases. Setting MEMO=0 runs the identical workload
// without memoization, so the difference between the two runs is the
// work that memoization saves a retried item.
//
// Parameters (env vars, all optional).
//   N_ITEMS             jobs in the group (default 200).
//   N_PHASES            memoized phases per item (default 4).
//   FAIL_AT             phases each item completes before its injected
//                       first-attempt transient failure; 0 disables
//                       the injection (default 2). Must be at most
//                       N_PHASES.
//   PHASE_WORK_MS       simulated work per phase execution (default 20).
//   MEMO                1 wraps phases in Memo::memoized, 0 runs
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
enum BenchError {
    #[error("{0}")]
    Transient(&'static str),
    #[error(transparent)]
    Runtime(#[from] taquba_workflow::Error),
}

/// The workload settings and counters, registered as handler state.
struct Resume {
    n_phases: usize,
    fail_at: usize,
    phase_work: Duration,
    memoize: bool,
    /// Number of times a phase body ran (memo hits excluded).
    executions: AtomicUsize,
    /// Items that have already taken their injected failure.
    failed_once: Mutex<HashSet<u32>>,
}

impl Resume {
    async fn run_phase(
        &self,
        ctx: &JobContext<'_>,
        phase: usize,
        value: u32,
    ) -> Result<u32, BenchError> {
        let work = self.phase_work;
        let body = async move {
            self.executions.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(work).await;
            Ok::<_, BenchError>(value.wrapping_add(1))
        };
        if self.memoize {
            ctx.memo.memoized(&format!("phase-{phase}"), body).await
        } else {
            body.await
        }
    }
}

impl Job for Item {
    const NAME: &'static str = "bench.resume";
    type Output = u32;
    type Error = BenchError;

    async fn run(&self, ctx: JobContext<'_>) -> Result<u32, BenchError> {
        let resume = ctx.state::<Arc<Resume>>();
        let mut acc = self.idx;
        for phase in 0..resume.n_phases {
            if resume.fail_at > 0
                && phase == resume.fail_at
                && resume.failed_once.lock().unwrap().insert(self.idx)
            {
                return Err(BenchError::Transient("injected first-attempt failure"));
            }
            acc = resume.run_phase(&ctx, phase, acc).await?;
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
            OpenOptions::default()
                .default_queue_config(
                    QueueConfig::default()
                        .keep_done_jobs(None)
                        // Zero backoff: a retried item goes straight back to
                        // pending, so the measured resume cost is the replay
                        // itself, not the backoff wait.
                        .retry_backoff_base(Duration::ZERO),
                )
                .flush_interval(Some(Duration::from_millis(flush_interval_ms))),
        )
        .await?,
    );

    let resume = Arc::new(Resume {
        n_phases,
        fail_at,
        phase_work: Duration::from_millis(phase_work_ms),
        memoize,
        executions: AtomicUsize::new(0),
        failed_once: Mutex::new(HashSet::new()),
    });
    let mut runner = JobRunner::builder(queue.clone(), store)
        .register::<Item>()
        .state(resume.clone())
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

    // Each phase execution memoization avoided appears as the
    // difference between the executions a retry-free run needs and
    // the executions this run performed.
    let floor = n_items * n_phases;
    let executed = resume.executions.load(Ordering::SeqCst);
    let secs = elapsed.as_secs_f64();
    eprintln!(
        "summary: {n_items} items ({succeeded} succeeded, {failed} failed) in {secs:.2}s \
         ({:.0} items/s); phase executions {executed} against a no-retry floor of {floor} \
         ({} re-executed)",
        n_items as f64 / secs,
        executed - floor,
    );

    drop(group);
    drop(runner);
    let queue =
        Arc::try_unwrap(queue).map_err(|_| "queue still has outstanding references at shutdown")?;
    queue.close().await?;
    Ok(())
}
