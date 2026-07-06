// cargo bench -p taquba-bencher --bench settle_ab > settle_ab.csv
//
// A/B benchmark for deferred (non-durable) settlement. Runs the same
// prefill-and-drain workload once per arm: `durable` (the default
// mode: every ack awaits WAL durability) and `deferred`
// (`QueueConfig::durable_settlement = false`: acks return after the
// in-memory commit and the periodic WAL flush persists them later).
// Arms run sequentially, each on its own store path, so they share no
// state.
//
// Workers are sequential actors: claim one job, simulate PROC_MS of
// work, ack, claim the next. Per-actor throughput is therefore bounded
// by the per-job stall (claim + processing + settlement), and deferred
// settlement aims to remove the settlement term. The primary result is
// the jobs-per-actor multiple between the arms at fixed concurrency.
//
// Parameters (env vars, all optional).
//   ARMS                comma-separated list of `durable` / `deferred`
//                       arms to run (default both). Naming one arm
//                       runs the arms as separate processes, e.g. on a
//                       cloud host; naming an arm twice measures
//                       run-to-run variance. Every run gets its own
//                       store path.
//   N_JOBS              jobs enqueued before each arm's drain starts
//                       (default 5_000).
//   N_WORKERS           sequential claim/ack actors (default 8).
//   PROC_MS             simulated per-job processing time in ms
//                       (default 0: the fast-job regime where the
//                       settlement stall dominates).
//   PAYLOAD_BYTES       per-job payload size (default 64).
//   LEASE_SEC           claim lease in seconds (default 60). Must
//                       exceed PROC_MS plus the settlement stall.
//   FLUSH_INTERVAL_MS   SlateDB WAL flush interval in ms (default 1).
//                       The durable arm's settlement stall is roughly
//                       one flush wait plus one PUT, so this is a
//                       primary parameter to sweep alongside store
//                       latency.
//   STORE_LATENCY_MS    injected object-store latency per call (default 0).
//                       When set, the in-memory store is wrapped in
//                       object_store's ThrottledStore so every get, put,
//                       list, and delete sleeps this long before running,
//                       approximating an S3-class backend.
//   STORE_JITTER_MS     random tail latency in [0, STORE_JITTER_MS] added
//                       to each write on top of STORE_LATENCY_MS (default 0).
//   STORE_URL           object-store URL (s3://bucket/prefix, gs://...,
//                       az://..., file:///abs/path) to run against
//                       instead of the in-memory store; see the crate
//                       README. Incompatible with STORE_LATENCY_MS and
//                       STORE_JITTER_MS.
//
// Output (stdout): CSV with header
// `arm,n_jobs,n_workers,proc_ms,drain_secs,jobs_per_sec,jobs_per_actor_sec,claim_p50_us,claim_p99_us,ack_p50_us,ack_p99_us`.
// One row per arm. `drain_secs` is measured from drain start (prefill
// is excluded) to the last ack's completion. `ack_*` measures the
// settlement stall directly; `jobs_per_actor_sec` is the per-actor
// throughput the arms are compared on. When both arms run, the
// jobs-per-actor multiple (deferred over durable, first occurrence of
// each) is printed to stderr. Status and progress prints go to stderr
// so stdout stays a clean data stream.

use std::sync::Arc;
use std::time::{Duration, Instant};

use taquba::object_store::ObjectStore;
use taquba::{OpenOptions, Queue, QueueConfig};
use taquba_bencher::{env_var, init_tracing, pct, store_from_env};

const QUEUE_NAME: &str = "bench";

/// Watcher poll interval: how often stats are sampled for progress
/// output and the drain check.
const WATCHER_TICK: Duration = Duration::from_secs(1);

/// Run parameters, identical for every arm.
#[derive(Clone, Copy)]
struct ArmCfg {
    n_jobs: usize,
    n_workers: usize,
    proc_ms: u64,
    payload_bytes: usize,
    lease: Duration,
    flush_ms: u64,
}

/// One arm's outcome: ack count, drain wall time and per-operation
/// latency samples.
struct ArmResult {
    done: u64,
    drain_secs: f64,
    claim_us: Vec<u64>,
    ack_us: Vec<u64>,
}

/// Run one arm on `db_path`: prefill N_JOBS, drain with N_WORKERS
/// sequential actors, return per-operation latencies and the drain
/// wall time.
async fn run_arm(
    store: Arc<dyn ObjectStore>,
    db_path: String,
    durable_settlement: bool,
    cfg: &ArmCfg,
) -> Result<ArmResult, Box<dyn std::error::Error>> {
    let ArmCfg {
        n_jobs,
        n_workers,
        proc_ms,
        payload_bytes,
        lease,
        flush_ms,
    } = *cfg;
    let queue = Arc::new(
        Queue::open_with_options(
            store,
            &db_path,
            OpenOptions {
                default_queue_config: QueueConfig {
                    keep_done_jobs: None,
                    durable_settlement,
                    ..QueueConfig::default()
                },
                flush_interval: Some(Duration::from_millis(flush_ms)),
                ..OpenOptions::default()
            },
        )
        .await?,
    );

    eprintln!("  enqueuing {n_jobs} jobs (batch)...");
    let payload_template = vec![0u8; payload_bytes];
    let payloads: Vec<Vec<u8>> = (0..n_jobs).map(|_| payload_template.clone()).collect();
    let prefill_start = Instant::now();
    queue.enqueue_batch(QUEUE_NAME, payloads).await?;
    eprintln!(
        "  enqueue done in {:.1}s",
        prefill_start.elapsed().as_secs_f64(),
    );

    let bench_start = Instant::now();

    // Each entry is (elapsed_us_at_ack_completion, claim_us, ack_us).
    type Sample = (u64, u64, u64);
    let mut worker_handles = Vec::with_capacity(n_workers);
    for worker_idx in 0..n_workers {
        let queue = queue.clone();
        worker_handles.push(tokio::spawn(async move {
            let mut samples: Vec<Sample> = Vec::with_capacity(4096);
            loop {
                let claim_start = Instant::now();
                match queue.claim(QUEUE_NAME, lease).await {
                    Ok(Some(job)) => {
                        let claim_us = claim_start.elapsed().as_micros() as u64;
                        if proc_ms > 0 {
                            tokio::time::sleep(Duration::from_millis(proc_ms)).await;
                        }
                        let ack_start = Instant::now();
                        if let Err(e) = queue.ack(&job).await {
                            eprintln!("  worker {worker_idx}: ack error: {e}");
                            break;
                        }
                        let ack_us = ack_start.elapsed().as_micros() as u64;
                        let done_us = bench_start.elapsed().as_micros() as u64;
                        samples.push((done_us, claim_us, ack_us));
                    }
                    Ok(None) => {
                        // The arm pre-fills before workers start and never
                        // re-enqueues, so an empty observation is terminal.
                        break;
                    }
                    Err(e) => {
                        eprintln!("  worker {worker_idx}: claim error: {e}");
                        break;
                    }
                }
            }
            samples
        }));
    }

    // Progress watcher: prints per-second progress and exits once
    // stats report the queue drained. Workers self-terminate on an
    // empty claim, so there is no shutdown signal to coordinate.
    let watcher = {
        let queue = queue.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(WATCHER_TICK);
            tick.tick().await; // skip immediate first tick
            loop {
                tick.tick().await;
                let stats = match queue.stats(QUEUE_NAME).await {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let elapsed = bench_start.elapsed().as_secs();
                eprintln!(
                    "  t={elapsed}s pending={} claimed={} done={}",
                    stats.pending, stats.claimed, stats.done,
                );
                if stats.pending == 0 && stats.claimed == 0 {
                    eprintln!("  drain complete");
                    return;
                }
            }
        })
    };

    let mut claim_us = Vec::with_capacity(n_jobs);
    let mut ack_us = Vec::with_capacity(n_jobs);
    let mut last_done_us: u64 = 0;
    let mut done: u64 = 0;
    for (idx, handle) in worker_handles.into_iter().enumerate() {
        match handle.await {
            Ok(samples) => {
                for (done_us, c, a) in samples {
                    last_done_us = last_done_us.max(done_us);
                    claim_us.push(c);
                    ack_us.push(a);
                    done += 1;
                }
            }
            Err(e) => eprintln!("  worker {idx}: task join error: {e}"),
        }
    }
    let _ = watcher.await;

    let queue =
        Arc::try_unwrap(queue).map_err(|_| "queue still has outstanding references at shutdown")?;
    queue.close().await?;

    Ok(ArmResult {
        done,
        drain_secs: last_done_us as f64 / 1e6,
        claim_us,
        ack_us,
    })
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let arms_spec = std::env::var("ARMS").unwrap_or_else(|_| "durable,deferred".into());
    let arms: Vec<&str> = arms_spec
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    for arm in &arms {
        if !matches!(*arm, "durable" | "deferred") {
            return Err(format!("unknown arm '{arm}' in ARMS, expected durable|deferred").into());
        }
    }
    if arms.is_empty() {
        return Err("ARMS has no valid arms".into());
    }

    let cfg = ArmCfg {
        n_jobs: env_var("N_JOBS", 5_000),
        n_workers: env_var("N_WORKERS", 8).max(1),
        proc_ms: env_var("PROC_MS", 0),
        payload_bytes: env_var("PAYLOAD_BYTES", 64),
        lease: Duration::from_secs(env_var("LEASE_SEC", 60)),
        flush_ms: env_var("FLUSH_INTERVAL_MS", 1),
    };
    let store_latency_ms: u64 = env_var("STORE_LATENCY_MS", 0);

    eprintln!(
        "settle_ab: arms=[{}], n_jobs={}, workers={}, proc={}ms, payload={}B, \
         flush_interval={}ms, store_latency={store_latency_ms}ms",
        arms.join(","),
        cfg.n_jobs,
        cfg.n_workers,
        cfg.proc_ms,
        cfg.payload_bytes,
        cfg.flush_ms,
    );

    let store = store_from_env(store_latency_ms)?;

    println!(
        "arm,n_jobs,n_workers,proc_ms,drain_secs,jobs_per_sec,jobs_per_actor_sec,\
         claim_p50_us,claim_p99_us,ack_p50_us,ack_p99_us"
    );
    let mut per_actor: Vec<(String, f64)> = Vec::new();
    for (run_idx, arm) in arms.iter().copied().enumerate() {
        let durable_settlement = arm == "durable";
        eprintln!("arm {arm}: starting");
        // The path carries the run index so a repeated arm (a
        // run-to-run variance check) runs on a fresh store rather than
        // reopening the previous run's state.
        let mut result = run_arm(
            store.clone(),
            format!("bench-db-{run_idx}-{arm}"),
            durable_settlement,
            &cfg,
        )
        .await?;
        result.claim_us.sort_unstable();
        result.ack_us.sort_unstable();
        let jobs_per_sec = if result.drain_secs > 0.0 {
            result.done as f64 / result.drain_secs
        } else {
            0.0
        };
        let jobs_per_actor_sec = jobs_per_sec / cfg.n_workers as f64;
        let (claim_p50, claim_p99, ack_p50, ack_p99) = if result.ack_us.is_empty() {
            (0, 0, 0, 0)
        } else {
            (
                pct(&result.claim_us, 50),
                pct(&result.claim_us, 99),
                pct(&result.ack_us, 50),
                pct(&result.ack_us, 99),
            )
        };
        println!(
            "{arm},{},{},{},{:.3},{jobs_per_sec:.1},{jobs_per_actor_sec:.2},\
             {claim_p50},{claim_p99},{ack_p50},{ack_p99}",
            result.done, cfg.n_workers, cfg.proc_ms, result.drain_secs,
        );
        per_actor.push((arm.to_string(), jobs_per_actor_sec));
    }

    if let (Some(durable), Some(deferred)) = (
        per_actor
            .iter()
            .find(|(arm, _)| arm == "durable")
            .map(|(_, v)| *v),
        per_actor
            .iter()
            .find(|(arm, _)| arm == "deferred")
            .map(|(_, v)| *v),
    ) {
        eprintln!(
            "jobs-per-actor multiple (deferred/durable): {:.2}",
            deferred / durable,
        );
    }
    Ok(())
}
