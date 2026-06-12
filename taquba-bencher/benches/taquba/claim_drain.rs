// cargo bench -p taquba-bencher --bench claim_drain > drain.csv
//
// Drain-shape benchmark for the claim path. Pre-fills the queue with
// N_JOBS jobs, then spawns N_WORKERS workers that drain it. Measures
// per-claim latency and emits a per-second time series of p50/p95/p99
// in microseconds.
//
// Parameters (env vars, all optional).
//   N_JOBS              jobs enqueued before the drain starts (default 5_000)
//   N_WORKERS           concurrent claim/ack tasks (default 50)
//   PAYLOAD_BYTES       per-job payload size (default 64)
//   FLUSH_INTERVAL_MS   SlateDB WAL flush interval in ms (default 1)
//                       Lower than slatedb's 100ms default so the per-
//                       commit floor doesn't mask tombstone-scan time.
//   STORE_LATENCY_MS    injected object-store latency per call (default 0).
//                       When set, the in-memory store is wrapped in
//                       object_store's ThrottledStore so every get, put,
//                       list, and delete sleeps this long before running,
//                       approximating an S3-class backend.
//   STORE_URL           object-store URL (s3://bucket/prefix, gs://...,
//                       az://..., file:///abs/path) to run against
//                       instead of the in-memory store; see
//                       the crate README. Incompatible with
//                       STORE_LATENCY_MS.
//
// Output (stdout): CSV with header `window_sec,n_claims,p50_us,p95_us,p99_us`.
// Status / progress prints go to stderr so stdout stays a clean data stream.

use std::sync::Arc;
use std::time::{Duration, Instant};

use taquba::{OpenOptions, Queue, QueueConfig};
use taquba_bencher::{env_var, init_tracing, pct, store_from_env};

const QUEUE_NAME: &str = "bench";

/// Lease held while a worker has a job claimed. Long enough that an
/// idle scheduler tick during the bench never lets a lease expire.
const LEASE: Duration = Duration::from_secs(5);
/// Watcher poll interval: how often we read `stats()` to decide
/// whether the drain has finished.
const WATCHER_TICK: Duration = Duration::from_secs(1);

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let n_jobs: usize = env_var("N_JOBS", 5_000);
    let n_workers: usize = env_var("N_WORKERS", 50);
    let payload_bytes: usize = env_var("PAYLOAD_BYTES", 64);
    let flush_interval_ms: u64 = env_var("FLUSH_INTERVAL_MS", 1);
    let store_latency_ms: u64 = env_var("STORE_LATENCY_MS", 0);

    eprintln!(
        "claim_drain: n_jobs={n_jobs}, workers={n_workers}, \
         payload={payload_bytes}B, flush_interval={flush_interval_ms}ms, \
         store_latency={store_latency_ms}ms",
    );

    let store = store_from_env(store_latency_ms)?;
    let queue = Arc::new(
        Queue::open_with_options(
            store,
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

    // Pre-fill via `enqueue_batch`.
    eprintln!("enqueuing {n_jobs} jobs (batch)...");
    let payload_template = vec![0u8; payload_bytes];
    let payloads: Vec<Vec<u8>> = (0..n_jobs).map(|_| payload_template.clone()).collect();
    let prefill_start = Instant::now();
    queue.enqueue_batch(QUEUE_NAME, payloads).await?;
    eprintln!(
        "enqueue done in {:.1}s",
        prefill_start.elapsed().as_secs_f64(),
    );

    // Each entry is (elapsed_us_at_claim_start, claim_latency_us).
    type LatencySample = (u64, u64);

    let bench_start = Instant::now();

    // Workers
    let mut worker_handles = Vec::with_capacity(n_workers);
    for worker_idx in 0..n_workers {
        let queue = queue.clone();
        worker_handles.push(tokio::spawn(async move {
            let mut samples: Vec<LatencySample> = Vec::with_capacity(8192);
            loop {
                let claim_start = Instant::now();
                match queue.claim(QUEUE_NAME, LEASE).await {
                    Ok(Some(job)) => {
                        let latency_us = claim_start.elapsed().as_micros() as u64;
                        let elapsed_us = bench_start.elapsed().as_micros() as u64;
                        samples.push((elapsed_us, latency_us));
                        if let Err(e) = queue.ack(&job).await {
                            eprintln!("worker {worker_idx}: ack error: {e}");
                            break;
                        }
                    }
                    Ok(None) => {
                        // The bench pre-fills before workers start and
                        // never re-enqueues, so an empty observation is
                        // terminal for this worker.
                        break;
                    }
                    Err(e) => {
                        eprintln!("worker {worker_idx}: claim error: {e}");
                        break;
                    }
                }
            }
            samples
        }));
    }

    // Drain watcher: print per-second progress and exit when the
    // queue has fully drained. Workers self-terminate on empty, so
    // there is no shutdown signal to coordinate.
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
                    eprintln!("drain complete");
                    return;
                }
            }
        })
    };

    // Collect each worker's samples as it finishes.
    let mut worker_samples: Vec<Vec<LatencySample>> = Vec::with_capacity(n_workers);
    for (idx, handle) in worker_handles.into_iter().enumerate() {
        match handle.await {
            Ok(samples) => worker_samples.push(samples),
            Err(e) => eprintln!("worker {idx}: task join error: {e}"),
        }
    }
    let _ = watcher.await;

    // Merge into per-second windows.
    let mut windows: Vec<Vec<u64>> = Vec::new();
    for samples in worker_samples {
        for (elapsed_us, latency_us) in samples {
            let bucket = (elapsed_us / 1_000_000) as usize;
            while windows.len() <= bucket {
                windows.push(Vec::new());
            }
            windows[bucket].push(latency_us);
        }
    }

    println!("window_sec,n_claims,p50_us,p95_us,p99_us");
    for (i, mut window_samples) in windows.into_iter().enumerate() {
        if window_samples.is_empty() {
            continue;
        }
        window_samples.sort_unstable();
        let n = window_samples.len();
        let p50 = pct(&window_samples, 50);
        let p95 = pct(&window_samples, 95);
        let p99 = pct(&window_samples, 99);
        println!("{i},{n},{p50},{p95},{p99}");
    }

    let queue =
        Arc::try_unwrap(queue).map_err(|_| "queue still has outstanding references at shutdown")?;
    queue.close().await?;
    Ok(())
}
