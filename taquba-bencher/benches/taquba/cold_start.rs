// cargo bench -p taquba-bencher --bench cold_start > cold.csv
//
// Cold-start benchmark for the reopen (cold-open) cost and the claim path
// after a process restart. Phase one builds history: N_HISTORY jobs are
// enqueued, claimed, and acked, leaving a tombstone band at the front of
// the `pending:` key space, and N_LIVE jobs are then enqueued and left
// pending. Phase two reopens the same store and measures the reopen and
// each of the claims that follow. The claim cursor's scan bound is
// in-memory state lost on restart, so the first claim falls back to a front
// prefix scan across the band; the series shows what that scan costs and
// how quickly later claims recover once the bound is re-established. The
// reopen time itself (`open_ms`) is the cold-open metric: it is dominated by
// WAL replay since the last checkpoint, so it scales with store size and
// with how much WAL is left unflushed.
//
// PHASE selects how the two halves run:
//   full     (default) one process: build, graceful close (which
//            checkpoints the memtable), reopen, measure. The reopen here
//            replays a near-empty WAL, so this is the CHEAP cold-open arm.
//            Works with the in-memory store.
//   build    build the store, then either close gracefully or crash; then
//            exit. Requires STORE_URL (a persistent store shared with the
//            measure process).
//   measure  reopen an existing store (from a prior build) and measure.
//            Requires STORE_URL.
// The EXPENSIVE cold-open arm (reopen against a long unflushed WAL) is
// `PHASE=build GRACEFUL_CLOSE=0` (which exits via process::exit, skipping
// all flush and Drop, so the memtable is never checkpointed) followed by
// `PHASE=measure`, both with the same STORE_URL and STORE_PREFIX so the two
// processes share one store. Comparing its `open_ms` with the graceful
// arm's (`GRACEFUL_CLOSE=1`) quantifies the force-flush lever. For example:
//   STORE_URL=s3://bucket STORE_PREFIX=coldopen PHASE=build GRACEFUL_CLOSE=0 ... <bin>
//   STORE_URL=s3://bucket STORE_PREFIX=coldopen PHASE=measure ... <bin> > cold.csv
//
// Parameters (env vars, all optional).
//   PHASE               full | build | measure (default full)
//   GRACEFUL_CLOSE      in PHASE=build, 1 (default) closes cleanly and
//                       checkpoints the memtable; 0 crashes (process::exit
//                       without close), leaving the WAL unflushed since the
//                       last checkpoint. Ignored outside PHASE=build.
//   N_HISTORY           jobs enqueued and acked before the restart
//                       (default 20_000). Sets the width of the
//                       tombstone band the first claim scans across.
//   N_LIVE              jobs left pending across the restart (default 100)
//   PAYLOAD_BYTES       per-job payload size (default 64)
//   FLUSH_INTERVAL_MS   SlateDB WAL flush interval in ms (default 1)
//   STORE_LATENCY_MS    injected object-store latency per call (default 0).
//                       When set, the in-memory store is wrapped in
//                       object_store's ThrottledStore so every get, put,
//                       list, and delete sleeps this long before running,
//                       approximating an S3-class backend. Applies to the
//                       history build as well as the measured phase.
//   STORE_URL           object-store URL (s3://bucket/prefix, gs://...,
//                       az://..., file:///abs/path) to run against
//                       instead of the in-memory store; see
//                       the crate README. Incompatible with
//                       STORE_LATENCY_MS. Required for PHASE=build/measure.
//   STORE_PREFIX        fixed store sub-prefix (default: a unique per-run
//                       `bench-<millis>`). Required for PHASE=build/measure
//                       so both processes share one store location.
//
// Output (stdout): CSV with header `claim_idx,claim_us`, one row per
// post-restart claim in claim order (emitted by the full and measure
// phases). Reopen time and a summary go to stderr so stdout stays a clean
// data stream.

use std::sync::Arc;
use std::time::{Duration, Instant};

use taquba::object_store::ObjectStore;
use taquba::{OpenOptions, Queue, QueueConfig};
use taquba_bencher::{env_var, init_tracing, pct, store_from_env};

const QUEUE_NAME: &str = "bench";

/// Lease used for the measured post-restart claims, each acked
/// immediately after it is recorded.
const LEASE: Duration = Duration::from_secs(5);
/// Lease used while draining the history phase. Long enough that
/// acking a full batch against an injected-latency store never lets
/// the lease expire mid-batch.
const HISTORY_LEASE: Duration = Duration::from_secs(60);
/// Concurrent claim/ack tasks draining the history phase.
const HISTORY_WORKERS: usize = 32;
/// Jobs claimed per claim_batch call while draining the history phase.
const HISTORY_CLAIM_BATCH: usize = 64;
/// Jobs per enqueue_batch call while building the history phase.
const HISTORY_ENQUEUE_BATCH: usize = 1_000;

fn open_options(flush_interval_ms: u64) -> OpenOptions {
    OpenOptions {
        default_queue_config: QueueConfig {
            keep_done_jobs: None,
            ..QueueConfig::default()
        },
        flush_interval: Some(Duration::from_millis(flush_interval_ms)),
        ..OpenOptions::default()
    }
}

/// Build the on-disk history a restart will see: enqueue and drain
/// N_HISTORY jobs (leaving a tombstone band), then enqueue N_LIVE jobs and
/// leave them pending. Returns the still-open queue; the caller decides
/// whether to close it gracefully or crash.
async fn build_store(
    store: Arc<dyn ObjectStore>,
    flush_interval_ms: u64,
    n_history: usize,
    n_live: usize,
    payload: &[u8],
) -> Result<Arc<Queue>, Box<dyn std::error::Error>> {
    let queue = Arc::new(
        Queue::open_with_options(store, "bench-db", open_options(flush_interval_ms)).await?,
    );

    eprintln!("history: enqueuing {n_history} jobs...");
    for chunk_start in (0..n_history).step_by(HISTORY_ENQUEUE_BATCH) {
        let n = HISTORY_ENQUEUE_BATCH.min(n_history - chunk_start);
        queue
            .enqueue_batch(QUEUE_NAME, vec![payload.to_vec(); n])
            .await?;
    }

    eprintln!("history: draining...");
    let drain_start = Instant::now();
    let mut handles = Vec::with_capacity(HISTORY_WORKERS);
    for worker_idx in 0..HISTORY_WORKERS {
        let queue = queue.clone();
        handles.push(tokio::spawn(async move {
            loop {
                match queue
                    .claim_batch(QUEUE_NAME, HISTORY_CLAIM_BATCH, HISTORY_LEASE)
                    .await
                {
                    // The history phase never re-enqueues, so an empty
                    // batch is terminal for this worker.
                    Ok(jobs) if jobs.is_empty() => break,
                    Ok(jobs) => {
                        for job in &jobs {
                            if let Err(e) = queue.ack(job).await {
                                eprintln!("history worker {worker_idx}: ack error: {e}");
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("history worker {worker_idx}: claim error: {e}");
                        return;
                    }
                }
            }
        }));
    }
    for handle in handles {
        handle.await?;
    }
    let stats = queue.stats(QUEUE_NAME).await?;
    if stats.pending != 0 || stats.claimed != 0 {
        return Err(format!(
            "history drain incomplete: pending={} claimed={}",
            stats.pending, stats.claimed,
        )
        .into());
    }
    eprintln!(
        "history: drained in {:.1}s",
        drain_start.elapsed().as_secs_f64(),
    );

    queue
        .enqueue_batch(QUEUE_NAME, vec![payload.to_vec(); n_live])
        .await?;
    Ok(queue)
}

/// Reopen the store, recording the reopen time, then claim and ack the
/// surviving jobs serially so each row is one claim's latency, until the
/// queue is drained. Emits the `claim_idx,claim_us` CSV on stdout and a
/// summary (including the reopen time) on stderr.
async fn measure_reopen(
    store: Arc<dyn ObjectStore>,
    flush_interval_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let open_start = Instant::now();
    let queue =
        Queue::open_with_options(store, "bench-db", open_options(flush_interval_ms)).await?;
    let open_ms = open_start.elapsed().as_millis();
    eprintln!("reopen took {open_ms}ms");

    let mut claim_samples: Vec<u64> = Vec::new();
    loop {
        let claim_start = Instant::now();
        match queue.claim(QUEUE_NAME, LEASE).await? {
            Some(job) => {
                claim_samples.push(claim_start.elapsed().as_micros() as u64);
                queue.ack(&job).await?;
            }
            None => break,
        }
    }

    println!("claim_idx,claim_us");
    for (i, us) in claim_samples.iter().enumerate() {
        println!("{i},{us}");
    }

    let n = claim_samples.len();
    match claim_samples.first() {
        None => eprintln!("summary: open={open_ms}ms claims=0 (queue empty on reopen)"),
        Some(&first) => {
            let mut warm: Vec<u64> = claim_samples[1..].to_vec();
            if warm.is_empty() {
                eprintln!("summary: open={open_ms}ms claims={n} first_claim={first}us");
            } else {
                warm.sort_unstable();
                eprintln!(
                    "summary: open={open_ms}ms claims={n} first_claim={first}us \
                     warm_p50={}us warm_p99={}us",
                    pct(&warm, 50),
                    pct(&warm, 99),
                );
            }
        }
    }

    queue.close().await?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let phase = std::env::var("PHASE").unwrap_or_else(|_| "full".to_string());
    let n_history: usize = env_var("N_HISTORY", 20_000);
    let n_live: usize = env_var("N_LIVE", 100).max(1);
    let payload_bytes: usize = env_var("PAYLOAD_BYTES", 64);
    let flush_interval_ms: u64 = env_var("FLUSH_INTERVAL_MS", 1);
    let store_latency_ms: u64 = env_var("STORE_LATENCY_MS", 0);
    let graceful_close: bool = env_var("GRACEFUL_CLOSE", 1u8) != 0;

    eprintln!(
        "cold_start: phase={phase}, n_history={n_history}, n_live={n_live}, \
         payload={payload_bytes}B, flush_interval={flush_interval_ms}ms, \
         store_latency={store_latency_ms}ms, graceful_close={graceful_close}",
    );

    // build and measure run in separate processes and so need a store that
    // survives a process exit (STORE_URL) at a shared, fixed location
    // (STORE_PREFIX, since store_from_env otherwise picks a unique per-run
    // prefix that the two processes would not agree on).
    if phase == "build" || phase == "measure" {
        if std::env::var("STORE_URL").is_err() {
            return Err(format!(
                "PHASE={phase} requires STORE_URL (a persistent store shared across the build \
                 and measure processes); the in-memory store does not survive a process exit"
            )
            .into());
        }
        if std::env::var("STORE_PREFIX").is_err() {
            return Err(format!(
                "PHASE={phase} requires STORE_PREFIX so the build and measure processes share \
                 one store location (store_from_env otherwise uses a unique per-run prefix)"
            )
            .into());
        }
    }

    let store = store_from_env(store_latency_ms)?;
    let payload = vec![0u8; payload_bytes];

    match phase.as_str() {
        // One process: build, graceful close (checkpoints the memtable),
        // reopen, measure. The reopen replays a near-empty WAL, so this is
        // the cheap cold-open arm.
        "full" => {
            let queue = build_store(
                store.clone(),
                flush_interval_ms,
                n_history,
                n_live,
                &payload,
            )
            .await?;
            let queue = Arc::try_unwrap(queue)
                .map_err(|_| "queue still has outstanding references before restart")?;
            queue.close().await?;
            measure_reopen(store, flush_interval_ms).await?;
        }
        "build" => {
            let queue = build_store(store, flush_interval_ms, n_history, n_live, &payload).await?;
            if graceful_close {
                let queue = Arc::try_unwrap(queue)
                    .map_err(|_| "queue still has outstanding references at close")?;
                queue.close().await?;
                eprintln!("build: graceful close complete; memtable checkpointed");
            } else {
                // Crash arm: exit without close so the memtable is never
                // checkpointed, leaving a long WAL for the measure phase to
                // replay. process::exit skips all Drop, so there is no flush
                // and no lingering SlateDB flush task, matching a real crash.
                eprintln!("build: crash exit (no close); WAL left unflushed since last checkpoint");
                std::process::exit(0);
            }
        }
        "measure" => {
            measure_reopen(store, flush_interval_ms).await?;
        }
        other => {
            return Err(format!("unknown PHASE '{other}', expected full|build|measure").into());
        }
    }

    Ok(())
}
