// cargo bench -p taquba --bench cold_start > cold.csv
//
// Cold-start benchmark for the claim path after a process restart.
// Phase one builds history: N_HISTORY jobs are enqueued, claimed, and
// acked, leaving a tombstone band at the front of the `pending:` key
// space, and N_LIVE jobs are then enqueued and left pending. Phase two
// closes the queue, reopens the same store, and measures the reopen
// and each of the N_LIVE claims that follow. The claim cursor's scan
// bound is in-memory state lost on restart, so the first claim falls
// back to a front prefix scan across the band; the series shows what
// that scan costs and how quickly later claims recover once the bound
// is re-established.
//
// Parameters (env vars, all optional).
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
//
// Output (stdout): CSV with header `claim_idx,claim_us`, one row per
// post-restart claim in claim order. Reopen time and a summary go to
// stderr so stdout stays a clean data stream.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use taquba::{OpenOptions, Queue, QueueConfig};

use common::{env_var, init_tracing, pct, store_with_latency};

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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let n_history: usize = env_var("N_HISTORY", 20_000);
    let n_live: usize = env_var("N_LIVE", 100).max(1);
    let payload_bytes: usize = env_var("PAYLOAD_BYTES", 64);
    let flush_interval_ms: u64 = env_var("FLUSH_INTERVAL_MS", 1);
    let store_latency_ms: u64 = env_var("STORE_LATENCY_MS", 0);

    eprintln!(
        "cold_start: n_history={n_history}, n_live={n_live}, \
         payload={payload_bytes}B, flush_interval={flush_interval_ms}ms, \
         store_latency={store_latency_ms}ms",
    );

    let store = store_with_latency(store_latency_ms);
    let payload = vec![0u8; payload_bytes];

    // Phase one: build the on-disk history the restart will see.
    let queue = Arc::new(
        Queue::open_with_options(store.clone(), "bench-db", open_options(flush_interval_ms))
            .await?,
    );

    eprintln!("history: enqueuing {n_history} jobs...");
    for chunk_start in (0..n_history).step_by(HISTORY_ENQUEUE_BATCH) {
        let n = HISTORY_ENQUEUE_BATCH.min(n_history - chunk_start);
        queue
            .enqueue_batch(QUEUE_NAME, vec![payload.clone(); n])
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
        .enqueue_batch(QUEUE_NAME, vec![payload.clone(); n_live])
        .await?;

    let queue = Arc::try_unwrap(queue)
        .map_err(|_| "queue still has outstanding references before restart")?;
    queue.close().await?;

    // Phase two: reopen the same store, then claim and ack the live
    // jobs serially so each row is one claim's latency.
    let open_start = Instant::now();
    let queue =
        Queue::open_with_options(store, "bench-db", open_options(flush_interval_ms)).await?;
    let open_ms = open_start.elapsed().as_millis();
    eprintln!("reopen took {open_ms}ms");

    let mut claim_samples: Vec<u64> = Vec::with_capacity(n_live);
    for _ in 0..n_live {
        let claim_start = Instant::now();
        let job = queue
            .claim(QUEUE_NAME, LEASE)
            .await?
            .ok_or("queue empty before all live jobs were claimed")?;
        claim_samples.push(claim_start.elapsed().as_micros() as u64);
        queue.ack(&job).await?;
    }

    println!("claim_idx,claim_us");
    for (i, us) in claim_samples.iter().enumerate() {
        println!("{i},{us}");
    }

    let first = claim_samples[0];
    let mut warm: Vec<u64> = claim_samples[1..].to_vec();
    if warm.is_empty() {
        eprintln!("summary: open={open_ms}ms first_claim={first}us");
    } else {
        warm.sort_unstable();
        eprintln!(
            "summary: open={open_ms}ms first_claim={first}us \
             warm_p50={}us warm_p99={}us",
            pct(&warm, 50),
            pct(&warm, 99),
        );
    }

    queue.close().await?;
    Ok(())
}
