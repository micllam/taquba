// cargo bench -p taquba-bencher --bench sharding > sharding.csv
//
// In-process sharding throughput benchmark. SlateDB serializes WAL flushes
// per store (one object-store PUT in flight at a time), so a single store's
// durable-commit ceiling is roughly its per-PUT batch size divided by the
// PUT latency. Each store has its own flush loop, so running N_SHARDS stores
// in one process gives N_SHARDS independent PUT streams. This bench opens
// N_SHARDS stores, saturates each with producers doing durable enqueues, and
// reports the aggregate enqueue throughput, so a sweep over N_SHARDS shows
// whether throughput scales with shard count (it should, up to the object
// store's real PUT capacity) and how balanced the shards are.
//
// The shards are independent SlateDB databases under distinct sub-prefixes
// of one object store (paths `shard-0`, `shard-1`, ...), which also spreads
// load across prefixes (relevant to S3's per-prefix request limits). Each
// shard keeps single-writer semantics; this is the in-process form of the
// scale-out that an external coordinator would otherwise distribute across
// nodes.
//
// This is produce-only: jobs are enqueued and never consumed, so the stores
// grow for the duration of the run. Keep DURATION_SEC modest, especially on
// the in-memory store.
//
// Parameters (env vars, all optional).
//   N_SHARDS            independent stores opened in this process (default 1)
//   PRODUCERS_PER_SHARD concurrent enqueue tasks per shard (default 16).
//                       Held constant per shard so a sweep over N_SHARDS
//                       keeps each store's offered concurrency fixed.
//   DURATION_SEC        seconds producers run before stopping (default 30)
//   PAYLOAD_BYTES       per-job payload size (default 64)
//   FLUSH_INTERVAL_MS   SlateDB WAL flush interval in ms (default 1)
//   STORE_LATENCY_MS    injected object-store latency per call (default 0).
//                       When set, the in-memory store is wrapped in
//                       object_store's ThrottledStore so every get, put,
//                       list, and delete sleeps this long before running,
//                       approximating an S3-class backend. Set this to see
//                       the per-store serialized-flush ceiling and the
//                       multiplier from sharding without real cloud storage.
//   STORE_URL           object-store URL (s3://bucket/prefix, gs://...,
//                       az://..., file:///abs/path) to run against
//                       instead of the in-memory store; see the crate
//                       README. Incompatible with STORE_LATENCY_MS.
//
// Output (stdout): CSV with header `window_sec,enq_per_sec`, the aggregate
// enqueues completed in each one-second window across all shards. The final
// window may be partial. Per-shard totals and a summary go to stderr.

use std::sync::Arc;
use std::time::{Duration, Instant};

use taquba::{OpenOptions, Queue, QueueConfig};
use taquba_bencher::{env_var, init_tracing, store_from_env};

const QUEUE_NAME: &str = "bench";

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

    let n_shards: usize = env_var("N_SHARDS", 1).max(1);
    let producers_per_shard: usize = env_var("PRODUCERS_PER_SHARD", 16).max(1);
    let duration_sec: u64 = env_var("DURATION_SEC", 30);
    let payload_bytes: usize = env_var("PAYLOAD_BYTES", 64);
    let flush_interval_ms: u64 = env_var("FLUSH_INTERVAL_MS", 1);
    let store_latency_ms: u64 = env_var("STORE_LATENCY_MS", 0);

    eprintln!(
        "sharding: shards={n_shards}, producers_per_shard={producers_per_shard}, \
         duration={duration_sec}s, payload={payload_bytes}B, \
         flush_interval={flush_interval_ms}ms, store_latency={store_latency_ms}ms",
    );

    let store = store_from_env(store_latency_ms)?;

    // Each shard is an independent SlateDB database under its own sub-prefix
    // of the shared object store, so each has its own serialized flush loop.
    let mut shards: Vec<Arc<Queue>> = Vec::with_capacity(n_shards);
    for i in 0..n_shards {
        let queue = Queue::open_with_options(
            store.clone(),
            &format!("shard-{i}"),
            open_options(flush_interval_ms),
        )
        .await?;
        shards.push(Arc::new(queue));
    }

    let bench_start = Instant::now();
    let deadline = Duration::from_secs(duration_sec);

    // Producers saturate each shard with durable enqueues until the deadline,
    // bucketing their own completion counts per one-second window.
    let mut handles = Vec::with_capacity(n_shards * producers_per_shard);
    for (shard_idx, shard) in shards.iter().enumerate() {
        for _ in 0..producers_per_shard {
            let queue = shard.clone();
            handles.push(tokio::spawn(async move {
                let mut per_sec: Vec<u64> = Vec::new();
                loop {
                    if bench_start.elapsed() >= deadline {
                        break;
                    }
                    let payload = vec![0u8; payload_bytes];
                    match queue.enqueue(QUEUE_NAME, payload).await {
                        Ok(_) => {
                            let sec = bench_start.elapsed().as_secs() as usize;
                            while per_sec.len() <= sec {
                                per_sec.push(0);
                            }
                            per_sec[sec] += 1;
                        }
                        Err(e) => {
                            eprintln!("shard {shard_idx} producer: enqueue error: {e}");
                            break;
                        }
                    }
                }
                (shard_idx, per_sec)
            }));
        }
    }

    // Merge per-producer windows into an aggregate series and per-shard totals.
    let mut aggregate: Vec<u64> = Vec::new();
    let mut per_shard_total: Vec<u64> = vec![0; n_shards];
    for handle in handles {
        let (shard_idx, per_sec) = handle.await?;
        while aggregate.len() < per_sec.len() {
            aggregate.push(0);
        }
        for (sec, count) in per_sec.iter().enumerate() {
            aggregate[sec] += count;
            per_shard_total[shard_idx] += count;
        }
    }
    let elapsed = bench_start.elapsed();

    for shard in shards {
        let shard = Arc::try_unwrap(shard)
            .map_err(|_| "shard queue still has outstanding references at shutdown")?;
        shard.close().await?;
    }

    println!("window_sec,enq_per_sec");
    for (sec, count) in aggregate.iter().enumerate() {
        println!("{sec},{count}");
    }

    let total: u64 = per_shard_total.iter().sum();
    let secs = elapsed.as_secs_f64();
    let aggregate_per_sec = if secs > 0.0 { total as f64 / secs } else { 0.0 };
    let per_shard_mean = aggregate_per_sec / n_shards as f64;
    let shard_min = per_shard_total.iter().min().copied().unwrap_or(0);
    let shard_max = per_shard_total.iter().max().copied().unwrap_or(0);
    for (i, count) in per_shard_total.iter().enumerate() {
        eprintln!("  shard {i}: {count} enq");
    }
    eprintln!(
        "summary: shards={n_shards} producers_per_shard={producers_per_shard} \
         total_enq={total} elapsed={secs:.1}s aggregate={aggregate_per_sec:.0}/s \
         per_shard_mean={per_shard_mean:.0}/s shard_min={shard_min} shard_max={shard_max}",
    );

    Ok(())
}
