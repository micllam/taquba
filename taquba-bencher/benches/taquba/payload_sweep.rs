// cargo bench -p taquba-bencher --bench payload_sweep > payload_sweep.csv
//
// Payload-size sweep. For each size in PAYLOAD_SIZES, runs a saturating
// concurrent produce/consume load on its own SlateDB store for DURATION_SEC
// (producers enqueue as fast as possible while workers claim and ack), then
// drains the backlog. Reports one row per size: enqueue throughput and
// per-operation latencies, plus the per-job bytes and request counts written
// to object storage.
//
// Each size runs sequentially on its own store path (`bench-db-<size>`) so
// sizes do not share state. The whole run shares one `CountingStore`, so a
// size's byte and request totals are the change in its counters from the start
// to the end of that size's run.
//
// Parameters (env vars, all optional).
//   PAYLOAD_SIZES       comma-separated job payload sizes in bytes, each
//                       min 8 (default `64,1024,16384,262144`).
//   DURATION_SEC        seconds of saturating load per size (default 20).
//   N_PRODUCERS         concurrent enqueue tasks per size (default 4).
//   N_WORKERS           concurrent claim/ack tasks per size (default 8).
//   CLAIM_BATCH         jobs claimed per claim_batch call (default 64).
//   LEASE_SEC           claim lease in seconds (default 60).
//   FLUSH_INTERVAL_MS   SlateDB WAL flush interval in ms (default 1).
//   STORE_LATENCY_MS    injected per-call object-store latency (default 0;
//                       in-memory store only).
//   STORE_JITTER_MS     random write tail latency (default 0; in-memory only).
//   STORE_URL           object-store URL to run against instead of the
//                       in-memory store (s3://.., gs://.., az://..,
//                       file:///abs/path); see the crate README.
//
// Output (stdout): CSV with header
// `payload_bytes,enq_per_s,enq_mbps,enq_p50_us,enq_p99_us,done_per_s,e2e_p50_us,e2e_p99_us,ack_p99_us,bytes_per_job,puts_per_job,store_amp`.
// `enq_per_s` is the saturating durable-enqueue rate; `enq_mbps` is
// `enq_per_s * payload`. `bytes_per_job` is object-store PUT bytes per
// fully-processed job. `store_amp` is `bytes_per_job / payload`: end-to-end
// object-store bytes written per logical payload byte, combining taquba's
// per-transition rewrites and the engine's WAL, flush and compaction. It is
// not the storage engine's LSM write amplification in isolation, and in
// short runs compaction may not have run, so large-payload values are a lower
// bound. The `e2e_*` columns are enqueue-to-ack latency under saturating load,
// so they reflect backlog rather than clean round-trip latency; use
// steady_state with PAYLOAD_BYTES for round-trip latency versus payload.
// Progress prints go to stderr (so stdout stays a clean data stream) and
// include a per-second cumulative store_amp for each size, making its rise and
// plateau as compaction amortizes visible during the run.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use taquba::object_store::ObjectStore;
use taquba::{OpenOptions, Queue, QueueConfig};
use taquba_bencher::{CountingStore, env_var, init_tracing, pct, store_from_env};

/// How often the watcher samples stats for the drain check.
const WATCHER_TICK: Duration = Duration::from_secs(1);
/// How long an idle worker sleeps before re-polling while producing.
const IDLE_BACKOFF: Duration = Duration::from_millis(2);
/// Queue name used within each per-size store.
const QUEUE: &str = "bench";

/// Per-size throughput and latency. Storage bytes are not carried here; the
/// caller reads them from the `CountingStore` counter change across the run.
struct RunResult {
    enq_count: u64,
    enq_p50_us: u64,
    enq_p99_us: u64,
    done_count: u64,
    e2e_p50_us: u64,
    e2e_p99_us: u64,
    ack_p99_us: u64,
    producing_secs: f64,
    total_secs: f64,
}

/// Shared per-run parameters, identical for every payload size in a sweep.
#[derive(Clone, Copy)]
struct RunCfg {
    duration: Duration,
    n_producers: usize,
    n_workers: usize,
    claim_batch: usize,
    lease: Duration,
    flush_ms: u64,
}

/// Saturate one payload size on `db_path`: producers enqueue as fast as
/// possible for `duration` while workers claim and ack, then the backlog
/// drains.
async fn run_one(
    counting: Arc<CountingStore>,
    db_path: String,
    payload_bytes: usize,
    cfg: &RunCfg,
) -> Result<RunResult, Box<dyn std::error::Error>> {
    let RunCfg {
        duration,
        n_producers,
        n_workers,
        claim_batch,
        lease,
        flush_ms,
    } = *cfg;
    let store: Arc<dyn ObjectStore> = counting.clone();
    let queue = Arc::new(
        Queue::open_with_options(
            store,
            &db_path,
            OpenOptions {
                default_queue_config: QueueConfig {
                    keep_done_jobs: None,
                    ..QueueConfig::default()
                },
                flush_interval: Some(Duration::from_millis(flush_ms)),
                ..OpenOptions::default()
            },
        )
        .await?,
    );

    let bench_start = Instant::now();
    // PUT bytes counted before this size; subtracting it gives the per-size
    // delta, so the watcher's cumulative store_amp excludes earlier sizes.
    let bytes0 = counting.put_bytes();
    let acked = Arc::new(AtomicU64::new(0));
    let producers_done = Arc::new(AtomicBool::new(false));
    let drain_complete = Arc::new(AtomicBool::new(false));

    // Producers: enqueue as fast as possible until the duration elapses. The
    // enqueue timestamp is stored in the payload's first 8 bytes for e2e
    // latency.
    let mut producer_handles = Vec::with_capacity(n_producers);
    for producer_idx in 0..n_producers {
        let queue = queue.clone();
        producer_handles.push(tokio::spawn(async move {
            let mut latencies: Vec<u64> = Vec::with_capacity(8192);
            while bench_start.elapsed() < duration {
                let enq_start_us = bench_start.elapsed().as_micros() as u64;
                let mut payload = vec![0u8; payload_bytes];
                payload[..8].copy_from_slice(&enq_start_us.to_le_bytes());
                match queue.enqueue(QUEUE, payload).await {
                    Ok(_) => {
                        let done_us = bench_start.elapsed().as_micros() as u64;
                        latencies.push(done_us - enq_start_us);
                    }
                    Err(e) => {
                        eprintln!("producer {producer_idx}: enqueue error: {e}");
                        return latencies;
                    }
                }
            }
            latencies
        }));
    }

    // Workers: claim a batch, read each job's enqueue timestamp, ack each.
    // claim_batch amortizes the per-claim lock hold and commit across the
    // batch, so the drain rate matches the group-committed enqueue rate rather
    // than serializing one claim per object-store round trip. An empty batch
    // is terminal only once producers have stopped and the watcher has
    // declared the backlog drained.
    type DoneSample = (u64, u64); // (e2e_us, ack_us)
    let mut worker_handles = Vec::with_capacity(n_workers);
    for worker_idx in 0..n_workers {
        let queue = queue.clone();
        let drain_complete = drain_complete.clone();
        let acked = acked.clone();
        worker_handles.push(tokio::spawn(async move {
            let mut samples: Vec<DoneSample> = Vec::with_capacity(8192);
            'poll: loop {
                match queue.claim_batch(QUEUE, claim_batch, lease).await {
                    Ok(jobs) if jobs.is_empty() => {
                        if drain_complete.load(Ordering::Relaxed) {
                            break;
                        }
                        tokio::time::sleep(IDLE_BACKOFF).await;
                    }
                    Ok(jobs) => {
                        for job in &jobs {
                            let enq_start_us =
                                u64::from_le_bytes(job.payload[..8].try_into().unwrap());
                            let ack_start = Instant::now();
                            if let Err(e) = queue.ack(job).await {
                                eprintln!("worker {worker_idx}: ack error: {e}");
                                break 'poll;
                            }
                            let ack_us = ack_start.elapsed().as_micros() as u64;
                            let done_us = bench_start.elapsed().as_micros() as u64;
                            samples.push((done_us - enq_start_us, ack_us));
                            acked.fetch_add(1, Ordering::Relaxed);
                        }
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

    // Watcher: once producers stop and the queue reports empty, signal drain.
    let watcher = {
        let queue = queue.clone();
        let producers_done = producers_done.clone();
        let drain_complete = drain_complete.clone();
        let counting = counting.clone();
        let acked = acked.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(WATCHER_TICK);
            tick.tick().await; // skip immediate first tick
            loop {
                tick.tick().await;
                let s = match queue.stats(QUEUE).await {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                // Cumulative store_amp for this size so far, so its rise and
                // plateau as compaction amortizes are visible during the run.
                let done = acked.load(Ordering::Relaxed);
                let bytes = counting.put_bytes().saturating_sub(bytes0);
                let store_amp = if done > 0 {
                    bytes as f64 / (done as f64 * payload_bytes as f64)
                } else {
                    0.0
                };
                eprintln!(
                    "  [{payload_bytes}B] t={}s pending={} claimed={} acked={done} store_amp={store_amp:.2}",
                    bench_start.elapsed().as_secs(),
                    s.pending,
                    s.claimed,
                );
                if producers_done.load(Ordering::Relaxed) && s.pending == 0 && s.claimed == 0 {
                    drain_complete.store(true, Ordering::Relaxed);
                    return;
                }
            }
        })
    };

    let mut enq_lat: Vec<u64> = Vec::new();
    for (idx, handle) in producer_handles.into_iter().enumerate() {
        match handle.await {
            Ok(latencies) => enq_lat.extend(latencies),
            Err(e) => eprintln!("producer {idx}: task join error: {e}"),
        }
    }
    producers_done.store(true, Ordering::Relaxed);
    let producing_secs = bench_start.elapsed().as_secs_f64();

    let mut e2e: Vec<u64> = Vec::new();
    let mut ack: Vec<u64> = Vec::new();
    for (idx, handle) in worker_handles.into_iter().enumerate() {
        match handle.await {
            Ok(samples) => {
                for (e, a) in samples {
                    e2e.push(e);
                    ack.push(a);
                }
            }
            Err(e) => eprintln!("worker {idx}: task join error: {e}"),
        }
    }
    let _ = watcher.await;
    let total_secs = bench_start.elapsed().as_secs_f64();

    let queue =
        Arc::try_unwrap(queue).map_err(|_| "queue still has outstanding references at shutdown")?;
    queue.close().await?;

    enq_lat.sort_unstable();
    e2e.sort_unstable();
    ack.sort_unstable();
    let p = |v: &[u64], q: usize| if v.is_empty() { 0 } else { pct(v, q) };
    Ok(RunResult {
        enq_count: enq_lat.len() as u64,
        enq_p50_us: p(&enq_lat, 50),
        enq_p99_us: p(&enq_lat, 99),
        done_count: e2e.len() as u64,
        e2e_p50_us: p(&e2e, 50),
        e2e_p99_us: p(&e2e, 99),
        ack_p99_us: p(&ack, 99),
        producing_secs,
        total_secs,
    })
}

fn parse_sizes(spec: &str) -> Result<Vec<usize>, String> {
    let mut sizes = Vec::new();
    for item in spec.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let n: usize = item
            .parse()
            .map_err(|_| format!("invalid PAYLOAD_SIZES entry '{item}'"))?;
        sizes.push(n.max(8));
    }
    if sizes.is_empty() {
        return Err("PAYLOAD_SIZES has no valid entries".into());
    }
    Ok(sizes)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let sizes = parse_sizes(
        &std::env::var("PAYLOAD_SIZES").unwrap_or_else(|_| "64,1024,16384,262144".into()),
    )?;
    let duration_sec: u64 = env_var("DURATION_SEC", 20);
    let n_producers: usize = env_var("N_PRODUCERS", 4);
    let n_workers: usize = env_var("N_WORKERS", 8);
    let claim_batch: usize = env_var("CLAIM_BATCH", 64).max(1);
    let lease_sec: u64 = env_var("LEASE_SEC", 60).max(1);
    let flush_ms: u64 = env_var("FLUSH_INTERVAL_MS", 1);
    let store_latency_ms: u64 = env_var("STORE_LATENCY_MS", 0);

    eprintln!(
        "payload_sweep: sizes={sizes:?}B, duration={duration_sec}s, producers={n_producers}, \
         workers={n_workers}, claim_batch={claim_batch}, lease={lease_sec}s, flush={flush_ms}ms, \
         store_latency={store_latency_ms}ms",
    );

    let base = store_from_env(store_latency_ms)?;
    let counting = Arc::new(CountingStore::new(base));

    let cfg = RunCfg {
        duration: Duration::from_secs(duration_sec),
        n_producers,
        n_workers,
        claim_batch,
        lease: Duration::from_secs(lease_sec),
        flush_ms,
    };

    println!(
        "payload_bytes,enq_per_s,enq_mbps,enq_p50_us,enq_p99_us,done_per_s,e2e_p50_us,e2e_p99_us,ack_p99_us,bytes_per_job,puts_per_job,store_amp"
    );
    for &size in &sizes {
        let before_bytes = counting.put_bytes();
        let before_puts = counting.put_count();
        let r = run_one(counting.clone(), format!("bench-db-{size}"), size, &cfg).await?;
        let bytes = counting.put_bytes() - before_bytes;
        let puts = counting.put_count() - before_puts;

        let enq_per_s = r.enq_count as f64 / r.producing_secs;
        let enq_mbps = enq_per_s * size as f64 / 1e6;
        let done_per_s = r.done_count as f64 / r.total_secs;
        let done = r.done_count.max(1) as f64;
        let bytes_per_job = bytes as f64 / done;
        let puts_per_job = puts as f64 / done;
        let store_amp = bytes_per_job / size as f64;
        println!(
            "{size},{enq_per_s:.0},{enq_mbps:.2},{},{},{done_per_s:.0},{},{},{},{bytes_per_job:.0},{puts_per_job:.2},{store_amp:.2}",
            r.enq_p50_us, r.enq_p99_us, r.e2e_p50_us, r.e2e_p99_us, r.ack_p99_us,
        );
    }

    if counting.multipart_count() > 0 {
        eprintln!(
            "note: {} multipart uploads occurred (compaction streaming); their part bytes are included in the totals",
            counting.multipart_count()
        );
    }
    Ok(())
}
