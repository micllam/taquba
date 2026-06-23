// cargo bench -p taquba-bencher --bench reaper_storm > storm.csv
//
// Mass lease-expiry benchmark for the reaper. Phase one builds the
// storm: N_EXPIRED jobs are enqueued on one queue and claimed with a
// lease that expires immediately, never acked, simulating a crash
// that abandoned every claim; the reaper interval is set long
// enough that no sweep runs, so the storm survives a clean close
// intact. Phase two reopens the store with a
// normal reaper interval while producers and workers run a steady
// load on a second queue, and tracks both the sweep's progress and
// the live queue's latencies per second. The reaper requeues each
// expired claim in its own durable transaction, so the sweep rate is
// expected to track the WAL flush interval.
//
// Parameters (env vars, all optional).
//   N_EXPIRED           abandoned claims built before the restart
//                       (default 5_000)
//   RATE                offered enqueue rate in jobs/sec on the live
//                       queue (default 500)
//   N_PRODUCERS         concurrent enqueue tasks (default 4)
//   N_WORKERS           concurrent claim/ack tasks on the live queue
//                       (default 20)
//   REAPER_INTERVAL_MS  reaper interval for the measured phase
//                       (default 1_000; the library default is 5_000)
//   PAYLOAD_BYTES       per-job payload size, min 8 (default 64)
//   FLUSH_INTERVAL_MS   SlateDB WAL flush interval in ms (default 1)
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
//   DURATION_CAP_SEC    abort threshold for the measured phase
//                       (default 600)
//   METRICS_SAMPLE_MS   gauge sampler interval in ms (default 1000). Only
//                       effective when built with `--features metrics`, which
//                       installs a recorder so taquba's metric emission
//                       (including the reaper's reaped counter) runs under
//                       load; validates that path, not a measurement source.
//
// Output (stdout): CSV with header
// `window_sec,storm_claimed,storm_pending,n_done,e2e_p50_us,e2e_p99_us,claim_p99_us`.
// `storm_claimed` counts abandoned claims the sweep has not yet
// requeued; `storm_pending` counts requeued ones. The remaining
// columns describe the live queue: acks completed in that second,
// enqueue-to-ack latency, and per-claim latency. Status and progress
// prints go to stderr so stdout stays a clean data stream.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use taquba::{OpenOptions, Queue, QueueConfig};
use taquba_bencher::{env_var, init_tracing, pct, store_from_env};

const LIVE_QUEUE: &str = "live";
const STORM_QUEUE: &str = "storm";

/// Lease held while a live worker has a job claimed. Long enough that
/// the reaper never sweeps a live claim during the bench.
const LIVE_LEASE: Duration = Duration::from_secs(60);
/// Lease used to build the storm; expired well before phase two opens.
const STORM_LEASE: Duration = Duration::from_millis(1);
/// Jobs claimed per claim_batch call while building the storm.
const STORM_CLAIM_BATCH: usize = 256;
/// Jobs per enqueue_batch call while building the storm.
const STORM_ENQUEUE_BATCH: usize = 1_000;
/// Reaper interval for phase one, long enough that no sweep runs
/// while the storm is being built.
const BUILD_REAPER_INTERVAL: Duration = Duration::from_secs(3_600);
/// Watcher poll interval: how often both queues' stats are sampled.
const WATCHER_TICK: Duration = Duration::from_secs(1);
/// How long an idle live worker sleeps before re-polling.
const IDLE_BACKOFF: Duration = Duration::from_millis(2);

fn open_options(flush_interval_ms: u64, reaper_interval: Duration) -> OpenOptions {
    OpenOptions {
        default_queue_config: QueueConfig {
            keep_done_jobs: None,
            ..QueueConfig::default()
        },
        flush_interval: Some(Duration::from_millis(flush_interval_ms)),
        reaper_interval,
        metrics_sample_interval: taquba_bencher::metrics_sample_interval(),
        ..OpenOptions::default()
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    // With `--features metrics`, install a recorder so the queue's metric
    // emissions (including the reaper's reaped counter) run under load;
    // rendered once at shutdown as a sanity check.
    #[cfg(feature = "metrics")]
    let prometheus = taquba_bencher::install_metrics_recorder();

    let n_expired: usize = env_var("N_EXPIRED", 5_000);
    let rate: f64 = env_var("RATE", 500.0);
    let n_producers: usize = env_var("N_PRODUCERS", 4);
    let n_workers: usize = env_var("N_WORKERS", 20);
    let reaper_interval_ms: u64 = env_var("REAPER_INTERVAL_MS", 1_000);
    let payload_bytes: usize = env_var("PAYLOAD_BYTES", 64).max(8);
    let flush_interval_ms: u64 = env_var("FLUSH_INTERVAL_MS", 1);
    let store_latency_ms: u64 = env_var("STORE_LATENCY_MS", 0);
    let cap_sec: u64 = env_var("DURATION_CAP_SEC", 600);

    eprintln!(
        "reaper_storm: n_expired={n_expired}, rate={rate}/s, \
         producers={n_producers}, workers={n_workers}, \
         reaper_interval={reaper_interval_ms}ms, payload={payload_bytes}B, \
         flush_interval={flush_interval_ms}ms, store_latency={store_latency_ms}ms",
    );

    let store = store_from_env(store_latency_ms)?;

    // Phase one: build the storm of abandoned claims.
    let queue = Queue::open_with_options(
        store.clone(),
        "bench-db",
        open_options(flush_interval_ms, BUILD_REAPER_INTERVAL),
    )
    .await?;
    eprintln!("storm: enqueuing and claiming {n_expired} jobs...");
    for chunk_start in (0..n_expired).step_by(STORM_ENQUEUE_BATCH) {
        let n = STORM_ENQUEUE_BATCH.min(n_expired - chunk_start);
        queue
            .enqueue_batch(STORM_QUEUE, vec![vec![0u8; payload_bytes]; n])
            .await?;
    }
    let mut claimed_total = 0;
    while claimed_total < n_expired {
        let jobs = queue
            .claim_batch(STORM_QUEUE, STORM_CLAIM_BATCH, STORM_LEASE)
            .await?;
        if jobs.is_empty() {
            return Err("queue empty before all storm jobs were claimed".into());
        }
        claimed_total += jobs.len();
    }
    queue.close().await?;

    // Phase two: reopen with a normal reaper interval; the first sweep
    // finds every storm claim expired. Live traffic runs concurrently.
    let queue = Arc::new(
        Queue::open_with_options(
            store,
            "bench-db",
            open_options(flush_interval_ms, Duration::from_millis(reaper_interval_ms)),
        )
        .await?,
    );
    let bench_start = Instant::now();
    let producers_done = Arc::new(AtomicBool::new(false));
    // Set by the watcher once no storm claims remain; producers stop
    // offering load at that point.
    let sweep_complete = Arc::new(AtomicBool::new(false));
    // Set by the watcher once the live queue has fully drained after
    // the producers stopped; workers exit on this rather than on
    // their own empty polls.
    let drain_complete = Arc::new(AtomicBool::new(false));

    // Producers: sustain rate / N_PRODUCERS on the live queue until
    // the sweep completes. The enqueue timestamp is stored in the
    // payload's first 8 bytes so workers can compute end-to-end
    // latency.
    let mut producer_handles = Vec::with_capacity(n_producers);
    for producer_idx in 0..n_producers {
        let queue = queue.clone();
        let sweep_complete = sweep_complete.clone();
        producer_handles.push(tokio::spawn(async move {
            let period = Duration::from_secs_f64(n_producers as f64 / rate);
            let mut tick = tokio::time::interval(period);
            loop {
                tick.tick().await;
                if sweep_complete.load(Ordering::Relaxed) {
                    break;
                }
                let enq_start_us = bench_start.elapsed().as_micros() as u64;
                let mut payload = vec![0u8; payload_bytes];
                payload[..8].copy_from_slice(&enq_start_us.to_le_bytes());
                if let Err(e) = queue.enqueue(LIVE_QUEUE, payload).await {
                    eprintln!("producer {producer_idx}: enqueue error: {e}");
                    break;
                }
            }
        }));
    }

    // Workers: claim and ack on the live queue, recording per-claim
    // and end-to-end latency.
    type DoneSample = (u64, u64, u64); // (elapsed_us, e2e_us, claim_us)
    let mut worker_handles = Vec::with_capacity(n_workers);
    for worker_idx in 0..n_workers {
        let queue = queue.clone();
        let drain_complete = drain_complete.clone();
        worker_handles.push(tokio::spawn(async move {
            let mut samples: Vec<DoneSample> = Vec::with_capacity(8192);
            loop {
                let claim_start = Instant::now();
                match queue.claim(LIVE_QUEUE, LIVE_LEASE).await {
                    Ok(Some(job)) => {
                        let claim_us = claim_start.elapsed().as_micros() as u64;
                        let enq_start_us = u64::from_le_bytes(job.payload[..8].try_into().unwrap());
                        if let Err(e) = queue.ack(&job).await {
                            eprintln!("worker {worker_idx}: ack error: {e}");
                            break;
                        }
                        let done_us = bench_start.elapsed().as_micros() as u64;
                        samples.push((done_us, done_us - enq_start_us, claim_us));
                    }
                    Ok(None) => {
                        if drain_complete.load(Ordering::Relaxed) {
                            break;
                        }
                        tokio::time::sleep(IDLE_BACKOFF).await;
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

    // Watcher: sample both queues once per second, flag sweep
    // completion and the final live drain.
    let watcher = {
        let queue = queue.clone();
        let producers_done = producers_done.clone();
        let sweep_complete = sweep_complete.clone();
        let drain_complete = drain_complete.clone();
        tokio::spawn(async move {
            // Each entry is (elapsed_sec, storm_claimed, storm_pending).
            let mut storm_samples: Vec<(u64, i64, i64)> = Vec::new();
            let mut tick = tokio::time::interval(WATCHER_TICK);
            tick.tick().await; // skip immediate first tick
            loop {
                tick.tick().await;
                let (storm, live) = match (
                    queue.stats(STORM_QUEUE).await,
                    queue.stats(LIVE_QUEUE).await,
                ) {
                    (Ok(s), Ok(l)) => (s, l),
                    _ => continue,
                };
                let elapsed = bench_start.elapsed().as_secs();
                storm_samples.push((elapsed, storm.claimed, storm.pending));
                eprintln!(
                    "  t={elapsed}s storm_claimed={} storm_pending={} \
                     live_pending={} live_done={}",
                    storm.claimed, storm.pending, live.pending, live.done,
                );
                if storm.claimed == 0 && !sweep_complete.swap(true, Ordering::Relaxed) {
                    eprintln!("sweep complete");
                }
                if sweep_complete.load(Ordering::Relaxed)
                    && producers_done.load(Ordering::Relaxed)
                    && live.pending == 0
                    && live.claimed == 0
                {
                    drain_complete.store(true, Ordering::Relaxed);
                    eprintln!("live drain complete");
                    return storm_samples;
                }
                if elapsed >= cap_sec {
                    eprintln!("duration cap reached, aborting");
                    sweep_complete.store(true, Ordering::Relaxed);
                    drain_complete.store(true, Ordering::Relaxed);
                    return storm_samples;
                }
            }
        })
    };

    for (idx, handle) in producer_handles.into_iter().enumerate() {
        if let Err(e) = handle.await {
            eprintln!("producer {idx}: task join error: {e}");
        }
    }
    producers_done.store(true, Ordering::Relaxed);
    eprintln!("producers done, draining live queue...");

    let mut done_samples: Vec<Vec<DoneSample>> = Vec::with_capacity(n_workers);
    for (idx, handle) in worker_handles.into_iter().enumerate() {
        match handle.await {
            Ok(samples) => done_samples.push(samples),
            Err(e) => eprintln!("worker {idx}: task join error: {e}"),
        }
    }
    let storm_samples = watcher.await.unwrap_or_default();

    // Merge into per-second windows.
    #[derive(Default)]
    struct Window {
        e2e: Vec<u64>,
        claim: Vec<u64>,
        storm: Option<(i64, i64)>,
    }
    let mut windows: Vec<Window> = Vec::new();
    let window_at = |sec: usize, windows: &mut Vec<Window>| {
        while windows.len() <= sec {
            windows.push(Window::default());
        }
    };
    for samples in done_samples {
        for (elapsed_us, e2e_us, claim_us) in samples {
            let sec = (elapsed_us / 1_000_000) as usize;
            window_at(sec, &mut windows);
            windows[sec].e2e.push(e2e_us);
            windows[sec].claim.push(claim_us);
        }
    }
    for (sec, claimed, pending) in &storm_samples {
        let sec = *sec as usize;
        window_at(sec, &mut windows);
        windows[sec].storm = Some((*claimed, *pending));
    }

    println!("window_sec,storm_claimed,storm_pending,n_done,e2e_p50_us,e2e_p99_us,claim_p99_us");
    for (i, mut w) in windows.into_iter().enumerate() {
        if w.e2e.is_empty() && w.storm.is_none() {
            continue;
        }
        w.e2e.sort_unstable();
        w.claim.sort_unstable();
        let (e2e_p50, e2e_p99, claim_p99) = if w.e2e.is_empty() {
            (0, 0, 0)
        } else {
            (pct(&w.e2e, 50), pct(&w.e2e, 99), pct(&w.claim, 99))
        };
        let (storm_claimed, storm_pending) =
            w.storm.map_or((String::new(), String::new()), |(c, p)| {
                (c.to_string(), p.to_string())
            });
        println!(
            "{i},{storm_claimed},{storm_pending},{},{e2e_p50},{e2e_p99},{claim_p99}",
            w.e2e.len(),
        );
    }

    // Sweep summary at the watcher's one-second granularity. When the
    // sweep finishes before the first sample, no sample shows the full
    // storm and the start falls back to zero.
    let sweep_start = storm_samples
        .iter()
        .take_while(|(_, claimed, _)| *claimed == n_expired as i64)
        .last()
        .map_or(0, |(t, _, _)| *t);
    let sweep_end = storm_samples
        .iter()
        .find(|(_, claimed, _)| *claimed == 0)
        .map(|(t, _, _)| *t);
    if let Some(end) = sweep_end {
        let secs = (end - sweep_start).max(1);
        eprintln!(
            "summary: {n_expired} expired claims requeued in ~{secs}s \
             (~{}/s, observed at 1s granularity)",
            n_expired as u64 / secs,
        );
    } else {
        eprintln!("summary: sweep did not complete within the duration cap");
    }

    let queue =
        Arc::try_unwrap(queue).map_err(|_| "queue still has outstanding references at shutdown")?;
    queue.close().await?;

    #[cfg(feature = "metrics")]
    taquba_bencher::report_metrics(&prometheus);
    Ok(())
}
