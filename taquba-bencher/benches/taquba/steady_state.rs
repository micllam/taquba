// cargo bench -p taquba-bencher --bench steady_state > steady.csv
//
// Steady-state benchmark for concurrent produce and consume. Producers
// sustain a fixed offered enqueue rate for DURATION_SEC while workers
// claim and ack concurrently; producers then stop and workers drain the
// backlog. Emits a per-second time series so degradation over time
// (compaction stalls, tombstone accumulation, backlog growth) is visible,
// unlike a drain-shaped run that starts from a prefilled queue.
//
// Parameters (env vars, all optional).
//   DURATION_SEC        seconds producers sustain the offered rate (default 60)
//   RATE                offered enqueue rate in jobs/sec across all
//                       producers (default 500)
//   RATE_SCHEDULE       optional comma-separated `seconds:rate` segments
//                       describing a time-varying offered rate, e.g.
//                       `60:0,300:500,120:2000` for 60s idle, then 300s at
//                       500/s, then 120s at 2000/s. A rate of 0 is an idle
//                       segment (producers sleep). Mutually exclusive with
//                       RATE and DURATION_SEC; the total run length is the
//                       sum of the segment seconds. Producers follow the
//                       schedule independently; workers drain after the last
//                       segment as usual.
//   N_PRODUCERS         concurrent enqueue tasks (default 4). Each producer
//                       enqueues serially, so the offered rate is capped at
//                       roughly N_PRODUCERS / per-enqueue-latency.
//   N_WORKERS           concurrent claim/ack tasks (default 50). Must be
//                       at least N_QUEUES so every queue has a worker.
//   N_QUEUES            queues the load is spread across (default 1).
//                       Producers enqueue round-robin; worker i serves
//                       queue i mod N_QUEUES. Values above 1 exercise
//                       the global reaper / scheduler prefix scans and
//                       the per-queue claim state under many queues.
//   CLAIM_BATCH         jobs claimed per claim_batch call (default 1).
//                       Values above 1 amortize the per-claim lock hold
//                       and commit across the batch; each job is still
//                       acked individually.
//   WAIT_CLAIM_MS       when above 0, workers use claim_with_wait with
//                       this wait instead of polling claim_batch, so
//                       idle workers wait on the queue-scoped notify and
//                       wake one per inserted job. claim_p99_us is
//                       reported as 0 in this mode: a successful call's
//                       latency is dominated by time waiting for
//                       a job to exist, which is not claim-path cost.
//   PAYLOAD_BYTES       per-job payload size, min 8 (default 64)
//   FLUSH_INTERVAL_MS   SlateDB WAL flush interval in ms (default 1)
//   STORE_LATENCY_MS    injected object-store latency per call (default 0).
//                       When set, the in-memory store is wrapped in
//                       object_store's ThrottledStore so every get, put,
//                       list, and delete sleeps this long before running,
//                       approximating an S3-class backend.
//   STORE_JITTER_MS     random tail latency in [0, STORE_JITTER_MS] added
//                       to each write on top of STORE_LATENCY_MS (default 0),
//                       injecting object-store PUT tail latency to study its
//                       effect on e2e and backlog without real S3.
//   STORE_URL           object-store URL (s3://bucket/prefix, gs://...,
//                       az://..., file:///abs/path) to run against
//                       instead of the in-memory store; see
//                       the crate README. Incompatible with
//                       STORE_LATENCY_MS and STORE_JITTER_MS.
//   METRICS_SAMPLE_MS   gauge sampler interval in ms (default 1000). Only
//                       effective when built with `--features metrics`, which
//                       installs a recorder so taquba's metric emission runs
//                       under load; used to validate that path and its
//                       overhead, not as a measurement source (the bench
//                       still times operations directly).
//
// Output (stdout): CSV with header
// `window_sec,n_enq,enq_p99_us,n_done,e2e_p50_us,e2e_p95_us,e2e_p99_us,claim_p99_us,ack_p99_us,pending`.
// `n_enq`/`n_done` count enqueues and acks completed in that second.
// `e2e_*` is enqueue-call start to ack completion. `pending` is the
// queue depth sampled once per second; growth across windows means the
// offered rate exceeds what the queue sustains. Status and progress
// prints go to stderr so stdout stays a clean data stream.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use taquba::{OpenOptions, Queue, QueueConfig};
use taquba_bencher::{env_var, init_tracing, pct, store_from_env};

/// Lease held while a worker has a job claimed. Long enough that an
/// idle scheduler tick during the bench never lets a lease expire.
const LEASE: Duration = Duration::from_secs(5);
/// Watcher poll interval: how often stats are sampled for the
/// `pending` column and the drain check.
const WATCHER_TICK: Duration = Duration::from_secs(1);
/// How long an idle worker sleeps before re-polling while producers
/// are still running.
const IDLE_BACKOFF: Duration = Duration::from_millis(2);

/// Parse a `RATE_SCHEDULE` value: a comma-separated list of
/// `seconds:rate` segments (e.g. `60:0,300:500,120:2000`). A rate of 0 is
/// an idle segment. Empty segments are skipped; at least one valid segment
/// is required.
fn parse_schedule(spec: &str) -> Result<Vec<(u64, f64)>, String> {
    let mut schedule = Vec::new();
    for seg in spec.split(',') {
        let seg = seg.trim();
        if seg.is_empty() {
            continue;
        }
        let (secs, rate) = seg.split_once(':').ok_or_else(|| {
            format!("invalid RATE_SCHEDULE segment '{seg}', expected 'seconds:rate'")
        })?;
        let secs: u64 = secs
            .trim()
            .parse()
            .map_err(|_| format!("invalid seconds in RATE_SCHEDULE segment '{seg}'"))?;
        let rate: f64 = rate
            .trim()
            .parse()
            .map_err(|_| format!("invalid rate in RATE_SCHEDULE segment '{seg}'"))?;
        if secs == 0 {
            return Err(format!("RATE_SCHEDULE segment '{seg}' has zero seconds"));
        }
        if rate < 0.0 {
            return Err(format!("RATE_SCHEDULE segment '{seg}' has negative rate"));
        }
        schedule.push((secs, rate));
    }
    if schedule.is_empty() {
        return Err("RATE_SCHEDULE has no valid segments".into());
    }
    Ok(schedule)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let duration_sec: u64 = env_var("DURATION_SEC", 60);
    let rate: f64 = env_var("RATE", 500.0);
    let n_producers: usize = env_var("N_PRODUCERS", 4);
    let n_workers: usize = env_var("N_WORKERS", 50);
    let n_queues: usize = env_var("N_QUEUES", 1).max(1);
    let claim_batch: usize = env_var("CLAIM_BATCH", 1).max(1);
    let wait_claim_ms: u64 = env_var("WAIT_CLAIM_MS", 0);
    let payload_bytes: usize = env_var("PAYLOAD_BYTES", 64).max(8);
    let flush_interval_ms: u64 = env_var("FLUSH_INTERVAL_MS", 1);
    let store_latency_ms: u64 = env_var("STORE_LATENCY_MS", 0);

    // Offered-load schedule: either a time-varying RATE_SCHEDULE or a single
    // fixed segment from RATE for DURATION_SEC. RATE_SCHEDULE is mutually
    // exclusive with RATE and DURATION_SEC.
    let schedule: Vec<(u64, f64)> = match std::env::var("RATE_SCHEDULE") {
        Ok(spec) => {
            if std::env::var("RATE").is_ok() || std::env::var("DURATION_SEC").is_ok() {
                return Err(
                    "RATE_SCHEDULE is mutually exclusive with RATE and DURATION_SEC".into(),
                );
            }
            parse_schedule(&spec)?
        }
        Err(_) => vec![(duration_sec, rate)],
    };
    let total_sec: u64 = schedule.iter().map(|(secs, _)| secs).sum();
    let schedule_desc = schedule
        .iter()
        .map(|(secs, seg_rate)| format!("{secs}s@{seg_rate}/s"))
        .collect::<Vec<_>>()
        .join(",");

    if n_workers < n_queues {
        return Err("N_WORKERS must be at least N_QUEUES so every queue has a worker".into());
    }

    eprintln!(
        "steady_state: schedule=[{schedule_desc}] total={total_sec}s, \
         producers={n_producers}, workers={n_workers}, queues={n_queues}, \
         claim_batch={claim_batch}, wait_claim={wait_claim_ms}ms, \
         payload={payload_bytes}B, flush_interval={flush_interval_ms}ms, \
         store_latency={store_latency_ms}ms",
    );

    let queue_names: Arc<Vec<String>> =
        Arc::new((0..n_queues).map(|i| format!("bench-{i}")).collect());

    // With `--features metrics`, install a recorder so the queue's metric
    // emissions run under load (the facade macros are no-ops with no recorder);
    // the gauge sampler is enabled via metrics_sample_interval below. Rendered
    // once at shutdown as a sanity check.
    #[cfg(feature = "metrics")]
    let prometheus = taquba_bencher::install_metrics_recorder();

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
                metrics_sample_interval: taquba_bencher::metrics_sample_interval(),
                ..OpenOptions::default()
            },
        )
        .await?,
    );

    let bench_start = Instant::now();
    let producers_done = Arc::new(AtomicBool::new(false));
    // Set by the watcher once stats report the queue fully drained.
    // Workers exit on this rather than on their own empty polls: a
    // lease that expires after a worker's last poll is requeued by the
    // reaper and must still find live workers.
    let drain_complete = Arc::new(AtomicBool::new(false));

    // Each entry is (elapsed_us_at_completion, latency_us).
    type Sample = (u64, u64);

    // Producers: each follows the offered-rate schedule independently.
    // Within a non-idle segment a producer sustains seg_rate / N_PRODUCERS
    // via an interval whose default Burst missed-tick behaviour catches up
    // after a slow enqueue, preserving the offered rate on average,
    // enqueuing round-robin across the queues. An idle (rate 0) segment
    // sleeps until it ends. The enqueue timestamp is stored in the payload's
    // first 8 bytes so workers can compute end-to-end latency.
    let schedule = Arc::new(schedule);
    let mut producer_handles = Vec::with_capacity(n_producers);
    for producer_idx in 0..n_producers {
        let queue = queue.clone();
        let queue_names = queue_names.clone();
        let schedule = schedule.clone();
        producer_handles.push(tokio::spawn(async move {
            let mut samples: Vec<Sample> = Vec::with_capacity(8192);
            let mut seq = producer_idx;
            let mut segment_start = Duration::ZERO;
            for &(secs, seg_rate) in schedule.iter() {
                let segment_end = segment_start + Duration::from_secs(secs);
                segment_start = segment_end;
                if seg_rate <= 0.0 {
                    // Idle segment: sleep until it ends.
                    if let Some(remaining) = segment_end.checked_sub(bench_start.elapsed()) {
                        tokio::time::sleep(remaining).await;
                    }
                    continue;
                }
                let period = Duration::from_secs_f64(n_producers as f64 / seg_rate);
                let mut tick = tokio::time::interval(period);
                loop {
                    tick.tick().await;
                    if bench_start.elapsed() >= segment_end {
                        break;
                    }
                    let enq_start_us = bench_start.elapsed().as_micros() as u64;
                    let mut payload = vec![0u8; payload_bytes];
                    payload[..8].copy_from_slice(&enq_start_us.to_le_bytes());
                    let queue_name = &queue_names[seq % queue_names.len()];
                    seq += 1;
                    match queue.enqueue(queue_name, payload).await {
                        Ok(_) => {
                            let done_us = bench_start.elapsed().as_micros() as u64;
                            samples.push((done_us, done_us - enq_start_us));
                        }
                        Err(e) => {
                            eprintln!("producer {producer_idx}: enqueue error: {e}");
                            return samples;
                        }
                    }
                }
            }
            samples
        }));
    }

    // Workers: claim a batch, read each job's embedded enqueue
    // timestamp, ack each job. The batch's claim latency is recorded on
    // every job it delivered. Each worker serves one queue. An empty
    // poll is terminal only once producers have stopped.
    type DoneSample = (u64, u64, u64, u64); // (elapsed_us, e2e_us, claim_us, ack_us)
    let mut worker_handles = Vec::with_capacity(n_workers);
    for worker_idx in 0..n_workers {
        let queue = queue.clone();
        let queue_name = queue_names[worker_idx % queue_names.len()].clone();
        let drain_complete = drain_complete.clone();
        worker_handles.push(tokio::spawn(async move {
            let mut samples: Vec<DoneSample> = Vec::with_capacity(8192);
            'poll: loop {
                let claim_start = Instant::now();
                let claimed = if wait_claim_ms > 0 {
                    queue
                        .claim_with_wait(&queue_name, LEASE, Duration::from_millis(wait_claim_ms))
                        .await
                        .map(|job| job.into_iter().collect())
                } else {
                    queue.claim_batch(&queue_name, claim_batch, LEASE).await
                };
                match claimed {
                    Ok(jobs) if jobs.is_empty() => {
                        if drain_complete.load(Ordering::Relaxed) {
                            break;
                        }
                        if wait_claim_ms == 0 {
                            tokio::time::sleep(IDLE_BACKOFF).await;
                        }
                    }
                    Ok(jobs) => {
                        // In wait mode the call's latency is mostly time
                        // waiting for a job to exist, not claim
                        // cost; report zero rather than a misleading mix.
                        let claim_us = if wait_claim_ms > 0 {
                            0
                        } else {
                            claim_start.elapsed().as_micros() as u64
                        };
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
                            samples.push((done_us, done_us - enq_start_us, claim_us, ack_us));
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

    // Watcher: sample queue depth once per second for the `pending`
    // column and print progress, summed across all queues. Exits when
    // producers have stopped and every queue has fully drained.
    let watcher = {
        let queue = queue.clone();
        let queue_names = queue_names.clone();
        let producers_done = producers_done.clone();
        let drain_complete = drain_complete.clone();
        tokio::spawn(async move {
            let mut depth_samples: Vec<(u64, i64)> = Vec::new();
            let mut tick = tokio::time::interval(WATCHER_TICK);
            tick.tick().await; // skip immediate first tick
            'sample: loop {
                tick.tick().await;
                let (mut pending, mut claimed, mut done) = (0i64, 0i64, 0i64);
                for queue_name in queue_names.iter() {
                    match queue.stats(queue_name).await {
                        Ok(s) => {
                            pending += s.pending;
                            claimed += s.claimed;
                            done += s.done;
                        }
                        Err(_) => continue 'sample,
                    }
                }
                let elapsed = bench_start.elapsed().as_secs();
                depth_samples.push((elapsed, pending));
                eprintln!("  t={elapsed}s pending={pending} claimed={claimed} done={done}");
                if producers_done.load(Ordering::Relaxed) && pending == 0 && claimed == 0 {
                    drain_complete.store(true, Ordering::Relaxed);
                    eprintln!("drain complete");
                    return depth_samples;
                }
            }
        })
    };

    let mut enq_samples: Vec<Vec<Sample>> = Vec::with_capacity(n_producers);
    for (idx, handle) in producer_handles.into_iter().enumerate() {
        match handle.await {
            Ok(samples) => enq_samples.push(samples),
            Err(e) => eprintln!("producer {idx}: task join error: {e}"),
        }
    }
    producers_done.store(true, Ordering::Relaxed);
    eprintln!("producers done, draining backlog...");

    let mut done_samples: Vec<Vec<DoneSample>> = Vec::with_capacity(n_workers);
    for (idx, handle) in worker_handles.into_iter().enumerate() {
        match handle.await {
            Ok(samples) => done_samples.push(samples),
            Err(e) => eprintln!("worker {idx}: task join error: {e}"),
        }
    }
    let depth_samples = watcher.await.unwrap_or_default();

    // Merge into per-second windows.
    #[derive(Default)]
    struct Window {
        enq: Vec<u64>,
        e2e: Vec<u64>,
        claim: Vec<u64>,
        ack: Vec<u64>,
        pending: Option<i64>,
    }
    let mut windows: Vec<Window> = Vec::new();
    let window_at = |sec: usize, windows: &mut Vec<Window>| {
        while windows.len() <= sec {
            windows.push(Window::default());
        }
    };
    for samples in enq_samples {
        for (elapsed_us, latency_us) in samples {
            let sec = (elapsed_us / 1_000_000) as usize;
            window_at(sec, &mut windows);
            windows[sec].enq.push(latency_us);
        }
    }
    for samples in done_samples {
        for (elapsed_us, e2e_us, claim_us, ack_us) in samples {
            let sec = (elapsed_us / 1_000_000) as usize;
            window_at(sec, &mut windows);
            windows[sec].e2e.push(e2e_us);
            windows[sec].claim.push(claim_us);
            windows[sec].ack.push(ack_us);
        }
    }
    for (sec, pending) in depth_samples {
        let sec = sec as usize;
        window_at(sec, &mut windows);
        windows[sec].pending = Some(pending);
    }

    println!(
        "window_sec,n_enq,enq_p99_us,n_done,e2e_p50_us,e2e_p95_us,e2e_p99_us,claim_p99_us,ack_p99_us,pending"
    );
    for (i, mut w) in windows.into_iter().enumerate() {
        if w.enq.is_empty() && w.e2e.is_empty() && w.pending.is_none() {
            continue;
        }
        w.enq.sort_unstable();
        w.e2e.sort_unstable();
        w.claim.sort_unstable();
        w.ack.sort_unstable();
        let enq_p99 = if w.enq.is_empty() { 0 } else { pct(&w.enq, 99) };
        let (e2e_p50, e2e_p95, e2e_p99, claim_p99, ack_p99) = if w.e2e.is_empty() {
            (0, 0, 0, 0, 0)
        } else {
            (
                pct(&w.e2e, 50),
                pct(&w.e2e, 95),
                pct(&w.e2e, 99),
                pct(&w.claim, 99),
                pct(&w.ack, 99),
            )
        };
        let pending = w.pending.map_or(String::new(), |p| p.to_string());
        println!(
            "{i},{},{enq_p99},{},{e2e_p50},{e2e_p95},{e2e_p99},{claim_p99},{ack_p99},{pending}",
            w.enq.len(),
            w.e2e.len(),
        );
    }

    let queue =
        Arc::try_unwrap(queue).map_err(|_| "queue still has outstanding references at shutdown")?;
    queue.close().await?;

    // Confirm the metrics path produced data under load (counters/histograms
    // emitted inline, gauges from the sampler, SlateDB metrics via the bridge).
    #[cfg(feature = "metrics")]
    taquba_bencher::report_metrics(&prometheus);
    Ok(())
}
