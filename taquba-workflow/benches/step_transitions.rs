// cargo bench -p taquba-workflow --bench step_transitions > steps.csv
//
// Step-transition benchmark for the workflow runtime. Submits N_RUNS
// runs of N_STEPS steps each; the runner returns Continue immediately
// with the payload unchanged, so the measured cost is the runtime's
// own overhead: persisting the step transition, enqueuing the next
// step, and the claim / dispatch round trip back into the runner. The
// transition latency of step k is the time between step k-1 and step k
// of the same run completing.
//
// Parameters (env vars, all optional).
//   N_RUNS                concurrent workflow runs (default 100)
//   N_STEPS               steps per run, including the terminal one
//                         (default 10)
//   MAX_CONCURRENT_STEPS  worker concurrency (default 8, the runtime
//                         default)
//   SUBMIT_CONCURRENCY    concurrent submit calls (default 32)
//   PAYLOAD_BYTES         per-step payload size, min 8 (default 64)
//   FLUSH_INTERVAL_MS     SlateDB WAL flush interval in ms (default 1)
//   STORE_LATENCY_MS      injected object-store latency per call (default 0).
//                         When set, the in-memory store is wrapped in
//                         object_store's ThrottledStore so every get, put,
//                         list, and delete sleeps this long before running,
//                         approximating an S3-class backend.
//   DURATION_CAP_SEC      abort threshold (default 600)
//
// Output (stdout): CSV with header
// `window_sec,n_steps,transition_p50_us,transition_p99_us`, one row per
// second, counting step completions in that second and the
// distribution of transition latencies that ended in it. A summary
// (steps/s, run end-to-end percentiles) goes to stderr so stdout stays
// a clean data stream.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use taquba::object_store::ObjectStore;
use taquba::object_store::memory::InMemory;
use taquba::object_store::throttle::{ThrottleConfig, ThrottledStore};
use taquba::{OpenOptions, Queue, QueueConfig};
use taquba_workflow::{
    RunOutcome, RunSpec, Step, StepError, StepOutcome, StepRunner, TerminalHook, TerminalStatus,
    WorkflowRuntime,
};

fn env_var<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<T>().ok())
        .unwrap_or(default)
}

fn pct(sorted: &[u64], p: usize) -> u64 {
    let last = sorted.len() - 1;
    sorted[(sorted.len() * p / 100).min(last)]
}

fn store_with_latency(latency_ms: u64) -> Arc<dyn ObjectStore> {
    if latency_ms > 0 {
        let wait = Duration::from_millis(latency_ms);
        let config = ThrottleConfig {
            wait_delete_per_call: wait,
            wait_get_per_call: wait,
            wait_list_per_call: wait,
            wait_put_per_call: wait,
            ..ThrottleConfig::default()
        };
        Arc::new(ThrottledStore::new(InMemory::new(), config))
    } else {
        Arc::new(InMemory::new())
    }
}

/// Step completion sample: (elapsed_us, run_idx, step_number).
type StepSample = (u64, u32, u32);

struct BenchRunner {
    n_steps: u32,
    bench_start: Instant,
    samples: Arc<Mutex<Vec<StepSample>>>,
}

impl StepRunner for BenchRunner {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        let elapsed_us = self.bench_start.elapsed().as_micros() as u64;
        let run_idx = u32::from_le_bytes(step.payload[..4].try_into().unwrap());
        self.samples
            .lock()
            .unwrap()
            .push((elapsed_us, run_idx, step.step_number));
        if step.step_number + 1 >= self.n_steps {
            Ok(StepOutcome::Succeed { result: Vec::new() })
        } else {
            Ok(StepOutcome::Continue {
                payload: step.payload.clone(),
            })
        }
    }
}

struct CountingHook {
    terminated: Arc<AtomicUsize>,
}

impl TerminalHook for CountingHook {
    async fn on_termination(&self, outcome: &RunOutcome) {
        if !matches!(outcome.status, TerminalStatus::Succeeded) {
            eprintln!(
                "run {} terminated as {:?}: {:?}",
                outcome.run_id, outcome.status, outcome.error,
            );
        }
        self.terminated.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n_runs: usize = env_var("N_RUNS", 100);
    let n_steps: u32 = env_var("N_STEPS", 10).max(1);
    let max_concurrent_steps: usize = env_var("MAX_CONCURRENT_STEPS", 8).max(1);
    let submit_concurrency: usize = env_var("SUBMIT_CONCURRENCY", 32).max(1);
    let payload_bytes: usize = env_var("PAYLOAD_BYTES", 64).max(8);
    let flush_interval_ms: u64 = env_var("FLUSH_INTERVAL_MS", 1);
    let store_latency_ms: u64 = env_var("STORE_LATENCY_MS", 0);
    let cap_sec: u64 = env_var("DURATION_CAP_SEC", 600);

    eprintln!(
        "step_transitions: runs={n_runs}, steps={n_steps}, \
         max_concurrent_steps={max_concurrent_steps}, \
         submit_concurrency={submit_concurrency}, payload={payload_bytes}B, \
         flush_interval={flush_interval_ms}ms, store_latency={store_latency_ms}ms",
    );

    let store = store_with_latency(store_latency_ms);
    let queue = Arc::new(
        Queue::open_with_options(
            store.clone(),
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

    let bench_start = Instant::now();
    let samples: Arc<Mutex<Vec<StepSample>>> =
        Arc::new(Mutex::new(Vec::with_capacity(n_runs * n_steps as usize)));
    let terminated = Arc::new(AtomicUsize::new(0));
    let runtime = WorkflowRuntime::builder(
        queue.clone(),
        store,
        BenchRunner {
            n_steps,
            bench_start,
            samples: samples.clone(),
        },
        CountingHook {
            terminated: terminated.clone(),
        },
    )
    .max_concurrent_steps(max_concurrent_steps)
    .build();

    // Worker: runs until every submitted run has terminated.
    let worker = {
        let runtime = runtime.clone();
        let terminated = terminated.clone();
        tokio::spawn(async move {
            let all_terminated = async move {
                while terminated.load(Ordering::SeqCst) < n_runs {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            };
            runtime.run(all_terminated).await
        })
    };

    // Submissions: run_idx is stored in the payload's first 4 bytes so the
    // runner can attribute samples without parsing run ids.
    let mut submit_times: Vec<(u32, u64)> = Vec::with_capacity(n_runs);
    let mut set = tokio::task::JoinSet::new();
    for run_idx in 0..n_runs as u32 {
        while set.len() >= submit_concurrency {
            let (idx, at) = set.join_next().await.unwrap()??;
            submit_times.push((idx, at));
        }
        let runtime = runtime.clone();
        let mut payload = vec![0u8; payload_bytes];
        payload[..4].copy_from_slice(&run_idx.to_le_bytes());
        set.spawn(async move {
            let submit_at = bench_start.elapsed().as_micros() as u64;
            runtime
                .submit(RunSpec {
                    input: payload,
                    ..RunSpec::default()
                })
                .await
                .map(|_| (run_idx, submit_at))
        });
    }
    while let Some(joined) = set.join_next().await {
        let (idx, at) = joined??;
        submit_times.push((idx, at));
    }
    eprintln!(
        "submitted {n_runs} runs in {:.2}s",
        bench_start.elapsed().as_secs_f64(),
    );

    // Wait for the worker, which exits once every run has terminated.
    let cap = Duration::from_secs(cap_sec);
    match tokio::time::timeout(cap, worker).await {
        Ok(joined) => joined??,
        Err(_) => return Err("duration cap reached before all runs terminated".into()),
    }
    let wall_secs = bench_start.elapsed().as_secs_f64();
    let total_steps = n_runs * n_steps as usize;
    eprintln!(
        "completed {n_runs} runs ({total_steps} steps) in {wall_secs:.2}s \
         ({:.0} steps/s)",
        total_steps as f64 / wall_secs,
    );

    // Per-run step series: transitions and end-to-end latency. The
    // runner (inside the runtime) still holds the samples handle, so
    // take the contents rather than unwrapping the Arc.
    let samples = std::mem::take(&mut *samples.lock().unwrap());
    let mut per_run: Vec<Vec<(u32, u64)>> = vec![Vec::with_capacity(n_steps as usize); n_runs];
    for (elapsed_us, run_idx, step_number) in samples {
        per_run[run_idx as usize].push((step_number, elapsed_us));
    }

    #[derive(Default)]
    struct Window {
        n_steps: usize,
        transitions: Vec<u64>,
    }
    let mut windows: Vec<Window> = Vec::new();
    let mut e2e: Vec<u64> = Vec::with_capacity(n_runs);
    let submit_at_for: std::collections::HashMap<u32, u64> = submit_times.into_iter().collect();
    for (run_idx, mut steps) in per_run.into_iter().enumerate() {
        steps.sort_unstable();
        // Retried steps record one sample per attempt; keep the last.
        steps.dedup_by_key(|(step_number, _)| *step_number);
        for window in steps.windows(2) {
            let ((_, prev_us), (_, cur_us)) = (window[0], window[1]);
            let sec = (cur_us / 1_000_000) as usize;
            while windows.len() <= sec {
                windows.push(Window::default());
            }
            windows[sec].transitions.push(cur_us - prev_us);
        }
        for (_, elapsed_us) in &steps {
            let sec = (elapsed_us / 1_000_000) as usize;
            while windows.len() <= sec {
                windows.push(Window::default());
            }
            windows[sec].n_steps += 1;
        }
        if let (Some(&submit_at), Some((_, last_us))) =
            (submit_at_for.get(&(run_idx as u32)), steps.last())
        {
            e2e.push(last_us - submit_at);
        }
    }

    println!("window_sec,n_steps,transition_p50_us,transition_p99_us");
    for (i, mut w) in windows.into_iter().enumerate() {
        if w.n_steps == 0 {
            continue;
        }
        w.transitions.sort_unstable();
        let (p50, p99) = if w.transitions.is_empty() {
            (0, 0)
        } else {
            (pct(&w.transitions, 50), pct(&w.transitions, 99))
        };
        println!("{i},{},{p50},{p99}", w.n_steps);
    }

    e2e.sort_unstable();
    if !e2e.is_empty() {
        eprintln!(
            "summary: run e2e p50={}us p99={}us over {} runs",
            pct(&e2e, 50),
            pct(&e2e, 99),
            e2e.len(),
        );
    }

    drop(runtime);
    let queue =
        Arc::try_unwrap(queue).map_err(|_| "queue still has outstanding references at shutdown")?;
    queue.close().await?;
    Ok(())
}
