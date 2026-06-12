# taquba-workflow benchmarks

Performance benchmarks for the `taquba-workflow` crate. Each benchmark
is a self-contained `#[tokio::main]` binary that emits a CSV time
series to `stdout`; status and progress prints go to `stderr` so the
data stream stays clean. The `[[bench]]` entries in
`taquba-workflow/Cargo.toml` set `harness = false`, so each file runs
as a plain binary. Conventions (env-var parameters, `STORE_LATENCY_MS`
throttling, CSV output) match the `taquba` crate's benches; see
`taquba/benches/README.md`.

## Available benchmarks

| Benchmark | Workload | Question it answers |
|---|---|---|
| `step_transitions` | Submit `N_RUNS` runs of `N_STEPS` steps each; the runner returns `Continue` immediately, so only the runtime's own overhead is measured | What does a step transition cost (persisting the transition, enqueuing the next step, the claim / dispatch round trip), and does it hold while many runs progress concurrently? |

## Running

```bash
# Run with defaults (100 runs of 10 steps, 1ms flush).
cargo bench -p taquba-workflow --bench step_transitions > steps.csv

# Library-default flush interval and injected object-store latency,
# approximating an S3-class backend.
FLUSH_INTERVAL_MS=100 STORE_LATENCY_MS=20 \
    cargo bench -p taquba-workflow --bench step_transitions > steps.csv

# Longer chains at higher worker concurrency.
N_RUNS=200 N_STEPS=20 MAX_CONCURRENT_STEPS=32 \
    cargo bench -p taquba-workflow --bench step_transitions > steps.csv
```

The full parameter list (`N_RUNS`, `N_STEPS`, `MAX_CONCURRENT_STEPS`,
`SUBMIT_CONCURRENCY`, `PAYLOAD_BYTES`, `FLUSH_INTERVAL_MS`,
`STORE_LATENCY_MS`, `DURATION_CAP_SEC`) is documented in the header
comment of the benchmark source.

## Output format

```
window_sec,n_steps,transition_p50_us,transition_p99_us
```

One row per second. `n_steps` counts step completions in that second;
the percentile columns describe the transition latencies that ended in
it, in microseconds, where the transition latency of step k is the
time between step k-1 and step k of the same run completing. A summary
(steps/s, run end-to-end percentiles) prints to stderr.
