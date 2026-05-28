# Taquba benchmarks

Performance benchmarks for the `taquba` crate. Each benchmark is a
self-contained `#[tokio::main]` binary that pre-fills a queue,
exercises a workload, and emits a CSV time series to `stdout`. Status
and progress prints go to `stderr` so the data stream stays clean.

The `[[bench]]` entries in `taquba/Cargo.toml` set `harness = false`,
so each file runs as a plain binary: `cargo bench` discovers them
but does not wrap them in libtest's bench harness.

## Available benchmarks

| Benchmark | Workload | Question it answers |
|---|---|---|
| `claim_drain` | Pre-fill `N_JOBS` jobs, drain with `N_WORKERS` workers, record per-claim latency | Does claim latency stay flat across a drain, or grow with `pending:` tombstone density? |

## Running

```bash
# Run with defaults (5K jobs, 50 workers, 64-byte payload, 1ms flush).
cargo bench -p taquba --bench claim_drain > drain.csv

# Override parameters via env vars.
N_JOBS=100000 N_WORKERS=200 \
    cargo bench -p taquba --bench claim_drain > drain.csv
```

## Output format

Each benchmark prints CSV to `stdout`. The header tells you the
columns; for `claim_drain` they are:

```
window_sec,n_claims,p50_us,p95_us,p99_us
```

One row per drain second. `window_sec` is seconds since the workers
started (drain begins after the pre-fill completes), `n_claims` is
the number of successful claims in that second, and the percentile
columns are claim wall-clock latency in microseconds.

## When to add a new benchmark here

New entries make sense when:

- A user-visible perf claim in the README or CHANGELOG would
  benefit from a reproducible measurement.
- A code change is plausibly perf-sensitive and we want to gate
  the regression check on a numeric output rather than reading
  the diff.
