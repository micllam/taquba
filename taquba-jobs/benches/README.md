# taquba-jobs benchmarks

Performance benchmarks for the `taquba-jobs` crate. Each benchmark is
a self-contained `#[tokio::main]` binary that emits CSV to `stdout`;
status and progress prints go to `stderr` so the data stream stays
clean. The `[[bench]]` entries in `taquba-jobs/Cargo.toml` set
`harness = false`, so each file runs as a plain binary. Conventions
(env-var parameters, `STORE_LATENCY_MS` throttling, CSV output) match the
`taquba` crate's benches; see `taquba/benches/README.md`.

## Available benchmarks

| Benchmark | Workload | Question it answers |
|---|---|---|
| `fanout` | Submit `N_JOBS` jobs concurrently with idempotency keys and await every handle, then submit the identical batch again | What throughput does a typed-job fan-out sustain cold (idempotency record, enqueue, claim, run, result-blob write, completion notification, result read), and what does the idempotent short-circuit that crash-resume relies on cost? |

## Running

```bash
# Run with defaults (500 jobs per phase, no simulated work, 1ms flush).
cargo bench -p taquba-jobs --bench fanout > fanout.csv

# Library-default flush interval and injected object-store latency,
# approximating an S3-class backend.
FLUSH_INTERVAL_MS=100 STORE_LATENCY_MS=20 \
    cargo bench -p taquba-jobs --bench fanout > fanout.csv

# Larger fan-out with simulated per-job work.
N_JOBS=2000 JOB_WORK_MS=50 MAX_CONCURRENT=200 \
    cargo bench -p taquba-jobs --bench fanout > fanout.csv
```

The full parameter list (`N_JOBS`, `JOB_WORK_MS`, `MAX_CONCURRENT`,
`FLUSH_INTERVAL_MS`, `STORE_LATENCY_MS`) is documented in the header
comment of the benchmark source.

## Output format

```
phase,jobs,secs,jobs_per_sec
```

One row per phase. The `cold` row measures the full submit-to-result
round trip for a fresh batch; the `resubmit` row measures the same
batch submitted again, where every handle short-circuits to its cached
result blob without re-running the job.
