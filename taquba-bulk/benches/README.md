# taquba-bulk benchmarks

Performance benchmarks for the `taquba-bulk` crate. Each benchmark is
a self-contained `#[tokio::main]` binary that emits a CSV time series
to `stdout`; status and progress prints go to `stderr` so the data
stream stays clean. The `[[bench]]` entries in
`taquba-bulk/Cargo.toml` set `harness = false`, so each file runs as a
plain binary. Conventions (env-var parameters, `STORE_LATENCY_MS`
throttling, CSV output) match the `taquba` crate's benches; see
`taquba/benches/README.md`.

## Available benchmarks

| Benchmark | Workload | Question it answers |
|---|---|---|
| `bulk_throughput` | Run `N_ITEMS` items through a pipeline of `N_PHASES` memoized phases that do no work | What is the per-item orchestration overhead (run submission, the single workflow step, one memo write per phase, terminal accounting), and what item throughput does it bound? |
| `resume_replay` | Each item fails transiently on its first attempt after completing `FAIL_AT` phases of `PHASE_WORK_MS` simulated work; the retry re-enters the pipeline. `MEMO=0` runs the identical workload without memoization | How much completed work does `BulkCtx::memoized` save a retried item? The memoized run should re-execute zero completed phases; the `MEMO=0` run re-pays them. |

## Running

```bash
# Run with defaults (500 items, 3 no-op phases, 1ms flush).
cargo bench -p taquba-bulk --bench bulk_throughput > bulk.csv

# Library-default flush interval and injected object-store latency,
# approximating an S3-class backend.
FLUSH_INTERVAL_MS=100 STORE_LATENCY_MS=20 \
    cargo bench -p taquba-bulk --bench bulk_throughput > bulk.csv

# Resume: every item retries after 2 of 4 phases; compare the phase
# execution count against the same run with MEMO=0.
cargo bench -p taquba-bulk --bench resume_replay > resume.csv
MEMO=0 cargo bench -p taquba-bulk --bench resume_replay > resume_bare.csv
```

The full parameter lists (`N_ITEMS`, `N_PHASES`, `MAX_CONCURRENT`,
`FAIL_AT`, `PHASE_WORK_MS`, `MEMO`, `FLUSH_INTERVAL_MS`,
`STORE_LATENCY_MS`) are documented in the header comments of the
benchmark sources.

## Output format

Both benchmarks print:

```
window_sec,completed
```

One row per second with the cumulative number of terminal items. The
summary printed to stderr reports items/s and succeeded / failed
counts for `bulk_throughput`, and items/s plus the phase execution
count against the no-retry floor for `resume_replay` (executions above
the floor are phases a retry re-executed).
