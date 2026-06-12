# taquba-bencher

Benchmark binaries for the taquba workspace. This crate is an internal
workspace member (`publish = false`): it consumes `taquba`,
`taquba-workflow`, `taquba-bulk`, and `taquba-jobs` as normal
dependencies, so the published crates carry no bench targets or
bench-only dependencies.

Each benchmark is a self-contained `#[tokio::main]` binary that
exercises a workload and emits a CSV time series to `stdout`; status
and progress prints go to `stderr` so the data stream stays clean. The
`[[bench]]` entries in `Cargo.toml` set `harness = false`, so each
file runs as a plain binary: `cargo bench` discovers them but does not
wrap them in libtest's bench harness. Bench files are grouped under
`benches/<crate>` by the crate they exercise; the grouping does not
affect target names or invocations.

Parameters are env vars; each benchmark's full parameter list is
documented in the header comment of its source file.

## Available benchmarks

### taquba (core queue)

| Benchmark | Workload | Question it answers |
|---|---|---|
| `claim_drain` | Pre-fill `N_JOBS` jobs, drain with `N_WORKERS` workers, record per-claim latency | Does claim latency stay flat across a drain, or grow with `pending:` tombstone density? |
| `steady_state` | Producers enqueue at `RATE` jobs/sec for `DURATION_SEC` while `N_WORKERS` claim and ack concurrently, then drain | Do throughput and end-to-end latency hold over a sustained run, or degrade as compaction and tombstones accumulate? Does the backlog stay bounded at the offered rate? |
| `cold_start` | Build a history of `N_HISTORY` acked jobs plus `N_LIVE` pending jobs, close, reopen the same store, claim the live jobs serially | What does the first claim after a restart cost when the in-memory scan bound is gone and the claim falls back to a front prefix scan across the tombstone band, and how quickly do later claims recover? |
| `reaper_storm` | Abandon `N_EXPIRED` claims with expired leases (a simulated crash), reopen, and let the reaper requeue them while a second queue carries live traffic | How long does a mass lease-expiry sweep take, and how much does it disturb claim and end-to-end latency on a concurrently active queue? |

### taquba-workflow

| Benchmark | Workload | Question it answers |
|---|---|---|
| `step_transitions` | Submit `N_RUNS` runs of `N_STEPS` steps each; the runner returns `Continue` immediately, so only the runtime's own overhead is measured | What does a step transition cost (persisting the transition, enqueuing the next step, the claim / dispatch round trip), and does it hold while many runs progress concurrently? |

### taquba-bulk

| Benchmark | Workload | Question it answers |
|---|---|---|
| `bulk_throughput` | Run `N_ITEMS` items through a pipeline of `N_PHASES` memoized phases that do no work | What is the per-item orchestration overhead (run submission, the single workflow step, one memo write per phase, terminal accounting), and what item throughput does it bound? |
| `resume_replay` | Each item fails transiently on its first attempt after completing `FAIL_AT` phases of `PHASE_WORK_MS` simulated work; the retry re-enters the pipeline. `MEMO=0` runs the identical workload without memoization | How much completed work does `BulkCtx::memoized` save a retried item? The memoized run should re-execute zero completed phases; the `MEMO=0` run re-pays them. |

### taquba-jobs

| Benchmark | Workload | Question it answers |
|---|---|---|
| `fanout` | Submit `N_JOBS` jobs concurrently with idempotency keys and await every handle, then submit the identical batch again | What throughput does a typed-job fan-out sustain cold (idempotency record, enqueue, claim, run, result-blob write, completion notification, result read), and what does the idempotent short-circuit that crash-resume relies on cost? |

## Running

```bash
# Run with defaults (5K jobs, 50 workers, 64-byte payload, 1ms flush).
cargo bench -p taquba-bencher --bench claim_drain > drain.csv

# Override parameters via env vars.
N_JOBS=100000 N_WORKERS=200 \
    cargo bench -p taquba-bencher --bench claim_drain > drain.csv

# Sustain 500 jobs/sec for a minute.
cargo bench -p taquba-bencher --bench steady_state > steady.csv

# Same, with 20ms of injected object-store latency per call to
# approximate an S3-class backend instead of the in-memory store.
STORE_LATENCY_MS=20 RATE=200 \
    cargo bench -p taquba-bencher --bench steady_state > steady.csv

# Workers claim in batches of 16 via Queue::claim_batch, amortizing
# the per-claim lock hold and commit while draining a backlog.
CLAIM_BATCH=16 RATE=3000 N_PRODUCERS=12 \
    cargo bench -p taquba-bencher --bench steady_state > steady.csv

# Restart cost: 20K acked jobs of history, then measure the reopen
# and the first claims against the cold claim cursor.
cargo bench -p taquba-bencher --bench cold_start > cold.csv

# Crash recovery: 5K abandoned claims swept by the reaper while a
# live queue sustains 500 jobs/sec.
cargo bench -p taquba-bencher --bench reaper_storm > storm.csv

# Spread the load across 100 queues (one worker each), exercising the
# global reaper / scheduler prefix scans and per-queue claim state.
N_QUEUES=100 N_WORKERS=100 RATE=1000 \
    cargo bench -p taquba-bencher --bench steady_state > steady.csv

# Workflow step transitions with the library-default flush interval
# and injected object-store latency.
FLUSH_INTERVAL_MS=100 STORE_LATENCY_MS=20 \
    cargo bench -p taquba-bencher --bench step_transitions > steps.csv

# Longer workflow chains at higher worker concurrency.
N_RUNS=200 N_STEPS=20 MAX_CONCURRENT_STEPS=32 \
    cargo bench -p taquba-bencher --bench step_transitions > steps.csv

# Bulk per-item overhead (500 items, 3 no-op phases).
cargo bench -p taquba-bencher --bench bulk_throughput > bulk.csv

# Bulk resume: every item retries after 2 of 4 phases; compare the
# phase execution count against the same run with MEMO=0.
cargo bench -p taquba-bencher --bench resume_replay > resume.csv
MEMO=0 cargo bench -p taquba-bencher --bench resume_replay > resume_bare.csv

# Typed-job fan-out with simulated per-job work.
N_JOBS=2000 JOB_WORK_MS=50 MAX_CONCURRENT=200 \
    cargo bench -p taquba-bencher --bench fanout > fanout.csv
```

`STORE_LATENCY_MS` wraps the in-memory store in `object_store`'s
`ThrottledStore`, so every get, put, list, and delete sleeps that long
before running. It is available on every benchmark.

## Running against real object storage

By default every benchmark runs on an in-memory store. Set `STORE_URL`
to run against a real backend instead:

```bash
# Local filesystem (no cargo feature needed).
STORE_URL=file:///tmp/taquba-bench \
    cargo bench -p taquba-bencher --bench steady_state > steady.csv

# S3: requires the `aws` feature. Credentials, region, and endpoint
# are read from the standard env vars (AWS_ACCESS_KEY_ID,
# AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ENDPOINT for S3-compatible
# stores).
STORE_URL=s3://my-bench-bucket/taquba \
    cargo bench -p taquba-bencher --features aws \
    --bench steady_state > steady.csv
```

`gs://` URLs (feature `gcp`, `GOOGLE_*` env vars) and `az://` URLs
(feature `azure`, `AZURE_*` env vars) work the same way.

Each run is rooted under a fresh `bench-<unix-millis>` prefix inside
the URL's path, printed to stderr at startup, so a rerun never
observes a previous run's state. Bench data is left in place on exit;
delete the run prefixes afterwards or configure an object-lifecycle
rule on the parent prefix. `STORE_LATENCY_MS` applies only to the
in-memory store and is rejected when `STORE_URL` is set. Run from
compute in the bucket's region; over a longer network path the round
trip to the store dominates every number.

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

For `steady_state`:

```
window_sec,n_enq,enq_p99_us,n_done,e2e_p50_us,e2e_p95_us,e2e_p99_us,claim_p99_us,ack_p99_us,pending
```

One row per second of the run, including the drain after producers
stop. `n_enq` and `n_done` count enqueues and acks completed in that
second. `e2e_*` is enqueue-call start to ack completion, `claim_p99_us`
and `ack_p99_us` are per-operation latencies, all in microseconds.
`pending` is the queue depth sampled once that second; a depth that
grows across windows means the offered rate exceeds what the queue
sustains.

For `cold_start`:

```
claim_idx,claim_us
```

One row per post-restart claim, in claim order. Row 0 is the first
claim after the reopen, which re-establishes the claim cursor's scan
bound with a front prefix scan across the history's tombstone band;
later rows show the warm path. Reopen time and a summary (first claim
versus warm percentiles) print to stderr.

For `reaper_storm`:

```
window_sec,storm_claimed,storm_pending,n_done,e2e_p50_us,e2e_p99_us,claim_p99_us
```

One row per second of the measured phase. `storm_claimed` counts
abandoned claims the sweep has not yet requeued and `storm_pending`
counts requeued ones, so the sweep's progress and rate read directly
off those two columns. The remaining columns describe the live queue:
acks completed in that second, enqueue-to-ack latency, and per-claim
latency, all in microseconds. Rows before the first reaper tick give
the undisturbed baseline to compare against.

For `step_transitions`:

```
window_sec,n_steps,transition_p50_us,transition_p99_us
```

One row per second. `n_steps` counts step completions in that second;
the percentile columns describe the transition latencies that ended in
it, in microseconds, where the transition latency of step k is the
time between step k-1 and step k of the same run completing. A summary
(steps/s, run end-to-end percentiles) prints to stderr.

For `bulk_throughput` and `resume_replay`:

```
window_sec,completed
```

One row per second with the cumulative number of terminal items. The
summary printed to stderr reports items/s and succeeded / failed
counts for `bulk_throughput`, and items/s plus the phase execution
count against the no-retry floor for `resume_replay` (executions above
the floor are phases a retry re-executed).

For `fanout`:

```
phase,jobs,secs,jobs_per_sec
```

One row per phase. The `cold` row measures the full submit-to-result
round trip for a fresh batch; the `resubmit` row measures the same
batch submitted again, where every handle short-circuits to its cached
result blob without re-running the job.

## When to add a new benchmark here

New entries make sense when:

- A user-visible perf claim in a README or CHANGELOG would
  benefit from a reproducible measurement.
- A code change is plausibly perf-sensitive and we want to gate
  the regression check on a numeric output rather than reading
  the diff.
