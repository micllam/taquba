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
| `steady_state` | Producers enqueue at `RATE` jobs/sec for `DURATION_SEC` while `N_WORKERS` claim and ack concurrently, then drain | Do throughput and end-to-end latency hold over a sustained run, or degrade as compaction and tombstones accumulate? Does the backlog stay bounded at the offered rate? |
| `cold_start` | Build a history of `N_HISTORY` acked jobs plus `N_LIVE` pending jobs, close, reopen the same store, claim the live jobs serially | What does the first claim after a restart cost when the in-memory scan bound is gone and the claim falls back to a front prefix scan across the tombstone band, and how quickly do later claims recover? |
| `reaper_storm` | Abandon `N_EXPIRED` claims with expired leases (a simulated crash), reopen, and let the reaper requeue them while a second queue carries live traffic | How long does a mass lease-expiry sweep take, and how much does it disturb claim and end-to-end latency on a concurrently active queue? |

## Running

```bash
# Run with defaults (5K jobs, 50 workers, 64-byte payload, 1ms flush).
cargo bench -p taquba --bench claim_drain > drain.csv

# Override parameters via env vars.
N_JOBS=100000 N_WORKERS=200 \
    cargo bench -p taquba --bench claim_drain > drain.csv

# Sustain 500 jobs/sec for a minute.
cargo bench -p taquba --bench steady_state > steady.csv

# Same, with 20ms of injected object-store latency per call to
# approximate an S3-class backend instead of the in-memory store.
STORE_LATENCY_MS=20 RATE=200 \
    cargo bench -p taquba --bench steady_state > steady.csv

# Workers claim in batches of 16 via Queue::claim_batch, amortizing
# the per-claim lock hold and commit while draining a backlog.
CLAIM_BATCH=16 RATE=3000 N_PRODUCERS=12 \
    cargo bench -p taquba --bench steady_state > steady.csv

# Restart cost: 20K acked jobs of history, then measure the reopen
# and the first claims against the cold claim cursor.
cargo bench -p taquba --bench cold_start > cold.csv

# Crash recovery: 5K abandoned claims swept by the reaper while a
# live queue sustains 500 jobs/sec.
cargo bench -p taquba --bench reaper_storm > storm.csv

# Spread the load across 100 queues (one worker each), exercising the
# global reaper / scheduler prefix scans and per-queue claim state.
N_QUEUES=100 N_WORKERS=100 RATE=1000 \
    cargo bench -p taquba --bench steady_state > steady.csv
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
    cargo bench -p taquba --bench steady_state > steady.csv

# S3: requires the `aws` feature. Credentials, region, and endpoint
# are read from the standard env vars (AWS_ACCESS_KEY_ID,
# AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ENDPOINT for S3-compatible
# stores).
STORE_URL=s3://my-bench-bucket/taquba \
    cargo bench -p taquba --features aws --bench steady_state > steady.csv
```

`gs://` URLs (feature `gcp`, `GOOGLE_*` env vars) and `az://` URLs
(feature `azure`, `AZURE_*` env vars) work the same way. For the
satellite crates' benches, enable the feature on the dependency
instead: `--features taquba/aws`.

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

## When to add a new benchmark here

New entries make sense when:

- A user-visible perf claim in the README or CHANGELOG would
  benefit from a reproducible measurement.
- A code change is plausibly perf-sensitive and we want to gate
  the regression check on a numeric output rather than reading
  the diff.
