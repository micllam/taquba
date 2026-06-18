# Benchmark results

Recorded results from runs of the benchmarks in this crate. This file
is the single source of truth for published taquba performance numbers;
READMEs and other docs reference it rather than inlining figures, so a
number is always tied to the version and environment that produced it.

## Conventions

- **Append-only.** Add a new entry per run; never edit or overwrite a
  past entry. An old entry is stamped with its commit and date.
- **One entry per environment.** A run against the in-memory store and a
  run against real S3 are separate entries, never merged.
- **Reproducible.** Each entry records the exact commit, instance type,
  store, and parameters, so a reader can recreate the same environment
  (see `terraform/`) and rerun it.
- **The commit is the provenance anchor.** This crate is unpublished and
  has no version of its own. Because it and `taquba` move together in one
  repo, the entry's commit pins everything at once: taquba's source, the
  bench code, and the bench's default parameters. Two entries are
  comparable only if the bench code did not change between their commits.
- **A changed benchmark starts a new series.** When a bench's workload,
  defaults, or what it measures changes in a way that affects its
  numbers, note the change in the next entry and treat that bench's
  earlier entries as a closed series rather than continuing the same
  table. The commits differ, so the provenance holds, but the note is
  what tells a reader the numbers are no longer directly comparable.
- **Raw output is not committed.** Criterion-style CSV streams are
  transient build artifacts. Summarise the relevant percentiles here and
  discard the raw CSV, or archive it in the run's object-store prefix.

When a published claim depends on a number, cite the entry it comes
from (date and commit) so readers can see which run it was based on,
even after newer numbers replace it.

## Entry template

Copy this block to the top of the log below for each new run.

```
### YYYY-MM-DD - <one-line summary>

- **taquba:** <version> (`<commit>`)
- **Benchmark:** <bench name> (`benches/<crate>/<file>.rs`)
- **Host:** <instance type>, <vCPU>/<RAM>, <region>
- **Store:** <in-memory | file:// | real S3 | real GCS | real Azure>, region, storage class (record the class, not the bucket name)
- **Parameters:** `NAME=value NAME=value ...`
- **Command:** `cargo bench -p taquba-bencher [--features <cloud>] --bench <name>`

| Metric | Value |
|---|---|
| <e.g. claim p50> | <value> |
| <e.g. claim p99> | <value> |
| <e.g. throughput> | <value> |

Notes: <anything that shapes interpretation: warm-up, drift across
windows, backlog behaviour, deviations from defaults>.
```

## Log

### 2026-06-18 - cold_start (restart with sparse live jobs) on real S3

- **taquba:** 0.8.0 (`19888b0`)
- **Benchmark:** cold_start (`benches/taquba/cold_start.rs`)
- **Host:** m7i.xlarge, 4 vCPU / 16 GiB, us-east-1
- **Store:** real S3, Standard storage class, us-east-1 (same region as host)
- **Parameters:** `N_HISTORY=20000 N_LIVE=100 FLUSH_INTERVAL_MS=1` payload 64 B
- **Command:** `cargo bench -p taquba-bencher --features aws --bench cold_start`

| Metric | Value |
|---|---|
| Reopen (recovery) | 622 ms |
| First claim after reopen | 45.9 ms |
| Warm claim p50 | 55.9 ms |
| Warm claim p99 | 99.2 ms |

Notes: **no cold-start penalty.** The first post-restart claim (45.9 ms) is among the lowest-latency of the 100, not a spike, so re-establishing the claim cursor after reopen costs no more than a normal claim; reopen is ~0.6 s. But every post-restart claim is ~56 ms p50 / ~99 ms p99 and does not recover toward the ~1 ms of a deep drain within 100 claims. With 100 live jobs sparse among 20,000 tombstones, this is consistent with each claim scanning the tombstone band, likely amplified by a cold block cache after reopen whose reads hit S3. It matches the shallow-queue steady runs. Restart itself is fast; the per-claim cost appears to come from scanning a large, not-yet-compacted tombstone band rather than from the restart. (These mechanisms are inferred from the architecture; the benches measure the latencies, not the scan or cache directly.)

### 2026-06-18 - claim_drain (deep-backlog drain) on real S3

- **taquba:** 0.8.0 (`19888b0`)
- **Benchmark:** claim_drain (`benches/taquba/claim_drain.rs`)
- **Host:** m7i.xlarge, 4 vCPU / 16 GiB, us-east-1
- **Store:** real S3, Standard storage class, us-east-1 (same region as host)
- **Parameters:** `N_JOBS=20000 N_WORKERS=50 FLUSH_INTERVAL_MS=1` payload 64 B
- **Command:** `cargo bench -p taquba-bencher --features aws --bench claim_drain`

| Metric | Value |
|---|---|
| Drain throughput | ~685 claims/s |
| claim p50 | ~1.0 ms (flat) |
| claim p95 | ~2.3 ms (flat) |
| claim p99 | ~2.5 ms (flat) |

Notes: claim latency is **flat across the entire 20k-job drain** (p50 ~1 ms, p99 ~2.5 ms, no window-to-window growth). This is consistent with the proposed steady_state mechanism: draining a deep prefill likely keeps live jobs dense at the scan front, so claims stay low-latency regardless of accumulating tombstones, the same regime as the saturated (deep-backlog) steady run. Claim-latency growth appears only when the live queue is kept shallow by balanced enqueue/drain (see the 700/s steady runs), not during a deep drain. Claim latency does not grow across a drain.

### 2026-06-18 - steady_state load sweep on real S3

- **taquba:** 0.8.0 (`19888b0`)
- **Benchmark:** steady_state (`benches/taquba/steady_state.rs`)
- **Host:** m7i.xlarge, 4 vCPU / 16 GiB, us-east-1
- **Store:** real S3, Standard storage class, us-east-1 (same region as host)
- **Common parameters:** `N_WORKERS=50 FLUSH_INTERVAL_MS=1` payload 64 B, `STORE_LATENCY_MS=0`
- **Command:** `cargo bench -p taquba-bencher --features aws --bench steady_state`

Operating points, varying offered rate / producer concurrency / claim batch / duration:

| Offered | Producers | Batch | Dur | Achieved | e2e p50 | e2e p99 | pending | claim p99 |
|---|---|---|---|---|---|---|---|---|
| 500/s | 4 | 1 | 60 s | ~115/s (enqueue-bound) | ~55 ms | ~120 ms | ~0 | 0.5 -> 11 ms |
| 700/s | 50 | 16 | 60 s | ~700/s (sustained) | ~350 ms | ~600-900 ms | ~0-53 | 1 -> 260 ms |
| 3000/s | 50 | 16 | 60 s | ~1,000/s (saturated) | ~1.5 s | ~2.2 s | 20 -> 1,500 | flat ~1.3 ms |
| 700/s | 50 | 16 | 300 s | ~700/s (sustained) | ~350-450 ms (spikes 1-2 s) | ~700 ms-2.2 s | ~0-50 (spikes ~470) | sawtooth 1-5 ms <-> 200-470 ms |

Notes:
- **e2e is floored by S3 PUT (durable-write) latency, not the flush interval.** Each enqueue and ack is a durable S3 write (`enq_p99`/`ack_p99` ~50-260 ms), so a 1 ms flush does not yield single-digit-ms e2e on real S3; S3 round-trip sets the floor (~55 ms p50 at low load).
- **Throughput scales with producer concurrency:** ~115/s at 4 producers to ~1,000/s at 50, consistent with WAL group commit batching concurrent durable writes. The ~1,000/s plateau appears to be the single-writer durable-settlement ceiling; beyond it the backlog grows unbounded and e2e degrades to seconds.
- **Latency rises steeply with load:** e2e p50 ~55 ms (115/s) -> ~350 ms (700/s) -> ~1.5 s (1,000/s).
- **Claim latency depends on live-queue depth versus the tombstone band.** Low under deep backlog (run 3000/s: live jobs at the scan front, ~1.3 ms flat), but grows when the live queue is shallow while churning fast (run 700/s, 60 s: 1 -> 260 ms), consistent with `pending:` tombstones accumulating faster than compaction reclaims them. `claim_batch` amortizes per-claim transaction overhead but, on this reading, not the scan cost. The 300 s run shows the steady-state behavior: claim latency does **not** grow unbounded but follows a **sawtooth**, climbing to ~200-470 ms p99 then resetting to ~1-5 ms (clear resets at t~78, 121, 154, 230, 255 s), a pattern we attribute to periodic compaction reclaiming the tombstone band.
- **Throughput dips periodically.** Over 300 s there are periodic throughput dips that drive backlog spikes (pending to ~200-470) and e2e spikes to ~1-2 s, with an occasional ~1 s near-stall (one window completing ~40 ops) followed by a catch-up burst. These coincide with the claim-latency resets above, so we attribute them to compaction transiently slowing the single writer, though the benches do not observe compaction directly. Average throughput holds ~700/s, but **tail latency is governed by these periodic events**, not steady-state op cost.
- The 500/s row did not reach its offered rate: 4 producers each block ~35 ms on a durable S3 write, capping enqueue at ~115/s; it is recorded as the low-load latency datapoint, not a throughput measurement.

Add entries above this line, newest first.
