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

### 2026-06-23 - 8h time-varying soak on real S3

- **taquba:** 0.8.0 (`8190136`)
- **Benchmark:** steady_state (`benches/taquba/steady_state.rs`), RATE_SCHEDULE mode; plus cold_start measure phase
- **Host:** m7i.xlarge, 4 vCPU / 16 GiB, us-east-1
- **Store:** real S3, Standard storage class, us-east-1 (same region as host)
- **Parameters:** `RATE_SCHEDULE` = 7200s cycle `600:0,2400:400,2400:800,600:1200,1200:300` x 4 (28800s total), `N_PRODUCERS=128 N_WORKERS=128 FLUSH_INTERVAL_MS=50 CLAIM_BATCH=1` payload 64 B, 1 queue
- **Command:** `cargo bench -p taquba-bencher --features aws --bench steady_state` (soak); `STORE_PREFIX=bench-soak1 PHASE=measure cargo bench -p taquba-bencher --features aws --bench cold_start` (post-soak reopen of the same store)

Per-segment metrics, one column per cycle (c1/c2/c3/c4). Terms: a window is one 1-second stats bucket; a segment is one constant-rate phase of the schedule; a cycle is one full pass of the schedule, repeated 4x. The five segments per cycle, in order, are idle (0/s, 600s; omitted below, no throughput to measure), low (400/s, 2400s), high (800/s, 2400s), peak (1200/s, 600s), and cool (300/s, 1200s). e2e p99 and claim p99 are each the mean of the per-window p99 over the segment; pending peak is the max pending in the segment:

| segment | rate | e2e p99 c1/c2/c3/c4 | claim p99 c1/c2/c3/c4 | pending peak c1/c2/c3/c4 |
|---|---|---|---|---|
| low | 400/s | 200 / 199 / 197 / 206 ms | 13 / 15 / 15 / 17 ms | 372 / 375 / 372 / 373 |
| high | 800/s | 253 / 239 / 407 / 251 ms | 17 / 17 / 19 / 19 ms | 1198 / 1628 / 4421 / 1016 |
| peak | 1200/s | 516 / 312 / 2755 / 592 ms | 21 / 17 / 23 / 21 ms | 3381 / 1613 / 11451 / 2752 |
| cool | 300/s | 201 / 195 / 213 / 209 ms | 16 / 16 / 16 / 15 ms | 1022 / 246 / 1467 / 249 |

Idle storage (objects / MB) at the cycle 2/3/4 idle segments (cycle 1's idle precedes any load): 41/55.6, 75/111.5, 108/97.8. Peak storage (at heaviest load): 5517 objects / 780 MB. Cold reopen of the gracefully closed drained store: 1306 ms.

Notes: Zero errors/warns/conflicts over 8h; graceful drain at end. (a) No leak: idle storage bytes stayed bounded (cycle 4 below cycle 3) even as object count rose across the three idle samples, at ~2% of peak storage, with queue `pending`=0 at every idle. (b) Per-operation cost is flat across all 4 cycles and segments (enq/ack/claim p99 unchanged; mean claim p99 13-23 ms throughout); all cross-cycle variation is in backlog and the e2e it induces. The peak segment runs at the commit-rate ceiling (1200 jobs/s = ~2400 commits/s, 1 enqueue + 1 ack each, near the single-writer saturation ceiling measured in the 2026-06-19 sweep at flush 1 ms, though this run used flush 50 ms where the ceiling was not directly measured), where the queue is only marginally stable, so its backlog and e2e are inherently variable across cycles, not an artifact of a single cycle: pending peak 3381/1613/11451/2752, mean e2e p99 516/312/2755/592 ms. Cycle 3 is the most severe: a small, sustained shortfall in achievable commit rate built backlog through its high segment (pending 4421, e2e p99 407 ms vs ~250 ms in the other cycles) and into its peak (11451), where the maximum per-window e2e p99 reached 9888 ms (against the 2755 ms segment mean). That is queueing delay, not slow writes: e2e tracks the backlog divided by the service rate (11451 / ~1200/s = ~9.5 s) while per-op durable-write latency (enq_p99/ack_p99 ~110-320 ms) stayed at its baseline throughout. With only 4 cycles the run does not establish how often this occurs, only that it can; the trigger is undetermined from these metrics (candidates: transient reduction in S3 accepted throughput for the prefix, background compaction, or host contention), and the flat per-op latencies rule out slow individual PUTs. (c) Backlog always recovers: every segment's backlog, including cycle 3's 11451, drained to ~0 by the following cool and idle segments, and throughput returned to the offered rate after each peak. flush_interval=50 ms (between the 1 ms and 100 ms points of the 2026-06-19 sweep).

### 2026-06-19 - throughput ceiling and flush_interval sweep on real S3

- **taquba:** 0.8.0, post-fix (`f05acc0`, master)
- **Benchmark:** steady_state (`benches/taquba/steady_state.rs`)
- **Host:** m7i.xlarge, 4 vCPU / 16 GiB, us-east-1
- **Store:** real S3, Standard storage class, us-east-1 (same region as host)
- **Common parameters:** payload 64 B, `CLAIM_BATCH=16`; per-run rate / producers / workers / flush as noted

Saturating ceiling probe (`RATE=20000` flat-out, `DURATION_SEC=90`); commits = enqueues + acks, both durable; WAL PUTs = WAL SSTs over the run / 90 s:

| producers | workers | flush | commits/s | WAL PUTs/s | commits/PUT |
|---|---|---|---|---|---|
| 100 | 50 | 1 ms | 2,386 | 45 | 53 |
| 300 | 50 | 1 ms | 3,966 | 92 | 43 |
| 300 | 50 | 100 ms | 2,900 | 35 | 82 |

Sustained 700/s, flush comparison (`DURATION_SEC=300`); e2e_p99 is the mean of per-window p99:

| producers | workers | flush | done/s | e2e_p99 | pending peak |
|---|---|---|---|---|---|
| 50 | 50 | 1 ms | 695 | 928 ms | 260 |
| 100 | 100 | 100 ms | 693 | 1,798 ms | 121 |

(The 1 ms / 700 s row is the prior `8a67119` steady_state entry, repeated for comparison.)

Notes:
- **The single-writer throughput ceiling is PUT/IO-throughput-bound and scales with concurrency, not transaction-count-bound.** Commits/s tracks WAL PUTs/s; raising producers 100 to 300 doubled PUTs/s (45 to 92) and lifted commits/s ~66%, while commits-per-PUT stayed flat (53 to 43), so the gain is more PUTs/s (more IO), not bigger batches. A transaction/CPU-bound ceiling would not have moved with concurrency. The real ceiling is ~2,400-4,000+ commits/s (still climbing at 300 producers), well above the ~1,000/s figure quoted earlier, which was only 700-offered / 50-worker `done/s`.
- **Group commit already amortizes the durable write.** Each WAL PUT carried 43-82 committed operations across these runs, so the per-operation transaction cost is not what bounds throughput; the durable-write IO rate is.
- **`flush_interval` is a latency, throughput, and PUT-cost tradeoff, not a way to reduce spikes.** A larger interval *lowers* throughput (fewer PUTs/s) and, at matched 700/s, roughly doubled e2e_p99 (928 to 1,798 ms) while requiring twice the concurrency to sustain the rate (each durable operation waits up to one flush interval, halving per-actor enqueue/ack throughput; under-provisioned 100 ms runs sustained only ~480/s). It did not reduce the e2e tail. A smaller interval is the better default for latency and throughput; a larger interval reduces only PUT request count and object count. The periodic e2e and backlog spikes are better addressed in the object-store layer (request retry or hedging) or by provisioning throughput above the offered load.
- **The claim-scan prefix-bound fix holds under saturation:** zero windows with claim p99 > 50 ms across every run.

### 2026-06-18 - steady_state 700/s shallow queue after the claim-scan prefix bound on real S3

- **taquba:** 0.8.0, post-fix (`8a67119`)
- **Benchmark:** steady_state (`benches/taquba/steady_state.rs`)
- **Host:** m7i.xlarge, 4 vCPU / 16 GiB, us-east-1
- **Store:** real S3, Standard storage class, us-east-1 (same region as host)
- **Parameters:** `RATE=700 N_PRODUCERS=50 N_WORKERS=50 CLAIM_BATCH=16 FLUSH_INTERVAL_MS=1 DURATION_SEC=300` payload 64 B, `STORE_LATENCY_MS=0`
- **Command:** `cargo bench -p taquba-bencher --features aws --bench steady_state`

| Metric | Value |
|---|---|
| Achieved throughput | ~695/s (sustained) |
| e2e p50 | ~428 ms |
| e2e p99 | ~928 ms |
| pending peak | ~260 |
| claim p99 (mean / peak across windows) | 1.6 ms / 22.3 ms |

Notes: this re-runs the 700/s x 300 s shallow-queue operating point from the `19888b0` steady_state entry below, after the claim-scan prefix-bound fix (`8a67119`). **The claim-latency sawtooth is eliminated:** claim p99 is flat at ~1.6 ms mean / 22.3 ms peak, with zero of the 300 windows above 50 ms, versus the prior run's sawtooth between ~1-5 ms and ~200-470 ms. **This corrects the prior entry's attribution.** That entry attributed the sawtooth to "periodic compaction reclaiming the tombstone band"; claim-path instrumentation showed the actual cause was the cursor-resumed claim scan running with an unbounded end, so the step that detects a drained queue continued past the last live `pending:` key into the remainder of the keyspace, a traversal nearly every claim on a shallow queue incurred. Bounding the scan to the `pending:` prefix removed it. Per the append-only convention the `19888b0` entry is left as recorded; this entry supersedes its claim-latency line only. e2e (floored by S3 PUT latency) and throughput (single-writer ceiling) are unchanged, as the fix touches only the claim path. The periodic e2e and backlog spikes the prior entry described persist (pending peak ~260, e2e p99 ~928 ms), so those are independent of the claim sawtooth rather than a shared compaction cause as previously inferred.

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
