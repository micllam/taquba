# Changelog

All notable changes to the `taquba` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- `Queue::claim` commits without awaiting WAL durability. Claims
  serialise per queue through the claim lock, which excluded them from
  WAL group commit: the lock holder awaited its flush before the next
  claim could start, making the flush round trip the queue's claim
  throughput ceiling.
  Losing an unflushed claim in a crash leaves the job pending, so it
  is redelivered immediately on recovery instead of after its lease
  expires; at-least-once delivery is unaffected, and a settled job's
  claim is always durable because later durable commits flush
  preceding WAL entries.
- `Queue::claim` tracks per-queue emptiness and a scan bound in
  process memory. Polling an empty queue answers without a storage
  scan or the claim lock, and the pending tombstone band is never
  re-walked from the front while the process stays up; a full prefix
  scan now happens only on cold start or process restart.
- Queue stats counter merges are excluded from transaction conflict
  detection. The merges are commutative, so concurrent job-state
  transitions on the same queue no longer abort and retry each other
  over the shared stats keys.

### Fixed

- A `pending:` insert landing behind the claim cursor while a claim
  was in flight could have its cursor invalidation overwritten by
  that claim's cursor update, hiding the job from cursor scans until
  the queue next drained. The scan bound now moves back to include
  such inserts, and a claim drops its bound advance when the bound
  moved while it ran.
- A `pending:` key could be hidden from claims indefinitely when its
  enqueue committed while a claim was in flight and the key sorted
  below the keys that claim advanced the scan bound past. Job ids are
  generated before the enqueue transaction commits, so commit order
  can invert key order under concurrent producers. The next claim
  then recorded emptiness at a valid epoch and the queue answered
  `None` while live jobs were pending. Bound advances now clamp to
  the smallest key inserted ahead of the bound since the previous
  advance.

- Duplicate `EnqueueOptions::id_override` values are now rejected
  transactionally with `Error::DuplicateJobId` instead of overwriting
  `jobindex:{id}` and leaving older queue-state records behind.
- `Queue::ack`, `Queue::nack`, `Queue::dead_letter`, and
  `Queue::renew_lease` now check that the expected `claimed:` record
  still exists before settling a job. A worker finishing after its
  lease was reaped now gets `Error::InvalidState` instead of being
  able to ack, retry, dead-letter, renew, or corrupt stats from a
  stale `JobRecord`.
- `Queue::nack` and `Queue::renew_lease` now retry on transaction
  conflict like `Queue::ack` and `Queue::dead_letter` already did.
  A reaper committing the expired-lease delete concurrently with a
  late settlement is now retried (and resolves to `Error::InvalidState`
  on the next attempt) instead of surfacing a raw SlateDB transaction
  error to the caller.
- `Queue::requeue_dead_job` now checks that the dead-letter record
  still exists before reviving it. Requeueing a stale record after
  dead-letter retention swept it now returns `Error::JobNotFound`
  instead of recreating the job and corrupting queue stats.

## [0.7.0] - 2026-05-28

### Added

- `EnqueueOptions::id_override` lets callers supply the job id instead
  of receiving a generated ULID. Useful when the id must be known before
  the enqueue returns. Ids are validated at the API boundary (1-128 bytes
  of `[A-Za-z0-9_-]`) and bad inputs return the new
  `Error::InvalidId { id, reason }` variant. Callers should prefer
  ULID-shaped ids when FIFO-within-priority claim order matters:
  `pending`/`scheduled` keys end with the id, so claim order follows
  id sort.
- `Queue::clock()` accessor returns the `Arc<dyn Clock>` the queue
  was opened with (or the default `SystemClock`). Lets downstream
  crates share the queue's time source for their own timestamp work
  so virtualising time with `MockClock` advances the whole stack
  in lockstep.
- `OpenOptions::flush_interval: Option<Duration>` exposes SlateDB's
  WAL flush interval. `None` keeps SlateDB's own default (100ms).
  Every taquba state transition (`enqueue`, `claim`, `ack`, `nack`,
  `dead_letter`) blocks on `txn.commit()` which waits for the next
  flush tick, so this value is the lower bound on per-operation
  latency.

### Changed

- **Breaking on-disk layout:** the `done:` keyspace is reordered from
  `done:{queue}:{id}` to `done:{completed_at:020}:{queue}:{id}`,
  mirroring the existing time-first layout of `claimed:` and
  `scheduled:`. The retention sweep can now early-exit on the first
  unexpired record instead of walking the full prefix. Public API is
  unchanged; in-flight runs from prior versions must be drained
  before upgrading because the old keys will not be observed by the
  reaper.
- `Queue::claim` (and therefore `claim_next` / `claim_with_wait`)
  serialises same-queue claim attempts through an in-process
  `tokio::sync::Mutex`. Same-queue attempts no longer rely on
  SlateDB's transaction-conflict retry to resolve which worker
  takes the head of `pending:`. The lock is per-queue, so different
  queues' claims still run in parallel. Per-claim wall-clock latency
  under high single-queue concurrency drops from seconds to roughly
  one commit interval. Public API unchanged.
- `Queue::claim` now maintains an in-memory per-queue cursor that
  records the most recently claimed `pending:` key, and starts the
  next claim's scan from immediately after it. This skips the
  tombstone band left by previously claimed (and deleted) `pending:`
  entries that the SlateDB iterator would otherwise walk. The
  cursor is invalidated whenever a `pending:` key is written at or
  before it (nack-requeue, dead-job requeue, reaper-requeue,
  scheduler promotion, and any enqueue at a lower-numbered
  priority); when this happens the next claim falls back to a full
  prefix scan. The cursor is not persisted: on process restart the
  first claim falls back to the prefix scan and re-warms naturally.
  Public API unchanged.
- Bumped minimum `slatedb` version from 0.13 to 0.13.1.

### Fixed

- `enqueue_with`'s non-dedup path (`write_new`) now retries on
  transaction conflict, matching the dedup path (`write_unique`),
  `enqueue_with_kv`, `ack`, `dead_letter`, and every other write path
  in the crate. Previously a conflict during a non-dedup enqueue would
  surface as `Error::Storage` to the caller; under normal contention
  this would have manifested as spurious enqueue failures that a retry
  could resolve.

## [0.6.0] - 2026-05-20

### Added

- `Error::is_permanent()`: classifies each variant as transient or
  permanent so downstream crates can decide whether to retry or
  fast-fail. `Serialization`, `Deserialization`, `JobNotFound`,
  `InvalidState`, and `KvValueTooLarge` are permanent; `Storage(_)` is
  conservatively treated as transient.
- New `Clock` trait + `SystemClock` (default) + `MockClock` types for
  virtualising taquba's time source. Every state-transition timestamp
  (`enqueued_at`, `completed_at`, `failed_at`, `lease_expires_at`) and
  every time-based comparison (retention cutoffs, scheduled-job
  promotion, lease-expiry detection) reads through
  `OpenOptions::clock`. Production callers leave the default; tests can
  pass a `MockClock` and call `MockClock::advance(Duration)` to move
  time deterministically without `std::thread::sleep`.
- Per-queue retention via new `QueueConfig::keep_done_jobs` and
  `QueueConfig::dead_retention` fields. Different queues sharing one
  `Queue` instance can now pick different retention windows (e.g.
  short for ephemeral deliveries, longer for workflow runs).
  `Queue::ack` and the background reaper consult the per-queue value
  via the new `Queue::queue_keep_done_jobs` /
  `Queue::queue_dead_retention` resolvers.

### Changed

- Updated `slatedb` dependency from 0.12 to 0.13. `taquba`'s public API is
  unchanged.
- **Breaking:** `OpenOptions` gained a `clock: Arc<dyn Clock>` field.
  Code using `..OpenOptions::default()` is unaffected; explicit struct
  literals must set it (`clock: Arc::new(SystemClock)` reproduces the
  prior behaviour).
- **Breaking:** `keep_done_jobs` and `dead_retention` have moved from
  `OpenOptions` to `QueueConfig`. Migration: set them on
  `OpenOptions::default_queue_config` for an instance-wide default, or
  per queue in `OpenOptions::queue_configs`. The previous defaults
  (`None` for `keep_done_jobs`, `Some(7 days)` for `dead_retention`)
  now live on `QueueConfig::default()` and apply unchanged when
  unspecified.
### Removed

- **Breaking:** `Queue::sweep_done_now(retention)` and
  `Queue::sweep_dead_now(retention)` removed.

### Fixed

- `Queue::ack` and `Queue::dead_letter` now retry on transaction conflict,
  matching every other write path in the crate. Previously, a conflict
  during ack or dead-letter would surface the error to the caller and
  leave the job in `Claimed` until the lease expired and the reaper
  requeued or dead-lettered it, adding up to
  `lease_duration + reaper_interval` of wall-clock latency to the job's
  terminal state.

## [0.5.0] - 2026-05-15

### Added

- `Queue::enqueue_with_kv(queue, payload, opts, kv_writes)` enqueues a
  job *and* applies a set of writes to a caller-owned KV namespace in
  a single SlateDB transaction. Returns the new
  `EnqueueResult::{New, AlreadyEnqueued}` enum: on a `dedup_key` hit,
  the existing job's id is returned and **no KV writes are applied**.
  Enables downstream crates to keep durable coordination state
  (status markers, dedup records, pointers to externally-stored blobs)
  consistent with the queue across crashes.
- `Queue::kv_get(key)` and `Queue::kv_delete(key)` for reading and
  cleanup of entries in the user KV namespace. There is no standalone
  `kv_put` by design: the namespace mutates only as a side effect of
  queue operations.
- `MAX_KV_VALUE_SIZE` (256 KiB) constant, enforced at the API
  boundary. Values exceeding the cap return the new
  `Error::KvValueTooLarge { size, max }` variant.
- Reserved `usr:` key prefix for the user KV namespace. Caller keys
  are internally scoped under this prefix so they cannot collide
  with Taquba's internal layout (`pending:`, `claimed:`, `dead:`,
  `done:`, `scheduled:`, `jobindex:`, `dedup:`, `stats:`).

## [0.4.0] - 2026-05-14

### Added

- Cooperative cancellation of `Claimed` jobs. `Queue::cancel` now
  handles every lifecycle state and returns a `CancelOutcome` enum
  (`Removed` | `Requested` | `NotFound`). For `Claimed` jobs it
  persists a new `cancel_requested` flag and fires the in-process
  `CancellationToken` exposed on the new `JobRecord::cancel_token`
  field. Workers receive the token on every `claim*` path and can
  `select!` on it to short-circuit. The persisted flag ensures that if
  the lease expires and the reaper requeues the job, the next claim's
  token starts pre-cancelled. `requeue_dead_job` clears the flag:
  reviving a dead job is an operator decision to give it a fresh start.
- `Queue::wait_for_completion(id, timeout) -> WaitOutcome`. Notify-based:
  every terminal transition in the queue (`ack`, `nack`-to-dead,
  `dead_letter`, `cancel`-Removed, reaper dead-letter) fires a shared
  `Notify` that the call listens on, so there is no per-job polling.
  Returns one of:
  - `WaitOutcome::Completed(Some(Box<JobRecord>))` when taquba kept a
    terminal record (`Dead` always; `Done` only when `keep_done_jobs`
    is set).
  - `WaitOutcome::Completed(None)` when the job terminated but no
    record was retained (default `ack`, or a `cancel` of a
    Pending/Scheduled job).
  - `WaitOutcome::TimedOut` if `timeout` elapsed first.
  - `WaitOutcome::NotFound` if the job ID was not present at call
    time. With the default config, `Completed(None)` is ambiguous
    between "success" and "cancelled before claim"; see the
    `WaitOutcome` docs for the full retention matrix.

### Changed

- **Breaking:** `Queue::cancel` now returns `Result<CancelOutcome>`
  instead of `Result<bool>`. Migration: existing call sites that
  matched on `true` for "removed from queue" should match on
  `CancelOutcome::Removed`; sites that distinguish "Claimed (worker
  has it)" from "Pending/Scheduled" should match on
  `CancelOutcome::Requested` vs `Removed`. `CancelOutcome::acted()`
  is a `bool` helper covering the previous "any cancellation happened"
  semantics.
- `JobRecord` gained two fields:
  - `cancel_requested: bool` (persisted; defaults to `false` on
    records written by earlier versions, so the on-disk layout is
    backward-compatible).
  - `cancel_token: Option<CancellationToken>` (skipped from serde;
    populated by `claim*`, `None` everywhere else).

  Code that constructs `JobRecord` directly via a struct literal now
  has to set both fields.

## [0.3.0] - 2026-05-06

### Added

- `PermanentFailure` marker error type. Returning it from `Worker::process`
  routes the job to a new `Queue::dead_letter` exit instead of `nack`,
  skipping retry/backoff for failures known to be permanent (e.g. an HTTP
  410 Gone, a malformed input that won't change). `run_worker` and
  `run_worker_concurrent` downcast the worker error and route accordingly.
- `Queue::dead_letter` for moving a claimed job to the dead-letter set
  unconditionally, without bumping `attempts`.

## [0.2.0] - 2026-05-05

### Added

- `headers: HashMap<String, String>` on `JobRecord` and `EnqueueOptions`
  for application-defined per-job metadata (target URLs, signing key ids,
  schedule names, etc.). Serialized only when non-empty.

### Changed

- `Option` fields on `JobRecord` (`claimed_at`, `lease_expires_at`,
  `run_at`, `last_error`, `dedup_key`, `completed_at`, `failed_at`) skip
  serialization when `None`, reducing on-disk size for typical jobs.
  Backwards-compatible with records written by 0.1.0.

## [0.1.0] - 2026-05-01

Initial release.
