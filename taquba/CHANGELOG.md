# Changelog

All notable changes to the `taquba` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
- **Breaking:** `keep_done_jobs` and `dead_retention` have moved from
  `OpenOptions` to `QueueConfig`. Migration: set them on
  `OpenOptions::default_queue_config` for an instance-wide default, or
  per queue in `OpenOptions::queue_configs`. The previous defaults
  (`None` for `keep_done_jobs`, `Some(7 days)` for `dead_retention`)
  now live on `QueueConfig::default()` and apply unchanged when
  unspecified.
- `Queue::sweep_done_now(retention)` / `Queue::sweep_dead_now(retention)`
  now apply the argument uniformly to every record, overriding per-queue
  `QueueConfig::keep_done_jobs` / `QueueConfig::dead_retention`. Use
  `Queue::sweep_retention_now()` for a sweep that honours each queue's
  configured window.

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
