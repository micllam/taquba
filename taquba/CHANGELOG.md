# Changelog

All notable changes to the `taquba` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
