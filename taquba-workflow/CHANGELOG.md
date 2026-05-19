# Changelog

All notable changes to the `taquba-workflow` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Breaking:** `WorkflowRuntime::submit` now returns `SubmitOutcome`
  (struct with `run_id: String` and `newly_submitted: bool`) instead of
  `RunHandle`. Idempotent re-submissions of an active `run_id` are now
  surfaced as `Ok(SubmitOutcome { newly_submitted: false, .. })` rather
  than `Err(Error::DuplicateRun(_))`; they are no-ops, not failures.
  Callers that distinguish first-time submits from retries should branch
  on `outcome.newly_submitted`.

### Removed

- **Breaking:** `Error::DuplicateRun` removed. The duplicate-run case is
  no longer modelled as an error; see `SubmitOutcome` above.
- **Breaking:** `RunHandle` removed. It carried `run_id` (now on
  `SubmitOutcome`) and `first_job_id`, which had no consumers in the
  workspace or examples.

## [0.3.0] - 2026-05-15

### Added

- Cross-restart duplicate-submission rejection. `WorkflowRuntime::submit`
  now writes a durable per-run record (key `usr:workflow/runs/{run_id}`,
  carrying `run_id` and `submitted_at_ms`) atomically with the step-0
  enqueue via Taquba's new `Queue::enqueue_with_kv`. A resubmit of the
  same `run_id` after a process restart is rejected with
  `Error::DuplicateRun` even if the registry has been wiped and the
  step's dedup key released. The record is cleaned up via `kv_delete`
  when the run reaches a terminal state.

### Changed

- **Breaking:** now requires `taquba` 0.5 (for the new
  `enqueue_with_kv` / `kv_get` / `kv_delete` methods).
  `taquba-workflow`'s own signatures are unchanged.

## [0.2.0] - 2026-05-13

### Added

- Run cancellation, surfaced as a new `TerminalStatus::Cancelled`
  terminal state (wire form `"cancelled"`). Reachable via
  `WorkflowRuntime::cancel(run_id)` (external) or
  `StepOutcome::Cancel { reason }` (runner-issued). External
  cancellation suppresses any pending transient retry and never
  dead-letters: a pending step's queue job is removed and the hook
  fires from the `cancel` call; a running step's outcome is discarded
  and the hook fires from the worker once the step returns. While
  termination is in flight, `WorkflowRuntime::status` reports a new
  `RunState::Cancelling` overlay. `RunOutcome::error` is `None` for
  external cancellation and `Some(reason)` for runner-issued, so
  consumers can distinguish the two without an extra field.
  In-process only; `cancel` returns `Ok(false)` for runs not owned by
  this runtime instance.
- Cooperative mid-step cancellation via `Step::cancel_token` (a
  `tokio_util::sync::CancellationToken`). Runners that `select!` on it
  short-circuit slow work like LLM calls instead of running to
  completion before the worker terminates the run.
- `WebhookTerminalHook` delivers `Cancelled` outcomes, using the same
  body shape as `Failed` (UTF-8 cancellation reason).

## [0.1.0] - 2026-05-11

Initial release.
