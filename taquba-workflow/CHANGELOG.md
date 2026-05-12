# Changelog

All notable changes to the `taquba-workflow` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
