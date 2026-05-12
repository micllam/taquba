# Changelog

All notable changes to the `taquba-workflow` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `TerminalStatus::Cancelled` terminal state, wire form `"cancelled"`.
  Like `Failed` from `StepOutcome::Fail`, this is a clean run-level
  outcome: the step is acked and no dead-letter is produced.
- `StepOutcome::Cancel { reason }` for runner-issued cancellation.
- `WorkflowRuntime::cancel(run_id)` for external cancellation of an
  active run. Pending/scheduled step jobs are cancelled in the queue
  and the terminal hook fires from the `cancel` call; running steps
  are allowed to finish, their outcome is discarded, any pending
  transient retry is suppressed, and the hook fires from the worker.
  In-process only; returns `Ok(false)` for unknown runs.
  `RunOutcome::error` is `None` for external cancellation and
  `Some(reason)` for runner-issued `StepOutcome::Cancel`.
- `WebhookTerminalHook` now delivers `Cancelled` outcomes, using the
  same body shape as `Failed` (UTF-8 cancellation reason).

## [0.1.0] - 2026-05-11

Initial release.
