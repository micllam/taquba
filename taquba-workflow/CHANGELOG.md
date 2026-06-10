# Changelog

All notable changes to the `taquba-workflow` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `WorkflowRuntimeBuilder::step_output_replay`: opt-in
  content-addressed replay of runner-returned step outcomes, keyed by
  `(run_id, step_number, SHA-256(step payload))`. When enabled, the
  runtime persists every `StepOutcome` the runner returns (including
  `Fail` and `Cancel`) before applying it; if the same step is delivered
  again after a crash before ack, the stored outcome is replayed without
  invoking the runner again. Step errors are not recorded, so retries
  still invoke the runner. A replayed `ContinueAfter` reduces its delay
  by the time already elapsed since the outcome was stored, preserving
  the original schedule.
- `Memo::content_get` and `Memo::content_put` derive per-step memo keys
  from a MessagePack serialization of caller-supplied input hashed with
  SHA-256.

## [0.5.0] - 2026-05-28

### Added

- `Memo`: per-step durable key-value store for memoizing within-step
  side effects, backed by object storage. Bound to a specific
  `(run_id, step_number)`; `get(key)` / `put(key, value)` take only
  the user key. Strictly per-step; the durable channel between steps
  is `StepOutcome::Continue`'s payload, not memo.
- `MemoStore`: the backing store `Memo` views are derived from
  (`Arc<dyn ObjectStore>` + path prefix). Used internally by the
  runtime builder; users construct one directly mainly in tests.
- `Step::memo`: every step receives a `Memo` scoped to its own
  `(run_id, step_number)`. Runners use it to cache results of
  expensive within-step side effects (LLM calls, paid APIs) so
  at-least-once retries don't re-pay for work the prior attempt
  already did.
- `WorkflowRuntimeBuilder::memo_prefix`: configures the object-store
  prefix `Step::memo` entries live under. Defaults to `"workflow-memo"`;
  set a distinct prefix when multiple runtimes share one store.
- `Error::Store(taquba::object_store::Error)`: surfaced from memo
  read/write failures. Classified as transient by `is_permanent`.
- `WorkflowRuntimeBuilder::memo_retention(Duration)`: opts the runtime
  into writing a terminal marker via `MemoStore::write_terminal_marker`
  on every terminal state (Succeeded, Failed, Cancelled). Markers
  outlive the run record and provide the input a memo-retention sweep
  consumes to decide when a run's memo entries are eligible for
  deletion. Without this setter no marker is written and memo entries
  are retained indefinitely (appropriate for short-lived runs or
  external cleanup).
- Memo-retention sweeper: when `memo_retention` is set,
  `WorkflowRuntime::run` spawns a background task that periodically
  scans terminal markers and, for each marker older than the
  configured window, deletes the run's memo entries and then the
  marker itself. The first sweep fires on startup so a fresh process
  catches markers left behind by an earlier one. The sweeper shuts
  down with the caller-supplied shutdown future.
- `WorkflowRuntime` now reads every timestamp it writes
  (`DurableRunRecord::submitted_at_ms`, the `ContinueAfter` `run_at`,
  and the terminal-marker timestamp) through a `taquba::Clock`. By
  default the runtime shares the clock its `Queue` was opened with
  (via `Queue::clock`), so passing a `MockClock` to `OpenOptions`
  virtualises time for the queue and the workflow runtime together.
- `WorkflowRuntimeBuilder::clock(Arc<dyn Clock>)` overrides the
  defaulted-from-queue clock when a test or specialised setup needs a
  separate time source.

### Changed

- **Breaking:** `WorkflowRuntime::builder` now takes an additional
  required `object_store: Arc<dyn ObjectStore>` argument between the
  queue and the runner. The store backs `Step::memo` and need not be
  the same store the queue was opened with, though sharing one (just
  cloning the `Arc`) is the common case. Existing call sites must add
  the store argument:

  ```rust,ignore
  // Before:
  let runtime = WorkflowRuntime::builder(queue, runner, hook).build();
  // After:
  let runtime = WorkflowRuntime::builder(queue, store, runner, hook).build();
  ```

## [0.4.0] - 2026-05-20

### Added

- `Error::is_permanent()` is now public (previously `pub(crate)`) and
  classifies every variant. `Queue(_)` delegates to
  `taquba::Error::is_permanent` so classification stays consistent
  across crates that wrap the underlying taquba error.
- `Error::InputMismatch(run_id)`: returned when a re-submission of an
  active `run_id` carries `spec.input` bytes that differ from the
  original submission's. Classified `is_permanent() == true`.
  `WorkflowRuntime::submit` is now idempotent on `(run_id, spec.input)`
  rather than `run_id` alone; reusing a `run_id` with new content is
  surfaced as a programmer error instead of silently no-op-ing.

### Changed

- **Breaking on-disk layout:** the durable per-run record
  (`usr:workflow/runs/{run_id}`) now carries a `SHA-256` of the original
  `spec.input` to power the `InputMismatch` check. In-flight runs from
  prior versions must be drained before upgrading; records written by
  older versions will fail to deserialize.

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
