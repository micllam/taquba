# Changelog

All notable changes to the `taquba-workflow` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- The `bulk` module: bulk multi-step processing, moved from the
  `taquba-bulk` crate with its surface unchanged. Relative to
  taquba-bulk 0.6: `BulkCtx::effects` stages application KV effects
  applied with the item's successful completion and `BulkCtx::kv_get`
  reads a committed value; `Bulk::run` and `Bulk::run_with_shutdown`
  stop the worker and wait for it before returning a submission error;
  every run is a batch (`Bulk::batch(id)` or a generated id through
  `Bulk::run`), items are identified by key (`BulkBuilder::key_fn` now
  accepts any string) and their run ids are the SHA-256 digest of
  `{batch_id}/{key}`, so batches never share run state; each item writes
  an outcome record to its run memo, and a second run of the same batch
  skips the items that succeeded and runs the failed ones again;
  `OutputRecord::run_id` is `OutputRecord::key` (the JSONL field is
  `key`), `BulkReport::failed_run_ids` is `failed_keys`, `BulkReport`
  gains `batch_id`, `BulkCtx` gains `batch_id` and `key`, and
  `Error::InvalidBatchId` and `Error::ReservedHeader` are new.
  `serde_json` is a dependency.
- The `jobs` module: typed single-function jobs, moved from the
  `taquba-jobs` crate and re-founded on the runtime. A job is one run
  with a single step routed by the `jobs.type` header to the
  handler registered on `JobRunnerBuilder::register`; its outcome
  record is stored in the run's memo under the runner's memo prefix
  (default `"{queue_name}-memo"`), removed by the runtime's memo
  retention through `JobRunnerBuilder::retention`; job ids are run
  ids, a ULID or the SHA-256 digest of the idempotency key.
  `JobContext` exposes `memo`, `effects` and `kv_get` beside state,
  lease, cancellation token and `submit`. Relative to `taquba-jobs`
  0.7: registration moved to the builder, `result_prefix` is
  `memo_prefix`, `result_retention` is `retention`,
  `JobHandle::status` returns the in-process `RunStatus`,
  `JobContext::job_id` is `JobContext::id`, `JobContext::queue` is
  gone, and `Error::Store` is replaced by `Error::Workflow`.
- `RunSpec::run_at`: the earliest time the first step may run. The
  step-0 job waits in the queue's scheduled state until the queue's
  clock passes it.
- `SubmitOutcome::job_id`: the id of the queue job for the run's first
  step, `Some` for a new submission and for a duplicate of a run the
  runtime tracks in process, `None` for a duplicate known only from
  the durable run record.
- **Breaking (source):** `Step::max_attempts`, the attempt limit of the
  step. A `Step` built by struct literal in tests must set it.
- `Memo::content_key`, the key derivation used by `Memo::content_get`
  and `Memo::content_put`: `content:` followed by the hex SHA-256
  digest of the input encoded as MessagePack with named fields.
- `Memo::memoized` and `Memo::memoized_by_content`: return the value
  stored under a key, or run a computation, store its value under that
  key and return it. Values are encoded as MessagePack with named
  fields; an entry that fails to decode is treated as absent and
  overwritten by the recomputed value, and an error from the
  computation stores nothing.
- `Step` gains the `kv` field, a `KvReadHandle` reading committed
  values from Taquba's caller KV namespace during a step; `get` is the
  only operation. The read answers from committed state, so effects
  staged by the running step become readable once their settlement
  commits, and it is not transactional with that settlement.
  `KvReadHandle::detached` builds a handle for constructing a `Step` in
  tests, whose `get` returns `Ok(None)` for every key.
- `Step` gains the `run_memo` field, a run-scoped `Memo` shared by
  every step of the run, built by the new `MemoStore::new_run_memo`.
  Entries live under a `run` segment beside the numeric step segments
  (`<prefix>/memos/<run_id>/run/`), so `clear_memos_for_run` and the
  retention sweep remove them with the run's per-step entries.
- `MAX_RUN_ID_LEN` (128) and `Error::InvalidRunId`: a caller-supplied
  `RunSpec::run_id` must be 1 to 128 bytes of `[A-Za-z0-9_-]`, the same
  rule Taquba applies to a caller-supplied job id. The run id becomes an
  object-store path segment under the memo prefix and a key segment in
  the queue's key-value namespace, and an empty one resolved to the memo
  prefix itself, so the retention sweep removed every run's memo and
  step-output entries once that run's marker expired. Generated run ids
  are unaffected.

### Fixed

- An external cancellation is no longer lost when the run's in-process
  registry entry is rebuilt, which happens on every restart. The
  runtime read cancellation from its own registry entry and created its
  own token for `Step::cancel_token`, so a run cancelled before a
  restart resumed and completed normally, discarding the request the
  queue still held on the job record. The runtime now reads the
  claim's cancellation token, which `Queue::cancel` fires and a
  re-claim re-fires from the job's persisted `cancel_requested`, and
  the runner receives a child of that token so that a runner
  cancelling its own token is not treated as an external cancellation.

### Changed

- `Memo::step_number` returns `Option<u32>`: `Some` for a per-step
  memo, `None` for a run-scoped one.
- Built against the `taquba` option setters: `OpenOptions`,
  `QueueConfig`, `EnqueueOptions` and `SettlementEffects` are
  `#[non_exhaustive]` in `taquba` and are constructed through their
  setters here and in the documentation examples. Callers that build
  these types for a queue shared with this crate migrate the same way.
- Run termination is atomic. Every path that settles a run now commits
  the terminal marker, the durable run record's delete and the terminal
  notification's enqueue in the same transaction as the settlement,
  through the `SettlementEffects` that `taquba` applies with a
  dead-letter, an exhausted nack or a `cancel` that removes a pending
  job. Previously the three arms that do not acknowledge (a permanent
  step error, a transient error on the final attempt and a `cancel`
  that removed a pending step) applied them best-effort after the
  transition committed, so a crash in that window could lose a
  notification or leave a run record behind. Exactly-once enqueue of
  the notification now holds on every worker and cancel path; the
  reaper and open-time crash recovery settle inside the queue without a
  worker and still apply no effects.
- Terminal markers move from the object store to the queue's key-value
  namespace, under the reserved `workflow/terminals/` prefix, keyed by
  the terminating timestamp ahead of the run id. The retention sweep
  reads them with `Queue::kv_scan` and its expired set is the start of
  the range; a marker under the prefix that does not parse, or whose
  run id is not a valid run id, is deleted without clearing any
  entries. With retention enabled this removes an object-store round
  trip from every terminating settlement, and it makes markers readable
  through `QueueReader::kv_scan`, which requires no access to the memo
  store that previously held them. Markers written by 0.10 and earlier
  remain in the object store under `<memo prefix>/terminals/` and are
  not read: after upgrading, delete that prefix manually, and clear the
  memo and step-output entries of any run that terminated before the
  upgrade, since no marker remains to select them for sweeping.
- `MemoStore::clear_memos_for_run` fails with `Error::InvalidRunId` for
  an invalid run id. An empty run id resolved to the memo prefix itself
  and cleared every run's entries.

### Removed

- `MemoStore::write_terminal_marker`, `MemoStore::list_terminal_markers`,
  `MemoStore::list_expired_terminal_markers`,
  `MemoStore::delete_terminal_marker` and the `TerminalMarker` type.
  Terminal markers are no longer object-store entries. Custom retention
  policies read them with `Queue::kv_scan` over `workflow/terminals/`
  and still clear entries with `MemoStore::clear_memos_for_run`.

## [0.10.0] - 2026-08-17

### Added

- Application KV effects. `RunSpec` gains the `kv_writes` field: writes
  applied to the caller KV namespace in the same transaction as the
  step-0 enqueue, dropped on a duplicate submission. `Step` gains the
  `effects` field, an `EffectsHandle` that stages caller KV writes and
  deletes during a step; everything staged is applied in the settlement
  transaction that commits the outcome the runner returned (`Continue`,
  `Succeed`, `Fail` or `Cancel`). No effects are applied on `StepError`
  paths, and an external `WorkflowRuntime::cancel` that overrides the
  runner's outcome discards the staged effects. Keys must not start
  with the reserved `workflow/` prefix (the new `RESERVED_KV_PREFIX`
  constant); staging validates at the call site. New error variants
  `ReservedKvKey`, `ConflictingKvEffect` and `EffectsSealed`.

- Terminal notifications as queue jobs. The settlement that commits a
  run's terminal outcome atomically enqueues a notification job (new
  reserved header `workflow.terminal`, dedup key `run:{run_id}:terminal`,
  priority and `max_attempts` inherited from the terminal step) whose
  payload is the committed outcome, and the `TerminalHook` runs as that
  job's worker. The hook observes only outcomes that committed, delivery
  is at-least-once and a failing hook retries per the queue's backoff or
  dead-letters on a permanent error. The new `TerminalEffects` handle
  stages KV writes, deletes and follow-up enqueues that commit with the
  notification's acknowledgement, and the new defaulted
  `TerminalHook::observes` skips the notification per outcome.

### Changed

- Raised the minimum `taquba-webhooks` requirement to 0.7 (the
  `webhooks` feature).
- **Breaking:** `TerminalHook::on_termination` takes a `TerminalEffects`
  parameter and returns `Result<(), StepError>`; every implementation
  updates its signature. `NoopTerminalHook` now observes nothing, so
  runs terminate without enqueueing a notification.
- **Breaking:** `WebhookTerminalHook::new` no longer takes a `Queue`;
  the hook stages its delivery enqueue as a notification effect, so the
  webhook job is created exactly once with the acknowledgement, and
  runs without a callback header enqueue no notification.
- `WorkflowRuntime::cancel` of a pending step enqueues the notification
  job and returns without waiting for the hook to run. Runs terminated
  without an acknowledging settlement (pending-step cancel, a step that
  dead-letters) enqueue the notification best-effort after the terminal
  transition commits.
- **Breaking:** `Step` gains the public field `effects`. Code
  constructing a `Step` in tests adds
  `effects: taquba_workflow::EffectsHandle::detached()`. `RunSpec`
  gains the public field `kv_writes`; constructions using
  `..Default::default()` are unaffected.
- The stored step-output replay record now includes the effects staged
  during the recorded delivery, so a replayed outcome applies them.
  Records written by earlier versions deserialize with no effects.

## [0.9.0] - 2026-08-12

### Changed

- Raised the minimum `taquba` requirement to 0.11.
- **Breaking:** `Step` gains the public field `lease`, the delivery's
  `taquba::LeaseHandle`. A long-running runner calls `ensure_at_least`
  at progress points (or once, with a slow call's timeout, before
  issuing it) so the step is not re-queued while it still runs. Code
  constructing a `Step` in tests adds
  `lease: taquba::LeaseHandle::detached()`.

## [0.8.0] - 2026-08-07

### Added

- Durable signals: `Trigger::OnSignal { correlation_key, timeout }` (built
  by `StepOutcome::continue_on_signal`) defers the next step until
  `WorkflowRuntime::signal` delivers a payload for the correlation key, or
  until the timeout elapses. The next step reads the new `Step::signal`
  field: `Some(payload)` when a signal arrived, `None` on timeout. A signal
  with no registered waiter is buffered durably and consumed by the next
  waiter for its key; `WorkflowRuntime::clear_signal` discards a buffered
  signal. One buffered signal is held per key (a later signal replaces an
  unconsumed one) and one waiter is allowed per key (a second registration
  fails its run). New reserved headers `workflow.signal_wait` and
  `workflow.signal_delivered`; new `SignalOutcome` enum.

### Changed

- Raised the minimum `taquba` requirement to 0.10.
- **Breaking:** `StepOutcome::Continue` and `StepOutcome::ContinueAfter` are
  unified into `StepOutcome::Continue { payload, when }`, where `when` is the
  new `Trigger` enum (`Trigger::Immediate` or `Trigger::After(delay)`). The
  constructors `StepOutcome::continue_now(payload)` and
  `StepOutcome::continue_after(payload, delay)` build the two forms. This
  prepares for additional trigger kinds without further variant growth. The
  stored step-output replay record format changes accordingly.
- The `object_store` types in the public API are now `object_store` 0.14.
- A failure to serialize the durable run record during submit now surfaces
  as `Error::Serialization` instead of `Error::Queue`. Both classify as
  permanent; only the variant changed.

### Fixed

- Step enqueues take their job id from `Queue::next_job_id` rather than
  generating one independently, so steps enqueued in sequence inside one
  millisecond are claimed in that order instead of arbitrarily. Steps
  whose enqueues overlap are ordered by when each took its id, as for a
  direct enqueue. The id is still taken before the request is built and
  passed as `id_override`.

## [0.7.0] - 2026-06-23

### Changed

- Raised the minimum `taquba` requirement to 0.9.

## [0.6.0] - 2026-06-15

### Changed

- Terminal marker filenames lead with an inverted timestamp, and the
  memo-retention sweep lists only expired markers (via the
  object-store `list_with_offset` contract) instead of every retained
  marker on every tick, so a sweep's listing cost is proportional to
  the expired set. `MemoStore::list_expired_terminal_markers` is the
  new sweeper building block; `list_terminal_markers` remains for
  inspection. Markers written by earlier versions are not recognised
  by the sweeper: when upgrading a store that ran with
  `memo_retention` enabled, clear the `<memo_prefix>/terminals/`
  prefix out-of-band.
- Step transitions settle atomically. The next step's enqueue (for
  `Continue` / `ContinueAfter`) and the terminal run-record delete
  (for terminal outcomes) now join the current step's acknowledgement
  transaction via Taquba's `ack_with`, halving the durable commits
  per transition and removing the crash window between enqueuing the
  next step and acking the current one: a step's successor exists if
  and only if the step's settlement committed. The terminal hook now
  fires before the settlement commits rather than after the run-record
  delete; hooks remain at-least-once as before.

### Fixed

- `WorkflowRuntime::submit` no longer serialises every submission on a
  process-wide lock held across queue I/O. The duplicate-check lock is
  now per run id, so concurrent submissions of distinct runs proceed in
  parallel and share WAL group commits. Previously a batch of
  submissions completed at one run per flush interval regardless of
  submission concurrency (about ten runs per second at SlateDB's
  default 100 ms flush); same-run-id submissions keep their existing
  duplicate and input-mismatch semantics.

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
