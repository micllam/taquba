# Changelog

All notable changes to the `taquba-bulk` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.0] - 2026-09-02

### Changed

- **Breaking (source and storage):** the crate re-exports
  `taquba_workflow::bulk` and receives no further development; this is
  its final release. The implementation moved into `taquba-workflow`,
  and `taquba-workflow` 0.11 and `taquba` 0.12 are the dependencies.
  Relative to 0.6:
  - The worker is spawned once by `Bulk::spawn`, which returns a
    `taquba_workflow::RunnerHandle`, and runs the items of every batch,
    so batches run concurrently on one runner. `Batch::run` waits for
    its items and stops waiting when dropped; `Batch::progress` replaces
    `Bulk::progress`; a batch already running in the process is
    rejected.
  - Every run is a batch: `Bulk::batch(id)`, `Bulk::new_batch`, or a
    generated id through `Bulk::run`. Items are identified by key
    (`BulkBuilder::key_fn` accepts any string) and their run ids are the
    SHA-256 digest of `{batch_id}/{key}`, so batches never share run
    state.
  - A run writes the batch's manifest to
    `<memo_prefix>/batches/<batch_id>/manifest` before submitting and
    rejects a different item set or duplicate keys; `Batch::resume`
    drives a batch from its manifest.
  - Each item writes an outcome record to its run memo; a second run of
    the same batch skips the items that succeeded and runs the failed
    ones again.
  - The settlement that commits an item's outcome writes its marker
    under `workflow/bulk/batches/<batch_id>/items/<key>`; `Batch::status`
    reads the manifest and markers into a `BatchStatus`; `Batch::forget`
    removes a batch's state and `BulkBuilder::batch_retention` removes it
    a window after completion.
  - An item runs as one queue job with no terminal notification job, so
    an `OutputSink` receives each item once per run.
  - `OutputRecord::run_id` is `OutputRecord::key` (the JSONL field is
    `key`), `BulkReport::failed_run_ids` is `failed_keys`, `BulkReport`
    gains `batch_id`, and `BulkCtx` gains `batch_id` and `key`.
  - `BulkCtx::memoized` and `memoized_by_content` are replaced by
    `BulkCtx::memo`; the two cost-recording variants take an error type
    convertible from `taquba_workflow::Error`.
  - An item's batch id and key are carried in its run's input, so
    `BulkBuilder::headers` reserves only the `workflow.` prefix.

### Added

- `BulkCtx::effects` and `BulkCtx::kv_get`: staged KV effects applied
  with the item's successful completion, and committed KV reads.
- `BulkBuilder::clock` overrides the queue's clock.

### Removed

- **Breaking (source):** `Error` and `Result`. Operations return
  `taquba_workflow::Error`, which gains `Io`, `Json`, `Deserialization`,
  `DuplicateItemKey`, `BatchMismatch`, `BatchNotFound`, `BatchRunning`,
  `InvalidBatchId` and `FailureThresholdExceeded`.
- **Breaking (source):** `Bulk::run_with_shutdown`. Draining is
  `RunnerHandle::shutdown`.

## [0.6.0] - 2026-08-17

### Changed

- Raised the minimum `taquba-workflow` requirement to 0.10. Each item's
  termination is now delivered to the batch hook as a workflow
  notification job, so a batch performs one additional queue job per
  item and the output row is written only after the item's terminal
  outcome committed.

### Fixed

- A redelivered terminal notification for an item (delivery is
  at-least-once) is now recorded once: the duplicate neither writes a
  second output row nor advances the progress counters, which
  previously could report the batch done while items were still
  running.

## [0.5.0] - 2026-08-12

### Added

- `BulkCtx::lease`: the delivery's `taquba::LeaseHandle`. A
  long-running pipeline calls `ensure_at_least` at progress points (or
  once, with a slow call's timeout, before issuing it) so the item is
  not re-queued while it still runs.

### Changed

- Raised the minimum `taquba` requirement to 0.11 and `taquba-workflow`
  to 0.9.

## [0.4.0] - 2026-08-07

### Changed

- Raised the minimum `taquba` requirement to 0.10 and `taquba-workflow` to 0.8.
- The `object_store` types in the public API are now `object_store` 0.14.

### Removed

- **Breaking:** the `Error::Queue` variant and its
  `From<taquba::Error>` conversion. No code path constructed it: the
  crate performs no direct queue operations, and queue failures inside
  the workflow runtime surface as `Error::Workflow`.

## [0.3.0] - 2026-06-23

### Changed

- Raised the minimum `taquba` requirement to 0.9 and `taquba-workflow` to 0.7.

## [0.2.0] - 2026-06-15

### Changed

- Batch submission runs with bounded concurrency instead of one
  awaited submit at a time. Each submission blocks on a durable
  enqueue commit and concurrent commits share WAL flushes, so serial
  submission capped at one item per flush interval (one item per
  100ms at the SlateDB default). Enqueue order across in-flight
  submissions is not defined; batch items are independent.

### Added

- `BulkCtx::memoized_by_content` and
  `BulkCtx::memoized_by_content_with_cached_cost` for memoized steps
  whose keys should be derived from serialized input content rather
  than caller-supplied strings.
- `BulkCtx::memoized_with_cached_cost` for memoized steps whose cost counters
  should be recorded both on fresh compute and on memo hits.

## [0.1.0] - 2026-05-30

Initial release. Per-batch orchestrator that runs one pipeline over many
inputs in a single process on top of `taquba-workflow`.

### Added

- `Pipeline`: the per-item contract (typed `Input` / `Output`, an `Error`
  that converts into a `StepError`, and an async `run`). Each input item
  becomes one `taquba-workflow` run whose single step invokes `run`; the
  pipeline's own logical steps live inside `run` as `BulkCtx::memoized`
  calls.
- `BulkCtx<T>`: per-item execution context. Carries the typed `input`,
  `run_id`, and submitter `headers`; exposes `memoized` (durable per-step
  result caching so an at-least-once retry replays cached results instead of
  repeating a paid call), `record_cost`, and `cancel_token`.
- `CostReport`: generic named-metric accumulator (token counts, paid-API
  units, compute-seconds, dollars). Interior-mutable while a step runs and
  serializable for the per-item envelope and the batch rollup.
- `Bulk` / `BulkBuilder`: the runner. Submits N runs, drives the worker pool,
  streams output as items complete, and aggregates progress and cost.
  Builder options: `output`, `key_fn`, `headers`, `max_concurrent`,
  `poll_interval`, `queue_name`, `memo_prefix`, `fail_threshold`. `run`
  executes to completion; `run_with_shutdown` drains in-flight items on a
  shutdown signal (e.g. spot preemption).
- `ProgressSnapshot`: point-in-time counts, rate, estimated time remaining,
  and cost rollup, returned by `Bulk::progress`.
- `BulkReport`: final counts, elapsed time, cost rollup, and
  `failed_run_ids` (re-submitting those ids resumes from cached memo state).
- `OutputSink` with `JsonlSink` (one JSON record per line) and `NullSink`
  (discards records, for side-effecting pipelines); `read_jsonl` for
  line-delimited JSON input.
- `Error` / `Result`: crate error type, including
  `Error::FailureThresholdExceeded` when the share of failed items crosses
  the configured threshold.
- Re-exports `StepError` and `StepErrorKind` from `taquba-workflow` for the
  `Pipeline::Error` type.
