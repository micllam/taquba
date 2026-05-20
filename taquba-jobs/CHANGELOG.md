# Changelog

All notable changes to the `taquba-jobs` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-05-20

### Added

- `Error::is_permanent()`: classifies builder-misconfiguration variants
  (`MissingQueue`, `MissingObjectStore`), `Encode`/`Decode`,
  `JobNotFound`, and `ReservedHeader` as permanent; `Store(_)` as
  transient. `Queue(_)` delegates to [`taquba::Error::is_permanent`].
- `Error::InputMismatch(String)`: returned by `submit` when a job's
  `idempotency_key` matches a previous submission but the payload
  differs. Classified as permanent.
- Cross-restart input verification on `submit`: when a job has an
  `idempotency_key`, the SHA-256 of the serialized payload is persisted
  in the user KV namespace (`jobs/dedup/{key}`) atomically with the
  enqueue via [`taquba::Queue::enqueue_with_kv`]. A later submission
  with the same key but a different payload returns
  `Error::InputMismatch` instead of silently dedup-hitting to a job
  whose payload is something else. Submissions without an
  `idempotency_key` continue to take the plain `enqueue_with` path.
- `JobHandle::newly_submitted()`: returns `true` when the submission
  that produced the handle freshly enqueued the job, `false` when it
  dedup-hit a pending submission with a matching payload. Always
  `true` for submissions without an `idempotency_key`. Useful for
  logging or metrics that distinguish new-vs-attached submissions.

## [0.1.0] - 2026-05-15

Initial release. Durable, typed async function execution on top of `taquba`.

### Added

- `Job` trait for declaring a typed background unit of work: stable `NAME`
  tag, typed `Output` and `Error`, an `async fn run` body, plus hooks for
  idempotency (`idempotency_key`), retry budget (`max_attempts`), and
  transient-vs-permanent classification (`classify`).
- `JobRunner` + `JobRunnerBuilder` for registering job types, submitting
  jobs, and spawning a concurrent worker that routes by type tag. Builder
  surface: `queue`, `object_store`, `queue_name`, `result_prefix`, `state`
  (type-keyed application state), `max_concurrent_jobs`, `poll_interval`.
- `JobHandle` returned by `JobRunner::submit`, implementing `IntoFuture` so
  `handle.await` yields the typed result directly. Also exposes
  `join`/`join_timeout`/`fetch_result`/`status`. Result blobs survive process
  restarts and are read back from object storage on demand.
- `JobContext` handed to each `Job::run` call: typed application state
  (`state`/`try_state`), the job's `id` and `attempt`, a cooperative
  `cancel_token`, the underlying `queue` handle, and `submit` for fan-out /
  chaining follow-up jobs from inside a handler.
- `SubmitOptions` for per-submission overrides: `max_attempts`, `priority`,
  `run_at` (scheduled execution), and arbitrary `headers`. The reserved
  routing-header key is rejected with `Error::ReservedHeader` instead of
  being silently overwritten.
- `payload_idempotency_key` helper: opt-in hash-based dedup that hashes the
  job payload with SHA-256.
- Durable, persisted terminal outcomes (success or failure) written under a
  caller-controlled prefix in any `object_store::ObjectStore` backend.
- `RunnerHandle` for graceful shutdown of the spawned worker
  (`shutdown` / `wait`).
- `taquba` re-export so downstream code can name `taquba::Queue` without a
  separate dependency.

### Known limitations

- Result blobs accumulate indefinitely: there is no automatic retention
  sweep, and `JobRunnerBuilder` has no `result_retention` option. Plan your
  own object-store lifecycle policy on the result prefix; a built-in
  retention setting is planned for a later release.
- A retried attempt overwrites any prior result blob for the same job ID.
  Handlers that aren't perfectly idempotent can have an earlier
  "successful" outcome replaced; the trait's at-least-once contract is the
  intended surface for managing that.
