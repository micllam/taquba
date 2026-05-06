# Changelog

All notable changes to the `taquba` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
