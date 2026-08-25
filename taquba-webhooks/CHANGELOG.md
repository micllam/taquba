# Changelog

All notable changes to the `taquba-webhooks` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Built against the `taquba` option setters: `OpenOptions`,
  `QueueConfig`, `EnqueueOptions` and `SettlementEffects` are
  `#[non_exhaustive]` in `taquba` and are constructed through their
  setters here and in the documentation examples. Callers that build
  these types for a queue shared with this crate migrate the same way.

## [0.7.0] - 2026-08-17

### Added

- `webhook_enqueue_request` builds the `taquba::EnqueueRequest` for a
  delivery without performing the enqueue, so a delivery can be staged
  into a settlement transaction via `taquba::AckEffects::enqueues`.
  `enqueue_webhook` now delegates to it.

## [0.6.0] - 2026-08-12

### Added

- `DEFAULT_TIMEOUT` (30s) and `WebhookWorker::with_default_timeout`: the
  request timeout applied to jobs that declare no `webhook.timeout_ms`
  header.

### Changed

- Raised the minimum `taquba` requirement to 0.11.
- **Breaking (behaviour):** every delivery is bounded. A job without a
  `webhook.timeout_ms` header previously ran with no request timeout;
  it now uses the worker's default. The per-request timeout takes
  precedence over a timeout configured on the `reqwest::Client`.
- The worker extends the job's lease to cover the request timeout
  before sending, so a delivery slower than the queue's lease is not
  re-queued mid-request.

## [0.5.0] - 2026-08-07

### Changed

- Raised the minimum `taquba` requirement to 0.10.

## [0.4.0] - 2026-06-23

### Changed

- Raised the minimum `taquba` requirement to 0.9.

## [0.3.0] - 2026-06-15

### Changed

- Raised the minimum `taquba` requirement to 0.8.

## [0.2.0] - 2026-05-20

### Fixed

- `Error::is_permanent()` now classifies `Queue(_)` correctly by
  delegating to `taquba::Error::is_permanent`. Previously the `matches!`
  arm enumerated only webhook-specific variants and silently returned
  `false` (transient) for every inner taquba error, meaning permanent
  inner errors like `JobNotFound` or `InvalidState` would have been
  retried.

## [0.1.0] - 2026-05-07

Initial release.
