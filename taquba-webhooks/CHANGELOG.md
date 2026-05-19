# Changelog

All notable changes to the `taquba-webhooks` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `Error::is_permanent()` now classifies `Queue(_)` correctly by
  delegating to `taquba::Error::is_permanent`. Previously the `matches!`
  arm enumerated only webhook-specific variants and silently returned
  `false` (transient) for every inner taquba error, meaning permanent
  inner errors like `JobNotFound` or `InvalidState` would have been
  retried.

## [0.1.0] - 2026-05-07

Initial release.
