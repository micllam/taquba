# Changelog

All notable changes to the `taquba-cron` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-05-20

### Added

- `Error::is_permanent()`: classifies `InvalidExpression` and
  `DuplicateName` as permanent and delegates the `Queue` variant to
  [`taquba::Error::is_permanent`].

## [0.2.0] - 2026-05-06

### Added

- `ScheduleOptions::priority` and `ScheduleOptions::max_attempts` for
  per-schedule overrides of the queue's defaults. Both are passed through
  to the underlying `EnqueueOptions` when the schedule fires.
- `Error::DuplicateName` returned by `schedule` / `schedule_with` when the
  same `name` is registered twice. Previously, duplicate names would
  silently produce colliding dedup keys and lose firings.

## [0.1.0] - 2026-05-05

Initial release.
