# Changelog

All notable changes to the `taquba-cron` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Opt-in backfill: `ScheduleOptions::backfill` takes a `Backfill` policy
  under which firings missed while the scheduler was not running are
  replayed, one deduplicated job per occurrence, oldest first. The last
  enqueued firing time is persisted in the queue's KV namespace under
  `watermark_key(name)` (`cron/watermark/{name}`), written in the enqueue
  transaction; `Backfill::lookback` bounds the replay and
  `CronScheduler::clear_watermark` removes a watermark. An enqueue error
  under backfill holds the firing for retry instead of dropping it. Keys
  under the `cron/` prefix of the KV namespace are now reserved.

### Changed

- Every enqueued job carries the header `cron.fire_ms` (`FIRE_MS_HEADER`)
  which stores its firing time as milliseconds since the Unix epoch.
  Header names with the `cron.` prefix (`RESERVED_HEADER_PREFIX`) are
  reserved: `schedule_with` rejects a schedule that supplies one with the
  new `Error::ReservedHeader`.
- The scheduler reads the current time from the clock the queue was
  opened with (`Queue::clock`) instead of the system clock, so firings
  and the queue's own timestamps agree under a `MockClock`.

## [0.7.0] - 2026-08-12

### Changed

- Raised the minimum `taquba` requirement to 0.11.

## [0.6.0] - 2026-08-07

### Changed

- Raised the minimum `taquba` requirement to 0.10.

### Removed

- **Breaking:** the `Error::Queue` variant and its
  `From<taquba::Error>` conversion. No code path constructed it:
  enqueue failures inside the scheduler loop are logged and dropped by
  documented policy, so only `InvalidExpression` and `DuplicateName`
  are reachable.
- **Breaking:** `Error::is_permanent`. With `Error::Queue` gone it
  returned `true` for every variant; both remaining variants are
  permanent configuration errors, as the enum documentation now
  states.

## [0.5.0] - 2026-06-23

### Changed

- Raised the minimum `taquba` requirement to 0.9.

## [0.4.0] - 2026-06-15

### Changed

- Raised the minimum `taquba` requirement to 0.8.

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
