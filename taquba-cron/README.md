# taquba-cron

[![crates.io](https://img.shields.io/crates/v/taquba-cron.svg)](https://crates.io/crates/taquba-cron)
[![docs.rs](https://img.shields.io/docsrs/taquba-cron)](https://docs.rs/taquba-cron)
[![license](https://img.shields.io/crates/l/taquba-cron.svg)](#license)

POSIX cron-style scheduling on top of the [Taquba](../taquba) durable task queue.

> Part of the [Taquba ecosystem](https://github.com/micllam/taquba); see the
> workspace README for the queue core and the other crates that compose with
> this one.

Register named cron expressions paired with a payload; when each expression's
firing time arrives, the corresponding payload is enqueued onto a Taquba
queue. The scheduler is single-process and event-driven (sleeps until the
next firing rather than polling on a fixed interval).

## Install

```bash
cargo add taquba-cron taquba
cargo add tokio --features full
```

## Quick start

```rust
use std::sync::Arc;
use taquba::{Queue, object_store::memory::InMemory};
use taquba_cron::CronScheduler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = Arc::new(Queue::open(Arc::new(InMemory::new()), "demo").await?);

    let mut scheduler = CronScheduler::new(queue);
    scheduler.schedule("daily-report", "0 9 * * *".parse()?, "reports", b"daily".to_vec())?;
    scheduler.schedule("hourly-sweep", "0 * * * *".parse()?, "sweeps", b"sweep".to_vec())?;

    scheduler.run(std::future::pending::<()>()).await?;
    Ok(())
}
```

`CronScheduler::spawn` runs the scheduler as a Tokio task instead and
returns a `taquba::WorkerHandle` that stops it.

## Per-schedule options

`schedule_with` accepts a `ScheduleOptions` for per-schedule overrides
(HTTP-style headers, priority, max attempts, backfill):

```rust
use std::collections::HashMap;
use taquba_cron::ScheduleOptions;

let opts = ScheduleOptions {
    headers: HashMap::from([("target_url".into(), "https://example.com/hook".into())]),
    priority: Some(taquba::PRIORITY_HIGH),
    max_attempts: Some(10),
    ..Default::default()
};
```

Every enqueued job has the header `cron.fire_ms` (`FIRE_MS_HEADER`), the
firing time as milliseconds since the Unix epoch. The header
`cron.previous_fire_ms` (`PREVIOUS_FIRE_MS_HEADER`) is the occurrence of the
expression before the firing time, in the same form, so the two headers
bound the interval that the job covers. The scheduler computes the previous
occurrence from the expression, whether or not that occurrence was enqueued.
Header names with the `cron.` prefix are reserved, and a schedule with such
a header is rejected.

## Backfill

By default a firing missed while the scheduler is not running is dropped.
A schedule that opts in with `ScheduleOptions::backfill` replays missed
firings instead: the scheduler persists the time of the last enqueued
firing in the queue's KV namespace under `watermark_key(name)`
(`cron/watermark/{name}`), and on start enqueues one job per occurrence
between that watermark and the current time, oldest first, before resuming
live firings. `Backfill::lookback` bounds the replay: occurrences older
than the lookback are skipped.

```rust
use std::time::Duration;
use taquba_cron::{Backfill, BackfillStart, ScheduleOptions};

let opts = ScheduleOptions {
    backfill: Some(Backfill {
        lookback: Duration::from_secs(6 * 60 * 60),
        start: BackfillStart::CurrentTime,
    }),
    ..Default::default()
};
```

The scheduler writes the watermark in the transaction of the enqueue, so the
two commit together. The watermark advances only when a firing is enqueued:
an enqueue error under backfill keeps the schedule at the failed firing, and
the scheduler retries it.

`Backfill::start` determines the start of a schedule without a watermark.
With `BackfillStart::CurrentTime` the schedule starts at the current time and
does not replay a firing. With `BackfillStart::Lookback` the first run
replays the occurrences within the lookback. That start requires a bounded
lookback, and a registration with `Duration::MAX` fails with
`Error::UnboundedStart`.

The watermark records a position in the occurrence sequence and is
independent of the expression. After an expression change, the scheduler
replays the missed occurrences of the new expression after the watermark.
The watermark stays in the KV namespace after its schedule is removed, and
`CronScheduler::clear_watermark` deletes it. Keys with the `cron/` prefix of
the KV namespace are reserved for this crate.

## Changes while the scheduler runs

`CronScheduler::handle` returns a `ScheduleHandle`, which registers and
removes schedules before and during `CronScheduler::run`. The running
scheduler applies a change before its next firing. A schedule registered
through the handle starts at the time the scheduler applies it, or at its
watermark under backfill.

```rust
use std::sync::Arc;
use taquba::{Queue, object_store::memory::InMemory};
use taquba_cron::CronScheduler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = Arc::new(Queue::open(Arc::new(InMemory::new()), "demo").await?);

    let scheduler = CronScheduler::new(queue);
    let handle = scheduler.handle();
    let worker = scheduler.spawn(std::future::pending::<()>());

    let hourly = "0 * * * *".parse()?;
    handle.schedule("hourly-sweep", hourly, "sweeps", b"sweep".to_vec())?;
    assert!(handle.unschedule("hourly-sweep"));

    worker.shutdown().await?;
    Ok(())
}
```

`ScheduleHandle::unschedule` keeps the backfill watermark. A change of an
expression is an `unschedule` and a `schedule` with the same name, and the
schedule resumes at the watermark. After the scheduler stops, a registration
through the handle fails with `Error::Stopped`.

## Cron syntax

Expressions are 5-field POSIX cron, parsed by [`croner`](https://crates.io/crates/croner):

```text
┌───────────── minute       (0-59)
│ ┌─────────── hour         (0-23)
│ │ ┌───────── day of month (1-31)
│ │ │ ┌─────── month        (1-12)
│ │ │ │ ┌───── day of week  (0-6, Sunday = 0)
│ │ │ │ │
* * * * *
```

A step follows a range or `*`, as in `5-59/5 * * * *`, and the form
`5/5 * * * *` is rejected. An expression with a seconds field or a year
field is rejected. An `Expression` is parsed from a string, and the parse
fails with `Error::InvalidExpression`.

All firing times are evaluated in UTC, against the clock the queue was
opened with (`Queue::clock`).

## Guarantees

- **At-most-once enqueue per firing.** Each firing is enqueued via Taquba
  with a deterministic `dedup_key` of `"cron:{name}:{fire_time_ms}"`, so
  retries or duplicate attempts at the same firing instant cannot produce
  more than one job.
- **No backfill by default.** If the scheduler is offline when a firing
  should have happened, the missed firing is dropped, and the next firing is
  the next future occurrence. A schedule with `ScheduleOptions::backfill` set
  replays the missed firings within its lookback exactly once. Only the
  persisted watermark stops a firing from being enqueued twice, because claiming
  a job releases its dedup key.
- **Single-instance schedules.** A given schedule (identified by `name`)
  must be owned by at most one `CronScheduler` at a time.
- **No schedule persistence.** Schedules live only in memory; rebuild
  them in code on startup. The *enqueued jobs* are durable via Taquba, as
  is the backfill watermark.

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or
   <http://www.apache.org/licenses/LICENSE-2.0>)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or
   <http://opensource.org/licenses/MIT>)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
