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
    scheduler.schedule("daily-report", "0 9 * * *", "reports", b"daily".to_vec())?;
    scheduler.schedule("hourly-sweep", "0 * * * *", "sweeps",  b"sweep".to_vec())?;

    scheduler.run(std::future::pending::<()>()).await?;
    Ok(())
}
```

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

Every enqueued job carries the header `cron.fire_ms` (`FIRE_MS_HEADER`),
which stores the firing time as milliseconds since the Unix epoch, so a
worker can identify the window a job covers. Header names with the `cron.`
prefix are reserved; a schedule that supplies one is rejected.

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
use taquba_cron::{Backfill, ScheduleOptions};

let opts = ScheduleOptions {
    backfill: Some(Backfill {
        lookback: Duration::from_secs(6 * 60 * 60),
    }),
    ..Default::default()
};
```

The watermark is written in the same transaction as the enqueue, so a
crash between the two cannot occur, and it advances only when a firing is
enqueued: an enqueue error under backfill holds the schedule at the failed
firing and retries it. A schedule without a watermark (the first run after
opting in) starts at the current time and replays nothing. The watermark
records a position in the occurrence sequence rather than the schedule itself;
after an expression change the missed occurrences of the new expression
since the watermark are replayed. The watermark of a schedule that is no
longer registered is left in place; remove it with
`CronScheduler::clear_watermark`. Keys under the `cron/` prefix of the KV
namespace are reserved for this crate.

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

All firing times are evaluated in UTC, against the clock the queue was
opened with (`Queue::clock`).

## Guarantees

- **At-most-once enqueue per firing.** Each firing is enqueued via Taquba
  with a deterministic `dedup_key` of `"cron:{name}:{fire_time_ms}"`, so
  retries or duplicate attempts at the same firing instant cannot produce
  more than one job.
- **No backfill by default.** If the scheduler is offline when a firing
  should have happened, the missed firing is dropped; the next firing is
  the next future occurrence rather than a replay of the missed ones. A
  schedule with `ScheduleOptions::backfill` set replays the missed firings
  within its lookback exactly once, on the strength of the persisted
  watermark rather than the dedup key, which is released when the job
  completes.
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
