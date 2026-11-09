# taquba-cron

[![crates.io](https://img.shields.io/crates/v/taquba-cron.svg)](https://crates.io/crates/taquba-cron)
[![docs.rs](https://img.shields.io/docsrs/taquba-cron)](https://docs.rs/taquba-cron)
[![license](https://img.shields.io/crates/l/taquba-cron.svg)](#license)

Cron scheduling for the
[`taquba`](https://github.com/micllam/taquba/tree/master/taquba) queue. A
schedule pairs a named cron expression with a payload, and the scheduler
enqueues the payload as a job at each firing. The jobs are stored with the
queue, in S3, GCS, Azure Blob or a local directory.

The scheduler is embedded in the process that owns the queue. It sleeps until
the next firing and does not read from the store between firings. It suits
periodic work such as reports, sweeps and reminders. A job with the headers of
[`taquba-webhooks`](https://github.com/micllam/taquba/tree/master/taquba-webhooks)
becomes a periodic HTTP call.

> Part of the [Taquba ecosystem](https://github.com/micllam/taquba). The
> workspace README states when object storage is the correct choice and how the
> crates compose.

## Quick start

```bash
cargo add taquba-cron taquba
cargo add tokio --features full
```

```rust,no_run
use std::sync::Arc;
use taquba::{Queue, object_store::memory::InMemory};
use taquba_cron::{CronScheduler, Schedule};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = Arc::new(Queue::open(Arc::new(InMemory::new()), "demo").await?);

    let scheduler = CronScheduler::new(queue);
    scheduler.handle().schedule(Schedule::new(
        "daily-report",
        "0 9 * * *".parse()?,
        "reports",
        b"daily".to_vec(),
    ))?;

    scheduler.run(std::future::pending::<()>()).await?;
    Ok(())
}
```

A worker claims the jobs from the `reports` queue as from any other queue. The
[example](https://github.com/micllam/taquba/tree/master/taquba-cron/examples)
runs a scheduler and a worker in one process.

## Firings

Expressions are 5-field POSIX cron without a seconds field, so the finest
schedule fires once a minute. The scheduler evaluates every firing time in UTC
against the clock of the queue, which a test can replace with the `MockClock` of
taquba. Each job has headers with its firing time and the occurrence before it,
which bound the interval that the job covers.

## Missed firings

By default the scheduler drops a firing that it misses while it is not running.
A schedule with backfill replays the missed firings within a lookback, oldest
first. The scheduler records the last enqueued firing of the schedule in the
transaction of the enqueue, so a restart does not enqueue a firing twice.

## Schedules in memory

The scheduler keeps its schedules in memory and does not store them, so an
application must register them again at each start. The application can add,
remove or replace schedules while the scheduler runs, for example after a change
of its configuration. A schedule must belong to a single scheduler, or a firing
can produce a second job.

## Stability

taquba-cron is pre-1.0. A minor release can break the Rust API and the on-disk
format, whereas a patch release preserves both.

<!-- vale off -->

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

<!-- vale on -->
