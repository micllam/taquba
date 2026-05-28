# taquba-cron

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
(HTTP-style headers, priority, max attempts):

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

All firing times are evaluated in UTC.

## Guarantees

- **At-most-once enqueue per firing.** Each firing is enqueued via Taquba
  with a deterministic `dedup_key` of `"cron:{name}:{fire_time_ms}"`, so
  retries or duplicate attempts at the same firing instant cannot produce
  more than one job.
- **No backfill.** If the scheduler is offline when a firing should have
  happened, the missed firing is dropped; the next firing is the next
  future occurrence, not a replay of the missed window.
- **Single-instance schedules.** A given schedule (identified by `name`)
  must be owned by at most one `CronScheduler` at a time.
- **No persistence.** Schedules live only in memory; rebuild them in
  code on startup. The *enqueued jobs* are durable via Taquba.

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
