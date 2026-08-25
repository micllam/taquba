//! POSIX cron-style scheduling on a [Taquba] queue.
//!
//! Register named cron expressions paired with a payload; when each
//! expression's firing time arrives, the corresponding payload is enqueued
//! onto a Taquba queue. The scheduler is single-process and event-driven
//! (sleeps until the next firing rather than polling on a fixed interval).
//!
//! # Quick start
//!
//! ```no_run
//! use std::sync::Arc;
//! use taquba::{Queue, object_store::memory::InMemory};
//! use taquba_cron::CronScheduler;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let queue = Arc::new(Queue::open(Arc::new(InMemory::new()), "demo").await?);
//!
//! let mut scheduler = CronScheduler::new(queue);
//! scheduler.schedule("daily-report", "0 9 * * *", "reports", b"daily".to_vec())?;
//!
//! scheduler.run(std::future::pending::<()>()).await?;
//! # Ok(()) }
//! ```
//!
//! # Per-schedule options
//!
//! [`CronScheduler::schedule_with`] accepts a [`ScheduleOptions`] for
//! per-schedule overrides (HTTP-style headers, priority, max attempts,
//! backfill):
//!
//! ```
//! use std::collections::HashMap;
//! use taquba_cron::ScheduleOptions;
//!
//! let opts = ScheduleOptions {
//!     headers: HashMap::from([("target_url".into(), "https://example.com/hook".into())]),
//!     priority: Some(taquba::PRIORITY_HIGH),
//!     max_attempts: Some(10),
//!     ..Default::default()
//! };
//! ```
//!
//! Every enqueued job carries the header [`FIRE_MS_HEADER`] (`cron.fire_ms`),
//! which stores the firing time as milliseconds since the Unix epoch, so a
//! worker can identify the window a job covers. Header names with the
//! `cron.` prefix are reserved; a schedule that supplies one is rejected.
//!
//! # Backfill
//!
//! By default a firing missed while the scheduler is not running is
//! dropped. A schedule that opts in with [`ScheduleOptions::backfill`]
//! replays missed firings instead: the scheduler persists the time of the
//! last enqueued firing in the queue's KV namespace under
//! [`watermark_key`], and on start enqueues one job per occurrence between
//! that watermark and the current time, oldest first, before resuming
//! live firings. [`Backfill::lookback`] bounds the replay: occurrences
//! older than the lookback are skipped.
//!
//! ```
//! use std::time::Duration;
//! use taquba_cron::{Backfill, ScheduleOptions};
//!
//! let opts = ScheduleOptions {
//!     backfill: Some(Backfill {
//!         lookback: Duration::from_secs(6 * 60 * 60),
//!     }),
//!     ..Default::default()
//! };
//! ```
//!
//! The watermark is written in the same transaction as the enqueue, so a
//! crash between the two cannot occur, and it advances only when a firing
//! is enqueued: an enqueue error under backfill holds the schedule at the
//! failed firing and retries it. A schedule without a watermark (the first
//! run after opting in) starts at the current time and replays nothing.
//! The watermark records a position in the occurrence sequence rather than
//! the schedule itself; after an expression change the missed occurrences
//! of the new expression since the watermark are replayed. The watermark
//! of a schedule that is no longer registered is left in place; remove it
//! with [`CronScheduler::clear_watermark`]. Keys under the `cron/` prefix
//! of the KV namespace are reserved for this crate.
//!
//! # Cron syntax
//!
//! Expressions are 5-field POSIX cron, parsed by [`croner`]:
//!
//! ```text
//! ┌───────────── minute       (0-59)
//! │ ┌─────────── hour         (0-23)
//! │ │ ┌───────── day of month (1-31)
//! │ │ │ ┌─────── month        (1-12)
//! │ │ │ │ ┌───── day of week  (0-6, Sunday = 0)
//! │ │ │ │ │
//! * * * * *
//! ```
//!
//! All firing times are evaluated in UTC, against the clock the queue was
//! opened with ([`taquba::Queue::clock`]).
//!
//! # Guarantees
//!
//! - **At-most-once enqueue per firing.** Each firing is enqueued via Taquba
//!   with a deterministic [`taquba::EnqueueOptions::dedup_key`] of
//!   `"cron:{name}:{fire_time_ms}"`, so retries or duplicate attempts at
//!   the same firing instant cannot produce more than one job.
//! - **No backfill by default.** If the scheduler is offline when a firing
//!   should have happened, the missed firing is dropped; the next firing is
//!   the next *future* occurrence rather than a replay of the missed ones. A
//!   schedule with [`ScheduleOptions::backfill`] set replays the missed
//!   firings within its lookback exactly once, on the strength of the
//!   persisted watermark rather than the dedup key, which is released when
//!   the job completes.
//! - **Single-instance schedules.** A given schedule (identified by `name`)
//!   must be owned by at most one [`CronScheduler`] at a time.
//! - **No schedule persistence.** Schedules live only in memory; rebuild
//!   them in code on startup. The *enqueued jobs* are durable via Taquba,
//!   as is the backfill watermark.
//!
//! [Taquba]: https://docs.rs/taquba

#![warn(missing_docs)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use croner::Cron;
use taquba::{EnqueueOptions, EnqueueResult, Queue};
use tokio::time::sleep;
use tracing::{debug, error, warn};

/// Header attached to every enqueued job, storing the firing time as
/// milliseconds since the Unix epoch in decimal.
pub const FIRE_MS_HEADER: &str = "cron.fire_ms";

/// Prefix of the header names reserved for this crate. A schedule whose
/// [`ScheduleOptions::headers`] contains a name with this prefix is
/// rejected with [`Error::ReservedHeader`].
pub const RESERVED_HEADER_PREFIX: &str = "cron.";

/// Prefix of every watermark key in the queue's KV namespace.
const WATERMARK_PREFIX: &str = "cron/watermark/";

/// Delay before a schedule under backfill retries a failed enqueue.
const ENQUEUE_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Key of a schedule's backfill watermark in the queue's KV namespace:
/// `cron/watermark/{name}`. The value is the last enqueued firing time as
/// milliseconds since the Unix epoch in decimal.
pub fn watermark_key(name: &str) -> Vec<u8> {
    format!("{WATERMARK_PREFIX}{name}").into_bytes()
}

fn parse_watermark(value: &[u8]) -> Option<DateTime<Utc>> {
    std::str::from_utf8(value)
        .ok()?
        .parse::<i64>()
        .ok()
        .and_then(DateTime::from_timestamp_millis)
}

/// The earliest instant a backfill replays, or `None` when `lookback` is
/// too large to bound the replay.
fn lookback_floor(now: DateTime<Utc>, lookback: Duration) -> Option<DateTime<Utc>> {
    chrono::Duration::from_std(lookback)
        .ok()
        .and_then(|d| now.checked_sub_signed(d))
}

/// Errors returned by [`CronScheduler`].
///
/// Every variant is a permanent configuration error: retrying an
/// identical call cannot succeed.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The cron expression failed to parse.
    #[error("invalid cron expression `{expression}`: {message}")]
    InvalidExpression {
        /// The raw expression that failed.
        expression: String,
        /// Parser-supplied diagnostic message.
        message: String,
    },
    /// A schedule with this name is already registered.
    #[error("schedule `{0}` already exists")]
    DuplicateName(String),
    /// A schedule header uses the reserved [`RESERVED_HEADER_PREFIX`].
    #[error("schedule header `{0}` uses the reserved `cron.` prefix")]
    ReservedHeader(String),
}

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Replay policy for firings missed while the scheduler was not running.
/// See the crate documentation, section "Backfill".
#[derive(Debug, Clone)]
pub struct Backfill {
    /// Occurrences at or before this far before the current time are not
    /// replayed. `Duration::MAX` replays every occurrence since the
    /// watermark.
    pub lookback: Duration,
}

/// Per-schedule overrides for [`CronScheduler::schedule_with`]. Construct via
/// [`ScheduleOptions::default`] + struct-update syntax:
///
/// ```
/// use std::collections::HashMap;
/// use taquba_cron::ScheduleOptions;
///
/// let opts = ScheduleOptions {
///     headers: HashMap::from([("target_url".into(), "https://example.com/hook".into())]),
///     priority: Some(taquba::PRIORITY_HIGH),
///     ..ScheduleOptions::default()
/// };
/// ```
#[derive(Debug, Clone, Default)]
pub struct ScheduleOptions {
    /// Headers attached to every [`taquba::JobRecord`] produced by this
    /// schedule. Useful for cron-driven webhooks (target URL, signing key
    /// id) or alert routing metadata. Names with the
    /// [`RESERVED_HEADER_PREFIX`] are rejected.
    pub headers: HashMap<String, String>,
    /// Override the queue's `default_priority` for jobs produced by this
    /// schedule. `None` (default) inherits the queue config. Lower numbers
    /// are claimed first; see [`taquba::PRIORITY_HIGH`], [`taquba::PRIORITY_NORMAL`],
    /// [`taquba::PRIORITY_LOW`].
    pub priority: Option<u32>,
    /// Override the queue's `max_attempts` for jobs produced by this
    /// schedule. `None` (default) inherits the queue config.
    pub max_attempts: Option<u32>,
    /// Replay firings missed while the scheduler was not running. `None`
    /// (default) drops them.
    pub backfill: Option<Backfill>,
}

struct ScheduleEntry {
    name: String,
    expression: Cron,
    target_queue: String,
    payload: Vec<u8>,
    headers: HashMap<String, String>,
    priority: Option<u32>,
    max_attempts: Option<u32>,
    backfill: Option<Backfill>,
    /// The next firing to enqueue. `None` until the first tick
    /// establishes it; under backfill it is held across a failed
    /// enqueue so the firing is retried.
    next_fire: Option<DateTime<Utc>>,
}

/// A single-process cron scheduler that enqueues jobs onto a [`Queue`] when
/// each of its registered expressions fires.
///
/// Build with [`Self::new`], register entries with [`Self::schedule`] /
/// [`Self::schedule_with`], then call [`Self::run`].
pub struct CronScheduler {
    queue: Arc<Queue>,
    entries: Vec<ScheduleEntry>,
}

impl CronScheduler {
    /// Build a new scheduler that targets `queue`.
    pub fn new(queue: Arc<Queue>) -> Self {
        Self {
            queue,
            entries: Vec::new(),
        }
    }

    /// Register a schedule. When `expression` fires, `payload` is enqueued on
    /// `target_queue`.
    ///
    /// `name` is used in the [`taquba::EnqueueOptions::dedup_key`] of every
    /// enqueued job (`"cron:{name}:{fire_time_ms}"`); it must be stable
    /// across restarts so a re-fire after a crash deduplicates correctly.
    pub fn schedule(
        &mut self,
        name: impl Into<String>,
        expression: &str,
        target_queue: impl Into<String>,
        payload: Vec<u8>,
    ) -> Result<&mut Self> {
        self.schedule_with(
            name,
            expression,
            target_queue,
            payload,
            ScheduleOptions::default(),
        )
    }

    /// Like [`Self::schedule`], but with one or more [`ScheduleOptions`]
    /// fields overridden.
    pub fn schedule_with(
        &mut self,
        name: impl Into<String>,
        expression: &str,
        target_queue: impl Into<String>,
        payload: Vec<u8>,
        opts: ScheduleOptions,
    ) -> Result<&mut Self> {
        let name = name.into();
        if self.entries.iter().any(|e| e.name == name) {
            return Err(Error::DuplicateName(name));
        }
        if let Some(header) = opts
            .headers
            .keys()
            .find(|k| k.starts_with(RESERVED_HEADER_PREFIX))
        {
            return Err(Error::ReservedHeader(header.clone()));
        }
        let parsed = Cron::new(expression)
            .parse()
            .map_err(|e| Error::InvalidExpression {
                expression: expression.to_string(),
                message: e.to_string(),
            })?;
        self.entries.push(ScheduleEntry {
            name,
            expression: parsed,
            target_queue: target_queue.into(),
            payload,
            headers: opts.headers,
            priority: opts.priority,
            max_attempts: opts.max_attempts,
            backfill: opts.backfill,
            next_fire: None,
        });
        Ok(self)
    }

    /// Delete the backfill watermark of the schedule `name` from `queue`.
    ///
    /// A watermark outlives its schedule; call this after removing a
    /// schedule that used [`ScheduleOptions::backfill`], or to make the
    /// schedule start over at the current time on its next run.
    pub async fn clear_watermark(queue: &Queue, name: &str) -> taquba::Result<()> {
        queue.kv_delete(&watermark_key(name)).await
    }

    /// Run the scheduler until `shutdown` resolves.
    ///
    /// Sleeps until the soonest next firing across all entries, enqueues
    /// everything that's now due, then recomputes. No fixed-quantum polling.
    pub async fn run<F>(mut self, shutdown: F) -> Result<()>
    where
        F: std::future::Future<Output = ()>,
    {
        tokio::pin!(shutdown);

        // Nothing to fire: just wait for shutdown rather than spin a no-op
        // loop with a fallback sleep.
        if self.entries.is_empty() {
            shutdown.await;
            return Ok(());
        }

        loop {
            let Some(soonest) = self.step(self.now()).await else {
                // All registered expressions are unsatisfiable (e.g.
                // `0 0 30 2 *`); cron expressions are static, so this
                // state can't change. Wait for shutdown rather than spin
                // a no-op loop.
                let names: Vec<&str> = self.entries.iter().map(|e| e.name.as_str()).collect();
                warn!(
                    schedules = ?names,
                    "all registered cron expressions are unsatisfiable; scheduler will not fire any jobs"
                );
                shutdown.await;
                return Ok(());
            };

            let sleep_for = (soonest - self.now()).to_std().unwrap_or(Duration::ZERO);

            tokio::select! {
                _ = sleep(sleep_for) => {}
                _ = &mut shutdown => return Ok(()),
            }
        }
    }

    /// The current time according to the queue's clock.
    fn now(&self) -> DateTime<Utc> {
        let ms = i64::try_from(self.queue.clock().now_ms()).unwrap_or(i64::MAX);
        DateTime::from_timestamp_millis(ms).unwrap_or(DateTime::<Utc>::MAX_UTC)
    }

    /// One scheduling tick: enqueue every entry whose next firing is at
    /// or before `now`, then return the soonest instant at which any
    /// entry needs attention (its next firing, or a retry of a failed
    /// enqueue under backfill), or `None` if every expression is
    /// unsatisfiable.
    async fn step(&mut self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let mut soonest: Option<DateTime<Utc>> = None;

        for i in 0..self.entries.len() {
            if let Some(wake) = self.tick_entry(i, now).await {
                soonest = Some(soonest.map_or(wake, |s| s.min(wake)));
            }
        }

        soonest
    }

    /// Advance one entry to `now`, enqueueing every firing at or before
    /// it, and return the instant the entry next needs attention.
    ///
    /// Without backfill an entry fires at most once per tick and the
    /// next occurrence is searched strictly after `now`, so occurrences
    /// between the fired one and `now` are skipped, and a failed enqueue
    /// is dropped the same way. With backfill the search is anchored at
    /// the fired occurrence, so every missed occurrence is enqueued in
    /// order, and a failed enqueue leaves `next_fire` in place for a
    /// retry after [`ENQUEUE_RETRY_DELAY`].
    async fn tick_entry(&mut self, i: usize, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        if self.entries[i].next_fire.is_none() {
            let anchor = match self.initial_anchor(i, now).await {
                Ok(anchor) => anchor,
                Err(e) => {
                    error!(name = %self.entries[i].name, error = %e, "failed to read cron watermark");
                    return Some(now + ENQUEUE_RETRY_DELAY);
                }
            };
            let entry = &mut self.entries[i];
            entry.next_fire = entry.expression.find_next_occurrence(&anchor, false).ok();
        }

        while let Some(fire_at) = self.entries[i].next_fire
            && fire_at <= now
        {
            match self.fire(i, fire_at).await {
                Ok(()) => {
                    let entry = &mut self.entries[i];
                    let anchor = if entry.backfill.is_some() {
                        fire_at
                    } else {
                        now
                    };
                    entry.next_fire = entry.expression.find_next_occurrence(&anchor, false).ok();
                }
                Err(e) => {
                    let entry = &mut self.entries[i];
                    error!(name = %entry.name, error = %e, "failed to enqueue cron job");
                    if entry.backfill.is_some() {
                        return Some(now + ENQUEUE_RETRY_DELAY);
                    }
                    entry.next_fire = entry.expression.find_next_occurrence(&now, false).ok();
                    break;
                }
            }
        }

        self.entries[i].next_fire
    }

    /// The instant after which the entry's first occurrence is searched:
    /// `now` without backfill or without a watermark, otherwise the
    /// persisted watermark, raised to the lookback floor.
    async fn initial_anchor(&self, i: usize, now: DateTime<Utc>) -> taquba::Result<DateTime<Utc>> {
        let entry = &self.entries[i];
        let Some(backfill) = &entry.backfill else {
            return Ok(now);
        };
        let Some(raw) = self.queue.kv_get(&watermark_key(&entry.name)).await? else {
            return Ok(now);
        };
        let Some(watermark) = parse_watermark(&raw) else {
            warn!(name = %entry.name, "malformed cron watermark; starting at the current time");
            return Ok(now);
        };
        match lookback_floor(now, backfill.lookback) {
            Some(floor) if watermark < floor => {
                warn!(
                    name = %entry.name,
                    %watermark,
                    %floor,
                    "cron firings older than the backfill lookback are skipped"
                );
                Ok(floor)
            }
            _ => Ok(watermark),
        }
    }

    /// Enqueue the firing of entry `i` at `fire_at`. Under backfill the
    /// watermark is written in the enqueue transaction; a dedup hit
    /// applies no KV write, so the watermark is then advanced separately.
    async fn fire(&self, i: usize, fire_at: DateTime<Utc>) -> taquba::Result<()> {
        let entry = &self.entries[i];
        let fire_ms = fire_at.timestamp_millis();
        let mut headers = entry.headers.clone();
        headers.insert(FIRE_MS_HEADER.to_string(), fire_ms.to_string());
        let opts = EnqueueOptions {
            dedup_key: Some(format!("cron:{}:{}", entry.name, fire_ms)),
            headers,
            priority: entry.priority,
            max_attempts: entry.max_attempts,
            ..Default::default()
        };
        if entry.backfill.is_some() {
            let key = watermark_key(&entry.name);
            let value = fire_ms.to_string().into_bytes();
            let writes = HashMap::from([(key.clone(), value.clone())]);
            let result = self
                .queue
                .enqueue_with_kv(&entry.target_queue, entry.payload.clone(), opts, writes)
                .await?;
            if matches!(result, EnqueueResult::AlreadyEnqueued(_)) {
                self.queue.kv_put(&key, &value).await?;
            }
        } else {
            self.queue
                .enqueue_with(&entry.target_queue, entry.payload.clone(), opts)
                .await?;
        }
        debug!(name = %entry.name, fire_ms, "enqueued cron job");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use taquba::object_store::memory::InMemory;

    async fn test_queue() -> Arc<Queue> {
        Arc::new(
            Queue::open(Arc::new(InMemory::new()), "test")
                .await
                .unwrap(),
        )
    }

    async fn mock_clock_queue(now: DateTime<Utc>) -> (Arc<Queue>, Arc<taquba::MockClock>) {
        let clock = Arc::new(taquba::MockClock::new(now.timestamp_millis() as u64));
        let queue = Queue::open_with_options(
            Arc::new(InMemory::new()),
            "test",
            taquba::OpenOptions {
                clock: clock.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        (Arc::new(queue), clock)
    }

    fn minutes(n: u64) -> Duration {
        Duration::from_secs(n * 60)
    }

    fn t0() -> DateTime<Utc> {
        DateTime::from_timestamp_millis(10 * 60_000).unwrap()
    }

    fn backfill(lookback: Duration) -> ScheduleOptions {
        ScheduleOptions {
            backfill: Some(Backfill { lookback }),
            ..Default::default()
        }
    }

    async fn pending_fire_ms(q: &Queue, queue: &str) -> Vec<i64> {
        let page = q
            .list_jobs(queue, taquba::JobStatus::Pending, None, 100)
            .await
            .unwrap();
        let mut times: Vec<i64> = page
            .jobs
            .iter()
            .map(|j| j.headers[FIRE_MS_HEADER].parse().unwrap())
            .collect();
        times.sort_unstable();
        times
    }

    async fn watermark(q: &Queue, name: &str) -> Option<i64> {
        q.kv_get(&watermark_key(name))
            .await
            .unwrap()
            .map(|v| std::str::from_utf8(&v).unwrap().parse().unwrap())
    }

    fn ms(t: DateTime<Utc>) -> i64 {
        t.timestamp_millis()
    }

    #[tokio::test]
    async fn rejects_invalid_expression() {
        let q = test_queue().await;
        let mut s = CronScheduler::new(q);
        match s.schedule("bad", "this is not a cron", "out", b"x".to_vec()) {
            Err(Error::InvalidExpression { .. }) => {}
            Ok(_) => panic!("expected InvalidExpression"),
            Err(other) => panic!("expected InvalidExpression, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn accepts_valid_posix_expression() {
        let q = test_queue().await;
        let mut s = CronScheduler::new(q);
        s.schedule("daily", "0 9 * * *", "reports", b"x".to_vec())
            .unwrap();
        s.schedule("hourly", "0 * * * *", "reports", b"y".to_vec())
            .unwrap();
        s.schedule("weekday-am", "0 9 * * 1-5", "reports", b"z".to_vec())
            .unwrap();
    }

    #[tokio::test]
    async fn rejects_duplicate_name() {
        let q = test_queue().await;
        let mut s = CronScheduler::new(q);
        s.schedule("once", "0 9 * * *", "reports1", b"x".to_vec())
            .unwrap();
        match s.schedule("once", "0 10 * * *", "reports2", b"y".to_vec()) {
            Err(Error::DuplicateName(name)) => assert_eq!(name, "once"),
            Err(other) => panic!("expected DuplicateName, got {other:?}"),
            Ok(_) => panic!("expected DuplicateName"),
        }
    }

    #[tokio::test]
    async fn rejects_a_reserved_header() {
        let q = test_queue().await;
        let mut s = CronScheduler::new(q);
        let result = s.schedule_with(
            "tagged",
            "0 9 * * *",
            "reports",
            b"x".to_vec(),
            ScheduleOptions {
                headers: HashMap::from([(FIRE_MS_HEADER.to_string(), "0".to_string())]),
                ..Default::default()
            },
        );
        match result {
            Err(Error::ReservedHeader(name)) => assert_eq!(name, FIRE_MS_HEADER),
            Err(other) => panic!("expected ReservedHeader, got {other:?}"),
            Ok(_) => panic!("expected ReservedHeader"),
        }
    }

    #[tokio::test]
    async fn schedule_options_carries_priority_and_max_attempts() {
        let q = test_queue().await;
        let mut s = CronScheduler::new(q);
        s.schedule_with(
            "boosted",
            "0 9 * * *",
            "reports",
            b"x".to_vec(),
            ScheduleOptions {
                priority: Some(taquba::PRIORITY_HIGH),
                max_attempts: Some(7),
                ..Default::default()
            },
        )
        .unwrap();
        let entry = &s.entries[0];
        assert_eq!(entry.priority, Some(taquba::PRIORITY_HIGH));
        assert_eq!(entry.max_attempts, Some(7));
    }

    #[tokio::test(start_paused = true)]
    async fn shuts_down_immediately_when_signal_fires() {
        let q = mock_clock_queue(t0()).await.0;
        let mut s = CronScheduler::new(q);
        s.schedule("daily", "0 9 * * *", "reports", b"x".to_vec())
            .unwrap();
        let start = tokio::time::Instant::now();
        s.run(async {}).await.unwrap();
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test]
    async fn every_job_carries_the_firing_time_header() {
        let q = test_queue().await;
        let mut s = CronScheduler::new(q.clone());
        s.schedule("minutely", "* * * * *", "out", b"x".to_vec())
            .unwrap();
        s.step(t0()).await;
        s.step(t0() + minutes(1)).await;
        assert_eq!(
            pending_fire_ms(&q, "out").await,
            vec![ms(t0() + minutes(1))]
        );
        assert_eq!(watermark(&q, "minutely").await, None);
    }

    #[tokio::test]
    async fn backfill_replays_every_missed_firing_in_order() {
        let q = test_queue().await;
        let mut s = CronScheduler::new(q.clone());
        s.schedule_with(
            "minutely",
            "* * * * *",
            "out",
            b"x".to_vec(),
            backfill(Duration::MAX),
        )
        .unwrap();

        let soonest0 = s.step(t0()).await.expect("satisfiable");
        assert_eq!(soonest0, t0() + minutes(1));
        assert_eq!(pending_fire_ms(&q, "out").await, Vec::<i64>::new());

        let now = t0() + minutes(5) + Duration::from_secs(30);
        let soonest1 = s.step(now).await.expect("satisfiable");
        assert_eq!(soonest1, t0() + minutes(6));
        let expected: Vec<i64> = (1..=5).map(|m| ms(t0() + minutes(m))).collect();
        assert_eq!(pending_fire_ms(&q, "out").await, expected);
        assert_eq!(watermark(&q, "minutely").await, Some(ms(t0() + minutes(5))));
    }

    #[tokio::test]
    async fn a_restarted_scheduler_resumes_after_the_completed_firing() {
        let q = test_queue().await;
        let mut first = CronScheduler::new(q.clone());
        first
            .schedule_with(
                "minutely",
                "* * * * *",
                "out",
                b"x".to_vec(),
                backfill(Duration::MAX),
            )
            .unwrap();
        first.step(t0()).await;
        first.step(t0() + minutes(1)).await;
        drop(first);

        let claim = q.claim("out", minutes(1)).await.unwrap().unwrap();
        q.ack(&claim).await.unwrap();
        assert_eq!(pending_fire_ms(&q, "out").await, Vec::<i64>::new());

        let mut second = CronScheduler::new(q.clone());
        second
            .schedule_with(
                "minutely",
                "* * * * *",
                "out",
                b"x".to_vec(),
                backfill(Duration::MAX),
            )
            .unwrap();
        let soonest = second
            .step(t0() + minutes(2) + Duration::from_secs(30))
            .await
            .expect("satisfiable");
        assert_eq!(soonest, t0() + minutes(3));
        assert_eq!(
            pending_fire_ms(&q, "out").await,
            vec![ms(t0() + minutes(2))]
        );
        assert_eq!(watermark(&q, "minutely").await, Some(ms(t0() + minutes(2))));
    }

    #[tokio::test]
    async fn the_lookback_bounds_the_replay() {
        let q = test_queue().await;
        q.kv_put(&watermark_key("minutely"), ms(t0()).to_string().as_bytes())
            .await
            .unwrap();
        let mut s = CronScheduler::new(q.clone());
        s.schedule_with(
            "minutely",
            "* * * * *",
            "out",
            b"x".to_vec(),
            backfill(minutes(5)),
        )
        .unwrap();
        let now = t0() + minutes(60);
        let soonest = s.step(now).await.expect("satisfiable");
        assert_eq!(soonest, now + minutes(1));
        let expected: Vec<i64> = (56..=60).map(|m| ms(t0() + minutes(m))).collect();
        assert_eq!(pending_fire_ms(&q, "out").await, expected);
        assert_eq!(watermark(&q, "minutely").await, Some(ms(now)));
    }

    #[tokio::test]
    async fn a_duplicate_firing_still_advances_the_watermark() {
        let q = test_queue().await;
        let fire_at = t0() + minutes(1);
        q.enqueue_with(
            "out",
            b"x".to_vec(),
            EnqueueOptions {
                dedup_key: Some(format!("cron:minutely:{}", ms(fire_at))),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        q.kv_put(&watermark_key("minutely"), ms(t0()).to_string().as_bytes())
            .await
            .unwrap();
        let mut s = CronScheduler::new(q.clone());
        s.schedule_with(
            "minutely",
            "* * * * *",
            "out",
            b"x".to_vec(),
            backfill(Duration::MAX),
        )
        .unwrap();
        s.step(fire_at + Duration::from_secs(30)).await;
        assert_eq!(q.stats("out").await.unwrap().pending, 1);
        assert_eq!(watermark(&q, "minutely").await, Some(ms(fire_at)));
    }

    #[tokio::test]
    async fn an_enqueue_error_under_backfill_holds_the_firing_for_retry() {
        let q = test_queue().await;
        let mut s = CronScheduler::new(q.clone());
        s.schedule_with(
            "minutely",
            "* * * * *",
            "q".repeat(300),
            b"x".to_vec(),
            backfill(Duration::MAX),
        )
        .unwrap();
        s.step(t0()).await;
        let now = t0() + minutes(2);
        let soonest = s.step(now).await.expect("retry scheduled");
        assert_eq!(soonest, now + ENQUEUE_RETRY_DELAY);
        assert_eq!(s.entries[0].next_fire, Some(t0() + minutes(1)));
        assert_eq!(watermark(&q, "minutely").await, None);
    }

    #[tokio::test]
    async fn a_malformed_watermark_starts_at_the_current_time() {
        let q = test_queue().await;
        q.kv_put(&watermark_key("minutely"), b"not a time")
            .await
            .unwrap();
        let mut s = CronScheduler::new(q.clone());
        s.schedule_with(
            "minutely",
            "* * * * *",
            "out",
            b"x".to_vec(),
            backfill(Duration::MAX),
        )
        .unwrap();
        let soonest = s.step(t0()).await.expect("satisfiable");
        assert_eq!(soonest, t0() + minutes(1));
        assert_eq!(q.stats("out").await.unwrap().pending, 0);
    }

    #[tokio::test]
    async fn clear_watermark_removes_the_key() {
        let q = test_queue().await;
        q.kv_put(&watermark_key("minutely"), b"600000")
            .await
            .unwrap();
        CronScheduler::clear_watermark(&q, "minutely")
            .await
            .unwrap();
        assert_eq!(watermark(&q, "minutely").await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn run_fires_on_the_queue_clock() {
        let (q, clock) = mock_clock_queue(t0() + Duration::from_secs(30)).await;
        let mut s = CronScheduler::new(q.clone());
        s.schedule("minutely", "* * * * *", "out", b"x".to_vec())
            .unwrap();
        let (stop, shutdown) = tokio::sync::oneshot::channel::<()>();
        let run = tokio::spawn(s.run(async {
            let _ = shutdown.await;
        }));

        for _ in 0..100 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert_eq!(q.stats("out").await.unwrap().pending, 0);
        }

        clock.advance(Duration::from_secs(30));
        let mut fired = false;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if q.stats("out").await.unwrap().pending == 1 {
                fired = true;
                break;
            }
        }
        assert!(fired, "the firing must follow the queue clock");
        assert_eq!(
            pending_fire_ms(&q, "out").await,
            vec![ms(t0() + minutes(1))]
        );

        stop.send(()).unwrap();
        run.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn step_fires_one_missed_firing_after_clock_jump() {
        let q = test_queue().await;
        let mut s = CronScheduler::new(q.clone());
        s.schedule("minutely", "* * * * *", "out", b"x".to_vec())
            .unwrap();

        // T0 is a whole number of minutes past epoch, so it lands
        // on a `* * * * *` occurrence.
        let t0 = DateTime::from_timestamp_millis(10 * 60_000).unwrap();

        // Phase 1: at T0 (cold start), next firing is T0+1m;
        // nothing enqueued yet.
        let soonest0 = s.step(t0).await.expect("satisfiable");
        assert_eq!(soonest0, t0 + Duration::from_secs(60));
        assert_eq!(q.stats("out").await.unwrap().pending, 0);

        // Phase 2: at T0+5m30s, the recorded T0+1m firing
        // enqueues; the missed T0+2m/3m/4m/5m firings are dropped
        // (no-backfill); the next firing advances to T0+6m.
        let now1 = t0 + Duration::from_secs(5 * 60 + 30);
        let soonest1 = s.step(now1).await.expect("satisfiable");
        assert_eq!(
            soonest1,
            t0 + Duration::from_secs(6 * 60),
            "next firing must skip past missed occurrences"
        );
        assert_eq!(q.stats("out").await.unwrap().pending, 1);
    }
}
