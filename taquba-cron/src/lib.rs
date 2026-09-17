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
//! use taquba_cron::{CronScheduler, Schedule};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let queue = Arc::new(Queue::open(Arc::new(InMemory::new()), "demo").await?);
//!
//! let scheduler = CronScheduler::new(queue);
//! scheduler.handle().schedule(Schedule::new(
//!     "daily-report",
//!     "0 9 * * *".parse()?,
//!     "reports",
//!     b"daily".to_vec(),
//! ))?;
//!
//! scheduler.run(std::future::pending::<()>()).await?;
//! # Ok(()) }
//! ```
//!
//! [`CronScheduler::spawn`] runs the scheduler as a Tokio task instead
//! and returns a [`taquba::WorkerHandle`] that stops it.
//!
//! # Per-schedule options
//!
//! A [`Schedule`] has a setter for each optional field: HTTP-style headers,
//! a priority, a maximum attempt count and backfill.
//!
//! ```
//! use std::collections::HashMap;
//! use taquba_cron::Schedule;
//!
//! let schedule = Schedule::new("hook", "0 9 * * *".parse()?, "hooks", b"ping".to_vec())
//!     .headers(HashMap::from([("target_url".into(), "https://example.com/hook".into())]))
//!     .priority(Some(taquba::PRIORITY_HIGH))
//!     .max_attempts(Some(10));
//! # Ok::<(), taquba_cron::Error>(())
//! ```
//!
//! Every enqueued job has the header [`FIRE_MS_HEADER`] (`cron.fire_ms`),
//! the firing time as milliseconds since the Unix epoch. The header
//! [`PREVIOUS_FIRE_MS_HEADER`] (`cron.previous_fire_ms`) is the occurrence
//! of the expression before the firing time, in the same form, so the two
//! headers bound the interval that the job covers. The scheduler computes
//! the previous occurrence from the expression, whether or not that
//! occurrence was enqueued. Header names with the `cron.` prefix are
//! reserved, and a schedule with such a header is rejected.
//!
//! # Backfill
//!
//! By default the scheduler drops a firing that it misses while it is not
//! running. A schedule with [`Schedule::backfill`] replays missed firings.
//! The scheduler stores the time of the last enqueued firing in the queue's
//! KV namespace, at the key [`watermark_key`]. On start it enqueues one job
//! per occurrence between that watermark and the current time, oldest first,
//! and then resumes live firings. [`Backfill::lookback`] bounds the replay,
//! and the scheduler skips an occurrence older than the lookback.
//!
//! ```
//! use std::time::Duration;
//! use taquba_cron::{Backfill, BackfillStart, Schedule};
//!
//! let schedule = Schedule::new("sweep", "0 * * * *".parse()?, "sweeps", b"sweep".to_vec())
//!     .backfill(Some(Backfill {
//!         lookback: Duration::from_secs(6 * 60 * 60),
//!         start: BackfillStart::CurrentTime,
//!     }));
//! # Ok::<(), taquba_cron::Error>(())
//! ```
//!
//! The scheduler writes the watermark in the transaction of the enqueue, so
//! the two commit together. The watermark advances only when a firing is
//! enqueued: an enqueue error under backfill keeps the schedule at the
//! failed firing, and the scheduler retries it.
//!
//! A replay enqueues one firing of a schedule at a time, and the other
//! schedules fire between two firings of the replay. A shutdown or a removal
//! of the schedule also takes effect there. A replay that a shutdown ends
//! resumes at the watermark on the next start.
//!
//! [`Backfill::start`] determines the start of a schedule without a
//! watermark. With [`BackfillStart::CurrentTime`] the schedule starts at the
//! current time and does not replay a firing. With
//! [`BackfillStart::Lookback`] the first run replays the occurrences within
//! the lookback. That start requires a bounded lookback, and a registration
//! with `Duration::MAX` fails with [`Error::UnboundedStart`].
//!
//! The watermark records a position in the occurrence sequence and is
//! independent of the expression. After an expression change, the scheduler
//! replays the missed occurrences of the new expression after the
//! watermark. The watermark stays in the KV namespace after its schedule is
//! removed, and [`CronScheduler::clear_watermark`] deletes it. Keys with the
//! `cron/` prefix of the KV namespace are reserved for this crate.
//!
//! # Changes while the scheduler runs
//!
//! [`CronScheduler::handle`] returns a [`ScheduleHandle`], which registers
//! and removes schedules before and during [`CronScheduler::run`]. The
//! running scheduler applies a change before its next firing. A schedule
//! registered through the handle starts at the time the scheduler applies
//! it, or at its watermark under backfill.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use taquba::{Queue, object_store::memory::InMemory};
//! # use taquba_cron::{CronScheduler, Schedule};
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! # let queue = Arc::new(Queue::open(Arc::new(InMemory::new()), "demo").await?);
//! let scheduler = CronScheduler::new(queue);
//! let handle = scheduler.handle();
//! let worker = scheduler.spawn(std::future::pending::<()>());
//!
//! handle.schedule(Schedule::new(
//!     "hourly-sweep",
//!     "0 * * * *".parse()?,
//!     "sweeps",
//!     b"sweep".to_vec(),
//! ))?;
//! assert!(handle.unschedule("hourly-sweep"));
//! # worker.shutdown().await?;
//! # Ok(()) }
//! ```
//!
//! [`ScheduleHandle::unschedule`] keeps the backfill watermark. A change of
//! an expression is an `unschedule` and a `schedule` with the same name, and
//! the schedule resumes at the watermark. After the scheduler stops, a
//! registration through the handle fails with [`Error::Stopped`].
//!
//! [`ScheduleHandle::replace_all`] takes the whole schedule set, for a
//! consumer that builds the set from configuration. A schedule equal to a
//! registered schedule is untouched and keeps its next firing. Every other
//! schedule is a new registration, and a registered schedule that is absent
//! from the set is removed. The call applies the whole set or, on an error,
//! no part of it.
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
//! A step follows a range or `*`, as in `5-59/5 * * * *`, and the form
//! `5/5 * * * *` is rejected. An expression with a seconds field or a year
//! field is rejected. An [`Expression`] is parsed from a string, and the
//! parse fails with [`Error::InvalidExpression`].
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
//!   should have happened, the missed firing is dropped, and the next firing
//!   is the next *future* occurrence. A schedule with
//!   [`Schedule::backfill`] set replays the missed firings within its
//!   lookback exactly once. Only the persisted watermark stops a firing from
//!   being enqueued twice, because claiming a job releases its dedup key.
//! - **Single-instance schedules.** A given schedule (identified by `name`)
//!   must be owned by at most one [`CronScheduler`] at a time.
//! - **No schedule persistence.** Schedules live only in memory; rebuild
//!   them in code on startup. The *enqueued jobs* are durable via Taquba,
//!   as is the backfill watermark.
//!
//! [Taquba]: https://docs.rs/taquba

#![warn(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use croner::Cron;
use croner::parser::{CronParser, Seconds};
use taquba::{EnqueueOptions, EnqueueResult, Queue, WorkerHandle};
use tokio::sync::Notify;
use tokio::time::sleep;
use tracing::{debug, error, warn};

/// Header attached to every enqueued job, storing the firing time as
/// milliseconds since the Unix epoch in decimal.
pub const FIRE_MS_HEADER: &str = "cron.fire_ms";

/// Header attached to every enqueued job, storing the occurrence of the
/// expression before the firing time as milliseconds since the Unix epoch
/// in decimal. It is absent for a firing without an earlier occurrence.
pub const PREVIOUS_FIRE_MS_HEADER: &str = "cron.previous_fire_ms";

/// Prefix of the header names reserved for this crate. A schedule whose
/// [`Schedule::headers`] contains a name with this prefix is rejected with
/// [`Error::ReservedHeader`].
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

/// A parsed 5-field cron expression, the type of [`Schedule::expression`].
///
/// ```
/// use taquba_cron::Expression;
///
/// let expression: Expression = "0 9 * * 1-5".parse()?;
/// # Ok::<(), taquba_cron::Error>(())
/// ```
///
/// The `Display` form is the text as the parser normalises it: trimmed, in
/// upper case, and with a month name, a day name or a nickname such as
/// `@daily` written as numbers. It parses to an equal expression. Two
/// expressions are equal when their `Display` forms are equal, so
/// `0 9 * * mon-fri` equals `0 9 * * 1-5`, and `*/5 * * * *` does not equal
/// `0-59/5 * * * *`.
#[derive(Debug, Clone)]
pub struct Expression(Cron);

impl PartialEq for Expression {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}

impl Eq for Expression {}

impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Expression {
    /// The first occurrence after `anchor`, or `None` for an expression
    /// without one.
    fn next_after(&self, anchor: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.0.find_next_occurrence(&anchor, false).ok()
    }

    /// The last occurrence before `at`, or `None` for an expression
    /// without one.
    fn previous_before(&self, at: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.0.find_previous_occurrence(&at, false).ok()
    }
}

impl std::str::FromStr for Expression {
    type Err = Error;

    /// Fails with [`Error::InvalidExpression`]. A seconds field or a year
    /// field is rejected.
    fn from_str(expression: &str) -> Result<Self> {
        CronParser::builder()
            .seconds(Seconds::Disallowed)
            .build()
            .parse(expression)
            .map(Expression)
            .map_err(|e| Error::InvalidExpression {
                expression: expression.to_string(),
                message: e.to_string(),
            })
    }
}

/// Errors returned by [`CronScheduler`] and by the parse of an
/// [`Expression`].
///
/// Every variant is permanent: retrying an identical call cannot succeed.
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
    /// The schedule has [`BackfillStart::Lookback`] and a lookback that does
    /// not bound the replay.
    #[error("schedule `{0}` starts at the lookback, and the lookback is unbounded")]
    UnboundedStart(String),
    /// The scheduler of this [`ScheduleHandle`] is stopped or dropped.
    #[error("the scheduler is stopped")]
    Stopped,
}

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Replay policy for firings missed while the scheduler was not running.
/// See the crate documentation, section "Backfill".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Backfill {
    /// Occurrences at or before this far before the current time are not
    /// replayed. `Duration::MAX` replays every occurrence since the
    /// watermark.
    pub lookback: Duration,
    /// The start of a schedule without a watermark.
    pub start: BackfillStart,
}

/// The start of a schedule under backfill that does not have a watermark: a
/// new schedule, or a schedule after [`CronScheduler::clear_watermark`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillStart {
    /// The schedule starts at the current time and does not replay a
    /// firing.
    CurrentTime,
    /// The schedule replays the occurrences within [`Backfill::lookback`].
    /// A lookback too large to subtract from the Unix epoch does not bound
    /// that replay, and the registration fails with
    /// [`Error::UnboundedStart`].
    Lookback,
}

/// A named cron expression with the job that each firing enqueues, the
/// parameter type of [`ScheduleHandle::schedule`].
///
/// ```
/// use std::collections::HashMap;
/// use taquba_cron::Schedule;
///
/// let schedule = Schedule::new("hook", "0 9 * * *".parse()?, "hooks", b"ping".to_vec())
///     .headers(HashMap::from([("target_url".into(), "https://example.com/hook".into())]))
///     .priority(Some(taquba::PRIORITY_HIGH));
/// # Ok::<(), taquba_cron::Error>(())
/// ```
///
/// Two schedules are equal when every field is equal. The expression
/// compares by its normalised text, so an equivalent expression in another
/// form makes two schedules unequal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule {
    /// The name of the schedule, unique within a scheduler. It is part of
    /// the [`taquba::EnqueueOptions::dedup_key`] of every enqueued job
    /// (`"cron:{name}:{fire_time_ms}"`) and of the backfill watermark key,
    /// so it must be stable across restarts.
    pub name: String,
    /// The expression whose occurrences are the firing times.
    pub expression: Expression,
    /// The queue on which each firing enqueues a job.
    pub queue: String,
    /// The payload of every enqueued job.
    pub payload: Vec<u8>,
    /// The headers of every enqueued job, for example the target URL of a
    /// webhook. The registration rejects a name with the
    /// [`RESERVED_HEADER_PREFIX`].
    pub headers: HashMap<String, String>,
    /// The priority of the enqueued jobs. `None` inherits the
    /// `default_priority` of the queue. A lower number is claimed first, as
    /// in [`taquba::PRIORITY_HIGH`], [`taquba::PRIORITY_NORMAL`] and
    /// [`taquba::PRIORITY_LOW`].
    pub priority: Option<u32>,
    /// The maximum attempt count of the enqueued jobs. `None` inherits the
    /// `max_attempts` of the queue.
    pub max_attempts: Option<u32>,
    /// The replay policy for firings missed while the scheduler is not
    /// running. With `None` the scheduler drops them.
    pub backfill: Option<Backfill>,
}

impl Schedule {
    /// A schedule without headers, overrides or backfill.
    pub fn new(
        name: impl Into<String>,
        expression: Expression,
        queue: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            name: name.into(),
            expression,
            queue: queue.into(),
            payload,
            headers: HashMap::new(),
            priority: None,
            max_attempts: None,
            backfill: None,
        }
    }

    /// Set [`Self::headers`].
    #[must_use]
    pub fn headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers = headers;
        self
    }

    /// Set [`Self::priority`].
    #[must_use]
    pub fn priority(mut self, priority: Option<u32>) -> Self {
        self.priority = priority;
        self
    }

    /// Set [`Self::max_attempts`].
    #[must_use]
    pub fn max_attempts(mut self, max_attempts: Option<u32>) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Set [`Self::backfill`].
    #[must_use]
    pub fn backfill(mut self, backfill: Option<Backfill>) -> Self {
        self.backfill = backfill;
        self
    }
}

/// A registered entry and its position in the occurrence sequence.
struct ActiveEntry {
    entry: Arc<Schedule>,
    /// The next firing to enqueue, `None` until the first tick sets it.
    /// Under backfill it is kept across a failed enqueue, and the firing
    /// is retried.
    next_fire: Option<DateTime<Utc>>,
}

/// The registered schedules, shared by a scheduler and its handles.
struct Registry {
    state: Mutex<RegistryState>,
    changed: Notify,
}

#[derive(Default)]
struct RegistryState {
    entries: Vec<Arc<Schedule>>,
    /// Incremented by every change of `entries`.
    version: u64,
    /// Set when the scheduler is dropped, which includes the return of
    /// [`CronScheduler::run`].
    stopped: bool,
}

/// The checks of one schedule that do not depend on the registry.
fn validate(schedule: &Schedule) -> Result<()> {
    if let Some(header) = schedule
        .headers
        .keys()
        .find(|k| k.starts_with(RESERVED_HEADER_PREFIX))
    {
        return Err(Error::ReservedHeader(header.clone()));
    }
    if let Some(backfill) = &schedule.backfill
        && backfill.start == BackfillStart::Lookback
        && lookback_floor(DateTime::UNIX_EPOCH, backfill.lookback).is_none()
    {
        return Err(Error::UnboundedStart(schedule.name.clone()));
    }
    Ok(())
}

impl Registry {
    fn register(&self, schedule: Schedule) -> Result<()> {
        validate(&schedule)?;
        let mut state = self.state.lock().unwrap();
        if state.stopped {
            return Err(Error::Stopped);
        }
        if state.entries.iter().any(|e| e.name == schedule.name) {
            return Err(Error::DuplicateName(schedule.name));
        }
        state.version += 1;
        state.entries.push(Arc::new(schedule));
        Ok(())
    }

    /// Replace the entries with `schedules` and return whether the set
    /// changed. An entry equal to a schedule keeps its `Arc`, which
    /// [`CronScheduler::sync`] uses to keep the position of the entry.
    fn replace_all(&self, schedules: Vec<Schedule>) -> Result<bool> {
        for (i, schedule) in schedules.iter().enumerate() {
            validate(schedule)?;
            if schedules[..i].iter().any(|s| s.name == schedule.name) {
                return Err(Error::DuplicateName(schedule.name.clone()));
            }
        }
        let mut state = self.state.lock().unwrap();
        if state.stopped {
            return Err(Error::Stopped);
        }
        let mut kept = 0;
        let entries: Vec<Arc<Schedule>> = schedules
            .into_iter()
            .map(
                |schedule| match state.entries.iter().find(|e| ***e == schedule) {
                    Some(entry) => {
                        kept += 1;
                        entry.clone()
                    }
                    None => Arc::new(schedule),
                },
            )
            .collect();
        // Names are unique on both sides, so equal counts mean equal sets.
        if kept == entries.len() && kept == state.entries.len() {
            return Ok(false);
        }
        state.version += 1;
        state.entries = entries;
        Ok(true)
    }
}

/// A single-process cron scheduler that enqueues jobs onto a [`Queue`] when
/// each of its registered expressions fires.
///
/// Build with [`Self::new`], register schedules through [`Self::handle`],
/// then call [`Self::run`]. The handle also registers and removes schedules
/// while the scheduler runs.
pub struct CronScheduler {
    queue: Arc<Queue>,
    registry: Arc<Registry>,
    active: Vec<ActiveEntry>,
    /// The registry version that `active` reflects.
    synced: u64,
}

impl Drop for CronScheduler {
    fn drop(&mut self) {
        self.registry.state.lock().unwrap().stopped = true;
    }
}

/// Registers and removes schedules on a [`CronScheduler`], before and
/// during [`CronScheduler::run`]. The running scheduler applies a change
/// before its next firing.
#[derive(Clone)]
pub struct ScheduleHandle {
    registry: Arc<Registry>,
}

impl ScheduleHandle {
    /// Register a schedule. It fails with [`Error::DuplicateName`],
    /// [`Error::ReservedHeader`] or [`Error::UnboundedStart`], and with
    /// [`Error::Stopped`] after the scheduler is dropped.
    pub fn schedule(&self, schedule: Schedule) -> Result<()> {
        self.registry.register(schedule)?;
        self.registry.changed.notify_one();
        Ok(())
    }

    /// Replace the registered schedules with `schedules`. A schedule equal
    /// to a registered schedule is untouched and keeps its next firing. Every
    /// other schedule is a new registration, and a registered schedule that
    /// is absent from `schedules` is removed, as by [`Self::unschedule`].
    ///
    /// The call applies the whole set or, on an error, no part of it. It
    /// fails with the errors of [`Self::schedule`], and with
    /// [`Error::DuplicateName`] for a name that `schedules` contains twice.
    pub fn replace_all(&self, schedules: Vec<Schedule>) -> Result<()> {
        if self.registry.replace_all(schedules)? {
            self.registry.changed.notify_one();
        }
        Ok(())
    }

    /// Remove the schedule `name` and return whether it was registered.
    /// The backfill watermark of the schedule stays in place. A firing that
    /// the scheduler enqueues during the call is still enqueued.
    pub fn unschedule(&self, name: &str) -> bool {
        let mut state = self.registry.state.lock().unwrap();
        let before = state.entries.len();
        state.entries.retain(|e| e.name != name);
        let removed = state.entries.len() < before;
        if removed {
            state.version += 1;
        }
        drop(state);
        if removed {
            self.registry.changed.notify_one();
        }
        removed
    }
}

impl CronScheduler {
    /// Build a new scheduler that targets `queue`.
    pub fn new(queue: Arc<Queue>) -> Self {
        Self {
            queue,
            registry: Arc::new(Registry {
                state: Mutex::default(),
                changed: Notify::new(),
            }),
            active: Vec::new(),
            synced: 0,
        }
    }

    /// A handle that registers and removes schedules while the scheduler
    /// runs.
    pub fn handle(&self) -> ScheduleHandle {
        ScheduleHandle {
            registry: self.registry.clone(),
        }
    }

    /// Delete the backfill watermark of the schedule `name` from `queue`.
    ///
    /// A watermark outlives its schedule; call this after removing a
    /// schedule that used [`Schedule::backfill`], or to make the
    /// schedule start over at the current time on its next run.
    pub async fn clear_watermark(queue: &Queue, name: &str) -> taquba::Result<()> {
        queue.kv_delete(&watermark_key(name)).await
    }

    /// Spawn [`Self::run`] as a Tokio task and return the handle that
    /// stops it.
    pub fn spawn<F>(self, shutdown: F) -> WorkerHandle<Result<()>>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        WorkerHandle::spawn(shutdown, |stop| self.run(stop.cancelled_owned()))
    }

    /// Run the scheduler until `shutdown` resolves.
    ///
    /// Sleeps until the soonest next firing across all entries, enqueues
    /// one due firing per entry, then recomputes. No fixed-quantum polling.
    /// A replay under backfill continues at the next tick, so `shutdown` and
    /// a change through a [`ScheduleHandle`] take effect between two firings
    /// of the replay.
    pub async fn run<F>(mut self, shutdown: F) -> Result<()>
    where
        F: std::future::Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        let registry = self.registry.clone();

        loop {
            let soonest = self.step(self.now()).await;
            if soonest.is_none() && !self.active.is_empty() {
                // Every registered expression is unsatisfiable (for
                // example `0 0 30 2 *`), so the loop waits for a change
                // of the registry or for shutdown.
                let names: Vec<&str> = self.active.iter().map(|a| a.entry.name.as_str()).collect();
                warn!(
                    schedules = ?names,
                    "all registered cron expressions are unsatisfiable; scheduler will not fire any jobs"
                );
            }
            let sleep_for = soonest.map(|at| (at - self.now()).to_std().unwrap_or(Duration::ZERO));
            let timer = async {
                match sleep_for {
                    Some(duration) => sleep(duration).await,
                    None => std::future::pending().await,
                }
            };

            tokio::select! {
                biased;
                _ = &mut shutdown => return Ok(()),
                _ = registry.changed.notified() => {}
                _ = timer => {}
            }
        }
    }

    /// Bring `active` in line with the registry: a removed entry leaves,
    /// and a new entry starts without a next firing.
    fn sync(&mut self) {
        let entries = {
            let state = self.registry.state.lock().unwrap();
            if state.version == self.synced {
                return;
            }
            self.synced = state.version;
            state.entries.clone()
        };
        self.active
            .retain(|a| entries.iter().any(|e| Arc::ptr_eq(e, &a.entry)));
        for entry in entries {
            if !self.active.iter().any(|a| Arc::ptr_eq(&a.entry, &entry)) {
                self.active.push(ActiveEntry {
                    entry,
                    next_fire: None,
                });
            }
        }
    }

    /// The current time according to the queue's clock.
    fn now(&self) -> DateTime<Utc> {
        let ms = i64::try_from(self.queue.clock().now_ms()).unwrap_or(i64::MAX);
        DateTime::from_timestamp_millis(ms).unwrap_or(DateTime::<Utc>::MAX_UTC)
    }

    /// One scheduling tick: enqueue one firing of every entry whose next
    /// firing is at or before `now`, then return the soonest instant at
    /// which any entry needs attention (its next firing, or a retry of a
    /// failed enqueue under backfill), or `None` if every expression is
    /// unsatisfiable. An entry with a further due firing returns an instant
    /// at or before `now`.
    async fn step(&mut self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.sync();
        let mut soonest: Option<DateTime<Utc>> = None;

        for i in 0..self.active.len() {
            if let Some(wake) = self.tick_entry(i, now).await {
                soonest = Some(soonest.map_or(wake, |s| s.min(wake)));
            }
        }

        soonest
    }

    /// Enqueue the entry's next firing when it is at or before `now`, and
    /// return the instant the entry next needs attention.
    ///
    /// Without backfill the next occurrence is searched strictly after
    /// `now`, so occurrences between the fired one and `now` are skipped,
    /// and a failed enqueue is dropped the same way. With backfill the
    /// search is anchored at the fired occurrence, so the following ticks
    /// enqueue every missed occurrence in order, and a failed enqueue
    /// leaves `next_fire` in place for a retry after
    /// [`ENQUEUE_RETRY_DELAY`].
    async fn tick_entry(&mut self, i: usize, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let entry = self.active[i].entry.clone();
        if self.active[i].next_fire.is_none() {
            let anchor = match self.initial_anchor(&entry, now).await {
                Ok(anchor) => anchor,
                Err(e) => {
                    error!(name = %entry.name, error = %e, "failed to read cron watermark");
                    return Some(now + ENQUEUE_RETRY_DELAY);
                }
            };
            self.active[i].next_fire = entry.expression.next_after(anchor);
        }

        if let Some(fire_at) = self.active[i].next_fire
            && fire_at <= now
        {
            match self.fire(&entry, fire_at).await {
                Ok(()) => {
                    let anchor = if entry.backfill.is_some() {
                        fire_at
                    } else {
                        now
                    };
                    self.active[i].next_fire = entry.expression.next_after(anchor);
                }
                Err(e) => {
                    error!(name = %entry.name, error = %e, "failed to enqueue cron job");
                    if entry.backfill.is_some() {
                        return Some(now + ENQUEUE_RETRY_DELAY);
                    }
                    self.active[i].next_fire = entry.expression.next_after(now);
                }
            }
        }

        self.active[i].next_fire
    }

    /// The instant after which the entry's first occurrence is searched:
    /// `now` without backfill, the [`BackfillStart`] without a watermark,
    /// otherwise the persisted watermark, raised to the lookback floor.
    async fn initial_anchor(
        &self,
        entry: &Schedule,
        now: DateTime<Utc>,
    ) -> taquba::Result<DateTime<Utc>> {
        let Some(backfill) = &entry.backfill else {
            return Ok(now);
        };
        let Some(raw) = self.queue.kv_get(&watermark_key(&entry.name)).await? else {
            // The registration rejects a lookback without a floor.
            return Ok(match backfill.start {
                BackfillStart::CurrentTime => now,
                BackfillStart::Lookback => lookback_floor(now, backfill.lookback).unwrap_or(now),
            });
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

    /// Enqueue the firing of `entry` at `fire_at`. Under backfill the
    /// watermark is written in the enqueue transaction; a dedup hit
    /// applies no KV write, so the watermark is then advanced separately.
    async fn fire(&self, entry: &Schedule, fire_at: DateTime<Utc>) -> taquba::Result<()> {
        let fire_ms = fire_at.timestamp_millis();
        let mut headers = entry.headers.clone();
        headers.insert(FIRE_MS_HEADER.to_string(), fire_ms.to_string());
        if let Some(previous) = entry.expression.previous_before(fire_at) {
            headers.insert(
                PREVIOUS_FIRE_MS_HEADER.to_string(),
                previous.timestamp_millis().to_string(),
            );
        }
        let opts = EnqueueOptions::default()
            .dedup_key(Some(format!("cron:{}:{}", entry.name, fire_ms)))
            .headers(headers)
            .priority(entry.priority)
            .max_attempts(entry.max_attempts);
        if entry.backfill.is_some() {
            let key = watermark_key(&entry.name);
            let value = fire_ms.to_string().into_bytes();
            let writes = HashMap::from([(key.clone(), value.clone())]);
            let result = self
                .queue
                .enqueue_with_kv(&entry.queue, entry.payload.clone(), opts, writes)
                .await?;
            if matches!(result, EnqueueResult::AlreadyEnqueued(_)) {
                self.queue.kv_put(&key, &value).await?;
            }
        } else {
            self.queue
                .enqueue_with(&entry.queue, entry.payload.clone(), opts)
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
            taquba::OpenOptions::default().clock(clock.clone()),
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

    fn backfill(lookback: Duration) -> Option<Backfill> {
        Some(Backfill {
            lookback,
            start: BackfillStart::CurrentTime,
        })
    }

    fn backfill_from_lookback(lookback: Duration) -> Option<Backfill> {
        Some(Backfill {
            lookback,
            start: BackfillStart::Lookback,
        })
    }

    /// Calls `step` until no entry has a due firing, as the run loop does.
    async fn step_to(s: &mut CronScheduler, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        loop {
            match s.step(now).await {
                Some(wake) if wake <= now => {}
                soonest => return soonest,
            }
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

    async fn previous_fire_ms(q: &Queue, queue: &str) -> Vec<i64> {
        let page = q
            .list_jobs(queue, taquba::JobStatus::Pending, None, 100)
            .await
            .unwrap();
        page.jobs
            .iter()
            .map(|j| j.headers[PREVIOUS_FIRE_MS_HEADER].parse().unwrap())
            .collect()
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

    #[test]
    fn an_expression_parses_the_five_field_syntax() {
        for expression in ["0 9 * * *", "0 * * * *", "0 9 * * 1-5", "5-59/5 * * * *"] {
            expression.parse::<Expression>().unwrap();
        }
        // A seconds field, a year field and a step without a range are
        // outside the 5-field syntax.
        for expression in [
            "this is not a cron",
            "0 0 9 * * *",
            "0 0 9 * * * 2030",
            "5/5 * * * *",
        ] {
            match expression.parse::<Expression>() {
                Err(Error::InvalidExpression { .. }) => {}
                Ok(_) => panic!("expected InvalidExpression for `{expression}`"),
                Err(other) => panic!("expected InvalidExpression, got {other:?}"),
            }
        }
    }

    #[test]
    fn an_expression_displays_and_compares_its_normalised_text() {
        for (text, display) in [
            ("*/5 * * * *", "*/5 * * * *"),
            (" 0 9 * * mon-fri ", "0 9 * * 1-5"),
            ("0  9 * * *", "0  9 * * *"),
            ("0 0 l * *", "0 0 L * *"),
            ("@daily", "0 0 * * *"),
        ] {
            let expression: Expression = text.parse().unwrap();
            assert_eq!(expression.to_string(), display);
            assert_eq!(expression, display.parse().unwrap());
        }

        // The comparison is of the text, so an equivalent expression in
        // another form is unequal.
        let every_five: Expression = "*/5 * * * *".parse().unwrap();
        assert_ne!(every_five, "0-59/5 * * * *".parse().unwrap());
    }

    #[tokio::test]
    async fn rejects_duplicate_name() {
        let q = test_queue().await;
        let s = CronScheduler::new(q);
        s.handle()
            .schedule(Schedule::new(
                "once",
                "0 9 * * *".parse().unwrap(),
                "reports1",
                b"x".to_vec(),
            ))
            .unwrap();
        match s.handle().schedule(Schedule::new(
            "once",
            "0 10 * * *".parse().unwrap(),
            "reports2",
            b"y".to_vec(),
        )) {
            Err(Error::DuplicateName(name)) => assert_eq!(name, "once"),
            Err(other) => panic!("expected DuplicateName, got {other:?}"),
            Ok(_) => panic!("expected DuplicateName"),
        }
        // A handle registers into the name set of its scheduler.
        let result = s.handle().schedule(Schedule::new(
            "once",
            "0 11 * * *".parse().unwrap(),
            "reports3",
            b"z".to_vec(),
        ));
        assert!(matches!(result, Err(Error::DuplicateName(name)) if name == "once"));
    }

    #[tokio::test]
    async fn a_schedule_registered_through_the_handle_fires() {
        let q = test_queue().await;
        let mut s = CronScheduler::new(q.clone());
        let handle = s.handle();
        assert_eq!(s.step(t0()).await, None);

        handle
            .schedule(Schedule::new(
                "minutely",
                "* * * * *".parse().unwrap(),
                "out",
                b"x".to_vec(),
            ))
            .unwrap();
        assert_eq!(s.step(t0()).await, Some(t0() + minutes(1)));
        s.step(t0() + minutes(1)).await;
        assert_eq!(
            pending_fire_ms(&q, "out").await,
            vec![ms(t0() + minutes(1))]
        );
    }

    #[tokio::test]
    async fn an_unscheduled_entry_stops_firing_and_releases_its_name() {
        let q = test_queue().await;
        let mut s = CronScheduler::new(q.clone());
        s.handle()
            .schedule(Schedule::new(
                "minutely",
                "* * * * *".parse().unwrap(),
                "out",
                b"x".to_vec(),
            ))
            .unwrap();
        let handle = s.handle();
        s.step(t0()).await;

        assert!(handle.unschedule("minutely"));
        assert!(!handle.unschedule("minutely"));
        assert_eq!(s.step(t0() + minutes(1)).await, None);
        assert_eq!(q.stats("out").await.unwrap().pending, 0);

        // The entry registered again starts at the current time.
        handle
            .schedule(Schedule::new(
                "minutely",
                "* * * * *".parse().unwrap(),
                "out",
                b"x".to_vec(),
            ))
            .unwrap();
        assert_eq!(s.step(t0() + minutes(5)).await, Some(t0() + minutes(6)));
        assert_eq!(q.stats("out").await.unwrap().pending, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn run_applies_a_handle_change_while_it_waits() {
        let (q, clock) = mock_clock_queue(t0() + Duration::from_secs(30)).await;
        let s = CronScheduler::new(q.clone());
        let handle = s.handle();
        let (stop, shutdown) = tokio::sync::oneshot::channel::<()>();
        let run = tokio::spawn(s.run(async {
            let _ = shutdown.await;
        }));
        // The scheduler waits without an entry and without a timer.
        tokio::time::sleep(Duration::from_secs(1)).await;

        handle
            .schedule(Schedule::new(
                "minutely",
                "* * * * *".parse().unwrap(),
                "out",
                b"x".to_vec(),
            ))
            .unwrap();
        // The entry starts at the time the loop applies it. The loop runs
        // before the clock advances.
        tokio::time::sleep(Duration::from_secs(1)).await;
        clock.advance(Duration::from_secs(30));
        let mut fired = false;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if q.stats("out").await.unwrap().pending == 1 {
                fired = true;
                break;
            }
        }
        assert!(fired, "the run loop must apply the registration");
        assert_eq!(
            pending_fire_ms(&q, "out").await,
            vec![ms(t0() + minutes(1))]
        );

        stop.send(()).unwrap();
        run.await.unwrap().unwrap();
        let result = handle.schedule(Schedule::new(
            "late",
            "* * * * *".parse().unwrap(),
            "out",
            b"x".to_vec(),
        ));
        assert!(matches!(result, Err(Error::Stopped)));
        let result = handle.replace_all(Vec::new());
        assert!(matches!(result, Err(Error::Stopped)));
    }

    #[tokio::test]
    async fn replace_all_keeps_an_equal_schedule_and_registers_the_rest() {
        let q = test_queue().await;
        let mut s = CronScheduler::new(q.clone());
        let handle = s.handle();
        let minutely = |name: &str, queue: &str| {
            Schedule::new(name, "* * * * *".parse().unwrap(), queue, b"x".to_vec())
        };
        handle
            .replace_all(vec![
                minutely("kept", "kept"),
                minutely("changed", "before"),
                minutely("removed", "removed"),
            ])
            .unwrap();
        s.step(t0()).await;

        handle
            .replace_all(vec![
                minutely("kept", "kept"),
                minutely("changed", "after"),
                minutely("added", "added"),
            ])
            .unwrap();

        // The equal schedule keeps its next firing. A changed schedule and
        // an added schedule start at the current time.
        let now = t0() + minutes(1);
        s.step(now).await;
        assert_eq!(pending_fire_ms(&q, "kept").await, vec![ms(now)]);
        for queue in ["before", "after", "removed", "added"] {
            assert_eq!(pending_fire_ms(&q, queue).await, Vec::<i64>::new());
        }

        let now = t0() + minutes(2);
        s.step(now).await;
        for queue in ["after", "added"] {
            assert_eq!(pending_fire_ms(&q, queue).await, vec![ms(now)]);
        }
        for queue in ["before", "removed"] {
            assert_eq!(pending_fire_ms(&q, queue).await, Vec::<i64>::new());
        }

        // An equal set is not a change of the registry.
        let version = s.registry.state.lock().unwrap().version;
        handle
            .replace_all(vec![
                minutely("added", "added"),
                minutely("kept", "kept"),
                minutely("changed", "after"),
            ])
            .unwrap();
        assert_eq!(s.registry.state.lock().unwrap().version, version);
    }

    #[tokio::test]
    async fn replace_all_applies_nothing_when_a_schedule_is_rejected() {
        let q = test_queue().await;
        let s = CronScheduler::new(q);
        let handle = s.handle();
        let minutely =
            |name: &str| Schedule::new(name, "* * * * *".parse().unwrap(), "out", b"x".to_vec());
        handle.schedule(minutely("registered")).unwrap();

        let reserved = minutely("reserved").headers(HashMap::from([(
            FIRE_MS_HEADER.to_string(),
            "0".to_string(),
        )]));
        let result = handle.replace_all(vec![minutely("first"), reserved]);
        assert!(matches!(result, Err(Error::ReservedHeader(_))));

        let result = handle.replace_all(vec![minutely("twice"), minutely("twice")]);
        match result {
            Err(Error::DuplicateName(name)) => assert_eq!(name, "twice"),
            other => panic!("expected DuplicateName, got {other:?}"),
        }

        let state = s.registry.state.lock().unwrap();
        let names: Vec<&str> = state.entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, ["registered"]);
    }

    #[tokio::test]
    async fn rejects_a_reserved_header() {
        let q = test_queue().await;
        let s = CronScheduler::new(q);
        let result = s.handle().schedule(
            Schedule::new(
                "tagged",
                "0 9 * * *".parse().unwrap(),
                "reports",
                b"x".to_vec(),
            )
            .headers(HashMap::from([(
                FIRE_MS_HEADER.to_string(),
                "0".to_string(),
            )])),
        );
        match result {
            Err(Error::ReservedHeader(name)) => assert_eq!(name, FIRE_MS_HEADER),
            Err(other) => panic!("expected ReservedHeader, got {other:?}"),
            Ok(_) => panic!("expected ReservedHeader"),
        }
    }

    #[tokio::test]
    async fn a_firing_has_the_priority_and_max_attempts_of_its_schedule() {
        let q = test_queue().await;
        let mut s = CronScheduler::new(q.clone());
        s.handle()
            .schedule(
                Schedule::new(
                    "boosted",
                    "* * * * *".parse().unwrap(),
                    "reports",
                    b"x".to_vec(),
                )
                .priority(Some(taquba::PRIORITY_HIGH))
                .max_attempts(Some(7)),
            )
            .unwrap();
        s.step(t0()).await;
        s.step(t0() + minutes(1)).await;

        let page = q
            .list_jobs("reports", taquba::JobStatus::Pending, None, 100)
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].priority, taquba::PRIORITY_HIGH);
        assert_eq!(page.jobs[0].max_attempts, 7);
    }

    #[tokio::test(start_paused = true)]
    async fn shuts_down_immediately_when_signal_fires() {
        let q = mock_clock_queue(t0()).await.0;
        let s = CronScheduler::new(q);
        s.handle()
            .schedule(Schedule::new(
                "daily",
                "0 9 * * *".parse().unwrap(),
                "reports",
                b"x".to_vec(),
            ))
            .unwrap();
        let start = tokio::time::Instant::now();
        s.run(async {}).await.unwrap();
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test]
    async fn every_job_has_the_firing_time_and_the_previous_occurrence() {
        let q = test_queue().await;
        let mut s = CronScheduler::new(q.clone());
        s.handle()
            .schedule(Schedule::new(
                "minutely",
                "* * * * *".parse().unwrap(),
                "out",
                b"x".to_vec(),
            ))
            .unwrap();
        s.step(t0()).await;
        s.step(t0() + minutes(1)).await;
        assert_eq!(
            pending_fire_ms(&q, "out").await,
            vec![ms(t0() + minutes(1))]
        );
        assert_eq!(watermark(&q, "minutely").await, None);
        assert_eq!(previous_fire_ms(&q, "out").await, vec![ms(t0())]);

        // The previous occurrence comes from the expression: this scheduler
        // first ticks one hour before the firing of a weekly schedule.
        let monday: DateTime<Utc> = "2026-09-14T02:00:00Z".parse().unwrap();
        s.handle()
            .schedule(Schedule::new(
                "weekly",
                "0 2 * * 1".parse().unwrap(),
                "weekly",
                b"x".to_vec(),
            ))
            .unwrap();
        s.step(monday - minutes(60)).await;
        s.step(monday).await;
        assert_eq!(pending_fire_ms(&q, "weekly").await, vec![ms(monday)]);
        assert_eq!(
            previous_fire_ms(&q, "weekly").await,
            vec![ms(monday - minutes(7 * 24 * 60))]
        );
    }

    #[tokio::test]
    async fn backfill_replays_every_missed_firing_in_order() {
        let q = test_queue().await;
        let mut s = CronScheduler::new(q.clone());
        s.handle()
            .schedule(
                Schedule::new(
                    "minutely",
                    "* * * * *".parse().unwrap(),
                    "out",
                    b"x".to_vec(),
                )
                .backfill(backfill(Duration::MAX)),
            )
            .unwrap();

        let soonest0 = s.step(t0()).await.expect("satisfiable");
        assert_eq!(soonest0, t0() + minutes(1));
        assert_eq!(pending_fire_ms(&q, "out").await, Vec::<i64>::new());

        let now = t0() + minutes(5) + Duration::from_secs(30);
        let soonest1 = step_to(&mut s, now).await.expect("satisfiable");
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
            .handle()
            .schedule(
                Schedule::new(
                    "minutely",
                    "* * * * *".parse().unwrap(),
                    "out",
                    b"x".to_vec(),
                )
                .backfill(backfill(Duration::MAX)),
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
            .handle()
            .schedule(
                Schedule::new(
                    "minutely",
                    "* * * * *".parse().unwrap(),
                    "out",
                    b"x".to_vec(),
                )
                .backfill(backfill(Duration::MAX)),
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
        let now = t0() + minutes(60);
        q.kv_put(&watermark_key("minutely"), ms(t0()).to_string().as_bytes())
            .await
            .unwrap();
        let recent = now - minutes(3);
        q.kv_put(&watermark_key("recent"), ms(recent).to_string().as_bytes())
            .await
            .unwrap();
        let mut s = CronScheduler::new(q.clone());
        for (name, queue) in [("minutely", "out"), ("recent", "recent")] {
            s.handle()
                .schedule(
                    Schedule::new(name, "* * * * *".parse().unwrap(), queue, b"x".to_vec())
                        .backfill(backfill(minutes(5))),
                )
                .unwrap();
        }
        let soonest = step_to(&mut s, now).await.expect("satisfiable");
        assert_eq!(soonest, now + minutes(1));

        // A watermark older than the lookback is raised to the lookback.
        let expected: Vec<i64> = (56..=60).map(|m| ms(t0() + minutes(m))).collect();
        assert_eq!(pending_fire_ms(&q, "out").await, expected);
        assert_eq!(watermark(&q, "minutely").await, Some(ms(now)));

        // A watermark within the lookback is the start of the replay.
        let expected: Vec<i64> = (1..=3).map(|m| ms(recent + minutes(m))).collect();
        assert_eq!(pending_fire_ms(&q, "recent").await, expected);
        assert_eq!(watermark(&q, "recent").await, Some(ms(now)));
    }

    #[tokio::test]
    async fn a_schedule_without_a_watermark_starts_at_the_lookback() {
        let q = test_queue().await;
        let mut s = CronScheduler::new(q.clone());
        s.handle()
            .schedule(
                Schedule::new(
                    "minutely",
                    "* * * * *".parse().unwrap(),
                    "out",
                    b"x".to_vec(),
                )
                .backfill(backfill_from_lookback(minutes(5))),
            )
            .unwrap();
        let now = t0() + minutes(60);
        let soonest = step_to(&mut s, now).await.expect("satisfiable");
        assert_eq!(soonest, now + minutes(1));

        let expected: Vec<i64> = (56..=60).map(|m| ms(t0() + minutes(m))).collect();
        assert_eq!(pending_fire_ms(&q, "out").await, expected);
        assert_eq!(watermark(&q, "minutely").await, Some(ms(now)));
    }

    #[tokio::test]
    async fn a_start_at_an_unbounded_lookback_is_rejected() {
        let s = CronScheduler::new(test_queue().await);
        let result = s.handle().schedule(
            Schedule::new(
                "minutely",
                "* * * * *".parse().unwrap(),
                "out",
                b"x".to_vec(),
            )
            .backfill(backfill_from_lookback(Duration::MAX)),
        );
        match result {
            Err(Error::UnboundedStart(name)) => assert_eq!(name, "minutely"),
            Err(other) => panic!("expected UnboundedStart, got {other:?}"),
            Ok(_) => panic!("expected UnboundedStart"),
        }
    }

    #[tokio::test]
    async fn a_replay_enqueues_one_firing_per_entry_per_tick() {
        let q = test_queue().await;
        q.kv_put(&watermark_key("replayed"), ms(t0()).to_string().as_bytes())
            .await
            .unwrap();
        let mut s = CronScheduler::new(q.clone());
        let handle = s.handle();
        s.handle()
            .schedule(
                Schedule::new(
                    "replayed",
                    "* * * * *".parse().unwrap(),
                    "out",
                    b"x".to_vec(),
                )
                .backfill(backfill(Duration::MAX)),
            )
            .unwrap();
        s.handle()
            .schedule(Schedule::new(
                "live",
                "* * * * *".parse().unwrap(),
                "live",
                b"x".to_vec(),
            ))
            .unwrap();
        s.step(t0()).await;

        // The replay returns a due instant, and the live entry fires in the
        // same tick.
        let now = t0() + minutes(5);
        let soonest = s.step(now).await.expect("satisfiable");
        assert_eq!(soonest, t0() + minutes(2));
        assert_eq!(
            pending_fire_ms(&q, "out").await,
            vec![ms(t0() + minutes(1))]
        );
        assert_eq!(
            pending_fire_ms(&q, "live").await,
            vec![ms(t0() + minutes(1))]
        );

        // A removal between two ticks ends the replay.
        assert!(handle.unschedule("replayed"));
        step_to(&mut s, now).await;
        assert_eq!(
            pending_fire_ms(&q, "out").await,
            vec![ms(t0() + minutes(1))]
        );
    }

    #[tokio::test]
    async fn run_observes_shutdown_between_the_firings_of_a_replay() {
        let (q, _clock) = mock_clock_queue(t0() + minutes(5)).await;
        q.kv_put(&watermark_key("minutely"), ms(t0()).to_string().as_bytes())
            .await
            .unwrap();
        let s = CronScheduler::new(q.clone());
        s.handle()
            .schedule(
                Schedule::new(
                    "minutely",
                    "* * * * *".parse().unwrap(),
                    "out",
                    b"x".to_vec(),
                )
                .backfill(backfill(Duration::MAX)),
            )
            .unwrap();

        s.run(std::future::ready(())).await.unwrap();
        assert_eq!(
            pending_fire_ms(&q, "out").await,
            vec![ms(t0() + minutes(1))]
        );
        assert_eq!(watermark(&q, "minutely").await, Some(ms(t0() + minutes(1))));
    }

    #[tokio::test]
    async fn a_duplicate_firing_still_advances_the_watermark() {
        let q = test_queue().await;
        let fire_at = t0() + minutes(1);
        q.enqueue_with(
            "out",
            b"x".to_vec(),
            EnqueueOptions::default().dedup_key(Some(format!("cron:minutely:{}", ms(fire_at)))),
        )
        .await
        .unwrap();
        q.kv_put(&watermark_key("minutely"), ms(t0()).to_string().as_bytes())
            .await
            .unwrap();
        let mut s = CronScheduler::new(q.clone());
        s.handle()
            .schedule(
                Schedule::new(
                    "minutely",
                    "* * * * *".parse().unwrap(),
                    "out",
                    b"x".to_vec(),
                )
                .backfill(backfill(Duration::MAX)),
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
        s.handle()
            .schedule(
                Schedule::new(
                    "minutely",
                    "* * * * *".parse().unwrap(),
                    "q".repeat(300),
                    b"x".to_vec(),
                )
                .backfill(backfill(Duration::MAX)),
            )
            .unwrap();
        s.step(t0()).await;
        let now = t0() + minutes(2);
        let soonest = s.step(now).await.expect("retry scheduled");
        assert_eq!(soonest, now + ENQUEUE_RETRY_DELAY);
        assert_eq!(s.active[0].next_fire, Some(t0() + minutes(1)));
        assert_eq!(watermark(&q, "minutely").await, None);
    }

    #[tokio::test]
    async fn a_malformed_watermark_starts_at_the_current_time() {
        let q = test_queue().await;
        q.kv_put(&watermark_key("minutely"), b"not a time")
            .await
            .unwrap();
        let mut s = CronScheduler::new(q.clone());
        s.handle()
            .schedule(
                Schedule::new(
                    "minutely",
                    "* * * * *".parse().unwrap(),
                    "out",
                    b"x".to_vec(),
                )
                .backfill(backfill(Duration::MAX)),
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
        let s = CronScheduler::new(q.clone());
        s.handle()
            .schedule(Schedule::new(
                "minutely",
                "* * * * *".parse().unwrap(),
                "out",
                b"x".to_vec(),
            ))
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
        s.handle()
            .schedule(Schedule::new(
                "minutely",
                "* * * * *".parse().unwrap(),
                "out",
                b"x".to_vec(),
            ))
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
