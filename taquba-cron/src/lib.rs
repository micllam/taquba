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
//! per-schedule overrides (HTTP-style headers, priority, max attempts):
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
//! All firing times are evaluated in UTC.
//!
//! # Guarantees
//!
//! - **At-most-once enqueue per firing.** Each firing is enqueued via Taquba
//!   with a deterministic [`taquba::EnqueueOptions::dedup_key`] of
//!   `"cron:{name}:{fire_time_ms}"`, so retries or duplicate attempts at
//!   the same firing instant cannot produce more than one job.
//! - **No backfill.** If the scheduler is offline when a firing should have
//!   happened, the missed firing is dropped — the next firing is the next
//!   *future* occurrence, not a replay of the missed ones.
//! - **Single-instance schedules.** A given schedule (identified by `name`)
//!   must be owned by at most one [`CronScheduler`] at a time.
//! - **No persistence.** Schedules live only in memory; rebuild them in code
//!   on startup. The *enqueued jobs* are durable via Taquba.

#![warn(missing_docs)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use croner::Cron;
use taquba::{EnqueueOptions, Queue};
use tokio::time::sleep;
use tracing::{debug, error, warn};

/// Errors returned by [`CronScheduler`].
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
    /// Underlying error from a Taquba queue operation.
    #[error(transparent)]
    Queue(#[from] taquba::Error),
}

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

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
    /// id) or alert routing metadata.
    pub headers: HashMap<String, String>,
    /// Override the queue's `default_priority` for jobs produced by this
    /// schedule. `None` (default) inherits the queue config. Lower numbers
    /// are claimed first; see [`taquba::PRIORITY_HIGH`], [`taquba::PRIORITY_NORMAL`],
    /// [`taquba::PRIORITY_LOW`].
    pub priority: Option<u32>,
    /// Override the queue's `max_attempts` for jobs produced by this
    /// schedule. `None` (default) inherits the queue config.
    pub max_attempts: Option<u32>,
}

struct ScheduleEntry {
    name: String,
    expression: Cron,
    target_queue: String,
    payload: Vec<u8>,
    headers: HashMap<String, String>,
    priority: Option<u32>,
    max_attempts: Option<u32>,
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
        });
        Ok(self)
    }

    /// Run the scheduler until `shutdown` resolves.
    ///
    /// Sleeps until the soonest next firing across all entries, enqueues
    /// everything that's now due, then recomputes. No fixed-quantum polling.
    pub async fn run<F>(self, shutdown: F) -> Result<()>
    where
        F: std::future::Future<Output = ()>,
    {
        let CronScheduler { queue, entries } = self;
        tokio::pin!(shutdown);

        // Nothing to fire: just wait for shutdown rather than spin a no-op
        // loop with a fallback sleep.
        if entries.is_empty() {
            shutdown.await;
            return Ok(());
        }

        let mut last_fired: Vec<Option<DateTime<Utc>>> = vec![None; entries.len()];

        loop {
            let now = Utc::now();
            // Compute the next firing per entry. `last_fired.max(now)` enforces
            // the no-backfill rule: if we drifted behind real time, jump
            // forward to "next firing after now" instead of replaying the
            // missed window.
            let next_per_entry: Vec<Option<DateTime<Utc>>> = entries
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    let after = match last_fired[i] {
                        Some(t) => t.max(now),
                        None => now,
                    };
                    e.expression.find_next_occurrence(&after, false).ok()
                })
                .collect();

            // All registered expressions are unsatisfiable (e.g. `0 0 30 2 *`)
            // — cron expressions are static, so this state can't change. Wait
            // for shutdown rather than spin a no-op loop.
            let Some(soonest) = next_per_entry.iter().filter_map(|x| *x).min() else {
                let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
                warn!(
                    schedules = ?names,
                    "all registered cron expressions are unsatisfiable; scheduler will not fire any jobs"
                );
                shutdown.await;
                return Ok(());
            };
            let sleep_for = (soonest - Utc::now()).to_std().unwrap_or(Duration::ZERO);

            tokio::select! {
                _ = sleep(sleep_for) => {}
                _ = &mut shutdown => return Ok(()),
            }

            for (i, entry) in entries.iter().enumerate() {
                let Some(fire_at) = next_per_entry[i] else {
                    continue;
                };
                // Re-read the wall-clock per entry: a slow enqueue earlier in
                // the loop can push real time past a later entry's `fire_at`,
                // and a stale `now` snapshot would cause that firing to be
                // silently skipped.
                if fire_at > Utc::now() {
                    continue;
                }
                let fire_ms = fire_at.timestamp_millis() as u64;
                let opts = EnqueueOptions {
                    dedup_key: Some(format!("cron:{}:{}", entry.name, fire_ms)),
                    headers: entry.headers.clone(),
                    priority: entry.priority,
                    max_attempts: entry.max_attempts,
                    ..Default::default()
                };
                match queue
                    .enqueue_with(&entry.target_queue, entry.payload.clone(), opts)
                    .await
                {
                    Ok(_) => {
                        debug!(name = %entry.name, fire_ms, "enqueued cron job");
                        last_fired[i] = Some(fire_at);
                    }
                    Err(e) => {
                        // Leave `last_fired` untouched so the next loop retries.
                        error!(name = %entry.name, error = %e, "failed to enqueue cron job");
                    }
                }
            }
        }
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

    #[tokio::test]
    async fn shuts_down_immediately_when_signal_fires() {
        let q = test_queue().await;
        let mut s = CronScheduler::new(q);
        s.schedule("daily", "0 9 * * *", "reports", b"x".to_vec())
            .unwrap();
        // Run scheduler with a future that's ready on first poll. Scheduler
        // should observe shutdown and return immediately rather than sleeping
        // until 9am.
        let start = std::time::Instant::now();
        s.run(async {}).await.unwrap();
        assert!(start.elapsed() < Duration::from_secs(1));
    }
}
