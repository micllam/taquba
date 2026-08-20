//! Writer-liveness observables for admin tooling.
//!
//! A [`QueueReader`](crate::QueueReader) can observe a store without
//! fencing its writer, but nothing in the queue's job state indicates
//! whether a writer process is alive: the lease is process state with
//! no reader view, and opening a [`Queue`](crate::Queue) as a probe
//! fences a live writer and re-queues every claimed job. This module
//! defines the two observables a reader exposes instead:
//!
//! - [`StoreActivity`], read from the store's manifest and durable
//!   sequence number with no writer cooperation. Wall-clock fields are
//!   for display only; a caller deciding a destructive action watches
//!   [`StoreActivity::durable_seq`] for advance over a few poll
//!   intervals, a judgment free of clock comparison.
//! - [`WriterHeartbeat`], the record a writer with
//!   [`OpenOptions::liveness_heartbeat`](crate::OpenOptions::liveness_heartbeat)
//!   enabled commits on an interval. A beat is an ordinary store
//!   commit, so a superseded writer stops producing observable beats
//!   at its next flush: a fresh beat proves the process that owns the
//!   store is alive, and proves nothing about that process's workers.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use slatedb::Db;
use tokio::sync::watch;
use tracing::{debug, error};

use crate::clock::Clock;
use crate::error::Result;
use crate::keys::heartbeat_key;

/// Store-level activity read from a [`QueueReader`](crate::QueueReader),
/// with no writer cooperation required.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreActivity {
    /// Timestamp in milliseconds embedded in the newest L0 SST the
    /// manifest references, or `None` when the manifest lists no L0
    /// SSTs (a fresh store or one whose L0 was fully compacted).
    /// Flush-granular and read from the writer's clock: WAL writes do
    /// not update it and an idle writer leaves it stale, so it is for
    /// display only, never an input to a destructive decision.
    pub last_flush_at_ms: Option<u64>,
    /// The writer epoch recorded in the manifest. Advances each time a
    /// process opens the store as its writer.
    pub writer_epoch: u64,
    /// Sequence number at or below which every write is durably
    /// persisted, as observed by this reader's view. Free of clock
    /// skew and commit-granular: observing it advance across a few
    /// [`ReaderOptions::manifest_poll_interval`](crate::ReaderOptions::manifest_poll_interval)s
    /// proves live commits, the check a destructive operation performs
    /// before proceeding.
    pub durable_seq: u64,
}

/// The most recent liveness beat committed by a writer that has
/// [`OpenOptions::liveness_heartbeat`](crate::OpenOptions::liveness_heartbeat)
/// enabled, read through
/// [`QueueReader::writer_heartbeat`](crate::QueueReader::writer_heartbeat).
///
/// A beat is an ordinary store commit, so it proves the process that
/// owns the store was alive when the beat became durable; it proves
/// nothing about that process's workers. Judge staleness in units of
/// [`Self::interval`], allowing for the reader's own lag and for the
/// writer's commit latency (successive beats are spaced the interval
/// plus one durable commit apart, and a commit waits for the store's
/// flush interval), or watch [`Self::counter`] advance across polls to
/// avoid clock comparison entirely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterHeartbeat {
    /// Beat counter, increasing across beats and across writer
    /// restarts.
    pub counter: u64,
    /// The writer clock's time in milliseconds when the beat was
    /// written.
    pub at_ms: u64,
    /// The interval between the writer's beats.
    pub interval: Duration,
    /// The writer epoch the process held when it opened the store. A
    /// beat whose epoch is below the manifest's current writer epoch
    /// was written by a superseded writer.
    pub writer_epoch: u64,
}

/// Serialized form of a heartbeat, stored under the heartbeat key.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct HeartbeatRecord {
    pub(crate) counter: u64,
    pub(crate) at_ms: u64,
    pub(crate) interval_ms: u64,
    pub(crate) writer_epoch: u64,
}

impl HeartbeatRecord {
    pub(crate) fn into_public(self) -> WriterHeartbeat {
        WriterHeartbeat {
            counter: self.counter,
            at_ms: self.at_ms,
            interval: Duration::from_millis(self.interval_ms),
            writer_epoch: self.writer_epoch,
        }
    }
}

/// Commit one beat, awaiting durability so the beat is observable by
/// readers when the call returns and a fencing failure surfaces to the
/// caller.
pub(crate) async fn write_beat(
    db: &Db,
    clock: &dyn Clock,
    counter: u64,
    interval: Duration,
    writer_epoch: u64,
) -> Result<()> {
    let record = HeartbeatRecord {
        counter,
        at_ms: clock.now_ms(),
        interval_ms: interval.as_millis() as u64,
        writer_epoch,
    };
    let bytes = rmp_serde::to_vec(&record)?;
    db.put(heartbeat_key(), bytes).await?;
    Ok(())
}

/// Read the counter of the stored beat, so a reopening writer
/// continues the sequence.
pub(crate) async fn stored_counter(db: &Db) -> Result<u64> {
    match db.get(heartbeat_key()).await? {
        Some(bytes) => {
            let record: HeartbeatRecord = rmp_serde::from_slice(&bytes)?;
            Ok(record.counter)
        }
        None => Ok(0),
    }
}

/// Background task committing one beat per interval while the queue is
/// open. A failed beat is logged at error level and counted; the task
/// continues, because a fencing failure also fails the queue's own
/// writes and those surface to callers.
pub(crate) struct HeartbeatTask {
    pub(crate) db: Arc<Db>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) interval: Duration,
    pub(crate) next_counter: u64,
    pub(crate) writer_epoch: u64,
}

impl HeartbeatTask {
    pub(crate) async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.interval) => {
                    let counter = self.next_counter;
                    // Advance past the attempted counter even on failure: a
                    // failed put can still have committed durably, and
                    // reusing its counter would make the next beat repeat it,
                    // stalling a reader that watches the counter for advance.
                    // A gap left by a failed beat is harmless; the counter is
                    // only required to increase.
                    self.next_counter += 1;
                    if let Err(e) = write_beat(
                        &self.db,
                        self.clock.as_ref(),
                        counter,
                        self.interval,
                        self.writer_epoch,
                    )
                    .await
                    {
                        crate::obs::heartbeat_failed();
                        error!(
                            "liveness heartbeat commit failed: {e}; a fencing \
                             error indicates another process has opened this \
                             store as its writer"
                        );
                    }
                }
                _ = shutdown.changed() => break,
            }
        }
        debug!("liveness heartbeat stopped");
    }
}
