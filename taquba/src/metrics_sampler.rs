//! Background task that periodically samples point-in-time queue metrics
//! (depth gauges and the age of the oldest pending job) and emits them through
//! the [`crate::obs`] facade.
//!
//! Event metrics (counters and latency histograms) are emitted inline at the
//! transition sites; only the gauges, which describe a point-in-time state
//! rather than an event, are sampled here. The whole module is compiled only
//! with the `metrics` feature, and the task runs only when
//! [`crate::OpenOptions::metrics_sample_interval`] is set.

use std::sync::Arc;
use std::time::Duration;

use slatedb::Db;
use tokio::sync::watch;
use tracing::{debug, warn};

use crate::clock::Clock;
use crate::error::Result;
use crate::job::JobRecord;
use crate::queue::pending_prefix;
use crate::stats::read_stats;

pub(crate) struct MetricsSampler {
    pub(crate) db: Arc<Db>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) interval: Duration,
}

impl MetricsSampler {
    pub(crate) async fn run(self, mut shutdown: watch::Receiver<bool>) {
        let MetricsSampler {
            db,
            clock,
            interval,
        } = self;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    if let Err(e) = sample(&db, clock.as_ref()).await {
                        warn!("metrics sampler error: {e}");
                    }
                }
                _ = shutdown.changed() => break,
            }
        }
        debug!("metrics sampler stopped");
    }
}

/// Read each queue's depth and oldest-pending age once and set the gauges.
async fn sample(db: &Db, clock: &dyn Clock) -> Result<()> {
    let now = clock.now_ms();
    for queue in queues(db).await? {
        let stats = read_stats(db, &queue).await?;
        crate::obs::set_depth(&queue, stats.pending, stats.claimed);

        // The front of the pending prefix is the next job to be claimed; its
        // age is how long that job has waited so far, which climbs when the
        // queue is not being drained fast enough.
        let mut iter = db
            .scan_prefix(pending_prefix(&queue).as_bytes(), ..)
            .await?;
        let age_secs = match iter.next().await? {
            Some(kv) => {
                let job: JobRecord = rmp_serde::from_slice(&kv.value)?;
                now.saturating_sub(job.enqueued_at) as f64 / 1000.0
            }
            None => 0.0,
        };
        crate::obs::set_oldest_pending_age_seconds(&queue, age_secs);
    }
    Ok(())
}

/// Distinct queue names, discovered from the `stats:` key space (the same
/// source as [`crate::Queue::list_queues`]).
async fn queues(db: &Db) -> Result<Vec<String>> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    let mut iter = db.scan_prefix(b"stats:", ..).await?;
    while let Some(kv) = iter.next().await? {
        let Ok(key) = std::str::from_utf8(&kv.key) else {
            continue;
        };
        // Key: "stats:{queue}:{metric}".
        let without_prefix = key.strip_prefix("stats:").unwrap_or(key);
        if let Some(idx) = without_prefix.rfind(':') {
            let queue = &without_prefix[..idx];
            if seen.insert(queue.to_string()) {
                out.push(queue.to_string());
            }
        }
    }
    Ok(out)
}
