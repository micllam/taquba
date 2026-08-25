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

use crate::background::Periodic;
use crate::clock::Clock;
use crate::error::Result;
use crate::job::JobRecord;
use crate::keys::pending_prefix;
use crate::read::{list_queues, stats};
use slatedb::Db;

pub(crate) struct MetricsSampler {
    pub(crate) db: Arc<Db>,
    pub(crate) clock: Arc<dyn Clock>,
}

impl Periodic for MetricsSampler {
    const NAME: &'static str = "metrics sampler";

    async fn step(&self) -> Result<()> {
        sample(&self.db, self.clock.as_ref()).await
    }
}

/// Read each queue's depth and oldest-pending age once and set the gauges.
async fn sample(db: &Db, clock: &dyn Clock) -> Result<()> {
    let now = clock.now_ms();
    for queue in list_queues(db).await? {
        let stats = stats(db, &queue).await?;
        crate::obs::set_depth(&queue, stats.pending, stats.claimed);

        // The front of the pending prefix is the next job to be claimed; its
        // age is how long that job has waited so far, which climbs when the
        // queue is not being drained fast enough.
        let mut iter = db.scan_prefix(pending_prefix(&queue), ..).await?;
        let age_secs = match iter.next().await? {
            Some(kv) => {
                let job = JobRecord::decode(&kv.key, &kv.value)?;
                now.saturating_sub(job.enqueued_at) as f64 / 1000.0
            }
            None => 0.0,
        };
        crate::obs::set_oldest_pending_age_seconds(&queue, age_secs);
    }
    Ok(())
}
