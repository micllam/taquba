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
use crate::error::Result;
use crate::job::JobRecord;
use crate::keys::pending_prefix;
use crate::queue_core::QueueCore;
use crate::read::{list_queues, stats};
pub(crate) struct MetricsSampler {
    core: Arc<QueueCore>,
}

impl MetricsSampler {
    pub(crate) fn new(core: Arc<QueueCore>) -> Self {
        Self { core }
    }
}

impl Periodic for MetricsSampler {
    const NAME: &'static str = "metrics sampler";

    async fn step(&self) -> Result<()> {
        sample(&self.core).await
    }
}

/// Read each queue's depth and oldest-pending age once and set the gauges.
async fn sample(core: &QueueCore) -> Result<()> {
    let db = core.db.as_ref();
    let now = core.now_ms();
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

#[cfg(test)]
mod tests {
    use crate::test_util::*;

    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn metrics_sampler_emits_pending_depth_gauge() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // Only this test installs a global recorder (the obs unit test uses a
        // local one), so the install succeeds and the snapshotter observes the
        // sampler running in its spawned task.
        let _ = recorder.install();

        let q = Queue::open_with_options(
            make_store(),
            "test",
            OpenOptions {
                metrics_sample_interval: Some(Duration::from_millis(25)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        for _ in 0..3 {
            q.enqueue("gsamp", vec![0u8; 8]).await.unwrap();
        }

        let mut gauge = None;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            for (composite, _unit, _desc, value) in snapshotter.snapshot().into_vec() {
                let key = composite.key();
                let ours = key.name() == "taquba_pending_jobs"
                    && key
                        .labels()
                        .any(|l| l.key() == "queue" && l.value() == "gsamp");
                if ours && let DebugValue::Gauge(g) = value {
                    gauge = Some(g.into_inner());
                }
            }
            if gauge == Some(3.0) {
                break;
            }
        }
        assert_eq!(gauge, Some(3.0), "sampler should report 3 pending jobs");
        q.close().await.unwrap();
    }
}
