//! Optional metrics emission through the [`metrics`](https://docs.rs/metrics)
//! facade.
//!
//! taquba emits counters and latency histograms for each queue state
//! transition. The macros are no-ops until the host process installs a
//! recorder (for example `metrics-exporter-prometheus`, or
//! `metrics-exporter-opentelemetry` for OTLP); taquba never installs one or
//! depends on an exporter.
//!
//! All emission requires the `metrics` cargo feature. Every function
//! here has a no-op counterpart compiled when the feature is off, so call
//! sites stay unconditional and compile to nothing.
//!
//! Metric names and labels form a stable interface that dashboards depend
//! on, so treat a rename as a breaking change:
//! - `taquba_jobs_enqueued_total{queue}`: jobs written to pending/scheduled.
//! - `taquba_jobs_claimed_total{queue}`: jobs handed to workers.
//! - `taquba_jobs_completed_total{queue}`: jobs acked successfully.
//! - `taquba_jobs_nacked_total{queue}`: jobs requeued or scheduled after a nack.
//! - `taquba_jobs_dead_lettered_total{queue}`: jobs moved to the dead set.
//! - `taquba_jobs_reaped_total{queue}`: expired claims requeued by the reaper.
//! - `taquba_enqueue_duration_seconds{queue}` / `taquba_claim_duration_seconds{queue}`
//!   / `taquba_ack_duration_seconds{queue}`: per-operation latency histograms.
//! - `taquba_pending_jobs{queue}` / `taquba_claimed_jobs{queue}`: current
//!   queue depth gauges, sampled by the background metrics sampler.
//! - `taquba_oldest_pending_age_seconds{queue}`: age of the pending job at the
//!   front of the claim order, also sampled.

#[cfg(feature = "metrics")]
mod imp {
    use std::sync::Arc;
    use std::time::Instant;

    use slatedb_common::metrics::{
        CounterFn, GaugeFn, HistogramFn, MetricsRecorder, UpDownCounterFn,
    };

    /// A start instant captured when metrics are enabled, threaded into the
    /// emitting call so it can record the operation's latency.
    pub(crate) type Timer = Option<Instant>;

    /// Capture a start instant for a latency histogram.
    pub(crate) fn start() -> Timer {
        Some(Instant::now())
    }

    /// Register metric descriptions. Idempotent; called once per queue open.
    pub(crate) fn describe() {
        metrics::describe_counter!(
            "taquba_jobs_enqueued_total",
            "Jobs written to pending or scheduled"
        );
        metrics::describe_counter!("taquba_jobs_claimed_total", "Jobs handed to workers");
        metrics::describe_counter!("taquba_jobs_completed_total", "Jobs acked successfully");
        metrics::describe_counter!(
            "taquba_jobs_nacked_total",
            "Jobs requeued or scheduled after a nack"
        );
        metrics::describe_counter!(
            "taquba_jobs_dead_lettered_total",
            "Jobs moved to the dead-letter set"
        );
        metrics::describe_counter!(
            "taquba_jobs_reaped_total",
            "Expired claims requeued by the reaper"
        );
        metrics::describe_histogram!(
            "taquba_enqueue_duration_seconds",
            "Time for an enqueue to commit durably"
        );
        metrics::describe_histogram!(
            "taquba_claim_duration_seconds",
            "Time for a claim batch to commit"
        );
        metrics::describe_histogram!(
            "taquba_ack_duration_seconds",
            "Time for an ack to commit durably"
        );
        metrics::describe_gauge!(
            "taquba_pending_jobs",
            "Jobs currently waiting to be claimed"
        );
        metrics::describe_gauge!("taquba_claimed_jobs", "Jobs currently held under a lease");
        metrics::describe_gauge!(
            "taquba_oldest_pending_age_seconds",
            "Age of the pending job at the front of the claim order"
        );
    }

    pub(crate) fn enqueued(queue: &str, n: u64, t: Timer) {
        metrics::counter!("taquba_jobs_enqueued_total", "queue" => queue.to_owned()).increment(n);
        record(t, "taquba_enqueue_duration_seconds", queue);
    }

    pub(crate) fn claimed(queue: &str, n: u64, t: Timer) {
        metrics::counter!("taquba_jobs_claimed_total", "queue" => queue.to_owned()).increment(n);
        record(t, "taquba_claim_duration_seconds", queue);
    }

    pub(crate) fn completed(queue: &str, t: Timer) {
        metrics::counter!("taquba_jobs_completed_total", "queue" => queue.to_owned()).increment(1);
        record(t, "taquba_ack_duration_seconds", queue);
    }

    pub(crate) fn nacked(queue: &str) {
        metrics::counter!("taquba_jobs_nacked_total", "queue" => queue.to_owned()).increment(1);
    }

    pub(crate) fn dead_lettered(queue: &str) {
        metrics::counter!("taquba_jobs_dead_lettered_total", "queue" => queue.to_owned())
            .increment(1);
    }

    pub(crate) fn reaped(queue: &str, n: u64) {
        metrics::counter!("taquba_jobs_reaped_total", "queue" => queue.to_owned()).increment(n);
    }

    pub(crate) fn set_depth(queue: &str, pending: i64, claimed: i64) {
        metrics::gauge!("taquba_pending_jobs", "queue" => queue.to_owned()).set(pending as f64);
        metrics::gauge!("taquba_claimed_jobs", "queue" => queue.to_owned()).set(claimed as f64);
    }

    pub(crate) fn set_oldest_pending_age_seconds(queue: &str, secs: f64) {
        metrics::gauge!("taquba_oldest_pending_age_seconds", "queue" => queue.to_owned()).set(secs);
    }

    fn record(t: Timer, name: &'static str, queue: &str) {
        if let Some(start) = t {
            metrics::histogram!(name, "queue" => queue.to_owned())
                .record(start.elapsed().as_secs_f64());
        }
    }

    /// A SlateDB [`MetricsRecorder`] that forwards SlateDB's storage metrics
    /// (write/flush/compaction/cache, dot-separated names such as
    /// `slatedb.db.write_ops`) into the `metrics` facade, so they share the
    /// host's recorder with taquba's queue metrics. Installed on the
    /// `DbBuilder` when the `metrics` feature is on.
    pub(crate) fn slatedb_recorder() -> Arc<dyn MetricsRecorder> {
        Arc::new(SlateDbRecorder)
    }

    struct SlateDbRecorder;

    impl MetricsRecorder for SlateDbRecorder {
        fn register_counter(
            &self,
            name: &str,
            description: &str,
            labels: &[(&str, &str)],
        ) -> Arc<dyn CounterFn> {
            let name = name.to_string();
            metrics::describe_counter!(name.clone(), description.to_string());
            Arc::new(CounterHandle(metrics::counter!(name, to_labels(labels))))
        }

        fn register_gauge(
            &self,
            name: &str,
            description: &str,
            labels: &[(&str, &str)],
        ) -> Arc<dyn GaugeFn> {
            let name = name.to_string();
            metrics::describe_gauge!(name.clone(), description.to_string());
            Arc::new(GaugeHandle(metrics::gauge!(name, to_labels(labels))))
        }

        fn register_up_down_counter(
            &self,
            name: &str,
            description: &str,
            labels: &[(&str, &str)],
        ) -> Arc<dyn UpDownCounterFn> {
            // `metrics` has no up-down counter; map it onto a gauge.
            let name = name.to_string();
            metrics::describe_gauge!(name.clone(), description.to_string());
            Arc::new(UpDownCounterHandle(metrics::gauge!(
                name,
                to_labels(labels)
            )))
        }

        fn register_histogram(
            &self,
            name: &str,
            description: &str,
            labels: &[(&str, &str)],
            _boundaries: &[f64],
        ) -> Arc<dyn HistogramFn> {
            // Bucket boundaries are configured on the exporter, not here.
            let name = name.to_string();
            metrics::describe_histogram!(name.clone(), description.to_string());
            Arc::new(HistogramHandle(metrics::histogram!(
                name,
                to_labels(labels)
            )))
        }
    }

    fn to_labels(labels: &[(&str, &str)]) -> Vec<metrics::Label> {
        labels
            .iter()
            .map(|(k, v)| metrics::Label::new(k.to_string(), v.to_string()))
            .collect()
    }

    struct CounterHandle(metrics::Counter);
    impl CounterFn for CounterHandle {
        fn increment(&self, value: u64) {
            self.0.increment(value);
        }
    }

    struct GaugeHandle(metrics::Gauge);
    impl GaugeFn for GaugeHandle {
        fn set(&self, value: i64) {
            self.0.set(value as f64);
        }
    }

    struct UpDownCounterHandle(metrics::Gauge);
    impl UpDownCounterFn for UpDownCounterHandle {
        fn increment(&self, value: i64) {
            self.0.increment(value as f64);
        }
    }

    struct HistogramHandle(metrics::Histogram);
    impl HistogramFn for HistogramHandle {
        fn record(&self, value: f64) {
            self.0.record(value);
        }
    }
}

#[cfg(not(feature = "metrics"))]
mod imp {
    /// Zero-sized stand-in so `let timer = start();` at call sites is not a
    /// `let`-unit binding when the feature is off.
    #[derive(Clone, Copy)]
    pub(crate) struct Timer;

    #[inline]
    pub(crate) fn start() -> Timer {
        Timer
    }
    #[inline]
    pub(crate) fn describe() {}
    #[inline]
    pub(crate) fn enqueued(_queue: &str, _n: u64, _t: Timer) {}
    #[inline]
    pub(crate) fn claimed(_queue: &str, _n: u64, _t: Timer) {}
    #[inline]
    pub(crate) fn completed(_queue: &str, _t: Timer) {}
    #[inline]
    pub(crate) fn nacked(_queue: &str) {}
    #[inline]
    pub(crate) fn dead_lettered(_queue: &str) {}
    #[inline]
    pub(crate) fn reaped(_queue: &str, _n: u64) {}
}

pub(crate) use imp::*;

#[cfg(all(test, feature = "metrics"))]
mod tests {
    use metrics_util::debugging::DebuggingRecorder;

    #[test]
    fn emits_the_documented_metric_contract() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            super::describe();
            super::enqueued("q", 2, super::start());
            super::claimed("q", 2, super::start());
            super::completed("q", super::start());
            super::nacked("q");
            super::dead_lettered("q");
            super::reaped("q", 3);
        });

        let emitted: Vec<String> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(composite, _unit, _desc, _value)| composite.key().name().to_string())
            .collect();

        for expected in [
            "taquba_jobs_enqueued_total",
            "taquba_jobs_claimed_total",
            "taquba_jobs_completed_total",
            "taquba_jobs_nacked_total",
            "taquba_jobs_dead_lettered_total",
            "taquba_jobs_reaped_total",
            "taquba_enqueue_duration_seconds",
            "taquba_claim_duration_seconds",
            "taquba_ack_duration_seconds",
        ] {
            assert!(
                emitted.iter().any(|name| name == expected),
                "expected metric {expected} was not emitted; got {emitted:?}"
            );
        }
    }

    #[test]
    fn slatedb_recorder_forwards_to_the_metrics_facade() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let r = super::slatedb_recorder();
            r.register_counter("slatedb.db.write_ops", "writes", &[("kind", "wal")])
                .increment(2);
            r.register_gauge("slatedb.db.l0_sst_count", "l0", &[])
                .set(3);
            r.register_up_down_counter("slatedb.db.inflight", "inflight", &[])
                .increment(1);
            r.register_histogram("slatedb.db.flush_seconds", "flush", &[], &[])
                .record(0.1);
        });

        let emitted: Vec<String> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(composite, _unit, _desc, _value)| composite.key().name().to_string())
            .collect();

        for expected in [
            "slatedb.db.write_ops",
            "slatedb.db.l0_sst_count",
            "slatedb.db.inflight",
            "slatedb.db.flush_seconds",
        ] {
            assert!(
                emitted.iter().any(|name| name == expected),
                "expected forwarded metric {expected}; got {emitted:?}"
            );
        }
    }
}
