//! The [`Bulk`] runner: submit one pipeline over N inputs, monitor progress
//! and cost, stream outputs as items complete.

use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use taquba::Queue;
use taquba::object_store::ObjectStore;
use taquba_workflow::{RunOutcome, RunSpec, TerminalHook, TerminalStatus, WorkflowRuntime};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::error::{Error, Result};
use crate::io::{NullSink, OutputRecord, OutputSink, output_to_value};
use crate::pipeline::Pipeline;
use crate::progress::{BulkReport, ProgressSnapshot, ProgressState};
use crate::runner::{ItemEnvelope, PipelineRunner};

/// Default queue name for bulk item steps.
const DEFAULT_QUEUE_NAME: &str = "bulk-items";
/// Default object-store prefix for per-item memo entries.
const DEFAULT_MEMO_PREFIX: &str = "bulk-memo";
/// Default ceiling on concurrently-processing items in one process.
const DEFAULT_MAX_CONCURRENT: usize = 200;

/// Counters plus the wake-up primitive the runner waits on. Shared between
/// the runner and the terminal hook.
struct Shared {
    state: Mutex<ProgressState>,
    notify: Notify,
}

impl Shared {
    /// Resolve once submission has set a total and every expected item has
    /// terminated. Re-checks the condition under the lock around each
    /// notification so a completion that races the wait is not missed.
    async fn wait_until_done(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().unwrap().is_done() {
                return;
            }
            notified.await;
        }
    }
}

/// Terminal hook that streams each completed item's output to the sink and
/// folds its counts and cost into the shared progress state. Generic over
/// the pipeline's output type so it can decode the per-item envelope.
struct BulkHook<O> {
    shared: Arc<Shared>,
    sink: Arc<dyn OutputSink>,
    _output: PhantomData<fn() -> O>,
}

impl<O> TerminalHook for BulkHook<O>
where
    O: Serialize + DeserializeOwned + Send + 'static,
{
    async fn on_termination(&self, outcome: &RunOutcome) {
        let status = outcome.status;

        // For a succeeded item, decode the envelope to recover the output
        // value and the per-item cost. Anything else carries no output.
        let (output, cost) = match status {
            TerminalStatus::Succeeded => match &outcome.result {
                Some(bytes) => match rmp_serde::from_slice::<ItemEnvelope<O>>(bytes) {
                    Ok(envelope) => (output_to_value(&envelope.output).ok(), Some(envelope.cost)),
                    Err(err) => {
                        warn!(
                            run_id = %outcome.run_id,
                            error = %err,
                            "failed to decode bulk item envelope",
                        );
                        (None, None)
                    }
                },
                None => (None, None),
            },
            _ => (None, None),
        };

        let record = OutputRecord {
            run_id: &outcome.run_id,
            status: status.as_str(),
            output,
            error: outcome.error.as_deref(),
        };
        if let Err(err) = self.sink.write(&record) {
            warn!(run_id = %outcome.run_id, error = %err, "failed to write bulk output record");
        }

        let done = {
            let mut state = self.shared.state.lock().unwrap();
            match status {
                TerminalStatus::Succeeded => state.succeeded += 1,
                TerminalStatus::Failed => {
                    state.failed += 1;
                    state.failed_run_ids.push(outcome.run_id.clone());
                }
                TerminalStatus::Cancelled => state.cancelled += 1,
                // `TerminalStatus` is non_exhaustive. Count an unrecognized
                // terminal state toward completion so the run still settles.
                other => {
                    warn!(run_id = %outcome.run_id, status = %other, "unknown terminal status");
                    state.cancelled += 1;
                }
            }
            if let Some(cost) = cost {
                state.cost.merge(&cost);
            }
            state.is_done()
        };
        if done {
            self.shared.notify.notify_one();
        }
    }
}

/// Closure that derives a stable run id from an input item. Stable ids make
/// a re-submission resume from cached memo state.
type KeyFn<I> = Box<dyn Fn(&I) -> String + Send + Sync>;

/// Builder for a [`Bulk`] runner. Construct via [`Bulk::builder`].
pub struct BulkBuilder<P: Pipeline> {
    queue: Arc<Queue>,
    object_store: Arc<dyn ObjectStore>,
    pipeline: P,
    sink: Option<Arc<dyn OutputSink>>,
    key_fn: Option<KeyFn<P::Input>>,
    headers: HashMap<String, String>,
    max_concurrent: usize,
    poll_interval: Duration,
    queue_name: String,
    memo_prefix: String,
    fail_threshold: Option<f64>,
}

impl<P: Pipeline> BulkBuilder<P> {
    /// Where completed item records are written. Defaults to
    /// [`NullSink`](crate::NullSink), which discards them.
    pub fn output(mut self, sink: Arc<dyn OutputSink>) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Derive each item's run id from its input. The default is positional
    /// (`item-0`, `item-1`, ...). Supply a key when items have a natural
    /// identifier so a replay re-uses the right memo state.
    pub fn key_fn(mut self, f: impl Fn(&P::Input) -> String + Send + Sync + 'static) -> Self {
        self.key_fn = Some(Box::new(f));
        self
    }

    /// Submitter metadata applied to every item, threaded through to the
    /// pipeline via [`BulkCtx::headers`](crate::BulkCtx::headers). Keys must
    /// not start with the reserved `workflow.` prefix.
    pub fn headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers = headers;
        self
    }

    /// Maximum number of items processed concurrently in this process.
    /// Defaults to 200. Bulk workloads are I/O-bound (each step awaits a
    /// remote call), so this can be set well above the CPU count.
    pub fn max_concurrent(mut self, n: usize) -> Self {
        self.max_concurrent = n;
        self
    }

    /// Maximum time a worker waits on an empty queue before re-checking.
    /// Defaults to 250ms.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Taquba queue name for item steps. Defaults to `"bulk-items"`.
    pub fn queue_name(mut self, name: impl Into<String>) -> Self {
        self.queue_name = name.into();
        self
    }

    /// Object-store prefix for per-item memo entries. Defaults to
    /// `"bulk-memo"`. Use a distinct value when several runners share a
    /// store.
    pub fn memo_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.memo_prefix = prefix.into();
        self
    }

    /// Fail the whole run if more than `percent` of items terminate failed.
    ///
    /// `percent` is on a 0 to 100 scale, e.g. pass `5.0` to fail when over
    /// 5% of items fail. `0.0` fails the run if any item fails; a value of
    /// 100.0 or more behaves the same as not setting a threshold at all.
    ///
    /// `None` (the default) records failures but always returns an `Ok`
    /// report. With a threshold set, [`Bulk::run`] returns
    /// [`Error::FailureThresholdExceeded`] when the failed share exceeds it.
    pub fn fail_threshold(mut self, percent: f64) -> Self {
        self.fail_threshold = Some(percent);
        self
    }

    /// Finalize the builder.
    pub fn build(self) -> Bulk<P> {
        let shared = Arc::new(Shared {
            state: Mutex::new(ProgressState::new()),
            notify: Notify::new(),
        });
        let sink: Arc<dyn OutputSink> = self.sink.unwrap_or_else(|| Arc::new(NullSink));
        let hook = BulkHook {
            shared: shared.clone(),
            sink: sink.clone(),
            _output: PhantomData,
        };
        let runner = PipelineRunner::new(Arc::new(self.pipeline));
        let runtime = WorkflowRuntime::builder(self.queue, self.object_store, runner, hook)
            .queue_name(self.queue_name)
            .memo_prefix(self.memo_prefix)
            .max_concurrent_steps(self.max_concurrent)
            .poll_interval(self.poll_interval)
            .build();
        Bulk {
            runtime,
            shared,
            sink,
            key_fn: self.key_fn,
            headers: self.headers,
            fail_threshold: self.fail_threshold,
        }
    }
}

/// Runs one [`Pipeline`] over many inputs in a single process: submits N
/// workflow runs, drives the worker pool, and aggregates progress, cost, and
/// streamed output.
pub struct Bulk<P: Pipeline> {
    runtime: WorkflowRuntime<PipelineRunner<P>, BulkHook<P::Output>>,
    shared: Arc<Shared>,
    sink: Arc<dyn OutputSink>,
    key_fn: Option<KeyFn<P::Input>>,
    headers: HashMap<String, String>,
    fail_threshold: Option<f64>,
}

impl<P: Pipeline> Bulk<P> {
    /// Start configuring a runner over `pipeline`, with item steps and memo
    /// entries living in `queue` / `object_store`. Optional settings are set
    /// on the returned [`BulkBuilder`].
    pub fn builder(
        queue: Arc<Queue>,
        object_store: Arc<dyn ObjectStore>,
        pipeline: P,
    ) -> BulkBuilder<P> {
        BulkBuilder {
            queue,
            object_store,
            pipeline,
            sink: None,
            key_fn: None,
            headers: HashMap::new(),
            max_concurrent: DEFAULT_MAX_CONCURRENT,
            poll_interval: Duration::from_millis(250),
            queue_name: DEFAULT_QUEUE_NAME.to_string(),
            memo_prefix: DEFAULT_MEMO_PREFIX.to_string(),
            fail_threshold: None,
        }
    }

    /// Submit every input and run to completion, returning the final
    /// [`BulkReport`].
    pub async fn run<I>(&self, inputs: I) -> Result<BulkReport>
    where
        I: IntoIterator<Item = P::Input>,
    {
        self.run_with_shutdown(inputs, std::future::pending::<()>())
            .await
    }

    /// Like [`run`](Self::run), but stops early and drains in-flight items
    /// when `shutdown` resolves (e.g. a spot-preemption signal). The
    /// returned report reflects whatever completed before the drain. Items
    /// still in flight keep their durable state and memo entries, so a later
    /// run resumes them.
    pub async fn run_with_shutdown<I, S>(&self, inputs: I, shutdown: S) -> Result<BulkReport>
    where
        I: IntoIterator<Item = P::Input>,
        S: Future<Output = ()>,
    {
        *self.shared.state.lock().unwrap() = ProgressState::new();

        let stop = CancellationToken::new();
        let worker = {
            let runtime = self.runtime.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                if let Err(err) = runtime.run(stop.cancelled_owned()).await {
                    warn!(error = %err, "bulk worker loop exited with error");
                }
            })
        };

        let expected = self.submit_all(inputs).await?;
        {
            let mut state = self.shared.state.lock().unwrap();
            state.total = expected;
        }
        // Cover the case where every item completed during submission: a
        // completion that fired while total was still 0 did not notify.
        self.shared.notify.notify_one();

        let mut shutdown = std::pin::pin!(shutdown);
        tokio::select! {
            _ = self.shared.wait_until_done() => {}
            _ = shutdown.as_mut() => {
                tracing::info!("bulk run draining on shutdown signal");
            }
        }

        stop.cancel();
        let _ = worker.await;
        self.sink.flush()?;

        let report = self.shared.state.lock().unwrap().to_report();
        if let Some(threshold) = self.fail_threshold {
            if report.total > 0 {
                let pct = report.failed as f64 / report.total as f64 * 100.0;
                if pct > threshold {
                    return Err(Error::FailureThresholdExceeded {
                        failed: report.failed,
                        total: report.total,
                        threshold,
                    });
                }
            }
        }
        Ok(report)
    }

    /// Submit every input, returning the number of newly-enqueued runs. A
    /// duplicate run id that was already active (or already recorded) is not
    /// counted, so the expected total matches the number of terminal hooks
    /// that will fire.
    ///
    /// Submissions run with bounded concurrency. Each submission blocks
    /// on a durable enqueue commit, and concurrent commits share WAL
    /// flushes, so at flush-bound latencies (for example the SlateDB
    /// default 100ms flush interval) serial submission would cap at one
    /// item per flush. Enqueue order across in-flight submissions is not
    /// defined; batch items are independent. The first submission error
    /// aborts the remaining in-flight submissions and is returned.
    async fn submit_all<I>(&self, inputs: I) -> Result<usize>
    where
        I: IntoIterator<Item = P::Input>,
    {
        const SUBMIT_CONCURRENCY: usize = 32;

        fn tally(
            joined: std::result::Result<
                taquba_workflow::Result<taquba_workflow::SubmitOutcome>,
                tokio::task::JoinError,
            >,
            expected: &mut usize,
        ) -> Result<()> {
            match joined {
                Ok(Ok(outcome)) => {
                    if outcome.newly_submitted {
                        *expected += 1;
                    }
                    Ok(())
                }
                Ok(Err(err)) => Err(err.into()),
                // The set is never aborted while joining, so a join error
                // is a panic in a submission task; propagate it.
                Err(join_err) => std::panic::resume_unwind(join_err.into_panic()),
            }
        }

        let mut set = tokio::task::JoinSet::new();
        let mut expected = 0usize;
        for (i, input) in inputs.into_iter().enumerate() {
            let run_id = match &self.key_fn {
                Some(f) => f(&input),
                None => format!("item-{i}"),
            };
            let payload = rmp_serde::to_vec_named(&input)?;
            if set.len() >= SUBMIT_CONCURRENCY {
                let joined = set.join_next().await.expect("set is non-empty");
                tally(joined, &mut expected)?;
            }
            let runtime = self.runtime.clone();
            let headers = self.headers.clone();
            set.spawn(async move {
                runtime
                    .submit(RunSpec {
                        run_id: Some(run_id),
                        input: payload,
                        headers,
                        ..RunSpec::default()
                    })
                    .await
            });
        }
        while let Some(joined) = set.join_next().await {
            tally(joined, &mut expected)?;
        }
        Ok(expected)
    }

    /// A point-in-time snapshot of the current run's progress.
    pub fn progress(&self) -> ProgressSnapshot {
        self.shared.state.lock().unwrap().snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::BulkCtx;
    use serde::Deserialize;
    use taquba::object_store::memory::InMemory;
    use taquba_workflow::StepError;

    #[derive(Serialize, Deserialize)]
    struct Item {
        n: u32,
    }

    struct Doubler;

    impl Pipeline for Doubler {
        type Input = Item;
        type Output = u32;
        type Error = StepError;

        async fn run(&self, ctx: &BulkCtx<Item>) -> std::result::Result<u32, StepError> {
            if ctx.input.n == 13 {
                return Err(StepError::permanent("unlucky"));
            }
            ctx.record_cost("calls", 1.0);
            Ok(ctx.input.n * 2)
        }
    }

    #[derive(Default)]
    struct Collect {
        records: Mutex<Vec<(String, String, Option<u32>)>>,
    }

    impl OutputSink for Collect {
        fn write(&self, record: &OutputRecord<'_>) -> Result<()> {
            let output = record
                .output
                .as_ref()
                .and_then(|v| v.as_u64())
                .map(|v| v as u32);
            self.records.lock().unwrap().push((
                record.run_id.to_string(),
                record.status.to_string(),
                output,
            ));
            Ok(())
        }
    }

    async fn fresh() -> (Arc<Queue>, Arc<dyn ObjectStore>) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let queue = Arc::new(Queue::open(store.clone(), "db").await.unwrap());
        (queue, store)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runs_all_items_and_rolls_up_cost() {
        let (queue, store) = fresh().await;
        let sink = Arc::new(Collect::default());
        let bulk = Bulk::builder(queue, store, Doubler)
            .output(sink.clone())
            .poll_interval(Duration::from_millis(10))
            .build();

        let inputs = vec![Item { n: 1 }, Item { n: 2 }, Item { n: 3 }];
        let report = tokio::time::timeout(Duration::from_secs(10), bulk.run(inputs))
            .await
            .expect("run finished in time")
            .unwrap();

        assert_eq!(report.total, 3);
        assert_eq!(report.succeeded, 3);
        assert_eq!(report.failed, 0);
        assert_eq!(report.cost.get("calls"), 3.0);

        let records = sink.records.lock().unwrap();
        assert_eq!(records.len(), 3);
        assert!(records.iter().all(|(_, status, _)| status == "succeeded"));
        let outputs: Vec<u32> = records.iter().filter_map(|(_, _, o)| *o).collect();
        assert!(outputs.contains(&2) && outputs.contains(&4) && outputs.contains(&6));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn large_batch_submits_with_bounded_concurrency() {
        let (queue, store) = fresh().await;
        let sink = Arc::new(Collect::default());
        let bulk = Bulk::builder(queue, store, Doubler)
            .output(sink.clone())
            .poll_interval(Duration::from_millis(10))
            .build();

        // More items than the submission concurrency window, so the
        // join-at-capacity path and the final drain both run. The range
        // includes n = 13, which Doubler fails permanently.
        let inputs: Vec<Item> = (0..80).map(|n| Item { n }).collect();
        let report = tokio::time::timeout(Duration::from_secs(30), bulk.run(inputs))
            .await
            .expect("run finished in time")
            .unwrap();

        assert_eq!(report.total, 80);
        assert_eq!(report.succeeded, 79);
        assert_eq!(report.failed, 1);
        assert_eq!(report.failed_run_ids, vec!["item-13".to_string()]);
        assert_eq!(sink.records.lock().unwrap().len(), 80);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn records_failed_items() {
        let (queue, store) = fresh().await;
        let bulk = Bulk::builder(queue, store, Doubler)
            .poll_interval(Duration::from_millis(10))
            .build();

        let inputs = vec![Item { n: 1 }, Item { n: 13 }, Item { n: 3 }];
        let report = tokio::time::timeout(Duration::from_secs(10), bulk.run(inputs))
            .await
            .expect("run finished in time")
            .unwrap();

        assert_eq!(report.total, 3);
        assert_eq!(report.succeeded, 2);
        assert_eq!(report.failed, 1);
        assert_eq!(report.failed_run_ids, vec!["item-1".to_string()]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fail_threshold_trips_when_exceeded() {
        let (queue, store) = fresh().await;
        let bulk = Bulk::builder(queue, store, Doubler)
            .poll_interval(Duration::from_millis(10))
            .fail_threshold(20.0)
            .build();

        // One of three failing is 33%, over the 20% threshold.
        let inputs = vec![Item { n: 1 }, Item { n: 13 }, Item { n: 3 }];
        let err = tokio::time::timeout(Duration::from_secs(10), bulk.run(inputs))
            .await
            .expect("run finished in time")
            .unwrap_err();
        assert!(matches!(
            err,
            Error::FailureThresholdExceeded {
                failed: 1,
                total: 3,
                ..
            }
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn custom_key_fn_sets_run_ids() {
        let (queue, store) = fresh().await;
        let sink = Arc::new(Collect::default());
        let bulk = Bulk::builder(queue, store, Doubler)
            .output(sink.clone())
            .key_fn(|item| format!("n-{}", item.n))
            .poll_interval(Duration::from_millis(10))
            .build();

        let report = tokio::time::timeout(
            Duration::from_secs(10),
            bulk.run(vec![Item { n: 5 }, Item { n: 7 }]),
        )
        .await
        .expect("run finished in time")
        .unwrap();
        assert_eq!(report.succeeded, 2);

        let ids: Vec<String> = sink
            .records
            .lock()
            .unwrap()
            .iter()
            .map(|(id, _, _)| id.clone())
            .collect();
        assert!(ids.contains(&"n-5".to_string()));
        assert!(ids.contains(&"n-7".to_string()));
    }
}
