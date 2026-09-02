//! The [`Bulk`] runner: submit one pipeline over N inputs as a batch,
//! monitor progress and cost, stream outputs as items complete.

use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::{
    MemoStore, RunOutcome, RunSpec, StepError, TerminalEffects, TerminalHook, TerminalStatus,
    WorkflowRuntime,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use taquba::Queue;
use taquba::object_store::ObjectStore;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::bulk::cost::CostReport;
use crate::bulk::error::{Error, Result};
use crate::bulk::io::{NullSink, OutputRecord, OutputSink};
use crate::bulk::manifest::{Manifest, ManifestItem, ManifestStore};
use crate::bulk::pipeline::Pipeline;
use crate::bulk::progress::{BatchStatus, BulkReport, ItemMarker, ProgressSnapshot, ProgressState};
use crate::bulk::runner::{ItemEnvelope, PipelineRunner};
use crate::keys::{bulk_item_kv_key, bulk_items_kv_prefix};
use crate::outcome::{StoredOutcome, read_outcome};

/// Default queue name for bulk item steps.
const DEFAULT_QUEUE_NAME: &str = "bulk-items";
/// Default object-store prefix for per-item memo entries.
const DEFAULT_MEMO_PREFIX: &str = "bulk-memo";
/// Default ceiling on concurrently-processing items in one process.
const DEFAULT_MAX_CONCURRENT: usize = 200;

/// Header holding the batch id of an item's run.
pub(crate) const HEADER_BATCH: &str = "bulk.batch";
/// Header holding the item key of an item's run.
pub(crate) const HEADER_KEY: &str = "bulk.key";
/// Prefix of the headers the runner reserves.
const RESERVED_HEADER_PREFIX: &str = "bulk.";

/// The workflow run id of an item: the hex SHA-256 digest of
/// `{batch_id}/{key}`, so batches never share run state and any key string
/// maps onto the character set a run id accepts.
pub(crate) fn item_run_id(batch_id: &str, key: &str) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write;
    let mut hasher = Sha256::new();
    hasher.update(batch_id.as_bytes());
    hasher.update(b"/");
    hasher.update(key.as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

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

    /// Write an item's record to the sink and fold it into the counters.
    /// An item already counted under `run_id` is neither written nor
    /// counted again.
    fn record(&self, sink: &dyn OutputSink, item: TerminalItem<'_>) {
        if self.state.lock().unwrap().counted.contains(item.run_id) {
            return;
        }
        let record = OutputRecord {
            key: item.key,
            status: item.status.as_str(),
            output: item.output,
            error: item.error,
        };
        if let Err(err) = sink.write(&record) {
            warn!(key = %item.key, error = %err, "failed to write bulk output record");
        }
        let done = {
            let mut state = self.state.lock().unwrap();
            if !state.counted.insert(item.run_id.to_string()) {
                return;
            }
            match item.status {
                TerminalStatus::Succeeded => state.succeeded += 1,
                TerminalStatus::Failed => {
                    state.failed += 1;
                    state.failed_keys.push(item.key.to_string());
                }
                TerminalStatus::Cancelled => state.cancelled += 1,
            }
            if let Some(cost) = item.cost {
                state.cost.merge(&cost);
            }
            state.is_done()
        };
        if done {
            self.notify.notify_one();
        }
    }
}

/// One terminated item, as the hook or the short-circuit path reports it.
struct TerminalItem<'a> {
    key: &'a str,
    run_id: &'a str,
    status: TerminalStatus,
    output: Option<Value>,
    error: Option<&'a str>,
    cost: Option<CostReport>,
}

/// Decode a succeeded item's envelope into its JSON output and cost.
fn decode_envelope<O>(key: &str, bytes: &[u8]) -> (Option<Value>, Option<CostReport>)
where
    O: Serialize + DeserializeOwned,
{
    match rmp_serde::from_slice::<ItemEnvelope<O>>(bytes) {
        Ok(envelope) => (
            serde_json::to_value(&envelope.output).ok(),
            Some(envelope.cost),
        ),
        Err(err) => {
            warn!(key = %key, error = %err, "failed to decode bulk item envelope");
            (None, None)
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
    async fn on_termination(
        &self,
        outcome: &RunOutcome,
        effects: &TerminalEffects,
    ) -> std::result::Result<(), StepError> {
        let key = outcome
            .headers
            .get(HEADER_KEY)
            .map_or(outcome.run_id.as_str(), String::as_str);
        let (output, cost) = match (outcome.status, &outcome.result) {
            (TerminalStatus::Succeeded, Some(bytes)) => decode_envelope::<O>(key, bytes),
            _ => (None, None),
        };
        // The marker commits with this notification's acknowledgement.
        if let Some(batch_id) = outcome.headers.get(HEADER_BATCH) {
            let marker = ItemMarker {
                status: outcome.status.as_str().to_string(),
                error: outcome.error.clone(),
                cost: cost.clone().unwrap_or_default(),
            };
            let bytes = rmp_serde::to_vec_named(&marker)
                .map_err(|err| StepError::permanent(err.to_string()))?;
            effects
                .put_reserved(bulk_item_kv_key(batch_id, key), bytes)
                .map_err(|err| StepError::permanent(err.to_string()))?;
        }
        self.shared.record(
            self.sink.as_ref(),
            TerminalItem {
                key,
                run_id: &outcome.run_id,
                status: outcome.status,
                output,
                error: outcome.error.as_deref(),
                cost,
            },
        );
        Ok(())
    }
}

/// Closure that derives an item's key from its input.
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
    /// [`NullSink`](crate::bulk::NullSink), which discards them.
    pub fn output(mut self, sink: Arc<dyn OutputSink>) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Derive each item's key from its input. The default is positional
    /// (`item-0`, `item-1`, ...). Supply a key when items have a natural
    /// identifier: a later run of the same batch matches items by key, so
    /// it skips the ones that succeeded and runs the failed ones again.
    /// Any string is accepted.
    pub fn key_fn(mut self, f: impl Fn(&P::Input) -> String + Send + Sync + 'static) -> Self {
        self.key_fn = Some(Box::new(f));
        self
    }

    /// Submitter metadata applied to every item, threaded through to the
    /// pipeline via [`BulkCtx::headers`](crate::bulk::BulkCtx::headers). Keys
    /// must not start with the reserved `workflow.` or `bulk.` prefixes.
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

    /// Object-store prefix for per-item memo entries and outcome records.
    /// Defaults to `"bulk-memo"`. Use a distinct value when several runners
    /// share a store.
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
    /// report. With a threshold set, [`Batch::run`] returns
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
        let memo_store = MemoStore::new(self.object_store.clone(), self.memo_prefix.clone());
        let manifests = ManifestStore::new(self.object_store.clone(), self.memo_prefix.clone());
        let runner = PipelineRunner::new(self.pipeline);
        let queue = self.queue.clone();
        let runtime = WorkflowRuntime::builder(self.queue, self.object_store, runner, hook)
            .queue_name(self.queue_name)
            .memo_prefix(self.memo_prefix)
            .max_concurrent_steps(self.max_concurrent)
            .poll_interval(self.poll_interval)
            .build();
        Bulk {
            runtime,
            queue,
            memo_store,
            manifests,
            shared,
            sink,
            key_fn: self.key_fn,
            headers: self.headers,
            fail_threshold: self.fail_threshold,
        }
    }
}

/// Runs one [`Pipeline`] over many inputs in a single process: submits one
/// workflow run per item, drives the worker pool, and aggregates progress,
/// cost, and streamed output per batch.
pub struct Bulk<P: Pipeline> {
    runtime: WorkflowRuntime<PipelineRunner<P>, BulkHook<P::Output>>,
    queue: Arc<Queue>,
    memo_store: MemoStore,
    manifests: ManifestStore,
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

    /// A handle on the batch named `id`, which must be 1 to 128 bytes of
    /// `[A-Za-z0-9_-]`. A batch groups the items of one submission: a
    /// second run of the same batch skips the items that succeeded and runs
    /// the failed ones again.
    pub fn batch(&self, id: impl Into<String>) -> Result<Batch<'_, P>> {
        let id = id.into();
        crate::keys::validate_run_id(&id).map_err(|_| Error::InvalidBatchId(id.clone()))?;
        Ok(Batch { bulk: self, id })
    }

    /// A handle on a new batch with a generated id.
    pub fn new_batch(&self) -> Batch<'_, P> {
        Batch {
            bulk: self,
            id: ulid::Ulid::new().to_string(),
        }
    }

    /// Run every input as a new batch with a generated id and return the
    /// final [`BulkReport`].
    pub async fn run<I>(&self, inputs: I) -> Result<BulkReport>
    where
        I: IntoIterator<Item = P::Input>,
    {
        self.new_batch().run(inputs).await
    }

    /// Like [`run`](Self::run), but stops early and drains in-flight items
    /// when `shutdown` resolves; see [`Batch::run_with_shutdown`].
    pub async fn run_with_shutdown<I, S>(&self, inputs: I, shutdown: S) -> Result<BulkReport>
    where
        I: IntoIterator<Item = P::Input>,
        S: Future<Output = ()>,
    {
        self.new_batch().run_with_shutdown(inputs, shutdown).await
    }

    /// A point-in-time snapshot of the current run's progress.
    pub fn progress(&self) -> ProgressSnapshot {
        self.shared.state.lock().unwrap().snapshot()
    }
}

/// A handle on one batch of a [`Bulk`] runner. Obtained from
/// [`Bulk::batch`] or [`Bulk::new_batch`].
pub struct Batch<'a, P: Pipeline> {
    bulk: &'a Bulk<P>,
    id: String,
}

impl<P: Pipeline> Batch<'_, P> {
    /// The batch id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Submit every input and run to completion, returning the final
    /// [`BulkReport`]. The batch's manifest is written before any item is
    /// submitted; a run of an existing batch with a different item set is
    /// rejected with [`Error::BatchMismatch`]. An item whose outcome record
    /// from an earlier run of this batch is a success is counted and
    /// written to the sink from that record without running again; an item
    /// whose record is a failure runs again.
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
    /// still in flight keep their durable state, so a later run of the same
    /// batch resumes them.
    ///
    /// A submission error stops the worker and returns after it has
    /// exited. Items submitted before the error keep their durable state
    /// and are resumed by a later run of the same batch.
    pub async fn run_with_shutdown<I, S>(&self, inputs: I, shutdown: S) -> Result<BulkReport>
    where
        I: IntoIterator<Item = P::Input>,
        S: Future<Output = ()>,
    {
        self.check_headers()?;
        let manifest = Manifest {
            batch_id: self.id.clone(),
            items: self.materialize(inputs)?,
        };
        match self.bulk.manifests.read(&self.id).await? {
            Some(existing) if existing.items != manifest.items => {
                return Err(Error::BatchMismatch(self.id.clone()));
            }
            Some(_) => {}
            None => self.bulk.manifests.write(&manifest).await?,
        }
        self.drive(manifest.items, shutdown).await
    }

    /// Run the batch from its manifest, without the inputs: completed
    /// items are answered from their outcome records, items still queued
    /// continue, and the rest run. Returns [`Error::BatchNotFound`] when
    /// no manifest exists.
    pub async fn resume(&self) -> Result<BulkReport> {
        self.resume_with_shutdown(std::future::pending::<()>())
            .await
    }

    /// Like [`resume`](Self::resume), but stops early and drains in-flight
    /// items when `shutdown` resolves.
    pub async fn resume_with_shutdown<S>(&self, shutdown: S) -> Result<BulkReport>
    where
        S: Future<Output = ()>,
    {
        self.check_headers()?;
        let manifest = self
            .bulk
            .manifests
            .read(&self.id)
            .await?
            .ok_or_else(|| Error::BatchNotFound(self.id.clone()))?;
        self.drive(manifest.items, shutdown).await
    }

    /// The durable state of the batch: its item count from the manifest
    /// and, per item, the last outcome recorded by a terminal notification.
    /// An item that ran again after a failure is reported by its latest
    /// outcome. Returns [`Error::BatchNotFound`] when no manifest exists.
    pub async fn status(&self) -> Result<BatchStatus> {
        let bulk = self.bulk;
        let manifest = bulk
            .manifests
            .read(&self.id)
            .await?
            .ok_or_else(|| Error::BatchNotFound(self.id.clone()))?;
        let prefix = bulk_items_kv_prefix(&self.id);
        let mut status = BatchStatus {
            batch_id: self.id.clone(),
            total: manifest.items.len(),
            succeeded: 0,
            failed: 0,
            cancelled: 0,
            cost: CostReport::new(),
            failed_keys: Vec::new(),
        };
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let page = bulk
                .queue
                .kv_scan(&prefix, cursor.as_deref(), 1000)
                .await
                .map_err(crate::Error::from)?;
            for (kv_key, value) in &page.entries {
                let key = String::from_utf8_lossy(&kv_key[prefix.len()..]).into_owned();
                let marker: ItemMarker = match rmp_serde::from_slice(value) {
                    Ok(marker) => marker,
                    Err(err) => {
                        warn!(batch_id = %self.id, key = %key, error = %err, "item marker failed to decode");
                        continue;
                    }
                };
                match marker.status() {
                    Some(TerminalStatus::Succeeded) => status.succeeded += 1,
                    Some(TerminalStatus::Failed) => {
                        status.failed += 1;
                        status.failed_keys.push(key);
                    }
                    Some(TerminalStatus::Cancelled) => status.cancelled += 1,
                    None => continue,
                }
                status.cost.merge(&marker.cost);
            }
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        Ok(status)
    }

    fn check_headers(&self) -> Result<()> {
        for key in self.bulk.headers.keys() {
            if key.starts_with(RESERVED_HEADER_PREFIX) {
                return Err(Error::ReservedHeader(key.clone()));
            }
        }
        Ok(())
    }

    /// Derive each input's key and serialize it. Two inputs with one key
    /// are rejected.
    fn materialize<I>(&self, inputs: I) -> Result<Vec<ManifestItem>>
    where
        I: IntoIterator<Item = P::Input>,
    {
        let mut seen = std::collections::HashSet::new();
        let mut items = Vec::new();
        for (i, input) in inputs.into_iter().enumerate() {
            let key = match &self.bulk.key_fn {
                Some(f) => f(&input),
                None => format!("item-{i}"),
            };
            if !seen.insert(key.clone()) {
                return Err(Error::DuplicateKey(key));
            }
            items.push(ManifestItem {
                key,
                input: rmp_serde::to_vec_named(&input)?,
            });
        }
        Ok(items)
    }

    /// Submit every item, drive the worker until every item has
    /// terminated or `shutdown` resolves, and build the report.
    async fn drive<S>(&self, items: Vec<ManifestItem>, shutdown: S) -> Result<BulkReport>
    where
        S: Future<Output = ()>,
    {
        let bulk = self.bulk;
        *bulk.shared.state.lock().unwrap() = ProgressState::new();

        let stop = CancellationToken::new();
        let worker = {
            let runtime = bulk.runtime.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                if let Err(err) = runtime.run(stop.cancelled_owned()).await {
                    warn!(error = %err, "bulk worker loop exited with error");
                }
            })
        };

        let expected = items.len();
        let submitted = self.submit_all(items).await;
        match submitted {
            Ok(()) => {}
            Err(err) => {
                // Stop the worker before reporting the error so that no
                // task outlives this call.
                stop.cancel();
                let _ = worker.await;
                if let Err(flush_err) = bulk.sink.flush() {
                    warn!(error = %flush_err, "sink flush failed after a submission error");
                }
                return Err(err);
            }
        }
        {
            let mut state = bulk.shared.state.lock().unwrap();
            state.total = expected;
        }
        // Cover the case where every item completed during submission: a
        // completion that fired while total was still 0 did not notify.
        bulk.shared.notify.notify_one();

        let mut shutdown = std::pin::pin!(shutdown);
        tokio::select! {
            _ = bulk.shared.wait_until_done() => {}
            _ = shutdown.as_mut() => {
                tracing::info!(batch_id = %self.id, "bulk run draining on shutdown signal");
            }
        }

        stop.cancel();
        let _ = worker.await;
        bulk.sink.flush()?;

        let report = bulk.shared.state.lock().unwrap().to_report(&self.id);
        if let Some(threshold) = bulk.fail_threshold
            && report.total > 0
        {
            let pct = report.failed as f64 / report.total as f64 * 100.0;
            if pct > threshold {
                return Err(Error::FailureThresholdExceeded {
                    failed: report.failed,
                    total: report.total,
                    threshold,
                });
            }
        }
        Ok(report)
    }

    /// Submit every item. Every item terminates in this process: a
    /// newly enqueued run and a run still queued from an earlier run of
    /// the batch both fire the terminal hook here, and an item answered
    /// from a success record is counted at submission.
    ///
    /// Submissions run with bounded concurrency. Each submission blocks
    /// on a durable enqueue commit, and concurrent commits share WAL
    /// flushes, so at flush-bound latencies (for example the SlateDB
    /// default 100ms flush interval) serial submission would cap at one
    /// item per flush. Enqueue order across in-flight submissions is not
    /// defined; batch items are independent. The first submission error
    /// aborts the remaining in-flight submissions and is returned.
    async fn submit_all(&self, items: Vec<ManifestItem>) -> Result<()> {
        const SUBMIT_CONCURRENCY: usize = 32;

        fn tally(joined: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
            match joined {
                Ok(result) => result,
                // The set is never aborted while joining, so a join error
                // is a panic in a submission task; propagate it.
                Err(join_err) => std::panic::resume_unwind(join_err.into_panic()),
            }
        }

        let bulk = self.bulk;
        let mut set = tokio::task::JoinSet::new();
        for ManifestItem { key, input } in items {
            let run_id = item_run_id(&self.id, &key);
            let payload = input;
            if set.len() >= SUBMIT_CONCURRENCY {
                let joined = set.join_next().await.expect("set is non-empty");
                tally(joined)?;
            }
            let runtime = bulk.runtime.clone();
            let run_memo = bulk.memo_store.new_run_memo(&run_id);
            let shared = bulk.shared.clone();
            let sink = bulk.sink.clone();
            let mut headers = bulk.headers.clone();
            headers.insert(HEADER_BATCH.to_string(), self.id.clone());
            headers.insert(HEADER_KEY.to_string(), key.clone());
            set.spawn(async move {
                // A success record from an earlier run of the batch answers
                // the item; a failure record is ignored so the item runs
                // again.
                if let Some(record) = read_outcome(&run_memo).await?
                    && let StoredOutcome::Success { output } = record.outcome
                {
                    let (output, cost) = decode_envelope::<P::Output>(&key, &output);
                    shared.record(
                        sink.as_ref(),
                        TerminalItem {
                            key: &key,
                            run_id: &run_id,
                            status: TerminalStatus::Succeeded,
                            output,
                            error: None,
                            cost,
                        },
                    );
                    return Ok(());
                }
                runtime
                    .submit(RunSpec {
                        run_id: Some(run_id),
                        input: payload,
                        headers,
                        ..RunSpec::default()
                    })
                    .await?;
                Ok(())
            });
        }
        while let Some(joined) = set.join_next().await {
            tally(joined)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StepError;
    use crate::bulk::pipeline::BulkCtx;
    use serde::Deserialize;
    use taquba::object_store::memory::InMemory;

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
                record.key.to_string(),
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
        assert_eq!(report.failed_keys, vec!["item-13".to_string()]);
        assert_eq!(sink.records.lock().unwrap().len(), 80);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pipelines_staged_write_is_visible_after_the_item_acks() {
        struct EffectsPipeline;
        impl Pipeline for EffectsPipeline {
            type Input = Item;
            type Output = u32;
            type Error = StepError;

            async fn run(&self, ctx: &BulkCtx<Item>) -> std::result::Result<u32, StepError> {
                let seed = ctx.kv_get(b"app/seed").await?.unwrap_or_default();
                ctx.effects()
                    .put(format!("app/items/{}", ctx.key), seed)
                    .map_err(StepError::from)?;
                Ok(ctx.input.n)
            }
        }

        let (queue, store) = fresh().await;
        queue.kv_put(b"app/seed", b"seeded").await.unwrap();
        let bulk = Bulk::builder(queue.clone(), store, EffectsPipeline)
            .poll_interval(Duration::from_millis(10))
            .build();

        let report = tokio::time::timeout(Duration::from_secs(10), bulk.run(vec![Item { n: 1 }]))
            .await
            .expect("run finished in time")
            .unwrap();
        assert_eq!(report.succeeded, 1);

        assert_eq!(
            queue.kv_get(b"app/items/item-0").await.unwrap().as_deref(),
            Some(b"seeded".as_slice()),
        );
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
        assert_eq!(report.failed_keys, vec!["item-1".to_string()]);
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
    async fn custom_key_fn_sets_item_keys() {
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rejected_submission_stops_the_worker_before_returning() {
        let (queue, store) = fresh().await;
        let bulk = Bulk::builder(queue, store, Doubler)
            .headers(HashMap::from([("workflow.x".to_string(), "y".to_string())]))
            .poll_interval(Duration::from_millis(10))
            .build();

        let metrics = tokio::runtime::Handle::current().metrics();
        let alive_before = metrics.num_alive_tasks();
        let err = tokio::time::timeout(
            Duration::from_secs(10),
            bulk.run(vec![Item { n: 1 }, Item { n: 2 }, Item { n: 3 }]),
        )
        .await
        .expect("run finished in time")
        .expect_err("a reserved header fails the submission");
        assert!(
            matches!(
                err,
                Error::Workflow(crate::Error::ReservedHeaderInSubmit(_))
            ),
            "unexpected error: {err}",
        );

        let settled = tokio::time::timeout(Duration::from_secs(5), async {
            while metrics.num_alive_tasks() > alive_before {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(settled.is_ok(), "the worker task outlived the failed run");
    }

    struct Counting {
        runs: Arc<std::sync::atomic::AtomicU32>,
    }

    impl Pipeline for Counting {
        type Input = Item;
        type Output = u32;
        type Error = StepError;

        async fn run(&self, ctx: &BulkCtx<Item>) -> std::result::Result<u32, StepError> {
            self.runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if ctx.input.n == 13 {
                return Err(StepError::permanent("unlucky"));
            }
            Ok(ctx.input.n * 2)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_second_run_of_a_batch_skips_succeeded_items_and_reruns_failed_ones() {
        let (queue, store) = fresh().await;
        let runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let sink = Arc::new(Collect::default());
        let bulk = Bulk::builder(queue, store, Counting { runs: runs.clone() })
            .output(sink.clone())
            .poll_interval(Duration::from_millis(10))
            .build();
        let inputs = || vec![Item { n: 1 }, Item { n: 13 }, Item { n: 3 }];

        let first = tokio::time::timeout(
            Duration::from_secs(10),
            bulk.batch("nightly").unwrap().run(inputs()),
        )
        .await
        .expect("run finished in time")
        .unwrap();
        assert_eq!((first.succeeded, first.failed), (2, 1));
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 3);

        let second = tokio::time::timeout(
            Duration::from_secs(10),
            bulk.batch("nightly").unwrap().run(inputs()),
        )
        .await
        .expect("run finished in time")
        .unwrap();

        assert_eq!(second.batch_id, "nightly");
        assert_eq!((second.total, second.succeeded, second.failed), (3, 2, 1));
        assert_eq!(second.failed_keys, vec!["item-1".to_string()]);
        // Only the failed item ran again.
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 4);
        let records = sink.records.lock().unwrap();
        assert_eq!(records.len(), 6);
        assert!(records.iter().any(|(key, status, output)| {
            key == "item-0" && status == "succeeded" && *output == Some(2)
        }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batches_with_the_same_keys_do_not_share_state() {
        let (queue, store) = fresh().await;
        let runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let bulk = Bulk::builder(queue, store, Counting { runs: runs.clone() })
            .poll_interval(Duration::from_millis(10))
            .build();

        for id in ["a", "b"] {
            let report = tokio::time::timeout(
                Duration::from_secs(10),
                bulk.batch(id)
                    .unwrap()
                    .run(vec![Item { n: 1 }, Item { n: 2 }]),
            )
            .await
            .expect("run finished in time")
            .unwrap();
            assert_eq!(report.succeeded, 2);
        }
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resume_runs_a_batch_from_its_manifest() {
        let (queue, store) = fresh().await;
        let runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let sink = Arc::new(Collect::default());
        let bulk = Bulk::builder(queue, store, Counting { runs: runs.clone() })
            .output(sink.clone())
            .poll_interval(Duration::from_millis(10))
            .build();

        let first = tokio::time::timeout(
            Duration::from_secs(10),
            bulk.batch("b")
                .unwrap()
                .run(vec![Item { n: 1 }, Item { n: 13 }]),
        )
        .await
        .expect("run finished in time")
        .unwrap();
        assert_eq!((first.succeeded, first.failed), (1, 1));

        let resumed =
            tokio::time::timeout(Duration::from_secs(10), bulk.batch("b").unwrap().resume())
                .await
                .expect("resume finished in time")
                .unwrap();

        assert_eq!(
            (resumed.total, resumed.succeeded, resumed.failed),
            (2, 1, 1)
        );
        // The succeeded item was answered from its record; the failed one ran again.
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 3);
        assert_eq!(sink.records.lock().unwrap().len(), 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_with_a_different_item_set_is_rejected() {
        let (queue, store) = fresh().await;
        let bulk = Bulk::builder(queue, store, Doubler)
            .poll_interval(Duration::from_millis(10))
            .build();
        tokio::time::timeout(
            Duration::from_secs(10),
            bulk.batch("b")
                .unwrap()
                .run(vec![Item { n: 1 }, Item { n: 2 }]),
        )
        .await
        .expect("run finished in time")
        .unwrap();

        let err = tokio::time::timeout(
            Duration::from_secs(10),
            bulk.batch("b")
                .unwrap()
                .run(vec![Item { n: 1 }, Item { n: 3 }]),
        )
        .await
        .expect("run finished in time")
        .unwrap_err();
        assert!(matches!(err, Error::BatchMismatch(id) if id == "b"));
    }

    #[tokio::test]
    async fn resume_of_an_unknown_batch_and_duplicate_keys_are_rejected() {
        let (queue, store) = fresh().await;
        let bulk = Bulk::builder(queue, store, Doubler)
            .key_fn(|_| "same".to_string())
            .build();
        let err = bulk.batch("missing").unwrap().resume().await.unwrap_err();
        assert!(matches!(err, Error::BatchNotFound(id) if id == "missing"));

        let err = bulk
            .batch("b")
            .unwrap()
            .run(vec![Item { n: 1 }, Item { n: 2 }])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::DuplicateKey(key) if key == "same"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn status_reports_the_last_recorded_outcome_of_each_item() {
        let (queue, store) = fresh().await;
        let bulk = Bulk::builder(queue.clone(), store, Doubler)
            .poll_interval(Duration::from_millis(10))
            .build();
        let batch = bulk.batch("b").unwrap();
        assert!(matches!(batch.status().await, Err(Error::BatchNotFound(_))));

        tokio::time::timeout(
            Duration::from_secs(10),
            batch.run(vec![Item { n: 1 }, Item { n: 13 }, Item { n: 3 }]),
        )
        .await
        .expect("run finished in time")
        .unwrap();

        let status = batch.status().await.unwrap();
        assert_eq!((status.total, status.succeeded, status.failed), (3, 2, 1));
        assert_eq!(status.failed_keys, vec!["item-1".to_string()]);
        assert_eq!(status.cost.get("calls"), 2.0);
        let markers = queue
            .kv_scan(b"workflow/bulk/batches/b/items/", None, 10)
            .await
            .unwrap()
            .entries;
        assert_eq!(markers.len(), 3);
    }

    #[tokio::test]
    async fn an_invalid_batch_id_is_rejected() {
        let (queue, store) = fresh().await;
        let bulk = Bulk::builder(queue, store, Doubler).build();
        assert!(matches!(
            bulk.batch("a/b").map(|b| b.id().to_string()),
            Err(Error::InvalidBatchId(id)) if id == "a/b"
        ));
        assert_eq!(bulk.batch("ok-1").unwrap().id(), "ok-1");
    }

    #[tokio::test]
    async fn a_redelivered_notification_counts_an_item_once() {
        let mut state = ProgressState::new();
        state.total = 2;
        let shared = Arc::new(Shared {
            state: Mutex::new(state),
            notify: Notify::new(),
        });
        let sink = Arc::new(Collect::default());
        let hook = BulkHook::<u32> {
            shared: shared.clone(),
            sink: sink.clone(),
            _output: PhantomData,
        };

        let outcome = RunOutcome {
            run_id: "item-1".to_string(),
            status: TerminalStatus::Succeeded,
            result: None,
            error: None,
            headers: HashMap::new(),
            final_step: 0,
        };
        hook.on_termination(&outcome, &TerminalEffects::detached())
            .await
            .unwrap();
        hook.on_termination(&outcome, &TerminalEffects::detached())
            .await
            .unwrap();

        let state = shared.state.lock().unwrap();
        assert_eq!(state.succeeded, 1);
        assert!(!state.is_done(), "one of two items is terminal");
        assert_eq!(sink.records.lock().unwrap().len(), 1);
    }
}
