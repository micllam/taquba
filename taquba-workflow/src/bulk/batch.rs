//! The [`Bulk`] runner: a spawned worker over which batches of one
//! pipeline are run, with per-batch progress and cost and streamed
//! output as items complete.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::{MemoStore, RunSpec, RunnerHandle, TerminalStatus};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use taquba::object_store::ObjectStore;
use taquba::{Clock, Queue};
use tokio::sync::Semaphore;
use tracing::warn;

use crate::bulk::cost::CostReport;
use crate::bulk::io::{NullSink, OutputRecord, OutputSink};
use crate::bulk::manifest::{Manifest, ManifestItem, ManifestStore};
use crate::bulk::pipeline::Pipeline;
use crate::bulk::progress::{
    BatchStatus, BulkReport, ItemMarker, MarkerStatus, ProgressSnapshot, ProgressState,
};
use crate::bulk::runner::{ItemEnvelope, ItemPayload, PipelineRunner};
use crate::keys::{
    BULK_TERMINAL_KV_PREFIX, bulk_items_kv_prefix, bulk_terminal_kv_key, hex_sha256,
};
use crate::outcome::{
    OutcomeRecord, StoredOutcome, Terminal, TypedRuntime, TypedRuntimeOptions, Unrecorded,
};
use crate::sweep::Sweep;
use crate::{Error, Result};

/// Default queue name for bulk item steps.
const DEFAULT_QUEUE_NAME: &str = "bulk-items";
/// Default object-store prefix for per-item memo entries.
const DEFAULT_MEMO_PREFIX: &str = "bulk-memo";
/// Default ceiling on concurrently-processing items in one process.
const DEFAULT_MAX_CONCURRENT: usize = 200;
/// Submissions in flight at once. Each blocks on a durable enqueue
/// commit, and concurrent commits share WAL flushes, so at flush-bound
/// latencies serial submission would cap at one item per flush.
const SUBMIT_CONCURRENCY: usize = 32;

/// The workflow run id of an item: the hex SHA-256 digest of
/// `{batch_id}/{key}`, so batches never share run state and any key string
/// maps onto the character set a run id accepts.
pub(crate) fn item_run_id(batch_id: &str, key: &str) -> String {
    hex_sha256(&[batch_id.as_bytes(), b"/", key.as_bytes()])
}

/// The in-process state of a batch being run in this process: its
/// counters, updated as its items terminate.
struct BatchState {
    state: Mutex<ProgressState>,
}

impl BatchState {
    /// Write an item's record to the sink and fold it into the counters.
    fn record(&self, sink: &dyn OutputSink, item: TerminalItem<'_>) {
        let record = OutputRecord {
            key: item.key,
            status: item.status.as_str(),
            output: item.output,
            error: item.error,
        };
        if let Err(err) = sink.write(&record) {
            warn!(key = %item.key, error = %err, "failed to write bulk output record");
        }
        let mut state = self.state.lock().unwrap();
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
    }
}

/// The batches being run in this process, keyed by batch id; an entry
/// exists for the duration of [`Batch::run`] or [`Batch::resume`].
type ActiveBatches = Mutex<HashMap<String, Arc<BatchState>>>;

/// The registration of a running batch in [`ActiveBatches`]; removed on
/// drop, so a run future dropped before completion deregisters its
/// batch.
struct ActiveBatch {
    batches: Arc<ActiveBatches>,
    id: String,
    state: Arc<BatchState>,
}

impl Drop for ActiveBatch {
    fn drop(&mut self) {
        self.batches.lock().unwrap().remove(&self.id);
    }
}

/// One terminated item, as observed in this process.
struct TerminalItem<'a> {
    key: &'a str,
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

/// Fold an item's outcome record into the batch: a success carries its
/// output and cost, a failure its error.
fn record_outcome<O>(state: &BatchState, sink: &dyn OutputSink, key: &str, record: OutcomeRecord)
where
    O: Serialize + DeserializeOwned,
{
    match record.outcome {
        StoredOutcome::Success { output } => {
            let (output, cost) = decode_envelope::<O>(key, &output);
            state.record(
                sink,
                TerminalItem {
                    key,
                    status: TerminalStatus::Succeeded,
                    output,
                    error: None,
                    cost,
                },
            );
        }
        StoredOutcome::Failure { message, .. } => state.record(
            sink,
            TerminalItem {
                key,
                status: TerminalStatus::Failed,
                output: None,
                error: Some(&message),
                cost: None,
            },
        ),
    }
}

/// The durable state of batches: manifests and outcome records in the
/// object store, item markers in the queue's KV namespace.
#[derive(Clone)]
struct BatchStore {
    queue: Arc<Queue>,
    memo_store: MemoStore,
    manifests: ManifestStore,
}

impl BatchStore {
    /// Remove the durable state of batch `id`: its manifest, item markers,
    /// memo entries and outcome records. A batch without a manifest has
    /// its markers removed and nothing else.
    async fn forget(&self, id: &str) -> Result<()> {
        if let Some(manifest) = self.manifests.read(id).await? {
            for item in &manifest.items {
                self.memo_store
                    .clear_memos_for_run(&item_run_id(id, &item.key))
                    .await?;
            }
        }
        let prefix = bulk_items_kv_prefix(id);
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let page = self
                .queue
                .kv_scan(&prefix, cursor.as_deref(), 1000)
                .await
                .map_err(crate::Error::from)?;
            for (key, _) in &page.entries {
                self.queue
                    .kv_delete(key)
                    .await
                    .map_err(crate::Error::from)?;
            }
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        self.manifests.delete(id).await
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
    batch_retention: Option<Duration>,
    clock: Option<Arc<dyn Clock>>,
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
    /// must not start with the runtime's reserved `workflow.` prefix.
    pub fn headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers = headers;
        self
    }

    /// Maximum number of items processed concurrently by the worker.
    /// Defaults to 200. Bulk workloads are I/O-bound (each step awaits a
    /// remote call), so this can be set well above the CPU count.
    pub fn max_concurrent(mut self, n: usize) -> Self {
        self.max_concurrent = n;
        self
    }

    /// Maximum time the worker waits on an empty queue before re-checking.
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

    /// Fail a batch run if more than `percent` of its items terminate
    /// failed.
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

    /// Remove a batch's durable state (its manifest, item markers, memo
    /// entries and outcome records) `retention` after the batch completes.
    /// A completing run writes a terminal marker; the spawned worker
    /// removes the batches whose markers have expired when it starts and
    /// on every retention interval after that. When unset (default),
    /// batches are retained until [`Batch::forget`]. The window counts
    /// from the batch's first completion: a run of the same batch inside
    /// the window completes against the same expiry.
    ///
    /// # Panics
    ///
    /// [`build`](Self::build) panics if `retention < 1ms`.
    pub fn batch_retention(mut self, retention: Duration) -> Self {
        self.batch_retention = Some(retention);
        self
    }

    /// Override the [`Clock`] the runner reads timestamps from. Defaults to
    /// the queue's clock ([`Queue::clock`]).
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Finalize the builder.
    pub fn build(self) -> Bulk<P> {
        let sink: Arc<dyn OutputSink> = self.sink.unwrap_or_else(|| Arc::new(NullSink));
        let store = BatchStore {
            queue: self.queue.clone(),
            memo_store: MemoStore::new(self.object_store.clone(), self.memo_prefix.clone()),
            manifests: ManifestStore::new(self.object_store.clone(), self.memo_prefix.clone()),
        };
        let options = TypedRuntimeOptions {
            queue_name: self.queue_name,
            memo_prefix: self.memo_prefix,
            max_concurrent: self.max_concurrent,
            poll_interval: self.poll_interval,
            clock: self.clock,
        };
        let runner = PipelineRunner::new(self.pipeline);
        let batch_sweep = self.batch_retention.map(|retention| {
            let store = store.clone();
            Sweep::new(BULK_TERMINAL_KV_PREFIX, retention, move |id| {
                let store = store.clone();
                async move { store.forget(&id).await }
            })
        });
        let typed = options.build(
            self.queue,
            self.object_store,
            runner,
            |builder| match batch_sweep {
                Some(sweep) => builder.sweep(sweep),
                None => builder,
            },
        );
        Bulk {
            inner: Arc::new(BulkInner {
                typed,
                store,
                batches: Arc::default(),
                sink,
                key_fn: self.key_fn,
                headers: self.headers,
                fail_threshold: self.fail_threshold,
                batch_retention: self.batch_retention,
            }),
        }
    }
}

/// Runs one [`Pipeline`] over many inputs in a single process. Build it
/// with [`Bulk::builder`], [`spawn`](Self::spawn) the worker, then run
/// batches through [`Bulk::batch`], [`Bulk::new_batch`] or [`Bulk::run`];
/// several batches can run concurrently on one worker. One runner per
/// process: taquba is single-writer.
pub struct Bulk<P: Pipeline> {
    inner: Arc<BulkInner<P>>,
}

/// The state shared by the runner, its batch handles and the spawned
/// worker task.
struct BulkInner<P: Pipeline> {
    typed: TypedRuntime<PipelineRunner<P>>,
    store: BatchStore,
    batches: Arc<ActiveBatches>,
    sink: Arc<dyn OutputSink>,
    key_fn: Option<KeyFn<P::Input>>,
    headers: HashMap<String, String>,
    fail_threshold: Option<f64>,
    batch_retention: Option<Duration>,
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
            batch_retention: None,
            clock: None,
        }
    }

    /// Spawn the worker task and return a handle for graceful shutdown.
    ///
    /// The worker runs the items of every batch submitted to this runner,
    /// concurrently up to [`BulkBuilder::max_concurrent`], until either
    /// `shutdown` resolves or [`RunnerHandle::shutdown`] is called;
    /// in-flight items are allowed to finish. Under
    /// [`BulkBuilder::batch_retention`] it also sweeps expired batches
    /// when it starts and on every retention interval. A batch run
    /// started before the worker is spawned waits for it.
    ///
    /// # Panics
    ///
    /// Panics if called more than once.
    pub fn spawn<F>(&mut self, shutdown: F) -> RunnerHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.inner.typed.spawn_once(shutdown)
    }

    /// A handle on the batch named `id`, which must be 1 to 128 bytes of
    /// `[A-Za-z0-9_-]`. A batch groups the items of one submission: a
    /// second run of the same batch skips the items that succeeded and runs
    /// the failed ones again.
    pub fn batch(&self, id: impl Into<String>) -> Result<Batch<'_, P>> {
        let id = id.into();
        crate::keys::validate_run_id(&id).map_err(|_| Error::InvalidBatchId(id.clone()))?;
        Ok(Batch {
            inner: &self.inner,
            id,
        })
    }

    /// A handle on a new batch with a generated id.
    pub fn new_batch(&self) -> Batch<'_, P> {
        Batch {
            inner: &self.inner,
            id: ulid::Ulid::new().to_string(),
        }
    }

    /// Run every input as a new batch with a generated id and return the
    /// final [`BulkReport`]; see [`Batch::run`].
    pub async fn run<I>(&self, inputs: I) -> Result<BulkReport>
    where
        I: IntoIterator<Item = P::Input>,
    {
        self.new_batch().run(inputs).await
    }
}

impl<P: Pipeline> BulkInner<P> {
    /// Register `id` as running in this process with `total` items.
    /// Rejects a batch that is already running here.
    fn activate(&self, id: &str, total: usize) -> Result<ActiveBatch> {
        let mut batches = self.batches.lock().unwrap();
        if batches.contains_key(id) {
            return Err(Error::BatchRunning(id.to_string()));
        }
        let state = Arc::new(BatchState {
            state: Mutex::new(ProgressState::new(total)),
        });
        batches.insert(id.to_string(), state.clone());
        Ok(ActiveBatch {
            batches: self.batches.clone(),
            id: id.to_string(),
            state,
        })
    }

    /// Run one item to its terminal state in this process: answer it from
    /// a success record, or submit it (a failure record is ignored so the
    /// item runs again), wait for the run to terminate and fold the
    /// outcome into the batch.
    async fn drive_item(
        &self,
        batch_id: &str,
        state: &BatchState,
        submits: &Semaphore,
        item: ManifestItem,
    ) -> Result<()> {
        let ManifestItem { key, input } = item;
        let run_id = item_run_id(batch_id, &key);
        if let Some(record) = self.typed.outcome(&run_id).await?
            && matches!(record.outcome, StoredOutcome::Success { .. })
        {
            record_outcome::<P::Output>(state, self.sink.as_ref(), &key, record);
            return Ok(());
        }
        let payload = rmp_serde::to_vec_named(&ItemPayload {
            batch_id: batch_id.to_string(),
            key: key.clone(),
            input,
        })?;
        let submitted = {
            let _permit = submits
                .acquire()
                .await
                .expect("the semaphore is never closed");
            self.typed
                .runtime
                .submit(RunSpec {
                    run_id: Some(run_id.clone()),
                    input: payload,
                    headers: self.headers.clone(),
                    ..RunSpec::default()
                })
                .await?
        };
        match self.typed.wait_terminal(&run_id, &submitted.job_id).await? {
            Terminal::Recorded(record) => {
                record_outcome::<P::Output>(state, self.sink.as_ref(), &key, record);
            }
            // A run dead-lettered outside its step (its lease expired
            // past the attempt limit) failed without a record; every
            // other unrecorded end is a cancellation.
            Terminal::Unrecorded(Unrecorded::Dead(error)) => state.record(
                self.sink.as_ref(),
                TerminalItem {
                    key: &key,
                    status: TerminalStatus::Failed,
                    output: None,
                    error: Some(
                        error
                            .as_deref()
                            .unwrap_or("dead-lettered without an outcome"),
                    ),
                    cost: None,
                },
            ),
            Terminal::Unrecorded(_) => state.record(
                self.sink.as_ref(),
                TerminalItem {
                    key: &key,
                    status: TerminalStatus::Cancelled,
                    output: None,
                    error: None,
                    cost: None,
                },
            ),
        }
        Ok(())
    }
}

/// A handle on one batch of a [`Bulk`] runner. Obtained from
/// [`Bulk::batch`] or [`Bulk::new_batch`].
pub struct Batch<'a, P: Pipeline> {
    inner: &'a Arc<BulkInner<P>>,
    id: String,
}

impl<P: Pipeline> Batch<'_, P> {
    /// The batch id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Submit every input and wait until every item has terminated,
    /// returning the final [`BulkReport`]. The batch's manifest is
    /// written before any item is submitted; a run of an existing batch
    /// with a different item set is rejected with
    /// [`Error::BatchMismatch`]. An item whose outcome record from an
    /// earlier run of this batch is a success is counted and written to
    /// the sink from that record without running again; an item whose
    /// record is a failure runs again.
    ///
    /// The items run on the worker spawned by [`Bulk::spawn`]. Dropping
    /// the returned future stops waiting: the items already submitted
    /// keep running on the worker, keep their durable state and are
    /// continued by a later run or [`resume`](Self::resume) of the
    /// batch. A submission error returns after the items submitted so
    /// far, and those keep their durable state as well. A batch already
    /// running in this process is rejected with [`Error::BatchRunning`].
    pub async fn run<I>(&self, inputs: I) -> Result<BulkReport>
    where
        I: IntoIterator<Item = P::Input>,
    {
        let manifest = Manifest {
            batch_id: self.id.clone(),
            items: self.materialize(inputs)?,
        };
        match self.inner.store.manifests.read(&self.id).await? {
            Some(existing) if existing.items != manifest.items => {
                return Err(Error::BatchMismatch(self.id.clone()));
            }
            Some(_) => {}
            None => self.inner.store.manifests.write(&manifest).await?,
        }
        self.drive(manifest.items).await
    }

    /// Run the batch from its manifest, without the inputs: completed
    /// items are answered from their outcome records, items still queued
    /// continue, and the rest run. Returns [`Error::BatchNotFound`] when
    /// no manifest exists. Waits and stops waiting as [`run`](Self::run)
    /// does.
    pub async fn resume(&self) -> Result<BulkReport> {
        let manifest = self
            .inner
            .store
            .manifests
            .read(&self.id)
            .await?
            .ok_or_else(|| Error::BatchNotFound(self.id.clone()))?;
        self.drive(manifest.items).await
    }

    /// A point-in-time snapshot of the batch's progress while it is being
    /// run in this process, `None` otherwise.
    pub fn progress(&self) -> Option<ProgressSnapshot> {
        let state = self.inner.batches.lock().unwrap().get(&self.id).cloned();
        state.map(|state| state.state.lock().unwrap().snapshot())
    }

    /// The durable state of the batch: its item count from the manifest
    /// and, per item, the last outcome its settlement recorded. An item
    /// that ran again after a failure is reported by its latest outcome.
    /// Returns [`Error::BatchNotFound`] when no manifest exists.
    pub async fn status(&self) -> Result<BatchStatus> {
        let inner = self.inner;
        let manifest = inner
            .store
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
            cost: CostReport::new(),
            failed_keys: Vec::new(),
        };
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let page = inner
                .store
                .queue
                .kv_scan(&prefix, cursor.as_deref(), 1000)
                .await?;
            for (kv_key, value) in &page.entries {
                let key = String::from_utf8_lossy(&kv_key[prefix.len()..]).into_owned();
                let marker: ItemMarker = match rmp_serde::from_slice(value) {
                    Ok(marker) => marker,
                    Err(err) => {
                        warn!(batch_id = %self.id, key = %key, error = %err, "item marker failed to decode");
                        continue;
                    }
                };
                match marker.status {
                    MarkerStatus::Succeeded => status.succeeded += 1,
                    MarkerStatus::Failed => {
                        status.failed += 1;
                        status.failed_keys.push(key);
                    }
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

    /// Remove the batch's durable state: its manifest, item markers, memo
    /// entries and outcome records. A later run of the same id starts
    /// from nothing.
    pub async fn forget(&self) -> Result<()> {
        self.inner.store.forget(&self.id).await
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
            let key = match &self.inner.key_fn {
                Some(f) => f(&input),
                None => format!("item-{i}"),
            };
            if !seen.insert(key.clone()) {
                return Err(Error::DuplicateItemKey(key));
            }
            items.push(ManifestItem {
                key,
                input: rmp_serde::to_vec_named(&input)?,
            });
        }
        Ok(items)
    }

    /// Register the batch, drive every item to its terminal state and
    /// build the report. Items are driven concurrently; the first error
    /// stops the others and is returned.
    async fn drive(&self, items: Vec<ManifestItem>) -> Result<BulkReport> {
        let inner = self.inner;
        let active = inner.activate(&self.id, items.len())?;
        let submits = Arc::new(Semaphore::new(SUBMIT_CONCURRENCY));
        let driven = {
            let mut set = tokio::task::JoinSet::new();
            for item in items {
                let inner = Arc::clone(inner);
                let state = active.state.clone();
                let submits = submits.clone();
                let batch_id = self.id.clone();
                set.spawn(async move { inner.drive_item(&batch_id, &state, &submits, item).await });
            }
            let mut result = Ok(());
            while let Some(joined) = set.join_next().await {
                match joined {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        result = Err(err);
                        break;
                    }
                    // The set is never aborted while joining, so a join
                    // error is a panic in an item task; propagate it.
                    Err(join_err) => std::panic::resume_unwind(join_err.into_panic()),
                }
            }
            result
        };
        if let Err(flush_err) = inner.sink.flush() {
            match driven {
                Ok(()) => return Err(flush_err),
                Err(_) => warn!(error = %flush_err, "sink flush failed after an item error"),
            }
        }
        driven?;

        if inner.batch_retention.is_some() {
            let key = bulk_terminal_kv_key(&self.id, inner.typed.clock.now_ms());
            if let Err(err) = inner.store.queue.kv_put(&key, b"").await {
                warn!(batch_id = %self.id, "batch terminal marker write failed: {err}");
            }
        }

        let report = active.state.state.lock().unwrap().to_report(&self.id);
        if let Some(threshold) = inner.fail_threshold
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

    fn spawned<P: Pipeline>(mut bulk: Bulk<P>) -> (Bulk<P>, RunnerHandle) {
        let worker = bulk.spawn(std::future::pending::<()>());
        (bulk, worker)
    }

    async fn fresh() -> (Arc<Queue>, Arc<dyn ObjectStore>) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let queue = Arc::new(Queue::open(store.clone(), "db").await.unwrap());
        (queue, store)
    }

    #[tokio::test(start_paused = true)]
    async fn runs_all_items_and_rolls_up_cost() {
        let (queue, store) = fresh().await;
        let sink = Arc::new(Collect::default());
        let (bulk, worker) = spawned(
            Bulk::builder(queue, store, Doubler)
                .output(sink.clone())
                .poll_interval(Duration::from_millis(10))
                .build(),
        );

        let inputs = vec![Item { n: 1 }, Item { n: 2 }, Item { n: 3 }];
        let report = tokio::time::timeout(Duration::from_secs(10), bulk.run(inputs))
            .await
            .expect("run finished in time")
            .unwrap();

        assert_eq!(report.total, 3);
        assert_eq!(report.succeeded, 3);
        assert_eq!(report.failed, 0);
        assert_eq!(report.cost.get("calls"), 3.0);

        worker.shutdown().await.unwrap();
        let records = sink.records.lock().unwrap();
        assert_eq!(records.len(), 3);
        assert!(records.iter().all(|(_, status, _)| status == "succeeded"));
        let outputs: Vec<u32> = records.iter().filter_map(|(_, _, o)| *o).collect();
        assert!(outputs.contains(&2) && outputs.contains(&4) && outputs.contains(&6));
    }

    #[tokio::test(start_paused = true)]
    async fn large_batch_submits_with_bounded_concurrency() {
        let (queue, store) = fresh().await;
        let sink = Arc::new(Collect::default());
        let (bulk, worker) = spawned(
            Bulk::builder(queue, store, Doubler)
                .output(sink.clone())
                .poll_interval(Duration::from_millis(10))
                .build(),
        );

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
        worker.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
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
        let (bulk, worker) = spawned(
            Bulk::builder(queue.clone(), store, EffectsPipeline)
                .poll_interval(Duration::from_millis(10))
                .build(),
        );

        let report = tokio::time::timeout(Duration::from_secs(10), bulk.run(vec![Item { n: 1 }]))
            .await
            .expect("run finished in time")
            .unwrap();
        assert_eq!(report.succeeded, 1);

        assert_eq!(
            queue.kv_get(b"app/items/item-0").await.unwrap().as_deref(),
            Some(b"seeded".as_slice()),
        );
        worker.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn records_failed_items() {
        let (queue, store) = fresh().await;
        let (bulk, worker) = spawned(
            Bulk::builder(queue, store, Doubler)
                .poll_interval(Duration::from_millis(10))
                .build(),
        );

        let inputs = vec![Item { n: 1 }, Item { n: 13 }, Item { n: 3 }];
        let report = tokio::time::timeout(Duration::from_secs(10), bulk.run(inputs))
            .await
            .expect("run finished in time")
            .unwrap();

        assert_eq!(report.total, 3);
        assert_eq!(report.succeeded, 2);
        assert_eq!(report.failed, 1);
        assert_eq!(report.failed_keys, vec!["item-1".to_string()]);
        worker.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn fail_threshold_trips_when_exceeded() {
        let (queue, store) = fresh().await;
        let (bulk, worker) = spawned(
            Bulk::builder(queue, store, Doubler)
                .poll_interval(Duration::from_millis(10))
                .fail_threshold(20.0)
                .build(),
        );

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
        worker.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn custom_key_fn_sets_item_keys() {
        let (queue, store) = fresh().await;
        let sink = Arc::new(Collect::default());
        let (bulk, worker) = spawned(
            Bulk::builder(queue, store, Doubler)
                .output(sink.clone())
                .key_fn(|item| format!("n-{}", item.n))
                .poll_interval(Duration::from_millis(10))
                .build(),
        );

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
        worker.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_batches_on_one_worker_report_independently() {
        let (queue, store) = fresh().await;
        let runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let (bulk, worker) = spawned(
            Bulk::builder(queue, store, Counting { runs: runs.clone() })
                .poll_interval(Duration::from_millis(10))
                .build(),
        );

        let (a, b) = tokio::time::timeout(
            Duration::from_secs(10),
            futures_util::future::join(
                bulk.batch("a")
                    .unwrap()
                    .run(vec![Item { n: 1 }, Item { n: 2 }, Item { n: 3 }]),
                bulk.batch("b").unwrap().run(vec![Item { n: 13 }]),
            ),
        )
        .await
        .expect("runs finished in time");
        let (a, b) = (a.unwrap(), b.unwrap());

        assert_eq!(
            (a.batch_id.as_str(), a.total, a.succeeded, a.failed),
            ("a", 3, 3, 0)
        );
        assert_eq!(
            (b.batch_id.as_str(), b.total, b.succeeded, b.failed),
            ("b", 1, 0, 1)
        );
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 4);
        worker.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_batch_running_in_this_process_is_rejected_until_its_run_is_dropped() {
        let (queue, store) = fresh().await;
        let mut bulk = Bulk::builder(queue, store, Doubler)
            .poll_interval(Duration::from_millis(10))
            .build();
        let inputs = || vec![Item { n: 1 }, Item { n: 2 }];
        {
            let batch = bulk.batch("b").unwrap();
            // No worker yet: the run submits its items and waits.
            let mut first = Box::pin(batch.run(inputs()));
            assert!(
                tokio::time::timeout(Duration::from_secs(5), first.as_mut())
                    .await
                    .is_err(),
                "the run waits for a worker",
            );
            assert_eq!(batch.progress().map(|p| p.total), Some(2));
            let err = batch.run(inputs()).await.unwrap_err();
            assert!(matches!(err, Error::BatchRunning(id) if id == "b"));

            drop(first);
            assert!(batch.progress().is_none());
        }

        // The submitted items keep their state and a resume continues them.
        let worker = bulk.spawn(std::future::pending::<()>());
        let batch = bulk.batch("b").unwrap();
        let report = tokio::time::timeout(Duration::from_secs(10), batch.resume())
            .await
            .expect("resume finished in time")
            .unwrap();
        assert_eq!((report.total, report.succeeded), (2, 2));
        worker.shutdown().await.unwrap();
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

    #[tokio::test(start_paused = true)]
    async fn a_second_run_of_a_batch_skips_succeeded_items_and_reruns_failed_ones() {
        let (queue, store) = fresh().await;
        let runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let sink = Arc::new(Collect::default());
        let (bulk, worker) = spawned(
            Bulk::builder(queue, store, Counting { runs: runs.clone() })
                .output(sink.clone())
                .poll_interval(Duration::from_millis(10))
                .build(),
        );
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
        worker.shutdown().await.unwrap();
        let records = sink.records.lock().unwrap();
        assert_eq!(records.len(), 6);
        assert!(records.iter().any(|(key, status, output)| {
            key == "item-0" && status == "succeeded" && *output == Some(2)
        }));
    }

    #[tokio::test(start_paused = true)]
    async fn batches_with_the_same_keys_do_not_share_state() {
        let (queue, store) = fresh().await;
        let runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let (bulk, worker) = spawned(
            Bulk::builder(queue, store, Counting { runs: runs.clone() })
                .poll_interval(Duration::from_millis(10))
                .build(),
        );

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
        worker.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn resume_runs_a_batch_from_its_manifest() {
        let (queue, store) = fresh().await;
        let runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let sink = Arc::new(Collect::default());
        let (bulk, worker) = spawned(
            Bulk::builder(queue, store, Counting { runs: runs.clone() })
                .output(sink.clone())
                .poll_interval(Duration::from_millis(10))
                .build(),
        );

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
        worker.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_run_with_a_different_item_set_is_rejected() {
        let (queue, store) = fresh().await;
        let (bulk, worker) = spawned(
            Bulk::builder(queue, store, Doubler)
                .poll_interval(Duration::from_millis(10))
                .build(),
        );
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
        worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn resume_of_an_unknown_batch_and_duplicate_keys_are_rejected() {
        let (queue, store) = fresh().await;
        let (bulk, worker) = spawned(
            Bulk::builder(queue, store, Doubler)
                .key_fn(|_| "same".to_string())
                .build(),
        );
        let err = bulk.batch("missing").unwrap().resume().await.unwrap_err();
        assert!(matches!(err, Error::BatchNotFound(id) if id == "missing"));

        let err = bulk
            .batch("b")
            .unwrap()
            .run(vec![Item { n: 1 }, Item { n: 2 }])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::DuplicateItemKey(key) if key == "same"));
        worker.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn status_reports_the_last_recorded_outcome_of_each_item() {
        let (queue, store) = fresh().await;
        let (bulk, worker) = spawned(
            Bulk::builder(queue.clone(), store, Doubler)
                .poll_interval(Duration::from_millis(10))
                .build(),
        );
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
        worker.shutdown().await.unwrap();
    }

    async fn fresh_with_clock(t0: u64) -> (Arc<Queue>, Arc<dyn ObjectStore>, taquba::MockClock) {
        let clock = taquba::MockClock::new(t0);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let opts = taquba::OpenOptions::default().clock(Arc::new(clock.clone()));
        let queue = Arc::new(
            Queue::open_with_options(store.clone(), "db", opts)
                .await
                .unwrap(),
        );
        (queue, store, clock)
    }

    #[tokio::test(start_paused = true)]
    async fn forget_removes_the_batch_state_and_a_later_run_starts_over() {
        let (queue, store) = fresh().await;
        let runs = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let (bulk, worker) = spawned(
            Bulk::builder(queue.clone(), store, Counting { runs: runs.clone() })
                .poll_interval(Duration::from_millis(10))
                .build(),
        );
        let batch = bulk.batch("b").unwrap();
        tokio::time::timeout(Duration::from_secs(10), batch.run(vec![Item { n: 1 }]))
            .await
            .expect("run finished in time")
            .unwrap();

        batch.forget().await.unwrap();

        assert!(matches!(batch.status().await, Err(Error::BatchNotFound(_))));
        let markers = queue
            .kv_scan(b"workflow/bulk/batches/b/", None, 10)
            .await
            .unwrap()
            .entries;
        assert!(markers.is_empty());
        tokio::time::timeout(Duration::from_secs(10), batch.run(vec![Item { n: 1 }]))
            .await
            .expect("run finished in time")
            .unwrap();
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 2);
        worker.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn an_expired_batch_is_swept_on_the_retention_interval() {
        let t0 = 1_700_000_000_000;
        let retention = Duration::from_secs(60);
        let (queue, store, clock) = fresh_with_clock(t0).await;
        let (bulk, worker) = spawned(
            Bulk::builder(queue.clone(), store, Doubler)
                .poll_interval(Duration::from_millis(10))
                .batch_retention(retention)
                .clock(Arc::new(clock.clone()))
                .build(),
        );
        let bulk = &bulk;
        let run = |id: &'static str, n: u32| async move {
            tokio::time::timeout(
                Duration::from_secs(10),
                bulk.batch(id).unwrap().run(vec![Item { n }]),
            )
            .await
            .expect("run finished in time")
            .unwrap();
        };
        // Whether a sweep pass has removed the batch.
        let swept = |id: &'static str| async move {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if matches!(
                        bulk.batch(id).unwrap().status().await,
                        Err(Error::BatchNotFound(_))
                    ) {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .is_ok()
        };

        run("old", 1).await;
        assert!(bulk.batch("old").unwrap().status().await.is_ok());
        let terminals = queue
            .kv_scan(b"workflow/bulk/terminals/", None, 10)
            .await
            .unwrap()
            .entries;
        assert_eq!(terminals.len(), 1);

        // Inside the window a sweep pass removes nothing.
        clock.advance(Duration::from_secs(30));
        run("mid", 2).await;
        tokio::time::advance(retention).await;
        assert!(!swept("old").await);

        // Past the window the next pass removes the old batch.
        clock.advance(Duration::from_secs(31));
        tokio::time::advance(retention).await;
        assert!(swept("old").await);
        assert!(bulk.batch("mid").unwrap().status().await.is_ok());
        worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn an_invalid_batch_id_is_rejected() {
        let (queue, store) = fresh().await;
        let (bulk, worker) = spawned(Bulk::builder(queue, store, Doubler).build());
        assert!(matches!(
            bulk.batch("a/b").map(|b| b.id().to_string()),
            Err(Error::InvalidBatchId(id)) if id == "a/b"
        ));
        assert_eq!(bulk.batch("ok-1").unwrap().id(), "ok-1");
        worker.shutdown().await.unwrap();
    }
}
