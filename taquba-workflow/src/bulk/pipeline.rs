//! The [`Pipeline`] contract and the per-item [`BulkCtx`] handed to it.

use std::collections::HashMap;
use std::future::Future;

use crate::{EffectsHandle, KvReadHandle, Memo, Step, StepError};
use serde::Serialize;
use serde::de::DeserializeOwned;
use taquba::LeaseHandle;
use tokio_util::sync::CancellationToken;

use crate::bulk::cost::CostReport;

/// Defines a per-item processing pipeline. Each bulk run executes one
/// `Pipeline` for every input item independently, materialised internally
/// as a [`crate`] run.
///
/// A `Pipeline` is a single async [`run`](Pipeline::run) method: the bulk
/// runner deserializes one input item, builds a [`BulkCtx`] around it, and
/// awaits `run`. The expensive logical steps inside `run` (LLM calls, paid
/// APIs, CPU-bound work) are wrapped in [`BulkCtx::memoized`] or
/// [`BulkCtx::memoized_by_content`] so an at-least-once retry of the item
/// replays cached step results instead of paying for them twice.
///
/// # Error classification
///
/// [`Self::Error`] must convert into a [`StepError`], which is what decides
/// retry behaviour: a [`StepError::transient`] error nacks and retries with
/// the queue's backoff up to `max_attempts` (then dead-letters and the item
/// terminates failed); a [`StepError::permanent`] error dead-letters the
/// item immediately. The simplest choice is to use `StepError` directly as
/// `type Error` (as the example below does); otherwise implement
/// `From<YourError> for StepError`.
///
/// # Example
///
/// ```no_run
/// use serde::{Deserialize, Serialize};
/// use taquba_workflow::bulk::{BulkCtx, CostReport, Pipeline};
/// use taquba_workflow::StepError;
///
/// #[derive(Serialize, Deserialize)]
/// struct Ticket { id: String, body: String }
///
/// #[derive(Serialize, Deserialize)]
/// struct Processed { id: String, classification: String }
///
/// struct TicketPipeline;
///
/// impl Pipeline for TicketPipeline {
///     type Input = Ticket;
///     type Output = Processed;
///     type Error = StepError;
///
///     async fn run(&self, ctx: &BulkCtx<Ticket>) -> Result<Processed, StepError> {
///         let classification = ctx
///             .memoized_with_cached_cost("classify", async {
///                 let cost = CostReport::new();
///                 cost.record("llm_calls", 1.0);
///                 Ok::<_, StepError>(("billing".to_string(), cost))
///             })
///             .await?;
///         Ok(Processed { id: ctx.input.id.clone(), classification })
///     }
/// }
/// ```
pub trait Pipeline: Send + Sync + 'static {
    /// One input item. Deserialized from the bulk input source and handed to
    /// [`run`](Pipeline::run) via [`BulkCtx::input`].
    type Input: Serialize + DeserializeOwned + Send + 'static;
    /// The per-item result. Serialized into the bulk output stream once the
    /// item completes.
    type Output: Serialize + DeserializeOwned + Send + 'static;
    /// Failure type. Must convert into a [`StepError`] so the runner can
    /// decide transient vs. permanent handling. Use `StepError` directly for
    /// the common case.
    type Error: Into<StepError> + Send + 'static;

    /// Process one input item. Wrap expensive logical steps in
    /// [`BulkCtx::memoized`] or [`BulkCtx::memoized_by_content`] to make
    /// retries cheap.
    fn run(
        &self,
        ctx: &BulkCtx<Self::Input>,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
}

/// Per-item execution context handed to [`Pipeline::run`].
///
/// Wraps the typed input together with the durable per-item
/// [memo](crate::Memo), a [cost accumulator](CostReport), the
/// run's cooperative [cancellation token](CancellationToken), the
/// delivery's [lease handle](LeaseHandle), the item's staged
/// [KV effects](EffectsHandle) and read access to the caller KV
/// namespace.
pub struct BulkCtx<T> {
    /// The deserialized input item for this run.
    pub input: T,
    /// The run identifier for this item (the value the bulk runner derived
    /// from the input, or a positional `item-{i}` default).
    pub run_id: String,
    /// Submitter-supplied metadata threaded through from the bulk run.
    pub headers: HashMap<String, String>,
    memo: Memo,
    cost: CostReport,
    cancel_token: CancellationToken,
    lease: LeaseHandle,
    effects: EffectsHandle,
    kv: KvReadHandle,
}

impl<T> BulkCtx<T> {
    pub(crate) fn new(input: T, step: &Step) -> Self {
        Self {
            input,
            run_id: step.run_id.clone(),
            headers: step.headers.clone(),
            memo: step.memo.clone(),
            cost: CostReport::new(),
            cancel_token: step.cancel_token.clone(),
            lease: step.lease.clone(),
            effects: step.effects.clone(),
            kv: step.kv.clone(),
        }
    }

    /// Return the value memoized under `key`, or run `f`, memoize its
    /// value under `key` and return it.
    ///
    /// This is [`Memo::memoized`] on the item's memo, with memo errors
    /// converted into the caller's error type: a memo read or write
    /// failure is a transient [`StepError`] and a failure to serialize
    /// the computed value is a permanent one. An `Err` from `f` is
    /// returned unchanged and memoizes nothing.
    ///
    /// `key` namespaces the memo within this item; use a distinct key
    /// per logical step. Calls to [`record_cost`](Self::record_cost)
    /// inside `f` run only when `f` runs. Use
    /// [`memoized_with_cached_cost`](Self::memoized_with_cached_cost)
    /// when memoized results should also contribute to the final cost
    /// report.
    pub async fn memoized<R, F, E>(&self, key: &str, f: F) -> Result<R, E>
    where
        R: Serialize + DeserializeOwned,
        F: Future<Output = Result<R, E>>,
        E: From<StepError>,
    {
        self.memo
            .memoized(key, async { f.await.map_err(MemoizedError::Caller) })
            .await
            .map_err(MemoizedError::into_caller)
    }

    /// [`Self::memoized`] under [`Memo::content_key`] of `input`.
    ///
    /// The entry remains scoped to this item's workflow run and step;
    /// this method does not create a cross-item cache. The derived key
    /// is stable only when `input` serializes deterministically; types
    /// with unordered iteration, such as `HashMap`, can serialize the
    /// same logical content into different bytes and therefore
    /// different keys. If several logical operations may receive
    /// identical inputs, include an operation name in the serialized
    /// input.
    pub async fn memoized_by_content<K, R, F, E>(&self, input: &K, f: F) -> Result<R, E>
    where
        K: Serialize + ?Sized,
        R: Serialize + DeserializeOwned,
        F: Future<Output = Result<R, E>>,
        E: From<StepError>,
    {
        self.memo
            .memoized_by_content(input, async { f.await.map_err(MemoizedError::Caller) })
            .await
            .map_err(MemoizedError::into_caller)
    }

    /// Run `f` once and cache both its value and counters under `key`,
    /// or return the cached value and replay its counters on a retry.
    ///
    /// Use this when cost counters are known only inside a memoized step.
    /// The closure returns `(value, cost)`, and the helper records the
    /// `CostReport` after memoization returns, so counters are included
    /// whether the step computes freshly or hits memo state.
    pub async fn memoized_with_cached_cost<R, F, E>(&self, key: &str, f: F) -> Result<R, E>
    where
        R: Serialize + DeserializeOwned,
        F: Future<Output = Result<(R, CostReport), E>>,
        E: From<StepError>,
    {
        let (value, cost) = self.memoized(key, f).await?;
        self.cost.merge(&cost);
        Ok(value)
    }

    /// Run `f` once and cache both its value and counters under a key
    /// derived from serialized `input`, or return the cached value and
    /// replay its counters on a retry.
    ///
    /// Use this when the memo key should be content-derived and cost
    /// counters are known only inside the memoized step. The closure
    /// returns `(value, cost)`, and the helper records the `CostReport`
    /// after memoization returns, so counters are included whether the
    /// step computes freshly or hits memo state.
    pub async fn memoized_by_content_with_cached_cost<K, R, F, E>(
        &self,
        input: &K,
        f: F,
    ) -> Result<R, E>
    where
        K: Serialize + ?Sized,
        R: Serialize + DeserializeOwned,
        F: Future<Output = Result<(R, CostReport), E>>,
        E: From<StepError>,
    {
        let (value, cost) = self.memoized_by_content(input, f).await?;
        self.cost.merge(&cost);
        Ok(value)
    }

    /// Add `amount` to the cost counter named `metric` for this item. The
    /// per-item totals roll up into the batch-level
    /// [`ProgressSnapshot`](crate::bulk::ProgressSnapshot) and
    /// [`BulkReport`](crate::bulk::BulkReport).
    pub fn record_cost(&self, metric: &str, amount: f64) {
        self.cost.record(metric, amount);
    }

    /// The run's cooperative cancellation token. Watch it to short-circuit a
    /// long-running step when the bulk run is draining (e.g. on spot
    /// preemption); see [`crate::Step::cancel_token`].
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }

    /// The lease handle for this item's delivery. A long-running
    /// pipeline calls [`LeaseHandle::ensure_at_least`] at progress
    /// points (or once, with a slow call's timeout, before issuing it)
    /// so the item is not re-queued while it still runs; see
    /// [`crate::Step::lease`].
    pub fn lease(&self) -> &LeaseHandle {
        &self.lease
    }

    /// The item's staged application KV effects. Writes and deletes
    /// staged here are applied atomically with the item's successful
    /// completion; an item that fails applies nothing, and a staged
    /// value must be correct when applied more than once, since
    /// delivery is at-least-once. See [`EffectsHandle`] for the
    /// staging rules.
    pub fn effects(&self) -> &EffectsHandle {
        &self.effects
    }

    /// Read the committed value under `key` from the caller KV
    /// namespace, `None` when no value exists. Effects staged by this
    /// item become readable only once its completion commits; see
    /// [`crate::KvReadHandle`] for the read semantics.
    pub async fn kv_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StepError> {
        Ok(self
            .kv
            .get(key)
            .await
            .map_err(StepError::from)?
            .map(|bytes| bytes.to_vec()))
    }

    /// Snapshot of the cost accumulated so far for this item.
    pub(crate) fn cost(&self) -> CostReport {
        self.cost.clone()
    }
}

/// Error of a memoized computation before conversion into the caller's
/// error type: the caller's own error, or a memo error.
enum MemoizedError<E> {
    Caller(E),
    Memo(crate::Error),
}

impl<E> From<crate::Error> for MemoizedError<E> {
    fn from(err: crate::Error) -> Self {
        Self::Memo(err)
    }
}

impl<E: From<StepError>> MemoizedError<E> {
    fn into_caller(self) -> E {
        match self {
            Self::Caller(err) => err,
            Self::Memo(err) => E::from(StepError::from(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoStore;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use taquba::object_store::memory::InMemory;

    #[derive(Serialize)]
    struct ContentInput<'a> {
        operation: &'static str,
        payload: &'a [u8],
    }

    fn test_step(store: &MemoStore) -> Step {
        Step {
            run_id: "run-1".into(),
            step_number: 0,
            payload: Vec::new(),
            headers: HashMap::new(),
            job_id: "job-1".into(),
            attempts: 1,
            max_attempts: 3,
            cancel_token: CancellationToken::new(),
            lease: taquba::LeaseHandle::detached(),
            memo: store.new_memo("run-1", 0),
            run_memo: store.new_run_memo("run-1"),
            effects: EffectsHandle::detached(),
            kv: KvReadHandle::detached(),
            signal: None,
        }
    }

    fn ctx_for_tests() -> BulkCtx<()> {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        BulkCtx::new((), &test_step(&store))
    }

    #[tokio::test]
    async fn memoized_does_not_cache_errors() {
        let ctx = ctx_for_tests();
        let calls = AtomicU32::new(0);

        let err = ctx
            .memoized::<u32, _, StepError>("k", async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(StepError::transient("boom"))
            })
            .await
            .unwrap_err();
        assert_eq!(err.message, "boom");

        // A second attempt re-runs because the error was not cached.
        let ok = ctx
            .memoized::<u32, _, StepError>("k", async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(99)
            })
            .await
            .unwrap();
        assert_eq!(ok, 99);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_memo_error_is_converted_into_the_callers_error_type() {
        #[derive(Debug)]
        struct Unserializable;

        impl Serialize for Unserializable {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("unserializable"))
            }
        }

        impl<'de> serde::Deserialize<'de> for Unserializable {
            fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
                Ok(Self)
            }
        }

        let ctx = ctx_for_tests();
        let err = ctx
            .memoized::<Unserializable, _, StepError>("k", async { Ok(Unserializable) })
            .await
            .unwrap_err();

        assert_eq!(err.kind, crate::StepErrorKind::Permanent);
    }

    #[tokio::test]
    async fn memoized_with_cached_cost_records_cost_on_compute_and_memo_hit() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        let first_ctx = BulkCtx::new((), &test_step(&store));
        let replay_ctx = BulkCtx::new((), &test_step(&store));
        let calls = AtomicU32::new(0);

        let first = first_ctx
            .memoized_with_cached_cost("k", async {
                calls.fetch_add(1, Ordering::SeqCst);
                let cost = CostReport::new();
                cost.record("tokens", 42.0);
                Ok::<_, StepError>(("value".to_string(), cost))
            })
            .await
            .unwrap();
        let second = replay_ctx
            .memoized_with_cached_cost("k", async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, StepError>(("other".to_string(), CostReport::new()))
            })
            .await
            .unwrap();

        assert_eq!(first, "value");
        assert_eq!(second, "value");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "memo hit did not run closure"
        );
        assert_eq!(first_ctx.cost().get("tokens"), 42.0);
        assert_eq!(replay_ctx.cost().get("tokens"), 42.0);
    }

    #[tokio::test]
    async fn memoized_by_content_with_cached_cost_records_cost_on_compute_and_memo_hit() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        let first_ctx = BulkCtx::new((), &test_step(&store));
        let replay_ctx = BulkCtx::new((), &test_step(&store));
        let calls = AtomicU32::new(0);
        let input = ContentInput {
            operation: "classify",
            payload: b"ticket",
        };

        let first = first_ctx
            .memoized_by_content_with_cached_cost(&input, async {
                calls.fetch_add(1, Ordering::SeqCst);
                let cost = CostReport::new();
                cost.record("tokens", 42.0);
                Ok::<_, StepError>(("value".to_string(), cost))
            })
            .await
            .unwrap();
        let second = replay_ctx
            .memoized_by_content_with_cached_cost(&input, async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, StepError>(("other".to_string(), CostReport::new()))
            })
            .await
            .unwrap();

        assert_eq!(first, "value");
        assert_eq!(second, "value");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "memo hit did not run closure"
        );
        assert_eq!(first_ctx.cost().get("tokens"), 42.0);
        assert_eq!(replay_ctx.cost().get("tokens"), 42.0);
    }

    #[tokio::test]
    async fn record_cost_accumulates_into_snapshot() {
        let ctx = ctx_for_tests();
        ctx.record_cost("tokens", 100.0);
        ctx.record_cost("tokens", 50.0);
        assert_eq!(ctx.cost().get("tokens"), 150.0);
    }
}
