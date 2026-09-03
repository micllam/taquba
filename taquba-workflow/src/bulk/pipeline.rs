//! The [`Pipeline`] contract and the per-item [`BulkCtx`] handed to it.

use std::future::Future;
use std::ops::Deref;

use crate::{Delivery, StepError};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::bulk::cost::CostReport;

/// Defines a per-item processing pipeline. Each bulk run executes one
/// `Pipeline` for every input item independently, materialised internally
/// as a [`crate`] run.
///
/// A `Pipeline` is a single async [`run`](Pipeline::run) method: the bulk
/// runner deserializes one input item, builds a [`BulkCtx`] around it, and
/// awaits `run`. The expensive logical steps inside `run` (LLM calls, paid
/// APIs, CPU-bound work) are wrapped in
/// [`Memo::memoized`](crate::Memo::memoized) or
/// [`Memo::memoized_by_content`](crate::Memo::memoized_by_content) on the
/// item's [`memo`](Delivery::memo), so an at-least-once retry of the item
/// reads the completed steps back and pays for none of them twice.
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
    /// [`Memo::memoized`](crate::Memo::memoized) or
    /// [`Memo::memoized_by_content`](crate::Memo::memoized_by_content) on
    /// the item's [`memo`](Delivery::memo) to make retries cheap.
    fn run(
        &self,
        ctx: &BulkCtx<Self::Input>,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
}

/// Per-item execution context handed to [`Pipeline::run`]: the typed
/// input, the item's identity within its batch, a [cost
/// accumulator](CostReport) and the [`Delivery`] the item runs under,
/// which it dereferences to (run identity, memo, cancellation token,
/// lease, staged KV effects and committed KV reads).
pub struct BulkCtx<T> {
    /// The deserialized input item for this run.
    pub input: T,
    /// The batch this item belongs to.
    pub batch_id: String,
    /// The item's key: the value of
    /// [`BulkRunnerBuilder::key_fn`](crate::bulk::BulkRunnerBuilder::key_fn) for the
    /// input, or the positional `item-{i}` default.
    pub key: String,
    /// The delivery this item runs under. Its `run_id` is derived from
    /// the batch id and the key.
    pub delivery: Delivery,
    cost: CostReport,
}

impl<T> Deref for BulkCtx<T> {
    type Target = Delivery;

    fn deref(&self) -> &Delivery {
        &self.delivery
    }
}

impl<T> BulkCtx<T> {
    pub(crate) fn new(batch_id: &str, key: &str, input: T, delivery: Delivery) -> Self {
        Self {
            input,
            batch_id: batch_id.to_string(),
            key: key.to_string(),
            delivery,
            cost: CostReport::new(),
        }
    }

    /// Run `f` once and memoize both its value and counters under `key`
    /// in the item's [`memo`](Delivery::memo), or return the memoized
    /// value and replay its counters on a retry.
    ///
    /// `f` returns `(value, cost)`, and the counters are recorded after
    /// memoization returns, so they are included whether the phase runs or
    /// reads its memo entry. Calls to [`record_cost`](Self::record_cost)
    /// inside a plain [`Memo::memoized`](crate::Memo::memoized) future run
    /// only when the future runs.
    pub async fn memoized_with_cached_cost<R, F, E>(&self, key: &str, f: F) -> Result<R, E>
    where
        R: Serialize + DeserializeOwned,
        F: Future<Output = Result<(R, CostReport), E>>,
        E: From<crate::Error>,
    {
        let (value, cost) = self.memo.memoized(key, f).await?;
        self.cost.merge(&cost);
        Ok(value)
    }

    /// [`Self::memoized_with_cached_cost`] under
    /// [`Memo::content_key`](crate::Memo::content_key) of `input`.
    pub async fn memoized_by_content_with_cached_cost<K, R, F, E>(
        &self,
        input: &K,
        f: F,
    ) -> Result<R, E>
    where
        K: Serialize + ?Sized,
        R: Serialize + DeserializeOwned,
        F: Future<Output = Result<(R, CostReport), E>>,
        E: From<crate::Error>,
    {
        let (value, cost) = self.memo.memoized_by_content(input, f).await?;
        self.cost.merge(&cost);
        Ok(value)
    }

    /// Add `amount` to the cost counter named `metric` for this item. The
    /// per-item totals roll up into the batch-level
    /// [`ProgressSnapshot`](crate::bulk::ProgressSnapshot) and
    /// [`BatchReport`](crate::bulk::BatchReport).
    pub fn record_cost(&self, metric: &str, amount: f64) {
        self.cost.record(metric, amount);
    }

    /// Snapshot of the cost accumulated so far for this item.
    pub(crate) fn cost(&self) -> CostReport {
        self.cost.clone()
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

    fn test_delivery(store: &MemoStore) -> Delivery {
        let mut delivery = Delivery::detached();
        delivery.memo = store.new_memo(&delivery.run_id, 0);
        delivery.run_memo = store.new_run_memo(&delivery.run_id);
        delivery
    }

    fn ctx_for_tests() -> BulkCtx<()> {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        BulkCtx::new("b", "item-1", (), test_delivery(&store))
    }

    #[tokio::test]
    async fn memoized_with_cached_cost_records_cost_on_compute_and_memo_hit() {
        let store = MemoStore::new(Arc::new(InMemory::new()), "memo");
        let first_ctx = BulkCtx::new("b", "item-1", (), test_delivery(&store));
        let replay_ctx = BulkCtx::new("b", "item-1", (), test_delivery(&store));
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
        let first_ctx = BulkCtx::new("b", "item-1", (), test_delivery(&store));
        let replay_ctx = BulkCtx::new("b", "item-1", (), test_delivery(&store));
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
