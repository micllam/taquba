//! The [`Pipeline`] contract and the per-item [`BulkCtx`] handed to it.

use std::collections::HashMap;
use std::future::Future;

use serde::Serialize;
use serde::de::DeserializeOwned;
use taquba_workflow::{Memo, StepError};
use tokio_util::sync::CancellationToken;

use crate::cost::CostReport;

/// Defines a per-item processing pipeline. Each bulk run executes one
/// `Pipeline` for every input item independently, materialised internally
/// as a [`taquba_workflow`] run.
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
/// use taquba_bulk::{BulkCtx, CostReport, Pipeline, StepError};
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
/// [memo](taquba_workflow::Memo), a [cost accumulator](CostReport), and the
/// run's cooperative [cancellation token](CancellationToken).
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
}

impl<T> BulkCtx<T> {
    pub(crate) fn new(
        input: T,
        run_id: String,
        headers: HashMap<String, String>,
        memo: Memo,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            input,
            run_id,
            headers,
            memo,
            cost: CostReport::new(),
            cancel_token,
        }
    }

    /// Run `f` once and cache its result durably under `key`, or return the
    /// previously cached result on a retry.
    ///
    /// On the first execution of a step, `f` runs and its `Ok` value is
    /// rmp-serialized into the item's [`Memo`] under `key` before being
    /// returned. If the step is later re-executed (at-least-once retry after
    /// a lease expiry), the cached bytes are returned without running `f`
    /// again, so a paid call inside `f` bills once, not once per attempt.
    /// An `Err` from `f` is never cached and propagates unchanged.
    ///
    /// `key` namespaces the cache within this item; use a distinct key per
    /// logical step. A cached entry that fails to deserialize (e.g. an
    /// output type changed shape between runs) is treated as a miss and `f`
    /// re-runs, overwriting it. Memo I/O failures surface as a transient
    /// [`StepError`]; serializing the computed value fails
    /// deterministically, so that surfaces as a permanent [`StepError`].
    /// Both are converted into the caller's error type.
    ///
    /// Calls to [`record_cost`](Self::record_cost) inside `f` run only on a
    /// cache miss. Use [`memoized_with_cached_cost`](Self::memoized_with_cached_cost)
    /// when cached results should also contribute to the final cost report.
    pub async fn memoized<R, F, E>(&self, key: &str, f: F) -> Result<R, E>
    where
        R: Serialize + DeserializeOwned,
        F: Future<Output = Result<R, E>>,
        E: From<StepError>,
    {
        match self.memo.get(key).await {
            Ok(Some(bytes)) => match rmp_serde::from_slice::<R>(&bytes) {
                Ok(value) => return Ok(value),
                Err(err) => {
                    // Self-healing: a cached entry we can't decode is treated
                    // as a miss so `f` recomputes and overwrites it.
                    tracing::warn!(
                        run_id = %self.run_id,
                        key = %key,
                        error = %err,
                        "memoized cache entry failed to deserialize; recomputing",
                    );
                }
            },
            Ok(None) => {}
            Err(err) => return Err(E::from(memo_error(err))),
        }

        let value = f.await?;
        // A serialization failure is deterministic, so a retry produces the
        // same error; fail permanently.
        let bytes = rmp_serde::to_vec_named(&value)
            .map_err(|e| E::from(StepError::permanent(format!("memo serialize failed: {e}"))))?;
        self.memo
            .put(key, &bytes)
            .await
            .map_err(|e| E::from(memo_error(e)))?;
        Ok(value)
    }

    /// Run `f` once and cache its result under a key derived from
    /// serialized `input`, or return the previously cached result on a
    /// retry.
    ///
    /// This has the same typed compute-on-miss behaviour as
    /// [`memoized`](Self::memoized), but the memo key is derived by
    /// serializing `input` as MessagePack and hashing it with SHA-256 via
    /// [`taquba_workflow::Memo::content_get`] and
    /// [`taquba_workflow::Memo::content_put`]. The entry remains scoped to
    /// this item's workflow run and step; this method does not create a
    /// cross-item cache.
    ///
    /// The derived key is stable only when `input` serializes
    /// deterministically. If several logical operations may receive the
    /// same input shape, include an operation name in the serialized input.
    pub async fn memoized_by_content<K, R, F, E>(&self, input: &K, f: F) -> Result<R, E>
    where
        K: Serialize + ?Sized,
        R: Serialize + DeserializeOwned,
        F: Future<Output = Result<R, E>>,
        E: From<StepError>,
    {
        match self.memo.content_get(input).await {
            Ok(Some(bytes)) => match rmp_serde::from_slice::<R>(&bytes) {
                Ok(value) => return Ok(value),
                Err(err) => {
                    tracing::warn!(
                        run_id = %self.run_id,
                        error = %err,
                        "content-addressed memoized cache entry failed to deserialize; recomputing",
                    );
                }
            },
            Ok(None) => {}
            Err(err) => return Err(E::from(memo_error(err))),
        }

        let value = f.await?;
        let bytes = rmp_serde::to_vec_named(&value)
            .map_err(|e| E::from(StepError::permanent(format!("memo serialize failed: {e}"))))?;
        self.memo
            .content_put(input, &bytes)
            .await
            .map_err(|e| E::from(memo_error(e)))?;
        Ok(value)
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
    /// [`ProgressSnapshot`](crate::ProgressSnapshot) and
    /// [`BulkReport`](crate::BulkReport).
    pub fn record_cost(&self, metric: &str, amount: f64) {
        self.cost.record(metric, amount);
    }

    /// The run's cooperative cancellation token. Watch it to short-circuit a
    /// long-running step when the bulk run is draining (e.g. on spot
    /// preemption); see [`taquba_workflow::Step::cancel_token`].
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }

    /// Snapshot of the cost accumulated so far for this item.
    pub(crate) fn cost(&self) -> CostReport {
        self.cost.clone()
    }
}

/// Map a workflow/object-store memo error to a [`StepError`], preserving the
/// transient/permanent classification.
fn memo_error(err: taquba_workflow::Error) -> StepError {
    err.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use taquba::object_store::memory::InMemory;
    use taquba_workflow::MemoStore;

    #[derive(Serialize)]
    struct ContentInput<'a> {
        operation: &'static str,
        payload: &'a [u8],
    }

    fn ctx_for_tests() -> BulkCtx<()> {
        let memo = MemoStore::new(Arc::new(InMemory::new()), "memo").new_memo("run-1", 0);
        BulkCtx::new(
            (),
            "run-1".into(),
            HashMap::new(),
            memo,
            CancellationToken::new(),
        )
    }

    #[tokio::test]
    async fn memoized_runs_once_then_serves_cache() {
        let ctx = ctx_for_tests();
        let calls = AtomicU32::new(0);
        let compute = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, StepError>(7u32)
        };

        let first = ctx.memoized("k", compute()).await.unwrap();
        let second = ctx.memoized("k", compute()).await.unwrap();
        assert_eq!(first, 7);
        assert_eq!(second, 7);
        assert_eq!(calls.load(Ordering::SeqCst), 1, "closure ran exactly once");
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
    async fn distinct_keys_cache_independently() {
        let ctx = ctx_for_tests();
        let a = ctx
            .memoized::<String, _, StepError>("a", async { Ok("first".to_string()) })
            .await
            .unwrap();
        let b = ctx
            .memoized::<String, _, StepError>("b", async { Ok("second".to_string()) })
            .await
            .unwrap();
        assert_eq!(a, "first");
        assert_eq!(b, "second");
    }

    #[tokio::test]
    async fn memoized_by_content_runs_once_then_serves_cache() {
        let ctx = ctx_for_tests();
        let calls = AtomicU32::new(0);
        let input = ContentInput {
            operation: "classify",
            payload: b"ticket",
        };
        let compute = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, StepError>("billing".to_string())
        };

        let first = ctx.memoized_by_content(&input, compute()).await.unwrap();
        let second = ctx.memoized_by_content(&input, compute()).await.unwrap();

        assert_eq!(first, "billing");
        assert_eq!(second, "billing");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "closure ran exactly once");
    }

    #[tokio::test]
    async fn memoized_by_content_distinguishes_serialized_inputs() {
        let ctx = ctx_for_tests();
        let classify = ContentInput {
            operation: "classify",
            payload: b"ticket",
        };
        let summarize = ContentInput {
            operation: "summarize",
            payload: b"ticket",
        };

        let first = ctx
            .memoized_by_content::<_, String, _, StepError>(&classify, async {
                Ok("class-a".to_string())
            })
            .await
            .unwrap();
        let second = ctx
            .memoized_by_content::<_, String, _, StepError>(&summarize, async {
                Ok("summary".to_string())
            })
            .await
            .unwrap();

        assert_eq!(first, "class-a");
        assert_eq!(second, "summary");
    }

    #[tokio::test]
    async fn memoized_with_cached_cost_records_cost_on_compute_and_memo_hit() {
        let memo = MemoStore::new(Arc::new(InMemory::new()), "memo").new_memo("run-1", 0);
        let first_ctx = BulkCtx::new(
            (),
            "run-1".into(),
            HashMap::new(),
            memo.clone(),
            CancellationToken::new(),
        );
        let replay_ctx = BulkCtx::new(
            (),
            "run-1".into(),
            HashMap::new(),
            memo,
            CancellationToken::new(),
        );
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
        let memo = MemoStore::new(Arc::new(InMemory::new()), "memo").new_memo("run-1", 0);
        let first_ctx = BulkCtx::new(
            (),
            "run-1".into(),
            HashMap::new(),
            memo.clone(),
            CancellationToken::new(),
        );
        let replay_ctx = BulkCtx::new(
            (),
            "run-1".into(),
            HashMap::new(),
            memo,
            CancellationToken::new(),
        );
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
