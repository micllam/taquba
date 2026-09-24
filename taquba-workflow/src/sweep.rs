//! Retention sweeps. A [`Sweep`] is the [`ExpiryIndex`] of the terminal
//! markers of one kind of entity (a run, a group), the window after an
//! entity's marker during which its state is retained and the store
//! that removes one entity's state ([`Clearable`]). A marker is an
//! entry of the index with the id of the entity as its suffix and an
//! empty value. A pass removes each expired entity's object-store
//! state, then its KV state and its marker in one transaction.
//! Deletion is unguarded by design: every consumer of a swept entry
//! tolerates its absence and re-executes the step.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use taquba::{Clock, Expired, ExpiryIndex, Queue, SettlementEffects};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::error::Result;
use crate::keys::RunId;

type ClearError = Box<dyn std::error::Error + Send + Sync>;
type ClearFuture<'a> =
    Pin<Box<dyn Future<Output = std::result::Result<Vec<Vec<u8>>, ClearError>> + Send + 'a>>;

/// The store of one kind of entity's retained state, able to remove
/// the state of the entity a terminal marker names.
pub(crate) trait Clearable: Send + Sync + 'static {
    /// The store's own error; a pass only logs it.
    type Error: Into<ClearError>;

    /// Remove the object-store state of the entity `id` and return the
    /// KV keys of its remaining state, which the pass deletes with the
    /// entity's marker in one transaction.
    fn clear(
        &self,
        id: &RunId,
    ) -> impl Future<Output = std::result::Result<Vec<Vec<u8>>, Self::Error>> + Send;
}

/// [`Clearable`] behind a boxed future, so sweeps over different stores
/// share one type.
trait DynClearable: Send + Sync {
    fn clear_dyn<'a>(&'a self, id: &'a RunId) -> ClearFuture<'a>;
}

impl<C: Clearable> DynClearable for C {
    fn clear_dyn<'a>(&'a self, id: &'a RunId) -> ClearFuture<'a> {
        Box::pin(async move { self.clear(id).await.map_err(Into::into) })
    }
}

/// One retention sweep: the index of the markers it reads, how long an
/// entity is retained after its marker and the store that removes an
/// entity's state.
pub(crate) struct Sweep {
    index: ExpiryIndex,
    retention: Duration,
    store: Box<dyn DynClearable>,
}

impl Sweep {
    /// A sweep over the markers with `prefix`, clearing an entity from
    /// `store` once its marker is `retention` old.
    ///
    /// Panics if `retention < 1ms`: smaller values would turn the sweep
    /// loop into a hot spin.
    pub(crate) fn new(prefix: &'static [u8], retention: Duration, store: impl Clearable) -> Self {
        assert!(
            retention >= Duration::from_millis(1),
            "retention must be at least 1ms",
        );
        Self {
            index: ExpiryIndex::new(prefix),
            retention,
            store: Box::new(store),
        }
    }

    /// The key of the marker of the entity `id`, terminated at `at_ms`.
    /// The call lowers the time until which a pass returns without a
    /// read, so every marker is built through the sweep that reads it.
    pub(crate) fn marker_key(&self, id: &RunId, at_ms: u64) -> Vec<u8> {
        self.index.entry_key(at_ms, id.as_str().as_bytes())
    }

    /// The sweep loop: the first pass runs immediately so a fresh
    /// process catches markers left behind by an earlier one, then one
    /// pass every `retention` until `stop` is cancelled. A failed pass
    /// is logged; the next pass retries.
    pub(crate) async fn run(&self, queue: &Queue, clock: &dyn Clock, stop: CancellationToken) {
        run_periodically(self.retention, &stop, (), |()| async move {
            if let Err(err) = self.pass(queue, clock).await {
                warn!("retention sweep failed: {err}");
            }
        })
        .await;
    }

    /// One pass: clear every entity whose marker is `retention` or more
    /// before the clock's current time, then remove the marker. Returns
    /// the number of markers removed. A marker whose suffix is not a
    /// run id is removed without clearing anything. A failure to clear
    /// one entity leaves its marker for a later pass, and the pass
    /// continues.
    pub(crate) async fn pass(&self, queue: &Queue, clock: &dyn Clock) -> Result<usize> {
        let now_ms = clock.now_ms();
        let store = &self.store;
        let removed = self
            .index
            .pass(queue, now_ms, self.retention, |_, suffix| async move {
                let id = std::str::from_utf8(&suffix)
                    .ok()
                    .and_then(|id| RunId::new(id).ok());
                let Some(id) = id else {
                    warn!(
                        suffix = %String::from_utf8_lossy(&suffix),
                        "marker without a run id; deleting without clearing",
                    );
                    return Expired::Delete(SettlementEffects::default());
                };
                match store.clear_dyn(&id).await {
                    Ok(kv_deletes) => {
                        Expired::Delete(SettlementEffects::default().kv_deletes(kv_deletes))
                    }
                    Err(err) => {
                        warn!(id = %id, "clear failed during sweep: {err}");
                        Expired::Keep
                    }
                }
            })
            .await?;
        Ok(removed)
    }
}

/// Run `pass` immediately and then once per `interval`, until `stop`
/// is cancelled. Each pass receives the state the previous one
/// returned, `state` for the first.
pub(crate) async fn run_periodically<S, Fut>(
    interval: Duration,
    stop: &CancellationToken,
    mut state: S,
    mut pass: impl FnMut(S) -> Fut,
) where
    Fut: Future<Output = S>,
{
    loop {
        state = pass(state).await;
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }
    }
}
