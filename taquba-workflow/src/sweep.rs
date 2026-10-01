//! Retention sweeps. A [`Sweep`] is the [`ExpiryIndex`] of the terminal markers
//! of one kind of entity (a run, a group), the window after an entity's marker
//! during which its state is retained and the store that removes one entity's
//! state ([`Clearable`]). A marker is an entry of the index with the id of the
//! entity as its suffix and an empty value. A pass removes each expired
//! entity's object-store state, then its KV state and its marker in one
//! transaction. A marker that its entity superseded is removed alone. The
//! removal of a run's state is unguarded by design: every consumer of a swept
//! entry tolerates its absence and re-executes the step.

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
    Pin<Box<dyn Future<Output = std::result::Result<Cleared, ClearError>> + Send + 'a>>;

/// The outcome of [`Clearable::clear`] for one marker.
pub(crate) enum Cleared {
    /// The object-store state of the entity is removed. The pass deletes these
    /// KV keys with the marker in one transaction.
    Removed(Vec<Vec<u8>>),
    /// The entity changed after the marker, so its state is retained and the
    /// pass deletes the marker alone.
    Superseded,
}

/// The store of one kind of entity's retained state, able to remove the state
/// of the entity a terminal marker names.
pub(crate) trait Clearable: Send + Sync + 'static {
    /// The store's own error, which a pass only logs.
    type Error: Into<ClearError>;

    /// Remove the state of the entity `id`, whose marker is dated
    /// `marked_at_ms`.
    fn clear(
        &self,
        id: &RunId,
        marked_at_ms: u64,
    ) -> impl Future<Output = std::result::Result<Cleared, Self::Error>> + Send;
}

/// [`Clearable`] with a boxed future, so sweeps over different stores share one
/// type.
trait DynClearable: Send + Sync {
    fn clear_dyn<'a>(&'a self, id: &'a RunId, marked_at_ms: u64) -> ClearFuture<'a>;
}

impl<C: Clearable> DynClearable for C {
    fn clear_dyn<'a>(&'a self, id: &'a RunId, marked_at_ms: u64) -> ClearFuture<'a> {
        Box::pin(async move { self.clear(id, marked_at_ms).await.map_err(Into::into) })
    }
}

/// One retention sweep: the index of the markers it reads, how long an entity
/// is retained after its marker and the store that removes an entity's state.
pub(crate) struct Sweep {
    index: ExpiryIndex,
    retention: Duration,
    store: Box<dyn DynClearable>,
}

impl Sweep {
    /// A sweep over the markers with `prefix`, clearing an entity from `store`
    /// once its marker is `retention` old.
    pub(crate) fn new(prefix: &'static [u8], retention: Duration, store: impl Clearable) -> Self {
        Self {
            index: ExpiryIndex::new(prefix),
            retention,
            store: Box::new(store),
        }
    }

    /// The key of the marker of the entity `id`, terminated at `at_ms`.
    #[cfg(test)]
    pub(crate) fn marker_key(&self, id: &RunId, at_ms: u64) -> Vec<u8> {
        self.index.entry_key(at_ms, id.as_str().as_bytes())
    }

    /// `effects` with the marker of the entity `id`, terminated at `at_ms`.
    pub(crate) fn mark(
        &self,
        effects: SettlementEffects,
        id: &RunId,
        at_ms: u64,
    ) -> SettlementEffects {
        effects.expiry_entry(&self.index, at_ms, id.as_str().as_bytes())
    }

    /// The sweep loop: the first pass runs immediately so a fresh process
    /// catches the markers that remain from an earlier one, then one pass every
    /// `interval` until `stop` is cancelled. A pass before a marker can be
    /// expired does not read the index. A failed pass is logged, and the next
    /// pass retries.
    pub(crate) async fn run(
        &self,
        queue: &Queue,
        clock: &dyn Clock,
        interval: Duration,
        stop: CancellationToken,
    ) {
        run_periodically(interval, &stop, (), |()| async move {
            if let Err(err) = self.pass(queue, clock).await {
                warn!("retention sweep failed: {err}");
            }
        })
        .await;
    }

    /// One pass: clear every entity whose marker is `retention` or more before
    /// the clock's current time, then remove the marker. Returns the number of
    /// markers removed. A marker whose suffix is not a run id, or that its
    /// entity superseded, is removed without clearing anything. A failure to
    /// clear one entity leaves its marker for a later pass, and the pass
    /// continues.
    pub(crate) async fn pass(&self, queue: &Queue, clock: &dyn Clock) -> Result<usize> {
        let now_ms = clock.now_ms();
        let store = &self.store;
        let removed = self
            .index
            .pass(queue, now_ms, self.retention, |at_ms, suffix| async move {
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
                match store.clear_dyn(&id, at_ms).await {
                    Ok(Cleared::Removed(kv_deletes)) => {
                        Expired::Delete(SettlementEffects::default().kv_deletes(kv_deletes))
                    }
                    Ok(Cleared::Superseded) => Expired::Delete(SettlementEffects::default()),
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

/// Run `pass` immediately and then once per `interval`, until `stop` is
/// cancelled. Each pass receives the state the previous one returned, `state`
/// for the first.
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
