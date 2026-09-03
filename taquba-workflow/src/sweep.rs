//! Retention sweeps. A [`Sweep`] names the marker prefix of one kind of
//! entity (a run, a batch), the window after an entity's terminal marker
//! during which its state is retained and the store that removes one
//! entity's state ([`Clearable`]). Markers are `{prefix}{ts:020}/{id}`
//! keys in the caller KV namespace (see
//! [`crate::keys::timestamped_kv_key`]), so a prefix scan reads them
//! oldest first; a pass removes each expired entity's state and then
//! its marker, and stops at the first unexpired marker.
//! Deletion is unguarded by design: every consumer of a swept entry
//! tolerates its absence and re-executes the step.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use futures_util::TryStreamExt;
use taquba::{Clock, Queue};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::error::Result;
use crate::keys::{parse_timestamped_kv_key, validate_run_id};

/// Terminal markers read per page by a sweep pass.
const SWEEP_PAGE_SIZE: usize = 256;

type ClearError = Box<dyn std::error::Error + Send + Sync>;
type ClearFuture<'a> =
    Pin<Box<dyn Future<Output = std::result::Result<(), ClearError>> + Send + 'a>>;

/// The store of one kind of entity's retained state, able to remove
/// the state of the entity a terminal marker names.
pub(crate) trait Clearable: Send + Sync + 'static {
    /// The store's own error; a pass only logs it.
    type Error: Into<ClearError>;

    /// Remove the state of the entity `id`.
    fn clear(&self, id: &str) -> impl Future<Output = std::result::Result<(), Self::Error>> + Send;
}

/// [`Clearable`] behind a boxed future, so sweeps over different stores
/// share one type.
trait DynClearable: Send + Sync {
    fn clear_dyn<'a>(&'a self, id: &'a str) -> ClearFuture<'a>;
}

impl<C: Clearable> DynClearable for C {
    fn clear_dyn<'a>(&'a self, id: &'a str) -> ClearFuture<'a> {
        Box::pin(async move { self.clear(id).await.map_err(Into::into) })
    }
}

/// One retention sweep: which markers it reads, how long an entity is
/// retained after its marker and the store that removes an entity's
/// state.
pub(crate) struct Sweep {
    prefix: &'static [u8],
    retention: Duration,
    store: Box<dyn DynClearable>,
}

impl Sweep {
    /// A sweep over the markers under `prefix`, clearing an entity from
    /// `store` once its marker is older than `retention`.
    ///
    /// Panics if `retention < 1ms`: smaller values would turn the sweep
    /// loop into a hot spin.
    pub(crate) fn new(prefix: &'static [u8], retention: Duration, store: impl Clearable) -> Self {
        assert!(
            retention >= Duration::from_millis(1),
            "retention must be at least 1ms",
        );
        Self {
            prefix,
            retention,
            store: Box::new(store),
        }
    }

    /// The sweep loop: the first pass runs immediately so a fresh
    /// process catches markers left behind by an earlier one, then one
    /// pass every `retention` until `stop` is cancelled. A failed pass
    /// is logged; the next pass retries.
    pub(crate) async fn run(&self, queue: &Queue, clock: &dyn Clock, stop: CancellationToken) {
        run_periodically(self.retention, &stop, (), |()| async move {
            if let Err(err) = self.pass(queue, clock).await {
                warn!(prefix = %String::from_utf8_lossy(self.prefix), "retention sweep failed: {err}");
            }
        })
        .await;
    }

    /// One pass: clear every entity whose marker is more than `retention`
    /// before the clock's current time, then remove the marker. Returns
    /// the number of entities cleared. A malformed marker, or one whose
    /// id is not a valid run id, is deleted without clearing anything; a
    /// failure to clear one entity leaves its marker for the next pass,
    /// and the pass continues.
    pub(crate) async fn pass(&self, queue: &Queue, clock: &dyn Clock) -> Result<usize> {
        let cutoff_ms = clock
            .now_ms()
            .saturating_sub(self.retention.as_millis() as u64);
        let mut cleared = 0usize;
        let mut markers = std::pin::pin!(queue.kv_entries(self.prefix, SWEEP_PAGE_SIZE));
        while let Some((key, _)) = markers.try_next().await? {
            let parsed = parse_timestamped_kv_key(self.prefix, &key)
                .filter(|(id, _)| validate_run_id(id).is_ok());
            let Some((id, ts_ms)) = parsed else {
                warn!(
                    key = %String::from_utf8_lossy(&key),
                    "malformed marker; deleting without clearing",
                );
                if let Err(err) = queue.kv_delete(&key).await {
                    warn!(
                        key = %String::from_utf8_lossy(&key),
                        "malformed marker delete failed during sweep: {err}",
                    );
                }
                continue;
            };
            if ts_ms >= cutoff_ms {
                break;
            }
            if let Err(err) = self.store.clear_dyn(&id).await {
                warn!(id = %id, "clear failed during sweep: {err}");
                continue;
            }
            if let Err(err) = queue.kv_delete(&key).await {
                warn!(id = %id, "marker delete failed during sweep: {err}");
                continue;
            }
            cleared += 1;
        }
        Ok(cleared)
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
