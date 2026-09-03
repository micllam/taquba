//! Retention sweeps. A [`Sweep`] names the marker prefix of one kind of
//! entity (a run, a batch), the window after an entity's terminal marker
//! during which its state is retained and the removal of one entity's
//! state. Markers are `{prefix}{ts:020}/{id}` keys in the caller KV
//! namespace (see [`crate::keys::timestamped_kv_key`]), so a prefix scan
//! reads them oldest first; a pass removes each expired entity's state
//! and then its marker, and stops at the first unexpired marker.
//! Deletion is unguarded by design: every consumer of a swept entry
//! tolerates its absence and re-executes the step.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::TryStreamExt;
use taquba::{Clock, Queue};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::error::Result;
use crate::keys::{parse_timestamped_kv_key, validate_run_id};
use crate::paging::kv_entries;

/// Terminal markers read per page by a sweep pass.
const SWEEP_PAGE_SIZE: usize = 256;

type ClearError = Box<dyn std::error::Error + Send + Sync>;
type ClearFuture = Pin<Box<dyn Future<Output = std::result::Result<(), ClearError>> + Send>>;
type ClearFn = Arc<dyn Fn(String) -> ClearFuture + Send + Sync>;

/// One retention sweep: which markers it reads, how long an entity is
/// retained after its marker and how an entity's state is removed.
pub(crate) struct Sweep {
    prefix: &'static [u8],
    retention: Duration,
    clear: ClearFn,
}

impl Sweep {
    /// A sweep over the markers under `prefix`, removing an entity's
    /// state with `clear` once its marker is older than `retention`.
    ///
    /// Panics if `retention < 1ms`: smaller values would turn the sweep
    /// loop into a hot spin.
    pub(crate) fn new<F, Fut, E>(prefix: &'static [u8], retention: Duration, clear: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), E>> + Send + 'static,
        E: Into<ClearError>,
    {
        assert!(
            retention >= Duration::from_millis(1),
            "retention must be at least 1ms",
        );
        Self {
            prefix,
            retention,
            clear: Arc::new(move |id| {
                let fut = clear(id);
                Box::pin(async move { fut.await.map_err(Into::into) })
            }),
        }
    }

    /// The sweep loop: the first pass runs immediately so a fresh
    /// process catches markers left behind by an earlier one, then one
    /// pass every `retention` until `stop` is cancelled. A failed pass
    /// is logged; the next pass retries.
    pub(crate) async fn run(&self, queue: &Queue, clock: &dyn Clock, stop: CancellationToken) {
        let mut ticker = tokio::time::interval(self.retention);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = ticker.tick() => {
                    if let Err(err) = self.pass(queue, clock).await {
                        warn!(prefix = %String::from_utf8_lossy(self.prefix), "retention sweep failed: {err}");
                    }
                }
            }
        }
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
        let mut markers = std::pin::pin!(kv_entries(queue, self.prefix, SWEEP_PAGE_SIZE));
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
            if let Err(err) = (self.clear)(id.clone()).await {
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
