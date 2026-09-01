//! The memo-retention sweep: a periodic pass that reads the terminal
//! markers oldest first, removes the memo entries of each run whose
//! marker is older than the retention window and then removes the
//! marker. Runs only when
//! [`WorkflowRuntimeBuilder::memo_retention`](crate::WorkflowRuntimeBuilder::memo_retention)
//! is set. Deletion is unguarded by design: every consumer of a swept
//! entry tolerates its absence and re-executes the step.

use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::error::Result;
use crate::keys::{TERMINAL_KV_PREFIX, parse_terminal_kv_key, validate_run_id};
use crate::runtime::RuntimeCore;

/// Terminal markers read per page by the memo-retention sweep.
const SWEEP_PAGE_SIZE: usize = 256;

impl RuntimeCore {
    /// Memo-retention sweep loop. Runs only when
    /// [`WorkflowRuntimeBuilder::memo_retention`] was set; the first
    /// tick fires immediately so a fresh runtime catches markers left
    /// behind by an earlier process, then ticks every `retention` until
    /// `shutdown` is cancelled.
    pub(crate) async fn run_memo_sweep(&self, shutdown: CancellationToken) {
        let Some(retention) = self.memo_retention else {
            return;
        };
        let mut ticker = tokio::time::interval(retention);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    if let Err(err) = self.sweep_expired_memos(retention).await {
                        warn!("memo retention sweep failed: {err}");
                    }
                }
            }
        }
    }

    /// One pass of memo retention: list the terminal markers older
    /// than `retention` and, for each, delete the run's memo entries
    /// and then the marker. Returns the number of runs whose memos
    /// were cleared. Errors on individual entries are logged and
    /// skipped (the next pass retries) so a transient failure on one
    /// marker doesn't stall the rest of the sweep.
    pub(crate) async fn sweep_expired_memos(&self, retention: Duration) -> Result<usize> {
        let now_ms = self.clock.now_ms();
        let retention_ms = retention.as_millis() as u64;
        let cutoff = now_ms.saturating_sub(retention_ms);
        let mut cleared = 0usize;
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let page = self
                .queue
                .kv_scan(TERMINAL_KV_PREFIX, cursor.as_deref(), SWEEP_PAGE_SIZE)
                .await?;
            let exhausted = page.next_cursor.is_none();
            cursor = page.next_cursor;
            for (key, _) in page.entries {
                let parsed = parse_terminal_kv_key(&key)
                    .filter(|(run_id, _)| validate_run_id(run_id).is_ok());
                let Some((run_id, terminal_at_ms)) = parsed else {
                    // An empty run id resolves to the memo prefix itself, so
                    // nothing is cleared under a marker that fails to parse.
                    warn!(
                        key = %String::from_utf8_lossy(&key),
                        "malformed terminal marker; deleting without clearing memos",
                    );
                    if let Err(err) = self.queue.kv_delete(&key).await {
                        warn!(
                            key = %String::from_utf8_lossy(&key),
                            "malformed terminal marker delete failed during sweep: {err}",
                        );
                    }
                    continue;
                };
                // Markers sort by timestamp, so the first unexpired one
                // ends the sweep: everything after it is newer.
                if terminal_at_ms >= cutoff {
                    return Ok(cleared);
                }
                if let Err(err) = self.memo_store.clear_memos_for_run(&run_id).await {
                    warn!(
                        run_id = %run_id,
                        "clear_memos_for_run failed during sweep: {err}",
                    );
                    continue;
                }
                // Memos first, marker second: a failure here leaves the
                // marker for the next pass, whose `clear_memos_for_run`
                // is a no-op on the now-empty run prefix.
                if let Err(err) = self.queue.kv_delete(&key).await {
                    warn!(
                        run_id = %run_id,
                        "terminal marker delete failed during sweep: {err}",
                    );
                    continue;
                }
                cleared += 1;
            }
            if exhausted {
                return Ok(cleared);
            }
        }
    }
}
