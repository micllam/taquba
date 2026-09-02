//! The runtime's reserved namespaces: the `workflow.` header keys, the
//! `workflow/` prefix in the caller KV namespace with the builders for
//! the durable keys under it, the step dedup-key prefix and run-id
//! validation.

use crate::error::{Error, Result};

/// Header key carrying the run identifier on every step job.
pub const HEADER_RUN_ID: &str = "workflow.run_id";
/// Header key carrying the zero-based step number on every step job.
pub const HEADER_STEP: &str = "workflow.step";
/// Reserved prefix the runtime owns on step-job headers. Submitter-supplied
/// headers must not start with this prefix; if they do, the runtime treats
/// them as its own and strips them before invoking the runner.
pub const RESERVED_HEADER_PREFIX: &str = "workflow.";

/// Reserved prefix the runtime owns in the caller KV namespace. Keys
/// passed via [`RunSpec::kv_writes`](crate::RunSpec::kv_writes) or staged through an
/// [`crate::EffectsHandle`] must not start with this prefix; they are
/// rejected with [`Error::ReservedKvKey`].
pub const RESERVED_KV_PREFIX: &str = "workflow/";

/// Header key marking a job as a terminal-notification job, whose
/// payload is the run's committed outcome and whose worker is the
/// configured [`TerminalHook`](crate::TerminalHook).
pub const HEADER_TERMINAL: &str = "workflow.terminal";

/// Header key marking a step job as a signal waiter; the value is the
/// correlation key the waiter is registered under.
pub const HEADER_SIGNAL_WAIT: &str = "workflow.signal_wait";
/// Header key marking a step job whose signal was already consumed at the
/// previous step's settlement; the payload is read from the durable
/// delivered record.
pub const HEADER_SIGNAL_DELIVERED: &str = "workflow.signal_delivered";

pub(crate) const DEDUP_PREFIX: &str = "run:";

/// Maximum length of a caller-supplied [`RunSpec::run_id`](crate::RunSpec::run_id), matching the
/// limit Taquba applies to a caller-supplied job id.
pub const MAX_RUN_ID_LEN: usize = 128;

/// Prefix for the durable per-run record in Taquba's user KV namespace.
pub(crate) const RUN_KV_PREFIX: &[u8] = b"workflow/runs/";

/// Prefix for the durable waiter index: `correlation key -> job id` of the
/// step job waiting on that key.
pub(crate) const SIGNAL_WAIT_KV_PREFIX: &[u8] = b"workflow/signal-wait/";

/// Prefix for the durable signal buffer: `correlation key -> payload` of a
/// signal that arrived while no waiter was registered.
pub(crate) const SIGNAL_BUF_KV_PREFIX: &[u8] = b"workflow/signal-buf/";

/// Prefix for the durable delivered record: `(run id, step) -> payload` of
/// a signal consumed on the waiter's behalf, read when that step is
/// claimed and deleted with its settlement.
pub(crate) const SIGNAL_DELIVERED_KV_PREFIX: &[u8] = b"workflow/signal-delivered/";

/// Prefix for the durable terminal marker: the time-ordered index of
/// runs that have reached a terminal state, read by the memo-retention
/// sweep. Written only when [`WorkflowRuntimeBuilder::memo_retention`]
/// is set, in the same transaction that settles the run.
pub(crate) const TERMINAL_KV_PREFIX: &[u8] = b"workflow/terminals/";

/// Prefix for the durable per-item markers of bulk batches:
/// `workflow/bulk/batches/{batch_id}/items/{key}`, one per terminated
/// item, written in the settlement that commits the item's terminal
/// outcome.
pub(crate) const BULK_KV_PREFIX: &[u8] = b"workflow/bulk/batches/";

/// Prefix under which the markers of one batch's items are stored.
pub(crate) fn bulk_items_kv_prefix(batch_id: &str) -> Vec<u8> {
    let mut k = Vec::from(BULK_KV_PREFIX);
    k.extend_from_slice(batch_id.as_bytes());
    k.extend_from_slice(b"/items/");
    k
}

/// Key of the marker of item `key` in batch `batch_id`.
pub(crate) fn bulk_item_kv_key(batch_id: &str, key: &str) -> Vec<u8> {
    let mut k = bulk_items_kv_prefix(batch_id);
    k.extend_from_slice(key.as_bytes());
    k
}

/// Key of the terminal marker for `run_id`, terminated at
/// `terminal_at_ms`. The zero-padded timestamp leads the suffix, so a
/// prefix scan returns markers oldest first and the sweep's expired set
/// is the front of the range. The value is empty: both fields are in
/// the key.
pub(crate) fn terminal_kv_key(run_id: &str, terminal_at_ms: u64) -> Vec<u8> {
    timestamped_kv_key(TERMINAL_KV_PREFIX, run_id, terminal_at_ms)
}

/// Prefix for the durable terminal markers of bulk batches, read by the
/// batch retention sweep: `workflow/bulk/terminals/{ts:020}/{batch_id}`.
pub(crate) const BULK_TERMINAL_KV_PREFIX: &[u8] = b"workflow/bulk/terminals/";

/// Key of the terminal marker of batch `batch_id`, completed at
/// `terminal_at_ms`.
pub(crate) fn bulk_terminal_kv_key(batch_id: &str, terminal_at_ms: u64) -> Vec<u8> {
    timestamped_kv_key(BULK_TERMINAL_KV_PREFIX, batch_id, terminal_at_ms)
}

/// `{prefix}{ts:020}/{id}`: a marker whose zero-padded timestamp leads the
/// suffix, so a prefix scan returns markers oldest first.
pub(crate) fn timestamped_kv_key(prefix: &[u8], id: &str, ts_ms: u64) -> Vec<u8> {
    let mut k = Vec::from(prefix);
    k.extend_from_slice(format!("{ts_ms:020}/").as_bytes());
    k.extend_from_slice(id.as_bytes());
    k
}

/// The `(id, ts_ms)` of a key built by [`timestamped_kv_key`].
pub(crate) fn parse_timestamped_kv_key(prefix: &[u8], key: &[u8]) -> Option<(String, u64)> {
    let suffix = key.strip_prefix(prefix)?;
    let text = std::str::from_utf8(suffix).ok()?;
    let (ts, id) = text.split_once('/')?;
    Some((id.to_string(), ts.parse().ok()?))
}

/// The SHA-256 digest of `input`.
pub(crate) fn hash_input(input: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(input).into()
}

/// The lowercase hex SHA-256 digest of `parts` concatenated.
pub(crate) fn hex_sha256(parts: &[&[u8]]) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write;
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    let mut hex = String::with_capacity(64);
    for byte in hasher.finalize() {
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

/// Validate a caller-supplied run id. The run id becomes an object-store
/// path segment under the memo prefix and a key segment in the queue's
/// key-value namespace, so it is restricted to the same 1 to
/// [`MAX_RUN_ID_LEN`] bytes of `[A-Za-z0-9_-]` that Taquba requires of a
/// caller-supplied job id. An empty run id would resolve to the memo
/// prefix itself, whose entries the retention sweep would then remove
/// for every run.
pub(crate) fn validate_run_id(run_id: &str) -> Result<()> {
    let reason = if run_id.is_empty() {
        "run id must not be empty"
    } else if run_id.len() > MAX_RUN_ID_LEN {
        "run id exceeds maximum length of 128 bytes"
    } else if !run_id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        "run id must contain only `[A-Za-z0-9_-]`"
    } else {
        return Ok(());
    };
    Err(Error::InvalidRunId {
        run_id: run_id.to_string(),
        reason,
    })
}

pub(crate) fn run_kv_key(run_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(RUN_KV_PREFIX.len() + run_id.len());
    k.extend_from_slice(RUN_KV_PREFIX);
    k.extend_from_slice(run_id.as_bytes());
    k
}

pub(crate) fn signal_wait_kv_key(correlation_key: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(SIGNAL_WAIT_KV_PREFIX.len() + correlation_key.len());
    k.extend_from_slice(SIGNAL_WAIT_KV_PREFIX);
    k.extend_from_slice(correlation_key.as_bytes());
    k
}

pub(crate) fn signal_buf_kv_key(correlation_key: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(SIGNAL_BUF_KV_PREFIX.len() + correlation_key.len());
    k.extend_from_slice(SIGNAL_BUF_KV_PREFIX);
    k.extend_from_slice(correlation_key.as_bytes());
    k
}

pub(crate) fn signal_delivered_kv_key(run_id: &str, step_number: u32) -> Vec<u8> {
    let mut k = Vec::from(SIGNAL_DELIVERED_KV_PREFIX);
    k.extend_from_slice(run_id.as_bytes());
    k.push(b'/');
    k.extend_from_slice(step_number.to_string().as_bytes());
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_kv_prefixes_are_under_the_reserved_prefix() {
        for prefix in [
            RUN_KV_PREFIX,
            SIGNAL_WAIT_KV_PREFIX,
            SIGNAL_BUF_KV_PREFIX,
            SIGNAL_DELIVERED_KV_PREFIX,
            TERMINAL_KV_PREFIX,
            BULK_KV_PREFIX,
            BULK_TERMINAL_KV_PREFIX,
        ] {
            assert!(
                prefix.starts_with(RESERVED_KV_PREFIX.as_bytes()),
                "internal kv prefix `{}` is outside the reserved prefix",
                String::from_utf8_lossy(prefix),
            );
        }
    }
}
