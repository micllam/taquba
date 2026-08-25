//! Transaction helpers shared by the queue, reaper and scheduler.
use slatedb::DbTransaction;

use crate::error::{Error, Result};
use crate::job::JobRecord;
use crate::keys::claimed_key;
use crate::lease_registry::LeaseRegistry;

/// Write a job record at `key` and repoint its index entry at the same
/// key.
///
/// `index_key` must be [`crate::keys::job_index_key`] of the record's
/// job id. `value` is the serialized record; a record carrying a
/// materialized payload must be serialized with
/// [`JobRecord::stored_bytes`](crate::JobRecord::stored_bytes).
pub(crate) fn put_job_record(
    txn: &DbTransaction,
    key: &[u8],
    index_key: &[u8],
    value: &[u8],
) -> Result<()> {
    txn.put(key, value)?;
    txn.put(index_key, key)?;
    Ok(())
}

/// Verify that the job still holds the claim `token` identifies, stage
/// the deletion of its record and return the stored record. Returns
/// [`Error::ClaimLost`] when the claim has ended.
///
/// This is the fence every settlement passes through, in three parts.
/// The registry token check rejects a settlement superseded by a
/// re-claim. The in-transaction record read rejects a settlement whose
/// claim ended while its registry entry, removed only after the ending
/// commit, was still present. The staged delete makes a settlement
/// racing a concurrent requeue or re-claim a transaction conflict.
/// Call it inside the retry loop so a retry re-runs both checks. A
/// renewal changes neither the token nor the record, so a claim held
/// across one still settles.
///
/// A settlement that writes a record must base it on the returned
/// record, which includes changes committed during the claim (a
/// cancel's `cancel_requested` flag); the claim's own copy predates
/// them.
pub(crate) async fn take_claim(
    txn: &DbTransaction,
    registry: &LeaseRegistry,
    queue: &str,
    id: &str,
    token: u64,
) -> Result<JobRecord> {
    match registry.current(queue, id) {
        Some((_, current)) if current == token => {}
        _ => return Err(Error::ClaimLost),
    }
    let key = claimed_key(queue, id);
    let Some(raw) = txn.get(&key).await? else {
        return Err(Error::ClaimLost);
    };
    let job = JobRecord::decode(&key, &raw)?;
    txn.delete(&key)?;
    Ok(job)
}
