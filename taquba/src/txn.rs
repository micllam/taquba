//! Transaction helpers shared by the queue, reaper and scheduler.
use slatedb::DbTransaction;

use crate::error::{Error, Result};

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

/// Verify the claim at `claimed_key` still exists and delete it,
/// returning [`Error::ClaimLost`] if it does not.
///
/// This is the fence every settlement passes through. The claim key
/// embeds the lease expiry, so a reaper requeue or a lease renewal
/// invalidates the key and the settlement is rejected rather than
/// applied to a delivery the caller no longer owns.
pub(crate) async fn take_claim(txn: &DbTransaction, claimed_key: &[u8]) -> Result<()> {
    txn.get(claimed_key).await?.ok_or(Error::ClaimLost)?;
    txn.delete(claimed_key)?;
    Ok(())
}
