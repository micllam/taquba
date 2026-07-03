//! Object-store storage for offloaded job payloads.
//!
//! Payloads larger than [`crate::OpenOptions::payload_offload_threshold`]
//! are written here as one object per payload instead of inline in the
//! job record. The record stores the object's name in
//! [`crate::JobRecord::payload_ref`]; the object name is a ULID
//! generated at enqueue, so no two enqueue attempts ever write the same
//! object and a rejected duplicate-id enqueue cannot overwrite a live
//! job's payload.
//!
//! Ordering invariant: the payload object is written before the
//! transaction that writes the record, and deleted only after the
//! transaction that removes the record has committed. A crash between
//! the two steps leaves an orphaned object, never a live record whose
//! payload is gone.

use std::sync::Arc;

use slatedb::object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt, path::Path};

use crate::error::{Error, Result};

/// Storage for offloaded payload objects under a dedicated prefix.
///
/// If the payload store and the queue's SlateDB store share an object
/// store, the prefix must not overlap the path the queue was opened at,
/// so payload objects never collide with SlateDB's internal layout. The
/// default prefix (`"{path}-payloads"`) satisfies this.
pub(crate) struct PayloadStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl PayloadStore {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self { store, prefix }
    }

    fn object_path(&self, payload_ref: &str) -> Path {
        Path::from(format!("{}/{}", self.prefix, payload_ref))
    }

    /// Write a payload object. Overwrites are impossible in practice
    /// because every offload generates a new ULID as the object name.
    pub(crate) async fn put(&self, payload_ref: &str, payload: Vec<u8>) -> Result<()> {
        self.store
            .put(&self.object_path(payload_ref), payload.into())
            .await?;
        Ok(())
    }

    /// Fetch a payload object. An absent object returns
    /// [`Error::PayloadMissing`] for `job_id`: the caller holds a record
    /// that points at the object, so absence is unrecoverable.
    pub(crate) async fn get(&self, payload_ref: &str, job_id: &str) -> Result<Vec<u8>> {
        match self.store.get(&self.object_path(payload_ref)).await {
            Ok(result) => Ok(result.bytes().await.map_err(Error::PayloadStore)?.to_vec()),
            Err(ObjectStoreError::NotFound { .. }) => Err(Error::PayloadMissing {
                id: job_id.to_string(),
            }),
            Err(err) => Err(Error::PayloadStore(err)),
        }
    }

    /// Delete a payload object. A missing object is treated as success
    /// so deletion retries after a partial failure complete cleanly.
    pub(crate) async fn delete(&self, payload_ref: &str) -> Result<()> {
        match self.store.delete(&self.object_path(payload_ref)).await {
            Ok(()) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
            Err(err) => Err(Error::PayloadStore(err)),
        }
    }

    /// Delete a payload object, logging instead of returning any
    /// failure. Used after a record-removing transaction has committed,
    /// where surfacing an error would misreport an operation that
    /// already took effect; the cost of a failed deletion is an
    /// orphaned object.
    pub(crate) async fn delete_best_effort(&self, payload_ref: &str, job_id: &str) {
        if let Err(err) = self.delete(payload_ref).await {
            tracing::warn!(
                job_id,
                payload_ref,
                "failed to delete payload object: {err}"
            );
        }
    }
}
