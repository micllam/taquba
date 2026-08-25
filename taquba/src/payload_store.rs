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
use ulid::Ulid;

use crate::error::{Error, Result};
use crate::job::JobRecord;

/// Storage for offloaded payload objects under a dedicated prefix.
///
/// If the payload store and the queue's SlateDB store share an object
/// store, the prefix must not overlap the path the queue was opened at,
/// so payload objects never collide with SlateDB's internal layout. The
/// default prefix (`"{path}-payloads"`) satisfies this.
pub(crate) struct PayloadStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    /// Payload size above which [`Self::offload`] offloads; `None`
    /// keeps every payload inline.
    offload_threshold: Option<usize>,
}

impl PayloadStore {
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        offload_threshold: Option<usize>,
    ) -> Self {
        Self {
            store,
            prefix,
            offload_threshold,
        }
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

    /// Offload `job`'s payload when it exceeds the threshold: the
    /// payload is written once as an object named by a new ULID,
    /// [`JobRecord::payload_ref`] is set and the inline payload is
    /// emptied. Runs before the transaction that writes the record, so
    /// a committed record never points at an unwritten object. The
    /// object name is unique to this call, so it cannot overwrite
    /// another job's object, including when a duplicate-id enqueue is
    /// later rejected.
    pub(crate) async fn offload(&self, job: &mut JobRecord) -> Result<()> {
        let Some(threshold) = self.offload_threshold else {
            return Ok(());
        };
        if job.payload.len() <= threshold {
            return Ok(());
        }
        let payload_ref = Ulid::new().to_string();
        self.put(&payload_ref, std::mem::take(&mut job.payload))
            .await?;
        job.payload_ref = Some(payload_ref);
        Ok(())
    }

    /// Offload every oversized payload in `jobs`, in order. On a
    /// failure the objects already written for earlier entries are
    /// deleted (no record points at them yet) and the error is
    /// returned.
    pub(crate) async fn offload_all<'a, I>(&self, jobs: I) -> Result<()>
    where
        I: IntoIterator<Item = &'a mut JobRecord>,
    {
        let mut jobs: Vec<&mut JobRecord> = jobs.into_iter().collect();
        for i in 0..jobs.len() {
            if let Err(err) = self.offload(&mut *jobs[i]).await {
                for job in &jobs[..i] {
                    self.delete_for(job).await;
                }
                return Err(err);
            }
        }
        Ok(())
    }

    /// Fetch an offloaded payload into `job.payload`. No-op for records
    /// whose payload is inline.
    pub(crate) async fn materialize(&self, job: &mut JobRecord) -> Result<()> {
        if let Some(ref payload_ref) = job.payload_ref {
            job.payload = self.get(payload_ref, &job.id).await?;
        }
        Ok(())
    }

    /// Delete `job`'s payload object if it has one, logging any failure.
    /// Called only after the transaction that removed (or declined to
    /// write) the record: deleting earlier could leave a live record
    /// whose payload is gone.
    pub(crate) async fn delete_for(&self, job: &JobRecord) {
        if let Some(ref payload_ref) = job.payload_ref {
            self.delete_best_effort(payload_ref, &job.id).await;
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
