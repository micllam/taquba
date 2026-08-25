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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::*;

    #[tokio::test]
    async fn offloaded_payload_round_trips_through_claim() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let payload = vec![7u8; 1024];
        let id = q.enqueue("work", payload.clone()).await.unwrap();
        assert_eq!(object_count(&store, "test-payloads").await, 1);

        let read = q.get_job(&id).await.unwrap().unwrap();
        assert!(read.payload_ref.is_some());
        assert_eq!(read.payload, payload);

        let job = q.claim("work", Duration::from_secs(30)).await.unwrap();
        let job = job.unwrap();
        assert!(job.payload_ref.is_some());
        assert_eq!(job.payload, payload);

        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn offloaded_payload_survives_nack_and_reclaim() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let payload = vec![3u8; 512];
        q.enqueue("work", payload.clone()).await.unwrap();

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.nack(&job, "retry").await.unwrap();
        assert_eq!(
            object_count(&store, "test-payloads").await,
            1,
            "a nack must not rewrite or remove the payload object"
        );

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.payload, payload);
        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    /// OpenOptions offloading to `payloads`, leaving the queue's own store
    /// healthy so only payload-object requests are subject to faults.
    fn faulty_payload_opts(payloads: Arc<dyn ObjectStore>) -> OpenOptions {
        OpenOptions {
            payload_store: Some(payloads),
            ..offload_opts()
        }
    }

    #[tokio::test]
    async fn enqueue_writes_no_record_when_the_payload_object_write_fails() {
        let payloads = FaultStore::wrap();
        let payload_store: Arc<dyn ObjectStore> = payloads.clone();
        let q = Queue::open_with_options(
            make_store(),
            "test",
            faulty_payload_opts(payload_store.clone()),
        )
        .await
        .unwrap();

        payloads.fail_puts(true);
        let err = q.enqueue("work", vec![9u8; 256]).await.unwrap_err();

        assert!(
            matches!(err, Error::PayloadStore(StoreError::Generic { store, .. }) if store == "FaultStore"),
            "unexpected error: {err:?}"
        );
        assert_eq!(
            q.stats("work").await.unwrap().pending,
            0,
            "a record must not be written when its payload object was not"
        );
        assert_eq!(object_count(&payload_store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn enqueue_batch_removes_earlier_payload_objects_when_a_later_write_fails() {
        let payloads = FaultStore::wrap();
        let payload_store: Arc<dyn ObjectStore> = payloads.clone();
        let q = Queue::open_with_options(
            make_store(),
            "test",
            faulty_payload_opts(payload_store.clone()),
        )
        .await
        .unwrap();

        // The third payload write fails, after two objects have been written.
        payloads.fail_puts_after(2);
        let err = q
            .enqueue_batch("work", vec![vec![1u8; 256], vec![2u8; 256], vec![3u8; 256]])
            .await
            .unwrap_err();

        assert!(
            matches!(err, Error::PayloadStore(StoreError::Generic { store, .. }) if store == "FaultStore"),
            "unexpected error: {err:?}"
        );
        assert_eq!(
            object_count(&payload_store, "test-payloads").await,
            0,
            "objects written before the failure must be removed, since no record points at them"
        );
        assert_eq!(q.stats("work").await.unwrap().pending, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn ack_settles_the_job_when_the_payload_object_delete_fails() {
        let payloads = FaultStore::wrap();
        let payload_store: Arc<dyn ObjectStore> = payloads.clone();
        let q = Queue::open_with_options(
            make_store(),
            "test",
            faulty_payload_opts(payload_store.clone()),
        )
        .await
        .unwrap();

        let id = q.enqueue("work", vec![4u8; 256]).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();

        payloads.fail_deletes(true);
        q.ack(&job).await.unwrap();

        assert!(
            q.get_job(&id).await.unwrap().is_none(),
            "the payload delete is best-effort and must not prevent settlement"
        );
        assert_eq!(
            object_count(&payload_store, "test-payloads").await,
            1,
            "a failed delete leaves an unreferenced object, the record having been removed first"
        );
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn dead_lettered_payload_survives_requeue_and_redelivery() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let payload = vec![8u8; 512];
        q.enqueue("work", payload.clone()).await.unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        q.dead_letter(&job, "permanent").await.unwrap();

        let dead = q.dead_jobs("work", None, 10).await.unwrap();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].payload, payload, "dead_jobs materializes payloads");

        q.requeue_dead_job(dead.into_iter().next().unwrap())
            .await
            .unwrap();
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.payload, payload);
        q.ack(&job).await.unwrap();
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_id_rejection_preserves_the_existing_payload_object() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let payload = vec![1u8; 256];
        let opts_with_id = || EnqueueOptions {
            id_override: Some("fixed-id".to_string()),
            ..EnqueueOptions::default()
        };
        q.enqueue_with("work", payload.clone(), opts_with_id())
            .await
            .unwrap();
        let err = q
            .enqueue_with("work", vec![2u8; 256], opts_with_id())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::DuplicateJobId { .. }));

        // The rejected enqueue's object is removed; the live job's object
        // is untouched and still readable.
        assert_eq!(object_count(&store, "test-payloads").await, 1);
        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.payload, payload);
        q.ack(&job).await.unwrap();
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn dedup_hit_removes_the_new_payload_object() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let opts_with_dedup = || EnqueueOptions {
            dedup_key: Some("once".to_string()),
            ..EnqueueOptions::default()
        };
        let first = q
            .enqueue_with("work", vec![1u8; 256], opts_with_dedup())
            .await
            .unwrap();
        let second = q
            .enqueue_with("work", vec![2u8; 256], opts_with_dedup())
            .await
            .unwrap();
        assert_eq!(first, second, "dedup returns the existing job's id");
        assert_eq!(object_count(&store, "test-payloads").await, 1);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn payloads_at_or_below_the_threshold_stay_inline() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let id = q.enqueue("work", vec![0u8; 64]).await.unwrap();
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        let job = q.get_job(&id).await.unwrap().unwrap();
        assert!(job.payload_ref.is_none());
        assert_eq!(job.payload.len(), 64);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn disabled_offload_keeps_large_payloads_inline() {
        let store = make_store();
        let opts = OpenOptions {
            payload_offload_threshold: None,
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(store.clone(), "test", opts)
            .await
            .unwrap();

        let id = q.enqueue("work", vec![0u8; 1024 * 1024]).await.unwrap();
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        let job = q.get_job(&id).await.unwrap().unwrap();
        assert!(job.payload_ref.is_none());
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancel_of_a_pending_job_deletes_the_payload_object() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let id = q.enqueue("work", vec![4u8; 256]).await.unwrap();
        assert_eq!(object_count(&store, "test-payloads").await, 1);
        assert_eq!(q.cancel(&id).await.unwrap(), CancelOutcome::Removed);
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn separate_payload_store_receives_the_payload_objects() {
        let queue_store = make_store();
        let payload_store = make_store();
        let opts = OpenOptions {
            payload_offload_threshold: Some(64),
            payload_store: Some(payload_store.clone()),
            payload_path: Some("blobs".to_string()),
            ..OpenOptions::default()
        };
        let q = Queue::open_with_options(queue_store.clone(), "test", opts)
            .await
            .unwrap();

        let payload = vec![6u8; 256];
        q.enqueue("work", payload.clone()).await.unwrap();
        assert_eq!(object_count(&payload_store, "blobs").await, 1);
        assert_eq!(object_count(&queue_store, "test-payloads").await, 0);

        let job = q
            .claim("work", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.payload, payload);
        q.ack(&job).await.unwrap();
        assert_eq!(object_count(&payload_store, "blobs").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn batch_enqueue_offloads_each_oversized_payload() {
        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let payloads = vec![vec![1u8; 256], vec![2u8; 16], vec![3u8; 256]];
        q.enqueue_batch("work", payloads.clone()).await.unwrap();
        assert_eq!(
            object_count(&store, "test-payloads").await,
            2,
            "only the two oversized payloads offload"
        );

        for expected in payloads {
            let job = q
                .claim("work", Duration::from_secs(30))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(job.payload, expected);
            q.ack(&job).await.unwrap();
        }
        assert_eq!(object_count(&store, "test-payloads").await, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn external_payload_deletion_surfaces_payload_missing() {
        use slatedb::object_store::ObjectStoreExt;

        let store = make_store();
        let q = Queue::open_with_options(store.clone(), "test", offload_opts())
            .await
            .unwrap();

        let id = q.enqueue("work", vec![1u8; 256]).await.unwrap();
        let objects = store
            .list_with_delimiter(Some(&slatedb::object_store::path::Path::from(
                "test-payloads",
            )))
            .await
            .unwrap()
            .objects;
        assert_eq!(objects.len(), 1);
        store.delete(&objects[0].location).await.unwrap();

        // The record is live, so the missing object is a real loss.
        let err = q.get_job(&id).await.unwrap_err();
        assert!(matches!(err, Error::PayloadMissing { id: ref e } if *e == id));

        q.close().await.unwrap();
    }
}
