use std::sync::Arc;

use serde::{Deserialize, Serialize};
use taquba::object_store::{Error as ObjectStoreError, ObjectStore, path::Path};

use crate::error::{Error, Result};
use crate::job::ErrorKind;

/// A job's terminal outcome, as persisted to object storage.
///
/// Written by the dispatch worker the moment a job reaches a terminal state,
/// so the result survives the worker process and can be retrieved later via
/// [`JobHandle`](crate::JobHandle).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum StoredOutcome {
    /// The job ran successfully; `output` is the serialized `Job::Output`.
    Success { output: Vec<u8> },
    /// The job reached a terminal failure (classified permanent, or exhausted
    /// its retry budget).
    Failure {
        kind: StoredErrorKind,
        message: String,
    },
}

/// Serializable mirror of [`ErrorKind`] for the persisted outcome.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum StoredErrorKind {
    Transient,
    Permanent,
}

impl From<ErrorKind> for StoredErrorKind {
    fn from(kind: ErrorKind) -> Self {
        match kind {
            ErrorKind::Transient => Self::Transient,
            ErrorKind::Permanent => Self::Permanent,
        }
    }
}

impl From<StoredErrorKind> for ErrorKind {
    fn from(kind: StoredErrorKind) -> Self {
        match kind {
            StoredErrorKind::Transient => Self::Transient,
            StoredErrorKind::Permanent => Self::Permanent,
        }
    }
}

/// Persists and retrieves job result blobs in a caller-provided object store.
///
/// Blobs live under a dedicated prefix (default `"{queue_name}-results"`). If
/// the result store and the queue's SlateDB store share an object store, this
/// prefix must not overlap the `path` the queue was opened at, so result
/// blobs never collide with SlateDB's internal layout.
#[derive(Clone)]
pub(crate) struct ResultStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ResultStore {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self { store, prefix }
    }

    fn key(&self, job_id: &str) -> Path {
        Path::from(format!("{}/{}", self.prefix, job_id))
    }

    /// Persist a job's terminal outcome. Overwrites any prior outcome for the
    /// same job ID: under at-least-once delivery the same job may run more
    /// than once (e.g. after a lease expiry or an ack that didn't reach the
    /// queue), and each terminal attempt writes its own blob. A handler that
    /// isn't perfectly idempotent can therefore have an earlier "successful"
    /// blob replaced by a later attempt's outcome. `Job::run` is the
    /// contract surface for ensuring that's safe.
    pub(crate) async fn put(&self, job_id: &str, outcome: &StoredOutcome) -> Result<()> {
        let bytes = rmp_serde::to_vec_named(outcome)?;
        self.store.put(&self.key(job_id), bytes.into()).await?;
        Ok(())
    }

    /// Read a job's persisted outcome, or `None` if none has been written yet.
    pub(crate) async fn get(&self, job_id: &str) -> Result<Option<StoredOutcome>> {
        match self.store.get(&self.key(job_id)).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                Ok(Some(rmp_serde::from_slice(&bytes)?))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(err) => Err(Error::Store(err)),
        }
    }
}
