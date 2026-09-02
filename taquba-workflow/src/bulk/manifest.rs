//! The batch manifest: the keys and serialized inputs of a batch, written
//! before any item is submitted and read back by a resume.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use taquba::object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt, path::Path};

use crate::Result;

/// The items of one batch, in submission order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Manifest {
    pub(crate) batch_id: String,
    pub(crate) items: Vec<ManifestItem>,
}

/// One item: its key and its serialized input, the run payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ManifestItem {
    pub(crate) key: String,
    pub(crate) input: Vec<u8>,
}

/// Reads and writes manifests under `<prefix>/batches/<batch_id>/manifest`.
#[derive(Clone)]
pub(crate) struct ManifestStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ManifestStore {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
        }
    }

    fn path(&self, batch_id: &str) -> Path {
        Path::from(format!("{}/batches/{batch_id}/manifest", self.prefix))
    }

    pub(crate) async fn read(&self, batch_id: &str) -> Result<Option<Manifest>> {
        match self.store.get(&self.path(batch_id)).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                Ok(Some(rmp_serde::from_slice(&bytes)?))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    pub(crate) async fn delete(&self, batch_id: &str) -> Result<()> {
        match self.store.delete(&self.path(batch_id)).await {
            Ok(()) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub(crate) async fn write(&self, manifest: &Manifest) -> Result<()> {
        let bytes = rmp_serde::to_vec_named(manifest)?;
        self.store
            .put(&self.path(&manifest.batch_id), bytes.into())
            .await?;
        Ok(())
    }
}
