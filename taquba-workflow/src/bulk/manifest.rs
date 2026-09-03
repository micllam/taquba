//! The batch manifest: the keys and serialized inputs of a batch, written
//! before any item is submitted and read back by a resume.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use taquba::object_store::{ObjectStore, path::Path};

use crate::Result;
use crate::blob::ObjectPrefix;

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
    objects: ObjectPrefix,
}

impl ManifestStore {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            objects: ObjectPrefix::new(store, prefix),
        }
    }

    fn path(&self, batch_id: &str) -> Path {
        self.objects.path(&format!("batches/{batch_id}/manifest"))
    }

    pub(crate) async fn read(&self, batch_id: &str) -> Result<Option<Manifest>> {
        match self.objects.get(&self.path(batch_id)).await? {
            Some(bytes) => Ok(Some(rmp_serde::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    pub(crate) async fn delete(&self, batch_id: &str) -> Result<()> {
        self.objects.delete(&self.path(batch_id)).await.map(|_| ())
    }

    pub(crate) async fn write(&self, manifest: &Manifest) -> Result<()> {
        let bytes = rmp_serde::to_vec_named(manifest)?;
        self.objects
            .put(&self.path(&manifest.batch_id), &bytes)
            .await
    }
}
