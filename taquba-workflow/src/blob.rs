//! The objects under one prefix of an object store, read and written
//! with the absence tolerance every store of this crate shares: a
//! missing object reads as `None` and deletes as already gone, because
//! the retention sweep may remove any object at any time and every
//! reader re-executes on absence.

use std::sync::Arc;

use futures_util::stream::BoxStream;
use taquba::object_store::{
    Error as ObjectStoreError, ObjectMeta, ObjectStore, ObjectStoreExt, path::Path,
};

use crate::error::Result;

/// An object store and a path prefix the objects live under.
#[derive(Clone)]
pub(crate) struct ObjectPrefix {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ObjectPrefix {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
        }
    }

    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The path `<prefix>/<suffix>`.
    pub(crate) fn path(&self, suffix: &str) -> Path {
        Path::from(format!("{}/{suffix}", self.prefix))
    }

    /// The object at `path`, or `Ok(None)` when none exists.
    pub(crate) async fn get(&self, path: &Path) -> Result<Option<Vec<u8>>> {
        match self.store.get(path).await {
            Ok(result) => Ok(Some(result.bytes().await?.to_vec())),
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Write `value` at `path`, overwriting any prior object.
    pub(crate) async fn put(&self, path: &Path, value: &[u8]) -> Result<()> {
        self.store.put(path, value.to_vec().into()).await?;
        Ok(())
    }

    /// Delete the object at `path`; `Ok(false)` when none existed.
    pub(crate) async fn delete(&self, path: &Path) -> Result<bool> {
        match self.store.delete(path).await {
            Ok(()) => Ok(true),
            Err(ObjectStoreError::NotFound { .. }) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    /// Every object under `prefix`.
    pub(crate) fn list(
        &self,
        prefix: &Path,
    ) -> BoxStream<'static, std::result::Result<ObjectMeta, ObjectStoreError>> {
        self.store.list(Some(prefix))
    }
}
