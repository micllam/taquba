//! The bulk deletion of objects in an object store.

use futures_util::{StreamExt, stream};
use slatedb::object_store::{Error as ObjectStoreError, ObjectStore, path::Path};

/// Delete the objects at `locations` through the bulk delete of `store`. A
/// missing object counts as deleted, and each failure is logged at debug level.
/// Returns the number of failed deletes with the first error, or `None` when
/// every delete succeeded.
pub(crate) async fn delete_objects(
    store: &dyn ObjectStore,
    locations: Vec<Path>,
) -> Option<(u64, ObjectStoreError)> {
    let locations = stream::iter(locations.into_iter().map(Ok)).boxed();
    let mut deleted = store.delete_stream(locations);
    let mut failed: Option<(u64, ObjectStoreError)> = None;
    while let Some(result) = deleted.next().await {
        match result {
            Ok(_) | Err(ObjectStoreError::NotFound { .. }) => {}
            Err(err) => {
                tracing::debug!(%err, "failed to delete object");
                match &mut failed {
                    Some((count, _)) => *count += 1,
                    None => failed = Some((1, err)),
                }
            }
        }
    }
    failed
}
