use std::sync::Arc;

use futures_util::StreamExt;
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
///
/// # Layout
///
/// Two sibling key spaces share the prefix:
///
/// - `<prefix>/<job_id>`: outcome blobs written by [`ResultStore::put`].
///   Job ids are ULIDs, so they cannot collide with the reserved
///   `terminals` segment below.
/// - `<prefix>/terminals/<(u64::MAX - terminal_at_ms):020>_<job_id>`:
///   zero-byte markers written by
///   [`ResultStore::write_terminal_marker`]. The leading zero-padded
///   *inverted* millisecond timestamp sorts markers newest-first, so
///   every marker older than a cutoff sorts after the cutoff's key and
///   [`ResultStore::list_expired_terminal_markers`] can reach the
///   expired set through `list_with_offset`, whose key-greater-than
///   filter is part of the object-store contract (list *order* is
///   not). The sweep's listing cost is therefore proportional to the
///   number of expired markers, not the total retained.
#[derive(Clone)]
pub(crate) struct ResultStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

/// A terminal marker as returned by
/// [`ResultStore::list_expired_terminal_markers`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TerminalMarker {
    pub(crate) job_id: String,
    pub(crate) terminal_at_ms: u64,
}

impl ResultStore {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self { store, prefix }
    }

    fn key(&self, job_id: &str) -> Path {
        Path::from(format!("{}/{}", self.prefix, job_id))
    }

    fn terminals_prefix(&self) -> Path {
        Path::from(format!("{}/terminals", self.prefix))
    }

    fn terminal_marker_path(&self, job_id: &str, terminal_at_ms: u64) -> Path {
        self.terminals_prefix()
            .child(format!("{:020}_{job_id}", invert_ts(terminal_at_ms)))
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

    /// Delete a job's persisted outcome. A missing blob is treated as
    /// success so a crash-and-retry sweeper can re-run cleanly.
    pub(crate) async fn delete(&self, job_id: &str) -> Result<()> {
        match self.store.delete(&self.key(job_id)).await {
            Ok(()) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
            Err(err) => Err(Error::Store(err)),
        }
    }

    /// Write a terminal marker for `job_id` at `terminal_at_ms`. The
    /// marker is a zero-byte object whose path encodes both fields so a
    /// sweeper can decide retention without reading any content.
    /// Idempotent: a second call with the same `(job_id, terminal_at_ms)`
    /// overwrites the empty value with another empty value.
    pub(crate) async fn write_terminal_marker(
        &self,
        job_id: &str,
        terminal_at_ms: u64,
    ) -> Result<()> {
        let path = self.terminal_marker_path(job_id, terminal_at_ms);
        self.store.put(&path, Vec::new().into()).await?;
        Ok(())
    }

    /// List the terminal markers whose `terminal_at_ms` is strictly
    /// before `cutoff_ms`. A cutoff of `u64::MAX` lists every marker.
    ///
    /// Marker filenames lead with the inverted timestamp, so every
    /// expired marker's key is greater than the cutoff's key and the
    /// listing goes through `list_with_offset`: its key-greater-than
    /// filter is part of the object-store contract, and stores such as
    /// S3 and GCS push the offset down, so the cost is proportional to
    /// the number of expired markers rather than the total retained.
    /// Markers are returned in arbitrary order; unparseable filenames
    /// are skipped with a warning.
    pub(crate) async fn list_expired_terminal_markers(
        &self,
        cutoff_ms: u64,
    ) -> Result<Vec<TerminalMarker>> {
        let prefix = self.terminals_prefix();
        // A marker at exactly `cutoff_ms` shares the offset's leading
        // segment and is therefore listed; the parse-side filter below
        // keeps the predicate strict.
        let offset = prefix.child(format!("{:020}", invert_ts(cutoff_ms)));
        let mut stream = self.store.list_with_offset(Some(&prefix), &offset);
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            let meta = item.map_err(Error::Store)?;
            let Some(name) = meta.location.filename() else {
                continue;
            };
            match parse_terminal_marker_name(name) {
                Some((terminal_at_ms, job_id)) if terminal_at_ms < cutoff_ms => {
                    out.push(TerminalMarker {
                        job_id,
                        terminal_at_ms,
                    })
                }
                Some(_) => {}
                None => {
                    tracing::warn!(
                        path = %meta.location,
                        "unparseable terminal marker; skipping",
                    );
                }
            }
        }
        Ok(out)
    }

    /// Delete the terminal marker identified by `marker`.
    ///
    /// A missing marker (already swept by another pass) is treated as
    /// success.
    pub(crate) async fn delete_terminal_marker(&self, marker: &TerminalMarker) -> Result<()> {
        let path = self.terminal_marker_path(&marker.job_id, marker.terminal_at_ms);
        match self.store.delete(&path).await {
            Ok(()) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
            Err(err) => Err(Error::Store(err)),
        }
    }
}

/// Invert a millisecond timestamp so newer values sort first in the
/// zero-padded marker filenames. `u64::MAX` is 20 decimal digits, so
/// every inverted value fits the fixed-width segment and lexicographic
/// order equals numeric order.
fn invert_ts(ms: u64) -> u64 {
    u64::MAX - ms
}

/// Parse a terminal marker filename in the form
/// `<inverted_ts:020>_<job_id>`, returning the original timestamp.
/// Returns `None` if the leading 20 characters are not a base-10
/// integer or the underscore separator is missing.
fn parse_terminal_marker_name(name: &str) -> Option<(u64, String)> {
    let (ts_str, rest) = name.split_at_checked(20)?;
    let inverted: u64 = ts_str.parse().ok()?;
    let job_id = rest.strip_prefix('_')?;
    Some((invert_ts(inverted), job_id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use taquba::object_store::memory::InMemory;

    fn make_store() -> ResultStore {
        ResultStore::new(Arc::new(InMemory::new()), "results".into())
    }

    #[tokio::test]
    async fn put_then_get_round_trips() {
        let store = make_store();
        let outcome = StoredOutcome::Success {
            output: b"hello".to_vec(),
        };
        store.put("job-1", &outcome).await.unwrap();
        let read = store.get("job-1").await.unwrap().unwrap();
        match read {
            StoredOutcome::Success { output } => assert_eq!(output, b"hello"),
            other => panic!("expected Success, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn delete_removes_the_blob() {
        let store = make_store();
        let outcome = StoredOutcome::Success {
            output: b"x".to_vec(),
        };
        store.put("job-1", &outcome).await.unwrap();
        store.delete("job-1").await.unwrap();
        assert!(store.get("job-1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_succeeds_on_missing() {
        let store = make_store();
        store.delete("never-written").await.unwrap();
    }

    #[tokio::test]
    async fn write_terminal_marker_then_list_returns_it() {
        let store = make_store();
        store
            .write_terminal_marker("job-1", 1_700_000_000_000)
            .await
            .unwrap();
        let markers = store.list_expired_terminal_markers(u64::MAX).await.unwrap();
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].job_id, "job-1");
        assert_eq!(markers[0].terminal_at_ms, 1_700_000_000_000);
    }

    #[tokio::test]
    async fn list_expired_terminal_markers_is_empty_when_none_written() {
        let store = make_store();
        assert!(
            store
                .list_expired_terminal_markers(u64::MAX)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn list_expired_terminal_markers_honours_a_strict_cutoff() {
        let store = make_store();
        store.write_terminal_marker("job-old", 1_000).await.unwrap();
        store
            .write_terminal_marker("job-edge", 2_000)
            .await
            .unwrap();
        store.write_terminal_marker("job-new", 3_000).await.unwrap();

        let expired = store.list_expired_terminal_markers(2_000).await.unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].job_id, "job-old");
        assert_eq!(expired[0].terminal_at_ms, 1_000);

        assert!(
            store
                .list_expired_terminal_markers(1_000)
                .await
                .unwrap()
                .is_empty(),
        );
        assert_eq!(
            store
                .list_expired_terminal_markers(3_001)
                .await
                .unwrap()
                .len(),
            3,
        );
    }

    #[tokio::test]
    async fn delete_terminal_marker_removes_only_the_named_one() {
        let store = make_store();
        store.write_terminal_marker("a", 1_000).await.unwrap();
        store.write_terminal_marker("b", 2_000).await.unwrap();
        store
            .delete_terminal_marker(&TerminalMarker {
                job_id: "a".into(),
                terminal_at_ms: 1_000,
            })
            .await
            .unwrap();
        let remaining = store.list_expired_terminal_markers(u64::MAX).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].job_id, "b");
    }

    #[tokio::test]
    async fn delete_terminal_marker_is_idempotent() {
        let store = make_store();
        let marker = TerminalMarker {
            job_id: "job-1".into(),
            terminal_at_ms: 1_000,
        };
        store
            .write_terminal_marker(&marker.job_id, marker.terminal_at_ms)
            .await
            .unwrap();
        store.delete_terminal_marker(&marker).await.unwrap();
        store.delete_terminal_marker(&marker).await.unwrap();
    }
}
