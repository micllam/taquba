//! Paged reads of the queue as streams: every entry under a prefix of
//! the caller KV namespace and every job of a queue in one status. A
//! consumer reads items with `try_next` and stops reading whenever it
//! is done, so a scan that ends early fetches no further page.

use std::future::Future;

use bytes::Bytes;
use futures_util::stream::{self, Stream, TryStreamExt};
use taquba::{JobRecord, JobStatus, Queue};

use crate::error::{Error, Result};

/// The items of a paged read, fetched one page at a time: `fetch`
/// takes the cursor of the page to read, `None` for the first, and
/// returns the page's items with the cursor of the next page, `None`
/// once the read is exhausted.
pub(crate) fn pages<T, F, Fut>(fetch: F) -> impl Stream<Item = Result<T>>
where
    F: FnMut(Option<Vec<u8>>) -> Fut,
    Fut: Future<Output = Result<(Vec<T>, Option<Vec<u8>>)>>,
{
    // The outer `Option` is `None` once the read is exhausted; the
    // inner one is the cursor `fetch` takes.
    stream::try_unfold((Some(None), fetch), |(cursor, mut fetch)| async move {
        let Some(cursor) = cursor else {
            return Ok::<_, Error>(None);
        };
        let (items, next) = fetch(cursor).await?;
        Ok(Some((
            stream::iter(items.into_iter().map(Ok::<T, Error>)),
            (next.map(Some), fetch),
        )))
    })
    .try_flatten()
}

/// Every entry under `prefix` in the caller KV namespace, in ascending
/// key order, read `page_size` entries at a time.
pub(crate) fn kv_entries<'a>(
    queue: &'a Queue,
    prefix: &'a [u8],
    page_size: usize,
) -> impl Stream<Item = Result<(Vec<u8>, Bytes)>> + 'a {
    pages(move |cursor| async move {
        let page = queue.kv_scan(prefix, cursor.as_deref(), page_size).await?;
        Ok((page.entries, page.next_cursor))
    })
}

/// Every job of `queue_name` in `status`, in the order
/// [`Queue::list_jobs`] pages them, read `page_size` jobs at a time.
pub(crate) fn jobs<'a>(
    queue: &'a Queue,
    queue_name: &'a str,
    status: JobStatus,
    page_size: usize,
) -> impl Stream<Item = Result<JobRecord>> + 'a {
    pages(move |cursor| async move {
        let page = queue
            .list_jobs(queue_name, status, cursor.as_deref(), page_size)
            .await?;
        Ok((page.jobs, page.next_cursor))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use taquba::object_store::memory::InMemory;

    #[tokio::test]
    async fn kv_entries_cross_page_boundaries_in_key_order() {
        let store = Arc::new(InMemory::new());
        let queue = Queue::open(store, "paging").await.unwrap();
        for i in 0..5u8 {
            queue.kv_put(&[b'p', b'/', b'0' + i], b"v").await.unwrap();
        }
        queue.kv_put(b"q/0", b"v").await.unwrap();

        let keys: Vec<Vec<u8>> = kv_entries(&queue, b"p/", 2)
            .map_ok(|(key, _)| key)
            .try_collect()
            .await
            .unwrap();
        let expected: Vec<Vec<u8>> = (0..5u8).map(|i| vec![b'p', b'/', b'0' + i]).collect();
        assert_eq!(keys, expected);
    }
}
