//! Queue construction shared by the crate's tests.

use std::sync::Arc;
use std::time::Duration;

use taquba::object_store::ObjectStore;
use taquba::object_store::memory::InMemory;
use taquba::{MockClock, OpenOptions, Queue, QueueConfig};

/// A queue named `test` over an in-memory object store of its own,
/// opened with `opts`.
pub(crate) async fn open_queue_with(opts: OpenOptions) -> (Arc<Queue>, Arc<dyn ObjectStore>) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let queue = Arc::new(
        Queue::open_with_options(store.clone(), "test", opts)
            .await
            .unwrap(),
    );
    (queue, store)
}

/// [`open_queue_with`] under the default options.
pub(crate) async fn open_queue() -> (Arc<Queue>, Arc<dyn ObjectStore>) {
    open_queue_with(OpenOptions::default()).await
}

/// [`open_queue_with`] with a [`MockClock`] at `initial_ms` as the
/// queue's clock.
pub(crate) async fn open_queue_at_with(
    initial_ms: u64,
    opts: OpenOptions,
) -> (Arc<Queue>, Arc<dyn ObjectStore>, MockClock) {
    let clock = MockClock::new(initial_ms);
    let (queue, store) = open_queue_with(opts.clock(Arc::new(clock.clone()))).await;
    (queue, store, clock)
}

/// [`open_queue_at_with`] under the default options.
pub(crate) async fn open_queue_at(
    initial_ms: u64,
) -> (Arc<Queue>, Arc<dyn ObjectStore>, MockClock) {
    open_queue_at_with(initial_ms, OpenOptions::default()).await
}

/// Options with zero retry backoff and short reaper and scheduler
/// intervals, for multi-attempt tests.
pub(crate) fn fast_options() -> OpenOptions {
    OpenOptions::default()
        .default_queue_config(QueueConfig::default().retry_backoff_base(Duration::ZERO))
        .reaper_interval(Duration::from_millis(10))
        .scheduler_interval(Duration::from_millis(10))
}

/// Advance `clock` and tokio's paused time by `by` together.
pub(crate) async fn advance(clock: &MockClock, by: Duration) {
    clock.advance(by);
    tokio::time::advance(by).await;
}
