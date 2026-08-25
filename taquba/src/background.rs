//! Background tasks of an open queue: a spawned task with a shutdown
//! signal, and the periodic tick every such task runs on.

use std::future::Future;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

use crate::error::Result;

/// A unit of work run once per tick by [`BackgroundTask::spawn_periodic`].
pub(crate) trait Periodic: Send + Sync + 'static {
    /// Name used in the task's log lines.
    const NAME: &'static str;

    /// Run one tick. An error is logged at warn level and the task
    /// continues.
    fn step(&self) -> impl Future<Output = Result<()>> + Send;
}

/// A spawned task stopped by [`BackgroundTask::stop`]. The task is
/// given a [`Ticker`] and runs until the ticker reports shutdown,
/// which dropping the handle also signals.
pub(crate) struct BackgroundTask<T = ()> {
    shutdown: watch::Sender<bool>,
    handle: JoinHandle<T>,
}

impl<T: Send + 'static> BackgroundTask<T> {
    /// Spawn `run` with a ticker firing every `interval`.
    pub(crate) fn spawn<F, Fut>(interval: Duration, run: F) -> Self
    where
        F: FnOnce(Ticker) -> Fut,
        Fut: Future<Output = T> + Send + 'static,
    {
        let (shutdown, receiver) = watch::channel(false);
        let ticker = Ticker {
            interval,
            shutdown: receiver,
        };
        Self {
            shutdown,
            handle: tokio::spawn(run(ticker)),
        }
    }

    /// Signal shutdown and wait for the task to finish. Returns its
    /// output, or `None` when the task panicked.
    pub(crate) async fn stop(self) -> Option<T> {
        let _ = self.shutdown.send(true);
        match self.handle.await {
            Ok(output) => Some(output),
            Err(e) => {
                error!("background task ended abnormally: {e}");
                None
            }
        }
    }
}

impl BackgroundTask {
    /// Spawn `task` so that its [`Periodic::step`] runs once per
    /// `interval` until shutdown.
    pub(crate) fn spawn_periodic<P: Periodic>(interval: Duration, task: P) -> BackgroundTask {
        BackgroundTask::spawn(interval, |mut ticker| async move {
            while ticker.tick().await {
                if let Err(e) = task.step().await {
                    warn!("{} error: {e}", P::NAME);
                }
            }
            debug!("{} stopped", P::NAME);
        })
    }
}

/// The periodic tick of a background task.
pub(crate) struct Ticker {
    interval: Duration,
    shutdown: watch::Receiver<bool>,
}

impl Ticker {
    /// Wait for the next tick. Returns `false` once shutdown is
    /// signalled or the [`BackgroundTask`] is dropped, which ends the
    /// task's loop.
    pub(crate) async fn tick(&mut self) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(self.interval) => true,
            _ = self.shutdown.changed() => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    struct SetOnDrop(Arc<AtomicBool>);

    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stop_returns_the_output_after_the_current_tick() {
        let task = BackgroundTask::spawn(Duration::from_secs(1), |mut ticker| async move {
            let mut ticks = 0u32;
            while ticker.tick().await {
                ticks += 1;
            }
            ticks
        });
        tokio::time::sleep(Duration::from_millis(2_500)).await;
        assert_eq!(task.stop().await, Some(2));
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_the_handle_stops_the_task() {
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = SetOnDrop(dropped.clone());
        let task = BackgroundTask::spawn(Duration::from_secs(1), |mut ticker| async move {
            let _guard = guard;
            while ticker.tick().await {}
        });
        tokio::task::yield_now().await;
        assert!(!dropped.load(Ordering::SeqCst));
        drop(task);
        tokio::task::yield_now().await;
        assert!(dropped.load(Ordering::SeqCst));
    }
}
