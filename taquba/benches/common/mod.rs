// Setup shared by the bench binaries. Stored in a subdirectory so cargo
// does not discover it as a bench target; each bench declares
// `mod common;`.

use std::sync::Arc;
use std::time::Duration;

use taquba::object_store::ObjectStore;
use taquba::object_store::memory::InMemory;
use taquba::object_store::throttle::{ThrottleConfig, ThrottledStore};

/// Parse an env var, falling back to `default` when unset or unparsable.
pub fn env_var<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<T>().ok())
        .unwrap_or(default)
}

/// Nearest-rank percentile of an ascending-sorted, non-empty slice.
pub fn pct(sorted: &[u64], p: usize) -> u64 {
    let last = sorted.len() - 1;
    sorted[(sorted.len() * p / 100).min(last)]
}

/// In-memory object store, wrapped in `object_store`'s `ThrottledStore`
/// when `latency_ms` is above 0 so every get, put, list, and delete
/// sleeps that long before running, approximating an S3-class backend.
pub fn store_with_latency(latency_ms: u64) -> Arc<dyn ObjectStore> {
    if latency_ms > 0 {
        let wait = Duration::from_millis(latency_ms);
        let config = ThrottleConfig {
            wait_delete_per_call: wait,
            wait_get_per_call: wait,
            wait_list_per_call: wait,
            wait_put_per_call: wait,
            ..ThrottleConfig::default()
        };
        Arc::new(ThrottledStore::new(InMemory::new(), config))
    } else {
        Arc::new(InMemory::new())
    }
}

/// Install a stderr tracing subscriber honouring `RUST_LOG` (e.g.
/// `RUST_LOG=taquba=warn`) so queue warnings such as
/// transaction-conflict retries are visible during runs.
pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
}
