//! Shared setup for the taquba workspace's benchmark binaries, which
//! live under `benches/`. This crate is an internal workspace member
//! and is never published; see `README.md` for the benchmark
//! catalogue and conventions.

use std::sync::Arc;
use std::time::Duration;

use taquba::object_store::memory::InMemory;
use taquba::object_store::prefix::PrefixStore;
use taquba::object_store::throttle::{ThrottleConfig, ThrottledStore};
use taquba::object_store::{ObjectStore, parse_url_opts};

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

/// Object store for a bench run, selected by env vars.
///
/// With `STORE_URL` set (`s3://bucket/prefix`, `gs://...`, `az://...`,
/// `file:///abs/path`), opens that store and places each run under a
/// fresh `bench-<unix-millis>` prefix so a rerun never observes a
/// previous run's state; the prefix is printed to stderr. Cloud
/// schemes require the matching cargo feature on this crate and read
/// provider configuration from the `AWS_*` / `GOOGLE_*` / `AZURE_*`
/// env vars. `STORE_LATENCY_MS` throttles the in-memory store only,
/// so combining it with `STORE_URL` is an error.
///
/// Without `STORE_URL`, the in-memory store from `store_with_latency`.
pub fn store_from_env(latency_ms: u64) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error>> {
    let Ok(raw) = std::env::var("STORE_URL") else {
        return Ok(store_with_latency(latency_ms));
    };
    if latency_ms > 0 {
        return Err(
            "STORE_LATENCY_MS throttles the in-memory store only; unset it when STORE_URL is set"
                .into(),
        );
    }
    let url = url::Url::parse(&raw)?;
    // object_store's config keys are lowercase versions of the provider
    // env var names; the prefix filter keeps unrelated env vars whose
    // lowercase form is also a valid config key (TOKEN, ENDPOINT) out
    // of the store configuration.
    let options = std::env::vars().filter_map(|(key, value)| {
        let key = key.to_ascii_lowercase();
        (key.starts_with("aws_") || key.starts_with("google_") || key.starts_with("azure_"))
            .then_some((key, value))
    });
    let (store, path) = parse_url_opts(&url, options)?;
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let run_prefix = path.child(format!("bench-{millis}"));
    eprintln!("store: {raw}, run prefix: {run_prefix}");
    Ok(Arc::new(PrefixStore::new(store, run_prefix)))
}

/// In-memory object store, wrapped in `object_store`'s `ThrottledStore`
/// when `latency_ms` is above 0 so every get, put, list, and delete
/// sleeps that long before running, approximating an S3-class backend.
fn store_with_latency(latency_ms: u64) -> Arc<dyn ObjectStore> {
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
