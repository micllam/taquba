//! HTTP webhook delivery on top of the [Taquba] durable task queue.
//!
//! Aimed at workloads where a webhook is the announcement of an event
//! (alerting on a timeseries DB, event relays, push notifications).
//!
//! # Usage
//!
//! Taquba is single-process: producer and worker run in the same process and
//! share one `Arc<Queue>`.
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use taquba::{Queue, object_store::memory::InMemory, run_worker};
//! use taquba_webhooks::{WebhookRequest, WebhookWorker, enqueue_webhook};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let queue = Arc::new(Queue::open(Arc::new(InMemory::new()), "demo").await?);
//!
//! // Worker: deliver each request.
//! let worker_queue = queue.clone();
//! tokio::spawn(async move {
//!     let worker = WebhookWorker::new();
//!     run_worker(
//!         &worker_queue,
//!         "webhooks",
//!         &worker,
//!         Duration::from_millis(100),
//!         std::future::pending::<()>(),
//!     )
//!     .await
//! });
//!
//! // Producer: enqueue a delivery.
//! let request = WebhookRequest::new("https://example.com/hook")
//!     .header("Content-Type", "application/json")
//!     .timeout(Duration::from_secs(10));
//! enqueue_webhook(&queue, "webhooks", request, br#"{"event":"ping"}"#.to_vec()).await?;
//! # Ok(()) }
//! ```
//!
//! # Reserved header keys
//!
//! Webhook configuration travels with each job in [`taquba::JobRecord::headers`]
//! under reserved keys:
//!
//! | Key | Required | Meaning |
//! |---|---|---|
//! | [`HEADER_URL`] (`webhook.url`) | yes | Target URL |
//! | [`HEADER_METHOD`] (`webhook.method`) | no | HTTP method (default `POST`) |
//! | [`HEADER_TIMEOUT_MS`] (`webhook.timeout_ms`) | no | Per-request timeout |
//! | [`HTTP_HEADER_PREFIX`]`<name>` (`http.<name>`) | no | HTTP header to send |
//!
//! Other entries in `headers` are ignored by the worker; your application
//! can use them for its own metadata.
//!
//! # Delivery semantics
//!
//! - **2xx response**: ack (Taquba marks the job done).
//! - **5xx, 408 Request Timeout, 429 Too Many Requests, transport errors,
//!   timeouts**: nack. Taquba retries on its configured exponential backoff
//!   up to the queue's `max_attempts`, then dead-letters.
//! - **Other 4xx (client errors), missing/invalid configuration headers**:
//!   dead-letter immediately via [`taquba::PermanentFailure`]. The receiver
//!   has explicitly rejected the request; retrying won't help.
//!
//! # Receiver-side idempotency
//!
//! Each delivery includes a `Webhook-Id` header (configurable via
//! [`WebhookWorker::with_delivery_id_header`]) carrying [`taquba::JobRecord::id`].
//! Taquba is at-least-once, so receivers must dedupe on this header to handle
//! retries correctly.
//!
//! [Taquba]: https://docs.rs/taquba

#![warn(missing_docs)]

use std::collections::HashMap;
use std::time::Duration;

use taquba::{EnqueueOptions, Queue};

/// Errors returned by the producer and worker.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The job's headers don't include [`HEADER_URL`], or the value is empty.
    /// Permanent: a misconfigured job will not become valid on retry.
    #[error("missing webhook URL (header `{HEADER_URL}`)")]
    MissingUrl,
    /// The [`HEADER_METHOD`] value is not a recognizable HTTP method.
    /// Permanent: header value won't change across retries.
    #[error("invalid HTTP method `{0}`")]
    InvalidMethod(String),
    /// [`HEADER_TIMEOUT_MS`] was present but couldn't be parsed as a non-negative integer.
    /// Permanent: header value won't change across retries.
    #[error("invalid `{HEADER_TIMEOUT_MS}` `{0}`: not a non-negative integer")]
    InvalidTimeout(String),
    /// HTTP delivery failed transiently (network, TLS, timeout, 5xx, 408, 429).
    /// Retried per the queue's backoff policy.
    #[error("delivery failed: {0}")]
    Delivery(String),
    /// HTTP delivery failed with a permanent client error (4xx other than
    /// 408 Request Timeout and 429 Too Many Requests). Dead-lettered
    /// immediately by the worker via [`taquba::PermanentFailure`].
    #[error("permanent delivery failure: {0}")]
    PermanentDelivery(String),
    /// Underlying error from a Taquba queue operation.
    #[error(transparent)]
    Queue(#[from] taquba::Error),
}

impl Error {
    /// True if this error should dead-letter the job rather than retry.
    /// Permanent errors include configuration mistakes and HTTP client
    /// errors.
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            Error::MissingUrl
                | Error::InvalidMethod(_)
                | Error::InvalidTimeout(_)
                | Error::PermanentDelivery(_)
        )
    }
}

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// [`taquba::JobRecord::headers`] key for the target URL. Required.
pub const HEADER_URL: &str = "webhook.url";
/// [`taquba::JobRecord::headers`] key for the HTTP method. Optional, defaults to `POST`.
pub const HEADER_METHOD: &str = "webhook.method";
/// [`taquba::JobRecord::headers`] key for the per-request timeout, in milliseconds. Optional.
pub const HEADER_TIMEOUT_MS: &str = "webhook.timeout_ms";
/// Prefix marking a [`taquba::JobRecord::headers`] entry that should be passed
/// through as an outgoing HTTP request header (e.g. `http.Content-Type`).
pub const HTTP_HEADER_PREFIX: &str = "http.";

/// A single webhook delivery to enqueue. Build with [`Self::new`] and the
/// chainable setters, then pass to [`enqueue_webhook`].
#[derive(Debug, Clone)]
pub struct WebhookRequest {
    /// Target URL (required).
    pub url: String,
    /// HTTP method. Defaults to `POST`.
    pub method: String,
    /// Outgoing HTTP headers (sent on the wire to the target URL).
    pub headers: HashMap<String, String>,
    /// Optional per-request timeout. The worker enforces it via reqwest.
    pub timeout: Option<Duration>,
}

impl WebhookRequest {
    /// Build a new POST request to `url` with no headers and no timeout.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            method: "POST".to_string(),
            headers: HashMap::new(),
            timeout: None,
        }
    }

    /// Override the HTTP method (e.g. `"PUT"`, `"PATCH"`).
    pub fn method(mut self, method: impl Into<String>) -> Self {
        self.method = method.into();
        self
    }

    /// Add an HTTP header to send on the outgoing request.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// Set a per-request timeout. Without it, the reqwest client's default applies.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Enqueue a webhook delivery onto Taquba's `target_queue`. The returned
/// string is the [`taquba::JobRecord::id`] of the new job.
///
/// `body` becomes the HTTP request body; `request` is encoded into the job's
/// [`taquba::JobRecord::headers`] under the reserved keys documented at the
/// crate root.
pub async fn enqueue_webhook(
    queue: &Queue,
    target_queue: &str,
    request: WebhookRequest,
    body: Vec<u8>,
) -> Result<String> {
    let WebhookRequest {
        url,
        method,
        headers,
        timeout,
    } = request;

    let mut job_headers = HashMap::new();
    job_headers.insert(HEADER_URL.to_string(), url);
    job_headers.insert(HEADER_METHOD.to_string(), method);
    if let Some(t) = timeout {
        job_headers.insert(HEADER_TIMEOUT_MS.to_string(), t.as_millis().to_string());
    }
    for (name, value) in headers {
        job_headers.insert(format!("{HTTP_HEADER_PREFIX}{name}"), value);
    }

    let opts = EnqueueOptions {
        headers: job_headers,
        ..Default::default()
    };

    Ok(queue.enqueue_with(target_queue, body, opts).await?)
}

mod worker;
pub use worker::WebhookWorker;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use taquba::object_store::memory::InMemory;

    #[tokio::test]
    async fn enqueue_packs_request_into_headers() {
        let q = Queue::open(Arc::new(InMemory::new()), "test")
            .await
            .unwrap();
        let request = WebhookRequest::new("https://example.com/hook")
            .method("PUT")
            .header("Content-Type", "application/json")
            .header("Custom-Header", "value")
            .timeout(Duration::from_secs(15));

        let id = enqueue_webhook(&q, "webhooks", request, b"body".to_vec())
            .await
            .unwrap();
        let job = q.get_job(&id).await.unwrap().expect("job exists");

        assert_eq!(job.payload, b"body");
        assert_eq!(
            job.headers.get(HEADER_URL).unwrap(),
            "https://example.com/hook"
        );
        assert_eq!(job.headers.get(HEADER_METHOD).unwrap(), "PUT");
        assert_eq!(job.headers.get(HEADER_TIMEOUT_MS).unwrap(), "15000");
        assert_eq!(
            job.headers.get("http.Content-Type").unwrap(),
            "application/json"
        );
        assert_eq!(job.headers.get("http.Custom-Header").unwrap(), "value");
    }

    #[tokio::test]
    async fn enqueue_defaults_to_post_with_no_timeout() {
        let q = Queue::open(Arc::new(InMemory::new()), "test")
            .await
            .unwrap();
        let request = WebhookRequest::new("https://example.com");

        let id = enqueue_webhook(&q, "webhooks", request, b"".to_vec())
            .await
            .unwrap();
        let job = q.get_job(&id).await.unwrap().unwrap();

        assert_eq!(job.headers.get(HEADER_METHOD).unwrap(), "POST");
        assert!(!job.headers.contains_key(HEADER_TIMEOUT_MS));
    }
}
