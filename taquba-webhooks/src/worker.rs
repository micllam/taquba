use std::str::FromStr;
use std::time::Duration;

use taquba::{JobRecord, PermanentFailure, Worker, WorkerError};
use tracing::debug;

use crate::{Error, HEADER_METHOD, HEADER_TIMEOUT_MS, HEADER_URL, HTTP_HEADER_PREFIX};

/// HTTP-based webhook delivery worker. Implements [`taquba::Worker`] so it
/// drops straight into [`taquba::run_worker`] / [`taquba::run_worker_concurrent`].
///
/// Build with [`Self::new`] (or [`Self::with_client`] if you need to share a
/// pre-configured [`reqwest::Client`]) and chain the optional builder methods.
pub struct WebhookWorker {
    client: reqwest::Client,
    delivery_id_header: Option<String>,
}

impl WebhookWorker {
    /// Build a worker with a default [`reqwest::Client`].
    pub fn new() -> Self {
        Self::with_client(reqwest::Client::new())
    }

    /// Build a worker that uses the given [`reqwest::Client`].
    pub fn with_client(client: reqwest::Client) -> Self {
        Self {
            client,
            delivery_id_header: Some("Webhook-Id".to_string()),
        }
    }

    /// Override the header name used to carry [`taquba::JobRecord::id`] for
    /// receiver-side idempotency. Defaults to `Webhook-Id`.
    pub fn with_delivery_id_header(mut self, name: impl Into<String>) -> Self {
        self.delivery_id_header = Some(name.into());
        self
    }

    /// Disable the delivery-ID header entirely. Receivers won't be able to
    /// dedupe retries; only set this if you have your own idempotency mechanism.
    pub fn without_delivery_id_header(mut self) -> Self {
        self.delivery_id_header = None;
        self
    }
}

impl Default for WebhookWorker {
    fn default() -> Self {
        Self::new()
    }
}

impl Worker for WebhookWorker {
    async fn process(&self, job: &JobRecord) -> std::result::Result<(), WorkerError> {
        match deliver(self, job).await {
            Ok(()) => Ok(()),
            Err(e) if e.is_permanent() => Err(PermanentFailure::new(e.to_string()).into()),
            Err(e) => Err(e.into()),
        }
    }
}

async fn deliver(worker: &WebhookWorker, job: &JobRecord) -> Result<(), Error> {
    let url = job
        .headers
        .get(HEADER_URL)
        .filter(|s| !s.is_empty())
        .ok_or(Error::MissingUrl)?
        .clone();

    let method_str = job
        .headers
        .get(HEADER_METHOD)
        .map(String::as_str)
        .unwrap_or("POST");
    let method = reqwest::Method::from_str(method_str)
        .map_err(|_| Error::InvalidMethod(method_str.to_string()))?;

    let timeout = match job.headers.get(HEADER_TIMEOUT_MS) {
        Some(s) => Some(Duration::from_millis(
            s.parse::<u64>()
                .map_err(|_| Error::InvalidTimeout(s.clone()))?,
        )),
        None => None,
    };

    let mut req = worker.client.request(method, &url);

    // Pass through `http.<name>` entries as outgoing HTTP headers.
    for (key, value) in &job.headers {
        if let Some(name) = key.strip_prefix(HTTP_HEADER_PREFIX) {
            req = req.header(name, value);
        }
    }

    // Receiver-side idempotency: tag the request with the job ID.
    if let Some(name) = &worker.delivery_id_header {
        req = req.header(name, job.id.as_str());
    }

    if let Some(t) = timeout {
        req = req.timeout(t);
    }

    let response = req
        .body(job.payload.clone())
        .send()
        .await
        .map_err(|e| Error::Delivery(format!("transport error: {e}")))?;

    let status = response.status();
    if status.is_success() {
        debug!(job_id = %job.id, %status, "webhook delivered");
        return Ok(());
    }

    // Capture a short body preview to help with debugging without bloating logs.
    let body_preview = response
        .text()
        .await
        .ok()
        .map(|s| s.chars().take(200).collect::<String>())
        .unwrap_or_default();
    let message = format!("HTTP {status}: {body_preview}");

    // 4xx client errors are permanent (the receiver is rejecting the request
    // intentionally, retrying won't help), except 408 Request Timeout and
    // 429 Too Many Requests, which are retry-friendly per HTTP semantics.
    if status.is_client_error()
        && status != reqwest::StatusCode::REQUEST_TIMEOUT
        && status != reqwest::StatusCode::TOO_MANY_REQUESTS
    {
        Err(Error::PermanentDelivery(message))
    } else {
        Err(Error::Delivery(message))
    }
}
