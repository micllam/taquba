use std::str::FromStr;
use std::time::Duration;

use taquba::{JobRecord, LeaseHandle, PermanentFailure, Worker, WorkerError};
use tracing::debug;

use crate::{Error, HEADER_METHOD, HEADER_TIMEOUT_MS, HEADER_URL, HTTP_HEADER_PREFIX};

/// Upper bound on a delivery when the job declares no
/// [`HEADER_TIMEOUT_MS`](crate::HEADER_TIMEOUT_MS) override and the
/// worker sets no [`WebhookWorker::with_default_timeout`].
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// HTTP-based webhook delivery worker. Implements [`taquba::Worker`] so it
/// drops straight into [`taquba::run_worker`] / [`taquba::run_worker_concurrent`].
///
/// Build with [`Self::new`] (or [`Self::with_client`] if you need to share a
/// pre-configured [`reqwest::Client`]) and chain the optional builder methods.
///
/// Every delivery is bounded: the request timeout is the job's
/// [`HEADER_TIMEOUT_MS`](crate::HEADER_TIMEOUT_MS) header when present,
/// otherwise the worker's default ([`DEFAULT_TIMEOUT`] unless overridden
/// with [`Self::with_default_timeout`]). The timeout is applied per
/// request and takes precedence over a timeout configured on the
/// [`reqwest::Client`]. Before sending, the worker extends the job's
/// lease to cover the timeout, so a slow receiver does not cause the
/// job to be re-queued mid-delivery.
pub struct WebhookWorker {
    client: reqwest::Client,
    delivery_id_header: Option<String>,
    default_timeout: Duration,
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
            default_timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Override the request timeout applied to jobs that declare no
    /// [`HEADER_TIMEOUT_MS`](crate::HEADER_TIMEOUT_MS) header. Defaults
    /// to [`DEFAULT_TIMEOUT`].
    pub fn with_default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = timeout;
        self
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
    async fn process(
        &self,
        job: &JobRecord,
        lease: &LeaseHandle,
    ) -> std::result::Result<(), WorkerError> {
        match deliver(self, job, lease).await {
            Ok(()) => Ok(()),
            Err(e) if e.is_permanent() => Err(PermanentFailure::new(e.to_string()).into()),
            Err(e) => Err(e.into()),
        }
    }
}

/// The request timeout for a delivery: the job's [`HEADER_TIMEOUT_MS`]
/// header, or `default` when the header is absent.
fn effective_timeout(
    headers: &std::collections::HashMap<String, String>,
    default: Duration,
) -> Result<Duration, Error> {
    match headers.get(HEADER_TIMEOUT_MS) {
        Some(s) => Ok(Duration::from_millis(
            s.parse::<u64>()
                .map_err(|_| Error::InvalidTimeout(s.clone()))?,
        )),
        None => Ok(default),
    }
}

async fn deliver(
    worker: &WebhookWorker,
    job: &JobRecord,
    lease: &LeaseHandle,
) -> Result<(), Error> {
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

    let timeout = effective_timeout(&job.headers, worker.default_timeout)?;

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

    req = req.timeout(timeout);

    // One extension covering the bounded send; there is no progress
    // signal inside it to renew on.
    lease
        .ensure_at_least(timeout)
        .map_err(|e| Error::Delivery(format!("lease extension failed: {e}")))?;

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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn effective_timeout_defaults_when_no_header_is_present() {
        let headers = HashMap::new();
        assert_eq!(
            effective_timeout(&headers, DEFAULT_TIMEOUT).unwrap(),
            DEFAULT_TIMEOUT
        );
    }

    #[test]
    fn effective_timeout_prefers_the_header_override() {
        let mut headers = HashMap::new();
        headers.insert(HEADER_TIMEOUT_MS.to_string(), "15000".to_string());
        assert_eq!(
            effective_timeout(&headers, DEFAULT_TIMEOUT).unwrap(),
            Duration::from_millis(15_000)
        );
    }

    #[test]
    fn effective_timeout_rejects_a_non_numeric_header() {
        let mut headers = HashMap::new();
        headers.insert(HEADER_TIMEOUT_MS.to_string(), "soon".to_string());
        assert!(matches!(
            effective_timeout(&headers, DEFAULT_TIMEOUT),
            Err(Error::InvalidTimeout(_))
        ));
    }
}
