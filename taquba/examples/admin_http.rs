// cargo run -p taquba --example admin_http
//
// A minimal admin HTTP server for inspecting and operating a live queue.
// It maps the queue's inspection and intervention APIs onto JSON endpoints:
//
//   GET  /queues                  list_queues: every queue seen so far
//   GET  /queues/{queue}/stats    stats: job counts by lifecycle state
//   GET  /queues/{queue}/jobs     list_jobs: page through one lifecycle
//                                 state (?status=pending|scheduled|claimed|
//                                 done|dead&cursor=<token>&limit=<n>)
//   GET  /queues/{queue}/dead     dead_jobs: page through the dead-letter
//                                 set (?after=<id>&limit=<n>)
//   GET  /jobs/{id}               get_job: one job, in any state
//   GET  /jobs/{id}/history       attempt_history: every settled attempt
//                                 (retries, dead-letters, lease expiries)
//   POST /jobs/{id}/cancel        cancel: remove a pending/scheduled job, or
//                                 request cooperative cancellation of a
//                                 claimed one
//   POST /jobs/{id}/requeue       requeue_dead_job: revive a dead job with
//                                 a fresh retry budget
//
// The process generates its own demo traffic so every endpoint returns
// data: an "emails" queue with a worker and a producer (some jobs fail
// permanently and dead-letter, some retry transiently before
// succeeding), and a workerless "reports" queue (jobs stay pending,
// plus one scheduled for tomorrow).
//
// A typical triage flow, in another terminal:
//
//   curl -s localhost:3000/queues
//   curl -s localhost:3000/queues/emails/stats
//   curl -s 'localhost:3000/queues/reports/jobs?status=pending'
//   curl -s 'localhost:3000/queues/emails/dead?limit=10'
//   curl -s localhost:3000/jobs/<id>            # why did it die?
//   curl -s localhost:3000/jobs/<id>/history    # every attempt's error
//   curl -s -X POST localhost:3000/jobs/<id>/requeue
//   curl -s -X POST localhost:3000/jobs/<id>/cancel
//
// This is a recipe to copy and adapt, not a production admin plane: there
// is no authentication, no TLS and no rate limiting. Because a store is
// single-writer, an admin surface that mutates state (requeue, cancel)
// must live inside the process that owns the queue, as it does here.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use taquba::{
    CancelOutcome, EnqueueOptions, JobAttempt, JobRecord, JobStatus, OpenOptions, PermanentFailure,
    Queue, QueueConfig, QueueStats, Worker, WorkerError, object_store::memory::InMemory,
    run_worker,
};

/// Admin-facing view of a [`JobRecord`]: serde renders the raw-byte
/// `payload` as a JSON array of numbers, so this view substitutes a
/// bounded, human-readable preview.
#[derive(Serialize)]
struct JobView {
    id: String,
    queue: String,
    status: JobStatus,
    attempts: u32,
    max_attempts: u32,
    priority: u32,
    enqueued_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    claimed_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lease_expires_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failed_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    headers: HashMap<String, String>,
    cancel_requested: bool,
    payload_len: usize,
    payload_offloaded: bool,
    payload_preview: String,
}

impl From<JobRecord> for JobView {
    fn from(job: JobRecord) -> Self {
        const PREVIEW_MAX: usize = 128;
        let cut = job.payload.len().min(PREVIEW_MAX);
        let mut payload_preview = String::from_utf8_lossy(&job.payload[..cut]).into_owned();
        if job.payload.len() > PREVIEW_MAX {
            payload_preview.push('…');
        }
        Self {
            id: job.id,
            queue: job.queue,
            status: job.status,
            attempts: job.attempts,
            max_attempts: job.max_attempts,
            priority: job.priority,
            enqueued_at: job.enqueued_at,
            claimed_at: job.claimed_at,
            lease_expires_at: job.lease_expires_at,
            run_at: job.run_at,
            completed_at: job.completed_at,
            failed_at: job.failed_at,
            last_error: job.last_error,
            headers: job.headers,
            cancel_requested: job.cancel_requested,
            payload_len: job.payload.len(),
            payload_offloaded: job.payload_ref.is_some(),
            payload_preview,
        }
    }
}

/// Maps `taquba::Error` onto HTTP status codes. Only the variants an admin
/// caller can trigger get their own status; everything else is a 500.
enum ApiError {
    BadRequest(String),
    NotFound(String),
    Conflict(String),
    Internal(taquba::Error),
}

impl From<taquba::Error> for ApiError {
    fn from(e: taquba::Error) -> Self {
        match e {
            taquba::Error::JobNotFound(id) => ApiError::NotFound(format!("job not found: {id}")),
            taquba::Error::InvalidState => {
                ApiError::Conflict("job is not in the expected state".to_string())
            }
            e @ taquba::Error::InvalidQueueName { .. } => ApiError::BadRequest(e.to_string()),
            other => ApiError::Internal(other),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ApiError::Conflict(m) => (StatusCode::CONFLICT, m),
            ApiError::Internal(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

async fn queues(State(q): State<Arc<Queue>>) -> Result<Json<Vec<String>>, ApiError> {
    Ok(Json(q.list_queues().await?))
}

async fn queue_stats(
    State(q): State<Arc<Queue>>,
    Path(queue): Path<String>,
) -> Result<Json<QueueStats>, ApiError> {
    Ok(Json(q.stats(&queue).await?))
}

#[derive(Deserialize)]
struct JobsPageParams {
    status: String,
    /// Opaque resume token from the previous page's `next_cursor`.
    cursor: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
}

async fn jobs_page(
    State(q): State<Arc<Queue>>,
    Path(queue): Path<String>,
    Query(params): Query<JobsPageParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let status = match params.status.as_str() {
        "pending" => JobStatus::Pending,
        "scheduled" => JobStatus::Scheduled,
        "claimed" => JobStatus::Claimed,
        "done" => JobStatus::Done,
        "dead" => JobStatus::Dead,
        other => return Err(ApiError::BadRequest(format!("unknown status: {other}"))),
    };
    let cursor = params.cursor.as_deref().map(hex_decode).transpose()?;
    let page = q
        .list_jobs(&queue, status, cursor.as_deref(), params.limit)
        .await?;
    let views: Vec<JobView> = page.jobs.into_iter().map(JobView::from).collect();
    Ok(Json(json!({
        "jobs": views,
        "next_cursor": page.next_cursor.map(|c| hex_encode(&c)),
    })))
}

// The listing cursor is opaque bytes; hex makes it URL-safe.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_decode(s: &str) -> Result<Vec<u8>, ApiError> {
    if !s.len().is_multiple_of(2) {
        return Err(ApiError::BadRequest("invalid cursor".to_string()));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|_| ApiError::BadRequest("invalid cursor".to_string()))
        })
        .collect()
}

#[derive(Deserialize)]
struct DeadPageParams {
    /// Exclusive cursor: the `id` of the last job on the previous page.
    after: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    20
}

async fn dead_page(
    State(q): State<Arc<Queue>>,
    Path(queue): Path<String>,
    Query(params): Query<DeadPageParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let jobs = q
        .dead_jobs(&queue, params.after.as_deref(), params.limit)
        .await?;
    // A full page may continue; a short page is the end of the set.
    let next_after = (jobs.len() == params.limit).then(|| jobs.last().map(|j| j.id.clone()));
    let views: Vec<JobView> = jobs.into_iter().map(JobView::from).collect();
    Ok(Json(json!({ "jobs": views, "next_after": next_after })))
}

async fn job(
    State(q): State<Arc<Queue>>,
    Path(id): Path<String>,
) -> Result<Json<JobView>, ApiError> {
    match q.get_job(&id).await? {
        Some(job) => Ok(Json(job.into())),
        None => Err(ApiError::NotFound(format!("job not found: {id}"))),
    }
}

async fn job_history(
    State(q): State<Arc<Queue>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<JobAttempt>>, ApiError> {
    // The history shares the job's lifetime, so an unknown or expunged
    // job returns an empty list, consistent with get_job returning None.
    Ok(Json(q.attempt_history(&id).await?))
}

async fn cancel_job(
    State(q): State<Arc<Queue>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    match q.cancel(&id).await? {
        CancelOutcome::Removed => Ok(Json(json!({ "outcome": "removed" }))),
        CancelOutcome::Requested => Ok(Json(json!({ "outcome": "cancellation_requested" }))),
        CancelOutcome::NotFound => Err(ApiError::NotFound(format!("job not found: {id}"))),
    }
}

async fn requeue_job(
    State(q): State<Arc<Queue>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let job = q
        .get_job(&id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("job not found: {id}")))?;
    // A non-dead job maps to 409 via `Error::InvalidState`.
    q.requeue_dead_job(job).await?;
    Ok(Json(json!({ "outcome": "requeued" })))
}

struct EmailWorker;

impl Worker for EmailWorker {
    async fn process(&self, job: &JobRecord) -> Result<(), WorkerError> {
        if job.payload.starts_with(b"boom") {
            return Err(PermanentFailure::new("simulated permanent SMTP rejection").into());
        }
        // "flaky" mails fail transiently until the third attempt, so
        // /jobs/{id}/history shows Retried entries before the outcome.
        if job.payload.starts_with(b"flaky") && job.attempts < 3 {
            return Err(format!(
                "simulated transient SMTP timeout (attempt {})",
                job.attempts
            )
            .into());
        }
        // Simulate slow work so claimed jobs are observable and a cancel
        // has a live claim to target.
        let work = tokio::time::sleep(Duration::from_secs(3));
        if let Some(token) = &job.cancel_token {
            tokio::select! {
                _ = token.cancelled() => {
                    // Cancellation is cooperative: stop early, ack normally.
                    println!("worker: job {} cancelled by operator, acking early", job.id);
                    return Ok(());
                }
                _ = work => {}
            }
        } else {
            work.await;
        }
        println!("worker: job {} sent", job.id);
        Ok(())
    }
}

async fn spawn_demo_traffic(q: &Arc<Queue>) -> Result<(), taquba::Error> {
    // Seed the dead-letter set so /dead has content immediately, and one
    // flaky mail so /jobs/{id}/history shows a retries-then-success run.
    q.enqueue("emails", b"boom: mail to nobody@example.com".to_vec())
        .await?;
    q.enqueue("emails", b"boom: mail to invalid@@address".to_vec())
        .await?;
    q.enqueue("emails", b"flaky: mail to greylisted@example.com".to_vec())
        .await?;

    // "reports" has no worker: its jobs stay pending, plus one scheduled
    // for tomorrow.
    for name in ["weekly-usage", "billing-summary", "storage-audit"] {
        q.enqueue("reports", format!("report: {name}").into_bytes())
            .await?;
    }
    q.enqueue_with(
        "reports",
        b"report: quarterly-rollup".to_vec(),
        EnqueueOptions {
            run_at: Some(SystemTime::now() + Duration::from_secs(24 * 3600)),
            ..Default::default()
        },
    )
    .await?;

    // Worker loop for "emails"; the pending() shutdown future never
    // resolves, so it runs until the process exits.
    let worker_q = q.clone();
    tokio::spawn(async move {
        if let Err(e) = run_worker(
            &worker_q,
            "emails",
            &EmailWorker,
            Duration::from_millis(200),
            std::future::pending::<()>(),
        )
        .await
        {
            eprintln!("worker loop terminated: {e}");
        }
    });

    // Producer: one email every few seconds; every fourth fails permanently
    // and dead-letters, and every fourth starting from the second retries
    // twice before succeeding.
    let producer_q = q.clone();
    tokio::spawn(async move {
        let mut n = 0u64;
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            n += 1;
            let payload = if n.is_multiple_of(4) {
                format!("boom: mail #{n} to bounce@example.com")
            } else if (n + 2).is_multiple_of(4) {
                format!("flaky: mail #{n} to greylisted{n}@example.com")
            } else {
                format!("mail #{n} to user{n}@example.com")
            };
            if let Err(e) = producer_q.enqueue("emails", payload.into_bytes()).await {
                eprintln!("producer stopped: {e}");
                break;
            }
        }
    });

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut opts = OpenOptions::default();
    opts.queue_configs.insert(
        "emails".to_string(),
        QueueConfig {
            max_attempts: 3,
            retry_backoff_base: Duration::from_millis(500),
            // Retain done records for an hour, so a completed job's record
            // and attempt history stay inspectable instead of being
            // removed on ack.
            keep_done_jobs: Some(Duration::from_secs(3600)),
            ..QueueConfig::default()
        },
    );
    let q =
        Arc::new(Queue::open_with_options(Arc::new(InMemory::new()), "admin-demo", opts).await?);

    spawn_demo_traffic(&q).await?;

    let app = Router::new()
        .route("/queues", get(queues))
        .route("/queues/{queue}/stats", get(queue_stats))
        .route("/queues/{queue}/jobs", get(jobs_page))
        .route("/queues/{queue}/dead", get(dead_page))
        .route("/jobs/{id}", get(job))
        .route("/jobs/{id}/history", get(job_history))
        .route("/jobs/{id}/cancel", post(cancel_job))
        .route("/jobs/{id}/requeue", post(requeue_job))
        .with_state(q);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    println!("admin server listening on http://127.0.0.1:3000");
    println!();
    println!("  GET  /queues");
    println!("  GET  /queues/{{queue}}/stats");
    println!("  GET  /queues/{{queue}}/jobs?status=<state>&cursor=<token>&limit=<n>");
    println!("  GET  /queues/{{queue}}/dead?after=<id>&limit=<n>");
    println!("  GET  /jobs/{{id}}");
    println!("  GET  /jobs/{{id}}/history");
    println!("  POST /jobs/{{id}}/cancel");
    println!("  POST /jobs/{{id}}/requeue");
    println!();
    println!("try: curl -s 'localhost:3000/queues/emails/dead?limit=5'");
    axum::serve(listener, app).await?;
    Ok(())
}
