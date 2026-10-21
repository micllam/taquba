//! The entries of the inbox: the kind trait, the entry types of the core kinds
//! and the validation of the kind and id segments of an entry's path.

use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::effects::EnqueueRequest;
use crate::error::{Characters, Error, Form, Result};
use crate::options::EnqueueOptions;
use crate::queue::{CancelOutcome, EnqueueResult, SignalOutcome};

/// Maximum byte length of an inbox entry id and of a kind name.
pub const MAX_INBOX_ID_LEN: usize = 64;

/// An entry type of the inbox: the kind name, the JSON body of an entry and the
/// outcome recorded for an applied entry. The core defines [`EnqueueEntry`],
/// [`CancelEntry`] and [`SignalEntry`], and a layer defines a type per kind it
/// registers with
/// [`Queue::register_inbox_handler`](crate::Queue::register_inbox_handler). A
/// bytes field of a body is base64 through [`inbox_bytes`].
pub trait InboxKind: Serialize + DeserializeOwned + Send + 'static {
    /// The kind name: `[a-z][a-z0-9._-]*`, at most [`MAX_INBOX_ID_LEN`] bytes.
    const KIND: &'static str;
    /// The outcome of an applied entry, in its
    /// [`InboxRecord`](crate::InboxRecord).
    type Outcome: Serialize + DeserializeOwned + Send;
}

/// Serde adapter for a bytes field of an entry body, stored as standard base64
/// with padding: `#[serde(with = "taquba::inbox_bytes")]`.
pub mod inbox_bytes {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use serde::{Deserialize, Deserializer, Serializer};

    /// Serialize `bytes` as a base64 string.
    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    /// Deserialize a base64 string into bytes.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let encoded = String::deserialize(deserializer)?;
        STANDARD.decode(encoded).map_err(serde::de::Error::custom)
    }
}

/// The entry of the core kind `enqueue`: a job for a queue in
/// [`InboxOptions::queues`](crate::InboxOptions::queues). Its outcome is the
/// [`EnqueueResult`] of the enqueue.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EnqueueEntry {
    /// The queue of the job.
    pub queue: String,
    /// The payload of the job, base64 in the body.
    #[serde(with = "inbox_bytes")]
    pub payload: Vec<u8>,
    /// [`EnqueueOptions::headers`](crate::EnqueueOptions::headers).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
    /// [`EnqueueOptions::run_at`](crate::EnqueueOptions::run_at), as
    /// milliseconds from the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_at: Option<u64>,
    /// [`EnqueueOptions::priority`](crate::EnqueueOptions::priority).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u32>,
    /// [`EnqueueOptions::max_attempts`](crate::EnqueueOptions::max_attempts).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
    /// [`EnqueueOptions::dedup_key`](crate::EnqueueOptions::dedup_key).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedup_key: Option<String>,
    /// [`EnqueueOptions::id_override`](crate::EnqueueOptions::id_override).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    /// [`EnqueueOptions::signal_wait`](crate::EnqueueOptions::signal_wait).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub signal_wait: bool,
}

impl EnqueueEntry {
    /// An entry for `queue` with `payload` and every option unset.
    pub fn new(queue: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            queue: queue.into(),
            payload: payload.into(),
            ..Self::default()
        }
    }
}

/// The entry of `request`. A `run_at` before the Unix epoch becomes 0.
impl From<EnqueueRequest> for EnqueueEntry {
    fn from(request: EnqueueRequest) -> Self {
        let options = request.options;
        Self {
            queue: request.queue,
            payload: request.payload,
            headers: options.headers,
            run_at: options.run_at.map(|at| {
                at.duration_since(UNIX_EPOCH)
                    .map_or(0, |since| since.as_millis() as u64)
            }),
            priority: options.priority,
            max_attempts: options.max_attempts,
            dedup_key: options.dedup_key,
            job_id: options.id_override,
            signal_wait: options.signal_wait,
        }
    }
}

impl From<EnqueueEntry> for EnqueueRequest {
    fn from(entry: EnqueueEntry) -> Self {
        Self {
            queue: entry.queue,
            payload: entry.payload,
            options: EnqueueOptions {
                max_attempts: entry.max_attempts,
                priority: entry.priority,
                run_at: entry
                    .run_at
                    .map(|ms| UNIX_EPOCH + Duration::from_millis(ms)),
                dedup_key: entry.dedup_key,
                headers: entry.headers,
                id_override: entry.job_id,
                signal_wait: entry.signal_wait,
            },
        }
    }
}

impl InboxKind for EnqueueEntry {
    const KIND: &'static str = "enqueue";
    type Outcome = EnqueueResult;
}

/// The entry of the core kind `cancel`: [`Queue::cancel`](crate::Queue::cancel)
/// of a job on a queue in [`InboxOptions::queues`](crate::InboxOptions::queues).
/// Its outcome is the [`CancelOutcome`] of the cancel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelEntry {
    /// The id of the job.
    pub job_id: String,
}

impl InboxKind for CancelEntry {
    const KIND: &'static str = "cancel";
    type Outcome = CancelOutcome;
}

/// The entry of the core kind `signal`: [`Queue::signal`](crate::Queue::signal)
/// to the job `job_id`. Its outcome is the [`SignalOutcome`] of the signal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalEntry {
    /// The id of the waiting job.
    pub job_id: String,
    /// The payload of the signal, base64 in the body.
    #[serde(with = "inbox_bytes")]
    pub payload: Vec<u8>,
}

impl InboxKind for SignalEntry {
    const KIND: &'static str = "signal";
    type Outcome = SignalOutcome;
}

/// Whether `kind` is a kind that the core applies.
pub(crate) fn is_core_kind(kind: &str) -> bool {
    [EnqueueEntry::KIND, CancelEntry::KIND, SignalEntry::KIND].contains(&kind)
}

const KIND_FORM: Form = Form {
    max: MAX_INBOX_ID_LEN,
    start: Some(Characters {
        test: |c| c.is_ascii_lowercase(),
        allowed: "a lower-case letter",
    }),
    characters: Characters {
        test: |c| c.is_ascii_lowercase() || c.is_ascii_digit() || "._-".contains(c),
        allowed: "`[a-z0-9._-]`",
    },
};

const ID_FORM: Form = Form {
    max: MAX_INBOX_ID_LEN,
    start: None,
    characters: Characters {
        test: |c| c.is_ascii_alphanumeric() || "_-".contains(c),
        allowed: "`[A-Za-z0-9_-]`",
    },
};

/// Validate a kind name: `[a-z][a-z0-9._-]*`, at most [`MAX_INBOX_ID_LEN`]
/// bytes.
pub(crate) fn validate_kind(kind: &str) -> Result<()> {
    KIND_FORM
        .check(kind)
        .map_err(|reason| Error::InvalidInboxKind {
            kind: kind.to_string(),
            reason,
        })
}

/// Validate an entry id: `[A-Za-z0-9_-]`, 1 to [`MAX_INBOX_ID_LEN`] bytes.
pub(crate) fn validate_id(id: &str) -> Result<()> {
    ID_FORM.check(id).map_err(|reason| Error::InvalidId {
        id: id.to_string(),
        reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_kind_is_lower_case_and_starts_with_a_letter() {
        for kind in ["enqueue", "workflow.submit", "a-b_c9"] {
            validate_kind(kind).unwrap();
        }
        for bad in ["", "Enqueue", "1x", "a/b", "a b", &"k".repeat(65)] {
            assert!(
                matches!(validate_kind(bad), Err(Error::InvalidInboxKind { .. })),
                "`{bad}` must be rejected"
            );
        }
    }

    #[test]
    fn an_id_is_a_path_segment_of_at_most_64_bytes() {
        validate_id("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        validate_id(&"a".repeat(64)).unwrap();
        for bad in ["", "a/b", "a.b", &"a".repeat(65)] {
            assert!(
                matches!(validate_id(bad), Err(Error::InvalidId { .. })),
                "`{bad}` must be rejected"
            );
        }
    }

    #[test]
    fn a_bytes_field_is_base64_in_the_body() {
        let entry = SignalEntry {
            job_id: "j".to_string(),
            payload: vec![0, 255, 10],
        };
        let body = serde_json::to_value(&entry).unwrap();
        assert_eq!(body["payload"], "AP8K");
        let decoded: SignalEntry = serde_json::from_value(body).unwrap();
        assert_eq!(decoded.payload, vec![0, 255, 10]);
        assert!(serde_json::from_str::<SignalEntry>(r#"{"job_id":"j","payload":"@@"}"#).is_err());
    }

    #[test]
    fn an_enqueue_request_converts_to_an_entry_and_back_unchanged() {
        let request = EnqueueRequest {
            queue: "work".to_string(),
            payload: b"p".to_vec(),
            options: EnqueueOptions {
                max_attempts: Some(3),
                priority: Some(1),
                run_at: Some(UNIX_EPOCH + Duration::from_millis(1_700_000_000_123)),
                dedup_key: Some("d".to_string()),
                headers: HashMap::from([("h".to_string(), "v".to_string())]),
                id_override: Some("j".to_string()),
                signal_wait: true,
            },
        };
        let back = EnqueueRequest::from(EnqueueEntry::from(request.clone()));
        assert_eq!(back.queue, request.queue);
        assert_eq!(back.payload, request.payload);
        assert_eq!(
            format!("{:?}", back.options),
            format!("{:?}", request.options)
        );
    }
}
