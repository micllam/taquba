//! The entries of the inbox: the kind trait, the entry types of the core kinds
//! and the validation of the kind and id segments of an entry's path.

use std::collections::HashMap;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::error::{Error, Result};

/// Maximum byte length of an inbox entry id and of a kind name.
pub const MAX_INBOX_ID_LEN: usize = 64;

/// An entry type of the inbox: the kind name and the JSON body of an entry. The
/// core defines [`EnqueueEntry`], [`CancelEntry`] and [`SignalEntry`], and a
/// layer defines a type per kind it registers with
/// [`Queue::register_inbox_handler`](crate::Queue::register_inbox_handler). A
/// bytes field of a body is base64 through [`inbox_bytes`].
pub trait InboxKind: Serialize + DeserializeOwned {
    /// The kind name: `[a-z][a-z0-9._-]*`, at most [`MAX_INBOX_ID_LEN`] bytes.
    const KIND: &'static str;
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
/// [`InboxOptions::queues`](crate::InboxOptions::queues). The outcome recorded
/// for the entry is `{"job_id": id, "new": true}`, or `{"job_id": existing,
/// "new": false}` when `dedup_key` matched a pending or scheduled job.
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

impl InboxKind for EnqueueEntry {
    const KIND: &'static str = "enqueue";
}

/// The entry of the core kind `cancel`: [`Queue::cancel`](crate::Queue::cancel)
/// of a job on a queue in [`InboxOptions::queues`](crate::InboxOptions::queues).
/// The outcome recorded for the entry is `{"outcome": "removed"}`,
/// `{"outcome": "requested"}` or `{"outcome": "not_found"}`, as
/// [`CancelOutcome`](crate::CancelOutcome) reports them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelEntry {
    /// The id of the job.
    pub job_id: String,
}

impl InboxKind for CancelEntry {
    const KIND: &'static str = "cancel";
}

/// The entry of the core kind `signal`: [`Queue::signal`](crate::Queue::signal)
/// to a signal key. The outcome recorded for the entry is
/// `{"outcome": "delivered"}` or `{"outcome": "buffered"}`, as
/// [`SignalOutcome`](crate::SignalOutcome) reports them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalEntry {
    /// The signal key.
    pub key: String,
    /// The payload of the signal, base64 in the body.
    #[serde(with = "inbox_bytes")]
    pub payload: Vec<u8>,
}

impl InboxKind for SignalEntry {
    const KIND: &'static str = "signal";
}

/// Whether `kind` is a kind that the core applies.
pub(crate) fn is_core_kind(kind: &str) -> bool {
    [EnqueueEntry::KIND, CancelEntry::KIND, SignalEntry::KIND].contains(&kind)
}

/// Validate a kind name: `[a-z][a-z0-9._-]*`, at most [`MAX_INBOX_ID_LEN`]
/// bytes.
pub(crate) fn validate_kind(kind: &str) -> Result<()> {
    let reason = if kind.is_empty() {
        "kind must not be empty"
    } else if kind.len() > MAX_INBOX_ID_LEN {
        "kind exceeds the maximum length of 64 bytes"
    } else if !kind.as_bytes()[0].is_ascii_lowercase() {
        "kind must start with a lower-case letter"
    } else if !kind
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))
    {
        "kind must contain only `[a-z0-9._-]`"
    } else {
        return Ok(());
    };
    Err(Error::InvalidInboxKind {
        kind: kind.to_string(),
        reason,
    })
}

/// Validate an entry id: `[A-Za-z0-9_-]`, 1 to [`MAX_INBOX_ID_LEN`] bytes.
pub(crate) fn validate_id(id: &str) -> Result<()> {
    let reason = if id.is_empty() {
        "id must not be empty"
    } else if id.len() > MAX_INBOX_ID_LEN {
        "id exceeds the maximum length of 64 bytes"
    } else if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        "id must contain only `[A-Za-z0-9_-]`"
    } else {
        return Ok(());
    };
    Err(Error::InvalidId {
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
            key: "k".to_string(),
            payload: vec![0, 255, 10],
        };
        let body = serde_json::to_value(&entry).unwrap();
        assert_eq!(body["payload"], "AP8K");
        let decoded: SignalEntry = serde_json::from_value(body).unwrap();
        assert_eq!(decoded.payload, vec![0, 255, 10]);
        assert!(serde_json::from_str::<SignalEntry>(r#"{"key":"k","payload":"@@"}"#).is_err());
    }
}
