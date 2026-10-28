//! The entries of the inbox: the entry of each kind and the validation of the
//! id segment of an entry's path.

use serde::{Deserialize, Serialize};

use crate::codec::encode;
use crate::error::{Error, Form, Result};
use crate::inbox::record::Rejection;
use crate::options::EnqueueOptions;

/// Maximum byte length of an inbox entry id.
pub const MAX_INBOX_ID_LEN: usize = 64;

/// Maximum byte length of the object of an inbox entry. The writer rejects a
/// larger entry with [`Rejection::TooLarge`] without reading it.
pub const MAX_INBOX_ENTRY_SIZE: usize = 16 * 1024 * 1024;

/// An entry of the inbox, of the kind of its variant. The path of an entry
/// contains its queue and its kind, and the body contains the variant's entry
/// as a MessagePack map with field names. A sender in another language writes
/// the map of its kind:
///
/// - `enqueue`: the binary string `payload` and the map `options` with the
///   fields of [`EnqueueOptions`]. `max_attempts` and `priority` are integers,
///   `run_at` is an integer of milliseconds from the Unix epoch, `dedup_key`
///   and `id_override` are strings, `headers` is a map of strings and
///   `signal_wait` is a boolean.
/// - `cancel`: the string `job_id`.
/// - `signal`: the string `job_id` and the binary string `payload`.
///
/// The writer decodes an absent `options` and an absent or nil field of it as
/// the default. It ignores a field that it does not know.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboxEntry {
    /// An entry of the kind `enqueue`.
    Enqueue(EnqueueEntry),
    /// An entry of the kind `cancel`.
    Cancel(CancelEntry),
    /// An entry of the kind `signal`.
    Signal(SignalEntry),
}

/// The kind of an inbox entry, as the path of its object names it. The display
/// form is the segment of the path.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EntryKind {
    /// The kind `enqueue`.
    Enqueue,
    /// The kind `cancel`.
    Cancel,
    /// The kind `signal`.
    Signal,
    /// A kind that the writer does not know, with the segment of the path.
    Unknown(String),
}

impl EntryKind {
    /// The kind of the path segment `segment`.
    pub(crate) fn of_segment(segment: &str) -> Self {
        match segment {
            "enqueue" => Self::Enqueue,
            "cancel" => Self::Cancel,
            "signal" => Self::Signal,
            other => Self::Unknown(other.to_string()),
        }
    }

    /// The segment of the path.
    pub(crate) fn segment(&self) -> &str {
        match self {
            Self::Enqueue => "enqueue",
            Self::Cancel => "cancel",
            Self::Signal => "signal",
            Self::Unknown(segment) => segment,
        }
    }
}

impl std::fmt::Display for EntryKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.segment())
    }
}

impl InboxEntry {
    /// The kind of the entry.
    pub(crate) fn kind(&self) -> EntryKind {
        match self {
            Self::Enqueue(_) => EntryKind::Enqueue,
            Self::Cancel(_) => EntryKind::Cancel,
            Self::Signal(_) => EntryKind::Signal,
        }
    }

    /// The body of the entry.
    pub(crate) fn encode(&self) -> Vec<u8> {
        match self {
            Self::Enqueue(entry) => encode(entry),
            Self::Cancel(entry) => encode(entry),
            Self::Signal(entry) => encode(entry),
        }
    }

    /// The entry of `kind` with `body`. Fails with the reason of the rejection
    /// for an unknown kind or a body that does not decode.
    pub(crate) fn decode(kind: &EntryKind, body: &[u8]) -> std::result::Result<Self, Rejection> {
        let decoded = match kind {
            EntryKind::Enqueue => rmp_serde::from_slice(body).map(Self::Enqueue),
            EntryKind::Cancel => rmp_serde::from_slice(body).map(Self::Cancel),
            EntryKind::Signal => rmp_serde::from_slice(body).map(Self::Signal),
            EntryKind::Unknown(_) => return Err(Rejection::UnknownKind),
        };
        decoded.map_err(|err| Rejection::Malformed {
            detail: err.to_string(),
        })
    }
}

impl From<EnqueueEntry> for InboxEntry {
    fn from(entry: EnqueueEntry) -> Self {
        Self::Enqueue(entry)
    }
}

impl From<CancelEntry> for InboxEntry {
    fn from(entry: CancelEntry) -> Self {
        Self::Cancel(entry)
    }
}

impl From<SignalEntry> for InboxEntry {
    fn from(entry: SignalEntry) -> Self {
        Self::Signal(entry)
    }
}

/// The entry of the kind `enqueue`: a job for the queue of the entry. Its
/// outcome is [`InboxOutcome::Enqueue`](crate::InboxOutcome::Enqueue).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnqueueEntry {
    /// The payload of the job.
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    /// The options of the enqueue, default when the body omits them.
    #[serde(default)]
    pub options: EnqueueOptions,
}

impl EnqueueEntry {
    /// An entry with `payload` and every option unset.
    pub fn new(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            payload: payload.into(),
            ..Self::default()
        }
    }
}

/// The entry of the kind `cancel`: [`Queue::cancel`](crate::Queue::cancel) of a
/// job on the queue of the entry. Its outcome is
/// [`InboxOutcome::Cancel`](crate::InboxOutcome::Cancel).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelEntry {
    /// The id of the job.
    pub job_id: String,
}

/// The entry of the kind `signal`: [`Queue::signal`](crate::Queue::signal) to
/// the job `job_id` on the queue of the entry. Its outcome is
/// [`InboxOutcome::Signal`](crate::InboxOutcome::Signal).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalEntry {
    /// The id of the waiting job.
    pub job_id: String,
    /// The payload of the signal.
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

const ID_FORM: Form = Form::id(MAX_INBOX_ID_LEN);

/// Validate an entry id: `[A-Za-z0-9_-]`, 1 to [`MAX_INBOX_ID_LEN`] bytes.
pub(crate) fn validate_id(id: &str) -> Result<()> {
    ID_FORM.check(id).map_err(|reason| Error::InvalidId {
        id: id.to_string(),
        reason,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;

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
    fn a_body_is_a_map_with_field_names_and_binary_bytes() {
        let entry = InboxEntry::Signal(SignalEntry {
            job_id: "j".to_string(),
            payload: vec![0, 255, 10],
        });
        let mut expected = vec![0x82, 0xa6];
        expected.extend(b"job_id");
        expected.extend([0xa1, b'j', 0xa7]);
        expected.extend(b"payload");
        expected.extend([0xc4, 3, 0, 255, 10]);
        assert_eq!(entry.encode(), expected);
        assert_eq!(
            InboxEntry::decode(&EntryKind::Signal, &expected).unwrap(),
            entry
        );
    }

    #[test]
    fn a_body_decodes_absent_options_and_ignores_unknown_fields() {
        #[derive(Serialize)]
        struct Body {
            #[serde(with = "serde_bytes")]
            payload: Vec<u8>,
            unknown: u32,
            options: HashMap<&'static str, u32>,
        }
        let body = encode(&Body {
            payload: b"p".to_vec(),
            unknown: 1,
            options: HashMap::from([("priority", 2), ("unknown", 3)]),
        });
        let InboxEntry::Enqueue(entry) = InboxEntry::decode(&EntryKind::Enqueue, &body).unwrap()
        else {
            panic!("an enqueue entry");
        };
        assert_eq!(entry.options, EnqueueOptions::default().priority(2));
        #[derive(Serialize)]
        struct Bare {
            #[serde(with = "serde_bytes")]
            payload: Vec<u8>,
        }
        let bare = encode(&Bare {
            payload: b"p".to_vec(),
        });
        assert_eq!(
            InboxEntry::decode(&EntryKind::Enqueue, &bare).unwrap(),
            InboxEntry::Enqueue(EnqueueEntry::new(b"p"))
        );
    }

    #[test]
    fn run_at_is_milliseconds_from_the_epoch_and_zero_before_it() {
        let at = UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
        let entry = InboxEntry::Enqueue(EnqueueEntry {
            payload: Vec::new(),
            options: EnqueueOptions::default().run_at(at),
        });
        assert_eq!(
            InboxEntry::decode(&EntryKind::Enqueue, &entry.encode()).unwrap(),
            entry
        );
        let before = InboxEntry::Enqueue(EnqueueEntry {
            payload: Vec::new(),
            options: EnqueueOptions::default().run_at(UNIX_EPOCH - Duration::from_secs(1)),
        });
        let InboxEntry::Enqueue(decoded) =
            InboxEntry::decode(&EntryKind::Enqueue, &before.encode()).unwrap()
        else {
            panic!("an enqueue entry");
        };
        assert_eq!(decoded.options.run_at, Some(UNIX_EPOCH));
    }
}
