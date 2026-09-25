//! The runtime's reserved namespaces: the `workflow.` header keys, the
//! `workflow/` prefix in the caller KV namespace and the builders of the
//! durable keys within it, the step dedup-key prefix and the validated
//! [`RunId`].

use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};

use crate::error::{Error, Result};

/// Header key carrying the run identifier on every step job.
pub const HEADER_RUN_ID: &str = "workflow.run_id";
/// Header key carrying the zero-based step number on every step job.
pub const HEADER_STEP: &str = "workflow.step";
/// Reserved prefix the runtime owns on step-job headers. Submitter-supplied
/// headers must not start with this prefix; if they do, the runtime treats
/// them as its own and strips them before invoking the runner.
pub const RESERVED_HEADER_PREFIX: &str = "workflow.";

/// Reserved prefix the runtime owns in the caller KV namespace. Keys passed via
/// [`RunSpec::effects`](crate::RunSpec::effects) or staged through an
/// [`crate::EffectsHandle`] must not start with this prefix. Such a key is
/// rejected with [`Error::ReservedKvKey`].
pub const RESERVED_KV_PREFIX: &str = "workflow/";

/// Header key marking a job as a terminal-notification job, whose
/// payload is the run's committed outcome and whose worker is the
/// configured [`TerminalHook`](crate::TerminalHook).
pub const HEADER_TERMINAL: &str = "workflow.terminal";

/// Header key marking a step job as a signal waiter; the value is the
/// correlation key the waiter is registered under.
pub const HEADER_SIGNAL_WAIT: &str = "workflow.signal_wait";
/// Header key marking a step job whose signal was already consumed at the
/// previous step's settlement; the payload is read from the durable
/// delivered record.
pub const HEADER_SIGNAL_DELIVERED: &str = "workflow.signal_delivered";

/// Header key naming the group a run is a member of, set on every step
/// job of a grouped run.
pub const HEADER_GROUP: &str = "workflow.group";
/// Header key naming a grouped run's member key within its group, set
/// beside [`HEADER_GROUP`].
pub const HEADER_GROUP_KEY: &str = "workflow.group_key";

pub(crate) const DEDUP_PREFIX: &str = "run:";

/// Maximum byte length of a [`RunId`], the limit Taquba applies to a
/// caller-supplied job id.
pub const MAX_RUN_ID_LEN: usize = 128;

/// Prefix for the durable per-run record in Taquba's user KV namespace.
pub(crate) const RUN_KV_PREFIX: &[u8] = b"workflow/runs/";

/// Prefix for the durable current-step pointer: `run id -> (step, job id)`
/// of the queue job currently representing the run, written with the
/// step-0 enqueue, rewritten with every advance and deleted with the
/// run's termination.
pub(crate) const STEP_KV_PREFIX: &[u8] = b"workflow/steps/";

/// Prefix for the durable waiter index: `correlation key -> job id` of the
/// step job waiting on that key.
pub(crate) const SIGNAL_WAIT_KV_PREFIX: &[u8] = b"workflow/signal-wait/";

/// Prefix for the durable signal buffer: `correlation key -> payload` of a
/// signal that arrived while no waiter was registered.
pub(crate) const SIGNAL_BUF_KV_PREFIX: &[u8] = b"workflow/signal-buf/";

/// Prefix for the durable delivered record: `(run id, step) -> payload` of
/// a signal consumed on the waiter's behalf, read when that step is
/// claimed and deleted with its settlement.
pub(crate) const SIGNAL_DELIVERED_KV_PREFIX: &[u8] = b"workflow/signal-delivered/";

/// Prefix of the terminal markers of runs, read by the memo retention
/// sweep: entries of a [`taquba::ExpiryIndex`] with the run id as the
/// suffix.
pub(crate) const TERMINAL_KV_PREFIX: &[u8] = b"workflow/terminals/";

/// Prefix for the durable terminal record of a run:
/// `workflow/outcomes/{run_id}`, written in the settlement that
/// terminates the run and removed with the run's memo entries by the
/// memo sweep.
pub(crate) const OUTCOME_KV_PREFIX: &[u8] = b"workflow/outcomes/";

/// Prefix for the durable member records of run groups:
/// `workflow/groups/{group_id}/{key}`, one per member, written with the
/// member's submission and rewritten in the settlement that terminates
/// it.
pub(crate) const GROUP_KV_PREFIX: &[u8] = b"workflow/groups/";

/// Prefix under which the member records of one group are stored.
pub(crate) fn group_members_kv_prefix(group_id: &RunId) -> Vec<u8> {
    prefixed(GROUP_KV_PREFIX, &format!("{group_id}/"))
}

/// Key of the member record of `key` in group `group_id`.
pub(crate) fn group_member_kv_key(group_id: &RunId, key: &str) -> Vec<u8> {
    prefixed(&group_members_kv_prefix(group_id), key)
}

/// Prefix of the terminal markers of run groups, read by the group
/// retention sweep: entries of a [`taquba::ExpiryIndex`] with the group
/// id as the suffix.
pub(crate) const GROUP_TERMINAL_KV_PREFIX: &[u8] = b"workflow/group-terminals/";

/// The SHA-256 digest of `input`.
pub(crate) fn hash_input(input: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(input).into()
}

/// The lowercase hex SHA-256 digest of `parts` concatenated.
pub(crate) fn hex_sha256(parts: &[&[u8]]) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write;
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    let mut hex = String::with_capacity(64);
    for byte in hasher.finalize() {
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

/// A run id: 1 to [`MAX_RUN_ID_LEN`] bytes of `[A-Za-z0-9_-]`. A run id
/// is a path segment in the memo store and a key segment in the queue's
/// KV namespace, so it is restricted to the characters Taquba accepts
/// in a caller-supplied job id. `RunId` is the parameter type of every
/// key and path builder, so a key over an unvalidated id does not
/// compile. An id is validated by [`RunId::new`], by [`str::parse`] and
/// by deserialization. A group id is a `RunId` as well, because it is
/// stored at the same key positions. The type dereferences to `str`
/// and implements `PartialEq<str>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct RunId(String);

impl RunId {
    /// Validate `id`. An empty id, an id over [`MAX_RUN_ID_LEN`] bytes
    /// or one with a character outside `[A-Za-z0-9_-]` is
    /// [`Error::InvalidRunId`].
    pub fn new(id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        // An empty id is the memo prefix itself, and the sweep then
        // clears every run's entries.
        let reason = if id.is_empty() {
            "run id must not be empty"
        } else if id.len() > MAX_RUN_ID_LEN {
            "run id exceeds maximum length of 128 bytes"
        } else if !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            "run id must contain only `[A-Za-z0-9_-]`"
        } else {
            return Ok(Self(id));
        };
        Err(Error::InvalidRunId { run_id: id, reason })
    }

    /// A generated id: a ULID.
    pub(crate) fn generate() -> Self {
        Self(ulid::Ulid::new().to_string())
    }

    /// The id that is the lowercase hex SHA-256 digest of `parts`
    /// concatenated, valid by construction.
    pub(crate) fn digest(parts: &[&[u8]]) -> Self {
        Self(hex_sha256(parts))
    }

    /// The id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The id as an owned string.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl Deref for RunId {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for RunId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for RunId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for RunId {
    type Err = Error;

    fn from_str(id: &str) -> Result<Self> {
        Self::new(id)
    }
}

impl PartialEq<str> for RunId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for RunId {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<String> for RunId {
    fn eq(&self, other: &String) -> bool {
        &self.0 == other
    }
}

impl From<RunId> for String {
    fn from(id: RunId) -> Self {
        id.0
    }
}

impl<'de> Deserialize<'de> for RunId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let id = String::deserialize(deserializer)?;
        Self::new(id).map_err(serde::de::Error::custom)
    }
}

/// `{prefix}{suffix}`.
fn prefixed(prefix: &[u8], suffix: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(prefix.len() + suffix.len());
    k.extend_from_slice(prefix);
    k.extend_from_slice(suffix.as_bytes());
    k
}

pub(crate) fn run_kv_key(run_id: &RunId) -> Vec<u8> {
    prefixed(RUN_KV_PREFIX, run_id)
}

pub(crate) fn step_kv_key(run_id: &RunId) -> Vec<u8> {
    prefixed(STEP_KV_PREFIX, run_id)
}

pub(crate) fn outcome_kv_key(run_id: &RunId) -> Vec<u8> {
    prefixed(OUTCOME_KV_PREFIX, run_id)
}

pub(crate) fn signal_wait_kv_key(correlation_key: &str) -> Vec<u8> {
    prefixed(SIGNAL_WAIT_KV_PREFIX, correlation_key)
}

pub(crate) fn signal_buf_kv_key(correlation_key: &str) -> Vec<u8> {
    prefixed(SIGNAL_BUF_KV_PREFIX, correlation_key)
}

pub(crate) fn signal_delivered_kv_key(run_id: &RunId, step_number: u32) -> Vec<u8> {
    prefixed(
        SIGNAL_DELIVERED_KV_PREFIX,
        &format!("{run_id}/{step_number}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_kv_prefixes_are_under_the_reserved_prefix() {
        for prefix in [
            RUN_KV_PREFIX,
            STEP_KV_PREFIX,
            SIGNAL_WAIT_KV_PREFIX,
            SIGNAL_BUF_KV_PREFIX,
            SIGNAL_DELIVERED_KV_PREFIX,
            TERMINAL_KV_PREFIX,
            OUTCOME_KV_PREFIX,
            GROUP_KV_PREFIX,
            GROUP_TERMINAL_KV_PREFIX,
        ] {
            assert!(
                prefix.starts_with(RESERVED_KV_PREFIX.as_bytes()),
                "internal kv prefix `{}` is outside the reserved prefix",
                String::from_utf8_lossy(prefix),
            );
        }
    }

    #[test]
    fn run_id_rejects_empty_long_and_unsafe_ids() {
        for bad in [
            "",
            "run/1",
            "run 1",
            "run:1",
            &"a".repeat(MAX_RUN_ID_LEN + 1),
        ] {
            assert!(
                matches!(RunId::new(bad), Err(Error::InvalidRunId { .. })),
                "`{bad}` must be rejected",
            );
        }
        assert!(RunId::new("a".repeat(MAX_RUN_ID_LEN)).is_ok());
        assert!(rmp_serde::from_slice::<RunId>(&rmp_serde::to_vec("").unwrap()).is_err());
        assert_eq!(
            rmp_serde::from_slice::<RunId>(&rmp_serde::to_vec("run-1").unwrap()).unwrap(),
            "run-1"
        );
    }
}
