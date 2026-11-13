//! The runtime's reserved namespaces: the `workflow.` header keys, the
//! `workflow/` prefix in the caller KV namespace and the builders of the
//! durable keys within it, the step dedup-key prefix and the validated
//! [`RunId`].

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt;
use std::ops::{Deref, Range};
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};
use taquba::{EnqueueRequest, Queue, SettlementEffects};

use crate::error::{Error, Result};
use crate::owner::OwnerRef;

/// Header key carrying the run identifier on every step job.
pub const HEADER_RUN_ID: &str = "workflow.run_id";
/// Header key carrying the zero-based step number on every step job.
pub const HEADER_STEP: &str = "workflow.step";
/// Reserved prefix the runtime owns on job headers. A header of a submission,
/// or of an enqueue among the effects of a caller, must not start with this
/// prefix. Such a header is rejected with [`Error::ReservedHeader`].
pub const RESERVED_HEADER_PREFIX: &str = "workflow.";

/// Reserved prefix the runtime owns in the caller KV namespace. Keys passed via
/// [`RunSpec::effects`](crate::RunSpec::effects) or staged through an
/// [`crate::EffectsHandle`] must not start with this prefix. Such a key is
/// rejected with [`Error::ReservedKvKey`].
pub const RESERVED_KV_PREFIX: &str = "workflow/";

/// Fails with [`Error::ReservedKvKey`] for a caller KV key with the reserved
/// prefix.
pub(crate) fn check_kv_key(key: &[u8]) -> Result<()> {
    if key.starts_with(RESERVED_KV_PREFIX.as_bytes()) {
        return Err(Error::ReservedKvKey(
            String::from_utf8_lossy(key).into_owned(),
        ));
    }
    Ok(())
}

/// Reserved prefix the runtime owns in the count names of the queue. The name
/// of a count changed through [`RunSpec::effects`](crate::RunSpec::effects)
/// must not start with this prefix, and such a count is rejected with
/// [`Error::ReservedCountName`].
pub const RESERVED_COUNT_PREFIX: &str = "workflow/";

/// Fails with [`Error::ReservedCountName`] for a caller count name with the
/// reserved prefix.
fn check_count_name(name: &str) -> Result<()> {
    if name.starts_with(RESERVED_COUNT_PREFIX) {
        return Err(Error::ReservedCountName(name.to_string()));
    }
    Ok(())
}

/// The headers that the submitter set on a step job: `headers` without those
/// with [`RESERVED_HEADER_PREFIX`].
pub(crate) fn submitter_headers(headers: &HashMap<String, String>) -> HashMap<String, String> {
    headers
        .iter()
        .filter(|(name, _)| !name.starts_with(RESERVED_HEADER_PREFIX))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

/// Fails with [`Error::ReservedHeader`] for a caller header with the reserved
/// prefix.
pub(crate) fn check_headers(headers: &HashMap<String, String>) -> Result<()> {
    match headers
        .keys()
        .find(|name| name.starts_with(RESERVED_HEADER_PREFIX))
    {
        Some(name) => Err(Error::ReservedHeader(name.clone())),
        None => Ok(()),
    }
}

/// Checks an enqueue of a caller: its headers, and its queue against
/// `runtime_queue`, the queue of the runtime. Fails with
/// [`Error::ReservedHeader`] or [`Error::ReservedQueue`].
pub(crate) fn check_enqueue(request: &EnqueueRequest, runtime_queue: Option<&str>) -> Result<()> {
    if runtime_queue == Some(request.queue.as_str()) {
        return Err(Error::ReservedQueue(request.queue.clone()));
    }
    check_headers(&request.options.headers)
}

/// Checks every KV key and every enqueue of the effects of a caller of the
/// runtime with the queue `runtime_queue`, and rejects a compare.
pub(crate) fn check_effects(effects: &SettlementEffects, runtime_queue: &str) -> Result<()> {
    if !effects.kv_compares.is_empty() {
        return Err(Error::KvCompareInEffects);
    }
    let deletes = effects.kv_deletes.iter();
    for key in effects.kv_writes.keys().chain(deletes) {
        check_kv_key(key)?;
    }
    for name in effects.count_changes.keys() {
        check_count_name(name)?;
    }
    for request in &effects.enqueues {
        check_enqueue(request, Some(runtime_queue))?;
    }
    Ok(())
}

/// Header key identifying the group a run is a member of, set on every step job
/// of a grouped run.
pub const HEADER_GROUP: &str = "workflow.group";
/// Header key identifying a grouped run's member key within its group, set
/// together with [`HEADER_GROUP`].
pub const HEADER_GROUP_KEY: &str = "workflow.group_key";

pub(crate) const DEDUP_PREFIX: &str = "run:";

/// Maximum byte length of a [`RunId`], the limit Taquba applies to a
/// caller-supplied job id.
pub const MAX_RUN_ID_LEN: usize = 128;

/// Prefix for the durable record of a run in Taquba's user KV namespace: the
/// run record of an active run and the terminal record of a terminated one.
pub(crate) const RUN_KV_PREFIX: &[u8] = b"workflow/runs/";

/// Prefix for the durable current-step pointer: `run id -> (step, job id)` of
/// the queue job currently representing the run, written with the step-0
/// enqueue, rewritten with every advance and deleted with the run's
/// termination.
pub(crate) const STEP_KV_PREFIX: &[u8] = b"workflow/steps/";

/// Prefix of the terminal markers of runs, read by the memo retention sweep:
/// entries of a [`taquba::ExpiryIndex`] with the run id as the suffix.
pub(crate) const TERMINAL_KV_PREFIX: &[u8] = b"workflow/terminals/";

/// Prefix of the run KV entries: `workflow/run-kv/{run_id}/{key}`, staged
/// through [`EffectsHandle::put_run_kv`](crate::EffectsHandle::put_run_kv) and
/// removed with the run by the memo sweep and the group sweep.
pub(crate) const RUN_KV_ENTRY_PREFIX: &[u8] = b"workflow/run-kv/";

/// Prefix for the durable member records of run groups:
/// `workflow/groups/{group_id}/{key}`, one per member, written with the
/// member's submission and rewritten in the settlement that terminates it.
pub(crate) const GROUP_KV_PREFIX: &[u8] = b"workflow/groups/";

/// Prefix of the index of owned runs:
/// `workflow/owned/{owner}:{submission_id}/{queue}/{run_id}`, written with the
/// submission of each owned run and deleted with its termination. A queue name
/// does not contain `/`, so the prefix of one queue contains its entries alone.
pub(crate) const OWNED_KV_PREFIX: &[u8] = b"workflow/owned/";

/// Prefix of the queues with owned runs of each owner:
/// `workflow/owner-queues/{owner}/{submission_id}`, removed with the owner by
/// the memo sweep and the group sweep.
pub(crate) const OWNER_QUEUES_KV_PREFIX: &[u8] = b"workflow/owner-queues/";

/// The index entry of the run `run_id` on `queue`, owned by `owner`.
pub(crate) fn owned_run_kv_key(owner: &OwnerRef, queue: &str, run_id: &RunId) -> Vec<u8> {
    prefixed(
        OWNED_KV_PREFIX,
        &format!("{}/{queue}/{run_id}", owner.header_value()),
    )
}

/// The range of the index entries of the runs on `queue` owned by `owner`, from
/// their prefix to the prefix with `0`, the byte after `/`, in its place.
pub(crate) fn owned_runs_range(owner: &OwnerRef, queue: &str) -> Range<Vec<u8>> {
    let owner = owner.header_value();
    let start = prefixed(OWNED_KV_PREFIX, &format!("{owner}/{queue}/"));
    let end = prefixed(OWNED_KV_PREFIX, &format!("{owner}/{queue}0"));
    start..end
}

/// The key of the queues with owned runs of `owner`.
pub(crate) fn owner_queues_kv_key(owner: &OwnerRef) -> Vec<u8> {
    prefixed(
        OWNER_QUEUES_KV_PREFIX,
        &format!("{}/{}", owner.run_id, owner.submission_id),
    )
}

/// The range of the keys of the queues of every submission of `run_id` as an
/// owner.
pub(crate) fn owner_queues_range(run_id: &RunId) -> Range<Vec<u8>> {
    let start = prefixed(OWNER_QUEUES_KV_PREFIX, &format!("{run_id}/"));
    let end = prefixed(OWNER_QUEUES_KV_PREFIX, &format!("{run_id}0"));
    start..end
}

/// Prefix under which the member records of one group are stored.
pub(crate) fn group_members_kv_prefix(group_id: &RunId) -> Vec<u8> {
    prefixed(GROUP_KV_PREFIX, &format!("{group_id}/"))
}

/// Key of the member record of `key` in group `group_id`.
pub(crate) fn group_member_kv_key(group_id: &RunId, key: &str) -> Vec<u8> {
    prefixed(&group_members_kv_prefix(group_id), key)
}

/// The name of the count of the active members of the group `group_id`.
pub(crate) fn group_count_name(group_id: &RunId) -> String {
    format!("{GROUP_COUNT_PREFIX}{group_id}")
}

/// The group of the count name `name`, when [`group_count_name`] built it.
pub(crate) fn group_of_count_name(name: &str) -> Option<RunId> {
    RunId::new(name.strip_prefix(GROUP_COUNT_PREFIX)?).ok()
}

/// The prefix of the names of the counts of groups.
const GROUP_COUNT_PREFIX: &str = "workflow/groups/";

/// Prefix of the terminal markers of run groups, read by the group retention
/// sweep: entries of a [`taquba::ExpiryIndex`] with the group id as the suffix.
pub(crate) const GROUP_TERMINAL_KV_PREFIX: &[u8] = b"workflow/group-terminals/";

/// The SHA-256 digest of `input`.
pub(crate) fn hash_input(input: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(input).into()
}

/// The job id of the step that follows the step job `job_id`: 32 lower-case hex
/// characters of a digest of the id, the same on every attempt of the step.
pub(crate) fn next_step_job_id(job_id: &str) -> String {
    let mut id = hex_sha256(&[b"workflow/next-step/", job_id.as_bytes()]);
    id.truncate(32);
    id
}

/// The lower-case hex SHA-256 digest of `parts` concatenated.
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

/// A run id: 1 to [`MAX_RUN_ID_LEN`] bytes of `[A-Za-z0-9_-]`. A run id is a
/// path segment in the memo store and a key segment in the queue's KV
/// namespace, so it is restricted to the characters Taquba accepts in a
/// caller-supplied job id. `RunId` is the parameter type of every key and path
/// builder, so a key over an unvalidated id does not compile. An id is
/// validated by [`RunId::new`], by [`str::parse`] and by deserialization. A
/// group id is a `RunId` as well, because keys store it in the positions that
/// hold a run id. The type dereferences to `str` and implements
/// `PartialEq<str>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct RunId(String);

impl RunId {
    /// Validate `id`. An empty id, an id over [`MAX_RUN_ID_LEN`] bytes or one
    /// with a character outside `[A-Za-z0-9_-]` is [`Error::InvalidRunId`].
    pub fn new(id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        // An empty id is the memo prefix itself, and the sweep then clears
        // every run's entries.
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

    /// A generated id: a ULID from [`Queue::next_job_id`] of `queue`.
    pub(crate) fn generate(queue: &Queue) -> Self {
        Self(queue.next_job_id())
    }

    /// The id that is the lower-case hex SHA-256 digest of `parts`
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

/// The prefix of the run KV entries of `run_id`. A run id does not contain `/`,
/// so the prefix of one run does not contain the entries of another.
fn run_kv_entries_prefix(run_id: &RunId) -> Vec<u8> {
    prefixed(RUN_KV_ENTRY_PREFIX, &format!("{run_id}/"))
}

/// The range of the run KV entries of `run_id` within [`RUN_KV_ENTRY_PREFIX`],
/// from its prefix to the prefix with `0`, the byte after `/`, in its place.
pub(crate) fn run_kv_entries_range(run_id: &RunId) -> Range<Vec<u8>> {
    let start = run_kv_entries_prefix(run_id);
    let end = prefixed(RUN_KV_ENTRY_PREFIX, &format!("{run_id}0"));
    start..end
}

/// The key of the run KV entry `key` of `run_id`.
pub(crate) fn run_kv_entry_key(run_id: &RunId, key: &[u8]) -> Vec<u8> {
    [run_kv_entries_prefix(run_id).as_slice(), key].concat()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_prefixes_are_under_the_reserved_prefixes() {
        for prefix in [
            RUN_KV_PREFIX,
            STEP_KV_PREFIX,
            TERMINAL_KV_PREFIX,
            RUN_KV_ENTRY_PREFIX,
            GROUP_KV_PREFIX,
            GROUP_TERMINAL_KV_PREFIX,
            OWNED_KV_PREFIX,
            OWNER_QUEUES_KV_PREFIX,
        ] {
            assert!(
                prefix.starts_with(RESERVED_KV_PREFIX.as_bytes()),
                "internal kv prefix `{}` is outside the reserved prefix",
                String::from_utf8_lossy(prefix),
            );
        }
        assert!(GROUP_COUNT_PREFIX.starts_with(RESERVED_COUNT_PREFIX));
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
        assert!(matches!(
            "run/1".parse::<RunId>(),
            Err(Error::InvalidRunId { .. })
        ));
        assert_eq!("run-1".parse::<RunId>().unwrap(), "run-1");
        assert!(crate::durable::decode::<RunId>(&rmp_serde::to_vec("").unwrap()).is_err());
        assert_eq!(
            crate::durable::decode::<RunId>(&rmp_serde::to_vec("run-1").unwrap()).unwrap(),
            "run-1"
        );
    }
}
