//! Binary key encoding for the queue's internal key spaces.
//!
//! Every internal key is `[tag, version, fields...]`: a [`KeyTag`]
//! discriminator byte, the key-format version byte and the key space's
//! fields. Fields use order-preserving encodings: timestamps are
//! big-endian `u64`, priorities are big-endian `u32`, and a queue name
//! that is followed by further fields is preceded by its length as one
//! byte (queue names are limited to [`MAX_QUEUE_NAME_LEN`] bytes).
//! Job IDs are stored as their byte form and always occupy the tail of
//! a key.
//!
//! Scan layouts follow one rule: the field the scan orders by comes
//! first. The `Claimed`, `Scheduled` and `Done` spaces lead with a
//! timestamp so the reaper, scheduler and retention sweeps perform one
//! global scan with an early exit; the `Pending` space leads with the
//! queue name so claims scan one queue, ordered by priority and then
//! by ID.
//!
//! User KV keys are `[KeyTag::User, caller bytes]` with no version
//! byte: caller bytes are opaque data, not a schema this module owns.

/// Maximum byte length of a queue name, imposed by the one-byte length
/// field in key encodings.
pub const MAX_QUEUE_NAME_LEN: usize = 255;

/// Version byte written into every internal key after the tag.
/// `0x00` is reserved as invalid.
pub(crate) const KEY_VERSION: u8 = 1;

/// Key-space discriminator: the first byte of every stored key.
/// `0x00` is reserved as invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum KeyTag {
    /// `[tag, ver, qlen, queue, priority u32 BE, id]`
    Pending = 0x01,
    /// `[tag, ver, lease_expires_at u64 BE, qlen, queue, id]`
    Claimed = 0x02,
    /// `[tag, ver, run_at u64 BE, qlen, queue, id]`
    Scheduled = 0x03,
    /// `[tag, ver, completed_at u64 BE, qlen, queue, id]`
    Done = 0x04,
    /// `[tag, ver, qlen, queue, id]`
    Dead = 0x05,
    /// `[tag, ver, id]`; the value is the job's current primary key.
    JobIndex = 0x06,
    /// `[tag, ver, qlen, queue, dedup key bytes]`
    Dedup = 0x07,
    /// `[tag, ver, queue]`
    Cursor = 0x08,
    /// `[tag, ver, qlen, queue, metric name]`
    Stats = 0x09,
    /// `[tag, caller bytes]`; no version byte, caller bytes are opaque.
    User = 0xFF,
}

impl KeyTag {
    /// The tag byte value.
    pub(crate) fn id(self) -> u8 {
        self as u8
    }
}

/// `[tag, version]` header shared by every internal key.
fn header(tag: KeyTag) -> [u8; 2] {
    [tag.id(), KEY_VERSION]
}

/// Length-prefixed queue name field. Callers validate the length via
/// [`MAX_QUEUE_NAME_LEN`] at the API boundary.
fn push_queue(out: &mut Vec<u8>, queue: &str) {
    debug_assert!(queue.len() <= MAX_QUEUE_NAME_LEN);
    out.push(queue.len() as u8);
    out.extend_from_slice(queue.as_bytes());
}

pub(crate) fn pending_key(queue: &str, priority: u32, id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 + 1 + queue.len() + 4 + id.len());
    k.extend_from_slice(&header(KeyTag::Pending));
    push_queue(&mut k, queue);
    k.extend_from_slice(&priority.to_be_bytes());
    k.extend_from_slice(id.as_bytes());
    k
}

pub(crate) fn pending_prefix(queue: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 + 1 + queue.len());
    k.extend_from_slice(&header(KeyTag::Pending));
    push_queue(&mut k, queue);
    k
}

fn time_first_key(tag: KeyTag, ts: u64, queue: &str, id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 + 8 + 1 + queue.len() + id.len());
    k.extend_from_slice(&header(tag));
    k.extend_from_slice(&ts.to_be_bytes());
    push_queue(&mut k, queue);
    k.extend_from_slice(id.as_bytes());
    k
}

pub(crate) fn claimed_key(queue: &str, lease_expires_at: u64, id: &str) -> Vec<u8> {
    time_first_key(KeyTag::Claimed, lease_expires_at, queue, id)
}

pub(crate) fn scheduled_key(queue: &str, run_at: u64, id: &str) -> Vec<u8> {
    time_first_key(KeyTag::Scheduled, run_at, queue, id)
}

pub(crate) fn done_key(completed_at: u64, queue: &str, id: &str) -> Vec<u8> {
    time_first_key(KeyTag::Done, completed_at, queue, id)
}

pub(crate) fn dead_key(queue: &str, id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 + 1 + queue.len() + id.len());
    k.extend_from_slice(&header(KeyTag::Dead));
    push_queue(&mut k, queue);
    k.extend_from_slice(id.as_bytes());
    k
}

pub(crate) fn dead_prefix(queue: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 + 1 + queue.len());
    k.extend_from_slice(&header(KeyTag::Dead));
    push_queue(&mut k, queue);
    k
}

pub(crate) fn job_index_key(id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 + id.len());
    k.extend_from_slice(&header(KeyTag::JobIndex));
    k.extend_from_slice(id.as_bytes());
    k
}

pub(crate) fn dedup_index_key(queue: &str, key: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 + 1 + queue.len() + key.len());
    k.extend_from_slice(&header(KeyTag::Dedup));
    push_queue(&mut k, queue);
    k.extend_from_slice(key.as_bytes());
    k
}

pub(crate) fn cursor_key(queue: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 + queue.len());
    k.extend_from_slice(&header(KeyTag::Cursor));
    k.extend_from_slice(queue.as_bytes());
    k
}

pub(crate) fn stats_key(queue: &str, metric: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 + 1 + queue.len() + metric.len());
    k.extend_from_slice(&header(KeyTag::Stats));
    push_queue(&mut k, queue);
    k.extend_from_slice(metric.as_bytes());
    k
}

/// Scan prefix covering an entire key space: `[tag, version]`.
pub(crate) fn tag_prefix(tag: KeyTag) -> [u8; 2] {
    header(tag)
}

/// Scope a caller-supplied KV key under the user tag.
pub(crate) fn user_scoped_key(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + key.len());
    out.push(KeyTag::User as u8);
    out.extend_from_slice(key);
    out
}

/// Parse the leading big-endian timestamp from a time-first key of the
/// given tag. Returns `None` when the key is not in that key space, is
/// not the current key version or is too short.
///
/// Used by the reaper, scheduler and retention sweeps to early-exit a
/// prefix scan once they reach a key whose timestamp is past the
/// relevant cutoff.
pub(crate) fn parse_key_timestamp(key: &[u8], tag: KeyTag) -> Option<u64> {
    let rest = key.strip_prefix(header(tag).as_slice())?;
    let ts: [u8; 8] = rest.get(..8)?.try_into().ok()?;
    Some(u64::from_be_bytes(ts))
}

/// Parse `(queue, metric)` from a stats key. Returns `None` when the
/// key is not a current-version stats key or is malformed.
pub(crate) fn parse_stats_key(key: &[u8]) -> Option<(String, String)> {
    let rest = key.strip_prefix(header(KeyTag::Stats).as_slice())?;
    let (qlen, rest) = rest.split_first()?;
    let qlen = *qlen as usize;
    if rest.len() < qlen {
        return None;
    }
    let queue = std::str::from_utf8(&rest[..qlen]).ok()?;
    let metric = std::str::from_utf8(&rest[qlen..]).ok()?;
    Some((queue.to_string(), metric.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_keys_order_by_priority_then_id_within_a_queue() {
        let prefix = pending_prefix("q");
        let high = pending_key("q", 100, "01A");
        let normal_a = pending_key("q", 1_000, "01A");
        let normal_b = pending_key("q", 1_000, "01B");
        assert!(high.starts_with(&prefix));
        assert!(high < normal_a);
        assert!(normal_a < normal_b);
    }

    #[test]
    fn pending_prefixes_of_nested_queue_names_do_not_collide() {
        let key = pending_key("ab", 1_000, "01A");
        assert!(!key.starts_with(&pending_prefix("a")));
        assert!(key.starts_with(&pending_prefix("ab")));
    }

    #[test]
    fn time_first_keys_order_globally_by_timestamp() {
        let earlier = scheduled_key("zzz", 100, "01A");
        let later = scheduled_key("aaa", 200, "01A");
        assert!(earlier < later);
        assert!(earlier.starts_with(&tag_prefix(KeyTag::Scheduled)));
    }

    #[test]
    fn parse_key_timestamp_round_trips() {
        let key = claimed_key("work", 1_700_000_000_123, "abc");
        assert_eq!(
            parse_key_timestamp(&key, KeyTag::Claimed),
            Some(1_700_000_000_123)
        );
    }

    #[test]
    fn parse_key_timestamp_rejects_other_tags_and_short_keys() {
        let key = claimed_key("work", 42, "abc");
        assert_eq!(parse_key_timestamp(&key, KeyTag::Done), None);
        assert_eq!(parse_key_timestamp(&key[..6], KeyTag::Claimed), None);
    }

    #[test]
    fn stats_key_round_trips_queue_and_metric() {
        let key = stats_key("email", "pending");
        assert_eq!(
            parse_stats_key(&key),
            Some(("email".to_string(), "pending".to_string()))
        );
    }

    #[test]
    fn key_spaces_do_not_overlap() {
        let keys = [
            pending_key("q", 1, "id"),
            claimed_key("q", 1, "id"),
            scheduled_key("q", 1, "id"),
            done_key(1, "q", "id"),
            dead_key("q", "id"),
            job_index_key("id"),
            dedup_index_key("q", "id"),
            cursor_key("q"),
            stats_key("q", "m"),
            user_scoped_key(b"id"),
        ];
        for (i, a) in keys.iter().enumerate() {
            for (j, b) in keys.iter().enumerate() {
                if i != j {
                    assert!(!a.starts_with(b.as_slice()), "{i} collides with {j}");
                }
            }
        }
    }
}
