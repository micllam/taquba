//! The encoding of the crate's own records.

use serde::Serialize;

/// Encode one of the crate's own records as MessagePack with named fields. The
/// record types hold strings, bytes, integers, maps with string keys and
/// enumerations, whose encoding cannot fail.
pub(crate) fn encode<T: Serialize>(record: &T) -> Vec<u8> {
    rmp_serde::to_vec_named(record).expect("a record of the crate encodes")
}
