//! The generation of ULIDs that increase with call order.

use std::sync::Mutex;
use std::time::SystemTime;

use ulid::{Generator, Ulid};

/// A source of ULIDs, each above every ULID that it returned before.
pub(crate) struct MonotonicUlids(Mutex<Generator>);

impl MonotonicUlids {
    pub(crate) const fn new() -> Self {
        Self(Mutex::new(Generator::new()))
    }

    /// A ULID with the time `at`, or the last ULID incremented when `at` is not
    /// after its time. The increment fails only when the 80 random bits of a
    /// millisecond overflow, which takes 2^79 ULIDs on average.
    pub(crate) fn next(&self, at: SystemTime) -> Ulid {
        self.0
            .lock()
            .expect("ULID generator mutex poisoned")
            .generate_from_datetime(at)
            .expect("the random bits of a millisecond do not overflow")
    }
}
