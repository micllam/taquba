# Preface

This part of the book describes how taquba is built: the records it stores, the
keys they are stored at and the mechanisms that move a job through its lifecycle.

Everything described here is internal to the crates, and any minor release
before 1.0 can change it. The stability guarantees apply to each crate's public
API, documented on docs.rs: [taquba](https://docs.rs/taquba),
[taquba-workflow](https://docs.rs/taquba-workflow),
[taquba-cron](https://docs.rs/taquba-cron) and
[taquba-webhooks](https://docs.rs/taquba-webhooks).
