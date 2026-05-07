# Taquba

A durable, single-process task queue for Rust, plus higher-level patterns built on top.

Backed by object storage via [SlateDB](https://github.com/slatedb/slatedb).

## Crates

- [`taquba`](./taquba) — the core durable queue, backed by object storage.
- [`taquba-cron`](./taquba-cron) — POSIX cron-style scheduling on a Taquba queue.
- [`taquba-webhooks`](./taquba-webhooks) — HTTP webhook delivery on a Taquba queue.

See each crate's README for details.

## License

Apache-2.0
