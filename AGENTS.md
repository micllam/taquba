# AGENTS.md

Rules for agents working in this repository. A crate's own `AGENTS.md` adds the
rules for that crate.

## Checks

Before reporting a change as done, run the commands of the `lint` and `test`
jobs in `.github/workflows/ci.yml`. Run `vale sync` after a clone.

## Invariants

- **Delivery.** Do not add an exactly-once mode. See
  [ADR-0002](docs/decisions/0002-at-least-once-delivery.md).
- **State.** Design the key layout before adding durable state. Do not add a
  durable copy of the lease without a consumer outside the writing process.
- **Ingress.** A request from outside the process enters through the inbox, and
  only the writer commits it. Do not add a reply path or a second channel. See
  [ADR-0005](docs/decisions/0005-bucket-inbox.md).
- **Signals.** The signal wait is the queue's. Do not add a waiter index, a
  signal buffer or a delivered record in a layer.

## Versioning

Do not add migration code or a read path for an older on-disk layout. See
[ADR-0004](docs/decisions/0004-layout-breaks-between-minor-releases.md).

A new enum variant, struct field or function parameter in the public API goes
into a minor release. Do not add a compatibility alias or `#[non_exhaustive]`.

## Book

The book in `book/src/` is part of the public surface. A change to what a page
states updates the page in the same commit.
