# AGENTS.md

Rules for agents working in this repository. A crate's own `AGENTS.md` adds the
rules for that crate.

## Checks

Before reporting a change as done, run the commands of the `lint` and `test`
jobs in `.github/workflows/ci.yml`.

## Invariants

- **Delivery.** Do not add an exactly-once mode. See
  [ADR-0002](docs/decisions/0002-at-least-once-delivery.md).
- **State.** Design the key layout before adding durable state. Do not add a
  durable copy of the lease without a consumer outside the writing process.

## Versioning

Do not add migration code or a read path for an older on-disk layout. See
[ADR-0004](docs/decisions/0004-layout-breaks-between-minor-releases.md).

A new enum variant, struct field or function parameter in the public API goes
into a minor release. Do not add a compatibility alias or `#[non_exhaustive]`.

## Book

The book in `book/src/` is part of the public surface. A change to what a page
states updates the page in the same commit.
